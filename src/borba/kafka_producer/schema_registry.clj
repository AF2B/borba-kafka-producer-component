(ns borba.kafka-producer.schema-registry
  "The Schema Registry of a producer: a JSON Schema for each topic, checked
   against the registry when the component starts, and the properties that make
   the client write each value in the wire format of Confluent, a magic byte
   and the id of its schema before the JSON.

     :schema-registry
     {:url       #env SCHEMA_REGISTRY_URL
      :schemas   {\"order-events\" {:type       \"object\"
                                    :properties {:event-type {:type \"string\"}}
                                    :required   [\"event-type\"]}}
      :register? false}

   The schemas are the contract, so they are registered on purpose: with
   :register? true the component registers them when it starts, which the
   registry refuses when a schema is not compatible with the versions before
   it; otherwise it checks that each one is registered, and fails the start
   when one is not. The serializer never registers one by itself.

   Every value is validated against the schema of its topic before it is
   sent, and a topic without a schema cannot be written to."
  (:require
   [clojure.string :as str]
   [jsonista.core :as jsonista])
  (:import
   (com.fasterxml.jackson.databind JsonNode ObjectMapper)
   (io.confluent.kafka.schemaregistry.client
    SchemaRegistryClient SchemaRegistryClientFactory)
   (io.confluent.kafka.schemaregistry.client.rest.exceptions
    RestClientException)
   (io.confluent.kafka.schemaregistry.json
    JsonSchema JsonSchemaProvider JsonSchemaUtils)
   (io.confluent.kafka.serializers.json KafkaJsonSchemaSerializer)
   (java.io IOException)
   (java.net URI)))

(set! *warn-on-reflection* true)

(def default-timeout-ms
  "How long a request to the registry waits to connect and to be answered,
   unless told otherwise."
  5000)

(def ^:private client-cache-capacity 100)

(def ^:private value-subject-suffix "-value")

;; The error codes of the registry for a subject, a version or a schema that it
;; does not have, and for a schema that is not compatible.
(def ^:private not-found-codes #{40401 40402 40403})

(def ^:private incompatible-code 409)

(def ^:private user-info-source "USER_INFO")

(defn subject
  "Returns the subject of the values of a topic, as the default strategy of
   Confluent names it.
   - topic: the name of the topic"
  ^String [topic]
  (str topic value-subject-suffix))

;; Options

(defn- invalid-option
  [option
   value
   expected]
  (ex-info (str ":schema-registry " (name option) " is "
                (if (= :basic-auth option) "not valid" (pr-str value))
                ", and must be " expected)
           {:error  ::invalid-option
            :option option}))

(defn- non-blank-string?
  [value]
  (and (string? value) (not (str/blank? value))))

(defn check-options
  "Checks the :schema-registry options of the component, and fails naming the
   first one that is not valid. The credentials are never in the message.
   - url: the URL of the registry
   - schemas: a map from each topic to its JSON Schema, as data or as text
   - register?: whether the start registers the schemas (default false: it
     checks that they are registered)
   - basic-auth: the credentials, \"user:password\" (default none)
   - timeout-ms: how long a request to the registry waits (default 5000)
   - options: the whole map, which is returned when it is valid"
  [{:keys [url schemas register? basic-auth timeout-ms] :as options}]
  (when-not (non-blank-string? url)
    (throw (invalid-option :url url "the URL of the registry")))
  (when-not (and (map? schemas)
                 (seq schemas)
                 (every? non-blank-string? (keys schemas))
                 (every? #(or (map? %) (non-blank-string? %)) (vals schemas)))
    (throw (invalid-option :schemas schemas
                           "a map from each topic to its JSON Schema")))
  (when-not (or (nil? register?) (boolean? register?))
    (throw (invalid-option :register? register? "true or false")))
  (when-not (or (nil? basic-auth) (non-blank-string? basic-auth))
    (throw (invalid-option :basic-auth basic-auth "\"user:password\"")))
  (when-not (or (nil? timeout-ms) (and (int? timeout-ms) (pos? timeout-ms)))
    (throw (invalid-option :timeout-ms timeout-ms "a positive integer")))
  options)

(defn- connection-properties
  "Returns the properties that reach the registry: where, with which
   credentials and how long to wait."
  [{:keys [url basic-auth timeout-ms]
    :or   {timeout-ms default-timeout-ms}}]
  (cond-> {"schema.registry.url"     url
           "http.connect.timeout.ms" (int timeout-ms)
           "http.read.timeout.ms"    (int timeout-ms)}
    basic-auth (assoc "basic.auth.credentials.source" user-info-source
                      "basic.auth.user.info"          basic-auth)))

(defn serializer-properties
  "Returns the properties of the client that write the values with the JSON
   Schema serializer of Confluent, which validates each value and never
   registers a schema by itself.
   - options: the :schema-registry options of the component"
  [options]
  (merge (connection-properties options)
         {"value.serializer"         (.getName KafkaJsonSchemaSerializer)
          "auto.register.schemas"    false
          "use.latest.version"       false
          "json.fail.invalid.schema" true}))

;; The start

(defn- address
  "Returns where the registry is, without the credentials a URL can carry."
  [url]
  (try
    (let [uri  (URI. url)
          port (.getPort uri)]
      (str (.getHost uri) (when (pos? port) (str ":" port))))
    (catch Exception _
      "the registry")))

(defn- parse-schema
  [topic
   schema]
  (try
    (let [^String text (if (string? schema)
                         schema
                         (jsonista/write-value-as-string schema))]
      (doto (JsonSchema. text)
        (.validate false)))
    (catch Exception cause
      (throw (ex-info (str "the schema of " topic " is not a JSON Schema")
                      {:error ::invalid-schema
                       :topic topic}
                      cause)))))

(defn- new-client
  ^SchemaRegistryClient [{:keys [url] :as options}]
  (let [^java.util.List urls      [url]
        ^java.util.List providers [(JsonSchemaProvider.)]
        ^java.util.Map  configs   (connection-properties options)
        ^java.util.Map  headers   {}]
    (SchemaRegistryClientFactory/newClient urls
                                           (int client-cache-capacity)
                                           providers
                                           configs
                                           headers)))

(defn- registry-failure
  "Returns the exception to throw for a schema the registry did not take or
   does not have, or for a registry that does not answer."
  [url
   topic
   ^Exception cause]
  (let [code (when (instance? RestClientException cause)
               (.getErrorCode ^RestClientException cause))
        data {:topic topic :subject (subject topic)}]
    (cond
      (contains? not-found-codes code)
      (ex-info (str "the schema of " topic " is not registered")
               (assoc data :error ::schema-not-registered)
               cause)

      (= incompatible-code code)
      (ex-info (str "the schema of " topic
                    " is not compatible with the registered versions")
               (assoc data :error ::incompatible-schema)
               cause)

      (instance? IOException cause)
      (ex-info (str "the schema registry does not answer on " (address url))
               {:error   ::cannot-connect
                :address (address url)}
               cause)

      :else
      (ex-info (str "the schema registry refused the schema of " topic)
               (assoc data :error ::registry-error :code code)
               cause))))

(defn- settle!
  "Registers the schema of a topic, or checks that it is registered."
  [^SchemaRegistryClient client
   url
   register?
   [topic ^JsonSchema schema]]
  (try
    (if register?
      (.register client (subject topic) schema)
      (.getId client (subject topic) schema))
    (catch Exception cause
      (throw (registry-failure url topic cause)))))

(defn start!
  "Parses the schemas and registers them, or checks that they are registered,
   and returns a map of the :schemas, a map from each topic to its parsed
   schema, and the :client of the registry, which `stop!` closes. Fails the
   start, closing the client, when a schema is not valid, not registered or
   not compatible, or when the registry does not answer.
   - url: the URL of the registry
   - schemas: a map from each topic to its JSON Schema
   - register?: whether to register the schemas, or to check that they are
   - options: the rest of the :schema-registry options, see `check-options`"
  [{:keys [url schemas register?] :as options}]
  (let [parsed (into {}
                     (map (fn [[topic schema]]
                            [topic (parse-schema topic schema)]))
                     schemas)
        client (new-client options)]
    (try
      (run! #(settle! client url register? %) parsed)
      {:schemas parsed
       :client  client}
      (catch Exception failure
        (.close client)
        (throw failure)))))

(defn stop!
  "Closes the client of the registry.
   - client: the client in what `start!` returned (nil does nothing)"
  [{:keys [^SchemaRegistryClient client]}]
  (when client
    (.close client)))

;; Sending

(defn envelope
  "Returns a value wrapped with the schema of its topic, which is what the
   serializer writes. A topic without a schema fails, since nothing could
   check what is written to it.
   - schemas: a map from each topic to its parsed schema
   - topic: the name of the topic
   - value: the value, as data"
  [schemas
   topic
   value]
  (if-let [schema (get schemas topic)]
    (JsonSchemaUtils/envelope ^JsonSchema schema
                              ^JsonNode (.valueToTree
                                         ^ObjectMapper
                                         jsonista/default-object-mapper
                                         value))
    (throw (ex-info (str "the topic " topic " has no schema")
                    {:error ::unknown-schema
                     :topic topic}))))
