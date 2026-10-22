(ns borba.kafka-producer.schema-registry-test
  (:require
   [borba.kafka-producer.config :as config]
   [borba.kafka-producer.errors :as errors]
   [borba.kafka-producer.schema-registry :as schema-registry]
   [clojure.test :refer [deftest is testing]]
   [jsonista.core :as jsonista])
  (:import
   (io.confluent.kafka.schemaregistry.client SchemaRegistryClient)
   (io.confluent.kafka.serializers.json KafkaJsonSchemaSerializer)
   (java.util Arrays)))

(set! *warn-on-reflection* true)

(def ^:private order-schema
  {:type                 "object"
   :properties           {:total {:type "number"}
                          :paid? {:type "boolean"}}
   :required             ["total"]
   :additionalProperties false})

(defn- mock-url
  "A registry of its own for each test: the mock registries of Confluent are
   shared by scope across the JVM."
  []
  (str "mock://" (random-uuid)))

(defn- options
  [url]
  {:url     url
   :schemas {"orders" order-schema}})

(defn- thrown-data
  [f]
  (try (f)
       nil
       (catch clojure.lang.ExceptionInfo e (ex-data e))))

(defn- serializer
  ^KafkaJsonSchemaSerializer [url]
  (doto (KafkaJsonSchemaSerializer.)
    (.configure (dissoc (schema-registry/serializer-properties {:url url})
                        "value.serializer")
                false)))

(defn- are-invalid
  []
  (doseq [[option value] [[:url nil]
                          [:url " "]
                          [:schemas {}]
                          [:schemas {"orders" 42}]
                          [:schemas {"" order-schema}]
                          [:register? "yes"]
                          [:timeout-ms 0]]]
    (is (= {:error  ::schema-registry/invalid-option
            :option option}
           (thrown-data #(schema-registry/check-options
                          (assoc (options "mock://x") option value))))
        (str option " " (pr-str value)))))

(deftest options-test
  (testing "takes a URL and a schema per topic"
    (is (= (options "mock://x")
           (schema-registry/check-options (options "mock://x")))))

  (testing "names the option that is not valid"
    (are-invalid))

  (testing "never puts the credentials in the message"
    (let [failure (try (schema-registry/check-options
                        (assoc (options "mock://x") :basic-auth ""))
                       (catch clojure.lang.ExceptionInfo e e))]
      (is (= :basic-auth (:option (ex-data failure))))
      (is (not (re-find #"\"\"" (ex-message failure)))))))

(deftest properties-test
  (testing "writes the values with the JSON Schema serializer, which validates
            them and registers nothing"
    (let [properties (config/client-properties
                      {:bootstrap-servers "localhost:9092"
                       :schema-registry   (options "http://registry:8081")})]
      (is (= (.getName KafkaJsonSchemaSerializer)
             (get properties "value.serializer")))
      (is (= "http://registry:8081" (get properties "schema.registry.url")))
      (is (false? (get properties "auto.register.schemas")))
      (is (true? (get properties "json.fail.invalid.schema")))
      (is (= 5000 (get properties "http.read.timeout.ms")))))

  (testing "sends the credentials as user info"
    (let [properties (schema-registry/serializer-properties
                      (assoc (options "http://registry:8081")
                             :basic-auth "app:secret"))]
      (is (= "USER_INFO" (get properties "basic.auth.credentials.source")))
      (is (= "app:secret" (get properties "basic.auth.user.info")))))

  (testing "without a registry the values are JSON text"
    (is (not= (.getName KafkaJsonSchemaSerializer)
              (get (config/client-properties
                    {:bootstrap-servers "localhost:9092"})
                   "value.serializer")))))

(deftest start-test
  (testing "registers the schemas when told to, and then finds them"
    (let [url        (mock-url)
          registered (schema-registry/start! (assoc (options url)
                                                    :register? true))]
      (is (= ["orders"] (keys (:schemas registered))))
      (schema-registry/stop! registered)
      (let [checked (schema-registry/start! (options url))]
        (is (instance? SchemaRegistryClient (:client checked)))
        (schema-registry/stop! checked))))

  (testing "fails the start for a schema that is not registered"
    (is (= {:error   ::schema-registry/schema-not-registered
            :topic   "orders"
            :subject "orders-value"}
           (thrown-data #(schema-registry/start! (options (mock-url)))))))

  (testing "fails the start for a schema that is not a JSON Schema"
    (is (= {:error ::schema-registry/invalid-schema
            :topic "orders"}
           (thrown-data #(schema-registry/start!
                          (assoc (options (mock-url))
                                 :schemas {"orders" {:type 42}}))))))

  (testing "fails the start for a registry that does not answer"
    (is (= {:error   ::schema-registry/cannot-connect
            :address "127.0.0.1:1"}
           (thrown-data #(schema-registry/start!
                          (assoc (options "http://127.0.0.1:1")
                                 :timeout-ms 500)))))))

(deftest envelope-test
  (let [url        (mock-url)
        registered (schema-registry/start! (assoc (options url)
                                                  :register? true))
        schemas    (:schemas registered)
        serialize  (fn [value]
                     (.serialize (serializer url)
                                 "orders"
                                 (schema-registry/envelope schemas
                                                           "orders"
                                                           value)))]
    (try
      (testing "writes the magic byte, the id of the schema and the JSON"
        (let [written ^bytes (serialize {:total 10 :paid? false})]
          (is (= 0 (aget written 0)))
          (is (= {:total 10 :paid? false}
                 (jsonista/read-value
                  (Arrays/copyOfRange written 5 (alength written))
                  jsonista/keyword-keys-object-mapper)))))

      (testing "refuses a value that its schema does not allow"
        (let [failure (try (serialize {:total "ten"})
                           (catch Exception e e))]
          (is (= :schema-violation (:error (errors/error-data failure))))))

      (testing "a topic without a schema cannot be written to"
        (is (= {:error ::schema-registry/unknown-schema
                :topic "payments"}
               (thrown-data #(schema-registry/envelope schemas
                                                       "payments"
                                                       {:total 1})))))
      (finally
        (schema-registry/stop! registered)))))

(deftest error-test
  (testing "a schema the registry does not have"
    (let [failure (try (.serialize (serializer (mock-url))
                                   "orders"
                                   (schema-registry/envelope
                                    (:schemas (schema-registry/start!
                                               (assoc (options (mock-url))
                                                      :register? true)))
                                    "orders"
                                    {:total 1}))
                       (catch Exception e e))]
      (is (= :schema-not-registered (:error (errors/error-data failure))))))

  (testing "a registry that does not answer"
    (let [url     "http://127.0.0.1:1"
          broken  (doto (KafkaJsonSchemaSerializer.)
                    (.configure (dissoc (schema-registry/serializer-properties
                                         {:url url :timeout-ms 500})
                                        "value.serializer")
                                false))
          schemas (:schemas (schema-registry/start!
                             (assoc (options (mock-url)) :register? true)))
          failure (try (.serialize broken
                                   "orders"
                                   (schema-registry/envelope schemas
                                                             "orders"
                                                             {:total 1}))
                       (catch Exception e e))]
      (is (= :schema-registry-unavailable
             (:error (errors/error-data failure)))))))
