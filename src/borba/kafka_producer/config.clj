(ns borba.kafka-producer.config
  "The options of the producer: checked, and turned into the properties of the
   Kafka client.

   What a service needs from a producer is that a message is stored, so it is
   written with acks=all and idempotence, which makes a retry not a duplicate.
   Every wait is bounded: Kafka waits a minute for the metadata of a topic and
   two minutes to deliver a message unless told otherwise, which for a service
   that answers requests is the same as not answering."
  (:require
   [clojure.string :as str])
  (:import
   (org.apache.kafka.clients.producer ProducerConfig)
   (org.apache.kafka.common.serialization StringSerializer)))

(set! *warn-on-reflection* true)

(def default-client-id
  "The name of the producer for the brokers, unless told otherwise."
  "borba-producer")

(def default-linger-ms
  "How long a message waits to be sent with others, unless told otherwise."
  5)

(def default-max-block-ms
  "How long a send waits for the metadata of a topic or for room in its buffer,
   unless told otherwise."
  5000)

(def default-delivery-timeout-ms
  "How long a message has to be delivered, retries included, unless told
   otherwise."
  30000)

(def default-request-timeout-ms
  "How long a request waits for the answer of a broker, unless told
   otherwise."
  10000)

(def default-close-timeout-ms
  "How long closing the producer waits for the messages that are not sent yet,
   unless told otherwise."
  10000)

(def default-compression-type
  "How the messages are compressed, unless told otherwise."
  "none")

(def compression-types
  "The ways the messages can be compressed."
  #{"none" "gzip" "snappy" "lz4" "zstd"})

(defn- invalid-option
  [option
   value
   expected]
  (ex-info (str ":" (name option) " is " (pr-str value) ", and must be "
                expected)
           {:error  ::invalid-option
            :option option
            :value  value}))

(defn- positive-int?
  [value]
  (and (int? value) (pos? value)))

(defn- servers
  "Returns the brokers as the text the client takes, or nil when they are not
   given as one."
  [bootstrap-servers]
  (cond
    (and (string? bootstrap-servers) (not (str/blank? bootstrap-servers)))
    bootstrap-servers

    (and (sequential? bootstrap-servers)
         (seq bootstrap-servers)
         (every? #(and (string? %) (not (str/blank? %))) bootstrap-servers))
    (str/join "," bootstrap-servers)))

(defn client-properties
  "Checks the options of the component and returns the properties of the Kafka
   client that they make, as a map of strings to values. Fails naming the first
   option that is not valid.
   - bootstrap-servers: the brokers, \"host:port,host:port\" or a vector of
     \"host:port\"
   - client-id: the name of the producer for the brokers (default
     \"borba-producer\")
   - linger-ms: how long a message waits to be sent with others (default 5)
   - compression-type: none, gzip, snappy, lz4 or zstd (default none)
   - max-block-ms: how long a send waits for metadata or for room (default
     5000)
   - delivery-timeout-ms: how long a message has to be delivered (default
     30000)
   - request-timeout-ms: how long a request waits for a broker, which cannot be
     more than the delivery timeout (default 10000)
   - properties: more properties of the client, a map of strings to strings,
     such as the security protocol and the SASL settings, which take the
     place of the ones above (default none)"
  [{:keys [bootstrap-servers client-id linger-ms compression-type max-block-ms
           delivery-timeout-ms request-timeout-ms properties]
    :or   {client-id           default-client-id
           linger-ms           default-linger-ms
           compression-type    default-compression-type
           max-block-ms        default-max-block-ms
           delivery-timeout-ms default-delivery-timeout-ms
           request-timeout-ms  default-request-timeout-ms}}]
  (let [brokers (servers bootstrap-servers)]
    (when-not brokers
      (throw (invalid-option :bootstrap-servers bootstrap-servers
                             "\"host:port\" or a vector of them")))
    (when-not (and (string? client-id) (not (str/blank? client-id)))
      (throw (invalid-option :client-id client-id "a non-empty string")))
    (when-not (and (int? linger-ms) (not (neg? linger-ms)))
      (throw (invalid-option :linger-ms linger-ms "zero or more")))
    (when-not (contains? compression-types compression-type)
      (throw (invalid-option :compression-type compression-type
                             (str "one of " (sort compression-types)))))
    (doseq [[option value] [[:max-block-ms max-block-ms]
                            [:delivery-timeout-ms delivery-timeout-ms]
                            [:request-timeout-ms request-timeout-ms]]]
      (when-not (positive-int? value)
        (throw (invalid-option option value "a positive integer"))))
    (when-not (<= request-timeout-ms delivery-timeout-ms)
      (throw (invalid-option :request-timeout-ms request-timeout-ms
                             "no more than :delivery-timeout-ms")))
    (when-not (or (nil? properties)
                  (and (map? properties)
                       (every? string? (keys properties))
                       (every? string? (vals properties))))
      (throw (invalid-option :properties properties
                             "a map of strings to strings")))
    (merge {ProducerConfig/BOOTSTRAP_SERVERS_CONFIG      brokers
            ProducerConfig/CLIENT_ID_CONFIG              client-id
            ProducerConfig/KEY_SERIALIZER_CLASS_CONFIG
            (.getName StringSerializer)
            ProducerConfig/VALUE_SERIALIZER_CLASS_CONFIG
            (.getName StringSerializer)
            ProducerConfig/ACKS_CONFIG                   "all"
            ProducerConfig/ENABLE_IDEMPOTENCE_CONFIG     true
            ProducerConfig/LINGER_MS_CONFIG              (int linger-ms)
            ProducerConfig/COMPRESSION_TYPE_CONFIG       compression-type
            ProducerConfig/MAX_BLOCK_MS_CONFIG           (long max-block-ms)
            ProducerConfig/DELIVERY_TIMEOUT_MS_CONFIG
            (int delivery-timeout-ms)
            ProducerConfig/REQUEST_TIMEOUT_MS_CONFIG
            (int request-timeout-ms)}
           properties)))
