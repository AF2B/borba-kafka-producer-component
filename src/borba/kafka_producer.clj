(ns borba.kafka-producer
  "Kafka producer for a service: an Integrant component, and the functions that
   send a message as JSON.

     :components/kafka-producer
     {:bootstrap-servers #or [#env KAFKA_BROKERS \"localhost:9092\"]
      :client-id         \"orders-service\"}

   The value is the producer that every function takes first.

   A message is stored when the brokers have it, and a send that cannot be
   delivered says so. `send!` is asynchronous and returns a future of the
   metadata of the record, which is the place it was stored: a map of :topic,
   :partition, :offset and :timestamp. A failure that nobody waits for is not
   lost: it is logged, with the topic and the class of the failure, never the
   message. `send-sync!` waits, and `send-batch!` sends many and waits for them
   all.

   The key is the text of what is given, so a UUID or a keyword is a key, and
   messages with the same key go to the same partition, in order. The value is
   its JSON; a nil value is a tombstone, which is how a compacted topic
   forgets a key.

   Every wait is bounded: see borba.kafka-producer.config."
  (:require
   [borba.kafka-producer.config :as config]
   [borba.kafka-producer.errors :as errors]
   [clojure.string :as str]
   [clojure.tools.logging :as log]
   [integrant.core :as ig]
   [jsonista.core :as jsonista])
  (:import
   (java.nio.charset StandardCharsets)
   (java.time Duration)
   (java.util.concurrent CompletableFuture TimeUnit)
   (org.apache.kafka.clients.admin Admin AdminClient AdminClientConfig)
   (org.apache.kafka.clients.producer
    Callback KafkaProducer Producer ProducerRecord RecordMetadata)
   (org.apache.kafka.common KafkaFuture)
   (org.apache.kafka.common.header Headers)))

(set! *warn-on-reflection* true)

(def default-verify-timeout-ms
  "How long the start waits for the brokers to answer, unless told otherwise."
  5000)

;; The component

(defn- verify-brokers!
  "Asks the brokers who is in the cluster, and fails when none answers in the
   time given."
  [brokers
   timeout-ms]
  (let [properties {AdminClientConfig/BOOTSTRAP_SERVERS_CONFIG brokers
                    AdminClientConfig/REQUEST_TIMEOUT_MS_CONFIG
                    (int timeout-ms)
                    AdminClientConfig/DEFAULT_API_TIMEOUT_MS_CONFIG
                    (int timeout-ms)}]
    (with-open [^Admin admin (AdminClient/create ^java.util.Map properties)]
      (.get ^KafkaFuture (.nodes (.describeCluster admin))
            (long timeout-ms)
            TimeUnit/MILLISECONDS))))

(defmethod ig/init-key :components/kafka-producer
  [_ {:keys [close-timeout-ms verify-connection? verify-timeout-ms]
      :or   {close-timeout-ms     config/default-close-timeout-ms
             verify-connection?   true
             verify-timeout-ms    default-verify-timeout-ms}
      :as   options}]
  (let [properties (config/client-properties options)
        brokers    (get properties "bootstrap.servers")
        client-id  (get properties "client.id")]
    (when verify-connection?
      (try (verify-brokers! brokers verify-timeout-ms)
           (catch Exception cause
             (throw (ex-info (str "the Kafka brokers do not answer on "
                                  brokers)
                             {:error   ::cannot-connect
                              :brokers brokers}
                             cause)))))
    (log/infof "kafka producer %s started on %s" client-id brokers)
    {:producer         (KafkaProducer. ^java.util.Map properties)
     :close-timeout-ms close-timeout-ms
     :client-id        client-id}))

(defmethod ig/halt-key! :components/kafka-producer
  [_ {:keys [^Producer producer close-timeout-ms client-id]}]
  (when producer
    (.close producer (Duration/ofMillis close-timeout-ms))
    (log/infof "kafka producer %s stopped" client-id)))

;; Sending

(defn- metadata->map
  "Returns where a record was stored, as data."
  [^RecordMetadata metadata]
  {:topic     (.topic metadata)
   :partition (.partition metadata)
   :offset    (.offset metadata)
   :timestamp (.timestamp metadata)})

(defn- check-topic
  [topic]
  (when-not (and (string? topic) (not (str/blank? topic)))
    (throw (ex-info ":topic must be a non-empty string"
                    {:error ::invalid-topic
                     :topic topic}))))

(defn- add-headers!
  "Adds the headers to a record, as UTF-8 text."
  [^Headers headers
   header-map]
  (doseq [[header-name header-value] header-map]
    (.add headers
          ^String (name header-name)
          (.getBytes ^String (str header-value) StandardCharsets/UTF_8))))

(defn- message-record
  "Returns the record of a message."
  [topic
   message-key
   value
   {:keys [headers] partition-number :partition}]
  (check-topic topic)
  (let [kafka-record (ProducerRecord. ^String topic
                                      ^Integer (some-> partition-number int)
                                      ^String (some-> message-key str)
                                      ^String (when (some? value)
                                                (jsonista/write-value-as-string
                                                 value)))]
    (add-headers! (.headers kafka-record) headers)
    kafka-record))

(defn send!
  "Sends a message to a topic and returns at once with a future of where it was
   stored, a map of :topic, :partition, :offset and :timestamp. Waiting for the
   future, with deref, gives the map, or throws when the message could not be
   delivered; `error-data` says why. A failure that nobody waits for is logged.
   - producer: the value of the component
   - topic: the name of the topic
   - message-key: what orders the messages, as text: messages with the same key
     are in the same partition, in order (nil for no key)
   - value: what is sent, as JSON (nil sends a tombstone)
   - opts: a map of :headers, a map of names to values that are sent as text,
     and :partition, to choose the partition (optional)"
  ([producer
    topic
    message-key
    value]
   (send! producer topic message-key value {}))
  ([producer
    topic
    message-key
    value
    opts]
   (let [^Producer kafka-producer (:producer producer)
         result                   (CompletableFuture.)]
     (.send kafka-producer
            (message-record topic message-key value opts)
            (reify Callback
              (onCompletion [_ metadata exception]
                (if exception
                  (do (log/errorf "kafka send to %s failed: %s"
                                  topic
                                  (:error (errors/error-data exception)))
                      (.completeExceptionally result exception))
                  (.complete result (metadata->map metadata))))))
     result)))

(defn send-sync!
  "Sends a message to a topic and waits until the brokers have it, which is at
   most the delivery timeout, and returns where it was stored: a map of
   :topic, :partition, :offset and :timestamp. Throws when it could not be
   delivered; `error-data` says why.
   - producer: the value of the component
   - topic: the name of the topic
   - message-key: what orders the messages, as text (nil for no key)
   - value: what is sent, as JSON (nil sends a tombstone)
   - opts: a map of :headers and :partition (optional)"
  ([producer
    topic
    message-key
    value]
   (send-sync! producer topic message-key value {}))
  ([producer
    topic
    message-key
    value
    opts]
   @(send! producer topic message-key value opts)))

(defn send-batch!
  "Sends the messages to a topic, then waits for all of them, and returns what
   became of each, in order: where it was stored, a map of :topic,
   :partition, :offset and :timestamp, or the failure as data, a map with an
   :error, from `error-data`. One that failed does not keep the others from
   being sent.
   - producer: the value of the component
   - topic: the name of the topic
   - messages: a collection of maps of :key, :value and, optionally,
     :headers"
  [producer
   topic
   messages]
  (let [sent (doall (map (fn [{:keys [value headers] message-key :key}]
                           (send! producer
                                  topic
                                  message-key
                                  value
                                  {:headers headers}))
                         messages))]
    (.flush ^Producer (:producer producer))
    (mapv (fn [^CompletableFuture result]
            (try @result
                 (catch Exception e
                   (or (errors/error-data e)
                       {:error :kafka-error}))))
          sent)))

(defn flush!
  "Sends what is waiting to be sent, and returns when it has been.
   - producer: the value of the component"
  [producer]
  (.flush ^Producer (:producer producer)))

;; Health and failures

(defn ready?
  "Returns true when the brokers answer for a topic, within the time the
   producer waits for metadata, and false when they do not. It never throws, so
   a readiness check can call it.
   - producer: the value of the component
   - topic: the name of a topic the service writes to"
  [producer topic]
  (try
    (boolean (seq (.partitionsFor ^Producer (:producer producer) topic)))
    (catch Exception _
      false)))

(defn error-data
  "Returns the failure of a Kafka exception as data, with an :error keyword
   such as :timeout or :record-too-large, or nil when the exception is not
   Kafka's. See borba.kafka-producer.errors.
   - e: the exception"
  [e]
  (errors/error-data e))
