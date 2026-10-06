(ns ^:integration borba.kafka-producer.integration-test
  "Runs against a real Kafka, which the pipeline provides and which a developer
   starts with

     docker run --rm -d --name borba-kafka-it -p 127.0.0.1:9092:9092 \\
       apache/kafka:4.0.0

   and points the tests at with KAFKA_BROKERS (127.0.0.1:9092)."
  (:require
   [borba.kafka-producer :as kafka]
   [clojure.test :refer [deftest is testing]]
   [integrant.core :as ig])
  (:import
   (java.nio.charset StandardCharsets)
   (java.time Duration)
   (org.apache.kafka.clients.consumer
    ConsumerConfig ConsumerRecord KafkaConsumer)
   (org.apache.kafka.common TopicPartition)
   (org.apache.kafka.common.header Header)
   (org.apache.kafka.common.serialization StringDeserializer)))

(set! *warn-on-reflection* true)

(def ^:private wait-seconds 20)
(def ^:private poll-ms 500)

(defn- brokers
  []
  (or (System/getenv "KAFKA_BROKERS")
      (throw (ex-info "set KAFKA_BROKERS to run the integration tests" {}))))

(defn- topic-name
  [label]
  (str "borba-it-" label "-" (subs (str (random-uuid)) 0 8)))

(defn- start
  "Starts the component, with more options, and returns the system."
  [more]
  (ig/init {:components/kafka-producer
            (merge {:bootstrap-servers (brokers)
                    :client-id         "borba-it"}
                   more)}))

(defn- producer-of
  [system]
  (:components/kafka-producer system))

(defn- consumer-properties
  []
  {ConsumerConfig/BOOTSTRAP_SERVERS_CONFIG (brokers)
   ConsumerConfig/KEY_DESERIALIZER_CLASS_CONFIG (.getName StringDeserializer)
   ConsumerConfig/VALUE_DESERIALIZER_CLASS_CONFIG (.getName StringDeserializer)
   ConsumerConfig/ENABLE_AUTO_COMMIT_CONFIG "false"})

(defn- header-text
  "Returns the headers of a record as a map of names to text."
  [^ConsumerRecord record]
  (into {}
        (map (fn [^Header header]
               [(.key header)
                (String. (.value header) StandardCharsets/UTF_8)]))
        (.headers record)))

(defn- record-map
  "Returns what a test wants to know about a record, as data."
  [^ConsumerRecord record]
  {:key       (.key record)
   :value     (.value record)
   :partition (.partition record)
   :offset    (.offset record)
   :headers   (header-text record)})

(defn- consume
  "Reads what is in the partition 0 of a topic from its start, until there are
   as many records as asked for or the time is out."
  [topic
   expected]
  (let [properties (consumer-properties)]
    (with-open [consumer (KafkaConsumer. ^java.util.Map properties)]
      (let [topic-partition (TopicPartition. topic 0)
            deadline        (+ (System/currentTimeMillis)
                               (* wait-seconds 1000))]
        (.assign consumer [topic-partition])
        (.seekToBeginning consumer [topic-partition])
        (loop [records []]
          (if (or (>= (count records) expected)
                  (> (System/currentTimeMillis) deadline))
            records
            (recur (into records
                         (map record-map)
                         (.poll consumer (Duration/ofMillis poll-ms))))))))))

(deftest sending-test
  (let [system   (start {})
        producer (producer-of system)
        topic    (topic-name "send")]
    (try
      (testing "stores a message, and says where"
        (let [id     (random-uuid)
              stored (kafka/send-sync! producer topic id {:total 10 :ok true}
                                       {:headers {"request-id" "abc"}})
              [stored-record] (consume topic 1)]
          (is (= topic (:topic stored)))
          (is (= 0 (:partition stored)))
          (is (= 0 (:offset stored)))
          (is (= (str id) (:key stored-record)))
          (is (= "{\"total\":10,\"ok\":true}" (:value stored-record)))
          (is (= {"request-id" "abc"} (:headers stored-record)))
          (is (= (:offset stored) (:offset stored-record)))))

      (testing "stores a tombstone as a null value"
        (kafka/send-sync! producer topic "gone" nil)
        (is (nil? (:value (second (consume topic 2))))))

      (testing "keeps the messages of a key in the order they were sent in"
        (let [ordered (topic-name "order")]
          (doseq [n (range 20)]
            (kafka/send! producer ordered "same-key" {:n n}))
          (kafka/flush! producer)
          (is (= (range 20)
                 (map #(-> % :value (subs 5 (dec (count (:value %))))
                           parse-long)
                      (consume ordered 20))))))
      (finally
        (ig/halt! system)))))

(deftest asynchronous-test
  (let [system   (start {})
        producer (producer-of system)
        topic    (topic-name "async")]
    (try
      (testing "returns a future of where the record was stored"
        (let [result (kafka/send! producer topic "k" {:a 1})]
          (is (= topic (:topic @result)))))

      (testing "fails the future of a record that cannot be stored, saying why"
        (let [too-large (apply str (repeat (* 2 1024 1024) "x"))
              result    (kafka/send! producer topic "k" {:blob too-large})]
          (is (= :record-too-large
                 (try @result
                      nil
                      (catch Exception e (:error (kafka/error-data e))))))))
      (finally
        (ig/halt! system)))))

(deftest batch-test
  (let [system   (start {})
        producer (producer-of system)
        topic    (topic-name "batch")]
    (try
      (testing "says what became of each message, and a failure keeps no other"
        (let [too-large (apply str (repeat (* 2 1024 1024) "x"))
              results   (kafka/send-batch!
                         producer
                         topic
                         [{:key "a" :value {:n 1}}
                          {:key "b" :value {:blob too-large}}
                          {:key "c" :value {:n 3}}])]
          (is (= topic (:topic (first results))))
          (is (= :record-too-large (:error (second results))))
          (is (= topic (:topic (nth results 2))))
          (is (= ["a" "c"] (mapv :key (consume topic 2))))))
      (finally
        (ig/halt! system)))))

(deftest compression-test
  (testing "compresses with each codec the client has, and the messages arrive"
    (doseq [codec ["gzip" "snappy" "lz4" "zstd"]]
      (let [system   (start {:compression-type codec})
            producer (producer-of system)
            topic    (topic-name codec)]
        (try
          (kafka/send-sync! producer
                            topic
                            "k"
                            {:codec   codec
                             :padding (apply str (repeat 200 "z"))})
          (is (= codec
                 (second (re-find #"\"codec\":\"(\w+)\""
                                  (:value (first (consume topic 1))))))
              codec)
          (finally
            (ig/halt! system)))))))

(deftest health-test
  (let [system   (start {})
        producer (producer-of system)]
    (try
      (testing "the brokers answer for a topic"
        (is (true? (kafka/ready? producer (topic-name "ready")))))
      (finally
        (ig/halt! system)))))

(deftest lifecycle-test
  (testing "a producer that is stopped sends no more"
    (let [system   (start {})
          producer (producer-of system)]
      (ig/halt! system)
      (is (thrown? IllegalStateException
                   (kafka/send! producer (topic-name "closed") "k" {:a 1})))))

  (testing "brokers that do not answer fail the start, naming where they were"
    (let [started (System/nanoTime)
          thrown  (try (ig/init {:components/kafka-producer
                                 {:bootstrap-servers "127.0.0.1:1"
                                  :verify-timeout-ms 800}})
                       nil
                       (catch clojure.lang.ExceptionInfo e e))]
      (is (= {:error   :borba.kafka-producer/cannot-connect
              :brokers "127.0.0.1:1"}
             (ex-data (ex-cause thrown))))
      (is (< (/ (- (System/nanoTime) started) 1e6) 5000))))

  (testing "a send to brokers that do not answer fails soon, as a timeout"
    (let [system  (ig/init {:components/kafka-producer
                            {:bootstrap-servers  "127.0.0.1:1"
                             :verify-connection? false
                             :max-block-ms       500}})
          started (System/nanoTime)]
      (try
        (is (= :timeout
               (try (kafka/send-sync! (producer-of system)
                                      "nowhere"
                                      "k"
                                      {:a 1})
                    nil
                    (catch Exception e (:error (kafka/error-data e))))))
        (is (< (/ (- (System/nanoTime) started) 1e6) 5000))
        (finally
          (ig/halt! system))))))
