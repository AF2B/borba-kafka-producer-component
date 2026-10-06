(ns borba.kafka-producer-test
  (:require
   [borba.kafka-producer :as kafka]
   [clojure.test :refer [deftest is testing]])
  (:import
   (java.nio.charset StandardCharsets)
   (java.util.concurrent CompletableFuture)
   (org.apache.kafka.clients.producer MockProducer ProducerRecord)
   (org.apache.kafka.common.header Header)
   (org.apache.kafka.common.serialization StringSerializer)))

(set! *warn-on-reflection* true)

(defn- mock
  "A producer that stores what it is given, and completes every send at once,
   or when it is told, which is how a send fails."
  ([]
   (mock true))
  ([auto-complete?]
   (MockProducer. auto-complete? nil (StringSerializer.) (StringSerializer.))))

(defn- history
  [^MockProducer producer]
  (vec (.history producer)))

(defn- header-map
  "Returns the headers of a record as a map of names to text."
  [^ProducerRecord record]
  (into {}
        (map (fn [^Header header]
               [(.key header) (String. (.value header)
                                       StandardCharsets/UTF_8)]))
        (.headers record)))

(defn- thrown-error
  [f]
  (try (f)
       nil
       (catch clojure.lang.ExceptionInfo e (:error (ex-data e)))))

(deftest send-test
  (testing "sends the key as text and the value as JSON"
    (let [producer (mock)
          id       (random-uuid)]
      @(kafka/send! {:producer producer} "orders" id {:total 10 :paid? false})
      (let [[sent] (history producer)]
        (is (= "orders" (.topic ^ProducerRecord sent)))
        (is (= (str id) (.key ^ProducerRecord sent)))
        (is (= "{\"total\":10,\"paid?\":false}"
               (.value ^ProducerRecord sent))))))

  (testing "sends a message without a key"
    (let [producer (mock)]
      @(kafka/send! {:producer producer} "orders" nil {:a 1})
      (is (nil? (.key ^ProducerRecord (first (history producer)))))))

  (testing "sends a nil value as a tombstone"
    (let [producer (mock)]
      @(kafka/send! {:producer producer} "orders" "k" nil)
      (is (nil? (.value ^ProducerRecord (first (history producer)))))))

  (testing "sends the headers as text, and the partition when told"
    (let [producer (mock)]
      @(kafka/send! {:producer producer}
                    "orders"
                    "k"
                    {:a 1}
                    {:headers {"request-id" "abc" :version 2}
                     :partition 0})
      (let [^ProducerRecord sent (first (history producer))]
        (is (= {"request-id" "abc" "version" "2"} (header-map sent)))
        (is (= 0 (.partition sent))))))

  (testing "returns where the record was stored, as data"
    (let [producer (mock)
          result   @(kafka/send! {:producer producer} "orders" "k" {:a 1})]
      (is (= "orders" (:topic result)))
      (is (= #{:topic :partition :offset :timestamp} (set (keys result)))))))

(deftest send-sync-test
  (testing "waits, and returns where the record was stored"
    (let [producer (mock)
          result   (kafka/send-sync! {:producer producer} "orders" "k" {:a 1})]
      (is (= "orders" (:topic result)))
      (is (= 1 (count (history producer)))))))

(deftest send-batch-test
  (testing "sends each message, and says what became of each, in order"
    (let [producer (mock)
          results  (kafka/send-batch! {:producer producer}
                                      "orders"
                                      [{:key "a" :value {:n 1}}
                                       {:key "b" :value {:n 2}
                                        :headers {"h" "x"}}
                                       {:key "c" :value nil}])]
      (is (= 3 (count results)))
      (is (every? #(= "orders" (:topic %)) results))
      (is (= ["a" "b" "c"]
             (mapv #(.key ^ProducerRecord %) (history producer))))
      (is (= {"h" "x"} (header-map (second (history producer))))))))

(deftest failure-test
  (testing "a send that cannot be delivered fails the future, with the failure"
    (let [^MockProducer producer (mock false)
          result   (kafka/send! {:producer producer} "orders" "k" {:a 1})]
      (.errorNext producer (org.apache.kafka.common.errors.TimeoutException.
                            "no brokers"))
      (is (= :timeout
             (try @result
                  nil
                  (catch Exception e (:error (kafka/error-data e))))))))

  (testing "a topic that is not a name is refused before anything is sent"
    (let [producer (mock)]
      (doseq [bad [nil "" "  " :orders 5]]
        (is (= :borba.kafka-producer/invalid-topic
               (thrown-error #(kafka/send! {:producer producer} bad "k" {})))
            (pr-str bad)))
      (is (empty? (history producer)))))

  (testing "an exception that is not Kafka's is nil"
    (is (nil? (kafka/error-data (IllegalStateException. "no"))))))

(deftest flush-test
  (testing "sends what is waiting"
    (let [producer (mock false)
          result   (kafka/send! {:producer producer} "orders" "k" {:a 1})]
      (is (not (.isDone ^CompletableFuture result)))
      (kafka/flush! {:producer producer})
      (is (= "orders" (:topic @result))))))
