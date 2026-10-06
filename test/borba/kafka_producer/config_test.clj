(ns borba.kafka-producer.config-test
  (:require
   [borba.kafka-producer.config :as config]
   [clojure.test :refer [deftest is testing]]))

(defn- invalid-option
  "Returns the option that the options are refused for, or nil."
  [options]
  (try (config/client-properties options)
       nil
       (catch clojure.lang.ExceptionInfo e
         (when (= :borba.kafka-producer.config/invalid-option
                  (:error (ex-data e)))
           (:option (ex-data e))))))

(def ^:private brokers {:bootstrap-servers "kafka:9092"})

(deftest client-properties-test
  (testing "stores what the brokers have, and fails soon, by default"
    (is (= {"bootstrap.servers"       "kafka:9092"
            "client.id"               "borba-producer"
            "key.serializer"
            "org.apache.kafka.common.serialization.StringSerializer"
            "value.serializer"
            "org.apache.kafka.common.serialization.StringSerializer"
            "acks"                    "all"
            "enable.idempotence"      true
            "linger.ms"               5
            "compression.type"        "none"
            "max.block.ms"            5000
            "delivery.timeout.ms"     30000
            "request.timeout.ms"      10000}
           (config/client-properties brokers))))

  (testing "takes the brokers as a vector"
    (is (= "a:9092,b:9092"
           (get (config/client-properties {:bootstrap-servers ["a:9092"
                                                               "b:9092"]})
                "bootstrap.servers"))))

  (testing "takes what it is told"
    (let [properties (config/client-properties
                      (assoc brokers
                             :client-id           "orders"
                             :linger-ms           0
                             :compression-type    "lz4"
                             :max-block-ms        1000
                             :delivery-timeout-ms 20000
                             :request-timeout-ms  5000))]
      (is (= "orders" (get properties "client.id")))
      (is (= 0 (get properties "linger.ms")))
      (is (= "lz4" (get properties "compression.type")))
      (is (= 1000 (get properties "max.block.ms")))
      (is (= 20000 (get properties "delivery.timeout.ms")))
      (is (= 5000 (get properties "request.timeout.ms")))))

  (testing "lets the properties of the client take the place of the others"
    (let [properties (config/client-properties
                      (assoc brokers
                             :properties {"security.protocol" "SASL_SSL"
                                          "acks"              "1"}))]
      (is (= "SASL_SSL" (get properties "security.protocol")))
      (is (= "1" (get properties "acks"))))))

(deftest invalid-options-test
  (testing "the brokers are host:port, or a vector of them"
    (doseq [bad [nil "" "  " [] [""] [1] :kafka 9092]]
      (is (= :bootstrap-servers (invalid-option {:bootstrap-servers bad}))
          (pr-str bad))))

  (testing "the client id is a non-empty string"
    (is (= :client-id (invalid-option (assoc brokers :client-id ""))))
    (is (= :client-id (invalid-option (assoc brokers :client-id 5)))))

  (testing "the linger is zero or more"
    (is (= :linger-ms (invalid-option (assoc brokers :linger-ms -1))))
    (is (nil? (invalid-option (assoc brokers :linger-ms 0)))))

  (testing "the compression is one that Kafka has"
    (is (= :compression-type
           (invalid-option (assoc brokers :compression-type "zip"))))
    (doseq [compression config/compression-types]
      (is (nil? (invalid-option
                 (assoc brokers :compression-type compression))))))

  (testing "the timeouts are positive integers"
    (doseq [option [:max-block-ms :delivery-timeout-ms :request-timeout-ms]
            bad    [0 -1 1.5 "10"]]
      (is (= option (invalid-option (assoc brokers option bad)))
          (str option " " (pr-str bad)))))

  (testing "a request does not wait longer than a delivery has"
    (is (= :request-timeout-ms
           (invalid-option (assoc brokers
                                  :delivery-timeout-ms 1000
                                  :request-timeout-ms 2000)))))

  (testing "the properties are strings to strings"
    (is (= :properties (invalid-option (assoc brokers :properties [1]))))
    (is (= :properties (invalid-option (assoc brokers :properties {:a "b"}))))
    (is (= :properties (invalid-option (assoc brokers :properties {"a" 1}))))))
