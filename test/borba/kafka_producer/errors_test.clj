(ns borba.kafka-producer.errors-test
  (:require
   [borba.kafka-producer.errors :as errors]
   [clojure.test :refer [deftest is testing]])
  (:import
   (java.util.concurrent ExecutionException)
   (org.apache.kafka.common KafkaException)
   (org.apache.kafka.common.errors
    AuthenticationException AuthorizationException NetworkException
    RecordTooLargeException SerializationException TimeoutException
    TopicAuthorizationException UnknownTopicOrPartitionException)))

(deftest classification-test
  (testing "names the failures a program can do something about"
    (doseq [[exception expected]
            [[(TimeoutException. "x") :timeout]
             [(RecordTooLargeException. "x") :record-too-large]
             [(UnknownTopicOrPartitionException. "x") :unknown-topic]
             [(TopicAuthorizationException. "x") :not-authorized]
             [(AuthenticationException. "x") :authentication-failed]
             [(NetworkException. "x") :broker-unavailable]
             [(SerializationException. "x") :serialization-failed]]]
      (is (= expected (:error (errors/error-data exception)))
          (str (class exception)))))

  (testing "calls any other failure of the client a Kafka error"
    (is (= :kafka-error (:error (errors/error-data (KafkaException. "x"))))))

  (testing "says which class it was"
    (is (= {:error :timeout
            :class "org.apache.kafka.common.errors.TimeoutException"}
           (errors/error-data (TimeoutException. "topic orders"))))))

(deftest unwrapping-test
  (testing "reads the failure of a Future, which wraps it"
    (is (= :timeout
           (:error (errors/error-data
                    (ExecutionException. (TimeoutException. "x")))))))

  (testing "is nil for what is not Kafka's"
    (is (nil? (errors/error-data (IllegalStateException. "no"))))
    (is (nil? (errors/error-data
               (ExecutionException. (IllegalStateException. "no")))))))

(deftest message-test
  (testing "never has the message, which can name a topic or a record"
    (is (not-any? #(and (string? %) (re-find #"secret-topic" %))
                  (vals (errors/error-data
                         (TimeoutException. "secret-topic is missing")))))))

(deftest hierarchy-test
  (testing "a authorization failure of any kind is not authorized"
    (is (instance? AuthorizationException (TopicAuthorizationException. "x")))))
