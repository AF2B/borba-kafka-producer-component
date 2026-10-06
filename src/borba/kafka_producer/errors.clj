(ns borba.kafka-producer.errors
  "The failures of a send as data, in the convention of borba.railway: a map
   with an :error keyword that a caller matches on.

     (try (kafka/send-sync! producer \"orders\" order-id order)
          (catch Exception e
            (errors/error-data e)))
     ;; => {:error :timeout}

   The map has the class of the failure and never its message, which can quote
   a topic, an address or a record."
  (:import
   (java.util.concurrent ExecutionException)
   (org.apache.kafka.common.errors
    AuthenticationException AuthorizationException NetworkException
    RecordTooLargeException SerializationException TimeoutException
    UnknownTopicOrPartitionException)))

(set! *warn-on-reflection* true)

(defn- unwrap
  "Returns the exception that a Future wraps in an ExecutionException."
  [^Throwable e]
  (if (and (instance? ExecutionException e) (ex-cause e))
    (ex-cause e)
    e))

(defn error-data
  "Returns the failure of a Kafka exception as data, or nil when the exception
   is not Kafka's. The :error is :timeout, :record-too-large,
   :unknown-topic, :not-authorized, :authentication-failed,
   :broker-unavailable or :serialization-failed, or :kafka-error for any other
   failure of the client, and the :class of the exception comes with it.
   - e: the exception, or the ExecutionException of a Future that failed"
  [e]
  (let [cause (unwrap e)]
    (when (instance? org.apache.kafka.common.KafkaException cause)
      {:error (condp instance? cause
                TimeoutException                :timeout
                RecordTooLargeException         :record-too-large
                UnknownTopicOrPartitionException :unknown-topic
                AuthorizationException          :not-authorized
                AuthenticationException         :authentication-failed
                NetworkException                :broker-unavailable
                SerializationException          :serialization-failed
                :kafka-error)
       :class (.getName (class cause))})))
