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
   (io.confluent.kafka.schemaregistry.client.rest.exceptions
    RestClientException)
   (java.io IOException)
   (java.util.concurrent ExecutionException)
   (org.apache.kafka.common.errors
    AuthenticationException AuthorizationException NetworkException
    RecordTooLargeException SerializationException TimeoutException
    UnknownTopicOrPartitionException)
   (org.everit.json.schema ValidationException)))

(set! *warn-on-reflection* true)

(defn- unwrap
  "Returns the exception that a Future wraps in an ExecutionException."
  [^Throwable e]
  (if (and (instance? ExecutionException e) (ex-cause e))
    (ex-cause e)
    e))

(def ^:private not-found-codes
  "The error codes of the registry for a subject, a version or a schema that it
   does not have."
  #{40401 40402 40403})

(defn- causes
  "Returns an exception and the chain of its causes."
  [^Throwable e]
  (take-while some? (iterate #(.getCause ^Throwable %) e)))

(defn- not-found?
  [cause]
  (and (instance? RestClientException cause)
       (contains? not-found-codes
                  (.getErrorCode ^RestClientException cause))))

(defn- serialization-error
  "Returns what a failure to serialize a value was: a value that its schema
   refuses, a schema the registry does not have, a registry that does not
   answer, or another failure of the serializer."
  [cause]
  (let [chain (causes cause)]
    (cond
      (some #(instance? ValidationException %) chain)
      :schema-violation

      (some not-found? chain)
      :schema-not-registered

      (some #(instance? IOException %) chain)
      :schema-registry-unavailable

      :else
      :serialization-failed)))

(defn error-data
  "Returns the failure of a Kafka exception as data, or nil when the exception
   is not Kafka's. The :error is :timeout, :record-too-large,
   :unknown-topic, :not-authorized, :authentication-failed,
   :broker-unavailable or :serialization-failed, or :kafka-error for any other
   failure of the client, and the :class of the exception comes with it. With
   a Schema Registry, a value that fails to serialize is :schema-violation
   when its schema refuses it, :schema-not-registered when the registry does
   not have the schema, and :schema-registry-unavailable when the registry
   does not answer.
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
                SerializationException          (serialization-error cause)
                :kafka-error)
       :class (.getName (class cause))})))
