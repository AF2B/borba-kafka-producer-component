(ns borba.kafka-producer
  "Integrant component for Kafka Producer.

   Registers :components/kafka-producer with a KafkaProducer instance
   exposed as :kafka-producer in the component map.

   Provides send! and send-sync! wrappers. Values are serialized as JSON
   strings by default.

   Usage:
     (let [{:keys [kafka-producer]} components]
       (kafka/send! kafka-producer \"order-events\" order-id order-payload))"
  (:require [integrant.core :as ig]
            [cheshire.core :as json])
  (:import (org.apache.kafka.clients.producer
            KafkaProducer ProducerRecord ProducerConfig)
           (org.apache.kafka.common.serialization StringSerializer)))

;; ── Integrant lifecycle ──────────────────────────────────────────────────────

(defmethod ig/init-key :components/kafka-producer
  [_ {:keys [bootstrap-servers schema-registry client-id]}]
  (let [config {ProducerConfig/BOOTSTRAP_SERVERS_CONFIG      bootstrap-servers
                ProducerConfig/CLIENT_ID_CONFIG               (or client-id "borba-producer")
                ProducerConfig/KEY_SERIALIZER_CLASS_CONFIG    (.getName StringSerializer)
                ProducerConfig/VALUE_SERIALIZER_CLASS_CONFIG  (.getName StringSerializer)
                ProducerConfig/ACKS_CONFIG                    "all"
                ProducerConfig/RETRIES_CONFIG                 (int 3)
                ProducerConfig/ENABLE_IDEMPOTENCE_CONFIG      true}
        producer (KafkaProducer. config)]
    (println "📤 [kafka-producer] Started → brokers:" bootstrap-servers)
    {:producer        producer
     :schema-registry schema-registry}))

(defmethod ig/halt-key! :components/kafka-producer
  [_ {:keys [producer]}]
  (when producer
    (.close ^KafkaProducer producer)
    (println "📤 [kafka-producer] Stopped")))

;; ── Public API ───────────────────────────────────────────────────────────────

(defn send!
  "Publishes a message to topic asynchronously.
   Value is serialized to JSON.
   Returns a Future — deref if you need delivery confirmation.

   Example:
     (kafka/send! producer \"order-events\" order-id {:event-type :order.created ...})"
  [{:keys [producer]} topic key value]
  (let [record (ProducerRecord. ^String topic ^String (str key) ^String (json/generate-string value))]
    (.send ^KafkaProducer producer record)))

(defn send-sync!
  "Publishes a message to topic and blocks until the broker confirms delivery.
   Returns RecordMetadata.

   Example:
     (let [meta (kafka/send-sync! producer \"payments\" payment-id payload)]
       (println \"offset:\" (.offset meta)))"
  [{:keys [producer]} topic key value]
  (let [record (ProducerRecord. ^String topic ^String (str key) ^String (json/generate-string value))]
    @(.send ^KafkaProducer producer record)))

(defn send-batch!
  "Publishes a collection of [{:key k :value v}] messages to topic.
   Fires all sends asynchronously, then flushes.

   Example:
     (kafka/send-batch! producer \"order-events\"
       [{:key order-id-1 :value payload-1}
        {:key order-id-2 :value payload-2}])"
  [{:keys [producer]} topic messages]
  (doseq [{:keys [key value]} messages]
    (let [record (ProducerRecord. ^String topic ^String (str key) ^String (json/generate-string value))]
      (.send ^KafkaProducer producer record)))
  (.flush ^KafkaProducer producer))
