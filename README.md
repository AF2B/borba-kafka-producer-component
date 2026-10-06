# borba-kafka-producer-component

[![CI](https://github.com/AF2B/borba-kafka-producer-component/actions/workflows/ci.yml/badge.svg)](https://github.com/AF2B/borba-kafka-producer-component/actions/workflows/ci.yml)

A Kafka producer for a Borba service, as an [Integrant](https://github.com/weavejester/integrant) component, and the functions that
send a message as JSON. A message is stored when the brokers have it, a send that cannot be delivered says so and why, every wait is
bounded, and the producer closes cleanly.

## Install

```clojure
io.github.af2b/borba-kafka-producer-component
{:git/url "https://github.com/AF2B/borba-kafka-producer-component"
 :git/tag "v2.0.0"
 :git/sha "<the commit of the tag, printed in the release notes>"}
```

It depends on Clojure, Integrant, `tools.logging`, [jsonista](https://github.com/metosin/jsonista) and the Apache Kafka client 4.3.1.

## Use

```clojure
{:service/namespaces [borba.kafka-producer]

 :ig/system
 {:components/kafka-producer
  {:bootstrap-servers #or [#env KAFKA_BROKERS "localhost:9092"]
   :client-id         "orders-service"}}}
```

The value of the component is the producer that every function takes first.

```clojure
(require '[borba.kafka-producer :as kafka])

(kafka/send-sync! producer "orders" order-id {:total 10 :paid? false}
                  {:headers {"request-id" "abc"}})
;; => {:topic "orders", :partition 0, :offset 41, :timestamp 1790000000000}

@(kafka/send! producer "orders" order-id {:total 10})
;; => {:topic "orders", :partition 0, :offset 42, :timestamp 1790000000005}

(kafka/send-batch! producer "orders" [{:key "a" :value {:n 1}}
                                      {:key "b" :value {:n 2}}])
;; => [{:topic "orders", :partition 0, :offset 43, ...}
;;     {:topic "orders", :partition 0, :offset 44, ...}]
```

- **The key** is the text of what is given, so a UUID or a keyword is a key. Messages with the same key go to the same partition, in
  the order they were sent in. `nil` is no key.
- **The value** is its JSON. `nil` is a tombstone, which is how a compacted topic forgets a key.
- **The headers** are a map of names to values, which are sent as text. `:partition` chooses a partition.
- `send!` is asynchronous and returns a future of where the record was stored. `send-sync!` waits for it. `send-batch!` sends
  all the messages, waits for them and returns what became of each, in order. `flush!` sends what is waiting.

| Option | What it is | Default |
|---|---|---|
| `:bootstrap-servers` | The brokers, `"host:port,host:port"` or a vector of `"host:port"` | required |
| `:client-id` | The name of the producer for the brokers | `"borba-producer"` |
| `:linger-ms` | How long a message waits to be sent with others | `5` |
| `:compression-type` | `none`, `gzip`, `snappy`, `lz4` or `zstd` | `"none"` |
| `:max-block-ms` | How long a send waits for the metadata of a topic or for room | `5000` |
| `:delivery-timeout-ms` | How long a message has to be delivered, retries included | `30000` |
| `:request-timeout-ms` | How long a request waits for a broker, up to the delivery timeout | `10000` |
| `:close-timeout-ms` | How long closing waits for the messages that are not sent yet | `10000` |
| `:verify-connection?` | Whether the start asks the brokers who is in the cluster | `true` |
| `:verify-timeout-ms` | How long that takes at most | `5000` |
| `:properties` | More properties of the client, strings to strings: the security protocol, SASL, TLS | none |

`:properties` takes the place of what the options above set, which is how a service reaches a cluster that asks for credentials:

```clojure
:properties {"security.protocol" "SASL_SSL"
             "sasl.mechanism"    "PLAIN"
             "sasl.jaas.config"  #env KAFKA_JAAS_CONFIG}
```

## What a delivery means

The producer is written with `acks=all` and idempotence: a message is stored once the in-sync replicas have it, and a retry is not a
duplicate. That is not a choice to make per call, because the cost of getting it wrong is a message that was lost or twice over.

A send that cannot be delivered says so. `deref` of the future, and `send-sync!`, throw, and `error-data` says why. A failure that
nobody waits for is not lost: it is logged, with the topic and the kind of failure, never the message.

```clojure
(try (kafka/send-sync! producer "orders" order-id order)
     (catch Exception e
       (kafka/error-data e)))
;; => {:error :record-too-large, :class "org.apache.kafka.common.errors.RecordTooLargeException"}
```

| `:error` | When |
|---|---|
| `:timeout` | The message was not delivered in time, or the metadata of the topic did not arrive |
| `:record-too-large` | The message is larger than the brokers take |
| `:unknown-topic` | The topic does not exist and cannot be made |
| `:not-authorized` | The producer may not write to the topic |
| `:authentication-failed` | The credentials are not accepted |
| `:broker-unavailable` | The brokers cannot be reached |
| `:serialization-failed` | The message could not be written |
| `:kafka-error` | Any other failure of the client |

The map has the class of the failure and never its message, which can quote a topic, an address or a record. `error-data` is nil for
an exception that is not Kafka's. `send-batch!` puts the same map in the place of a message that failed, and the others are still
sent.

## Limits

Every wait has one. By default the client waits a minute for the metadata of a topic, which blocks a request of the service for a
minute when the brokers are away, and two minutes to deliver a message. Here a send waits five seconds for the metadata and a message
has thirty to be delivered:

```clojure
;; brokers that do not answer, with :verify-connection? false and :max-block-ms 500
(kafka/send-sync! producer "orders" "k" {:a 1})
;; throws a Kafka TimeoutException once the 0.5 seconds are over; (kafka/error-data e) => {:error :timeout, ...}
```

**Brokers that do not answer fail the start.** The component asks the cluster who is in it, and when no one answers it fails, naming
the brokers and nothing else:

```clojure
(ig/init {:components/kafka-producer {:bootstrap-servers "127.0.0.1:1"}})
;; throws the ExceptionInfo of Integrant, whose cause (ex-cause) is
;;   "the Kafka brokers do not answer on 127.0.0.1:1"
;;   {:error :borba.kafka-producer/cannot-connect, :brokers "127.0.0.1:1"}
```

A service that should start without Kafka, and send when it is back, sets `:verify-connection? false`.

`ready?` is for a readiness check: true when the brokers answer for a topic, false when they do not. It never throws.

## Closing

On a halt the producer is closed with `:close-timeout-ms`: the messages that are waiting are sent first, and what is not sent in time
is dropped. A producer that is closed sends no more, and `send!` throws.

## API

| Name | What it does |
|---|---|
| `send!` | Sends a message and returns a future of where it was stored |
| `send-sync!` | Sends a message and waits |
| `send-batch!` | Sends many messages, waits, and says what became of each |
| `flush!` | Sends what is waiting |
| `ready?` | Whether the brokers answer for a topic |
| `error-data` | The failure of a Kafka exception, as data |
| `borba.kafka-producer.config` | The checked options, turned into the properties of the client |
| `borba.kafka-producer.errors` | The classification of the failures |
| `:components/kafka-producer` | The Integrant key that starts and closes the producer |

## Tests

The unit suite runs anywhere; it sends through the `MockProducer` of Kafka and covers the options and the classification of the
failures. The integration suite runs against a real Kafka, which the pipeline provides, and which you can start with Docker:

```bash
docker run --rm -d --name borba-kafka-it -p 127.0.0.1:9092:9092 apache/kafka:4.0.0

KAFKA_BROKERS=127.0.0.1:9092 make test-integration
```

It covers a message stored and read back with its key, value and headers, a tombstone, the order of a key, a record that is too large,
a batch with a failure in the middle, the four compression codecs, a producer that is closed, and brokers that do not answer, at the
start and at a send.

## Design notes

- **A lost message is the failure to design out.** The delivery settings are fixed and strict, and a failure comes back to the caller
  or to the log, never to nowhere.
- **A bounded wait is a decision.** An unbounded one is the same as a decision to hang, made by whoever wrote the default.
- **The Confluent serializers are gone.** The component sends JSON as text, and the Avro serializer, the schema registry client and the
  repository they came from were not used.

## Development

```bash
make check      # lint, format, conventions, reflection, tests, coverage
make ci         # everything the pipelines enforce
```

See [CONTRIBUTING.md](CONTRIBUTING.md). The repository follows the [Borba standard](https://github.com/AF2B/borba-tooling/blob/main/docs/standard.md).

## License

[MIT](LICENSE)
