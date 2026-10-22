# borba-kafka-producer-component

[![CI](https://github.com/AF2B/borba-kafka-producer-component/actions/workflows/ci.yml/badge.svg)](https://github.com/AF2B/borba-kafka-producer-component/actions/workflows/ci.yml)

A Kafka producer for a Borba service, as an [Integrant](https://github.com/weavejester/integrant) component, and the functions that
send a message as JSON. A message is stored when the brokers have it, a send that cannot be delivered says so and why, every wait is
bounded, and the producer closes cleanly.

## Install

```clojure
io.github.af2b/borba-kafka-producer-component
{:git/url "https://github.com/AF2B/borba-kafka-producer-component"
 :git/tag "v2.1.0"
 :git/sha "<the commit of the tag, printed in the release notes>"}
```

It depends on Clojure, Integrant, `tools.logging`, [jsonista](https://github.com/metosin/jsonista), the Apache Kafka client 4.3.1 and
the JSON Schema serializer of Confluent 8.3.2, which comes from the Maven repository of Confluent, so a project that depends on the
component adds it:

```clojure
:mvn/repos {"confluent" {:url "https://packages.confluent.io/maven/"}}
```

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

## The Schema Registry

Without `:schema-registry` a value is JSON text, and what a consumer gets is whatever the producer wrote. With it, each topic has a
JSON Schema that a [Schema Registry](https://docs.confluent.io/platform/current/schema-registry/) holds, every value is checked against
it before it is sent, and it is written in the wire format of Confluent: a magic byte, the id of the schema, then the JSON.

```clojure
:components/kafka-producer
{:bootstrap-servers #env KAFKA_BROKERS
 :schema-registry   {:url       #env SCHEMA_REGISTRY_URL
                     :schemas   {"order-events"
                                 {:type                 "object"
                                  :properties           {:event-type {:type "string"}
                                                         :data       {:type "object"}}
                                  :required             ["event-type" "data"]
                                  :additionalProperties false}}
                     :register? false}}
```

| Option | What it is | Default |
|---|---|---|
| `:url` | The URL of the registry | required |
| `:schemas` | A map from each topic to its JSON Schema, as data or as text | required |
| `:register?` | Whether the start registers the schemas; otherwise it checks that they are registered | `false` |
| `:basic-auth` | The credentials, `"user:password"` | none |
| `:timeout-ms` | How long a request to the registry waits to connect and to be answered | `5000` |

The schemas are the contract, so they reach the registry on purpose and never by the serializer, which is told not to register. A
service that registers them itself, as a laptop or a test does, sets `:register? true`, and the registry refuses a schema that is not
compatible with the versions before it. Elsewhere the start checks that each schema is registered under the subject of its topic
(`<topic>-value`), so a deploy cannot change a contract by itself. A topic without a schema cannot be written to. The start fails,
naming the topic, for each case:

| `:error` | When |
|---|---|
| `::schema-registry/invalid-schema` | A schema is not a JSON Schema |
| `::schema-registry/schema-not-registered` | A schema is not registered, and `:register?` is false |
| `::schema-registry/incompatible-schema` | The registry refused to register a schema that breaks the versions before it |
| `::schema-registry/cannot-connect` | The registry does not answer (`:address` without the credentials) |

A consumer reads these values with the same registry, as `borba-kafka-consumer-component` does with its own `:schema-registry`; one
that reads JSON text gets a value that is not JSON.

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
| `:schema-violation` | The schema of the topic refuses the value, which was not sent |
| `:schema-not-registered` | The registry does not have the schema of the topic |
| `:schema-registry-unavailable` | The registry does not answer |
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
| `borba.kafka-producer.schema-registry` | The schemas of the topics, checked against the registry, and the envelope a value is sent in |
| `:components/kafka-producer` | The Integrant key that starts and closes the producer |

## Tests

The unit suite runs anywhere; it sends through the `MockProducer` of Kafka and covers the options and the classification of the
failures. The integration suite runs against a real Kafka, which the pipeline provides, and which you can start with Docker:

```bash
docker run --rm -d --name borba-kafka-it -p 127.0.0.1:9092:9092 apache/kafka:4.0.0

KAFKA_BROKERS=127.0.0.1:9092 make test-integration
```

The Schema Registry tests use the mock registry of Confluent, in the unit suite and against the real Kafka of the integration suite.
To run them against a real registry too:

```bash
docker run --rm -d --name borba-registry-it --network host \
  -e SCHEMA_REGISTRY_HOST_NAME=localhost \
  -e SCHEMA_REGISTRY_LISTENERS=http://127.0.0.1:8081 \
  -e SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS=PLAINTEXT://127.0.0.1:9092 \
  confluentinc/cp-schema-registry:8.0.0

KAFKA_BROKERS=127.0.0.1:9092 SCHEMA_REGISTRY_URL=http://127.0.0.1:8081 make test-integration
```

It covers a message stored and read back with its key, value and headers, a tombstone, the order of a key, a record that is too large,
a batch with a failure in the middle, the four compression codecs, a producer that is closed, and brokers that do not answer, at the
start and at a send.

## Design notes

- **A lost message is the failure to design out.** The delivery settings are fixed and strict, and a failure comes back to the caller
  or to the log, never to nowhere.
- **A bounded wait is a decision.** An unbounded one is the same as a decision to hang, made by whoever wrote the default.
- **A contract is registered on purpose.** The serializer never registers a schema, so the version of a contract changes when someone
  registers it, and a deploy that disagrees with it fails at the start instead of writing what nobody can read.
- **JSON Schema, not Avro.** The value stays JSON and the schema is data in the configuration, so nothing is generated and the REPL
  sees the same maps the topic holds.

## Development

```bash
make check      # lint, format, conventions, reflection, tests, coverage
make ci         # everything the pipelines enforce
```

See [CONTRIBUTING.md](CONTRIBUTING.md). The repository follows the [Borba standard](https://github.com/AF2B/borba-tooling/blob/main/docs/standard.md).

## License

[MIT](LICENSE)
