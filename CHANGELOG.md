# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `send!` returns a future of where the record was stored, a map of `:topic`, `:partition`, `:offset` and `:timestamp`, and logs a
  failure that nobody waits for, with the topic and the kind of failure and never the message. It returned the Java future of the
  client, and a failure that was not waited for was lost.
- `error-data`, and `borba.kafka-producer.errors`, which turn a failure of a send into data: `:timeout`, `:record-too-large`,
  `:unknown-topic`, `:not-authorized`, `:authentication-failed`, `:broker-unavailable`, `:serialization-failed` and `:kafka-error`.
- Headers and a partition for a message, `flush!`, and `ready?` for a readiness check.
- `:linger-ms`, `:compression-type`, `:max-block-ms` (5 seconds), `:delivery-timeout-ms` (30 seconds), `:request-timeout-ms`,
  `:close-timeout-ms` and `:properties`, for the security protocol, SASL and TLS. The options are checked when the system starts.
- The start asks the brokers who is in the cluster, and brokers that do not answer fail it with `::cannot-connect` and where they
  were. `:verify-connection? false` turns it off.
- A test suite with an integration suite against a real Kafka, run by the pipeline.

### Changed

- **Breaking:** `send-sync!` returns the metadata as a map, where it returned the `RecordMetadata` of the client, and `send-batch!`
  returns what became of each message, where it returned nothing and lost the failures.
- **Breaking:** the value of the component is the producer and its settings, without the `:schema-registry` that nothing used.
- **Breaking:** a send waits five seconds for the metadata of a topic and a message has thirty seconds to be delivered, where the client
  waits a minute and two minutes. The retries are the client's, bounded by the delivery timeout, where they were set to 3.
- **Breaking:** closing waits `:close-timeout-ms` for the messages that are not sent.
- Moves to the Apache Kafka client 4.3.1 and Integrant 1.0. JSON is written with jsonista, and Cheshire is no longer a dependency.
  The component logs through `tools.logging`, and no longer depends on a logging backend.
- The published library is named `io.github.af2b/borba-kafka-producer-component`.

### Removed

- The Confluent Avro serializer and schema registry client, and the repository they came from. The component sent JSON as text and used
  neither.

### Security

- Pins Jackson to 2.22.3 and `lz4-java` to 1.12.0. The 2.22.2 that jsonista 1.0.1 brings has four high advisories (GHSA-7hhh-6rmp-j9qf,
  GHSA-p6pp-m3f8-5c89, GHSA-cxp5-3px4-pw24 and GHSA-wv8q-qhhj-9h54), and the 1.10.2 that the Kafka client brings has a medium one
  (GHSA-xx22-p4ch-683r), which the dependency scan of the pipeline reported.

## [1.0.0] - 2026-03-29

First release: the `:components/kafka-producer` Integrant component, and `send!`, `send-sync!` and `send-batch!`.

[Unreleased]: https://github.com/AF2B/borba-kafka-producer-component/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/AF2B/borba-kafka-producer-component/releases/tag/v1.0.0
