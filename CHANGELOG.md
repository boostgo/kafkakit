# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.0.0] - 2026-11-20

First public release.

### Added

- `Client` with connection options: brokers, protocol version, TLS, SASL/PLAIN, timeouts,
  keep-alive, rack ID, client ID (default `kafkakit:<uuid>`), sarama inner logging.
- Sync and async producers behind the `Producer` interface; `CloseProducer`;
  `WithDeliveryCallback`; `*ProduceError` for partial failures of sync batches.
- Producer compression (`CompressionCodec`, `ParseCompression`) with an early protocol-version
  check (`ErrProducerCompressionVersion`).
- Consumer groups: `Consume` (one message at a time) and `ConsumeBatch` (per-partition batches),
  graceful `Close(ctx)`, restart backoff, rebalance-safe handler context,
  `WithConsumerHandlerCancelOnRevoke`, `WithConsumerOnClose`, static membership
  (`WithConsumerGroupInstanceID`).
- Trace ID propagation through a single message header: producers write the trace ID found in
  the context under `TraceKey` (`string` or `uuid.UUID`) into the header, consumers restore it
  into the handler context under `TraceKey` and expose it via `TraceID()`. The header is named
  `trace-id` (`DefaultTraceHeader`) and can be renamed per client with `WithTraceHeader`
  (`Client.TraceHeader`, `ErrClientTraceHeader` for invalid names).
- Shared sarama client for producers and consumers (`WithClientProducerOptions`,
  `WithClientConsumerOptions`, `WithProducerClient`, `WithConsumerClient`,
  `ErrSharedClientConfig`).
- `Coordinator` for consumer-group lag; `Dispatcher` for topic-bound producing
  (`Dispatcher.DispatchMany` errors wrap both `ErrDispatcherDispatch` and
  `ErrDispatcherDispatchMany`).
- Runnable examples (`example_test.go`, `examples/`), integration test gated by `KAFKA_BROKERS`,
  CI with lint, race tests and a Kafka service container.

### Security

- Requires `github.com/klauspost/compress` v1.20.2 (fixes GO-2026-6865, a panic on two-block
  frames reachable through sarama). This version of the dependency needs Go 1.25, which is why
  the module's `go` directive is 1.25.

[Unreleased]: https://github.com/boostgo/kafkakit/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/boostgo/kafkakit/releases/tag/v1.0.0
