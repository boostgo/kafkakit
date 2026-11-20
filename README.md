# kafkakit

[![Go Reference](https://pkg.go.dev/badge/github.com/boostgo/kafkakit.svg)](https://pkg.go.dev/github.com/boostgo/kafkakit)
[![Go Report Card](https://goreportcard.com/badge/github.com/boostgo/kafkakit)](https://goreportcard.com/report/github.com/boostgo/kafkakit)
[![CI](https://github.com/boostgo/kafkakit/actions/workflows/ci.yml/badge.svg)](https://github.com/boostgo/kafkakit/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

**kafkakit** is a production-minded toolkit on top of [IBM/sarama](https://github.com/IBM/sarama)
for building Kafka producers and consumer groups in Go with sane defaults, functional options,
graceful shutdown and at-least-once semantics you can reason about.

sarama is powerful but low-level: every service ends up re-implementing the consume loop,
restart backoff, batch collection, shutdown ordering, trace propagation and delivery reporting.
kafkakit packages those pieces once, with tests for the tricky parts (rebalances, in-flight
work during shutdown, partial batch failures, concurrent producers).

## Features

- **One client, many roles** — `Client` owns the connection settings (TLS, SASL/PLAIN, protocol
  version, timeouts); producers and consumers inherit them.
- **Sync and async producers** behind one `Producer` interface, safe for concurrent use.
- **Delivery callbacks** for every message, and **partial-failure reporting** (`*ProduceError`)
  so you resend only what failed.
- **Graceful shutdown** — `Consumer.Close(ctx)` waits for in-flight handlers, commits and leaves
  the group; `CloseProducer` flushes and reports undelivered messages.
- **Single-message and batch consumers** — `Consume` and `ConsumeBatch` (per-partition batches by
  size or timeout, committed as a unit).
- **Rebalance-safe handler context** — by default a rebalance does not cancel the handler's
  context, so work in flight completes and offsets match the work done.
- **Automatic restart with exponential backoff** (500ms → 30s) after consume-loop errors.
- **Trace propagation** — a trace ID in the context is written to a single message header
  (`trace-id` by default, configurable with `WithTraceHeader`) and restored into the consumer
  handler context under the shared `"trace_id"` key.
- **JSON parsing + validation** of payloads via `go-playground/validator`.
- **Compression** (gzip, snappy, lz4, zstd) with an early, actionable protocol-version check.
- **Shared sarama client** for many producers/consumers with config-drift detection.
- **Consumer lag** (`Coordinator`) per partition and in total.
- **Dispatcher** — a topic-bound producer with a pluggable marshaller.

## Installation

```sh
go get github.com/boostgo/kafkakit
```

Requires Go 1.25+.

## Quick start

```go
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog"

	"github.com/boostgo/kafkakit"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger := zerolog.New(os.Stderr)

	client, err := kafkakit.NewClient(ctx, logger,
		kafkakit.WithBrokers([]string{"localhost:9092"}),
		kafkakit.WithClientID("orders-service"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// Produce
	producer, err := client.NewSyncProducer()
	if err != nil {
		log.Fatal(err)
	}
	defer kafkakit.CloseProducer(producer)

	_ = producer.Produce(ctx, &kafkakit.ProducerMessage{
		Topic: "orders",
		Key:   "order-42",
		Value: []byte(`{"id":42}`),
	})

	// Consume
	consumer, err := client.NewConsumer(
		kafkakit.WithConsumerGroupID("orders-processor"),
		kafkakit.WithConsumerTopics("orders"),
	)
	if err != nil {
		log.Fatal(err)
	}

	consumer.Consume(func(c *kafkakit.ConsumerContext) error {
		log.Printf("got %s = %s", c.Key(), c.Value())
		c.Mark()
		return nil
	})

	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_ = consumer.Close(shutdownCtx)
}
```

Runnable programs live in [`examples/`](examples): `producer`, `consumer`, `batch-consumer`, `lag`.

## Usage

### Producing

```go
// Sync: returns after the brokers acknowledged the batch.
producer, err := client.NewSyncProducer(
	kafkakit.WithProducerRequiredAcks(kafkakit.WaitForAll),
	kafkakit.WithProducerIdempotent(true),
)

// Async: returns once messages are buffered; errors are logged.
producer, err := client.NewAsyncProducer(
	kafkakit.WithProducerFlushFrequency(10*time.Millisecond),
)

err = producer.Produce(ctx, msg1, msg2, msg3)

// Always close: async flushes and reports undelivered messages.
err = kafkakit.CloseProducer(producer)
```

`Produce` never mutates the messages, so a message or a header map may be shared between
goroutines.

### Delivery callbacks and partial failures

```go
producer, _ := client.NewAsyncProducer(
	kafkakit.WithDeliveryCallback(func(msg *kafkakit.ProducerMessage, err error) {
		if err != nil {
			log.Printf("not delivered: %s/%s: %v", msg.Topic, msg.Key, err)
		}
	}),
)
```

A sync producer reports partially failed batches with `*ProduceError`; messages not listed in it
were delivered:

```go
if err := producer.Produce(ctx, batch...); err != nil {
	var perr *kafkakit.ProduceError
	if errors.As(err, &perr) {
		for _, f := range perr.Failed {
			retry(f.Message) // only the failed ones
		}
	}
}
```

### Consuming one message at a time

```go
consumer, err := client.NewConsumer(
	kafkakit.WithConsumerGroupID("orders-processor"),
	kafkakit.WithConsumerTopics("orders"),
	kafkakit.WithConsumerInitialOffset(kafkakit.OffsetOldest),
	kafkakit.WithConsumerHandlerTimeout(5*time.Second),
	kafkakit.WithConsumerValidate(validator.New()),
)

consumer.Consume(func(c *kafkakit.ConsumerContext) error {
	var o Order
	if err := c.Parse(&o); err != nil { // JSON + validation
		c.Mark() // skip poison messages
		return err
	}
	// c.Context() carries the trace ID; c.TraceID(), c.Header("x"), c.Partition(), c.Offset() ...
	c.Mark()
	return nil
})
```

`Consume` is non-blocking. Errors returned by the handler are logged; offset marking is up to you
(`Mark` + auto-commit, or `Commit` for an immediate commit).

### Consuming in batches

```go
consumer.ConsumeBatch(func(b *kafkakit.BatchConsumerContext) error {
	rows := make([]Row, 0, b.Len())
	for _, m := range b.Messages() {
		var r Row
		if err := m.Parse(&r); err != nil {
			return err
		}
		rows = append(rows, r)
	}
	if err := db.BulkInsert(b.Context(), rows); err != nil {
		return err // not marked: redelivered after restart/rebalance
	}
	b.MarkAll()
	return nil
}, kafkakit.ConsumeBatchOptions{
	BatchSize:    500,                    // default 200
	BatchTimeout: 100 * time.Millisecond, // default 50ms
	ErrorHandler: func(first *kafkakit.ConsumerMessage, err error) { /* metrics */ },
})
```

Batches are collected per partition and flushed when full or when the timeout fires.

### Graceful shutdown and rebalances

Shut down in this order: consumers → producers → client.

```go
_ = consumer.Close(shutdownCtx)          // waits for in-flight handlers, commits, leaves the group
_ = kafkakit.CloseProducer(producer)     // flushes, reports undelivered messages
_ = client.Close()
```

By default the handler context is **not** canceled by a rebalance or by `Close`: work in flight
completes and its offset is marked consistently. A batch tail that has not reached the handler
when the session ends is neither processed nor marked and is redelivered (at-least-once). Bound
in-flight work with `WithConsumerHandlerTimeout` and the `Close` context. To get the old
"cancel on revoke" semantics use `WithConsumerHandlerCancelOnRevoke(true)`.

`WithConsumerOnClose` lets you react when the group dies unexpectedly (out of brokers, closed
client), e.g. to fail a readiness probe.

### Trace propagation

The trace ID travels in exactly one message header, `trace-id` (`kafkakit.DefaultTraceHeader`)
unless the client is created with `WithTraceHeader`:

```go
ctx = context.WithValue(ctx, kafkakit.TraceKey, traceID) // "trace_id"; string or uuid.UUID
_ = producer.Produce(ctx, msg)                            // adds the "trace-id" header

consumer.Consume(func(c *kafkakit.ConsumerContext) error {
	traceID := c.TraceID()                    // "" if the message has no trace header
	_ = c.Context().Value(kafkakit.TraceKey) // the same value, for code that only sees ctx
	_ = traceID
	return nil
})
```

- **Producer**: a `string` or `uuid.UUID` found in the context under `TraceKey` is written to the
  trace header of every message, replacing a header of the same name set in
  `ProducerMessage.Headers`. Without a trace ID in the context, message headers are sent as is.
- **Consumer**: the value of the trace header is stored in the handler context under `TraceKey`
  and returned by `ConsumerContext.TraceID()` / `ConsumerMessage.TraceID()`. A missing or empty
  header leaves the context untouched.

The header name is a client setting, so every producer and consumer built on the client agrees
on it. Use the same name on both sides of a topic:

```go
client, err := kafkakit.NewClient(ctx, logger,
	kafkakit.WithBrokers(brokers),
	kafkakit.WithTraceHeader("x-request-id"), // "" keeps the default "trace-id"
)
fmt.Println(client.TraceHeader()) // x-request-id
```

A name with whitespace, control characters or invalid UTF-8 makes `NewClient` return
`ErrClientTraceHeader`.

The same `"trace_id"` key is used by [httpkit](https://github.com/boostgo/httpkit) and
[grpckit](https://github.com/boostgo/grpckit), so logs line up across HTTP, gRPC and Kafka.

### Compression

```go
codec, err := kafkakit.ParseCompression(cfg.Compression) // "none" | "gzip" | "snappy" | "lz4" | "zstd"

producer, err := client.NewAsyncProducer(
	kafkakit.WithProducerCompression(codec),
	kafkakit.WithProducerFlushFrequency(20*time.Millisecond), // batches must be large enough to compress well
)
```

An incompatible codec/protocol version pair fails fast with `ErrProducerCompressionVersion`,
naming the minimum version to pass to `WithVersion`.

### Shared client

By default every producer and consumer opens its own connections. To share one sarama client,
declare the producer/consumer sections on the client and opt in:

```go
client, _ := kafkakit.NewClient(ctx, logger,
	kafkakit.WithBrokers(brokers),
	kafkakit.WithClientProducerOptions(kafkakit.WithProducerCompression(kafkakit.CompressionZSTD)),
	kafkakit.WithClientConsumerOptions(kafkakit.WithConsumerInitialOffset(kafkakit.OffsetOldest)),
)

producer, _ := client.NewSyncProducer(
	kafkakit.WithProducerClient(client),
	kafkakit.WithProducerCompression(kafkakit.CompressionZSTD), // must match the client section
)
```

Options that differ from the client section return `ErrSharedClientConfig` instead of being
silently ignored.

### Consumer lag

```go
coordinator := kafkakit.NewCoordinator(client, "orders-processor")
total, err := coordinator.TotalLag("orders")
perPartition, err := coordinator.PartitionLag("orders")
```

### Dispatcher

```go
orders := kafkakit.NewDispatcher(producer, "orders", json.Marshal)
err := orders.Dispatch(ctx, order.ID, order, map[string]string{"source": "checkout"})
```

## Options reference

### Client (`NewClient`)

| Option | Description | Default |
|---|---|---|
| `WithBrokers([]string)` | Bootstrap brokers (required) | — |
| `WithClientID(string)` | Client ID; `:<uuid>` is appended | `kafkakit:<uuid>` |
| `WithVersion(string)` | Kafka protocol version, e.g. `"3.6.0"` | sarama default (2.1.0) |
| `WithTLS(bool, *tls.Config)` | TLS for broker connections | off |
| `WithAuthentication(user, pass)` | SASL/PLAIN | off |
| `WithDialTimeout` / `WithReadTimeout` / `WithWriteTimeout` | Network timeouts | 30s |
| `WithKeepAlive(time.Duration)` | TCP keep-alive (negative disables) | 0 |
| `WithMaxOpenRequests(int)` | In-flight requests per connection | 5 |
| `WithRackID(string)` | Client rack (matches `broker.rack`) | — |
| `WithChannelBufferSize(int)` | sarama channel buffers | 256 |
| `WithInnerLogging(bool)` | Route sarama logs to the logger (debug) | off |
| `WithTraceHeader(string)` | Message header that carries the trace ID (`""` = default) | `trace-id` |
| `WithClientProducerOptions(...)` | Producer section for a shared client | — |
| `WithClientConsumerOptions(...)` | Consumer section for a shared client | — |

### Producer (`NewSyncProducer`, `NewAsyncProducer`)

| Option | Description | Default |
|---|---|---|
| `WithProducerRequiredAcks(int16)` | `NoResponse`, `WaitForLocal`, `WaitForAll` | `WaitForAll` |
| `WithProducerIdempotent(bool)` | Idempotent producer | off |
| `WithProducerTimeout(time.Duration)` | Broker ack timeout | 10s |
| `WithProducerMaxMessageBytes(int)` | Max (uncompressed) batch size per partition | 1000000 |
| `WithProducerCompression(CompressionCodec)` | Batch compression codec | `CompressionNone` |
| `WithProducerCompressionLevel(int)` | Codec level | `CompressionLevelDefault` |
| `WithProducerFlushBytes` / `FlushMessages` / `FlushFrequency` / `FlushMax` | Batching (linger) | sarama defaults |
| `WithProducerRetryMax(int)` | Send retries | 3 |
| `WithProducerRetryBackoff(time.Duration)` / `WithProducerRetryBackoffFunc` | Retry backoff | 100ms |
| `WithProducerRetryMaxBufferLength(int)` | Retry buffer length cap (≤0 = unlimited) | 4096 |
| `WithProducerRetryMaxBufferBytes(int64)` | Retry buffer size cap (≤0 = unlimited) | 32 MiB |
| `WithProducerReturnSuccesses(bool)` | Return successes (forced on for sync) | off |
| `WithProducerReturnErrors(bool)` | Return errors | on |
| `WithDeliveryCallback(fn)` | Per-message delivery outcome | — |
| `WithProducerClient(*Client)` | Use the shared client | own connection |

### Consumer (`NewConsumer`)

| Option | Description | Default |
|---|---|---|
| `WithConsumerGroupID(string)` | Group ID (required) | — |
| `WithConsumerTopics(...string)` | Topics (required) | — |
| `WithConsumerName(string)` | Name in logs; `:<uuid>` is appended | `kafka.consumer:<uuid>` |
| `WithConsumerGroupInstanceID(string)` | Static membership (KIP-345, needs version ≥ 2.3.0) | dynamic |
| `WithConsumerInitialOffset(int64)` | `OffsetNewest` / `OffsetOldest` when nothing is committed | `OffsetNewest` |
| `WithConsumerResetInvalidOffsets(bool)` | Jump to the initial offset on "out of range" (logged) | on |
| `WithConsumerHandlerTimeout(time.Duration)` | Per message/batch handler timeout | none |
| `WithConsumerHandlerCancelOnRevoke(bool)` | Cancel handler ctx on rebalance/Close (legacy) | off |
| `WithConsumerOnClose(func())` | Called once when the group dies unexpectedly | — |
| `WithConsumerValidate(*validator.Validate)` | Validate structs in `Parse` | — |
| `WithConsumerSessionTimeout(time.Duration)` | Group session timeout | 30s |
| `WithConsumerHeartbeatInterval(time.Duration)` | Heartbeat interval | 10s |
| `WithConsumerBalanceStrategies(...)` | `Range`, `RoundRobin`, `Sticky` | `Range` |
| `WithConsumerBalanceTimeout` / `MaxRetries` / `RetryBackoff` | Rebalance tuning | 60s / 3 / 2s |
| `WithConsumerAutocommitEnabled(bool)` / `WithConsumerAutocommitInterval` | Offset auto-commit | on / 1s |
| `WithConsumerFetchMin` / `FetchDefault` / `FetchMax` | Fetch sizes | 1 B / 1 MiB / 10 MiB |
| `WithConsumerMaxWaitTime` / `WithConsumerMaxProcessingTime` | Fetch wait / processing time | 3s / 3s |
| `WithConsumerRetryBackoff` / `WithConsumerRetryBackoffFunc` | Partition read retry backoff | 2s |
| `WithConsumerRetryMax(int)` | Commit retries on shutdown | 3 |
| `WithConsumerIsolationLevel(int8)` | `ReadUncommitted` / `ReadCommitted` | `ReadUncommitted` |
| `WithConsumerReturnErrors(bool)` | Deliver errors to the listener | on |
| `WithConsumerClient(*Client)` | Use the shared client | own connection |

## Errors

All errors wrap sentinels, check them with `errors.Is`: `ErrConnect`, `ErrClientVersion`,
`ErrClientTraceHeader`, `ErrProducerNew`, `ErrProducerSend`, `ErrProducerClosed`, `ErrProducerCompressionVersion`,
`ErrSharedClientConfig`, `ErrConsumerNew`, `ErrConsumerGroupInstanceIDVersion`,
`ErrDispatcherDispatch`, `ErrDispatcherDispatchMany`, `ErrExportIsNotPointer`.

## Testing

```sh
make test               # unit tests with -race, no Kafka needed
make test-integration   # also runs integration tests against KAFKA_BROKERS (default localhost:9092)
make lint
make cover
```

Integration tests are skipped unless `KAFKA_BROKERS` is set. A single-node broker:

```sh
docker run -d --name kafka -p 9092:9092 apache/kafka:3.8.0
```

## Contributing

Issues and pull requests are welcome. Please:

1. Open an issue first for larger changes.
2. Keep the public API backwards compatible where possible and add a `CHANGELOG.md` entry.
3. Add tests; run `make vet lint test` before pushing.

## License

[MIT](LICENSE) © 2026 boostgo
