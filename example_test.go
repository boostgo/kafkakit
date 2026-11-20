package kafkakit_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog"

	"github.com/boostgo/kafkakit"
)

// Examples that need a running Kafka broker are compile-only (no // Output:).

// This example shows the full lifecycle: connect, produce, consume and shut
// down gracefully on a signal.
func Example() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	client, err := kafkakit.NewClient(ctx, zerolog.Nop(),
		kafkakit.WithBrokers([]string{"localhost:9092"}),
		kafkakit.WithClientID("orders-service"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	producer, err := client.NewSyncProducer()
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = kafkakit.CloseProducer(producer) }()

	err = producer.Produce(ctx, &kafkakit.ProducerMessage{
		Topic: "orders",
		Key:   "order-42",
		Value: []byte(`{"id":42}`),
	})
	if err != nil {
		log.Printf("produce: %v", err)
	}
}

// ExampleConsumer_Close shows a graceful shutdown on a signal: consumers, then
// producers, then the client.
func ExampleConsumer_Close() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	client, err := kafkakit.NewClient(ctx, zerolog.Nop(), kafkakit.WithBrokers([]string{"localhost:9092"}))
	if err != nil {
		log.Fatal(err)
	}

	producer, err := client.NewAsyncProducer()
	if err != nil {
		log.Fatal(err)
	}

	consumer, err := client.NewConsumer(
		kafkakit.WithConsumerGroupID("example-group"),
		kafkakit.WithConsumerTopics("example-topic"),
	)
	if err != nil {
		log.Fatal(err)
	}

	consumer.Consume(func(c *kafkakit.ConsumerContext) error {
		c.Mark()

		return nil
	})

	<-ctx.Done() // SIGINT / SIGTERM

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 1. Consumer: waits for the loop to exit, final commit, LeaveGroup.
	if err := consumer.Close(shutdownCtx); err != nil {
		log.Printf("consumer close: %v", err)
	}

	// 2. Producer: flushes the buffer and waits for every message outcome.
	if err := kafkakit.CloseProducer(producer); err != nil {
		log.Printf("producer close: %v", err)
	}

	// 3. Client last.
	if err := client.Close(); err != nil {
		log.Printf("client close: %v", err)
	}
}

// ExampleConsumer_ConsumeBatch consumes per-partition batches and commits
// them as a unit.
func ExampleConsumer_ConsumeBatch() {
	ctx := context.Background()

	client, err := kafkakit.NewClient(ctx, zerolog.Nop(), kafkakit.WithBrokers([]string{"localhost:9092"}))
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	consumer, err := client.NewConsumer(
		kafkakit.WithConsumerGroupID("events-loader"),
		kafkakit.WithConsumerTopics("events"),
		kafkakit.WithConsumerHandlerTimeout(10*time.Second),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = consumer.Close(context.Background()) }()

	consumer.ConsumeBatch(func(b *kafkakit.BatchConsumerContext) error {
		for _, msg := range b.Messages() {
			_ = msg.Value() // e.g. append to a bulk insert
		}

		b.MarkAll()

		return nil
	}, kafkakit.ConsumeBatchOptions{
		BatchSize:    500,
		BatchTimeout: 100 * time.Millisecond,
		ErrorHandler: func(first *kafkakit.ConsumerMessage, err error) {
			log.Printf("batch starting at offset %d failed: %v", first.Offset(), err)
		},
	})
}

// ExampleWithDeliveryCallback reports the outcome of every async send.
func ExampleWithDeliveryCallback() {
	ctx := context.Background()

	client, err := kafkakit.NewClient(ctx, zerolog.Nop(), kafkakit.WithBrokers([]string{"localhost:9092"}))
	if err != nil {
		log.Fatal(err)
	}

	defer func() { _ = client.Close() }()

	producer, err := client.NewAsyncProducer(
		// The callback runs concurrently on reader goroutines: do not block in it.
		kafkakit.WithDeliveryCallback(func(msg *kafkakit.ProducerMessage, err error) {
			if err != nil {
				log.Printf("not delivered: topic=%s key=%s: %v", msg.Topic, msg.Key, err)
			}
		}),
	)
	if err != nil {
		log.Fatal(err)
	}

	err = producer.Produce(ctx, &kafkakit.ProducerMessage{Topic: "example-topic", Key: "k", Value: []byte("v")})
	if err != nil {
		log.Printf("produce: %v", err)
	}

	// Close waits for the outcome of every accepted message and its callback.
	if err := kafkakit.CloseProducer(producer); err != nil {
		log.Printf("producer close: %v", err)
	}
}

// ExampleProduceError resends only the messages that failed in a partial
// failure of a sync producer.
func ExampleProduceError() {
	// In real code: err := producer.Produce(ctx, batch...)
	err := fmt.Errorf("%w: %w", kafkakit.ErrProducerSend, &kafkakit.ProduceError{
		Failed: []kafkakit.ProduceFailure{
			{Message: &kafkakit.ProducerMessage{Topic: "orders", Key: "order-2"}, Err: errors.New("timeout")},
		},
	})

	var perr *kafkakit.ProduceError
	if errors.As(err, &perr) {
		for _, f := range perr.Failed {
			fmt.Println("resend", f.Message.Key, "after", f.Err)
		}
	}
	// Output:
	// resend order-2 after timeout
}

// ExampleParseCompression parses a codec name from configuration.
func ExampleParseCompression() {
	codec, err := kafkakit.ParseCompression("zstd")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(codec == kafkakit.CompressionZSTD)

	_, err = kafkakit.ParseCompression("brotli")
	fmt.Println(err != nil)
	// Output:
	// true
	// true
}

// ExampleNewDispatcher binds a producer to a topic and a JSON marshaller.
func ExampleNewDispatcher() {
	ctx := context.Background()

	// A stub producer that prints messages instead of sending them.
	var producer kafkakit.Producer = printProducer{}

	orders := kafkakit.NewDispatcher(producer, "orders", json.Marshal)

	err := orders.Dispatch(ctx, "order-42", map[string]int{"id": 42}, map[string]string{"source": "example"})
	if err != nil {
		log.Fatal(err)
	}
	// Output:
	// orders order-42 {"id":42} example
}

type printProducer struct{}

func (printProducer) Produce(_ context.Context, messages ...*kafkakit.ProducerMessage) error {
	for _, m := range messages {
		fmt.Println(m.Topic, m.Key, string(m.Value), m.Headers["source"])
	}

	return nil
}

// This example renames the trace header. Producers built on the client write
// the trace ID from the context into "x-request-id" instead of the default
// "trace-id"; consumers built on it read the same header back into the handler
// context under TraceKey.
func ExampleWithTraceHeader() {
	ctx := context.Background()

	client, err := kafkakit.NewClient(ctx, zerolog.Nop(),
		kafkakit.WithBrokers([]string{"localhost:9092"}),
		kafkakit.WithTraceHeader("x-request-id"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	producer, err := client.NewSyncProducer()
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = kafkakit.CloseProducer(producer) }()

	// Sent with the header x-request-id: 4bf92f3577b34da6.
	traceCtx := context.WithValue(ctx, kafkakit.TraceKey, "4bf92f3577b34da6") //nolint:staticcheck // shared string key
	if err := producer.Produce(traceCtx, &kafkakit.ProducerMessage{Topic: "orders", Value: []byte(`{}`)}); err != nil {
		log.Fatal(err)
	}

	consumer, err := client.NewConsumer(
		kafkakit.WithConsumerGroupID("orders-service"),
		kafkakit.WithConsumerTopics("orders"),
	)
	if err != nil {
		log.Fatal(err)
	}

	consumer.Consume(func(c *kafkakit.ConsumerContext) error {
		fmt.Println(c.TraceID(), client.TraceHeader()) // 4bf92f3577b34da6 x-request-id
		c.Mark()

		return nil
	})

	_ = consumer.Close(ctx)
}
