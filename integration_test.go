package kafkakit

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

// integrationBrokers returns the brokers from KAFKA_BROKERS (comma-separated)
// or skips the test when the variable is not set.
func integrationBrokers(t *testing.T) []string {
	t.Helper()

	raw := os.Getenv("KAFKA_BROKERS")
	if raw == "" {
		t.Skip("KAFKA_BROKERS is not set; skipping integration test")
	}

	return strings.Split(raw, ",")
}

func TestIntegration_ProduceConsume(t *testing.T) {
	brokers := integrationBrokers(t)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	topic := "kafkakit-it-" + uuid.NewString()
	traceID := uuid.NewString()

	client, err := NewClient(ctx, zerolog.Nop(), WithBrokers(brokers), WithClientID("kafkakit-it"))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer func() { _ = client.Close() }()

	// Generous retries: the topic is auto-created on first use and its leader
	// may not be elected yet.
	producer, err := client.NewSyncProducer(
		WithProducerRetryMax(10),
		WithProducerRetryBackoff(500*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("NewSyncProducer: %v", err)
	}

	// The first produce auto-creates the topic (default broker setting).
	produceCtx := context.WithValue(ctx, TraceKey, traceID) //nolint:staticcheck // TraceKey is a public string contract
	if err := producer.Produce(produceCtx, &ProducerMessage{
		Topic:   topic,
		Key:     "k1",
		Value:   []byte(`{"id":1}`),
		Headers: map[string]string{"source": "it"},
	}); err != nil {
		t.Fatalf("Produce: %v", err)
	}

	if err := CloseProducer(producer); err != nil {
		t.Fatalf("CloseProducer: %v", err)
	}

	consumer, err := client.NewConsumer(
		WithConsumerGroupID("kafkakit-it-"+uuid.NewString()),
		WithConsumerTopics(topic),
		WithConsumerInitialOffset(OffsetOldest),
	)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}

	type got struct{ key, value, header, trace string }

	received := make(chan got, 1)

	consumer.Consume(func(c *ConsumerContext) error {
		c.Mark()

		select {
		case received <- got{string(c.Key()), string(c.Value()), c.Header("source"), c.TraceID()}:
		default:
		}

		return nil
	})

	select {
	case g := <-received:
		if g.key != "k1" || g.value != `{"id":1}` || g.header != "it" || g.trace != traceID {
			t.Fatalf("unexpected message: %+v", g)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for the message")
	}

	closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer closeCancel()

	if err := consumer.Close(closeCtx); err != nil {
		t.Fatalf("Consumer.Close: %v", err)
	}
}
