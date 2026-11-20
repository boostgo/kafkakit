// Command batch-consumer reads per-partition batches (up to 500 messages or
// 200ms) and commits each batch as a unit.
//
//	KAFKA_BROKERS=localhost:9092 go run ./examples/batch-consumer
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/rs/zerolog"

	"github.com/boostgo/kafkakit"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	logger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr}).With().Timestamp().Logger()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	client, err := kafkakit.NewClient(ctx, logger, kafkakit.WithBrokers(brokers()))
	if err != nil {
		return err
	}
	defer func() { _ = client.Close() }()

	consumer, err := client.NewConsumer(
		kafkakit.WithConsumerGroupID("example-orders-batch"),
		kafkakit.WithConsumerTopics("orders"),
		kafkakit.WithConsumerInitialOffset(kafkakit.OffsetOldest),
		kafkakit.WithConsumerHandlerTimeout(30*time.Second),
	)
	if err != nil {
		return err
	}

	consumer.ConsumeBatch(func(b *kafkakit.BatchConsumerContext) error {
		// Process the batch as a whole, e.g. a single bulk insert.
		logger.Info().
			Str("topic", b.Topic()).
			Int32("partition", b.Partition()).
			Int("size", b.Len()).
			Int64("last_offset", b.LastOffset()).
			Msg("batch received")

		b.MarkAll()

		return nil
	}, kafkakit.ConsumeBatchOptions{
		BatchSize:    500,
		BatchTimeout: 200 * time.Millisecond,
		ErrorHandler: func(first *kafkakit.ConsumerMessage, err error) {
			logger.Error().Err(err).Int64("first_offset", first.Offset()).Msg("batch failed")
		},
	})

	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	return consumer.Close(shutdownCtx)
}

func brokers() []string {
	if v := os.Getenv("KAFKA_BROKERS"); v != "" {
		return strings.Split(v, ",")
	}

	return []string{"localhost:9092"}
}
