// Command consumer reads messages one by one and shuts down gracefully on
// SIGINT/SIGTERM.
//
//	KAFKA_BROKERS=localhost:9092 go run ./examples/consumer
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

type order struct {
	ID     int     `json:"id"`
	Amount float64 `json:"amount"`
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	logger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr}).With().Timestamp().Logger()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	client, err := kafkakit.NewClient(ctx, logger,
		kafkakit.WithBrokers(brokers()),
		kafkakit.WithClientID("example-consumer"),
	)
	if err != nil {
		return err
	}
	defer func() { _ = client.Close() }()

	consumer, err := client.NewConsumer(
		kafkakit.WithConsumerGroupID("example-orders"),
		kafkakit.WithConsumerTopics("orders"),
		kafkakit.WithConsumerInitialOffset(kafkakit.OffsetOldest),
		kafkakit.WithConsumerHandlerTimeout(5*time.Second),
		kafkakit.WithConsumerOnClose(func() { logger.Error().Msg("consumer group died") }),
	)
	if err != nil {
		return err
	}

	consumer.Consume(func(c *kafkakit.ConsumerContext) error {
		var o order
		if err := c.Parse(&o); err != nil {
			c.Mark() // skip poison messages

			return err
		}

		// TraceID is read from the "trace-id" header (kafkakit.DefaultTraceHeader)
		// and is also available as c.Context().Value(kafkakit.TraceKey).
		logger.Info().
			Str("trace_id", c.TraceID()).
			Int32("partition", c.Partition()).
			Int64("offset", c.Offset()).
			Int("id", o.ID).
			Msg("order received")

		c.Mark()

		return nil
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
