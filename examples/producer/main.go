// Command producer sends a few JSON messages to a topic with a sync producer
// and a Dispatcher.
//
//	KAFKA_BROKERS=localhost:9092 go run ./examples/producer
package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
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

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := kafkakit.NewClient(ctx, logger,
		kafkakit.WithBrokers(brokers()),
		kafkakit.WithClientID("example-producer"),
	)
	if err != nil {
		return err
	}
	defer func() { _ = client.Close() }()

	producer, err := client.NewSyncProducer(
		kafkakit.WithProducerCompression(kafkakit.CompressionZSTD),
	)
	if err != nil {
		return err
	}
	defer func() { _ = kafkakit.CloseProducer(producer) }()

	orders := kafkakit.NewDispatcher(producer, "orders", json.Marshal)

	// A trace ID in the context is written to the "trace-id" message header
	// (kafkakit.DefaultTraceHeader; rename it with kafkakit.WithTraceHeader on
	// both the producing and the consuming client).
	ctx = context.WithValue(ctx, kafkakit.TraceKey, uuid.NewString()) //nolint:staticcheck // shared string key

	for i := 1; i <= 3; i++ {
		if err := orders.Dispatch(ctx, uuid.NewString(), order{ID: i, Amount: float64(i) * 9.99}); err != nil {
			return err
		}

		logger.Info().Int("id", i).Msg("order sent")
	}

	return nil
}

func brokers() []string {
	if v := os.Getenv("KAFKA_BROKERS"); v != "" {
		return strings.Split(v, ",")
	}

	return []string{"localhost:9092"}
}
