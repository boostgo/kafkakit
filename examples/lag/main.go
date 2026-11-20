// Command lag prints the lag of a consumer group on a topic.
//
//	KAFKA_BROKERS=localhost:9092 go run ./examples/lag -group example-orders -topic orders
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/rs/zerolog"

	"github.com/boostgo/kafkakit"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	group := flag.String("group", "example-orders", "consumer group ID")
	topic := flag.String("topic", "orders", "topic name")
	flag.Parse()

	client, err := kafkakit.NewClient(context.Background(), zerolog.Nop(), kafkakit.WithBrokers(brokers()))
	if err != nil {
		return err
	}
	defer func() { _ = client.Close() }()

	coordinator := kafkakit.NewCoordinator(client, *group)
	defer func() { _ = coordinator.Close() }()

	perPartition, err := coordinator.PartitionLag(*topic)
	if err != nil {
		return err
	}

	var total int64
	for partition, lag := range perPartition {
		fmt.Printf("partition %d: %d\n", partition, lag)
		total += lag
	}

	fmt.Printf("total: %d\n", total)

	return nil
}

func brokers() []string {
	if v := os.Getenv("KAFKA_BROKERS"); v != "" {
		return strings.Split(v, ",")
	}

	return []string{"localhost:9092"}
}
