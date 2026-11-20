package kafkakit

import (
	"errors"
	"fmt"
	"sync"

	"github.com/IBM/sarama"
)

// Coordinator computes consumer group lag using the group coordinator broker.
//
// It is safe for concurrent use: the cached coordinator is guarded by a mutex.
// The coordinator broker belongs to the sarama client ([Client]); Coordinator
// never closes it.
type Coordinator struct {
	client  *Client
	groupID string

	mu          sync.Mutex
	coordinator *sarama.Broker
}

// NewCoordinator creates a lag calculator for groupID. It does not perform
// any network calls; the coordinator is looked up lazily.
func NewCoordinator(
	client *Client,
	groupID string,
) *Coordinator {
	return &Coordinator{
		client:  client,
		groupID: groupID,
	}
}

// Init looks up the group coordinator and caches it. Calling it is optional:
// TotalLag and PartitionLag do it themselves.
func (c *Coordinator) Init() error {
	_, err := c.broker()

	return err
}

func (c *Coordinator) broker() (*sarama.Broker, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.coordinator != nil {
		return c.coordinator, nil
	}

	broker, err := c.client.client.Coordinator(c.groupID)
	if err != nil {
		return nil, fmt.Errorf("kafka.coordinator.init: %w", err)
	}

	c.coordinator = broker

	return broker, nil
}

// resetCoordinator drops the cache and asks the sarama client to refresh the
// coordinator, so the next call picks the new one. The broker is not closed:
// the client owns it. stale is the broker the error happened on; if another
// goroutine has already updated the cache, it is left untouched.
func (c *Coordinator) resetCoordinator(stale *sarama.Broker) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.coordinator != stale {
		return
	}

	c.coordinator = nil
	_ = c.client.client.RefreshCoordinator(c.groupID)
}

// TotalLag returns the group's total lag across all partitions of topic. On
// any error it returns 0 and the error (see PartitionLag).
func (c *Coordinator) TotalLag(topic string) (int64, error) {
	if err := c.Init(); err != nil {
		return 0, fmt.Errorf("kafka.coordinator.total_lag.init: %w", err)
	}

	lagByPartition, err := c.PartitionLag(topic)
	if err != nil {
		return 0, fmt.Errorf("kafka.coordinator.total_lag.partition_lag: %w", err)
	}

	var totalLag int64
	for _, partitionLag := range lagByPartition {
		totalLag += partitionLag
	}

	return totalLag, nil
}

// isCoordinatorError reports whether err means the cached coordinator is stale.
func isCoordinatorError(err error) bool {
	return errors.Is(err, sarama.ErrNotCoordinatorForConsumer) ||
		errors.Is(err, sarama.ErrConsumerCoordinatorNotAvailable)
}

// PartitionLag returns the group's lag for every partition of topic.
//
// An error for any partition (the broker response has no block for the
// partition, or the block carries an error) is returned as is together with
// topic/partition, so errors.Is against sarama.KError works; multiple errors
// are combined with errors.Join. No partial map is returned with an error
// (nil): an incomplete lag would silently under-report the metric.
//
// If the coordinator broker has moved (request error,
// ErrNotCoordinatorForConsumer, ErrConsumerCoordinatorNotAvailable), the cache
// is reset and the next call uses the new coordinator; the current call
// returns the error.
func (c *Coordinator) PartitionLag(topic string) (map[int32]int64, error) {
	broker, err := c.broker()
	if err != nil {
		return nil, fmt.Errorf("kafka.coordinator.partition_lag.init: %w", err)
	}

	partitions, err := c.client.client.Partitions(topic)
	if err != nil {
		return nil, fmt.Errorf("kafka.coordinator.partition_lag.get_partitions: %w", err)
	}

	// Fetch the group's committed offsets.
	request := &sarama.OffsetFetchRequest{
		Version:       1,
		ConsumerGroup: c.groupID,
	}

	for _, partition := range partitions {
		request.AddPartition(topic, partition)
	}

	response, err := broker.FetchOffset(request)
	if err != nil {
		c.resetCoordinator(broker)

		return nil, fmt.Errorf("kafka.coordinator.partition_lag.fetch_offset: %w", err)
	}

	if isCoordinatorError(response.Err) {
		c.resetCoordinator(broker)

		return nil, fmt.Errorf("kafka.coordinator.partition_lag.fetch_offset: %w", response.Err)
	}

	if !errors.Is(response.Err, sarama.ErrNoError) {
		return nil, fmt.Errorf("kafka.coordinator.partition_lag.fetch_offset: %w", response.Err)
	}

	lagByPartition := make(map[int32]int64, len(partitions))

	var partitionErrs []error

	for _, partition := range partitions {
		groupOffset := response.GetBlock(topic, partition)
		if groupOffset == nil {
			partitionErrs = append(partitionErrs,
				fmt.Errorf("kafka.coordinator.partition_lag: topic %s partition %d: no block in the response",
					topic, partition))

			continue
		}

		if !errors.Is(groupOffset.Err, sarama.ErrNoError) {
			if isCoordinatorError(groupOffset.Err) {
				c.resetCoordinator(broker)
			}

			partitionErrs = append(partitionErrs,
				fmt.Errorf("kafka.coordinator.partition_lag: topic %s partition %d: %w",
					topic, partition, groupOffset.Err))

			continue
		}

		lag, err := c.partitionLag(topic, partition, groupOffset.Offset)
		if err != nil {
			return nil, err
		}

		lagByPartition[partition] = lag
	}

	if len(partitionErrs) > 0 {
		return nil, errors.Join(partitionErrs...)
	}

	return lagByPartition, nil
}

func (c *Coordinator) partitionLag(topic string, partition int32, groupOffset int64) (int64, error) {
	// offset == -1: the group has never committed to this partition.
	if groupOffset == -1 {
		oldest, err := c.client.client.GetOffset(topic, partition, sarama.OffsetOldest)
		if err != nil {
			return 0, fmt.Errorf("kafka.coordinator.partition_lag.get_oldest_offset: %w", err)
		}

		newest, err := c.client.client.GetOffset(topic, partition, sarama.OffsetNewest)
		if err != nil {
			return 0, fmt.Errorf("kafka.coordinator.partition_lag.get_newest_offset: %w", err)
		}

		return newest - oldest, nil
	}

	newest, err := c.client.client.GetOffset(topic, partition, sarama.OffsetNewest)
	if err != nil {
		return 0, fmt.Errorf("kafka.coordinator.partition_lag.get_offset: %w", err)
	}

	return newest - groupOffset, nil
}

// Close drops the cached coordinator. The broker connection is not closed: it
// belongs to the sarama client (producers may share it) and is closed together
// with [Client].
func (c *Coordinator) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.coordinator = nil

	return nil
}
