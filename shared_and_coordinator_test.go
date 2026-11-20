package kafkakit

import (
	"context"
	"errors"
	"testing"

	"github.com/IBM/sarama"
	"github.com/rs/zerolog"
)

const (
	mbTopic = "mb-topic"
	mbGroup = "mb-group"
)

// newMockClient builds a *Client on MockBrokers (in-process, on loopback).
// meta is the broker with metadata; handlers extend/override meta's responses.
func newMockClient(t *testing.T, meta *sarama.MockBroker, opts ...ClientOption) *Client {
	t.Helper()

	client, err := NewClient(
		context.Background(),
		zerolog.Nop(),
		append([]ClientOption{WithBrokers([]string{meta.Addr()})}, opts...)...,
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	t.Cleanup(func() { _ = client.Close() })

	return client
}

func metadataFor(t *testing.T, brokers ...*sarama.MockBroker) *sarama.MockMetadataResponse {
	t.Helper()

	m := sarama.NewMockMetadataResponse(t)
	for _, b := range brokers {
		m.SetBroker(b.Addr(), b.BrokerID())
	}

	return m.SetLeader(mbTopic, 0, brokers[0].BrokerID())
}

// --- coordinator

func TestCoordinator_ResetOnError(t *testing.T) {
	meta := sarama.NewMockBroker(t, 1)
	stale := sarama.NewMockBroker(t, 2)
	fresh := sarama.NewMockBroker(t, 3)

	t.Cleanup(func() { fresh.Close(); stale.Close(); meta.Close() })

	// sarama sends FindCoordinator/Metadata to any known broker, so all three
	// need the common responses; OffsetFetch only goes to coordinators.
	apply := func(coordinator *sarama.MockBroker) {
		for _, b := range []*sarama.MockBroker{meta, stale, fresh} {
			handlers := map[string]sarama.MockResponse{
				"MetadataRequest": metadataFor(t, meta, stale, fresh),
				"FindCoordinatorRequest": sarama.NewMockFindCoordinatorResponse(t).
					SetCoordinator(sarama.CoordinatorGroup, mbGroup, coordinator),
				"OffsetRequest": sarama.NewMockOffsetResponse(t).
					SetOffset(mbTopic, 0, sarama.OffsetNewest, 10).
					SetOffset(mbTopic, 0, sarama.OffsetOldest, 0),
			}

			switch b {
			case stale:
				// The first coordinator is no longer the coordinator.
				handlers["OffsetFetchRequest"] = sarama.NewMockOffsetFetchResponse(t).
					SetOffset(mbGroup, mbTopic, 0, 0, "", sarama.ErrNotCoordinatorForConsumer)
			case fresh:
				handlers["OffsetFetchRequest"] = sarama.NewMockOffsetFetchResponse(t).
					SetOffset(mbGroup, mbTopic, 0, 4, "", sarama.ErrNoError)
			}

			b.SetHandlerByMap(handlers)
		}
	}

	apply(stale)

	client := newMockClient(t, meta)
	coordinator := NewCoordinator(client, mbGroup)

	t.Cleanup(func() { _ = coordinator.Close() })

	// The coordinator is cached (stale), then it moves to fresh in the cluster.
	if err := coordinator.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}

	apply(fresh)

	if _, err := coordinator.PartitionLag(mbTopic); !errors.Is(err, sarama.ErrNotCoordinatorForConsumer) {
		t.Fatalf("first call: want ErrNotCoordinatorForConsumer, got %v", err)
	}

	lag, err := coordinator.PartitionLag(mbTopic)
	if err != nil {
		t.Fatalf("second call must go to the new coordinator: %v", err)
	}

	if lag[0] != 6 { // newest 10 - committed 4
		t.Fatalf("lag = %v, want partition 0 → 6", lag)
	}
}

func TestCoordinator_PartitionErrorReturned(t *testing.T) {
	meta := sarama.NewMockBroker(t, 1)
	t.Cleanup(meta.Close)

	meta.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": metadataFor(t, meta),
		"FindCoordinatorRequest": sarama.NewMockFindCoordinatorResponse(t).
			SetCoordinator(sarama.CoordinatorGroup, mbGroup, meta),
		"OffsetRequest": sarama.NewMockOffsetResponse(t).
			SetOffset(mbTopic, 0, sarama.OffsetNewest, 10),
		"OffsetFetchRequest": sarama.NewMockOffsetFetchResponse(t).
			SetOffset(mbGroup, mbTopic, 0, 0, "", sarama.ErrOffsetsLoadInProgress),
	})

	client := newMockClient(t, meta)
	coordinator := NewCoordinator(client, mbGroup)

	lag, err := coordinator.PartitionLag(mbTopic)
	if !errors.Is(err, sarama.ErrOffsetsLoadInProgress) {
		t.Fatalf("want ErrOffsetsLoadInProgress, got lag=%v err=%v", lag, err)
	}

	if lag != nil {
		t.Fatalf("no partial lag may be returned with an error, got %v", lag)
	}

	if _, err := coordinator.TotalLag(mbTopic); !errors.Is(err, sarama.ErrOffsetsLoadInProgress) {
		t.Fatalf("TotalLag must surface the error, got %v", err)
	}
}

// --- shared client

func newProduceBroker(t *testing.T) *sarama.MockBroker {
	t.Helper()

	b := sarama.NewMockBroker(t, 1)
	t.Cleanup(b.Close)

	b.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": metadataFor(t, b),
		"ProduceRequest":  sarama.NewMockProduceResponse(t).SetError(mbTopic, 0, sarama.ErrNoError),
		"FindCoordinatorRequest": sarama.NewMockFindCoordinatorResponse(t).
			SetCoordinator(sarama.CoordinatorGroup, mbGroup, b),
	})

	return b
}

func TestProducer_SharedClient(t *testing.T) {
	broker := newProduceBroker(t)
	client := newMockClient(t, broker, WithClientProducerOptions(WithProducerFlushMessages(1)))

	msg := &ProducerMessage{Topic: mbTopic, Key: "k", Value: []byte("v")}

	t.Run("sync", func(t *testing.T) {
		producer, err := NewSyncProducer(client, WithProducerClient(client), WithProducerFlushMessages(1))
		if err != nil {
			t.Fatalf("NewSyncProducer: %v", err)
		}

		if err := producer.Produce(context.Background(), msg); err != nil {
			t.Fatalf("Produce: %v", err)
		}

		if err := CloseProducer(producer); err != nil {
			t.Fatalf("CloseProducer: %v", err)
		}

		if client.Client().Closed() {
			t.Fatal("closing the producer must not close the shared client")
		}
	})

	t.Run("async with callback", func(t *testing.T) {
		delivered := make(chan error, 1)

		producer, err := NewAsyncProducer(client,
			WithProducerClient(client),
			WithProducerFlushMessages(1),
			WithDeliveryCallback(func(_ *ProducerMessage, err error) { delivered <- err }),
		)
		if err != nil {
			t.Fatalf("NewAsyncProducer: %v", err)
		}

		if err := producer.Produce(context.Background(), msg); err != nil {
			t.Fatalf("Produce: %v", err)
		}

		if err := CloseProducer(producer); err != nil {
			t.Fatalf("CloseProducer: %v", err)
		}

		if err := <-delivered; err != nil {
			t.Fatalf("delivery: %v", err)
		}

		if client.Client().Closed() {
			t.Fatal("closing the producer must not close the shared client")
		}
	})

	t.Run("conflicting options are not lost silently", func(t *testing.T) {
		_, err := NewAsyncProducer(client, WithProducerClient(client), WithProducerFlushMessages(1), WithProducerRetryMax(9))
		if !errors.Is(err, ErrSharedClientConfig) || !errors.Is(err, ErrProducerNew) {
			t.Fatalf("want ErrProducerNew + ErrSharedClientConfig, got %v", err)
		}
	})

	t.Run("client without producer section", func(t *testing.T) {
		plain := newMockClient(t, broker)

		_, err := NewSyncProducer(plain, WithProducerClient(plain))
		if !errors.Is(err, ErrSharedClientConfig) {
			t.Fatalf("want ErrSharedClientConfig, got %v", err)
		}
	})
}

func TestConsumer_SharedClient(t *testing.T) {
	broker := newProduceBroker(t)
	client := newMockClient(t, broker, WithClientConsumerOptions(WithConsumerInitialOffset(OffsetOldest)))

	t.Run("builds on the shared client and Close keeps it open", func(t *testing.T) {
		c, err := NewConsumer(context.Background(), client,
			WithConsumerGroupID(mbGroup),
			WithConsumerTopics(mbTopic),
			WithConsumerInitialOffset(OffsetOldest),
			WithConsumerClient(client),
		)
		if err != nil {
			t.Fatalf("NewConsumer: %v", err)
		}

		if err := c.Close(context.Background()); err != nil {
			t.Fatalf("Close: %v", err)
		}

		if client.Client().Closed() {
			t.Fatal("Consumer.Close must not close the shared client")
		}
	})

	t.Run("conflicting options are not lost silently", func(t *testing.T) {
		_, err := NewConsumer(context.Background(), client,
			WithConsumerGroupID(mbGroup),
			WithConsumerTopics(mbTopic),
			WithConsumerClient(client), // default InitialOffset (Newest) differs from the client's Oldest
		)
		if !errors.Is(err, ErrSharedClientConfig) || !errors.Is(err, ErrConsumerNew) {
			t.Fatalf("want ErrConsumerNew + ErrSharedClientConfig, got %v", err)
		}
	})

	t.Run("client without consumer section", func(t *testing.T) {
		plain := newMockClient(t, broker)

		_, err := NewConsumer(context.Background(), plain,
			WithConsumerGroupID(mbGroup), WithConsumerTopics(mbTopic), WithConsumerClient(plain))
		if !errors.Is(err, ErrSharedClientConfig) {
			t.Fatalf("want ErrSharedClientConfig, got %v", err)
		}
	})
}

// --- partial failures

type partialFailSyncProducer struct {
	sarama.SyncProducer
	failKeys map[string]bool
	cause    error
}

func (p partialFailSyncProducer) SendMessages(msgs []*sarama.ProducerMessage) error {
	var pErrs sarama.ProducerErrors

	for _, m := range msgs {
		key, _ := m.Key.Encode()
		if p.failKeys[string(key)] {
			pErrs = append(pErrs, &sarama.ProducerError{Msg: m, Err: p.cause})
		}
	}

	if len(pErrs) == 0 {
		return nil
	}

	return pErrs
}

func TestSyncProducer_PartialFailure(t *testing.T) {
	messages := make([]*ProducerMessage, 5)
	for i := range messages {
		messages[i] = &ProducerMessage{Topic: "t", Key: string(rune('a' + i))}
	}

	p := &syncProducer{producer: partialFailSyncProducer{
		failKeys: map[string]bool{"b": true, "d": true},
		cause:    sarama.ErrMessageSizeTooLarge,
	}}

	err := p.Produce(context.Background(), messages...)

	var perr *ProduceError
	if !errors.As(err, &perr) {
		t.Fatalf("want *ProduceError in the chain, got %v", err)
	}

	if len(perr.Failed) != 2 || perr.Failed[0].Message != messages[1] || perr.Failed[1].Message != messages[3] {
		t.Fatalf("Failed must hold exactly the 2 original messages, got %+v", perr.Failed)
	}

	if !errors.Is(err, ErrProducerSend) || !errors.Is(err, sarama.ErrMessageSizeTooLarge) {
		t.Fatalf("errors.Is must reach ErrProducerSend and the sarama cause, got %v", err)
	}
}

func TestSyncProducer_NonPartialFailureUnderSentinel(t *testing.T) {
	boom := errors.New("boom")

	p := &syncProducer{producer: failingSyncProducer{err: boom}}

	err := p.Produce(context.Background(), &ProducerMessage{Topic: "t"})
	if !errors.Is(err, ErrProducerSend) || !errors.Is(err, boom) {
		t.Fatalf("want ErrProducerSend + cause, got %v", err)
	}
}

type failingSyncProducer struct {
	sarama.SyncProducer
	err error
}

func (p failingSyncProducer) SendMessages([]*sarama.ProducerMessage) error { return p.err }
