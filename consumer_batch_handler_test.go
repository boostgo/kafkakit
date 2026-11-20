package kafkakit

import (
	"context"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/rs/zerolog"
)

type fakeSession struct {
	ctx    context.Context
	marked int
}

func (s *fakeSession) Claims() map[string][]int32 { return nil }
func (s *fakeSession) MemberID() string           { return "member" }
func (s *fakeSession) GenerationID() int32        { return 1 }

func (s *fakeSession) MarkOffset(_ string, _ int32, _ int64, _ string)  {}
func (s *fakeSession) ResetOffset(_ string, _ int32, _ int64, _ string) {}
func (s *fakeSession) Commit()                                          {}

func (s *fakeSession) MarkMessage(_ *sarama.ConsumerMessage, _ string) { s.marked++ }
func (s *fakeSession) Context() context.Context                        { return s.ctx }

type fakeClaim struct {
	messages chan *sarama.ConsumerMessage
}

func (c *fakeClaim) Topic() string                            { return "test-topic" }
func (c *fakeClaim) Partition() int32                         { return 0 }
func (c *fakeClaim) InitialOffset() int64                     { return 0 }
func (c *fakeClaim) HighWaterMarkOffset() int64               { return 0 }
func (c *fakeClaim) Messages() <-chan *sarama.ConsumerMessage { return c.messages }

func newBatchHandler(t testing.TB, batchSize int, handler BatchConsumerHandler) sarama.ConsumerGroupHandler {
	t.Helper()

	return newConsumerGroupBatchHandler(
		zerolog.Nop(),
		"test.batch_handler",
		nil,
		nil,
		handler,
		nil,
		0,
		false,
		batchSize,
		time.Hour, // the ticker is not used in tests: flush by batch size
		nil,
		"",
	)
}

func feedClaim(count int) *fakeClaim {
	claim := &fakeClaim{messages: make(chan *sarama.ConsumerMessage, count)}
	for i := range count {
		claim.messages <- &sarama.ConsumerMessage{
			Topic:     "test-topic",
			Partition: 0,
			Offset:    int64(i),
			Key:       []byte("key"),
			Value:     []byte(`{"i":1}`),
		}
	}
	close(claim.messages)

	return claim
}

func TestBatchHandler_FlushesByBatchSize(t *testing.T) {
	const (
		batchSize = 100
		total     = 250
	)

	var batches, messages int

	handler := newBatchHandler(t, batchSize, func(batch *BatchConsumerContext) error {
		batches++
		messages += batch.Len()
		batch.MarkAll()

		return nil
	})

	session := &fakeSession{ctx: context.Background()}

	if err := handler.ConsumeClaim(session, feedClaim(total)); err != nil {
		t.Fatalf("closing claim.Messages() must not be an error, got %v", err)
	}

	if batches != 3 { // 100 + 100 + a tail of 50 when the channel closes
		t.Fatalf("batches: got %d, want 3", batches)
	}

	if messages != total {
		t.Fatalf("messages: got %d, want %d", messages, total)
	}

	if session.marked != batches {
		t.Fatalf("marked: got %d, want %d", session.marked, batches)
	}
}

// A batch retained by the caller must not be corrupted by later batches:
// the handler must hand out its own copy of the messages on every flush.
func TestBatchHandler_RetainedBatchStaysValid(t *testing.T) {
	const batchSize = 10

	var retained []*ConsumerMessage

	handler := newBatchHandler(t, batchSize, func(batch *BatchConsumerContext) error {
		if retained == nil {
			retained = batch.Messages()
		}

		return nil
	})

	if err := handler.ConsumeClaim(&fakeSession{ctx: context.Background()}, feedClaim(batchSize*5)); err != nil {
		t.Fatalf("consume claim: %v", err)
	}

	if len(retained) != batchSize {
		t.Fatalf("retained batch length: got %d, want %d", len(retained), batchSize)
	}

	for i, message := range retained {
		if message.Offset() != int64(i) {
			t.Fatalf("retained batch was overwritten at %d: offset %d", i, message.Offset())
		}
	}
}

func BenchmarkBatchHandlerConsumeClaim(b *testing.B) {
	const (
		batchSize = 100
		total     = 10_000
	)

	handler := newBatchHandler(b, batchSize, func(batch *BatchConsumerContext) error {
		batch.MarkAll()

		return nil
	})

	session := &fakeSession{ctx: context.Background()}

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		b.StopTimer()
		claim := feedClaim(total)
		b.StartTimer()

		_ = handler.ConsumeClaim(session, claim)
	}
}
