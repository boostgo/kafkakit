package kafkakit

import (
	"context"

	"github.com/IBM/sarama"
	"github.com/go-playground/validator/v10"
)

// BatchConsumerContext provides access to a batch of Kafka messages
// collected from a single partition. Offset marking is done at the
// batch level via MarkAll which marks the last message in the batch.
type BatchConsumerContext struct {
	ctx      context.Context
	session  sarama.ConsumerGroupSession
	messages []*ConsumerMessage
	last     *sarama.ConsumerMessage
}

// newBatchConsumerContext copies the raw messages into its own slice of
// wrappers. consumerGroupBatchHandler relies on this copy because it reuses the
// batch buffer: the raw slice itself must never be retained by the context.
func newBatchConsumerContext(
	ctx context.Context,
	session sarama.ConsumerGroupSession,
	raw []*sarama.ConsumerMessage,
	validate *validator.Validate,
	traceHeader string,
) *BatchConsumerContext {
	messages := make([]*ConsumerMessage, 0, len(raw))
	for _, msg := range raw {
		messages = append(messages, newConsumerMessage(msg, validate, traceHeader))
	}

	return &BatchConsumerContext{
		ctx:      ctx,
		session:  session,
		messages: messages,
		last:     raw[len(raw)-1],
	}
}

// Context returns the context associated with this batch.
func (b *BatchConsumerContext) Context() context.Context {
	if b.ctx == nil {
		return context.Background()
	}

	return b.ctx
}

// Messages returns all messages in the batch as lightweight wrappers.
func (b *BatchConsumerContext) Messages() []*ConsumerMessage {
	return b.messages
}

// Len returns the number of messages in the batch.
func (b *BatchConsumerContext) Len() int {
	return len(b.messages)
}

// MarkAll marks the offset of the last message in the batch.
// The autocommit mechanism will pick up the marked offset.
func (b *BatchConsumerContext) MarkAll() {
	b.session.MarkMessage(b.last, "")
}

// Commit triggers an immediate offset commit for marked offsets.
func (b *BatchConsumerContext) Commit() {
	b.session.Commit()
}

// LastOffset returns the offset of the last message in the batch.
func (b *BatchConsumerContext) LastOffset() int64 {
	return b.last.Offset
}

// Partition returns the partition this batch was consumed from.
func (b *BatchConsumerContext) Partition() int32 {
	return b.last.Partition
}

// Topic returns the topic this batch was consumed from.
func (b *BatchConsumerContext) Topic() string {
	return b.last.Topic
}
