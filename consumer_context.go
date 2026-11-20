package kafkakit

import (
	"context"
	"encoding/json"
	"time"

	"github.com/IBM/sarama"
	"github.com/go-playground/validator/v10"
)

// ConsumerContext is passed to a [ConsumerHandler] for every consumed message.
// It exposes the message, its metadata and offset management (Mark/Commit).
type ConsumerContext struct {
	ctx      context.Context
	session  sarama.ConsumerGroupSession
	claim    sarama.ConsumerGroupClaim
	message  *sarama.ConsumerMessage
	validate *validator.Validate
	traceID  string
}

func newContext(
	ctx context.Context,
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
	message *sarama.ConsumerMessage,
	validate *validator.Validate,
	traceHeader string,
) *ConsumerContext {
	// The trace ID is computed once and stored in ctx under TraceKey (read by
	// Produce and by application code); an empty trace ID is not stored.
	traceID := traceIDFromHeaders(message.Headers, traceHeader)
	if traceID != "" {
		ctx = context.WithValue(ctx, TraceKey, traceID) //nolint:staticcheck // TraceKey is a public string contract
	}

	return &ConsumerContext{
		ctx:      ctx,
		session:  session,
		claim:    claim,
		message:  message,
		validate: validate,
		traceID:  traceID,
	}
}

// Context returns the handler context. It carries the trace ID under
// [TraceKey] and, by default, is not canceled by a rebalance or Close.
func (c *ConsumerContext) Context() context.Context {
	if c.ctx == nil {
		return context.Background()
	}

	return c.ctx
}

// Topic returns the topic of the message.
func (c *ConsumerContext) Topic() string {
	return c.claim.Topic()
}

// Partition returns the partition of the message.
func (c *ConsumerContext) Partition() int32 {
	return c.claim.Partition()
}

// Offset returns the offset of the message.
func (c *ConsumerContext) Offset() int64 {
	return c.message.Offset
}

// Timestamp returns the message timestamp.
func (c *ConsumerContext) Timestamp() time.Time {
	return c.message.Timestamp
}

// Mark marks the message as processed; the offset is committed by
// auto-commit or by [ConsumerContext.Commit].
func (c *ConsumerContext) Mark() {
	c.session.MarkMessage(c.message, "")
}

// Commit synchronously commits all marked offsets of the session.
func (c *ConsumerContext) Commit() {
	c.session.Commit()
}

// Key returns the message key.
func (c *ConsumerContext) Key() []byte {
	return c.message.Key
}

// Value returns the message payload.
func (c *ConsumerContext) Value() []byte {
	return c.message.Value
}

// Header returns the value of the first header named name, or "".
func (c *ConsumerContext) Header(name string) string {
	return readConsumerMessageHeader(c.message, name)
}

// Headers returns all message headers as a map (the last value wins for
// duplicate keys).
func (c *ConsumerContext) Headers() map[string]string {
	headers := make(map[string]string, len(c.message.Headers))
	for _, header := range c.message.Headers {
		headers[string(header.Key)] = convertStringFromBytes(header.Value)
	}
	return headers
}

// TraceID returns the message trace ID: the value of the trace header
// ([Client.TraceHeader]), or "" if the message has none. It is computed once
// when the context is created.
func (c *ConsumerContext) TraceID() string {
	return c.traceID
}

// Parse unmarshals the JSON payload into export (which must be a pointer)
// and validates it with the validator set by [WithConsumerValidate], if any.
func (c *ConsumerContext) Parse(export any) error {
	// check export type
	if !isPointer(export) {
		return ErrExportIsNotPointer
	}

	// parse message
	if err := json.Unmarshal(c.message.Value, export); err != nil {
		return err
	}

	// validate struct
	if c.validate != nil {
		if err := c.validate.Struct(export); err != nil {
			return err
		}
	}

	return nil
}
