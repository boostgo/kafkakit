package kafkakit

import (
	"encoding/json"
	"time"

	"github.com/IBM/sarama"
	"github.com/go-playground/validator/v10"
)

// ConsumerMessage is a lightweight wrapper over a single Kafka message
// used inside batch processing. Unlike ConsumerContext it does not expose
// Mark/Commit — offset management is handled at the batch level.
type ConsumerMessage struct {
	message  *sarama.ConsumerMessage
	validate *validator.Validate
	traceID  string
}

func newConsumerMessage(
	message *sarama.ConsumerMessage,
	validate *validator.Validate,
	traceHeader string,
) *ConsumerMessage {
	return &ConsumerMessage{
		message:  message,
		validate: validate,
		traceID:  traceIDFromHeaders(message.Headers, traceHeader),
	}
}

// Topic returns the topic of the message.
func (m *ConsumerMessage) Topic() string {
	return m.message.Topic
}

// Partition returns the partition of the message.
func (m *ConsumerMessage) Partition() int32 {
	return m.message.Partition
}

// Offset returns the offset of the message.
func (m *ConsumerMessage) Offset() int64 {
	return m.message.Offset
}

// Timestamp returns the message timestamp.
func (m *ConsumerMessage) Timestamp() time.Time {
	return m.message.Timestamp
}

// Key returns the message key.
func (m *ConsumerMessage) Key() []byte {
	return m.message.Key
}

// Value returns the message payload.
func (m *ConsumerMessage) Value() []byte {
	return m.message.Value
}

// Header returns the value of the first header named name, or "".
func (m *ConsumerMessage) Header(name string) string {
	return readConsumerMessageHeader(m.message, name)
}

// Headers returns all message headers as a map (the last value wins for
// duplicate keys).
func (m *ConsumerMessage) Headers() map[string]string {
	headers := make(map[string]string, len(m.message.Headers))
	for _, header := range m.message.Headers {
		headers[string(header.Key)] = convertStringFromBytes(header.Value)
	}
	return headers
}

// TraceID returns the message trace ID: the value of the trace header
// ([Client.TraceHeader]), or "" if the message has none. It is computed once
// when the wrapper is created.
func (m *ConsumerMessage) TraceID() string {
	return m.traceID
}

// Parse unmarshals the JSON payload into export (which must be a pointer)
// and validates it with the validator set by [WithConsumerValidate], if any.
func (m *ConsumerMessage) Parse(export any) error {
	if !isPointer(export) {
		return ErrExportIsNotPointer
	}

	if err := json.Unmarshal(m.message.Value, export); err != nil {
		return err
	}

	if m.validate != nil {
		if err := m.validate.Struct(export); err != nil {
			return err
		}
	}

	return nil
}
