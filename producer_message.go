package kafkakit

import (
	"github.com/IBM/sarama"
)

// ProducerMessages is a convenience slice type for batches of messages.
type ProducerMessages []*ProducerMessage

// ProducerMessage is a message to be sent with [Producer].Produce.
//
// Producers never mutate it, so the same message (and its Headers map) may be
// shared between goroutines.
type ProducerMessage struct {
	// Topic is the destination topic.
	Topic string
	// Key is the partitioning key (empty means no key).
	Key string
	// Value is the payload.
	Value []byte
	// Headers are sent as record headers.
	Headers map[string]string
}

// saramaMessage converts the message into its sarama representation.
//
// A non-empty traceID is added to the result headers under traceHeader (""
// means DefaultTraceHeader) and overrides a message header of the same name.
// The source *ProducerMessage and its header map are NOT mutated: the same
// message (or a shared header map) may be used from several goroutines, and
// mutating it would cause a data race or a "concurrent map writes" panic.
func (m *ProducerMessage) saramaMessage(traceHeader, traceID string) *sarama.ProducerMessage {
	traceHeader = traceHeaderOrDefault(traceHeader)
	headers := make([]sarama.RecordHeader, 0, len(m.Headers)+1)

	for k, v := range m.Headers {
		if traceID != "" && k == traceHeader {
			continue
		}

		headers = append(headers, sarama.RecordHeader{
			Key:   convertBytesFromString(k),
			Value: convertBytesFromString(v),
		})
	}

	if traceID != "" {
		headers = append(headers, sarama.RecordHeader{
			Key:   convertBytesFromString(traceHeader),
			Value: convertBytesFromString(traceID),
		})
	}

	return &sarama.ProducerMessage{
		Topic:   m.Topic,
		Key:     sarama.StringEncoder(m.Key),
		Value:   sarama.ByteEncoder(m.Value),
		Headers: headers,
		// Metadata links the sarama message to the original one for WithDeliveryCallback.
		Metadata: m,
	}
}

func adaptSaramaMessages(messages []*ProducerMessage, traceHeader, traceID string) []*sarama.ProducerMessage {
	msgs := make([]*sarama.ProducerMessage, 0, len(messages))
	for _, m := range messages {
		msgs = append(msgs, m.saramaMessage(traceHeader, traceID))
	}

	return msgs
}
