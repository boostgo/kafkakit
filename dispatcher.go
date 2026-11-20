package kafkakit

import (
	"context"
	"fmt"
)

// Marshaller encodes a message body, for example [encoding/json.Marshal].
type Marshaller func(message any) ([]byte, error)

// Dispatcher is a thin wrapper over a [Producer] bound to a topic and a
// [Marshaller].
//
// Dispatch, DispatchMany and Topic are safe for concurrent use: there is no
// mutable state, and neither messages nor header maps are mutated. The
// exception is WithMarshaller, which replaces the marshaller and must be
// called during setup, before workers start.
//
// Dispatch marshals body, sends it to the bound topic with the given key and
// optional headers, and wraps failures with [ErrDispatcherDispatch].
// DispatchMany sends pre-built messages as is (their Topic field is used) and
// wraps failures with both [ErrDispatcherDispatchMany] and
// [ErrDispatcherDispatch].
type Dispatcher interface {
	WithMarshaller(marshaller Marshaller) Dispatcher
	Topic() string
	Dispatch(ctx context.Context, key string, body any, headers ...map[string]string) error
	DispatchMany(ctx context.Context, messages ...*ProducerMessage) error
}

type kafkaDispatcher struct {
	producer   Producer
	topic      string
	marshaller Marshaller
}

// NewDispatcher returns a [Dispatcher] that sends to topic via producer and
// encodes bodies with marshaller.
func NewDispatcher(producer Producer, topic string, marshaller Marshaller) Dispatcher {
	return &kafkaDispatcher{
		producer:   producer,
		topic:      topic,
		marshaller: marshaller,
	}
}

// WithMarshaller replaces the marshaller. NOT safe for concurrent use: call it
// only while configuring the dispatcher, before concurrent Dispatch calls.
func (d *kafkaDispatcher) WithMarshaller(marshaller Marshaller) Dispatcher {
	d.marshaller = marshaller
	return d
}

func (d *kafkaDispatcher) Topic() string {
	return d.topic
}

// Dispatch is safe for concurrent use: the header map is only read and never
// mutated, so it can be shared between goroutines.
func (d *kafkaDispatcher) Dispatch(ctx context.Context, key string, body any, headers ...map[string]string) error {
	bodyInBytes, err := d.marshaller(body)
	if err != nil {
		return err
	}

	var setHeaders map[string]string
	if len(headers) > 0 {
		setHeaders = headers[0]
	}

	if err = d.producer.Produce(ctx, &ProducerMessage{
		Topic:   d.topic,
		Value:   bodyInBytes,
		Headers: setHeaders,
		Key:     key,
	}); err != nil {
		return fmt.Errorf("%w: %w: topic: %s", ErrDispatcherDispatch, err, d.topic)
	}

	return nil
}

func (d *kafkaDispatcher) DispatchMany(ctx context.Context, messages ...*ProducerMessage) error {
	if err := d.producer.Produce(ctx, messages...); err != nil {
		return fmt.Errorf("%w: %w: %w: topic: %s", ErrDispatcherDispatchMany, ErrDispatcherDispatch, err, d.topic)
	}

	return nil
}
