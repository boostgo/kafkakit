package kafkakit

import (
	"context"
	"errors"
	"testing"
)

type failingProducer struct{ err error }

func (p failingProducer) Produce(context.Context, ...*ProducerMessage) error { return p.err }

func TestDispatcher_ErrorsWrapSentinels(t *testing.T) {
	cause := errors.New("boom")
	d := NewDispatcher(failingProducer{err: cause}, "orders", func(any) ([]byte, error) { return []byte("{}"), nil })

	if d.Topic() != "orders" {
		t.Fatalf("Topic() = %q, want orders", d.Topic())
	}

	err := d.Dispatch(context.Background(), "k", struct{}{})
	if !errors.Is(err, ErrDispatcherDispatch) || !errors.Is(err, cause) {
		t.Fatalf("Dispatch error = %v, want ErrDispatcherDispatch and cause in chain", err)
	}

	err = d.DispatchMany(context.Background(), &ProducerMessage{Topic: "orders"})
	if !errors.Is(err, ErrDispatcherDispatchMany) || !errors.Is(err, ErrDispatcherDispatch) || !errors.Is(err, cause) {
		t.Fatalf("DispatchMany error = %v, want both sentinels and cause in chain", err)
	}
}

func TestDispatcher_MarshallerError(t *testing.T) {
	marshalErr := errors.New("marshal")
	d := NewDispatcher(failingProducer{}, "orders", func(any) ([]byte, error) { return nil, marshalErr })

	if err := d.Dispatch(context.Background(), "k", 1); !errors.Is(err, marshalErr) {
		t.Fatalf("Dispatch error = %v, want marshaller error", err)
	}
}
