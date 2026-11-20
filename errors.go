package kafkakit

import (
	"errors"
	"fmt"
)

// Sentinel errors. Returned errors wrap them, so check with [errors.Is].
var (
	// ErrConnect is returned by [NewClient] when the cluster is unreachable.
	ErrConnect = errors.New("kafka.connect")

	// ErrClientVersion is returned by [NewClient] when the protocol version
	// passed to [WithVersion] cannot be parsed (expected "2.8.0" / "0.11.0.2").
	ErrClientVersion = errors.New("kafka.client.version_invalid")

	// ErrClientTraceHeader is returned by [NewClient] when the name passed to
	// [WithTraceHeader] contains whitespace, control characters or invalid
	// UTF-8.
	ErrClientTraceHeader = errors.New("kafka.client.trace_header_invalid")

	// ErrProducerNew is returned when a producer cannot be created.
	ErrProducerNew = errors.New("kafka.producer.new")
	// ErrProducerSend is returned when one or more messages cannot be sent.
	ErrProducerSend = errors.New("kafka.producer.send")
	// ErrProducerClosed is returned (together with ErrProducerSend) by Produce
	// on a producer that is being closed or has been closed.
	ErrProducerClosed = errors.New("kafka.producer.closed")

	// ErrProducerCompressionVersion means the codec selected with
	// [WithProducerCompression] (CompressionZSTD / CompressionLZ4) requires a
	// newer protocol version than the one set with [WithVersion] (or
	// sarama.DefaultVersion). Raise the version with WithVersion; the minimum
	// version is included in the error text.
	ErrProducerCompressionVersion = errors.New("kafka.producer.compression_version_too_low")

	// ErrSharedClientConfig means a producer or consumer built on top of a
	// shared client ([WithProducerClient] / [WithConsumerClient]) conflicts
	// with the client's config: the client was created without the
	// Producer/Consumer section, or the producer/consumer options differ from
	// it. Sections are set when the client is created:
	// [WithClientProducerOptions] / [WithClientConsumerOptions].
	ErrSharedClientConfig = errors.New("kafka.shared_client.config")

	// ErrConsumerNew is returned when a consumer cannot be created.
	ErrConsumerNew = errors.New("kafka.consumer.new")

	// ErrConsumerGroupInstanceIDVersion means [WithConsumerGroupInstanceID]
	// (static membership, KIP-345) was set with a protocol version below
	// 2.3.0. Fix it with the client option WithVersion("2.3.0") or newer.
	ErrConsumerGroupInstanceIDVersion = errors.New("kafka.consumer.group_instance_id_version_too_low")

	// ErrDispatcherDispatch is returned by [Dispatcher] Dispatch and DispatchMany.
	ErrDispatcherDispatch = errors.New("kafka.dispatcher.dispatch")
	// ErrDispatcherDispatchMany is additionally returned by DispatchMany.
	ErrDispatcherDispatchMany = errors.New("kafka.dispatcher.dispatch_many")

	// ErrConsumerAppLifeDone, when delivered on the consumer group's error
	// channel, stops the consumer's error listener.
	ErrConsumerAppLifeDone = errors.New("kafka.consumer.app.done")

	// ErrExportIsNotPointer is returned by Parse when the target is not a pointer.
	ErrExportIsNotPointer = errors.New("kafka.export.not_pointer")
)

// ProduceFailure is a single message that could not be sent.
type ProduceFailure struct {
	// Message is the original message passed to Produce.
	Message *ProducerMessage

	// Err is the reason this message was rejected.
	Err error
}

// ProduceError reports a partial failure of a sync producer: some messages of
// the batch were not sent.
//
// It is returned by Produce inside the error chain (together with
// [ErrProducerSend]):
//
//	var perr *kafkakit.ProduceError
//	if errors.As(err, &perr) {
//		for _, f := range perr.Failed { resend(f.Message) } // only the failed ones
//	}
//
// Messages of the batch that are not listed in Failed were delivered; do not
// resend the whole batch or you will produce duplicates. errors.Is against the
// underlying causes (sarama errors) works via Unwrap.
type ProduceError struct {
	// Failed lists the messages that were not sent and the reason for each.
	Failed []ProduceFailure
}

// Error implements the error interface.
func (e *ProduceError) Error() string {
	if len(e.Failed) == 0 {
		return "kafka.producer.partial_failure: no details"
	}

	return fmt.Sprintf("kafka.producer.partial_failure: %d message(s) not sent, first: %v",
		len(e.Failed), e.Failed[0].Err)
}

// Unwrap returns the per-message failure causes.
func (e *ProduceError) Unwrap() []error {
	causes := make([]error, 0, len(e.Failed))
	for _, f := range e.Failed {
		causes = append(causes, f.Err)
	}

	return causes
}
