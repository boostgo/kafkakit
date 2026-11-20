package kafkakit

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

// Required acknowledgement levels for [WithProducerRequiredAcks].
const (
	// NoResponse doesn't send any response, the TCP ACK is all you get.
	NoResponse = 0
	// WaitForLocal waits for only the local commit to succeed before responding.
	WaitForLocal = 1
	// WaitForAll waits for all in-sync replicas to commit before responding.
	// The minimum number of in-sync replicas is configured on the broker via
	// the `min.insync.replicas` configuration key.
	WaitForAll = -1
)

// Producer sends messages to Kafka.
//
// All implementations are safe for concurrent use: Produce may be called from
// many goroutines (a worker pool) at once. The passed *ProducerMessage values
// are never mutated, so sharing a header map between goroutines is safe.
//
// If ctx carries a trace ID under [TraceKey] (string or uuid.UUID), it is
// added to every message as the trace header ([Client.TraceHeader]:
// [DefaultTraceHeader] or the name set with [WithTraceHeader]), overriding a
// message header of the same name.
type Producer interface {
	Produce(ctx context.Context, messages ...*ProducerMessage) error
}

// CloseProducer closes a producer created by this library ([NewSyncProducer],
// [NewAsyncProducer], [Client.NewSyncProducer], [Client.NewAsyncProducer]).
//
// The constructors return the [Producer] interface, which has no Close method,
// so closing goes through this function. The async producer flushes its buffer
// and waits for the outcome of every accepted message (see the constructor's
// godoc for the returned error); the sync producer closes its broker
// connections. For Producer implementations without a Close() error method
// (service mocks, third-party implementations) it returns nil. Calling it more
// than once is safe.
func CloseProducer(p Producer) error {
	closer, ok := p.(interface{ Close() error })
	if !ok {
		return nil
	}

	return closer.Close()
}

// traceIDFromContext extracts the trace ID from ctx. string and uuid.UUID are
// supported; any other type yields an empty string.
func traceIDFromContext(ctx context.Context) string {
	value := ctx.Value(TraceKey)
	if value == nil {
		return ""
	}

	switch traceID := value.(type) {
	case string:
		return traceID
	case uuid.UUID:
		return traceID.String()
	default:
		return ""
	}
}

// buildProducerConfig builds a sarama.Config from client and producer options.
// It is the shared assembly point for NewSyncProducer and NewAsyncProducer, so
// that their fields never diverge.
//
// sync=true forces Producer.Return.Successes=true: sarama.SyncProducer
// requires it structurally (verifyProducerConfig); it is not a matter of the
// ProducerOptions.ReturnSuccesses default. forceReturnSuccessesWarn=true means
// the caller explicitly passed WithProducerReturnSuccesses(false) and it had to
// be ignored; NewSyncProducer logs this as a warning.
func buildProducerConfig(client *Client, options *ProducerOptions, sync bool) (
	config *sarama.Config, forceReturnSuccessesWarn bool, err error,
) {
	config = client.GetConfig()

	forceReturnSuccessesWarn, err = options.applySaramaConfig(config, sync)
	if err != nil {
		return nil, false, err
	}

	return config, forceReturnSuccessesWarn, nil
}

// applySaramaConfig writes the Producer section into config. Shared by the
// producer constructors and by the Producer section of a shared client.
func (o *ProducerOptions) applySaramaConfig(config *sarama.Config, sync bool) (
	forceReturnSuccessesWarn bool, err error,
) {
	if err := validateCompressionVersion(o.Compression, config.Version); err != nil {
		return false, err
	}

	config.Producer.MaxMessageBytes = o.MaxMessageBytes
	config.Producer.RequiredAcks = sarama.RequiredAcks(o.RequiredAcks)
	config.Producer.Timeout = o.Timeout
	config.Producer.Idempotent = o.Idempotent
	config.Producer.Return.Successes = o.ReturnSuccesses
	config.Producer.Return.Errors = o.ReturnErrors
	config.Producer.Flush.Bytes = o.FlushBytes
	config.Producer.Flush.Messages = o.FlushMessages
	config.Producer.Flush.Frequency = o.FlushFrequency
	config.Producer.Flush.MaxMessages = o.FlushMaxMessages
	config.Producer.Retry.Max = o.RetryMax
	config.Producer.Retry.Backoff = o.RetryBackoff
	config.Producer.Retry.BackoffFunc = o.RetryBackoffFunc
	config.Producer.Retry.MaxBufferLength = o.RetryMaxBufferLength
	config.Producer.Retry.MaxBufferBytes = o.RetryMaxBufferBytes
	config.Producer.Compression = o.Compression.sarama()
	config.Producer.CompressionLevel = o.CompressionLevel

	if o.DeliveryCallback != nil {
		// Without Return.Successes the callback never learns about successes,
		// without Return.Errors it never learns about failures.
		config.Producer.Return.Successes = true
		config.Producer.Return.Errors = true
	}

	if sync {
		forceReturnSuccessesWarn = !o.ReturnSuccesses && o.returnSuccessesSet
		config.Producer.Return.Successes = true
	}

	return forceReturnSuccessesWarn, nil
}

type syncProducer struct {
	producer    sarama.SyncProducer
	callback    func(msg *ProducerMessage, err error)
	traceHeader string // "" means DefaultTraceHeader
}

// NewSyncProducer creates a synchronous producer: Produce returns once the
// brokers have acknowledged the whole batch (according to RequiredAcks).
//
// ProducerOptions.ReturnSuccesses defaults to false, but it is forced to true
// here because sarama.SyncProducer requires it structurally
// (verifyProducerConfig). An explicit WithProducerReturnSuccesses(false) is
// logged as a warning and ignored.
//
// [WithDeliveryCallback] is supported: after SendMessages returns, the
// callback is called once per message of the batch, on the calling goroutine
// (err == nil means delivered). On partial failure Produce returns an error
// that wraps both [ErrProducerSend] and a [*ProduceError]. Close it with
// [CloseProducer].
func NewSyncProducer(client *Client, opts ...ProducerOption) (Producer, error) {
	options := newProducerOptions()

	for _, o := range opts {
		o(options)
	}

	if err := options.validate(); err != nil {
		return nil, err
	}

	config, warn, err := buildProducerConfig(client, options, true)
	if err != nil {
		return nil, err
	}

	if warn {
		client.logger.
			Warn().
			Str("namespace", "kafka.sync_producer").
			Msg("WithProducerReturnSuccesses(false) ignored: sync producer requires Return.Successes=true")
	}

	var producer sarama.SyncProducer
	if options.client != nil {
		producer, err = newSharedSyncProducer(options)
	} else {
		producer, err = sarama.NewSyncProducer(client.brokers, config)
	}

	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrProducerNew, err)
	}

	return &syncProducer{
		producer:    producer,
		callback:    options.DeliveryCallback,
		traceHeader: client.TraceHeader(),
	}, nil
}

// Close closes the producer. After Close, Produce returns an error
// (sarama.ErrClosedClient in the chain). Calling it more than once is safe.
func (p *syncProducer) Close() error {
	return p.producer.Close()
}

// Produce is safe for concurrent use: sarama.SyncProducer allows concurrent
// SendMessages and the wrapper has no mutable state.
func (p *syncProducer) Produce(ctx context.Context, messages ...*ProducerMessage) error {
	select {
	case <-ctx.Done():
		return fmt.Errorf("%w: %w", ErrProducerSend, ctx.Err())
	default:
	}

	if len(messages) == 0 {
		return nil
	}

	err := p.producer.SendMessages(adaptSaramaMessages(messages, p.traceHeader, traceIDFromContext(ctx)))
	p.notify(messages, err)

	if err != nil {
		return wrapSendError(err)
	}

	return nil
}

// wrapSendError wraps a SendMessages error: on partial failure
// (sarama.ProducerErrors) into *ProduceError with the original messages,
// anything else under ErrProducerSend. In every case errors.Is(err,
// ErrProducerSend) and errors.Is against the cause work.
func wrapSendError(err error) error {
	var pErrs sarama.ProducerErrors
	if !errors.As(err, &pErrs) {
		return fmt.Errorf("%w: %w", ErrProducerSend, err)
	}

	failed := make([]ProduceFailure, 0, len(pErrs))
	for _, pErr := range pErrs {
		failed = append(failed, ProduceFailure{
			Message: originalMessage(pErr.Msg),
			Err:     pErr.Err,
		})
	}

	return fmt.Errorf("%w: %w", ErrProducerSend, &ProduceError{Failed: failed})
}

// notify calls the delivery callback with the outcome of SendMessages.
// Per-message outcomes come from sarama.ProducerErrors; an error that is not
// per-message (for example a closed client) applies to every message.
func (p *syncProducer) notify(messages []*ProducerMessage, sendErr error) {
	if p.callback == nil {
		return
	}

	failed := make(map[*ProducerMessage]error)

	var pErrs sarama.ProducerErrors
	if errors.As(sendErr, &pErrs) {
		for _, pErr := range pErrs {
			if orig, ok := pErr.Msg.Metadata.(*ProducerMessage); ok {
				failed[orig] = pErr.Err
			}
		}
	} else if sendErr != nil {
		for _, m := range messages {
			failed[m] = sendErr
		}
	}

	for _, m := range messages {
		p.callback(m, failed[m])
	}
}

// maxCloseErrors is how many delivery failure causes asyncProducer keeps for
// the error returned by Close.
const maxCloseErrors = 16

type asyncProducer struct {
	producer sarama.AsyncProducer
	logger   zerolog.Logger
	callback func(msg *ProducerMessage, err error)

	traceHeader string // "" means DefaultTraceHeader

	done      chan struct{} // closed at the start of Close
	wg        sync.WaitGroup
	closeOnce sync.Once
	closeErr  error

	// mu: Produce holds RLock while writing to Input(), Close takes Lock after
	// close(done), so AsyncClose never overlaps with a send.
	mu sync.RWMutex

	closing atomic.Bool // Close has started: failures from now on count towards Close's result

	failMu    sync.Mutex
	failed    int
	failCause []error
}

// NewAsyncProducer creates an asynchronous producer: Produce returns as soon
// as the messages are accepted into the producer's buffer.
//
// ProducerOptions.ReturnSuccesses defaults to false and, unlike
// NewSyncProducer, is applied as is. Delivery errors are logged.
//
// [WithDeliveryCallback] forces Return.Successes and Return.Errors to true and
// calls the callback for every message. The callback runs concurrently on
// sarama's reader goroutines (successes and errors are read by different
// goroutines); do not block in it for long, it slows down delivery.
//
// Close it with [CloseProducer]: it flushes the buffer and waits for the
// outcome of every accepted message. If some messages were not delivered, an
// error is returned (errors.Is(err, ErrProducerSend); causes are reachable via
// errors.Is/As). Without ReturnErrors and without a callback there is no way to
// observe failures, and Close returns nil.
func NewAsyncProducer(client *Client, opts ...ProducerOption) (Producer, error) {
	options := newProducerOptions()

	for _, o := range opts {
		o(options)
	}

	if err := options.validate(); err != nil {
		return nil, err
	}

	config, _, err := buildProducerConfig(client, options, false)
	if err != nil {
		return nil, err
	}

	var producer sarama.AsyncProducer
	if options.client != nil {
		producer, err = newSharedAsyncProducer(options)
	} else {
		producer, err = sarama.NewAsyncProducer(client.brokers, config)
	}

	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrProducerNew, err)
	}

	p := &asyncProducer{
		producer: producer,
		logger:   client.logger.With().Str("namespace", "kafka.async_producer").Logger(),
		callback: options.DeliveryCallback,
		done:     make(chan struct{}),

		traceHeader: client.TraceHeader(),
	}
	p.start()

	return p, nil
}

// start launches the Successes() and Errors() readers. sarama closes both
// channels at the end of shutdown (even with Return.*=false), so the readers
// live until the channels are closed rather than until done is signaled;
// otherwise delivery outcomes during Close would be lost.
func (p *asyncProducer) start() {
	p.wg.Add(2)

	go p.drainSuccesses()
	go p.drainErrors()
}

// Close stops accepting new Produce calls, waits for those already started,
// flushes sarama's buffer and waits for the outcome of every accepted message
// (and the callback call). It is idempotent and safe from any goroutine;
// repeated calls wait for the first one and return the same result. Produce
// after Close has started returns ErrProducerSend + ErrProducerClosed.
//
// sarama.AsyncProducer.Close() is not used: it drains Successes and Errors
// itself, racing with our readers.
func (p *asyncProducer) Close() error {
	p.closeOnce.Do(func() {
		p.closing.Store(true)
		close(p.done) // wakes up Produce calls blocked on Input()

		p.mu.Lock() // wait for in-progress Produce calls to return
		p.producer.AsyncClose()
		p.mu.Unlock()

		p.wg.Wait()

		p.closeErr = p.flushError()
	})

	return p.closeErr
}

func (p *asyncProducer) flushError() error {
	p.failMu.Lock()
	defer p.failMu.Unlock()

	if p.failed == 0 {
		return nil
	}

	return fmt.Errorf("%w: %d message(s) not delivered on close: %w",
		ErrProducerSend, p.failed, errors.Join(p.failCause...))
}

// Produce is safe for concurrent use: messages are not mutated, writing to
// Input() is safe from many goroutines, and the race with Close is prevented
// by an RWMutex.
//
// If ctx is canceled in the middle of a batch, some messages may already be
// accepted into the buffer and will be sent. A cancellation yields
// ErrProducerSend + ctx.Err(); a closed producer yields ErrProducerSend +
// ErrProducerClosed.
func (p *asyncProducer) Produce(ctx context.Context, messages ...*ProducerMessage) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	select {
	case <-ctx.Done():
		return fmt.Errorf("%w: %w", ErrProducerSend, ctx.Err())
	case <-p.done:
		return fmt.Errorf("%w: %w", ErrProducerSend, ErrProducerClosed)
	default:
	}

	if len(messages) == 0 {
		return nil
	}

	traceID := traceIDFromContext(ctx)
	for _, msg := range messages {
		select {
		case p.producer.Input() <- msg.saramaMessage(p.traceHeader, traceID):
		case <-ctx.Done():
			return fmt.Errorf("%w: %w", ErrProducerSend, ctx.Err())
		case <-p.done:
			return fmt.Errorf("%w: %w", ErrProducerSend, ErrProducerClosed)
		}
	}

	return nil
}

func (p *asyncProducer) drainSuccesses() {
	defer p.wg.Done()

	for msg := range p.producer.Successes() {
		if p.callback != nil {
			p.callback(originalMessage(msg), nil)
		}
	}
}

func (p *asyncProducer) drainErrors() {
	defer p.wg.Done()

	for pErr := range p.producer.Errors() {
		if pErr == nil {
			continue
		}

		p.logger.
			Error().
			Err(pErr.Err).
			Str("topic", pErr.Msg.Topic).
			Int32("partition", pErr.Msg.Partition).
			Bool("closing", p.closing.Load()).
			Msg("kafka.async_producer.send_error")

		if p.closing.Load() {
			p.recordFailure(pErr.Err)
		}

		if p.callback != nil {
			p.callback(originalMessage(pErr.Msg), pErr.Err)
		}
	}
}

func (p *asyncProducer) recordFailure(err error) {
	p.failMu.Lock()
	defer p.failMu.Unlock()

	p.failed++
	if len(p.failCause) < maxCloseErrors {
		p.failCause = append(p.failCause, err)
	}
}

// originalMessage returns the original *ProducerMessage attached to a sarama
// message via Metadata; nil for foreign messages.
func originalMessage(msg *sarama.ProducerMessage) *ProducerMessage {
	orig, _ := msg.Metadata.(*ProducerMessage)

	return orig
}
