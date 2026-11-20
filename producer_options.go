package kafkakit

import (
	"time"
)

// ProducerOptions holds the settings of a producer. Use the With*
// [ProducerOption] helpers rather than filling it directly. Most fields map
// one-to-one to sarama's Producer config section.
type ProducerOptions struct {
	// The maximum permitted size of a message (defaults to 1000000). Should be
	// set equal to or smaller than the broker's `message.max.bytes`.
	//
	// This is the per-partition batch size BEFORE compression (sarama compares
	// uncompressed bytes against the limit), while the broker's
	// `message.max.bytes` is checked against the COMPRESSED size. Keeping both
	// limits equal is therefore safe: they measure different things on
	// different sides of the network.
	MaxMessageBytes int // default: 1000000

	// The level of acknowledgement reliability needed from the broker (defaults
	// to WaitForAll). Equivalent to the `request.required.acks` setting of the
	// JVM producer.
	RequiredAcks int16 // default: WaitForAll

	// The maximum duration the broker will wait the receipt of the number of
	// RequiredAcks (defaults to 10 seconds). This is only relevant when
	// RequiredAcks is set to WaitForAll or a number > 1. Only supports
	// millisecond resolution, nanoseconds will be truncated. Equivalent to
	// the JVM producer's `request.timeout.ms` setting.
	Timeout time.Duration // default: 10 sec

	// If enabled, the producer will ensure that exactly one copy of each message is
	// written.
	Idempotent bool // default: false

	// If enabled, successfully delivered messages will be returned on the
	// Successes channel.
	//
	// default: false. The async producer would only drain and discard
	// successes, paying a channel hop and a goroutine wake-up per message.
	// Use WithDeliveryCallback to observe outcomes instead.
	//
	// For the SYNC producer (NewSyncProducer) this field is forced to true
	// regardless of the value here: sarama.SyncProducer structurally requires
	// Return.Successes=true. An explicit WithProducerReturnSuccesses(false) on a
	// sync producer is logged as a warning and ignored.
	ReturnSuccesses bool // default: false (forced to true for sync)

	// If enabled, messages that failed to deliver will be returned on the
	// Errors channel, including error (default enabled).
	ReturnErrors bool // default: enabled

	// The best-effort number of bytes needed to trigger a flush. Use the
	// (100 * 1024 * 1024) to set a hard upper limit.
	FlushBytes int

	// The best-effort number of messages needed to trigger a flush. Use
	// `MaxMessages` to set a hard upper limit.
	FlushMessages int

	// The best-effort frequency of flushes. Equivalent to
	// `queue.buffering.max.ms` setting of JVM producer.
	FlushFrequency time.Duration

	// The maximum number of messages the producer will send in a single
	// broker request. Defaults to 0 for unlimited. Similar to
	// `queue.buffering.max.messages` in the JVM producer.
	FlushMaxMessages int

	// The total number of times to retry sending a message (default 3).
	// Similar to the `message.send.max.retries` setting of the JVM producer.
	RetryMax int // default: 3

	// How long to wait for the cluster to settle between retries
	// (default 100ms). Similar to the `retry.backoff.ms` setting of the
	// JVM producer.
	RetryBackoff time.Duration // default: 100ms

	// Called to compute backoff time dynamically. Useful for implementing
	// more sophisticated backoff strategies. This takes precedence over
	// `Backoff` if set.
	RetryBackoffFunc func(retries, maxRetries int) time.Duration

	// The maximum length of the bridging buffer between `input` and `retries` channels
	// in AsyncProducer#retryHandler.
	// The limit is to prevent this buffer from overflowing or causing OOM.
	// Any value between 0 and 4096 is pushed to 4096.
	// A zero or negative value indicates unlimited.
	//
	// default: 4096. With no limit the retry buffer grows without bound while
	// a broker is unavailable and may cause an OOM. 4096 is the value sarama
	// itself bumps any number in (0; 4096] to. Unlimited is still available
	// via an explicit WithProducerRetryMaxBufferLength(0) (or a negative value).
	RetryMaxBufferLength int // default: 4096

	// The maximum total byte size of messages in the bridging buffer between `input`
	// and `retries` channels in AsyncProducer#retryHandler.
	// This limit prevents the buffer from consuming excessive memory.
	// Any value between 0 and 32 MB is pushed to 32 MB.
	// A zero or negative value indicates unlimited.
	//
	// default: 32 MB (33554432), for the same reason as RetryMaxBufferLength.
	RetryMaxBufferBytes int64 // default: 32 MB (33554432)

	// Compression is the record-batch codec: CompressionNone (default),
	// CompressionGZIP, CompressionSnappy, CompressionLZ4, CompressionZSTD.
	//
	// The BATCH is compressed, not the message: without lingering
	// (FlushBytes/FlushFrequency) batches stay small and compression loses
	// most of its benefit.
	//
	// CompressionZSTD requires protocol version >= 2.1.0, CompressionLZ4
	// >= 0.10.0.0 (see WithVersion). sarama.DefaultVersion is already 2.1.0, so
	// both codecs work without an explicit WithVersion; an incompatible
	// codec/version pair makes NewSyncProducer/NewAsyncProducer return
	// ErrProducerCompressionVersion naming the minimum version.
	Compression CompressionCodec // default: CompressionNone

	// CompressionLevel is the compression level. CompressionLevelDefault lets
	// the codec decide.
	CompressionLevel int // default: CompressionLevelDefault

	// DeliveryCallback receives the delivery outcome of every message; see
	// WithDeliveryCallback. nil disables it.
	DeliveryCallback func(msg *ProducerMessage, err error)

	// client is the shared client (WithProducerClient); nil means the producer builds its own.
	client *Client

	// returnSuccessesSet reports that WithProducerReturnSuccesses was called
	// explicitly (whatever the value). NewSyncProducer uses it to tell the
	// default false from a deliberate WithProducerReturnSuccesses(false) and
	// to warn only in the latter case.
	returnSuccessesSet bool
}

// ProducerOption configures a producer.
type ProducerOption func(*ProducerOptions)

func newProducerOptions() *ProducerOptions {
	return &ProducerOptions{
		MaxMessageBytes:      1000000,
		RequiredAcks:         WaitForAll,
		Timeout:              time.Second * 10,
		ReturnSuccesses:      false,
		ReturnErrors:         true,
		RetryMax:             3,
		RetryBackoff:         time.Millisecond * 100,
		RetryMaxBufferLength: 4096,
		RetryMaxBufferBytes:  32 * 1024 * 1024,
		Compression:          CompressionNone,
		CompressionLevel:     CompressionLevelDefault,
	}
}

func (o *ProducerOptions) validate() error {
	return nil
}

// WithProducerMaxMessageBytes sets ProducerOptions.MaxMessageBytes.
func WithProducerMaxMessageBytes(maxMessageBytes int) ProducerOption {
	return func(o *ProducerOptions) {
		o.MaxMessageBytes = maxMessageBytes
	}
}

// WithProducerRequiredAcks sets the required acks level (NoResponse, WaitForLocal, WaitForAll).
func WithProducerRequiredAcks(requiredAcks int16) ProducerOption {
	return func(o *ProducerOptions) {
		o.RequiredAcks = requiredAcks
	}
}

// WithProducerTimeout sets ProducerOptions.Timeout.
func WithProducerTimeout(timeout time.Duration) ProducerOption {
	return func(o *ProducerOptions) {
		o.Timeout = timeout
	}
}

// WithProducerIdempotent enables the idempotent producer.
func WithProducerIdempotent(idempotent bool) ProducerOption {
	return func(o *ProducerOptions) {
		o.Idempotent = idempotent
	}
}

// WithProducerReturnSuccesses sets ProducerOptions.ReturnSuccesses (default
// false). For a sync producer (NewSyncProducer) false is ignored; see the
// ReturnSuccesses field.
func WithProducerReturnSuccesses(returnSuccesses bool) ProducerOption {
	return func(o *ProducerOptions) {
		o.ReturnSuccesses = returnSuccesses
		o.returnSuccessesSet = true
	}
}

// WithProducerReturnErrors sets ProducerOptions.ReturnErrors.
func WithProducerReturnErrors(returnErrors bool) ProducerOption {
	return func(o *ProducerOptions) {
		o.ReturnErrors = returnErrors
	}
}

// WithProducerFlushBytes sets ProducerOptions.FlushBytes.
func WithProducerFlushBytes(flushBytes int) ProducerOption {
	return func(o *ProducerOptions) {
		o.FlushBytes = flushBytes
	}
}

// WithProducerFlushMessages sets ProducerOptions.FlushMessages.
func WithProducerFlushMessages(flushMessages int) ProducerOption {
	return func(o *ProducerOptions) {
		o.FlushMessages = flushMessages
	}
}

// WithProducerFlushFrequency sets ProducerOptions.FlushFrequency (linger).
func WithProducerFlushFrequency(flushFrequency time.Duration) ProducerOption {
	return func(o *ProducerOptions) {
		o.FlushFrequency = flushFrequency
	}
}

// WithProducerFlushMax sets ProducerOptions.FlushMaxMessages.
func WithProducerFlushMax(max int) ProducerOption {
	return func(o *ProducerOptions) {
		o.FlushMaxMessages = max
	}
}

// WithProducerRetryMax sets ProducerOptions.RetryMax.
func WithProducerRetryMax(retryMax int) ProducerOption {
	return func(o *ProducerOptions) {
		o.RetryMax = retryMax
	}
}

// WithProducerRetryBackoff sets ProducerOptions.RetryBackoff.
func WithProducerRetryBackoff(retryBackOff time.Duration) ProducerOption {
	return func(o *ProducerOptions) {
		o.RetryBackoff = retryBackOff
	}
}

// WithProducerRetryBackoffFunc sets ProducerOptions.RetryBackoffFunc.
func WithProducerRetryBackoffFunc(retryBackoffFunc func(retries, maxRetries int) time.Duration) ProducerOption {
	return func(o *ProducerOptions) {
		o.RetryBackoffFunc = retryBackoffFunc
	}
}

// WithProducerRetryMaxBufferLength sets ProducerOptions.RetryMaxBufferLength.
func WithProducerRetryMaxBufferLength(maxBufferLength int) ProducerOption {
	return func(o *ProducerOptions) {
		o.RetryMaxBufferLength = maxBufferLength
	}
}

// WithProducerRetryMaxBufferBytes sets ProducerOptions.RetryMaxBufferBytes.
func WithProducerRetryMaxBufferBytes(maxBufferBytes int64) ProducerOption {
	return func(o *ProducerOptions) {
		o.RetryMaxBufferBytes = maxBufferBytes
	}
}

// WithProducerCompression sets the record-batch codec (default
// CompressionNone). See ProducerOptions.Compression: compression pays off only
// together with lingering (WithProducerFlushBytes/WithProducerFlushFrequency),
// and CompressionZSTD/CompressionLZ4 require protocol version 2.1.0 /
// 0.10.0.0 or newer respectively (WithVersion on the client).
func WithProducerCompression(codec CompressionCodec) ProducerOption {
	return func(o *ProducerOptions) {
		o.Compression = codec
	}
}

// WithProducerCompressionLevel sets the compression level (default
// CompressionLevelDefault: the codec decides). It only has an effect together
// with WithProducerCompression and only for codecs that support levels (for
// example gzip); sarama ignores it for the others.
func WithProducerCompressionLevel(level int) ProducerOption {
	return func(o *ProducerOptions) {
		o.CompressionLevel = level
	}
}

// WithDeliveryCallback sets a delivery outcome callback: it receives the
// original *ProducerMessage passed to Produce and err == nil if the message
// was delivered.
//
// When a callback is set, Return.Successes and Return.Errors are forced to
// true (otherwise outcomes would never arrive), regardless of
// WithProducerReturnSuccesses / WithProducerReturnErrors.
//
// Async producer: the callback runs concurrently on sarama's reader goroutines
// (successes and errors come from different goroutines); do not block in it
// for long, it slows down delivery. CloseProducer returns only after the
// callback has been called for every accepted message.
//
// Sync producer: the callback is called after SendMessages returns, on the
// goroutine that called Produce, once per message of the batch.
func WithDeliveryCallback(fn func(msg *ProducerMessage, err error)) ProducerOption {
	return func(o *ProducerOptions) {
		o.DeliveryCallback = fn
	}
}

// WithProducerClient builds the producer on top of the shared sarama client of
// client instead of a dedicated connection (by default every producer opens
// its own sarama.Client: extra connections and metadata refreshes). Closing
// the producer does NOT close the shared client; the caller owns it
// (Client.Close, after the producers are closed).
//
// sarama uses the client's own config, so the Producer section must be set
// when the client is created ([WithClientProducerOptions]) and the producer
// options must match it: a mismatch yields [ErrSharedClientConfig] rather
// than being silently ignored. Return.Successes/Return.Errors are always true
// on such a client.
func WithProducerClient(client *Client) ProducerOption {
	return func(o *ProducerOptions) {
		o.client = client
	}
}
