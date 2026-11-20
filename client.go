package kafkakit

import (
	"context"
	"fmt"
	"strings"

	"github.com/IBM/sarama"
	"github.com/rs/zerolog"
)

// Trace propagation.
//
// Producers write the trace ID found in the context under [TraceKey] into a
// single message header, named [DefaultTraceHeader] unless the client was
// created with [WithTraceHeader]. Consumers read that header and put the value
// back into the handler context under [TraceKey].
const (
	// DefaultTraceHeader is the name of the message header that carries the
	// trace ID when [WithTraceHeader] is not set.
	DefaultTraceHeader = "trace-id"
	// TraceKey is the context key under which the trace ID is stored. It is a
	// plain string on purpose, so that HTTP, gRPC and Kafka layers can share it.
	TraceKey = "trace_id"
)

// Client is a connection to a Kafka cluster. It owns a [sarama.Client] and is
// the entry point for building producers, consumers and coordinators.
//
// A Client is safe for concurrent use. Close it after every producer and
// consumer built on top of it has been closed.
type Client struct {
	ctx    context.Context
	logger zerolog.Logger

	brokers []string
	options *ClientOptions
	config  *sarama.Config
	client  sarama.Client
}

// NewClient connects to the brokers configured with [WithBrokers] and returns
// a ready-to-use Client. ctx becomes the parent context of consumers created
// via [Client.NewConsumer].
//
// It returns an error wrapping [ErrConnect] when the cluster is unreachable
// and [ErrClientVersion] when [WithVersion] cannot be parsed.
func NewClient(ctx context.Context, logger zerolog.Logger, opts ...ClientOption) (*Client, error) {
	options := newClientOptions()

	for _, o := range opts {
		o(options)
	}

	if err := options.validate(); err != nil {
		return nil, err
	}

	config := options.saramaConfig()
	if err := options.applySharedSections(config); err != nil {
		return nil, err
	}

	if options.InnerLogging {
		sarama.Logger = newInnerLogger(logger)
	}

	client, err := sarama.NewClient(options.Brokers, config)
	if err != nil {
		return nil, fmt.Errorf("%w: %w: brokers: [%s]", ErrConnect, err, strings.Join(options.Brokers, ","))
	}

	return &Client{
		ctx:    ctx,
		logger: logger,

		brokers: options.Brokers,
		options: options,
		config:  config,
		client:  client,
	}, nil
}

// Close closes the underlying sarama client and all broker connections.
func (c *Client) Close() error {
	return c.client.Close()
}

// Client returns the underlying [sarama.Client] for advanced use cases.
func (c *Client) Client() sarama.Client {
	return c.client
}

// Brokers returns the brokers currently known to the client.
func (c *Client) Brokers() []*sarama.Broker {
	return c.client.Brokers()
}

// NewSyncProducer is a shortcut for [NewSyncProducer](c, opts...).
func (c *Client) NewSyncProducer(opts ...ProducerOption) (Producer, error) {
	return NewSyncProducer(c, opts...)
}

// NewAsyncProducer is a shortcut for [NewAsyncProducer](c, opts...).
func (c *Client) NewAsyncProducer(opts ...ProducerOption) (Producer, error) {
	return NewAsyncProducer(c, opts...)
}

// NewConsumer is a shortcut for [NewConsumer] with the context passed to
// [NewClient].
func (c *Client) NewConsumer(opts ...ConsumerOption) (*Consumer, error) {
	return NewConsumer(c.ctx, c, opts...)
}

// TraceHeader returns the name of the message header that carries the trace
// ID for producers and consumers built on this client: the value set with
// [WithTraceHeader], or [DefaultTraceHeader].
func (c *Client) TraceHeader() string {
	if c.options == nil {
		return DefaultTraceHeader
	}

	return traceHeaderOrDefault(c.options.TraceHeader)
}

// GetConfig returns a NEW sarama config built from the same client options as
// the client's own config (including Version). Mutating the returned config is
// safe: it is not attached to the running client. Producers and consumers use
// it as the base for their own sections.
func (c *Client) GetConfig() *sarama.Config {
	return c.options.saramaConfig()
}

type innerLogger struct {
	logger zerolog.Logger
}

// newInnerLogger creates a zerolog-backed logger for the sarama library.
func newInnerLogger(logger zerolog.Logger) sarama.StdLogger {
	return &innerLogger{
		logger: logger.
			With().
			Str("namespace", "kafka.logger").
			Logger(),
	}
}

// NewInnerLogger returns a [sarama.StdLogger] that writes sarama's internal
// logs to logger at debug level.
func NewInnerLogger(logger zerolog.Logger) sarama.StdLogger {
	return newInnerLogger(logger)
}

// EnableInnerLogger installs logger as the global sarama logger.
func EnableInnerLogger(logger sarama.StdLogger) {
	sarama.Logger = logger
}

// Print implements [sarama.StdLogger].
func (l *innerLogger) Print(args ...interface{}) {
	values := make([]string, 0, len(args))
	for _, arg := range args {
		values = append(values, fmt.Sprintf("%s", arg))
	}

	l.logger.
		Debug().
		Str("logger", "SARAMA").
		Strs("args", values).
		Msg("Sarama logger Print")
}

// Printf implements [sarama.StdLogger].
func (l *innerLogger) Printf(format string, v ...interface{}) {
	l.logger.
		Debug().
		Str("logger", "SARAMA").
		Msg(fmt.Sprintf(format, v...))
}

// Println implements [sarama.StdLogger].
func (l *innerLogger) Println(args ...interface{}) {
	values := make([]string, 0, len(args))
	for _, arg := range args {
		values = append(values, fmt.Sprintf("%s", arg))
	}

	l.logger.
		Debug().
		Str("logger", "SARAMA").
		Strs("args", values).
		Msg("Sarama logger Println")
}
