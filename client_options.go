package kafkakit

import (
	"crypto/tls"
	"errors"
	"fmt"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
)

// defaultClientID is the prefix of the client ID used when [WithClientID] is
// not set. A random UUID is appended to make every client unique.
const defaultClientID = "kafkakit"

// ClientOptions holds the connection-level settings of a [Client]. Use the
// With* [ClientOption] helpers rather than filling it directly.
type ClientOptions struct {
	// Brokers is the list of bootstrap brokers ("host:port"). Required.
	Brokers []string

	// InnerLogging routes sarama's internal logs to the client's logger at
	// debug level. Note that sarama's logger is process-global.
	InnerLogging bool

	// Version is the Kafka protocol version negotiated with the brokers, in the
	// form "2.8.0" (or "0.11.0.2" for pre-1.0 brokers). An empty string keeps
	// sarama's default (sarama.DefaultVersion, 2.1.0 as of sarama v1.45.x).
	//
	// Raise it deliberately: it switches whole groups of protocol features on
	// and off. For example static group membership (KIP-345,
	// [WithConsumerGroupInstanceID]) requires Version >= 2.3.0, otherwise
	// sarama refuses to create the consumer group.
	Version string // default: "" (sarama.DefaultVersion)

	// How many outstanding requests a connection is allowed to have before
	// sending on it blocks (default 5).
	//
	// Throughput can improve but message ordering is not guaranteed if Producer.Idempotent is disabled
	MaxOpenRequests int // default: 5

	// All three of the below configurations are similar to the
	// `socket.timeout.ms` setting in JVM kafka. All of them default
	// to 30 seconds.
	DialTimeout  time.Duration // How long to wait for the initial connection.
	ReadTimeout  time.Duration // How long to wait for a response.
	WriteTimeout time.Duration // How long to wait for a transmit.

	// KeepAlive specifies the keep-alive period for an active network connection (defaults to 0).
	// If zero or positive, keep-alives are enabled.
	// If negative, keep-alives are disabled.
	KeepAlive time.Duration // default: 0

	// TLSEnabled turns TLS on for broker connections.
	TLSEnabled bool // default: false
	// TLSConfig is the TLS configuration used when TLSEnabled is true.
	TLSConfig *tls.Config // default: nil

	// AuthenticationEnabled turns SASL/PLAIN authentication on.
	AuthenticationEnabled bool
	// Username is the SASL username.
	Username string
	// Password is the SASL password.
	Password string

	// A user-provided string sent with every request to the brokers for logging,
	// debugging, and auditing purposes. Defaults to "kafkakit:<uuid>", but you
	// should set it to something specific to your application.
	ClientID string // default: "kafkakit:<uuid>"

	// A rack identifier for this client. This can be any string value which
	// indicates where this client is physically located.
	// It corresponds with the broker config 'broker.rack'
	RackID string

	// The number of events to buffer in internal and external channels. This
	// permits the producer and consumer to continue processing some messages
	// in the background while user code is working, greatly improving throughput.
	ChannelBufferSize int // default: 256

	// TraceHeader is the name of the message header that carries the trace ID
	// (see [TraceKey]). Producers write it, consumers read it. An empty string
	// means [DefaultTraceHeader].
	TraceHeader string // default: DefaultTraceHeader ("trace-id")

	// Producer/Consumer sections for a shared client, see
	// WithClientProducerOptions / WithClientConsumerOptions.
	sharedProducer     bool
	sharedProducerOpts []ProducerOption
	sharedConsumer     bool
	sharedConsumerOpts []ConsumerOption
}

// ClientOption configures a [Client].
type ClientOption func(*ClientOptions)

func newClientOptions() *ClientOptions {
	return &ClientOptions{
		MaxOpenRequests:   5,
		DialTimeout:       30 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		ClientID:          defaultClientID + ":" + uuid.NewString(),
		ChannelBufferSize: 256,
		TraceHeader:       DefaultTraceHeader,
	}
}

func (o *ClientOptions) validate() error {
	if len(o.Brokers) == 0 {
		return errors.New("kafka.client.brokers_required")
	}

	if o.Version != "" {
		if _, err := sarama.ParseKafkaVersion(o.Version); err != nil {
			return fmt.Errorf("%w: %q: %w", ErrClientVersion, o.Version, err)
		}
	}

	if err := validateTraceHeader(o.TraceHeader); err != nil {
		return err
	}

	return nil
}

// validateTraceHeader checks a trace header name: it must be valid UTF-8 and
// must not contain whitespace or control characters. An empty name is valid
// and means DefaultTraceHeader.
func validateTraceHeader(name string) error {
	if !utf8.ValidString(name) {
		return fmt.Errorf("%w: %q: not valid UTF-8", ErrClientTraceHeader, name)
	}

	for _, r := range name {
		if unicode.IsSpace(r) || unicode.IsControl(r) {
			return fmt.Errorf("%w: %q: whitespace and control characters are not allowed",
				ErrClientTraceHeader, name)
		}
	}

	return nil
}

// traceHeaderOrDefault returns name, or DefaultTraceHeader if name is empty.
func traceHeaderOrDefault(name string) string {
	if name == "" {
		return DefaultTraceHeader
	}

	return name
}

// saramaVersion returns the protocol version for sarama. An empty string (the
// default) keeps sarama.DefaultVersion.
//
// The string has already been checked by validate(), so a parse error here is
// unreachable and is treated as "keep the default".
func (o *ClientOptions) saramaVersion() sarama.KafkaVersion {
	if o.Version == "" {
		return sarama.DefaultVersion
	}

	version, err := sarama.ParseKafkaVersion(o.Version)
	if err != nil {
		return sarama.DefaultVersion
	}

	return version
}

// saramaConfig builds a sarama config from the client options. This is the
// single assembly point: Client.GetConfig() uses it for producers and
// consumers so that client settings are never lost on the way.
func (o *ClientOptions) saramaConfig() *sarama.Config {
	config := sarama.NewConfig()

	config.Version = o.saramaVersion()

	config.Net.MaxOpenRequests = o.MaxOpenRequests
	config.Net.DialTimeout = o.DialTimeout
	config.Net.ReadTimeout = o.ReadTimeout
	config.Net.WriteTimeout = o.WriteTimeout
	config.Net.KeepAlive = o.KeepAlive

	if o.TLSEnabled {
		config.Net.TLS.Enable = true
		config.Net.TLS.Config = o.TLSConfig
	}

	if o.AuthenticationEnabled {
		config.Net.SASL.Enable = true
		config.Net.SASL.User = o.Username
		config.Net.SASL.Password = o.Password
		config.Net.SASL.Handshake = true
		config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	}

	config.ChannelBufferSize = o.ChannelBufferSize
	config.ClientID = o.ClientID
	config.RackID = o.RackID

	return config
}

// WithBrokers sets the bootstrap brokers ("host:port"). Required.
func WithBrokers(brokers []string) ClientOption {
	return func(o *ClientOptions) {
		o.Brokers = brokers
	}
}

// WithVersion sets the Kafka protocol version as "<major>.<minor>.<patch>"
// (for example "2.8.0", "3.6.0") or "0.<minor>.<veryMinor>.<patch>" for
// pre-1.0 brokers (for example "0.11.0.2").
//
// By default no version is set and sarama.DefaultVersion (2.1.0 in sarama
// v1.45.x) is used. Raise it deliberately: the version controls which
// protocol APIs are used.
//
// Minimum versions required by library features:
//   - [WithConsumerGroupInstanceID] (static membership, KIP-345): >= "2.3.0".
//
// An invalid string does NOT panic: [NewClient] returns [ErrClientVersion].
func WithVersion(version string) ClientOption {
	return func(o *ClientOptions) {
		o.Version = version
	}
}

// WithInnerLogging routes sarama's internal logs to the client's logger.
func WithInnerLogging(logging bool) ClientOption {
	return func(o *ClientOptions) {
		o.InnerLogging = logging
	}
}

// WithMaxOpenRequests sets ClientOptions.MaxOpenRequests.
func WithMaxOpenRequests(maxOpenRequests int) ClientOption {
	return func(o *ClientOptions) {
		o.MaxOpenRequests = maxOpenRequests
	}
}

// WithDialTimeout sets how long to wait for the initial connection.
func WithDialTimeout(timeout time.Duration) ClientOption {
	return func(o *ClientOptions) {
		o.DialTimeout = timeout
	}
}

// WithReadTimeout sets how long to wait for a broker response.
func WithReadTimeout(timeout time.Duration) ClientOption {
	return func(o *ClientOptions) {
		o.ReadTimeout = timeout
	}
}

// WithWriteTimeout sets how long to wait for a transmit.
func WithWriteTimeout(timeout time.Duration) ClientOption {
	return func(o *ClientOptions) {
		o.WriteTimeout = timeout
	}
}

// WithKeepAlive sets the TCP keep-alive period (negative disables it).
func WithKeepAlive(keepAlive time.Duration) ClientOption {
	return func(o *ClientOptions) {
		o.KeepAlive = keepAlive
	}
}

// WithTLS enables or disables TLS with the given configuration.
func WithTLS(enabled bool, config *tls.Config) ClientOption {
	return func(o *ClientOptions) {
		o.TLSEnabled = enabled
		o.TLSConfig = config
	}
}

// WithAuthentication enables SASL/PLAIN authentication.
func WithAuthentication(username, password string) ClientOption {
	return func(o *ClientOptions) {
		o.AuthenticationEnabled = true
		o.Username = username
		o.Password = password
	}
}

// WithClientID sets the client ID sent to the brokers. A random UUID is
// appended (clientID + ":" + uuid) so that every client instance is unique.
func WithClientID(clientID string) ClientOption {
	return func(o *ClientOptions) {
		o.ClientID = clientID + ":" + uuid.NewString()
	}
}

// WithRackID sets the rack identifier of the client (matches broker.rack).
func WithRackID(rackID string) ClientOption {
	return func(o *ClientOptions) {
		o.RackID = rackID
	}
}

// WithChannelBufferSize sets the size of sarama's internal channel buffers.
func WithChannelBufferSize(channelBufferSize int) ClientOption {
	return func(o *ClientOptions) {
		o.ChannelBufferSize = channelBufferSize
	}
}

// WithTraceHeader sets the name of the message header that carries the trace
// ID (default [DefaultTraceHeader], "trace-id"). Every producer and consumer
// built on the client uses it: producers write the trace ID from the context
// ([TraceKey]) into this header, consumers read it back into the handler
// context. Use the same name on both sides of a topic.
//
// An empty name keeps the default. A name with whitespace, control characters
// or invalid UTF-8 makes [NewClient] return [ErrClientTraceHeader].
func WithTraceHeader(name string) ClientOption {
	return func(o *ClientOptions) {
		o.TraceHeader = traceHeaderOrDefault(name)
	}
}
