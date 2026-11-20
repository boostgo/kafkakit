package kafkakit

import (
	"errors"
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
)

// BalanceStrategy is a client-side partition assignment strategy.
type BalanceStrategy string

const (
	// BalanceStrategySticky keeps assignments as stable as possible across rebalances.
	BalanceStrategySticky BalanceStrategy = "STICKY"
	// BalanceStrategyRoundRobin assigns partitions round-robin across members.
	BalanceStrategyRoundRobin BalanceStrategy = "ROUND_ROBIN"
	// BalanceStrategyRange assigns contiguous partition ranges to members. Default.
	BalanceStrategyRange BalanceStrategy = "RANGE"
)

const (
	// OffsetNewest stands for the log head offset, i.e. the offset that will be
	// assigned to the next message that will be produced to the partition. You
	// can send this to a client's GetOffset method to get this offset, or when
	// calling ConsumePartition to start consuming new messages.
	OffsetNewest int64 = -1

	// OffsetOldest stands for the oldest offset available on the broker for a
	// partition. You can send this to a client's GetOffset method to get this
	// offset, or when calling ConsumePartition to start consuming from the
	// oldest offset that is still available on the broker.
	OffsetOldest int64 = -2
)

// Isolation levels for [WithConsumerIsolationLevel].
const (
	// ReadUncommitted returns all messages, including those of aborted transactions. Default.
	ReadUncommitted int8 = iota
	// ReadCommitted hides messages that are part of aborted transactions.
	ReadCommitted
)

// ConsumerOptions holds the settings of a [Consumer]. Use the With*
// [ConsumerOption] helpers rather than filling it directly. Most fields map
// one-to-one to sarama's Consumer config section.
type ConsumerOptions struct {
	// Name identifies the consumer in logs. A random UUID is appended.
	Name string
	// GroupID is the consumer group ID. Required.
	GroupID string
	// GroupInstanceID enables KIP-345 static membership (group.instance.id);
	// empty means dynamic membership.
	GroupInstanceID string
	// Topics to subscribe to. Required.
	Topics []string
	// HandlerTimeout bounds the processing of a single message or batch
	// (0 = no timeout).
	HandlerTimeout time.Duration

	// HandlerCancelOnRevoke controls the handler ctx when the session ends.
	// false (default): the ctx is not canceled by rebalance or Close.
	// true: legacy behavior, the ctx is canceled by rebalance and Close.
	HandlerCancelOnRevoke bool

	// Validate, when set, validates structs decoded by Parse.
	Validate *validator.Validate
	// OnConsumerClose is called once when the group dies unexpectedly.
	OnConsumerClose OnConsumerClose

	// The timeout used to detect consumer failures when using Kafka's group management facility.
	// The consumer sends periodic heartbeats to indicate its liveness to the broker.
	// If no heartbeats are received by the broker before the expiration of this session timeout,
	// then the broker will remove this consumer from the group and initiate a rebalance.
	// Note that the value must be in the allowable range as configured in the broker configuration
	// by `group.min.session.timeout.ms` and `group.max.session.timeout.ms`.
	SessionTimeout time.Duration // default: 30s

	// The expected time between heartbeats to the consumer coordinator when using Kafka's group
	// management facilities. Heartbeats are used to ensure that the consumer's session stays active and
	// to facilitate rebalancing when new consumers join or leave the group.
	// The value must be set lower than Consumer.Group.HandlerContext.WithTimeout, but typically should be set no
	// higher than 1/3 of that value.
	// It can be adjusted even lower to control the expected time for normal rebalances.
	HeartbeatInterval time.Duration // default: 10s

	// BalanceStrategies is the priority-ordered list of client-side consumer group
	// balancing strategies that will be offered to the coordinator. The first
	// strategy that all group members support will be chosen by the leader.
	BalanceStrategies []BalanceStrategy // default: [ BalanceStrategyRange ]

	// The maximum allowed time for each worker to join the group once a rebalance has begun.
	// This is basically a limit on the amount of time needed for all tasks to flush any pending
	// data and commit offsets. If the timeout is exceeded, then the worker will be removed from
	// the group, which will cause offset commit failures (default 60s).
	BalanceTimeout time.Duration // default: 60s

	// When a new consumer joins a consumer group the set of consumers attempt to "rebalance"
	// the load to assign partitions to each consumer. If the set of consumers changes while
	// this assignment is taking place the rebalance will fail and retry. This setting controls
	// the maximum number of attempts before giving up.
	BalanceMaxRetries int // default: 3

	// Backoff time between retries during rebalance.
	BalanceRetryBackoff time.Duration // default: 2s

	// If true, consumer offsets will be automatically reset to configured Initial value
	// if the fetched consumer offset is out of range of available offsets. Out of range
	// can happen if the data has been deleted from the server, or during situations of
	// under-replication where a replica does not have all the data yet. It can be
	// dangerous to reset the offset automatically, particularly in the latter case
	//
	// default: true, as in sarama. On reset the group reads from InitialOffset
	// (OffsetNewest by default: unread data is skipped). sarama performs the
	// reset without logging, so a Warn
	// (kafka.consumer.reset_invalid_offsets_enabled) is written when a consumer
	// starts with this option enabled.
	ResetInvalidOffsets bool // default: true

	// How long to wait after a failing to read from a partition before trying again.
	RetryBackoff time.Duration // default: 2s

	// Called to compute backoff time dynamically. Useful for implementing
	// more sophisticated backoff strategies. This takes precedence over
	// `Backoff` if set.
	RetryBackoffFunc func(retries int) time.Duration

	// The minimum number of message bytes to fetch in a request - the broker
	// will wait until at least this many are available. The default is 1,
	// as 0 causes the consumer to spin when no messages are available.
	// Equivalent to the JVM's `fetch.min.bytes`.
	FetchMin int32 // default: 1

	// The default number of message bytes to fetch from the broker in each
	// request. This should be larger than the majority of
	// your messages, or else the consumer will spend a lot of time
	// negotiating sizes and not actually consuming. Similar to the JVM's
	// `fetch.message.max.bytes`.
	FetchDefault int32 // default: 1MB

	// The maximum number of message bytes to fetch from the broker in a
	// single request. Messages larger than this will return
	// ErrMessageTooLarge and will not be consumable, so you must be sure
	// this is at least as large as your largest message. 0 means
	// no limit. Similar to the JVM's `fetch.message.max.bytes`. The
	// global `sarama.MaxResponseSize` still applies.
	FetchMax int32 // default: 10MB

	// The maximum amount of time the broker will wait for Consumer.Fetch.Min
	// bytes to become available before it returns fewer than that anyways.
	// Do not set it to 0, since 0 causes the consumer to spin when no events are
	// available. 100-500ms is a reasonable range for most cases. Kafka only
	// supports precision up to milliseconds; nanoseconds will be truncated.
	// Equivalent to the JVM's `fetch.max.wait.ms`.
	MaxWaitTime time.Duration // default: 3s

	// The maximum amount of time the consumer expects a message takes to
	// process for the user. If writing to the Messages channel takes longer
	// than this, that partition will stop fetching more messages until it
	// can proceed again.
	// Note that, since the Messages channel is buffered, the actual grace time is
	// (MaxProcessingTime * ChannelBufferSize).
	// If a message is not written to the Messages channel between two ticks
	// of the expiryTicker then a timeout is detected.
	// Using a ticker instead of a timer to detect timeouts should typically
	// result in many fewer calls to Timer functions which may result in a
	// significant performance improvement if many messages are being sent
	// and timeouts are infrequent.
	// The disadvantage of using a ticker instead of a timer is that
	// timeouts will be less accurate. That is, the effective timeout could
	// be between `MaxProcessingTime` and `2 * MaxProcessingTime`. For
	// example, if `MaxProcessingTime` is 100ms then a delay of 180ms
	// between two messages being sent may not be recognized as a timeout.
	MaxProcessingTime time.Duration // default: 3s

	// If enabled, any errors that occurred while consuming are returned on
	// the Errors channel.
	ReturnErrors bool // default: true

	// The initial offset to use if no offset was previously committed.
	// Should be OffsetNewest or OffsetOldest.
	InitialOffset int64 // default: OffsetNewest

	// Whether or not to auto-commit updated offsets back to the broker.
	AutocommitEnabled bool // default: true

	// How frequently to commit updated offsets. Ineffective unless
	// auto-commit is enabled
	AutocommitInterval time.Duration // default: 1s

	// The total number of times to retry failing commit
	// requests during OffsetManager shutdown.
	RetryMax int // default: 3

	// IsolationLevel support 2 mode:
	// 	- use `ReadUncommitted` (default) to consume and return all messages in message channel
	//	- use `ReadCommitted` to hide messages that are part of an aborted transaction
	IsolationLevel int8 // default: Read Uncommitted

	// client is the shared client (WithConsumerClient); nil means the consumer builds its own.
	client *Client

	// traceHeader is the trace header name, taken from the client by
	// NewConsumer ([Client.TraceHeader]); "" means DefaultTraceHeader.
	traceHeader string
}

// ConsumerOption configures a [Consumer].
type ConsumerOption func(*ConsumerOptions)

func newConsumerOptions() *ConsumerOptions {
	consumerName := "kafka.consumer:" + uuid.NewString()
	return &ConsumerOptions{
		Name: consumerName,

		SessionTimeout:    time.Second * 30,
		HeartbeatInterval: time.Second * 10,

		BalanceStrategies:   []BalanceStrategy{BalanceStrategyRange},
		BalanceTimeout:      time.Second * 60,
		BalanceMaxRetries:   3,
		BalanceRetryBackoff: time.Second * 2,
		ResetInvalidOffsets: true,

		RetryBackoff: time.Second * 2,

		FetchMin:     1,
		FetchDefault: 1 << 20,  // 1MB,
		FetchMax:     10 << 20, // 10MB

		MaxWaitTime:       time.Second * 3,
		MaxProcessingTime: time.Second * 3,
		ReturnErrors:      true,

		InitialOffset: OffsetNewest,

		AutocommitEnabled:  true,
		AutocommitInterval: time.Second * 1,

		RetryMax: 3,

		IsolationLevel: ReadUncommitted,
	}
}

func (o *ConsumerOptions) validate() error {
	if o.GroupID == "" {
		return errors.New("kafka.consumer.group_id_required")
	}

	if len(o.Topics) == 0 {
		return errors.New("kafka.consumer.topics_required")
	}

	return nil
}

// applySaramaConfig writes the Consumer.* section into a config built by the client.
//
// It also checks static membership against the protocol version: sarama
// requires Version >= 2.3.0 for Consumer.Group.InstanceId (KIP-345) and would
// otherwise return a less helpful ConfigurationError from NewConsumerGroup.
func (o *ConsumerOptions) applySaramaConfig(config *sarama.Config) error {
	config.Consumer.Group.ResetInvalidOffsets = o.ResetInvalidOffsets
	config.Consumer.Offsets.Retry.Max = o.RetryMax
	config.Consumer.Group.Session.Timeout = o.SessionTimeout
	config.Consumer.Group.Heartbeat.Interval = o.HeartbeatInterval
	config.Consumer.Group.InstanceId = o.GroupInstanceID // KIP-345 static membership (empty = dynamic)
	config.Consumer.Group.Rebalance.GroupStrategies = o.saramaBalanceStrategies()
	config.Consumer.Group.Rebalance.Timeout = o.BalanceTimeout
	config.Consumer.Group.Rebalance.Retry.Max = o.BalanceMaxRetries
	config.Consumer.Group.Rebalance.Retry.Backoff = o.BalanceRetryBackoff
	config.Consumer.Retry.Backoff = o.RetryBackoff
	config.Consumer.Retry.BackoffFunc = o.RetryBackoffFunc
	config.Consumer.Fetch.Min = o.FetchMin
	config.Consumer.Fetch.Default = o.FetchDefault
	config.Consumer.Fetch.Max = o.FetchMax
	config.Consumer.MaxWaitTime = o.MaxWaitTime
	config.Consumer.MaxProcessingTime = o.MaxProcessingTime
	config.Consumer.Return.Errors = o.ReturnErrors
	config.Consumer.Offsets.AutoCommit.Enable = o.AutocommitEnabled
	config.Consumer.Offsets.AutoCommit.Interval = o.AutocommitInterval
	config.Consumer.Offsets.Initial = o.InitialOffset
	config.Consumer.IsolationLevel = o.saramaIsolationLevel()

	if o.GroupInstanceID != "" && !config.Version.IsAtLeast(sarama.V2_3_0_0) {
		return fmt.Errorf(
			"%w: current version: %s: set client option kafkakit.WithVersion(\"2.3.0\") or newer",
			ErrConsumerGroupInstanceIDVersion,
			config.Version.String(),
		)
	}

	return nil
}

func (o *ConsumerOptions) saramaBalanceStrategies() []sarama.BalanceStrategy {
	strategies := make([]sarama.BalanceStrategy, 0, len(o.BalanceStrategies))
	for _, strategy := range o.BalanceStrategies {
		var saramaStrategy sarama.BalanceStrategy
		switch strategy {
		case BalanceStrategySticky:
			saramaStrategy = sarama.NewBalanceStrategySticky()
		case BalanceStrategyRoundRobin:
			saramaStrategy = sarama.NewBalanceStrategyRoundRobin()
		case BalanceStrategyRange:
			saramaStrategy = sarama.NewBalanceStrategyRange()
		default:
			continue
		}
		strategies = append(strategies, saramaStrategy)
	}
	return strategies
}

func (o *ConsumerOptions) saramaIsolationLevel() sarama.IsolationLevel {
	switch o.IsolationLevel {
	case ReadUncommitted:
		return sarama.ReadUncommitted
	case ReadCommitted:
		return sarama.ReadCommitted
	default:
		return sarama.ReadUncommitted
	}
}

// WithConsumerName sets the consumer name used in logs (a random UUID is appended).
func WithConsumerName(name string) ConsumerOption {
	return func(o *ConsumerOptions) {
		o.Name = name + ":" + uuid.NewString()
	}
}

// WithConsumerGroupID sets the consumer group ID. Required.
func WithConsumerGroupID(groupID string) ConsumerOption {
	return func(o *ConsumerOptions) {
		o.GroupID = groupID
	}
}

// WithConsumerGroupInstanceID enables static group membership (KIP-345,
// group.instance.id). The value must be UNIQUE per instance (for example the
// hostname or pod name). Empty means dynamic membership (default). Requires
// [WithVersion]("2.3.0") or newer on the client.
func WithConsumerGroupInstanceID(instanceID string) ConsumerOption {
	return func(o *ConsumerOptions) {
		o.GroupInstanceID = instanceID
	}
}

// WithConsumerTopics sets the topics to subscribe to. Required. An empty call is ignored.
func WithConsumerTopics(topics ...string) ConsumerOption {
	return func(o *ConsumerOptions) {
		if len(topics) == 0 {
			return
		}

		o.Topics = topics
	}
}

// WithConsumerSessionTimeout sets ConsumerOptions.SessionTimeout.
func WithConsumerSessionTimeout(timeout time.Duration) ConsumerOption {
	return func(o *ConsumerOptions) {
		o.SessionTimeout = timeout
	}
}

// WithConsumerHeartbeatInterval sets ConsumerOptions.HeartbeatInterval.
func WithConsumerHeartbeatInterval(interval time.Duration) ConsumerOption {
	return func(o *ConsumerOptions) {
		o.HeartbeatInterval = interval
	}
}

// WithConsumerBalanceStrategies sets the priority-ordered balance strategies. An empty call is ignored.
func WithConsumerBalanceStrategies(strategies ...BalanceStrategy) ConsumerOption {
	return func(o *ConsumerOptions) {
		if len(strategies) == 0 {
			return
		}

		o.BalanceStrategies = strategies
	}
}

// WithConsumerBalanceTimeout sets ConsumerOptions.BalanceTimeout.
func WithConsumerBalanceTimeout(timeout time.Duration) ConsumerOption {
	return func(o *ConsumerOptions) {
		o.BalanceTimeout = timeout
	}
}

// WithConsumerBalanceMaxRetries sets ConsumerOptions.BalanceMaxRetries.
func WithConsumerBalanceMaxRetries(retries int) ConsumerOption {
	return func(o *ConsumerOptions) {
		o.BalanceMaxRetries = retries
	}
}

// WithConsumerBalanceRetryBackoff sets ConsumerOptions.BalanceRetryBackoff.
func WithConsumerBalanceRetryBackoff(backoff time.Duration) ConsumerOption {
	return func(o *ConsumerOptions) {
		o.BalanceRetryBackoff = backoff
	}
}

// WithConsumerResetInvalidOffsets sets ConsumerOptions.ResetInvalidOffsets.
func WithConsumerResetInvalidOffsets(resetInvalidOffsets bool) ConsumerOption {
	return func(o *ConsumerOptions) {
		o.ResetInvalidOffsets = resetInvalidOffsets
	}
}

// WithConsumerRetryBackoff sets ConsumerOptions.RetryBackoff.
func WithConsumerRetryBackoff(backoff time.Duration) ConsumerOption {
	return func(o *ConsumerOptions) {
		o.RetryBackoff = backoff
	}
}

// WithConsumerRetryBackoffFunc sets ConsumerOptions.RetryBackoffFunc.
func WithConsumerRetryBackoffFunc(fn func(retries int) time.Duration) ConsumerOption {
	return func(o *ConsumerOptions) {
		o.RetryBackoffFunc = fn
	}
}

// WithConsumerFetchMin sets ConsumerOptions.FetchMin.
func WithConsumerFetchMin(min int32) ConsumerOption {
	return func(o *ConsumerOptions) {
		o.FetchMin = min
	}
}

// WithConsumerFetchDefault sets ConsumerOptions.FetchDefault.
func WithConsumerFetchDefault(defaultValue int32) ConsumerOption {
	return func(o *ConsumerOptions) {
		o.FetchDefault = defaultValue
	}
}

// WithConsumerFetchMax sets ConsumerOptions.FetchMax.
func WithConsumerFetchMax(max int32) ConsumerOption {
	return func(o *ConsumerOptions) {
		o.FetchMax = max
	}
}

// WithConsumerMaxWaitTime sets ConsumerOptions.MaxWaitTime.
func WithConsumerMaxWaitTime(maxWaitTime time.Duration) ConsumerOption {
	return func(o *ConsumerOptions) {
		o.MaxWaitTime = maxWaitTime
	}
}

// WithConsumerMaxProcessingTime sets ConsumerOptions.MaxProcessingTime.
func WithConsumerMaxProcessingTime(maxProcessingTime time.Duration) ConsumerOption {
	return func(o *ConsumerOptions) {
		o.MaxProcessingTime = maxProcessingTime
	}
}

// WithConsumerReturnErrors sets ConsumerOptions.ReturnErrors.
func WithConsumerReturnErrors(returnErrors bool) ConsumerOption {
	return func(o *ConsumerOptions) {
		o.ReturnErrors = returnErrors
	}
}

// WithConsumerAutocommitEnabled enables or disables periodic offset auto-commit.
func WithConsumerAutocommitEnabled(autocommitEnabled bool) ConsumerOption {
	return func(o *ConsumerOptions) {
		o.AutocommitEnabled = autocommitEnabled
	}
}

// WithConsumerAutocommitInterval sets the auto-commit interval.
func WithConsumerAutocommitInterval(autocommitInterval time.Duration) ConsumerOption {
	return func(o *ConsumerOptions) {
		o.AutocommitInterval = autocommitInterval
	}
}

// WithConsumerInitialOffset sets the offset used when the group has no committed offset (OffsetNewest or OffsetOldest).
func WithConsumerInitialOffset(initialOffset int64) ConsumerOption {
	return func(o *ConsumerOptions) {
		o.InitialOffset = initialOffset
	}
}

// WithConsumerRetryMax sets ConsumerOptions.RetryMax.
func WithConsumerRetryMax(max int) ConsumerOption {
	return func(o *ConsumerOptions) {
		o.RetryMax = max
	}
}

// WithConsumerIsolationLevel sets the isolation level (ReadUncommitted or ReadCommitted).
func WithConsumerIsolationLevel(isolationLevel int8) ConsumerOption {
	return func(o *ConsumerOptions) {
		o.IsolationLevel = isolationLevel
	}
}

// WithConsumerHandlerTimeout bounds the processing time of a single message or batch.
func WithConsumerHandlerTimeout(timeout time.Duration) ConsumerOption {
	return func(o *ConsumerOptions) {
		o.HandlerTimeout = timeout
	}
}

// WithConsumerHandlerCancelOnRevoke selects the handler ctx behavior.
// true (legacy): rebalance and Close cancel the handler ctx, and the batch
// tail is passed to the handler (with a canceled ctx) when the session ends.
// false (default): the handler ctx is not canceled (context.WithoutCancel of
// the session context); the batch tail is neither processed nor marked when
// the session ends and is redelivered (at-least-once). HandlerTimeout applies
// in both modes.
func WithConsumerHandlerCancelOnRevoke(cancel bool) ConsumerOption {
	return func(o *ConsumerOptions) {
		o.HandlerCancelOnRevoke = cancel
	}
}

// WithConsumerOnClose sets a "consumer died" callback. It is called when the
// group's Errors() delivers sarama.ErrOutOfBrokers, ErrClosedConsumerGroup or
// ErrClosedClient, at most once per Consumer lifetime. It is NOT called on a
// regular Consumer.Close, since the caller initiated that. The callback runs
// on the error listener goroutine: do not block in it and do not call
// Consumer.Close from it (Close waits for that goroutine to exit).
func WithConsumerOnClose(onClose OnConsumerClose) ConsumerOption {
	return func(o *ConsumerOptions) {
		o.OnConsumerClose = onClose
	}
}

// WithConsumerValidate sets the validator used by Parse.
func WithConsumerValidate(validate *validator.Validate) ConsumerOption {
	return func(o *ConsumerOptions) {
		o.Validate = validate
	}
}

// WithConsumerClient builds the consumer group on top of the shared sarama
// client of client instead of a dedicated connection (by default every
// consumer opens its own sarama.Client). Consumer.Close does NOT close the
// shared client; the caller owns it.
//
// sarama uses the client's own config, so the Consumer section must be set
// when the client is created ([WithClientConsumerOptions]), and consumer
// options that affect sarama (timeouts, offsets, strategies, ...) must match
// it: a mismatch yields [ErrSharedClientConfig]. GroupID, Topics, Name,
// Validate, HandlerTimeout and OnConsumerClose are set on the consumer as
// usual.
func WithConsumerClient(client *Client) ConsumerOption {
	return func(o *ConsumerOptions) {
		o.client = client
	}
}
