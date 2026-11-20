package kafkakit

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/go-playground/validator/v10"
	"github.com/rs/zerolog"
)

// OnConsumerClose is called when the consumer group dies unexpectedly; see
// [WithConsumerOnClose].
type OnConsumerClose func()

// Backoff parameters for restarting the Consume loop after an error: the pause
// grows exponentially from restartBackoffInitial up to restartBackoffMax and
// is reset after a session ends normally (Consume returned nil: a rebalance).
const (
	restartBackoffInitial = 500 * time.Millisecond
	restartBackoffMax     = 30 * time.Second
)

// clock is the source of backoff pauses; tests replace it with a fake clock.
type clock interface {
	After(d time.Duration) <-chan time.Time
}

type realClock struct{}

func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// Consumer is a Kafka consumer group member. Start it with [Consumer.Consume]
// (one message at a time) or [Consumer.ConsumeBatch] (per-partition batches)
// and stop it with [Consumer.Close].
type Consumer struct {
	ctx    context.Context
	cancel context.CancelFunc
	logger zerolog.Logger

	consumer        sarama.ConsumerGroup
	name            string
	groupID         string
	topics          []string
	handlerTimeout  time.Duration
	cancelOnRevoke  bool
	onConsumerClose OnConsumerClose
	onCloseOnce     sync.Once

	resetInvalidOffsets bool
	initialOffset       int64

	validate    *validator.Validate
	traceHeader string

	clock clock

	mu     sync.Mutex // guards closed and wg.Add against Close
	closed bool
	wg     sync.WaitGroup // Consume loop and Errors() listener goroutines

	loopWG sync.WaitGroup // Consume loop goroutines only (for tests)
	errsWG sync.WaitGroup // Errors() listeners only (for tests)
}

// NewConsumer creates a consumer group member. [WithConsumerGroupID] and
// [WithConsumerTopics] are required. ctx is the parent of the consume loop:
// canceling it stops consumption (prefer [Consumer.Close] for a graceful stop).
func NewConsumer(ctx context.Context, client *Client, opts ...ConsumerOption) (*Consumer, error) {
	options := newConsumerOptions()

	for _, o := range opts {
		o(options)
	}

	if err := options.validate(); err != nil {
		return nil, err
	}

	options.traceHeader = client.TraceHeader()

	config := client.GetConfig()
	if err := options.applySaramaConfig(config); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrConsumerNew, err)
	}

	var (
		group sarama.ConsumerGroup
		err   error
	)

	if options.client != nil {
		group, err = newSharedConsumerGroup(options)
	} else {
		group, err = sarama.NewConsumerGroup(client.brokers, options.GroupID, config)
	}

	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrConsumerNew, err)
	}

	return newConsumerFromGroup(ctx, client.logger, group, options), nil
}

// newConsumerFromGroup builds a Consumer on top of an existing
// sarama.ConsumerGroup; it is the seam used by tests (fake group).
func newConsumerFromGroup(
	ctx context.Context,
	logger zerolog.Logger,
	group sarama.ConsumerGroup,
	options *ConsumerOptions,
) *Consumer {
	ctx, cancel := context.WithCancel(ctx)

	return &Consumer{
		ctx:    ctx,
		cancel: cancel,
		logger: logger.
			With().
			Str("namespace", "kafka.consumer").
			Logger(),

		handlerTimeout: options.HandlerTimeout,
		cancelOnRevoke: options.HandlerCancelOnRevoke,
		consumer:       group,
		name:           options.Name,
		groupID:        options.GroupID,
		topics:         options.Topics,

		onConsumerClose: options.OnConsumerClose,

		resetInvalidOffsets: options.ResetInvalidOffsets,
		initialOffset:       options.InitialOffset,

		validate:    options.Validate,
		traceHeader: traceHeaderOrDefault(options.traceHeader),

		clock: realClock{},
	}
}

// Close stops the consumer: it cancels the context, waits for the
// Consume/ConsumeBatch loop and the Errors() listener to exit, and then closes
// the sarama.ConsumerGroup (final offset commit and LeaveGroup).
//
// Close waits for in-flight processing: the handler context is not canceled
// by it (unless [WithConsumerHandlerCancelOnRevoke] is enabled), so a message
// or batch already in the handler runs to completion. The wait is bounded by
// HandlerTimeout and by ctx; set them so that shutdown fits into your
// deployment's termination grace period. Without HandlerTimeout in-flight
// processing is bounded only by ctx. Calling Close without a prior
// Consume/ConsumeBatch simply closes the group.
//
// Close is idempotent: subsequent calls return nil. If ctx expires before the
// loop exits, the group is still closed (so no sarama goroutines are leaked)
// and an error with ctx.Err() in its chain is returned. Consume/ConsumeBatch
// after Close do nothing (they only log).
//
// Do not call Close from a message handler: it waits for the loop the handler
// is running in.
func (c *Consumer) Close(ctx context.Context) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()

		return nil
	}

	c.closed = true
	c.mu.Unlock()

	c.cancel()

	exited := make(chan struct{})

	go func() {
		c.wg.Wait()
		close(exited)
	}()

	var waitErr error

	select {
	case <-exited:
	case <-ctx.Done():
		waitErr = fmt.Errorf("kafka.consumer.close: loop did not exit: %w", ctx.Err())
	}

	closeErr := c.consumer.Close()
	if closeErr != nil {
		closeErr = fmt.Errorf("kafka.consumer.close: group: %w", closeErr)
	}

	return errors.Join(waitErr, closeErr)
}

// Consume starts consuming messages one by one. It does not block: the loop
// and the error listener run in their own goroutines until ctx is canceled
// or Close is called.
//
// After an error from sarama.ConsumerGroup.Consume the loop pauses before
// restarting: exponentially from 500ms up to 30s, interruptible by stopping
// the consumer. A normal end of session (rebalance) restarts the loop without
// a pause and resets the backoff. sarama.ErrClosedConsumerGroup ends the loop.
func (c *Consumer) Consume(handler ConsumerHandler) {
	groupHandler := newConsumerGroupHandler(
		c.logger,
		c.name+".handler",
		nil,
		nil,
		handler,
		c.handlerTimeout,
		c.cancelOnRevoke,
		c.validate,
		c.traceHeader,
	)

	if !c.run(groupHandler, "kafka.consumer") {
		return
	}

	c.logger.
		Info().
		Str("name", c.name).
		Str("group_id", c.groupID).
		Strs("topics", c.topics).
		Msg("kafka.consumer.start")
}

// run starts the Consume loop and the Errors() listener under the WaitGroup.
// It returns false if the consumer is already closed. prefix is the log
// message prefix (kafka.consumer / kafka.consumer.batch).
func (c *Consumer) run(handler sarama.ConsumerGroupHandler, prefix string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		c.logger.
			Warn().
			Str("name", c.name).
			Msg(prefix + ".start_after_close")

		return false
	}

	c.warnResetInvalidOffsets()

	c.wg.Add(2)
	c.loopWG.Add(1)
	c.errsWG.Add(1)

	go c.consumeLoop(handler, prefix)
	go c.errorsLoop(prefix)

	return true
}

func (c *Consumer) consumeLoop(handler sarama.ConsumerGroupHandler, prefix string) {
	defer c.wg.Done()
	defer c.loopWG.Done()

	delay := time.Duration(0)

	for {
		if c.ctx.Err() != nil {
			c.logger.
				Info().
				Str("name", c.name).
				Strs("topics", c.topics).
				Msg(prefix + ".group_done")

			return
		}

		err := c.consumer.Consume(c.ctx, c.topics, handler)

		switch {
		case err == nil:
			delay = 0 // normal rebalance: sarama expects an immediate restart
		case errors.Is(err, sarama.ErrClosedConsumerGroup):
			c.logger.
				Info().
				Str("name", c.name).
				Strs("topics", c.topics).
				Msg(prefix + ".group_closing")

			return
		case c.ctx.Err() != nil:
			continue // canceled: the check at the top of the loop ends it
		default:
			c.logger.
				Error().
				Err(err).
				Str("name", c.name).
				Strs("topics", c.topics).
				Msg(prefix + ".group_done")

			delay = nextRestartDelay(delay)

			select {
			case <-c.ctx.Done():
				continue
			case <-c.clock.After(delay):
			}
		}

		if c.ctx.Err() != nil {
			continue
		}

		c.logger.
			Info().
			Str("name", c.name).
			Strs("topics", c.topics).
			Msg(prefix + ".group_restarting")
	}
}

func nextRestartDelay(prev time.Duration) time.Duration {
	if prev <= 0 {
		return restartBackoffInitial
	}

	return min(prev*2, restartBackoffMax)
}

// errorsLoop reads the group's Errors() until the channel is closed or the
// consumer stops. An error does not stop the listener: the first one usually
// arrives on the very first rebalance, and commit errors that follow matter.
// The exception is ErrConsumerAppLifeDone (application shutdown).
func (c *Consumer) errorsLoop(prefix string) {
	defer c.wg.Done()
	defer c.errsWG.Done()

	for {
		select {
		case err, ok := <-c.consumer.Errors():
			if !ok {
				return
			}

			if errors.Is(err, ErrConsumerAppLifeDone) {
				c.logger.
					Info().
					Msg(prefix + ".stop.cause_app_life_done")

				return
			}

			c.logger.
				Error().
				Err(err).
				Str("name", c.name).
				Strs("topics", c.topics).
				Msg(prefix + ".group_errors")

			if errors.Is(err, sarama.ErrOutOfBrokers) ||
				errors.Is(err, sarama.ErrClosedConsumerGroup) ||
				errors.Is(err, sarama.ErrClosedClient) {
				c.notifyConsumerClosed()
			}
		case <-c.ctx.Done():
			c.logger.
				Info().
				Str("name", c.name).
				Strs("topics", c.topics).
				Msg(prefix + ".life_stopped")

			return
		}
	}
}

// notifyConsumerClosed calls OnConsumerClose at most once per consumer
// lifetime, no matter how many matching errors arrive.
func (c *Consumer) notifyConsumerClosed() {
	if c.onConsumerClose == nil {
		return
	}

	c.onCloseOnce.Do(c.onConsumerClose)
}

// warnResetInvalidOffsets warns that on "offset out of range" the group will
// silently jump to InitialOffset: sarama v1.45 does it inside the claim with
// no log and no callback, so there is no reliable way to observe the reset.
func (c *Consumer) warnResetInvalidOffsets() {
	if !c.resetInvalidOffsets {
		return
	}

	initial := "oldest"
	if c.initialOffset == OffsetNewest {
		initial = "newest"
	}

	c.logger.
		Warn().
		Str("name", c.name).
		Str("group_id", c.groupID).
		Strs("topics", c.topics).
		Str("initial_offset", initial).
		Msg("kafka.consumer.reset_invalid_offsets_enabled: " +
			"on offset out of range the group silently jumps to initial_offset " +
			"(data may be skipped or re-read); disable with WithConsumerResetInvalidOffsets(false)")
}

func readConsumerMessageHeader(message *sarama.ConsumerMessage, name string) string {
	nameBlob := convertBytesFromString(name)

	for _, header := range message.Headers {
		if bytes.Equal(header.Key, nameBlob) {
			return convertStringFromBytes(header.Value)
		}
	}

	return ""
}
