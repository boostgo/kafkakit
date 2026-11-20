package kafkakit

import (
	"time"

	"github.com/IBM/sarama"
	"github.com/go-playground/validator/v10"
	"github.com/rs/zerolog"
)

// ConsumerHandler processes a single message. A returned error is logged;
// the message is not retried by the library, so mark it (or not) yourself.
type ConsumerHandler func(*ConsumerContext) error

// ConsumerGroupHandler is the [sarama.ConsumerGroupHandler] used by
// [Consumer.Consume]. It is exported for advanced use; most callers never
// need it directly.
type ConsumerGroupHandler struct {
	logger zerolog.Logger

	name    string
	setup   func(sarama.ConsumerGroupSession) error
	cleanup func(sarama.ConsumerGroupSession) error
	handler ConsumerHandler
	timeout time.Duration

	cancelOnRevoke bool

	validate    *validator.Validate
	traceHeader string
}

// newConsumerGroupHandler creates [sarama.ConsumerGroupHandler] interface implementation object.
//
// Provide 3 methods: handler, setup and cleanup for implementing interface.
//
// An optional timeout is applied to the processing of every message. The trace
// ID is read from the traceHeader message header ("" means DefaultTraceHeader).
func newConsumerGroupHandler(
	logger zerolog.Logger,
	name string,
	setup func(sarama.ConsumerGroupSession) error,
	cleanup func(sarama.ConsumerGroupSession) error,
	handler ConsumerHandler,
	timeout time.Duration,
	cancelOnRevoke bool,
	validate *validator.Validate,
	traceHeader string,
) sarama.ConsumerGroupHandler {
	return &ConsumerGroupHandler{
		logger:   logger.With().Str("namespace", "kafka.consumer.handler").Logger(),
		name:     name,
		setup:    setup,
		cleanup:  cleanup,
		handler:  handler,
		timeout:  timeout,
		validate: validate,

		cancelOnRevoke: cancelOnRevoke,
		traceHeader:    traceHeader,
	}
}

// Setup implements [sarama.ConsumerGroupHandler].
func (handler *ConsumerGroupHandler) Setup(session sarama.ConsumerGroupSession) error {
	if handler.setup != nil {
		return handler.setup(session)
	}

	return nil
}

// Cleanup implements [sarama.ConsumerGroupHandler].
func (handler *ConsumerGroupHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	if handler.cleanup != nil {
		return handler.cleanup(session)
	}

	return nil
}

// ConsumeClaim implements [sarama.ConsumerGroupHandler] and processes the
// claim's messages one by one. Closing claim.Messages() is a normal rebalance
// event, not an error: nil is returned.
//
// The handler context is context.WithoutCancel(session.Context()): session
// values are preserved, but a rebalance or Consumer.Close does not cancel it.
// When the session ends, the current message runs to completion on the
// non-cancellable ctx and then ConsumeClaim returns; further messages are not
// taken and will be redelivered (at-least-once). HandlerTimeout is applied on
// top, if set; without it in-flight processing is bounded only by the ctx of
// Consumer.Close.
//
// With WithConsumerHandlerCancelOnRevoke(true) (legacy behavior) the ctx
// derives from session.Context() and is canceled by rebalance and Close.
func (handler *ConsumerGroupHandler) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				return nil
			}

			if !handler.cancelOnRevoke && session.Context().Err() != nil {
				return nil // session ended: do not take the message, it will be redelivered
			}

			if err := handler.handle(session, claim, message); err != nil {
				handler.logger.
					Error().
					Err(err).
					Msg("kafka.consumer.handler")
			}
		case <-session.Context().Done():
			return nil
		}
	}
}

func (handler *ConsumerGroupHandler) handle(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
	message *sarama.ConsumerMessage,
) error {
	ctx, cancel := handlerContext(session, handler.timeout, handler.cancelOnRevoke)
	defer cancel()

	return handler.handler(newContext(ctx, session, claim, message, handler.validate, handler.traceHeader))
}

// traceIDFromHeaders returns the value of the first header named traceHeader
// ("" means DefaultTraceHeader), or "" if there is none.
func traceIDFromHeaders(headers []*sarama.RecordHeader, traceHeader string) string {
	traceHeader = traceHeaderOrDefault(traceHeader)

	for _, header := range headers {
		if header != nil && convertStringFromBytes(header.Key) == traceHeader {
			return convertStringFromBytes(header.Value)
		}
	}

	return ""
}
