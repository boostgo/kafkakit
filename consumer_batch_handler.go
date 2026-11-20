package kafkakit

import (
	"time"

	"github.com/IBM/sarama"
	"github.com/go-playground/validator/v10"
	"github.com/rs/zerolog"
)

// BatchConsumerHandler is a callback that receives a batch of messages.
type BatchConsumerHandler func(*BatchConsumerContext) error

// BatchErrorHandler is an optional callback invoked when a batch handler
// returns an error. It receives the first message of the failed batch and the
// error. If nil, errors are only logged.
type BatchErrorHandler func(msg *ConsumerMessage, err error)

type consumerGroupBatchHandler struct {
	logger zerolog.Logger

	name         string
	setup        func(sarama.ConsumerGroupSession) error
	cleanup      func(sarama.ConsumerGroupSession) error
	handler      BatchConsumerHandler
	errorHandler BatchErrorHandler
	timeout      time.Duration

	cancelOnRevoke bool

	batchSize    int
	batchTimeout time.Duration

	validate    *validator.Validate
	traceHeader string
}

func newConsumerGroupBatchHandler(
	logger zerolog.Logger,
	name string,
	setup func(sarama.ConsumerGroupSession) error,
	cleanup func(sarama.ConsumerGroupSession) error,
	handler BatchConsumerHandler,
	errorHandler BatchErrorHandler,
	timeout time.Duration,
	cancelOnRevoke bool,
	batchSize int,
	batchTimeout time.Duration,
	validate *validator.Validate,
	traceHeader string,
) sarama.ConsumerGroupHandler {
	return &consumerGroupBatchHandler{
		logger: logger.
			With().
			Str("namespace", "kafka.consumer.batch_handler").
			Logger(),
		name:           name,
		setup:          setup,
		cleanup:        cleanup,
		handler:        handler,
		errorHandler:   errorHandler,
		timeout:        timeout,
		cancelOnRevoke: cancelOnRevoke,
		batchSize:      batchSize,
		batchTimeout:   batchTimeout,
		validate:       validate,
		traceHeader:    traceHeader,
	}
}

func (h *consumerGroupBatchHandler) Setup(session sarama.ConsumerGroupSession) error {
	if h.setup != nil {
		return h.setup(session)
	}

	return nil
}

func (h *consumerGroupBatchHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	if h.cleanup != nil {
		return h.cleanup(session)
	}

	return nil
}

// ConsumeClaim collects messages into batches and passes them to the handler.
//
// Closing claim.Messages() is a normal rebalance: the tail is processed (the
// session is still alive and ctx is not canceled) and nil is returned. If the
// session itself ends, the tail not yet processed at that moment is NOT
// processed and NOT marked; it will be redelivered (at-least-once). A batch
// already in the handler runs to completion on a non-cancellable ctx (see
// processBatch).
//
// With WithConsumerHandlerCancelOnRevoke(true) (legacy behavior) the batch
// tail is passed to the handler with an already canceled ctx when the session
// ends.
func (h *consumerGroupBatchHandler) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	batch := make([]*sarama.ConsumerMessage, 0, h.batchSize)
	ticker := time.NewTicker(h.batchTimeout)
	defer ticker.Stop()

	// The batch buffer is reused between flushes: processBatch is synchronous,
	// and newBatchConsumerContext copies the raw messages into its own
	// []*ConsumerMessage slice, which is what the caller receives. The raw
	// slice never escapes and does not outlive the call.
	//
	// IMPORTANT: if processBatch ever starts exposing the raw slice (or
	// processes batches asynchronously), drop the reuse and go back to make()
	// on every flush.
	flush := func() {
		if len(batch) == 0 {
			return
		}

		h.processBatch(session, batch)
		batch = batch[:0]
	}

	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				// Messages() closed: a normal rebalance; flush the tail and exit without error.
				flush()

				return nil
			}

			batch = append(batch, message)

			if len(batch) >= h.batchSize {
				flush()
				ticker.Reset(h.batchTimeout)
			}

		case <-ticker.C:
			flush()

		case <-session.Context().Done():
			if h.cancelOnRevoke {
				flush() // legacy: the tail goes to the handler with a canceled ctx
			} else {
				batch = batch[:0] // the tail is neither processed nor marked: it will be redelivered
			}

			return nil
		}
	}
}

// processBatch calls the batch handler. The context is
// context.WithoutCancel(session.Context()) (rebalance and Consumer.Close do not
// cancel it; with cancelOnRevoke it derives from session.Context() directly)
// with HandlerTimeout on top, if set.
// On error it logs and calls errorHandler (if set) with the first message of
// the batch.
func (h *consumerGroupBatchHandler) processBatch(
	session sarama.ConsumerGroupSession,
	batch []*sarama.ConsumerMessage,
) {
	ctx, cancel := handlerContext(session, h.timeout, h.cancelOnRevoke)
	defer cancel()

	batchCtx := newBatchConsumerContext(ctx, session, batch, h.validate, h.traceHeader)

	err := h.handler(batchCtx)
	if err == nil {
		return
	}

	h.logger.
		Error().
		Err(err).
		Int("batch_size", len(batch)).
		Int32("partition", batch[0].Partition).
		Msg("kafka.consumer.batch_handler.process_batch")

	if h.errorHandler != nil {
		h.errorHandler(batchCtx.messages[0], err)
	}
}
