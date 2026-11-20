package kafkakit

import (
	"time"
)

// ConsumeBatchOptions holds configuration for batch consumption.
type ConsumeBatchOptions struct {
	// BatchSize is the maximum number of messages to collect before processing.
	BatchSize int

	// BatchTimeout is the maximum time to wait for a full batch.
	// If the timeout fires and the buffer is non-empty, the partial batch is processed.
	BatchTimeout time.Duration

	// ErrorHandler is an optional callback invoked when the batch handler
	// returns an error. The error is logged by the library in any case.
	//
	// The batch is a unit of failure, so the callback is called once per failed
	// batch with the FIRST message of the batch (msg is never nil) and the
	// error returned by the handler; use BatchConsumerContext for the rest.
	// Callers that do not need the message may ignore it.
	ErrorHandler BatchErrorHandler
}

// ConsumeBatch starts consuming messages in batches. Messages are collected
// per-partition up to batchSize or batchTimeout (whichever comes first),
// then the handler is called with the entire batch.
//
// The handler receives a BatchConsumerContext with access to all messages
// and a MarkAll method to mark the offset of the last message.
//
// This method is non-blocking (same as Consume).
func (c *Consumer) ConsumeBatch(handler BatchConsumerHandler, opts ConsumeBatchOptions) {
	if opts.BatchSize <= 0 {
		opts.BatchSize = 200
	}

	if opts.BatchTimeout <= 0 {
		opts.BatchTimeout = 50 * time.Millisecond
	}

	groupHandler := newConsumerGroupBatchHandler(
		c.logger,
		c.name+".batch_handler",
		nil,
		nil,
		handler,
		opts.ErrorHandler,
		c.handlerTimeout,
		c.cancelOnRevoke,
		opts.BatchSize,
		opts.BatchTimeout,
		c.validate,
		c.traceHeader,
	)

	if !c.run(groupHandler, "kafka.consumer.batch") {
		return
	}

	c.logger.
		Info().
		Str("name", c.name).
		Str("group_id", c.groupID).
		Strs("topics", c.topics).
		Int("batch_size", opts.BatchSize).
		Dur("batch_timeout", opts.BatchTimeout).
		Msg("kafka.consumer.batch.start")
}
