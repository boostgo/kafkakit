package kafkakit

import (
	"context"
	"time"

	"github.com/IBM/sarama"
)

// handlerContext builds the handler context from the session context.
//
// By default it is [context.WithoutCancel]: session values are preserved, but
// a rebalance or Consumer.Close does not cancel the context, so in-flight
// processing runs to completion and the marked offset matches the work that
// was actually done. With cancelOnRevoke the context derives directly from
// session.Context() (legacy behavior: canceled by rebalance and Close).
// timeout is applied on top when > 0; the caller must call the returned cancel.
func handlerContext(
	session sarama.ConsumerGroupSession,
	timeout time.Duration,
	cancelOnRevoke bool,
) (context.Context, context.CancelFunc) {
	ctx := session.Context()
	if !cancelOnRevoke {
		ctx = context.WithoutCancel(ctx)
	}

	if timeout > 0 {
		return context.WithTimeout(ctx, timeout)
	}

	return ctx, func() {}
}
