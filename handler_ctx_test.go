package kafkakit

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/rs/zerolog"
)

type k9bCtxKey struct{}

// openClaim is a claim that never closes by itself: messages are fed manually.
func openClaim(buffer int) *fakeClaim {
	return &fakeClaim{messages: make(chan *sarama.ConsumerMessage, buffer)}
}

func k9bMsg(offset int64) *sarama.ConsumerMessage {
	return &sarama.ConsumerMessage{Topic: "t", Offset: offset}
}

func k9bNewBatchHandler(
	h BatchConsumerHandler, timeout time.Duration, cancelOnRevoke bool, batchSize int,
) sarama.ConsumerGroupHandler {
	return newConsumerGroupBatchHandler(
		zerolog.Nop(), "test.batch_handler", nil, nil, h, nil, timeout, cancelOnRevoke, batchSize, time.Hour, nil, "",
	)
}

func k9bNewHandler(h ConsumerHandler, timeout time.Duration, cancelOnRevoke bool) sarama.ConsumerGroupHandler {
	return newConsumerGroupHandler(zerolog.Nop(), "test.handler", nil, nil, h, timeout, cancelOnRevoke, nil, "")
}

func waitDone(t *testing.T, done <-chan error) {
	t.Helper()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ConsumeClaim: %v", err)
		}
	case <-time.After(testWait):
		t.Fatal("ConsumeClaim did not return")
	}
}

// Batch: the session ends with a non-empty buffer; the tail is neither processed nor marked.
func TestBatchHandler_TailDroppedOnSessionEnd(t *testing.T) {
	sessionCtx, cancel := context.WithCancel(context.Background())
	session := &fakeSession{ctx: sessionCtx}

	called := make(chan struct{}, 1)
	handler := k9bNewBatchHandler(func(b *BatchConsumerContext) error {
		called <- struct{}{}
		b.MarkAll()

		return nil
	}, 0, false, 10)

	claim := openClaim(0) // unbuffered: send returns once ConsumeClaim has received the message

	done := make(chan error, 1)

	go func() { done <- handler.ConsumeClaim(session, claim) }()

	for i := range 3 {
		claim.messages <- k9bMsg(int64(i))
	}

	cancel() // the buffer (3 < batchSize) has not been flushed yet
	waitDone(t, done)

	select {
	case <-called:
		t.Fatal("tail must not be passed to the handler after session end")
	default:
	}

	if session.marked != 0 {
		t.Fatalf("tail must not be marked, marked=%d", session.marked)
	}
}

// Batch: handler in flight, session canceled; ctx is not canceled and processing completes.
func TestBatchHandler_InFlightCtxNotCanceled(t *testing.T) {
	sessionCtx, cancel := context.WithCancel(context.WithValue(context.Background(), k9bCtxKey{}, "v"))
	defer cancel()

	started, release := make(chan struct{}), make(chan struct{})
	observed := make(chan error, 1)
	value := make(chan any, 1)

	handler := k9bNewBatchHandler(func(b *BatchConsumerContext) error {
		close(started)
		<-release
		observed <- b.Context().Err()
		value <- b.Context().Value(k9bCtxKey{})
		b.MarkAll()

		return nil
	}, 0, false, 1)

	claim := openClaim(1)
	claim.messages <- k9bMsg(0)

	session := &fakeSession{ctx: sessionCtx}
	done := make(chan error, 1)

	go func() { done <- handler.ConsumeClaim(session, claim) }()

	<-started
	cancel()
	close(release)

	if err := <-observed; err != nil {
		t.Fatalf("handler ctx must not be canceled by the session, got %v", err)
	}

	if v := <-value; v != "v" { // session ctx values are visible in the handler ctx
		t.Fatalf("session ctx value lost, got %v", v)
	}

	waitDone(t, done)

	if session.marked != 1 {
		t.Fatalf("in-flight batch must be marked, marked=%d", session.marked)
	}
}

// Batch: Done() fires only on HandlerTimeout.
func TestBatchHandler_DoneOnlyByTimeout(t *testing.T) {
	sessionCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	started := make(chan struct{})
	got := make(chan error, 1)

	handler := k9bNewBatchHandler(func(b *BatchConsumerContext) error {
		close(started)
		<-b.Context().Done()
		got <- b.Context().Err()

		return nil
	}, 10*time.Millisecond, false, 1)

	claim := openClaim(1)
	claim.messages <- k9bMsg(0)

	done := make(chan error, 1)

	go func() { done <- handler.ConsumeClaim(&fakeSession{ctx: sessionCtx}, claim) }()

	<-started
	cancel() // the session ends in the middle of processing

	if err := <-got; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ctx error = %v, want DeadlineExceeded (not Canceled)", err)
	}

	waitDone(t, done)
}

// Single: a message in flight runs to completion on a non-cancellable ctx.
func TestConsumerHandler_InFlightCtxNotCanceled(t *testing.T) {
	sessionCtx, cancel := context.WithCancel(context.WithValue(context.Background(), k9bCtxKey{}, "v"))
	defer cancel()

	started, release := make(chan struct{}), make(chan struct{})
	observed := make(chan error, 1)
	value := make(chan any, 1)
	handled := 0

	handler := k9bNewHandler(func(c *ConsumerContext) error {
		handled++

		if handled == 1 {
			close(started)
			<-release
		}

		observed <- c.Context().Err()
		value <- c.Context().Value(k9bCtxKey{})

		return nil
	}, 0, false)

	claim := openClaim(2)
	claim.messages <- k9bMsg(0)
	claim.messages <- k9bMsg(1) // a second message after the session ended must not be taken

	done := make(chan error, 1)

	go func() { done <- handler.ConsumeClaim(&fakeSession{ctx: sessionCtx}, claim) }()

	<-started
	cancel()
	close(release)

	if err := <-observed; err != nil {
		t.Fatalf("handler ctx must not be canceled by the session, got %v", err)
	}

	if v := <-value; v != "v" {
		t.Fatalf("session ctx value lost, got %v", v)
	}

	waitDone(t, done)

	if handled != 1 {
		t.Fatalf("handled %d messages, want 1: no new messages after session end", handled)
	}
}

// Single: Done() fires only on HandlerTimeout.
func TestConsumerHandler_DoneOnlyByTimeout(t *testing.T) {
	sessionCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	started := make(chan struct{})
	got := make(chan error, 1)

	handler := k9bNewHandler(func(c *ConsumerContext) error {
		close(started)
		<-c.Context().Done()
		got <- c.Context().Err()

		return nil
	}, 10*time.Millisecond, false)

	claim := openClaim(1)
	claim.messages <- k9bMsg(0)

	done := make(chan error, 1)

	go func() { done <- handler.ConsumeClaim(&fakeSession{ctx: sessionCtx}, claim) }()

	<-started
	cancel()

	if err := <-got; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ctx error = %v, want DeadlineExceeded (not Canceled)", err)
	}

	waitDone(t, done)
}

// cancel-on-revoke(true), batch: legacy behavior, the tail is flushed and ctx is canceled.
func TestBatchHandler_CancelOnRevokeFlushesTail(t *testing.T) {
	sessionCtx, cancel := context.WithCancel(context.Background())

	var batches, messages int

	var ctxErr error

	handler := k9bNewBatchHandler(func(b *BatchConsumerContext) error {
		batches++
		messages += b.Len()
		ctxErr = b.Context().Err()

		return nil
	}, 0, true, 10)

	claim := openClaim(0)
	done := make(chan error, 1)

	go func() { done <- handler.ConsumeClaim(&fakeSession{ctx: sessionCtx}, claim) }()

	for i := range 3 {
		claim.messages <- k9bMsg(int64(i))
	}

	cancel()
	waitDone(t, done)

	if messages != 3 || batches != 1 || ctxErr != context.Canceled {
		t.Fatalf("legacy: want 1 flush of 3 with canceled ctx, got batches=%d messages=%d ctxErr=%v", batches, messages, ctxErr)
	}
}

// cancel-on-revoke(true), single: ctx is canceled by the session.
func TestConsumerHandler_CancelOnRevokeCancelsCtx(t *testing.T) {
	sessionCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	started := make(chan struct{})
	observed := make(chan error, 1)

	handler := k9bNewHandler(func(c *ConsumerContext) error {
		close(started)
		<-c.Context().Done()
		observed <- c.Context().Err()

		return nil
	}, 0, true)

	claim := openClaim(1)
	claim.messages <- k9bMsg(0)

	done := make(chan error, 1)

	go func() { done <- handler.ConsumeClaim(&fakeSession{ctx: sessionCtx}, claim) }()

	<-started
	cancel()

	if err := <-observed; !errors.Is(err, context.Canceled) {
		t.Fatalf("ctx error = %v, want Canceled", err)
	}

	waitDone(t, done)
}

// claim.Messages() closed: the tail is processed and ctx is not canceled (regression).
func TestBatchHandler_MessagesClosedTailProcessed(t *testing.T) {
	var messages int

	var ctxErr error

	handler := k9bNewBatchHandler(func(b *BatchConsumerContext) error {
		messages += b.Len()
		ctxErr = b.Context().Err()

		return nil
	}, 0, false, 100)

	if err := handler.ConsumeClaim(&fakeSession{ctx: context.Background()}, feedClaim(7)); err != nil {
		t.Fatalf("ConsumeClaim: %v", err)
	}

	if messages != 7 || ctxErr != nil {
		t.Fatalf("tail must be processed: messages=%d ctxErr=%v", messages, ctxErr)
	}
}

// The option reaches both the consumer and the handler.
func TestConsumerOptions_HandlerCancelOnRevoke(t *testing.T) {
	if newConsumerOptions().HandlerCancelOnRevoke {
		t.Fatal("default must be false")
	}

	o := newConsumerOptions()
	WithConsumerHandlerCancelOnRevoke(true)(o)

	c := newConsumerFromGroup(context.Background(), zerolog.Nop(), newFakeGroup(), o)
	if !c.cancelOnRevoke {
		t.Fatal("option must reach Consumer")
	}
}

// handlerGroup is a fake group that drives ConsumeClaim of a real handler.
type handlerGroup struct {
	*fakeGroup
	claim *fakeClaim
}

func (g *handlerGroup) Consume(ctx context.Context, _ []string, h sarama.ConsumerGroupHandler) error {
	g.calls.Add(1)
	g.started <- g.calls.Load()

	err := h.ConsumeClaim(&fakeSession{ctx: ctx}, g.claim)
	<-ctx.Done()

	return err
}

// Consumer.Close waits for in-flight processing and does not cancel its ctx.
func TestConsumer_CloseWaitsInFlight(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	observed := make(chan error, 1)

	claim := openClaim(1)
	claim.messages <- k9bMsg(0)

	group := &handlerGroup{fakeGroup: newFakeGroup(), claim: claim}
	c, _ := newTestConsumer(t, group)

	c.Consume(func(cc *ConsumerContext) error {
		close(started)
		<-release
		observed <- cc.Context().Err()

		return nil
	})

	<-started

	closed := make(chan error, 1)

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), testWait)
		defer cancel()

		closed <- c.Close(ctx)
	}()

	select {
	case err := <-closed:
		t.Fatalf("Close returned while the handler is in flight: %v", err)
	case <-time.After(50 * time.Millisecond): // negative check: Close must keep waiting
	}

	close(release)

	if err := <-observed; err != nil {
		t.Fatalf("handler ctx canceled by Close: %v", err)
	}

	if err := <-closed; err != nil {
		t.Fatalf("Close: %v", err)
	}
}
