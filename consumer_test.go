package kafkakit

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/rs/zerolog"
)

// syncBuffer is a goroutine-safe zerolog sink.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.Write(p)
}

func (b *syncBuffer) count(substr string) int {
	b.mu.Lock()
	defer b.mu.Unlock()

	return strings.Count(b.buf.String(), substr)
}

func newTestConsumerWith(
	t *testing.T,
	group sarama.ConsumerGroup,
	logger zerolog.Logger,
	mutate func(*ConsumerOptions),
) *Consumer {
	t.Helper()

	options := newConsumerOptions()
	options.GroupID = "group"
	options.Topics = []string{"topic"}
	options.Name = "test"

	if mutate != nil {
		mutate(options)
	}

	c := newConsumerFromGroup(context.Background(), logger, group, options)
	c.clock = newFakeClock()

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), testWait)
		defer cancel()

		_ = c.Close(ctx)
	})

	return c
}

// --- batch error handler

func TestConsumerBatch_ErrorHandlerCalled(t *testing.T) {
	boom := errors.New("boom")

	type call struct {
		msg *ConsumerMessage
		err error
	}

	var calls []call

	handler := newConsumerGroupBatchHandler(
		zerolog.Nop(),
		"test.batch_handler",
		nil,
		nil,
		func(*BatchConsumerContext) error { return boom },
		func(msg *ConsumerMessage, err error) { calls = append(calls, call{msg, err}) },
		0,
		false,
		3,
		time.Hour,
		nil,
		"",
	)

	if err := handler.ConsumeClaim(&fakeSession{ctx: context.Background()}, feedClaim(3)); err != nil {
		t.Fatalf("ConsumeClaim: %v", err)
	}

	if len(calls) != 1 {
		t.Fatalf("ErrorHandler calls = %d, want 1", len(calls))
	}

	if !errors.Is(calls[0].err, boom) {
		t.Fatalf("ErrorHandler got %v, want the handler error", calls[0].err)
	}

	if calls[0].msg == nil || calls[0].msg.Offset() != 0 {
		t.Fatalf("ErrorHandler must get the first message of the batch, got %+v", calls[0].msg)
	}
}

// --- errors listener and closed claims

func testErrorsListenerSurvivesFirstError(t *testing.T, v consumerVariant) {
	logs := &syncBuffer{}
	group := newFakeGroup()
	c := newTestConsumerWith(t, group, zerolog.New(logs), nil)

	v.start(c)

	for i := range 3 {
		select {
		case group.errs <- errors.New("commit failed"):
		case <-time.After(testWait):
			t.Fatalf("error #%d was not read: the listener died", i+1)
		}
	}

	close(group.errs)

	exited := make(chan struct{})

	go func() {
		c.errsWG.Wait()
		close(exited)
	}()

	select {
	case <-exited:
	case <-time.After(testWait):
		t.Fatal("listener must exit when the Errors() channel is closed")
	}

	if got := logs.count("group_errors"); got != 3 {
		t.Fatalf("logged %d group errors, want 3", got)
	}
}

func TestConsumer_ErrorsListenerSurvivesFirstError(t *testing.T) {
	testErrorsListenerSurvivesFirstError(t, consumerVariants[0])
}

func TestConsumerBatch_ErrorsListenerSurvivesFirstError(t *testing.T) {
	testErrorsListenerSurvivesFirstError(t, consumerVariants[1])
}

func TestConsumerHandler_MessagesClosedNotError(t *testing.T) {
	var handled int

	handler := newConsumerGroupHandler(
		zerolog.Nop(), "test.handler", nil, nil,
		func(*ConsumerContext) error { handled++; return nil }, 0, false, nil, "",
	)

	if err := handler.ConsumeClaim(&fakeSession{ctx: context.Background()}, feedClaim(3)); err != nil {
		t.Fatalf("closing claim.Messages() must return nil, got %v", err)
	}

	if handled != 3 {
		t.Fatalf("handled %d messages, want 3", handled)
	}
}

func TestConsumerBatchHandler_MessagesClosedNotError(t *testing.T) {
	var batches, messages int

	handler := newBatchHandler(t, 100, func(b *BatchConsumerContext) error {
		batches++
		messages += b.Len()

		return nil
	})

	if err := handler.ConsumeClaim(&fakeSession{ctx: context.Background()}, feedClaim(7)); err != nil {
		t.Fatalf("closing claim.Messages() must return nil, got %v", err)
	}

	if batches != 1 || messages != 7 {
		t.Fatalf("the tail must be flushed before returning: batches=%d messages=%d", batches, messages)
	}
}

// --- offset reset options

func TestConsumerOptions_ResetInvalidOffsetsReachesConfig(t *testing.T) {
	apply := func(opts ...ConsumerOption) *sarama.Config {
		options := newConsumerOptions()
		for _, o := range opts {
			o(options)
		}

		config := sarama.NewConfig()
		config.Version = sarama.DefaultVersion

		if err := options.applySaramaConfig(config); err != nil {
			t.Fatalf("applySaramaConfig: %v", err)
		}

		return config
	}

	if got := apply(); !got.Consumer.Group.ResetInvalidOffsets {
		t.Fatal("default must stay true (effective sarama default)")
	}

	if got := apply(WithConsumerResetInvalidOffsets(false)); got.Consumer.Group.ResetInvalidOffsets {
		t.Fatal("WithConsumerResetInvalidOffsets(false) did not reach the sarama config")
	}

	if got := apply(); got.Consumer.Offsets.Retry.Max != 3 {
		t.Fatalf("default RetryMax = %d, want 3", got.Consumer.Offsets.Retry.Max)
	}

	if got := apply(WithConsumerRetryMax(7)); got.Consumer.Offsets.Retry.Max != 7 {
		t.Fatalf("RetryMax = %d, want 7", got.Consumer.Offsets.Retry.Max)
	}
}

func TestConsumer_WarnsOnResetInvalidOffsets(t *testing.T) {
	for _, tc := range []struct {
		name  string
		reset bool
		want  int
	}{{"enabled", true, 1}, {"disabled", false, 0}} {
		t.Run(tc.name, func(t *testing.T) {
			logs := &syncBuffer{}
			c := newTestConsumerWith(t, newFakeGroup(), zerolog.New(logs), func(o *ConsumerOptions) {
				o.ResetInvalidOffsets = tc.reset
			})

			c.Consume(func(*ConsumerContext) error { return nil })

			if got := logs.count("reset_invalid_offsets_enabled"); got != tc.want {
				t.Fatalf("warnings = %d, want %d", got, tc.want)
			}
		})
	}
}

// --- handler ctx derived from the session

func TestConsumerHandler_CtxFromSession(t *testing.T) {
	t.Run("session cancel", func(t *testing.T) {
		sessionCtx, cancel := context.WithCancel(context.Background())
		defer cancel()

		started := make(chan struct{})
		observed := make(chan error, 1)

		handler := newConsumerGroupHandler(
			zerolog.Nop(), "test.handler", nil, nil,
			func(c *ConsumerContext) error {
				close(started)
				<-c.Context().Done()
				observed <- c.Context().Err()

				return nil
			}, 0, true, nil, "", // cancellation only happens in cancel-on-revoke mode
		)

		claim := &fakeClaim{messages: make(chan *sarama.ConsumerMessage, 1)}
		claim.messages <- &sarama.ConsumerMessage{Topic: "t"}

		done := make(chan error, 1)

		go func() { done <- handler.ConsumeClaim(&fakeSession{ctx: sessionCtx}, claim) }()

		<-started
		cancel()

		select {
		case err := <-observed:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("handler ctx error = %v, want Canceled", err)
			}
		case <-time.After(testWait):
			t.Fatal("session cancel is not visible in the handler ctx")
		}

		if err := <-done; err != nil {
			t.Fatalf("ConsumeClaim: %v", err)
		}
	})

	t.Run("handler timeout", func(t *testing.T) {
		got := make(chan error, 1)

		handler := newConsumerGroupHandler(
			zerolog.Nop(), "test.handler", nil, nil,
			func(c *ConsumerContext) error {
				<-c.Context().Done()
				got <- c.Context().Err()

				return nil
			}, time.Millisecond, false, nil, "",
		)

		if err := handler.ConsumeClaim(&fakeSession{ctx: context.Background()}, feedClaim(1)); err != nil {
			t.Fatalf("ConsumeClaim: %v", err)
		}

		if err := <-got; !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("handler ctx error = %v, want DeadlineExceeded", err)
		}
	})
}

func TestConsumerBatchHandler_CtxFromSession(t *testing.T) {
	t.Run("session cancel", func(t *testing.T) {
		sessionCtx, cancel := context.WithCancel(context.Background())
		defer cancel()

		started := make(chan struct{})
		observed := make(chan error, 1)

		handler := newConsumerGroupBatchHandler(
			zerolog.Nop(), "test.batch_handler", nil, nil,
			func(b *BatchConsumerContext) error {
				close(started)
				<-b.Context().Done()
				observed <- b.Context().Err()

				return nil
			}, nil, 0, true, 1, time.Hour, nil, "", // cancel-on-revoke mode
		)

		claim := &fakeClaim{messages: make(chan *sarama.ConsumerMessage, 1)}
		claim.messages <- &sarama.ConsumerMessage{Topic: "t"}

		done := make(chan error, 1)

		go func() { done <- handler.ConsumeClaim(&fakeSession{ctx: sessionCtx}, claim) }()

		<-started
		cancel()

		select {
		case err := <-observed:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("handler ctx error = %v, want Canceled", err)
			}
		case <-time.After(testWait):
			t.Fatal("session cancel is not visible in the batch handler ctx")
		}

		if err := <-done; err != nil {
			t.Fatalf("ConsumeClaim: %v", err)
		}
	})

	t.Run("handler timeout", func(t *testing.T) {
		got := make(chan error, 1)

		handler := newConsumerGroupBatchHandler(
			zerolog.Nop(), "test.batch_handler", nil, nil,
			func(b *BatchConsumerContext) error {
				<-b.Context().Done()
				got <- b.Context().Err()

				return nil
			}, nil, time.Millisecond, false, 1, time.Hour, nil, "",
		)

		if err := handler.ConsumeClaim(&fakeSession{ctx: context.Background()}, feedClaim(1)); err != nil {
			t.Fatalf("ConsumeClaim: %v", err)
		}

		if err := <-got; !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("handler ctx error = %v, want DeadlineExceeded", err)
		}
	})
}

// --- OnClose callback

func TestConsumer_OnCloseCopied(t *testing.T) {
	for _, v := range consumerVariants {
		t.Run(v.name, func(t *testing.T) {
			called := make(chan struct{}, 4)
			group := newFakeGroup()
			c := newTestConsumerWith(t, group, zerolog.Nop(), func(o *ConsumerOptions) {
				WithConsumerOnClose(func() { called <- struct{}{} })(o)
			})

			v.start(c)

			for range 2 { // repeated matching errors do not trigger repeated calls
				group.errs <- sarama.ErrClosedConsumerGroup
			}

			select {
			case <-called:
			case <-time.After(testWait):
				t.Fatal("OnConsumerClose was not called on ErrClosedConsumerGroup")
			}

			// The listener is still alive after the first error: the second send above completed.
			select {
			case <-called:
				t.Fatal("OnConsumerClose must be called at most once")
			default:
			}
		})
	}
}

// --- trace ID computed once from the trace header

func TestConsumerMessage_TraceIDFromHeader(t *testing.T) {
	header := func(k, v string) *sarama.RecordHeader {
		return &sarama.RecordHeader{Key: []byte(k), Value: []byte(v)}
	}

	for _, tc := range []struct {
		name        string
		traceHeader string // passed to the constructors; "" means the default
		headers     []*sarama.RecordHeader
		want        string
	}{
		{"missing header", "", nil, ""},
		{"unrelated headers", "", []*sarama.RecordHeader{header("x", "y")}, ""},
		{"default name", "", []*sarama.RecordHeader{header("x", "y"), header(DefaultTraceHeader, "d")}, "d"},
		{"default name passed explicitly", DefaultTraceHeader, []*sarama.RecordHeader{header(DefaultTraceHeader, "d")}, "d"},
		{"first of duplicates wins", "", []*sarama.RecordHeader{
			header(DefaultTraceHeader, "first"), header(DefaultTraceHeader, "second"),
		}, "first"},
		{"empty value", "", []*sarama.RecordHeader{header(DefaultTraceHeader, "")}, ""},
		{"nil header is skipped", "", []*sarama.RecordHeader{nil, header(DefaultTraceHeader, "d")}, "d"},
		{"custom name", "x-request-id", []*sarama.RecordHeader{header("x-request-id", "c")}, "c"},
		{"custom name ignores the default header", "x-request-id", []*sarama.RecordHeader{
			header(DefaultTraceHeader, "d"),
		}, ""},
		{"header name is case-sensitive", "", []*sarama.RecordHeader{header("Trace-Id", "d")}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := &sarama.ConsumerMessage{Headers: tc.headers}

			if got := newConsumerMessage(raw, nil, tc.traceHeader).TraceID(); got != tc.want {
				t.Fatalf("ConsumerMessage.TraceID() = %q, want %q", got, tc.want)
			}

			cc := newContext(context.Background(), &fakeSession{ctx: context.Background()}, &fakeClaim{}, raw, nil, tc.traceHeader)
			if got := cc.TraceID(); got != tc.want {
				t.Fatalf("ConsumerContext.TraceID() = %q, want %q", got, tc.want)
			}

			if got := traceIDFromContext(cc.Context()); got != tc.want {
				t.Fatalf("trace_id in the handler ctx = %q, want %q", got, tc.want)
			}

			if tc.want == "" && cc.Context().Value(TraceKey) != nil {
				t.Fatal("an empty trace ID must not be stored in the handler ctx")
			}
		})
	}
}

// The trace header configured on the consumer reaches both handler kinds.
func TestConsumer_TraceHeaderReachesHandlers(t *testing.T) {
	const custom = "x-correlation-id"

	options := newConsumerOptions()
	options.traceHeader = custom

	c := newConsumerFromGroup(context.Background(), zerolog.Nop(), newFakeGroup(), options)
	if c.traceHeader != custom {
		t.Fatalf("consumer trace header = %q, want %q", c.traceHeader, custom)
	}

	if d := newConsumerFromGroup(context.Background(), zerolog.Nop(), newFakeGroup(), newConsumerOptions()); d.traceHeader != DefaultTraceHeader {
		t.Fatalf("default consumer trace header = %q, want %q", d.traceHeader, DefaultTraceHeader)
	}

	msg := func() *sarama.ConsumerMessage {
		return &sarama.ConsumerMessage{Topic: "t", Headers: []*sarama.RecordHeader{
			{Key: []byte(DefaultTraceHeader), Value: []byte("default")},
			{Key: []byte(custom), Value: []byte("custom")},
		}}
	}

	var single string

	handler := newConsumerGroupHandler(
		zerolog.Nop(), "test.handler", nil, nil,
		func(cc *ConsumerContext) error { single = traceIDFromContext(cc.Context()); return nil },
		0, false, nil, c.traceHeader,
	)

	claim := &fakeClaim{messages: make(chan *sarama.ConsumerMessage, 1)}
	claim.messages <- msg()
	close(claim.messages)

	if err := handler.ConsumeClaim(&fakeSession{ctx: context.Background()}, claim); err != nil {
		t.Fatalf("ConsumeClaim: %v", err)
	}

	if single != "custom" {
		t.Fatalf("single handler trace = %q, want %q", single, "custom")
	}

	var batch string

	batchHandler := newConsumerGroupBatchHandler(
		zerolog.Nop(), "test.batch_handler", nil, nil,
		func(b *BatchConsumerContext) error { batch = b.Messages()[0].TraceID(); return nil },
		nil, 0, false, 1, time.Hour, nil, c.traceHeader,
	)

	batchClaim := &fakeClaim{messages: make(chan *sarama.ConsumerMessage, 1)}
	batchClaim.messages <- msg()
	close(batchClaim.messages)

	if err := batchHandler.ConsumeClaim(&fakeSession{ctx: context.Background()}, batchClaim); err != nil {
		t.Fatalf("ConsumeClaim: %v", err)
	}

	if batch != "custom" {
		t.Fatalf("batch handler trace = %q, want %q", batch, "custom")
	}
}

func benchHeaders() []*sarama.RecordHeader {
	return []*sarama.RecordHeader{
		{Key: []byte("h1"), Value: []byte("v1")},
		{Key: []byte("h2"), Value: []byte("v2")},
		{Key: []byte("h3"), Value: []byte("v3")},
		{Key: []byte(DefaultTraceHeader), Value: []byte("trace")},
		{Key: []byte("h4"), Value: []byte("v4")},
	}
}

// BenchmarkConsumerMessage_TraceID measures TraceID() as a field computed
// once when the wrapper is created.
func BenchmarkConsumerMessage_TraceID(b *testing.B) {
	m := newConsumerMessage(&sarama.ConsumerMessage{Headers: benchHeaders()}, nil, "")

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		_ = m.TraceID()
	}
}

// BenchmarkConsumerMessage_TraceIDLookup measures a linear header scan with
// conversion on every call, for comparison with the precomputed field.
func BenchmarkConsumerMessage_TraceIDLookup(b *testing.B) {
	raw := &sarama.ConsumerMessage{Headers: benchHeaders()}

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		_ = readConsumerMessageHeader(raw, DefaultTraceHeader)
	}
}
