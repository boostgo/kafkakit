package kafkakit

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/rs/zerolog"
)

// fakeGroup is a sarama.ConsumerGroup with scripted Consume responses.
//
// script[i] is the behavior of the (i+1)-th Consume call; after the script ends Consume
// blocks until ctx is canceled and returns nil (like sarama on shutdown).
type fakeGroup struct {
	script []func(ctx context.Context) error

	calls   atomic.Int64
	started chan int64 // index of the Consume call, sent when it starts
	closes  atomic.Int64
	errs    chan error

	active        atomic.Int64 // Consume calls running right now
	activeAtClose atomic.Int64 // how many Consume calls were still running when Close() was called
}

func newFakeGroup(script ...func(ctx context.Context) error) *fakeGroup {
	return &fakeGroup{
		script:  script,
		started: make(chan int64, 64),
		errs:    make(chan error),
	}
}

func (g *fakeGroup) Consume(ctx context.Context, _ []string, _ sarama.ConsumerGroupHandler) error {
	n := g.calls.Add(1)
	g.started <- n

	g.active.Add(1)
	defer g.active.Add(-1)

	if int(n) <= len(g.script) {
		return g.script[n-1](ctx)
	}

	<-ctx.Done()

	return nil
}

func (g *fakeGroup) Errors() <-chan error { return g.errs }

func (g *fakeGroup) Close() error {
	g.activeAtClose.Store(g.active.Load())
	g.closes.Add(1)

	return nil
}

func (g *fakeGroup) Pause(map[string][]int32)  {}
func (g *fakeGroup) Resume(map[string][]int32) {}
func (g *fakeGroup) PauseAll()                 {}
func (g *fakeGroup) ResumeAll()                {}

func returns(err error) func(context.Context) error {
	return func(context.Context) error { return err }
}

// fakeClock is a backoff clock: After reports the requested pause and waits for tick().
type fakeClock struct {
	asked chan time.Duration
	fire  chan time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{asked: make(chan time.Duration, 16), fire: make(chan time.Time)}
}

func (c *fakeClock) After(d time.Duration) <-chan time.Time {
	c.asked <- d

	return c.fire
}

// consumerVariant starts one of the two consumer modes.
type consumerVariant struct {
	name  string
	start func(c *Consumer)
}

var consumerVariants = []consumerVariant{
	{"Consume", func(c *Consumer) {
		c.Consume(func(*ConsumerContext) error { return nil })
	}},
	{"ConsumeBatch", func(c *Consumer) {
		c.ConsumeBatch(func(*BatchConsumerContext) error { return nil }, ConsumeBatchOptions{})
	}},
}

func newTestConsumer(t *testing.T, group sarama.ConsumerGroup) (*Consumer, *fakeClock) {
	t.Helper()

	options := newConsumerOptions()
	options.GroupID = "group"
	options.Topics = []string{"topic"}
	options.Name = "test"

	c := newConsumerFromGroup(context.Background(), zerolog.Nop(), group, options)
	clk := newFakeClock()
	c.clock = clk

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), testWait)
		defer cancel()

		_ = c.Close(ctx)
	})

	return c, clk
}

func waitCall(t *testing.T, g *fakeGroup, want int64) {
	t.Helper()

	select {
	case n := <-g.started:
		if n != want {
			t.Fatalf("Consume call #%d, want #%d", n, want)
		}
	case <-time.After(testWait):
		t.Fatalf("Consume call #%d did not happen", want)
	}
}

func waitAsked(t *testing.T, clk *fakeClock) time.Duration {
	t.Helper()

	select {
	case d := <-clk.asked:
		return d
	case <-time.After(testWait):
		t.Fatal("backoff was not requested")

		return 0
	}
}

func tick(t *testing.T, clk *fakeClock) {
	t.Helper()

	select {
	case clk.fire <- time.Now():
	case <-time.After(testWait):
		t.Fatal("loop is not waiting for the backoff tick")
	}
}

func testCloseWaitsLoopAndClosesGroup(t *testing.T, v consumerVariant) {
	group := newFakeGroup()
	c, _ := newTestConsumer(t, group)

	v.start(c)
	waitCall(t, group, 1)

	ctx, cancel := context.WithTimeout(context.Background(), testWait)
	defer cancel()

	if err := c.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// The group is closed only after the loop exits; otherwise the final commit
	// would race with a running Consume.
	if got := group.activeAtClose.Load(); got != 0 {
		t.Fatalf("group closed while %d Consume call(s) were still running", got)
	}

	if got := group.closes.Load(); got != 1 {
		t.Fatalf("group closed %d times, want 1", got)
	}

	if err := c.Close(ctx); err != nil {
		t.Fatalf("second Close must return nil, got %v", err)
	}

	if got := group.closes.Load(); got != 1 {
		t.Fatalf("group closed %d times after repeated Close, want 1", got)
	}

	// Consume after Close is a no-op: no new group calls.
	v.start(c)

	if got := group.calls.Load(); got != 1 {
		t.Fatalf("Consume after Close started the loop: %d group calls", got)
	}
}

func TestConsumer_CloseWaitsLoopAndClosesGroup(t *testing.T) {
	testCloseWaitsLoopAndClosesGroup(t, consumerVariants[0])
}

func TestConsumerBatch_CloseWaitsLoopAndClosesGroup(t *testing.T) {
	testCloseWaitsLoopAndClosesGroup(t, consumerVariants[1])
}

func TestConsumer_CloseWithoutConsumeClosesGroup(t *testing.T) {
	group := newFakeGroup()
	c, _ := newTestConsumer(t, group)

	if err := c.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if got := group.closes.Load(); got != 1 {
		t.Fatalf("group closed %d times, want 1", got)
	}
}

func TestConsumer_CloseCtxExpired(t *testing.T) {
	for _, v := range consumerVariants {
		t.Run(v.name, func(t *testing.T) {
			release := make(chan struct{})
			group := newFakeGroup(func(context.Context) error {
				<-release // the loop ignores cancellation

				return nil
			})
			c, _ := newTestConsumer(t, group)

			t.Cleanup(func() { close(release) })

			v.start(c)
			waitCall(t, group, 1)

			ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
			defer cancel()

			err := c.Close(ctx)
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("want DeadlineExceeded in the chain, got %v", err)
			}

			if got := group.closes.Load(); got != 1 {
				t.Fatalf("group must still be closed once, got %d", got)
			}
		})
	}
}

func testRestartBackoff(t *testing.T, v consumerVariant) {
	boom := errors.New("boom")
	group := newFakeGroup(returns(boom), returns(boom), returns(nil))
	c, clk := newTestConsumer(t, group)

	v.start(c)
	waitCall(t, group, 1)

	// Error: the next call happens only after a tick, with the initial pause.
	if d := waitAsked(t, clk); d != restartBackoffInitial {
		t.Fatalf("first backoff = %v, want %v", d, restartBackoffInitial)
	}

	if got := group.calls.Load(); got != 1 {
		t.Fatalf("Consume restarted before the tick: %d calls", got)
	}

	tick(t, clk)
	waitCall(t, group, 2)

	// A second consecutive error doubles the pause.
	if d := waitAsked(t, clk); d != 2*restartBackoffInitial {
		t.Fatalf("second backoff = %v, want %v", d, 2*restartBackoffInitial)
	}

	if got := group.calls.Load(); got != 2 {
		t.Fatalf("Consume restarted before the tick: %d calls", got)
	}

	tick(t, clk)
	waitCall(t, group, 3)

	// nil (rebalance): immediate restart, no pause requested.
	waitCall(t, group, 4)

	select {
	case d := <-clk.asked:
		t.Fatalf("restart after nil must not back off, asked %v", d)
	default:
	}
}

func TestConsumer_RestartBackoff(t *testing.T) {
	testRestartBackoff(t, consumerVariants[0])
}

func TestConsumerBatch_RestartBackoff(t *testing.T) {
	testRestartBackoff(t, consumerVariants[1])
}

func TestConsumer_BackoffInterruptedByClose(t *testing.T) {
	for _, v := range consumerVariants {
		t.Run(v.name, func(t *testing.T) {
			group := newFakeGroup(returns(errors.New("boom")))
			c, clk := newTestConsumer(t, group)

			v.start(c)
			waitCall(t, group, 1)
			waitAsked(t, clk)

			ctx, cancel := context.WithTimeout(context.Background(), testWait)
			defer cancel()

			if err := c.Close(ctx); err != nil {
				t.Fatalf("Close must interrupt the backoff pause: %v", err)
			}
		})
	}
}

func testExitOnClosedGroup(t *testing.T, v consumerVariant) {
	group := newFakeGroup(returns(sarama.ErrClosedConsumerGroup))
	c, clk := newTestConsumer(t, group)

	v.start(c)
	waitCall(t, group, 1)

	// The loop ends by itself, without Close and without canceling ctx.
	exited := make(chan struct{})

	go func() {
		c.loopWG.Wait()
		close(exited)
	}()

	select {
	case <-exited:
	case <-time.After(testWait):
		t.Fatal("loop did not exit on ErrClosedConsumerGroup")
	}

	if got := group.calls.Load(); got != 1 {
		t.Fatalf("Consume called %d times after the group was closed, want 1", got)
	}

	select {
	case d := <-clk.asked:
		t.Fatalf("no backoff expected on a closed group, asked %v", d)
	default:
	}
}

func TestConsumer_ExitOnClosedGroup(t *testing.T) {
	testExitOnClosedGroup(t, consumerVariants[0])
}

func TestConsumerBatch_ExitOnClosedGroup(t *testing.T) {
	testExitOnClosedGroup(t, consumerVariants[1])
}

func TestNextRestartDelay(t *testing.T) {
	d := time.Duration(0)

	var seen []time.Duration

	for range 12 {
		d = nextRestartDelay(d)
		seen = append(seen, d)
	}

	if seen[0] != restartBackoffInitial || seen[1] != 2*restartBackoffInitial {
		t.Fatalf("exponential growth expected, got %v", seen[:2])
	}

	if last := seen[len(seen)-1]; last != restartBackoffMax {
		t.Fatalf("delay must be capped at %v, got %v", restartBackoffMax, last)
	}
}
