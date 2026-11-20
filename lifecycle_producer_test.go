package kafkakit

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/IBM/sarama/mocks"
	"github.com/rs/zerolog"
)

const testWait = 5 * time.Second

// fakeAsyncProducer is a sarama.AsyncProducer with controllable behavior:
//
//   - read=false: nobody reads Input() (broker unavailable), no results arrive;
//   - deferResults=true: messages accumulate and their results are delivered only
//     after AsyncClose, as when the buffer is flushed on shutdown;
//   - fail: if it returns an error, the message is rejected.
type fakeAsyncProducer struct {
	input     chan *sarama.ProducerMessage
	successes chan *sarama.ProducerMessage
	errs      chan *sarama.ProducerError

	read         bool
	deferResults bool
	fail         func(*sarama.ProducerMessage) error

	closeOnce sync.Once
	closed    chan struct{}
}

func newFakeAsyncProducer(read, deferResults bool, fail func(*sarama.ProducerMessage) error) *fakeAsyncProducer {
	f := &fakeAsyncProducer{
		input:        make(chan *sarama.ProducerMessage),
		successes:    make(chan *sarama.ProducerMessage),
		errs:         make(chan *sarama.ProducerError),
		read:         read,
		deferResults: deferResults,
		fail:         fail,
		closed:       make(chan struct{}),
	}

	if read {
		go f.run()
	}

	return f
}

func (f *fakeAsyncProducer) emit(msg *sarama.ProducerMessage) {
	if f.fail != nil {
		if err := f.fail(msg); err != nil {
			f.errs <- &sarama.ProducerError{Msg: msg, Err: err}

			return
		}
	}

	f.successes <- msg
}

func (f *fakeAsyncProducer) run() {
	var pending []*sarama.ProducerMessage

	for msg := range f.input {
		if f.deferResults {
			pending = append(pending, msg)

			continue
		}

		f.emit(msg)
	}

	for _, msg := range pending {
		f.emit(msg)
	}

	close(f.successes)
	close(f.errs)
	close(f.closed)
}

func (f *fakeAsyncProducer) AsyncClose() {
	f.closeOnce.Do(func() {
		if f.read {
			close(f.input)

			return
		}

		close(f.successes)
		close(f.errs)
		close(f.closed)
	})
}

func (f *fakeAsyncProducer) Close() error {
	f.AsyncClose()
	<-f.closed

	return nil
}

func (f *fakeAsyncProducer) Input() chan<- *sarama.ProducerMessage     { return f.input }
func (f *fakeAsyncProducer) Successes() <-chan *sarama.ProducerMessage { return f.successes }
func (f *fakeAsyncProducer) Errors() <-chan *sarama.ProducerError      { return f.errs }

func (f *fakeAsyncProducer) IsTransactional() bool                   { return false }
func (f *fakeAsyncProducer) TxnStatus() sarama.ProducerTxnStatusFlag { return 0 }
func (f *fakeAsyncProducer) BeginTxn() error                         { return nil }
func (f *fakeAsyncProducer) CommitTxn() error                        { return nil }
func (f *fakeAsyncProducer) AbortTxn() error                         { return nil }
func (f *fakeAsyncProducer) AddOffsetsToTxn(map[string][]*sarama.PartitionOffsetMetadata, string) error {
	return nil
}

func (f *fakeAsyncProducer) AddMessageToTxn(*sarama.ConsumerMessage, string, *string) error {
	return nil
}

func newFakeBackedProducer(
	t *testing.T,
	fake sarama.AsyncProducer,
	callback func(*ProducerMessage, error),
) *asyncProducer {
	t.Helper()

	p := &asyncProducer{
		producer: fake,
		logger:   zerolog.Nop(),
		callback: callback,
		done:     make(chan struct{}),
	}
	p.start()

	t.Cleanup(func() { _ = p.Close() })

	return p
}

func testMessage() *ProducerMessage {
	return &ProducerMessage{Topic: "topic", Key: "key", Value: []byte("v")}
}

func TestAsyncProducer_CloseFlushesAndWaits(t *testing.T) {
	const total = 50

	var callbacks atomic.Int64

	// Results are delivered only after AsyncClose: Close must wait for them.
	fake := newFakeAsyncProducer(true, true, nil)
	p := newFakeBackedProducer(t, fake, func(_ *ProducerMessage, err error) {
		if err == nil {
			callbacks.Add(1)
		}
	})

	for range total {
		if err := p.Produce(context.Background(), testMessage()); err != nil {
			t.Fatalf("Produce: %v", err)
		}
	}

	if got := callbacks.Load(); got != 0 {
		t.Fatalf("results must not arrive before Close, got %d", got)
	}

	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if got := callbacks.Load(); got != total {
		t.Fatalf("callback calls after Close = %d, want %d", got, total)
	}

	if err := p.Close(); err != nil {
		t.Fatalf("second Close must be idempotent: %v", err)
	}
}

func TestAsyncProducer_CloseFlushesAndWaits_Mock(t *testing.T) {
	const total = 20

	var callbacks atomic.Int64

	producer, _ := newTestAsyncProducer(t, total, func(*sarama.ProducerMessage) error { return nil })
	producer.callback = func(_ *ProducerMessage, err error) {
		if err == nil {
			callbacks.Add(1)
		}
	}

	for range total {
		if err := producer.Produce(context.Background(), testMessage()); err != nil {
			t.Fatalf("Produce: %v", err)
		}
	}

	if err := producer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if got := callbacks.Load(); got != total {
		t.Fatalf("callback calls after Close = %d, want %d", got, total)
	}
}

func TestAsyncProducer_CloseReportsUndelivered(t *testing.T) {
	boom := errors.New("boom")

	fake := newFakeAsyncProducer(true, true, func(*sarama.ProducerMessage) error { return boom })
	p := newFakeBackedProducer(t, fake, nil)

	if err := p.Produce(context.Background(), testMessage(), testMessage()); err != nil {
		t.Fatalf("Produce: %v", err)
	}

	err := p.Close()
	if !errors.Is(err, ErrProducerSend) || !errors.Is(err, boom) {
		t.Fatalf("Close error must wrap ErrProducerSend and the cause, got %v", err)
	}
}

func TestAsyncProducer_ProduceConcurrentWithClose(t *testing.T) {
	const workers = 8

	var (
		accepted  atomic.Int64
		callbacks atomic.Int64
	)

	fake := newFakeAsyncProducer(true, false, nil)
	p := newFakeBackedProducer(t, fake, func(_ *ProducerMessage, err error) {
		if err == nil {
			callbacks.Add(1)
		}
	})

	started := make(chan struct{}, workers)

	var wg sync.WaitGroup

	for range workers {
		wg.Add(1)

		go func() {
			defer wg.Done()

			started <- struct{}{}

			for {
				err := p.Produce(context.Background(), testMessage())
				if err != nil {
					if !errors.Is(err, ErrProducerClosed) || !errors.Is(err, ErrProducerSend) {
						t.Errorf("unexpected Produce error: %v", err)
					}

					return
				}

				accepted.Add(1)
			}
		}()
	}

	for range workers {
		<-started
	}

	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	wg.Wait()

	if err := p.Produce(context.Background(), testMessage()); !errors.Is(err, ErrProducerClosed) {
		t.Fatalf("Produce after Close: want ErrProducerClosed, got %v", err)
	}

	if accepted.Load() != callbacks.Load() {
		t.Fatalf("accepted %d messages, callbacks %d", accepted.Load(), callbacks.Load())
	}
}

type noCloseProducer struct{}

func (noCloseProducer) Produce(context.Context, ...*ProducerMessage) error { return nil }

type closeSpySyncProducer struct {
	sarama.SyncProducer
	closed atomic.Int64
}

func (s *closeSpySyncProducer) Close() error {
	s.closed.Add(1)

	return nil
}

func TestCloseProducer(t *testing.T) {
	t.Run("async flushes and waits", func(t *testing.T) {
		var callbacks atomic.Int64

		fake := newFakeAsyncProducer(true, true, nil)

		var producer Producer = newFakeBackedProducer(t, fake, func(*ProducerMessage, error) {
			callbacks.Add(1)
		})

		if err := producer.Produce(context.Background(), testMessage(), testMessage()); err != nil {
			t.Fatalf("Produce: %v", err)
		}

		if err := CloseProducer(producer); err != nil {
			t.Fatalf("CloseProducer: %v", err)
		}

		if callbacks.Load() != 2 {
			t.Fatalf("callbacks after CloseProducer = %d, want 2", callbacks.Load())
		}
	})

	t.Run("sync closes", func(t *testing.T) {
		spy := &closeSpySyncProducer{}

		var producer Producer = &syncProducer{producer: spy}

		if err := CloseProducer(producer); err != nil {
			t.Fatalf("CloseProducer: %v", err)
		}

		if spy.closed.Load() != 1 {
			t.Fatalf("sync producer closed %d times, want 1", spy.closed.Load())
		}
	})

	t.Run("implementation without Close", func(t *testing.T) {
		if err := CloseProducer(noCloseProducer{}); err != nil {
			t.Fatalf("CloseProducer on a foreign Producer must return nil, got %v", err)
		}
	})
}

func TestAsyncProducer_DeliveryCallback(t *testing.T) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	mock := mocks.NewAsyncProducer(t, config)
	mock.ExpectInputAndSucceed()
	mock.ExpectInputAndFail(errors.New("boom"))

	type result struct {
		msg *ProducerMessage
		err error
	}

	var (
		mu      sync.Mutex
		results []result
	)

	p := newFakeBackedProducer(t, mock, func(msg *ProducerMessage, err error) {
		mu.Lock()
		defer mu.Unlock()

		results = append(results, result{msg, err})
	})

	okMsg, badMsg := testMessage(), testMessage()

	if err := p.Produce(context.Background(), okMsg); err != nil {
		t.Fatalf("Produce: %v", err)
	}

	if err := p.Produce(context.Background(), badMsg); err != nil {
		t.Fatalf("Produce: %v", err)
	}

	_ = p.Close() // Close's result depends on when the error arrives; the callbacks are what we check

	mu.Lock()
	defer mu.Unlock()

	if len(results) != 2 {
		t.Fatalf("callbacks = %d, want 2", len(results))
	}

	for _, r := range results {
		switch r.msg {
		case okMsg:
			if r.err != nil {
				t.Fatalf("delivered message got error %v", r.err)
			}
		case badMsg:
			if r.err == nil {
				t.Fatal("failed message got nil error")
			}
		default:
			t.Fatalf("callback got a foreign message %p", r.msg)
		}
	}
}

func TestBuildProducerConfig_DeliveryCallbackForcesReturns(t *testing.T) {
	client := &Client{options: newClientOptions()}

	options := newProducerOptions()
	options.ReturnSuccesses = false
	options.ReturnErrors = false

	config, _, err := buildProducerConfig(client, options, false)
	if err != nil {
		t.Fatalf("buildProducerConfig: %v", err)
	}

	if config.Producer.Return.Successes || config.Producer.Return.Errors {
		t.Fatal("without a callback the Return.* defaults must be kept")
	}

	WithDeliveryCallback(func(*ProducerMessage, error) {})(options)

	config, _, err = buildProducerConfig(client, options, false)
	if err != nil {
		t.Fatalf("buildProducerConfig: %v", err)
	}

	if !config.Producer.Return.Successes || !config.Producer.Return.Errors {
		t.Fatal("a callback must force Return.Successes and Return.Errors")
	}
}

// perMessageSyncProducer responds like a real sarama.SyncProducer: per-message
// failures are reported via sarama.ProducerErrors.
type perMessageSyncProducer struct {
	sarama.SyncProducer
	failTopic string
}

func (s perMessageSyncProducer) SendMessages(msgs []*sarama.ProducerMessage) error {
	var pErrs sarama.ProducerErrors

	for _, m := range msgs {
		if m.Topic == s.failTopic {
			pErrs = append(pErrs, &sarama.ProducerError{Msg: m, Err: errors.New("boom")})
		}
	}

	if len(pErrs) == 0 {
		return nil
	}

	return pErrs
}

func TestSyncProducer_DeliveryCallback(t *testing.T) {
	got := map[*ProducerMessage]error{}
	p := &syncProducer{
		producer: perMessageSyncProducer{failTopic: "bad"},
		callback: func(m *ProducerMessage, err error) { got[m] = err },
	}

	okMsg := &ProducerMessage{Topic: "ok"}
	badMsg := &ProducerMessage{Topic: "bad"}

	if err := p.Produce(context.Background(), okMsg, badMsg); !errors.Is(err, ErrProducerSend) {
		t.Fatalf("expected ErrProducerSend, got %v", err)
	}

	if len(got) != 2 || got[okMsg] != nil || got[badMsg] == nil {
		t.Fatalf("unexpected callback results: %v", got)
	}
}

func TestAsyncProducer_ProduceRespectsCtx(t *testing.T) {
	t.Run("ctx cancel", func(t *testing.T) {
		fake := newFakeAsyncProducer(false, false, nil)
		p := newFakeBackedProducer(t, fake, nil)

		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error, 1)

		go func() { errCh <- p.Produce(ctx, testMessage()) }()

		cancel()

		select {
		case err := <-errCh:
			if !errors.Is(err, context.Canceled) || !errors.Is(err, ErrProducerSend) {
				t.Fatalf("want ErrProducerSend + context.Canceled, got %v", err)
			}
		case <-time.After(testWait):
			t.Fatal("Produce did not return after ctx cancel")
		}
	})

	t.Run("Close unblocks Produce", func(t *testing.T) {
		fake := newFakeAsyncProducer(false, false, nil)
		p := newFakeBackedProducer(t, fake, nil)

		errCh := make(chan error, 1)

		go func() { errCh <- p.Produce(context.Background(), testMessage()) }()

		if err := p.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}

		select {
		case err := <-errCh:
			if !errors.Is(err, ErrProducerClosed) || !errors.Is(err, ErrProducerSend) {
				t.Fatalf("want ErrProducerSend + ErrProducerClosed, got %v", err)
			}
		case <-time.After(testWait):
			t.Fatal("Produce did not return after Close")
		}
	})
}
