package kafkakit

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/IBM/sarama/mocks"
	"github.com/rs/zerolog"
)

const (
	testGoroutines        = 16
	testMessagesPerWorker = 32
	testTraceID           = "trace-id-42"
)

func headersOf(msg *sarama.ProducerMessage) map[string]string {
	headers := make(map[string]string, len(msg.Headers))
	for _, header := range msg.Headers {
		headers[string(header.Key)] = string(header.Value)
	}

	return headers
}

func TestSaramaMessage_DoesNotMutateSource(t *testing.T) {
	source := &ProducerMessage{
		Topic:   "topic",
		Key:     "key",
		Value:   []byte("value"),
		Headers: map[string]string{"a": "1", DefaultTraceHeader: "old"},
	}

	msg := source.saramaMessage("", testTraceID)
	got := headersOf(msg)

	if got["a"] != "1" {
		t.Fatalf("user header lost: %v", got)
	}

	if got[DefaultTraceHeader] != testTraceID {
		t.Fatalf("trace header not applied: %v", got)
	}

	if len(msg.Headers) != 2 {
		t.Fatalf("duplicated trace header: %v", msg.Headers)
	}

	if len(source.Headers) != 2 || source.Headers[DefaultTraceHeader] != "old" {
		t.Fatalf("source headers were mutated: %v", source.Headers)
	}
}

func TestSaramaMessage_TraceHeader(t *testing.T) {
	const custom = "x-request-id"

	for _, tc := range []struct {
		name        string
		traceHeader string
		traceID     string
		headers     map[string]string
		want        map[string]string
	}{
		{
			name:    "default name",
			traceID: testTraceID,
			want:    map[string]string{DefaultTraceHeader: testTraceID},
		},
		{
			name:        "default name passed explicitly",
			traceHeader: DefaultTraceHeader,
			traceID:     testTraceID,
			want:        map[string]string{DefaultTraceHeader: testTraceID},
		},
		{
			name:        "custom name",
			traceHeader: custom,
			traceID:     testTraceID,
			want:        map[string]string{custom: testTraceID},
		},
		{
			name:        "context trace overrides the message header",
			traceHeader: custom,
			traceID:     testTraceID,
			headers:     map[string]string{custom: "from-message", "a": "1"},
			want:        map[string]string{custom: testTraceID, "a": "1"},
		},
		{
			name:        "custom name leaves a default-named message header alone",
			traceHeader: custom,
			traceID:     testTraceID,
			headers:     map[string]string{DefaultTraceHeader: "user"},
			want:        map[string]string{DefaultTraceHeader: "user", custom: testTraceID},
		},
		{
			name:    "no trace in context keeps the message header",
			headers: map[string]string{DefaultTraceHeader: "from-message"},
			want:    map[string]string{DefaultTraceHeader: "from-message"},
		},
		{
			name: "no trace and no headers",
			want: map[string]string{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			msg := (&ProducerMessage{Topic: "t", Headers: tc.headers}).saramaMessage(tc.traceHeader, tc.traceID)

			if len(msg.Headers) != len(tc.want) {
				t.Fatalf("headers = %v, want %v", headersOf(msg), tc.want)
			}

			got := headersOf(msg)
			for k, v := range tc.want {
				if got[k] != v {
					t.Fatalf("header %q = %q, want %q (all: %v)", k, got[k], v, got)
				}
			}
		})
	}
}

// Producers built from a client use the client's trace header; a message
// produced and consumed with the same name round-trips the trace ID.
func TestProducer_TraceHeaderFromClient(t *testing.T) {
	const custom = "x-correlation-id"

	for _, tc := range []struct {
		name   string
		opts   []ClientOption
		header string
	}{
		{"default", nil, DefaultTraceHeader},
		{"custom", []ClientOption{WithTraceHeader(custom)}, custom},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := testClient(t, tc.opts...)
			if got := client.TraceHeader(); got != tc.header {
				t.Fatalf("Client.TraceHeader() = %q, want %q", got, tc.header)
			}

			var sent *sarama.ProducerMessage

			mock := mocks.NewSyncProducer(t, sarama.NewConfig())
			mock.ExpectSendMessageWithMessageCheckerFunctionAndSucceed(func(msg *sarama.ProducerMessage) error {
				sent = msg
				return nil
			})

			producer := &syncProducer{producer: mock, traceHeader: client.TraceHeader()}
			ctx := context.WithValue(context.Background(), TraceKey, testTraceID) //nolint:staticcheck

			if err := producer.Produce(ctx, &ProducerMessage{Topic: "t"}); err != nil {
				t.Fatalf("produce: %v", err)
			}

			if len(sent.Headers) != 1 || string(sent.Headers[0].Key) != tc.header ||
				string(sent.Headers[0].Value) != testTraceID {
				t.Fatalf("sent headers = %v, want only %s=%s", headersOf(sent), tc.header, testTraceID)
			}

			raw := &sarama.ConsumerMessage{Headers: []*sarama.RecordHeader{&sent.Headers[0]}}
			if got := newConsumerMessage(raw, nil, client.TraceHeader()).TraceID(); got != testTraceID {
				t.Fatalf("round trip trace = %q, want %q", got, testTraceID)
			}
		})
	}
}

func TestTraceIDFromContext(t *testing.T) {
	if got := traceIDFromContext(context.Background()); got != "" {
		t.Fatalf("empty context: got %q", got)
	}

	ctx := context.WithValue(context.Background(), TraceKey, testTraceID) //nolint:staticcheck
	if got := traceIDFromContext(ctx); got != testTraceID {
		t.Fatalf("string trace: got %q", got)
	}

	ctx = context.WithValue(context.Background(), TraceKey, 42) //nolint:staticcheck
	if got := traceIDFromContext(ctx); got != "" {
		t.Fatalf("unsupported type must yield empty trace, got %q", got)
	}
}

func newTestAsyncProducer(t *testing.T, expectations int, check mocks.MessageChecker) (*asyncProducer, *mocks.AsyncProducer) {
	t.Helper()

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	mock := mocks.NewAsyncProducer(t, config)
	for range expectations {
		mock.ExpectInputWithMessageCheckerFunctionAndSucceed(check)
	}

	producer := &asyncProducer{
		producer: mock,
		logger:   zerolog.Nop(),
		done:     make(chan struct{}),
	}

	producer.start()

	t.Cleanup(func() { _ = producer.Close() })

	return producer, mock
}

// Worker pool: N goroutines send through one Dispatcher, sharing the same
// header map and one context with a trace ID. This used to cause
// "concurrent map writes" in Produce.
func TestAsyncProducer_ConcurrentDispatch(t *testing.T) {
	var delivered atomic.Int64

	check := func(msg *sarama.ProducerMessage) error {
		headers := headersOf(msg)
		if headers[DefaultTraceHeader] != testTraceID || len(msg.Headers) != 2 {
			return errors.New("trace header missing or duplicated")
		}

		if headers["shared"] != "header" {
			return errors.New("shared header missing")
		}

		delivered.Add(1)

		return nil
	}

	total := testGoroutines * testMessagesPerWorker
	producer, _ := newTestAsyncProducer(t, total, check)

	dispatcher := NewDispatcher(producer, "test-topic", json.Marshal)
	sharedHeaders := map[string]string{"shared": "header"}
	ctx := context.WithValue(context.Background(), TraceKey, testTraceID) //nolint:staticcheck

	var wg sync.WaitGroup
	for worker := range testGoroutines {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for i := range testMessagesPerWorker {
				key := strconv.Itoa(worker) + ":" + strconv.Itoa(i)
				if err := dispatcher.Dispatch(ctx, key, map[string]int{"i": i}, sharedHeaders); err != nil {
					t.Errorf("dispatch: %v", err)
					return
				}
			}
		}()
	}

	wg.Wait()

	if err := producer.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if got := delivered.Load(); got != int64(total) {
		t.Fatalf("delivered: got %d, want %d", got, total)
	}

	if len(sharedHeaders) != 1 || sharedHeaders["shared"] != "header" {
		t.Fatalf("shared headers map was mutated: %v", sharedHeaders)
	}
}

// DispatchMany with a shared message: the same *ProducerMessage is sent from
// several goroutines at once and must not be mutated.
func TestAsyncProducer_ConcurrentDispatchManySharedMessage(t *testing.T) {
	total := testGoroutines * testMessagesPerWorker
	producer, _ := newTestAsyncProducer(t, total, nil)

	dispatcher := NewDispatcher(producer, "test-topic", json.Marshal)
	shared := &ProducerMessage{
		Topic:   "test-topic",
		Key:     "key",
		Value:   []byte(`{"i":1}`),
		Headers: map[string]string{"shared": "header"},
	}
	ctx := context.WithValue(context.Background(), TraceKey, testTraceID) //nolint:staticcheck

	var wg sync.WaitGroup
	for range testGoroutines {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for range testMessagesPerWorker {
				if err := dispatcher.DispatchMany(ctx, shared); err != nil {
					t.Errorf("dispatch many: %v", err)
					return
				}
			}
		}()
	}

	wg.Wait()

	if err := producer.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if len(shared.Headers) != 1 {
		t.Fatalf("shared message headers were mutated: %v", shared.Headers)
	}
}

func TestSyncProducer_ConcurrentProduce(t *testing.T) {
	config := sarama.NewConfig()
	mock := mocks.NewSyncProducer(t, config)

	total := testGoroutines * testMessagesPerWorker
	for range total {
		mock.ExpectSendMessageAndSucceed()
	}

	producer := &syncProducer{producer: mock}
	dispatcher := NewDispatcher(producer, "test-topic", json.Marshal)
	sharedHeaders := map[string]string{"shared": "header"}
	ctx := context.WithValue(context.Background(), TraceKey, testTraceID) //nolint:staticcheck

	var wg sync.WaitGroup
	for range testGoroutines {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for i := range testMessagesPerWorker {
				if err := dispatcher.Dispatch(ctx, "key", map[string]int{"i": i}, sharedHeaders); err != nil {
					t.Errorf("dispatch: %v", err)
					return
				}
			}
		}()
	}

	wg.Wait()

	if err := mock.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if len(sharedHeaders) != 1 {
		t.Fatalf("shared headers map was mutated: %v", sharedHeaders)
	}
}

func TestAsyncProducer_CloseIsIdempotentAndBlocksProduce(t *testing.T) {
	producer, _ := newTestAsyncProducer(t, 0, nil)

	if err := producer.Close(); err != nil {
		t.Fatalf("first close: %v", err)
	}

	if err := producer.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}

	err := producer.Produce(context.Background(), &ProducerMessage{Topic: "test-topic"})
	if !errors.Is(err, ErrProducerClosed) {
		t.Fatalf("expected ErrProducerClosed, got %v", err)
	}
}

// testClient builds a *Client without network access: buildProducerConfig and
// compression version validation only touch client.options (and client.logger
// for the warning); they need no real connection.
func testClient(t *testing.T, opts ...ClientOption) *Client {
	t.Helper()

	options := buildClientOptions(t, append([]ClientOption{WithBrokers([]string{"localhost:9092"})}, opts...)...)

	return &Client{
		logger:  zerolog.Nop(),
		brokers: options.Brokers,
		options: options,
	}
}

// The codec and compression level must reach the final sarama.Config on both the
// sync (NewSyncProducer) and async (NewAsyncProducer) paths; both use
// buildProducerConfig, but the two paths have diverged before
// (see MaxBufferLength/MaxBufferBytes).
func TestBuildProducerConfig_CompressionReachesConfig(t *testing.T) {
	client := testClient(t)

	options := newProducerOptions()
	WithProducerCompression(CompressionLZ4)(options)
	WithProducerCompressionLevel(5)(options)

	for _, sync := range []bool{true, false} {
		config, _, err := buildProducerConfig(client, options, sync)
		if err != nil {
			t.Fatalf("sync=%v: buildProducerConfig: %v", sync, err)
		}

		if config.Producer.Compression != sarama.CompressionLZ4 {
			t.Fatalf("sync=%v: compression not applied: got %v", sync, config.Producer.Compression)
		}

		if config.Producer.CompressionLevel != 5 {
			t.Fatalf("sync=%v: compression level not applied: got %d", sync, config.Producer.CompressionLevel)
		}
	}
}

// sync=true forces Return.Successes=true regardless of the option: this is a
// structural requirement of sarama.SyncProducer, not a default. sync=false
// (async) must pass the option value through unchanged.
func TestBuildProducerConfig_SyncForcesReturnSuccesses(t *testing.T) {
	client := testClient(t)

	t.Run("sync default", func(t *testing.T) {
		config, warn, err := buildProducerConfig(client, newProducerOptions(), true)
		if err != nil {
			t.Fatalf("buildProducerConfig: %v", err)
		}

		if !config.Producer.Return.Successes {
			t.Fatal("sync producer config must force Return.Successes=true")
		}

		if warn {
			t.Fatal("default ReturnSuccesses=false must not trigger a warning (not explicitly requested)")
		}
	})

	t.Run("sync explicit false", func(t *testing.T) {
		options := newProducerOptions()
		WithProducerReturnSuccesses(false)(options)

		config, warn, err := buildProducerConfig(client, options, true)
		if err != nil {
			t.Fatalf("buildProducerConfig: %v", err)
		}

		if !config.Producer.Return.Successes {
			t.Fatal("sync producer config must force Return.Successes=true even when explicitly disabled")
		}

		if !warn {
			t.Fatal("explicit WithProducerReturnSuccesses(false) for a sync producer must be reported as ignored")
		}
	})

	t.Run("async respects the option", func(t *testing.T) {
		options := newProducerOptions()
		WithProducerReturnSuccesses(true)(options)

		config, _, err := buildProducerConfig(client, options, false)
		if err != nil {
			t.Fatalf("buildProducerConfig: %v", err)
		}

		if !config.Producer.Return.Successes {
			t.Fatal("async producer must honor ReturnSuccesses=true")
		}
	})

	t.Run("async default is false", func(t *testing.T) {
		config, _, err := buildProducerConfig(client, newProducerOptions(), false)
		if err != nil {
			t.Fatalf("buildProducerConfig: %v", err)
		}

		if config.Producer.Return.Successes {
			t.Fatal("async producer must default to ReturnSuccesses=false")
		}
	})
}

// CompressionZSTD + WithVersion("0.11.0.2") -> a clear error naming the minimum
// version; it is returned before any connection attempt, so no real broker
// is needed.
func TestNewSyncProducer_CompressionVersionValidation(t *testing.T) {
	client := testClient(t, WithVersion("0.11.0.2"))

	_, err := NewSyncProducer(client, WithProducerCompression(CompressionZSTD))
	if err == nil {
		t.Fatal("expected an error for zstd below its minimum protocol version")
	}

	if !errors.Is(err, ErrProducerCompressionVersion) {
		t.Fatalf("expected ErrProducerCompressionVersion, got %v", err)
	}

	if !strings.Contains(err.Error(), "2.1.0") {
		t.Fatalf("error must name the minimum required version: %v", err)
	}
}

// CompressionZSTD without WithVersion -> no error: sarama.DefaultVersion is already
// 2.1.0. Checked via buildProducerConfig so that no network is needed.
func TestBuildProducerConfig_CompressionZSTDDefaultVersionOK(t *testing.T) {
	client := testClient(t)

	options := newProducerOptions()
	WithProducerCompression(CompressionZSTD)(options)

	if _, _, err := buildProducerConfig(client, options, false); err != nil {
		t.Fatalf("zstd must be allowed under sarama.DefaultVersion: %v", err)
	}
}

// newLocalBroker starts a TCP listener that accepts and immediately drops
// connections. It is not Kafka, but all the test needs is for sarama to
// reach the network (i.e. verifyProducerConfig has already passed) and
// get a network/protocol error rather than a ConfigurationError about
// Return.Successes.
func newLocalBroker(t *testing.T) string {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("no local TCP available: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			_ = conn.Close()
		}
	}()

	return ln.Addr().String()
}

// Regression: ReturnSuccesses defaulting to false must not make
// NewSyncProducer fail with "Return.Successes must be true to be used in a
// SyncProducer", neither with the default configuration nor with an explicit
// WithProducerReturnSuccesses(false).
func TestNewSyncProducer_ReturnSuccessesForced(t *testing.T) {
	addr := newLocalBroker(t)

	cases := []struct {
		name string
		opts []ProducerOption
	}{
		{"default", nil},
		{"explicit false", []ProducerOption{WithProducerReturnSuccesses(false)}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			client := testClient(t, WithDialTimeout(2*time.Second), WithReadTimeout(2*time.Second))
			client.brokers = []string{addr}
			client.options.Brokers = []string{addr}

			_, err := NewSyncProducer(client, c.opts...)
			if err == nil {
				t.Fatal("expected an error: the local listener is not a real broker")
			}

			if strings.Contains(err.Error(), "Return.Successes") {
				t.Fatalf("sync producer must force Return.Successes=true, got a configuration error: %v", err)
			}
		})
	}
}
