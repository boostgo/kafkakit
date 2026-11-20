package kafkakit

import (
	"errors"
	"strings"
	"testing"

	"github.com/IBM/sarama"
)

func buildClientOptions(t *testing.T, opts ...ClientOption) *ClientOptions {
	t.Helper()

	options := newClientOptions()
	for _, o := range opts {
		o(options)
	}

	return options
}

func TestWithVersion_Default(t *testing.T) {
	options := buildClientOptions(t, WithBrokers([]string{"localhost:9092"}))

	if err := options.validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}

	if options.Version != "" {
		t.Fatalf("Version must stay empty by default, got %q", options.Version)
	}

	config := options.saramaConfig()
	if config.Version != sarama.DefaultVersion {
		t.Fatalf("default version changed: got %s, want %s", config.Version, sarama.DefaultVersion)
	}

	if err := config.Validate(); err != nil {
		t.Fatalf("default config invalid: %v", err)
	}
}

func TestWithVersion_Valid(t *testing.T) {
	cases := []struct {
		in   string
		want sarama.KafkaVersion
		// sarama rejects client IDs containing dots for versions below 1.0.0, and the
		// library's default client ID is "kafkakit:<uuid>", so Validate is skipped there.
		validate bool
	}{
		{"2.3.0", sarama.V2_3_0_0, true},
		{"2.8.0", sarama.V2_8_0_0, true},
		{"3.6.0", sarama.V3_6_0_0, true},
		{"0.11.0.2", sarama.V0_11_0_2, false},
	}

	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			options := buildClientOptions(t, WithBrokers([]string{"localhost:9092"}), WithVersion(c.in))

			if err := options.validate(); err != nil {
				t.Fatalf("validate(%q): %v", c.in, err)
			}

			config := options.saramaConfig()
			if config.Version != c.want {
				t.Fatalf("version: got %s, want %s", config.Version, c.want)
			}

			if c.validate {
				if err := config.Validate(); err != nil {
					t.Fatalf("config.Validate: %v", err)
				}
			}
		})
	}
}

func TestWithVersion_Invalid(t *testing.T) {
	for _, in := range []string{"2.8", "kafka-2.8.0", "abc", "..."} {
		t.Run(in, func(t *testing.T) {
			options := buildClientOptions(t, WithBrokers([]string{"localhost:9092"}), WithVersion(in))

			err := options.validate()
			if err == nil {
				t.Fatalf("expected error for version %q", in)
			}

			if !errors.Is(err, ErrClientVersion) {
				t.Fatalf("expected ErrClientVersion, got %v", err)
			}

			if !strings.Contains(err.Error(), in) {
				t.Fatalf("error must mention the bad value %q: %v", in, err)
			}
		})
	}
}

// Client settings must not get lost in the config handed out to producers
// and consumers (Version was once lost exactly here).
func TestSaramaConfig_CarriesClientSettings(t *testing.T) {
	options := buildClientOptions(t,
		WithBrokers([]string{"localhost:9092"}),
		WithVersion("2.8.0"),
		WithClientID("test-client"),
		WithRackID("rack-1"),
		WithChannelBufferSize(512),
		WithAuthentication("user", "pass"),
	)

	if err := options.validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}

	config := options.saramaConfig()

	if config.Version != sarama.V2_8_0_0 {
		t.Fatalf("version: got %s", config.Version)
	}

	if config.RackID != "rack-1" {
		t.Fatalf("rack id: got %q", config.RackID)
	}

	if config.ChannelBufferSize != 512 {
		t.Fatalf("channel buffer size: got %d", config.ChannelBufferSize)
	}

	if !config.Net.SASL.Enable || config.Net.SASL.User != "user" {
		t.Fatalf("sasl not carried: %+v", config.Net.SASL)
	}

	if !strings.HasPrefix(config.ClientID, "test-client:") {
		t.Fatalf("client id: got %q", config.ClientID)
	}
}

func TestWithTraceHeader(t *testing.T) {
	for _, tc := range []struct {
		name    string
		opts    []ClientOption
		want    string
		wantErr bool
	}{
		{name: "default", want: DefaultTraceHeader},
		{name: "custom", opts: []ClientOption{WithTraceHeader("x-request-id")}, want: "x-request-id"},
		{name: "empty keeps the default", opts: []ClientOption{WithTraceHeader("")}, want: DefaultTraceHeader},
		{name: "last call wins", opts: []ClientOption{WithTraceHeader("a"), WithTraceHeader("b")}, want: "b"},
		{name: "space", opts: []ClientOption{WithTraceHeader("trace id")}, wantErr: true},
		{name: "surrounding whitespace", opts: []ClientOption{WithTraceHeader(" trace-id")}, wantErr: true},
		{name: "control character", opts: []ClientOption{WithTraceHeader("trace\x00id")}, wantErr: true},
		{name: "invalid UTF-8", opts: []ClientOption{WithTraceHeader("trace\xffid")}, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			options := buildClientOptions(t, append([]ClientOption{WithBrokers([]string{"localhost:9092"})}, tc.opts...)...)

			err := options.validate()
			if tc.wantErr {
				if !errors.Is(err, ErrClientTraceHeader) {
					t.Fatalf("validate: got %v, want ErrClientTraceHeader", err)
				}

				return
			}

			if err != nil {
				t.Fatalf("validate: %v", err)
			}

			if got := (&Client{options: options}).TraceHeader(); got != tc.want {
				t.Fatalf("Client.TraceHeader() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestClientTraceHeader_ZeroValueOptions(t *testing.T) {
	if got := (&Client{}).TraceHeader(); got != DefaultTraceHeader {
		t.Fatalf("Client.TraceHeader() without options = %q, want %q", got, DefaultTraceHeader)
	}

	if got := (&Client{options: &ClientOptions{}}).TraceHeader(); got != DefaultTraceHeader {
		t.Fatalf("Client.TraceHeader() with empty name = %q, want %q", got, DefaultTraceHeader)
	}
}
