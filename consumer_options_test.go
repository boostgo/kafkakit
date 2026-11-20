package kafkakit

import (
	"errors"
	"strings"
	"testing"

	"github.com/IBM/sarama"
)

func buildConsumerOptions(t *testing.T, opts ...ConsumerOption) *ConsumerOptions {
	t.Helper()

	options := newConsumerOptions()
	for _, o := range opts {
		o(options)
	}

	return options
}

// The config is built exactly the way NewConsumer builds it:
// client options -> saramaConfig -> consumer applySaramaConfig.
func consumerConfig(t *testing.T, clientOpts []ClientOption, consumerOpts []ConsumerOption) (*sarama.Config, error) {
	t.Helper()

	client := buildClientOptions(t, append([]ClientOption{WithBrokers([]string{"localhost:9092"})}, clientOpts...)...)
	if err := client.validate(); err != nil {
		t.Fatalf("client options validate: %v", err)
	}

	config := client.saramaConfig()
	// sarama rejects client IDs containing dots for versions below 1.0.0; that is
	// unrelated to what is being tested here.
	config.ClientID = "kafka-test"

	consumer := buildConsumerOptions(t, consumerOpts...)

	return config, consumer.applySaramaConfig(config)
}

// Main case: without WithVersion static membership fails sarama's
// validation (Consumer.Group.InstanceId needs Version >= 2.3).
func TestGroupInstanceID_WithoutVersion(t *testing.T) {
	config, err := consumerConfig(t, nil, []ConsumerOption{
		WithConsumerGroupID("test-group"),
		WithConsumerTopics("test-topic"),
		WithConsumerGroupInstanceID("pod-0"),
	})

	if err == nil {
		t.Fatal("expected error for static membership on default version")
	}

	if !errors.Is(err, ErrConsumerGroupInstanceIDVersion) {
		t.Fatalf("expected ErrConsumerGroupInstanceIDVersion, got %v", err)
	}

	if !strings.Contains(err.Error(), "2.3.0") {
		t.Fatalf("error must hint the required version: %v", err)
	}

	// The library's check must agree with sarama's own verdict.
	if saramaErr := config.Validate(); saramaErr == nil {
		t.Fatal("sarama must reject InstanceId on version < 2.3.0")
	}
}

// With WithVersion(">= 2.3.0") both our check and sarama.Validate() pass.
func TestGroupInstanceID_WithVersion(t *testing.T) {
	for _, version := range []string{"2.3.0", "2.8.0", "3.6.0"} {
		t.Run(version, func(t *testing.T) {
			config, err := consumerConfig(t,
				[]ClientOption{WithVersion(version)},
				[]ConsumerOption{
					WithConsumerGroupID("test-group"),
					WithConsumerTopics("test-topic"),
					WithConsumerGroupInstanceID("pod-0"),
				},
			)
			if err != nil {
				t.Fatalf("applySaramaConfig: %v", err)
			}

			if config.Consumer.Group.InstanceId != "pod-0" {
				t.Fatalf("instance id: got %q", config.Consumer.Group.InstanceId)
			}

			if err := config.Validate(); err != nil {
				t.Fatalf("sarama config.Validate: %v", err)
			}
		})
	}
}

// An explicitly set version below 2.3.0 must be rejected as well.
func TestGroupInstanceID_TooOldVersion(t *testing.T) {
	_, err := consumerConfig(t,
		[]ClientOption{WithVersion("2.2.0")},
		[]ConsumerOption{
			WithConsumerGroupID("test-group"),
			WithConsumerTopics("test-topic"),
			WithConsumerGroupInstanceID("pod-0"),
		},
	)

	if !errors.Is(err, ErrConsumerGroupInstanceIDVersion) {
		t.Fatalf("expected ErrConsumerGroupInstanceIDVersion, got %v", err)
	}
}

// Without static membership an old version keeps working:
// the default behavior does not change.
func TestConsumerConfig_NoInstanceID(t *testing.T) {
	config, err := consumerConfig(t, nil, []ConsumerOption{
		WithConsumerGroupID("test-group"),
		WithConsumerTopics("test-topic"),
	})
	if err != nil {
		t.Fatalf("applySaramaConfig: %v", err)
	}

	if config.Version != sarama.DefaultVersion {
		t.Fatalf("version: got %s, want %s", config.Version, sarama.DefaultVersion)
	}

	if err := config.Validate(); err != nil {
		t.Fatalf("sarama config.Validate: %v", err)
	}
}
