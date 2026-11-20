package kafkakit

import (
	"errors"
	"testing"

	"github.com/IBM/sarama"
)

func TestParseCompression(t *testing.T) {
	cases := []struct {
		in   string
		want CompressionCodec
	}{
		{"none", CompressionNone},
		{"gzip", CompressionGZIP},
		{"snappy", CompressionSnappy},
		{"lz4", CompressionLZ4},
		{"zstd", CompressionZSTD},
	}

	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			got, err := ParseCompression(c.in)
			if err != nil {
				t.Fatalf("ParseCompression(%q): %v", c.in, err)
			}

			if got != c.want {
				t.Fatalf("ParseCompression(%q) = %v, want %v", c.in, got, c.want)
			}
		})
	}
}

func TestParseCompression_Invalid(t *testing.T) {
	_, err := ParseCompression("bzip2")
	if err == nil {
		t.Fatal("expected an error for an unknown codec")
	}
}

// CompressionZSTD (>= 2.1.0) and CompressionLZ4 (>= 0.10.0.0) require a
// codec-dependent protocol version; sarama.DefaultVersion is already 2.1.0,
// so both codecs pass without an explicit WithVersion.
func TestValidateCompressionVersion(t *testing.T) {
	cases := []struct {
		name    string
		codec   CompressionCodec
		version sarama.KafkaVersion
		wantErr bool
	}{
		{"none is always fine", CompressionNone, sarama.V0_9_0_0, false},
		{"gzip is always fine", CompressionGZIP, sarama.V0_9_0_0, false},
		{"snappy is always fine", CompressionSnappy, sarama.V0_9_0_0, false},
		{"lz4 below min version", CompressionLZ4, sarama.V0_9_0_0, true},
		{"lz4 at min version", CompressionLZ4, sarama.V0_10_0_0, false},
		{"lz4 with sarama default", CompressionLZ4, sarama.DefaultVersion, false},
		{"zstd below min version", CompressionZSTD, sarama.V0_11_0_2, true},
		{"zstd at min version", CompressionZSTD, sarama.V2_1_0_0, false},
		{"zstd with sarama default", CompressionZSTD, sarama.DefaultVersion, false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := validateCompressionVersion(c.codec, c.version)

			if c.wantErr {
				if err == nil {
					t.Fatalf("expected an error for %v at %s", c.codec, c.version)
				}

				if !errors.Is(err, ErrProducerCompressionVersion) {
					t.Fatalf("expected ErrProducerCompressionVersion, got %v", err)
				}

				return
			}

			if err != nil {
				t.Fatalf("unexpected error for %v at %s: %v", c.codec, c.version, err)
			}
		})
	}
}
