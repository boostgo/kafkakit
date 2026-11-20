package kafkakit

import (
	"fmt"

	"github.com/IBM/sarama"
)

// CompressionCodec is the record-batch compression codec used by producers.
// Its values match sarama.CompressionCodec, but the type is local so that
// callers do not need to import sarama just to pick a codec.
type CompressionCodec int8

const (
	// CompressionNone disables compression. Default.
	CompressionNone CompressionCodec = iota
	// CompressionGZIP compresses with GZIP.
	CompressionGZIP
	// CompressionSnappy compresses with Snappy.
	CompressionSnappy
	// CompressionLZ4 compresses with LZ4. Requires protocol version >= 0.10.0.0.
	CompressionLZ4
	// CompressionZSTD compresses with ZSTD. Requires protocol version >= 2.1.0.
	// sarama.DefaultVersion (see [WithVersion]) is exactly 2.1.0, so both LZ4
	// and ZSTD work out of the box without raising the version.
	CompressionZSTD

	// CompressionLevelDefault lets the codec pick its compression level
	// (same as sarama.CompressionLevelDefault).
	CompressionLevelDefault = -1000
)

// sarama converts the codec into its sarama representation.
func (c CompressionCodec) sarama() sarama.CompressionCodec {
	return sarama.CompressionCodec(c)
}

// ParseCompression parses a codec name (as it typically appears in service
// configuration) into a [CompressionCodec]. Accepted values: none, gzip,
// snappy, lz4, zstd.
func ParseCompression(s string) (CompressionCodec, error) {
	var codec sarama.CompressionCodec

	if err := codec.UnmarshalText([]byte(s)); err != nil {
		return CompressionNone, fmt.Errorf("kafka.compression.parse: %w", err)
	}

	return CompressionCodec(codec), nil
}

// minCompressionVersion returns the minimum protocol version required by the
// codec and whether there is such a requirement at all.
func minCompressionVersion(codec CompressionCodec) (version sarama.KafkaVersion, required bool) {
	switch codec {
	case CompressionZSTD:
		return sarama.V2_1_0_0, true
	case CompressionLZ4:
		return sarama.V0_10_0_0, true
	default:
		return sarama.KafkaVersion{}, false
	}
}

// validateCompressionVersion checks that the protocol version (see
// ClientOptions.Version / WithVersion) supports the selected codec.
//
// sarama itself returns a ConfigurationError for an incompatible codec/version
// pair, but that error does not say how to fix it; this one names the minimum
// version and the option that raises it.
func validateCompressionVersion(codec CompressionCodec, version sarama.KafkaVersion) error {
	minVersion, required := minCompressionVersion(codec)
	if !required || version.IsAtLeast(minVersion) {
		return nil
	}

	return fmt.Errorf(
		"%w: %s requires protocol version >= %s, got %s; set kafkakit.WithVersion(%q) or newer",
		ErrProducerCompressionVersion, codec.sarama(), minVersion, version, minVersion,
	)
}
