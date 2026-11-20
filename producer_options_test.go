package kafkakit

import "testing"

// Defaults that must survive refactoring: ReturnSuccesses defaults to false
// (for async) and retry buffers are bounded by default; regressing either
// would hurt existing users.
func TestNewProducerOptions_Defaults(t *testing.T) {
	options := newProducerOptions()

	if options.ReturnSuccesses {
		t.Fatal("ReturnSuccesses must default to false")
	}

	if !options.ReturnErrors {
		t.Fatal("ReturnErrors must default to true")
	}

	if options.RetryMaxBufferLength != 4096 {
		t.Fatalf("RetryMaxBufferLength: got %d, want 4096", options.RetryMaxBufferLength)
	}

	if options.RetryMaxBufferBytes != 32*1024*1024 {
		t.Fatalf("RetryMaxBufferBytes: got %d, want %d", options.RetryMaxBufferBytes, 32*1024*1024)
	}

	if options.Compression != CompressionNone {
		t.Fatalf("Compression: got %v, want CompressionNone", options.Compression)
	}

	if options.CompressionLevel != CompressionLevelDefault {
		t.Fatalf("CompressionLevel: got %d, want %d", options.CompressionLevel, CompressionLevelDefault)
	}

	if options.returnSuccessesSet {
		t.Fatal("returnSuccessesSet must stay false until WithProducerReturnSuccesses is called")
	}
}

func TestWithProducerReturnSuccesses_MarksExplicit(t *testing.T) {
	options := newProducerOptions()
	WithProducerReturnSuccesses(false)(options)

	if !options.returnSuccessesSet {
		t.Fatal("WithProducerReturnSuccesses must mark the option as explicitly set, even for false")
	}
}

func TestWithProducerCompression(t *testing.T) {
	options := newProducerOptions()
	WithProducerCompression(CompressionZSTD)(options)
	WithProducerCompressionLevel(5)(options)

	if options.Compression != CompressionZSTD {
		t.Fatalf("Compression: got %v, want CompressionZSTD", options.Compression)
	}

	if options.CompressionLevel != 5 {
		t.Fatalf("CompressionLevel: got %d, want 5", options.CompressionLevel)
	}
}
