package kafkakit

import (
	"testing"

	"go.uber.org/goleak"
)

// goleakOptions ignores the background goroutine of go-metrics: sarama.NewConfig()
// starts a global meterArbiter that lives until the process exits.
func goleakOptions() []goleak.Option {
	return []goleak.Option{
		goleak.IgnoreTopFunction("github.com/rcrowley/go-metrics.(*meterArbiter).tick"),
	}
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, goleakOptions()...)
}
