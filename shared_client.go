package kafkakit

import (
	"fmt"
	"reflect"
	"slices"

	"github.com/IBM/sarama"
)

// Shared sarama client.
//
// sarama.New*FromClient use the client's own config (sarama.Client.Config()),
// while a Client is built without Producer/Consumer sections. So that producer
// or consumer options are never silently lost, the sections are set when the
// Client is created:
//
//	client, _ := kafkakit.NewClient(ctx, logger,
//	    kafkakit.WithBrokers(brokers),
//	    kafkakit.WithClientProducerOptions(kafkakit.WithProducerCompression(kafkakit.CompressionZSTD)),
//	    kafkakit.WithClientConsumerOptions(kafkakit.WithConsumerInitialOffset(kafkakit.OffsetOldest)),
//	)
//	producer, _ := kafkakit.NewAsyncProducer(client, kafkakit.WithProducerClient(client))
//
// Producers/consumers built on such a client (WithProducerClient /
// WithConsumerClient) compare their options with the client's section and
// return ErrSharedClientConfig on mismatch, instead of silently applying
// different values.

// WithClientProducerOptions enables a Producer section on the client, built
// from the given ProducerOptions (everything else uses producer defaults). It
// is required to use the client as a shared client for producers
// ([WithProducerClient]).
//
// Return.Successes and Return.Errors are always true in the client section, so
// that both sync producers (sarama requires it structurally) and async
// producers with WithDeliveryCallback work on the same client. Async producers
// without a callback simply drain and discard successes. The section only
// affects producers built on this client; regular producers (without
// WithProducerClient) and Client.GetConfig do not see it.
func WithClientProducerOptions(opts ...ProducerOption) ClientOption {
	return func(o *ClientOptions) {
		o.sharedProducer = true
		o.sharedProducerOpts = append(o.sharedProducerOpts, opts...)
	}
}

// WithClientConsumerOptions enables a Consumer section on the client, built
// from the given ConsumerOptions (everything else uses consumer defaults). It
// is required to use the client as a shared client for consumers
// ([WithConsumerClient]). GroupID, Topics, Name, Validate, HandlerTimeout and
// OnConsumerClose are not part of the section; set them on each consumer.
func WithClientConsumerOptions(opts ...ConsumerOption) ClientOption {
	return func(o *ClientOptions) {
		o.sharedConsumer = true
		o.sharedConsumerOpts = append(o.sharedConsumerOpts, opts...)
	}
}

// applySharedSections writes the Producer/Consumer sections into the client config.
func (o *ClientOptions) applySharedSections(config *sarama.Config) error {
	if o.sharedProducer {
		po := newProducerOptions()
		for _, opt := range o.sharedProducerOpts {
			opt(po)
		}

		if err := po.validate(); err != nil {
			return err
		}

		// The client section always supports both sync and async-with-callback.
		po.ReturnSuccesses, po.ReturnErrors = true, true

		if _, err := po.applySaramaConfig(config, false); err != nil {
			return err
		}
	}

	if o.sharedConsumer {
		co := newConsumerOptions()
		for _, opt := range o.sharedConsumerOpts {
			opt(co)
		}

		if err := co.applySaramaConfig(config); err != nil {
			return fmt.Errorf("%w: %w", ErrConsumerNew, err)
		}
	}

	return nil
}

// checkSharedProducer checks that the producer options match the Producer
// section of the shared client. Return.* are not compared: they are always
// true on the client.
func checkSharedProducer(client *Client, options *ProducerOptions, sync bool) error {
	if !client.options.sharedProducer {
		return fmt.Errorf("%w: the client was created without WithClientProducerOptions", ErrSharedClientConfig)
	}

	want, _, err := buildProducerConfig(client, options, sync)
	if err != nil {
		return err
	}

	have := client.client.Config()

	if !producerSectionsEqual(want, have) {
		return fmt.Errorf(
			"%w: producer options differ from the producer section of the shared client; "+
				"pass them to WithClientProducerOptions instead",
			ErrSharedClientConfig)
	}

	return nil
}

// checkSharedConsumer does the same for consumers: the whole Consumer section.
func checkSharedConsumer(client *Client, options *ConsumerOptions) error {
	if !client.options.sharedConsumer {
		return fmt.Errorf("%w: the client was created without WithClientConsumerOptions", ErrSharedClientConfig)
	}

	want := client.GetConfig()
	if err := options.applySaramaConfig(want); err != nil {
		return fmt.Errorf("%w: %w", ErrConsumerNew, err)
	}

	have := client.client.Config()

	if !consumerSectionsEqual(want, have) {
		return fmt.Errorf(
			"%w: consumer options differ from the consumer section of the shared client; "+
				"pass them to WithClientConsumerOptions instead",
			ErrSharedClientConfig)
	}

	return nil
}

func producerSectionsEqual(a, b *sarama.Config) bool {
	ap, bp := a.Producer, b.Producer

	if !sameFunc(ap.Partitioner, bp.Partitioner) ||
		!sameFunc(ap.Retry.BackoffFunc, bp.Retry.BackoffFunc) ||
		!sameFunc(ap.Transaction.Retry.BackoffFunc, bp.Transaction.Retry.BackoffFunc) {
		return false
	}

	ap.Partitioner, bp.Partitioner = nil, nil
	ap.Retry.BackoffFunc, bp.Retry.BackoffFunc = nil, nil
	ap.Transaction.Retry.BackoffFunc, bp.Transaction.Retry.BackoffFunc = nil, nil
	ap.Return, bp.Return = struct {
		Successes bool
		Errors    bool
	}{}, struct {
		Successes bool
		Errors    bool
	}{}

	return reflect.DeepEqual(ap, bp)
}

func consumerSectionsEqual(a, b *sarama.Config) bool {
	ac, bc := a.Consumer, b.Consumer

	if !sameFunc(ac.Retry.BackoffFunc, bc.Retry.BackoffFunc) {
		return false
	}

	// Balance strategies contain funcs (DeepEqual cannot compare them), so
	// compare them by name and order.
	aNames := strategyNames(ac.Group.Rebalance.GroupStrategies)
	bNames := strategyNames(bc.Group.Rebalance.GroupStrategies)

	if !slices.Equal(aNames, bNames) {
		return false
	}

	ac.Retry.BackoffFunc, bc.Retry.BackoffFunc = nil, nil
	ac.Group.Rebalance.GroupStrategies, bc.Group.Rebalance.GroupStrategies = nil, nil
	ac.Group.Rebalance.Strategy, bc.Group.Rebalance.Strategy = nil, nil

	return reflect.DeepEqual(ac, bc)
}

func strategyNames(strategies []sarama.BalanceStrategy) []string {
	names := make([]string, 0, len(strategies))
	for _, strategy := range strategies {
		names = append(names, strategy.Name())
	}

	return names
}

// sameFunc compares funcs by code pointer: both nil or both the same function.
func sameFunc(a, b any) bool {
	av, bv := reflect.ValueOf(a), reflect.ValueOf(b)

	if av.IsNil() || bv.IsNil() {
		return av.IsNil() == bv.IsNil()
	}

	return av.Pointer() == bv.Pointer()
}

func newSharedSyncProducer(options *ProducerOptions) (sarama.SyncProducer, error) {
	if err := checkSharedProducer(options.client, options, true); err != nil {
		return nil, err
	}

	return sarama.NewSyncProducerFromClient(options.client.client)
}

func newSharedAsyncProducer(options *ProducerOptions) (sarama.AsyncProducer, error) {
	if err := checkSharedProducer(options.client, options, false); err != nil {
		return nil, err
	}

	return sarama.NewAsyncProducerFromClient(options.client.client)
}

func newSharedConsumerGroup(options *ConsumerOptions) (sarama.ConsumerGroup, error) {
	if err := checkSharedConsumer(options.client, options); err != nil {
		return nil, err
	}

	return sarama.NewConsumerGroupFromClient(options.GroupID, options.client.client)
}
