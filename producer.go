package kafka

import (
	"context"
	"errors"

	"github.com/twmb/franz-go/pkg/kgo"
)

// ProduceOptions mirrors the keyword arguments of ruby-kafka's
// producer.produce(value, topic:, key:, partition:, headers:).
type ProduceOptions struct {
	// Topic is the destination topic. Required.
	Topic string
	// Key is the optional partitioning/message key.
	Key []byte
	// Partition, when non-nil, pins the message to an explicit partition;
	// otherwise the partition is chosen by key hash (or balanced).
	Partition *int32
	// Headers is the optional set of record headers.
	Headers map[string][]byte
}

// Producer is a buffering producer, the analogue of the object returned by
// kafka.producer. Messages are buffered by [Producer.Produce] and flushed by
// [Producer.DeliverMessages]; [Producer.Shutdown] releases the connection.
type Producer struct {
	cfg    connConfig
	broker producerBroker
	buffer []*kgo.Record
}

// Produce buffers a message for later delivery, mirroring
// producer.produce(value, topic:, ...). It performs no I/O.
func (p *Producer) Produce(value []byte, o ProduceOptions) error {
	if o.Topic == "" {
		return &wrappedError{ErrDeliveryFailed, errors.New("empty topic")}
	}
	rec := &kgo.Record{Topic: o.Topic, Value: value, Key: o.Key, Partition: -1}
	if o.Partition != nil {
		rec.Partition = *o.Partition
	}
	for k, v := range o.Headers {
		rec.Headers = append(rec.Headers, kgo.RecordHeader{Key: k, Value: v})
	}
	p.buffer = append(p.buffer, rec)
	return nil
}

// ensure opens the underlying broker on first use.
func (p *Producer) ensure() error {
	if p.broker != nil {
		return nil
	}
	b, err := newProducerBroker(p.cfg)
	if err != nil {
		return mapError(err, ErrConnection)
	}
	p.broker = b
	return nil
}

// DeliverMessages flushes the buffered messages to Kafka, mirroring
// producer.deliver_messages. An empty buffer is a no-op. On success the buffer
// is cleared; on failure it is retained for a later retry.
func (p *Producer) DeliverMessages() error {
	if len(p.buffer) == 0 {
		return nil
	}
	if err := p.ensure(); err != nil {
		return err
	}
	if err := p.broker.Produce(context.Background(), p.buffer); err != nil {
		return mapError(err, ErrDeliveryFailed)
	}
	p.buffer = nil
	return nil
}

// Shutdown closes the producer's connection, mirroring producer.shutdown.
func (p *Producer) Shutdown() {
	if p.broker != nil {
		p.broker.Close()
		p.broker = nil
	}
}
