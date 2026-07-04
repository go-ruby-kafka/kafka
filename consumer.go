package kafka

import (
	"context"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// maxPollRecords bounds how many records a single poll returns.
const maxPollRecords = 100

// Message is a consumed record, the analogue of ruby-kafka's message object.
// Its Topic field plays the "subject" role: it names where the message came
// from, alongside the partition/offset coordinates and the payload.
type Message struct {
	Topic      string
	Partition  int32
	Offset     int64
	Key        []byte
	Value      []byte
	Headers    map[string][]byte
	CreateTime time.Time
}

// Batch is a per-(topic, partition) run of messages yielded by
// [Consumer.EachBatch], mirroring ruby-kafka's batch object.
type Batch struct {
	Topic     string
	Partition int32
	Messages  []*Message
}

// Consumer is a consumer-group member, the analogue of the object returned by
// kafka.consumer(group_id:). Topics are registered with [Consumer.Subscribe];
// the poll loop is driven by [Consumer.EachMessage] or [Consumer.EachBatch];
// offsets are persisted with [Consumer.CommitOffsets]; and [Consumer.Stop] ends
// the loop.
type Consumer struct {
	cfg           connConfig
	groupID       string
	topics        []string
	fromBeginning bool
	broker        consumerBroker
	ctx           context.Context
	cancel        context.CancelFunc
	pending       []*kgo.Record
}

// Subscribe registers a topic for consumption, mirroring
// consumer.subscribe(topic, start_from_beginning:). If any subscription asks to
// start from the beginning, the group resets to the earliest offset when it has
// no committed position.
func (c *Consumer) Subscribe(topic string, startFromBeginning bool) {
	c.topics = append(c.topics, topic)
	if startFromBeginning {
		c.fromBeginning = true
	}
}

// ensure opens the group consumer for a poll loop. Each loop closes its broker
// on return (see closeBroker), so a fresh one is opened per EachMessage/EachBatch.
func (c *Consumer) ensure() error {
	b, err := newConsumerBroker(c.cfg, c.groupID, c.topics, c.fromBeginning)
	if err != nil {
		return mapError(err, ErrConnection)
	}
	c.broker = b
	return nil
}

// closeBroker releases the group consumer once the poll loop has finished.
func (c *Consumer) closeBroker() {
	if c.broker != nil {
		c.broker.Close()
		c.broker = nil
	}
}

// EachMessage runs the poll loop, yielding one message at a time, mirroring
// consumer.each_message { |message| ... }. It returns nil after [Consumer.Stop],
// or the error the block returns, or a connection/offset error.
func (c *Consumer) EachMessage(fn func(*Message) error) error {
	if err := c.ensure(); err != nil {
		return err
	}
	defer c.closeBroker()
	for {
		if c.ctx.Err() != nil {
			return nil
		}
		recs, err := c.broker.Poll(c.ctx, maxPollRecords)
		if err != nil {
			if c.ctx.Err() != nil {
				return nil
			}
			return mapError(err, ErrConnection)
		}
		for _, r := range recs {
			if c.ctx.Err() != nil {
				return nil
			}
			c.pending = append(c.pending, r)
			if err := fn(recordToMessage(r)); err != nil {
				return err
			}
		}
	}
}

// EachBatch runs the poll loop, yielding a per-partition batch at a time,
// mirroring consumer.each_batch { |batch| ... }.
func (c *Consumer) EachBatch(fn func(*Batch) error) error {
	if err := c.ensure(); err != nil {
		return err
	}
	defer c.closeBroker()
	for {
		if c.ctx.Err() != nil {
			return nil
		}
		recs, err := c.broker.Poll(c.ctx, maxPollRecords)
		if err != nil {
			if c.ctx.Err() != nil {
				return nil
			}
			return mapError(err, ErrConnection)
		}
		for _, b := range batchRecords(recs) {
			if c.ctx.Err() != nil {
				return nil
			}
			c.pending = append(c.pending, b.records...)
			if err := fn(b.batch); err != nil {
				return err
			}
		}
	}
}

// CommitOffsets persists the offsets of the messages yielded so far, mirroring
// consumer.commit_offsets. It is a no-op if nothing has been consumed.
func (c *Consumer) CommitOffsets() error {
	if c.broker == nil {
		return nil
	}
	if err := c.broker.Commit(c.ctx, c.pending); err != nil {
		return mapError(err, ErrOffsetCommit)
	}
	c.pending = nil
	return nil
}

// Stop ends the poll loop, mirroring consumer.stop. It is safe to call from any
// goroutine.
func (c *Consumer) Stop() {
	c.cancel()
}

// recordToMessage converts a franz-go record into a Message.
func recordToMessage(r *kgo.Record) *Message {
	m := &Message{
		Topic:      r.Topic,
		Partition:  r.Partition,
		Offset:     r.Offset,
		Key:        r.Key,
		Value:      r.Value,
		CreateTime: r.Timestamp,
	}
	if len(r.Headers) > 0 {
		m.Headers = make(map[string][]byte, len(r.Headers))
		for _, h := range r.Headers {
			m.Headers[h.Key] = h.Value
		}
	}
	return m
}

// tp keys a batch by its topic and partition.
type tp struct {
	topic     string
	partition int32
}

// batchWithRecords pairs a public Batch with the raw records it was built from,
// so the consumer can track them for offset commits.
type batchWithRecords struct {
	batch   *Batch
	records []*kgo.Record
}

// batchRecords groups records into per-(topic, partition) batches, preserving
// first-seen order.
func batchRecords(recs []*kgo.Record) []batchWithRecords {
	var order []tp
	byTP := map[tp][]*kgo.Record{}
	for _, r := range recs {
		k := tp{r.Topic, r.Partition}
		if _, ok := byTP[k]; !ok {
			order = append(order, k)
		}
		byTP[k] = append(byTP[k], r)
	}
	out := make([]batchWithRecords, 0, len(order))
	for _, k := range order {
		rs := byTP[k]
		msgs := make([]*Message, 0, len(rs))
		for _, r := range rs {
			msgs = append(msgs, recordToMessage(r))
		}
		out = append(out, batchWithRecords{
			batch:   &Batch{Topic: k.topic, Partition: k.partition, Messages: msgs},
			records: rs,
		})
	}
	return out
}
