package kafka

import (
	"context"
	"errors"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
)

// The kfake suite drives the real franz-go brokers against an in-process fake
// Kafka cluster, verifying end-to-end round-trips (produce then consume,
// consumer-group resume, topic admin, error mapping). It runs on the native
// lanes; it is skipped when GO_RUBY_KAFKA_KFAKE=0, which the qemu-emulated
// cross-arch CI lanes set because a full protocol broker + client is too heavy
// under qemu-user. Those lanes still run the deterministic mock suite, and the
// coverage gate runs on a native lane where this suite executes.
func requireKfake(t *testing.T) {
	t.Helper()
	if os.Getenv("GO_RUBY_KAFKA_KFAKE") == "0" {
		t.Skip("kfake suite disabled (GO_RUBY_KAFKA_KFAKE=0)")
	}
}

// newCluster starts a fresh in-process cluster and returns a client for it.
func newCluster(t *testing.T, opts ...kfake.Opt) (*Kafka, *kfake.Cluster) {
	t.Helper()
	c, err := kfake.NewCluster(append([]kfake.Opt{kfake.NumBrokers(1)}, opts...)...)
	if err != nil {
		t.Fatalf("kfake.NewCluster: %v", err)
	}
	t.Cleanup(c.Close)
	k := New(Options{SeedBrokers: c.ListenAddrs(), ClientID: "go-ruby-kafka-test"})
	t.Cleanup(k.Close)
	return k, c
}

// collect runs the poll loop until n messages arrive (or a watchdog fires),
// committing after each and returning the messages.
func collect(t *testing.T, c *Consumer, n int) []*Message {
	t.Helper()
	var got []*Message
	watchdog := time.AfterFunc(20*time.Second, c.Stop)
	defer watchdog.Stop()
	err := c.EachMessage(func(m *Message) error {
		got = append(got, m)
		if err := c.CommitOffsets(); err != nil {
			return err
		}
		if len(got) >= n {
			c.Stop()
		}
		return nil
	})
	if err != nil {
		t.Fatalf("EachMessage: %v", err)
	}
	return got
}

func TestKfakeRoundTrip(t *testing.T) {
	requireKfake(t)
	k, _ := newCluster(t)
	if err := k.CreateTopic("orders", 2, 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	p := k.Producer()
	defer p.Shutdown()
	part := int32(1)
	if err := p.Produce([]byte("v-key"), ProduceOptions{
		Topic:   "orders",
		Key:     []byte("k1"),
		Headers: map[string][]byte{"trace": []byte("abc")},
	}); err != nil {
		t.Fatal(err)
	}
	if err := p.Produce([]byte("v-part"), ProduceOptions{
		Topic:     "orders",
		Partition: &part,
	}); err != nil {
		t.Fatal(err)
	}
	if err := p.DeliverMessages(); err != nil {
		t.Fatalf("DeliverMessages: %v", err)
	}

	c := k.Consumer("round-trip")
	c.Subscribe("orders", true)
	msgs := collect(t, c, 2)

	byVal := map[string]*Message{}
	for _, m := range msgs {
		byVal[string(m.Value)] = m
	}
	keyed, ok := byVal["v-key"]
	if !ok {
		t.Fatalf("missing keyed message: %v", byVal)
	}
	if string(keyed.Key) != "k1" || string(keyed.Headers["trace"]) != "abc" {
		t.Fatalf("keyed message fields wrong: %+v", keyed)
	}
	if keyed.Topic != "orders" || keyed.CreateTime.IsZero() {
		t.Fatalf("keyed message metadata wrong: %+v", keyed)
	}
	parted, ok := byVal["v-part"]
	if !ok || parted.Partition != 1 {
		t.Fatalf("explicit-partition message not on partition 1: %+v", parted)
	}
	if parted.Offset < 0 {
		t.Fatalf("message should have a non-negative offset, got %d", parted.Offset)
	}
}

func TestKfakeDeliverMessageSync(t *testing.T) {
	requireKfake(t)
	k, _ := newCluster(t)
	if err := k.CreateTopic("sync", 1, 1); err != nil {
		t.Fatal(err)
	}
	if err := k.DeliverMessage([]byte("one-shot"), "sync"); err != nil {
		t.Fatalf("DeliverMessage: %v", err)
	}
	c := k.Consumer("sync-group")
	c.Subscribe("sync", true)
	msgs := collect(t, c, 1)
	if len(msgs) != 1 || string(msgs[0].Value) != "one-shot" {
		t.Fatalf("round-trip failed: %+v", msgs)
	}
}

func TestKfakeConsumerGroupResume(t *testing.T) {
	requireKfake(t)
	k, _ := newCluster(t)
	if err := k.CreateTopic("stream", 1, 1); err != nil {
		t.Fatal(err)
	}
	p := k.Producer()
	for _, v := range []string{"m0", "m1", "m2"} {
		if err := p.Produce([]byte(v), ProduceOptions{Topic: "stream"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.DeliverMessages(); err != nil {
		t.Fatal(err)
	}
	p.Shutdown()

	// First consumer reads and commits the first two messages.
	c1 := k.Consumer("resumers")
	c1.Subscribe("stream", true)
	first := collect(t, c1, 2)
	if len(first) != 2 || string(first[0].Value) != "m0" || string(first[1].Value) != "m1" {
		t.Fatalf("first pass: %+v", first)
	}

	// A new member of the same group resumes after the committed offset.
	c2 := k.Consumer("resumers")
	c2.Subscribe("stream", true)
	second := collect(t, c2, 1)
	if len(second) != 1 || string(second[0].Value) != "m2" {
		t.Fatalf("resume pass: %+v", second)
	}
	if second[0].Offset != 2 {
		t.Fatalf("expected to resume at offset 2, got %d", second[0].Offset)
	}
}

func TestKfakeEachBatch(t *testing.T) {
	requireKfake(t)
	k, _ := newCluster(t)
	if err := k.CreateTopic("batched", 1, 1); err != nil {
		t.Fatal(err)
	}
	p := k.Producer()
	for _, v := range []string{"b0", "b1"} {
		_ = p.Produce([]byte(v), ProduceOptions{Topic: "batched"})
	}
	if err := p.DeliverMessages(); err != nil {
		t.Fatal(err)
	}
	p.Shutdown()

	c := k.Consumer("batchers")
	c.Subscribe("batched", true)
	var total int
	watchdog := time.AfterFunc(20*time.Second, c.Stop)
	defer watchdog.Stop()
	err := c.EachBatch(func(b *Batch) error {
		total += len(b.Messages)
		if err := c.CommitOffsets(); err != nil {
			return err
		}
		if total >= 2 {
			c.Stop()
		}
		return nil
	})
	if err != nil {
		t.Fatalf("EachBatch: %v", err)
	}
	if total != 2 {
		t.Fatalf("expected 2 messages across batches, got %d", total)
	}
}

func TestKfakeAdmin(t *testing.T) {
	requireKfake(t)
	k, _ := newCluster(t)
	if err := k.CreateTopic("admin-topic", 3, 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	// Creating the same topic again surfaces the broker's per-topic error.
	if err := k.CreateTopic("admin-topic", 3, 1); err == nil {
		t.Fatal("duplicate CreateTopic should fail")
	}
	topics, err := k.Topics()
	if err != nil {
		t.Fatalf("Topics: %v", err)
	}
	if !slices.Contains(topics, "admin-topic") {
		t.Fatalf("Topics missing admin-topic: %v", topics)
	}
	parts, err := k.PartitionsFor("admin-topic")
	if err != nil {
		t.Fatalf("PartitionsFor: %v", err)
	}
	if len(parts) != 3 {
		t.Fatalf("expected 3 partitions, got %v", parts)
	}
	if err := k.DeleteTopic("admin-topic"); err != nil {
		t.Fatalf("DeleteTopic: %v", err)
	}
}

func TestKfakeErrorMapping(t *testing.T) {
	requireKfake(t)
	k, _ := newCluster(t)

	// Produce to a nonexistent topic (auto-creation off) -> unknown topic.
	p := k.Producer()
	defer p.Shutdown()
	_ = p.Produce([]byte("v"), ProduceOptions{Topic: "ghost"})
	if err := p.DeliverMessages(); !errors.Is(err, ErrUnknownTopicOrPartition) {
		t.Fatalf("produce to ghost topic = %v", err)
	}
	if _, err := k.PartitionsFor("ghost"); !errors.Is(err, ErrUnknownTopicOrPartition) {
		t.Fatalf("PartitionsFor ghost = %v", err)
	}
	if err := k.DeleteTopic("ghost"); !errors.Is(err, ErrUnknownTopicOrPartition) {
		t.Fatalf("DeleteTopic ghost = %v", err)
	}
}

// TestKfakeRealBrokerErrors covers the real brokers' transport-error and
// construction-error branches deterministically.
func TestKfakeRealBrokerErrors(t *testing.T) {
	requireKfake(t)
	_, c := newCluster(t)
	cfg := connConfig{seedBrokers: c.ListenAddrs()}
	ctx := context.Background()

	// A closed group consumer reports a poll error.
	cb, err := newFranzConsumer(cfg, "g", []string{"t"}, true)
	if err != nil {
		t.Fatal(err)
	}
	cb.Close()
	if _, err := cb.Poll(ctx, 10); err == nil {
		t.Fatal("poll on closed consumer should error")
	}

	// A closed admin reports transport errors on every request.
	ab, err := newFranzAdmin(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ab.Close()
	if err := ab.CreateTopic(ctx, "x", 1, 1); err == nil {
		t.Fatal("CreateTopic on closed admin should error")
	}
	if _, err := ab.ListTopics(ctx); err == nil {
		t.Fatal("ListTopics on closed admin should error")
	}
	if _, err := ab.PartitionsFor(ctx, "x"); err == nil {
		t.Fatal("PartitionsFor on closed admin should error")
	}
	if err := ab.DeleteTopic(ctx, "x"); err == nil {
		t.Fatal("DeleteTopic on closed admin should error")
	}

	// Construction with no seed brokers fails for every role.
	empty := connConfig{}
	if _, err := newFranzProducer(empty); err == nil {
		t.Fatal("producer with no seeds should fail")
	}
	if _, err := newFranzConsumer(empty, "g", []string{"t"}, false); err == nil {
		t.Fatal("consumer with no seeds should fail")
	}
	if _, err := newFranzAdmin(empty); err == nil {
		t.Fatal("admin with no seeds should fail")
	}
}
