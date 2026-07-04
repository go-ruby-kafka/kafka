package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// --- deterministic mock brokers, injected via the package seams ---

type mockProducer struct {
	produced [][]*kgo.Record
	err      error
	closed   bool
}

func (m *mockProducer) Produce(_ context.Context, recs []*kgo.Record) error {
	m.produced = append(m.produced, recs)
	return m.err
}
func (m *mockProducer) Close() { m.closed = true }

type pollStep struct {
	recs []*kgo.Record
	err  error
	hook func() // runs when this step is polled (e.g. to Stop the consumer)
}

type mockConsumer struct {
	steps     []pollStep
	i         int
	commitErr error
	committed [][]*kgo.Record
	closed    bool
}

func (m *mockConsumer) Poll(ctx context.Context, _ int) ([]*kgo.Record, error) {
	if m.i >= len(m.steps) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	s := m.steps[m.i]
	m.i++
	if s.hook != nil {
		s.hook()
	}
	return s.recs, s.err
}
func (m *mockConsumer) Commit(_ context.Context, recs []*kgo.Record) error {
	m.committed = append(m.committed, recs)
	return m.commitErr
}
func (m *mockConsumer) Close() { m.closed = true }

type mockAdmin struct {
	createErr error
	created   []string
	topics    []string
	listErr   error
	parts     []int32
	partsErr  error
	deleteErr error
	deleted   []string
	closed    bool
}

func (m *mockAdmin) CreateTopic(_ context.Context, name string, _ int32, _ int16) error {
	m.created = append(m.created, name)
	return m.createErr
}
func (m *mockAdmin) ListTopics(context.Context) ([]string, error) { return m.topics, m.listErr }
func (m *mockAdmin) PartitionsFor(context.Context, string) ([]int32, error) {
	return m.parts, m.partsErr
}
func (m *mockAdmin) DeleteTopic(_ context.Context, name string) error {
	m.deleted = append(m.deleted, name)
	return m.deleteErr
}
func (m *mockAdmin) Close() { m.closed = true }

// saveSeams snapshots the injection seams and restores them after the test.
func saveSeams(t *testing.T) {
	t.Helper()
	p, c, a := newProducerBroker, newConsumerBroker, newAdminBroker
	t.Cleanup(func() { newProducerBroker, newConsumerBroker, newAdminBroker = p, c, a })
}

func useAdmin(a adminBroker)          { newAdminBroker = func(connConfig) (adminBroker, error) { return a, nil } }
func useProducer(p producerBroker)    { newProducerBroker = func(connConfig) (producerBroker, error) { return p, nil } }
func useConsumer(c consumerBroker)    { newConsumerBroker = func(connConfig, string, []string, bool) (consumerBroker, error) { return c, nil } }
func failAdmin(err error)             { newAdminBroker = func(connConfig) (adminBroker, error) { return nil, err } }
func failProducer(err error)          { newProducerBroker = func(connConfig) (producerBroker, error) { return nil, err } }
func failConsumer(err error)          { newConsumerBroker = func(connConfig, string, []string, bool) (consumerBroker, error) { return nil, err } }

// --- errors.go ---

func TestMapError(t *testing.T) {
	if got := mapError(nil, ErrDeliveryFailed); got != nil {
		t.Fatalf("nil error should map to nil, got %v", got)
	}
	cases := []struct {
		in   error
		dflt error
		want error
	}{
		{kerr.UnknownTopicOrPartition, ErrDeliveryFailed, ErrUnknownTopicOrPartition},
		{kerr.OffsetOutOfRange, ErrConnection, ErrOffsetOutOfRange},
		{errors.New("boom"), ErrDeliveryFailed, ErrDeliveryFailed},
	}
	for _, c := range cases {
		got := mapError(c.in, c.dflt)
		if !errors.Is(got, c.want) {
			t.Fatalf("mapError(%v) = %v, want Is %v", c.in, got, c.want)
		}
		if !errors.Is(got, c.in) {
			t.Fatalf("mapError should wrap the underlying error %v", c.in)
		}
		if got.Error() == "" {
			t.Fatal("wrapped error should render a message")
		}
	}
}

// --- kafka.go admin surface via mock ---

func TestAdminSurface(t *testing.T) {
	saveSeams(t)
	adm := &mockAdmin{topics: []string{"a", "b"}, parts: []int32{0, 1, 2}}
	useAdmin(adm)
	k := New(Options{SeedBrokers: []string{"x:9092"}, ClientID: "cid"})

	if err := k.CreateTopic("t", 3, 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	topics, err := k.Topics()
	if err != nil || len(topics) != 2 {
		t.Fatalf("Topics: %v %v", topics, err)
	}
	parts, err := k.PartitionsFor("t")
	if err != nil || len(parts) != 3 {
		t.Fatalf("PartitionsFor: %v %v", parts, err)
	}
	if err := k.DeleteTopic("t"); err != nil {
		t.Fatalf("DeleteTopic: %v", err)
	}
	// Second admin call reuses the cached connection.
	if _, err := k.Topics(); err != nil {
		t.Fatalf("Topics cached: %v", err)
	}
	k.Close()
	if !adm.closed {
		t.Fatal("Close should close the admin broker")
	}
	k.Close() // idempotent: admin already nil
}

func TestAdminErrors(t *testing.T) {
	saveSeams(t)
	adm := &mockAdmin{
		createErr: kerr.UnknownTopicOrPartition,
		listErr:   errors.New("net"),
		partsErr:  kerr.UnknownTopicOrPartition,
		deleteErr: errors.New("net"),
	}
	useAdmin(adm)
	k := New(Options{SeedBrokers: []string{"x:9092"}})
	if err := k.CreateTopic("t", 1, 1); !errors.Is(err, ErrUnknownTopicOrPartition) {
		t.Fatalf("CreateTopic err = %v", err)
	}
	if _, err := k.Topics(); !errors.Is(err, ErrConnection) {
		t.Fatalf("Topics err = %v", err)
	}
	if _, err := k.PartitionsFor("t"); !errors.Is(err, ErrUnknownTopicOrPartition) {
		t.Fatalf("PartitionsFor err = %v", err)
	}
	if err := k.DeleteTopic("t"); !errors.Is(err, ErrConnection) {
		t.Fatalf("DeleteTopic err = %v", err)
	}
}

func TestAdminConnFailure(t *testing.T) {
	saveSeams(t)
	failAdmin(errors.New("no seeds"))
	k := New(Options{})
	if err := k.CreateTopic("t", 1, 1); !errors.Is(err, ErrConnection) {
		t.Fatalf("CreateTopic conn err = %v", err)
	}
	if _, err := k.Topics(); !errors.Is(err, ErrConnection) {
		t.Fatalf("Topics conn err = %v", err)
	}
	if _, err := k.PartitionsFor("t"); !errors.Is(err, ErrConnection) {
		t.Fatalf("PartitionsFor conn err = %v", err)
	}
	if err := k.DeleteTopic("t"); !errors.Is(err, ErrConnection) {
		t.Fatalf("DeleteTopic conn err = %v", err)
	}
	k.Close() // admin never opened
}

// --- producer.go via mock ---

func TestProducer(t *testing.T) {
	saveSeams(t)
	mp := &mockProducer{}
	useProducer(mp)
	k := New(Options{SeedBrokers: []string{"x:9092"}})
	p := k.Producer()

	// Empty buffer: deliver is a no-op and opens nothing.
	if err := p.DeliverMessages(); err != nil {
		t.Fatalf("empty deliver: %v", err)
	}

	if err := p.Produce([]byte("v"), ProduceOptions{}); !errors.Is(err, ErrDeliveryFailed) {
		t.Fatalf("empty topic should fail: %v", err)
	}
	part := int32(2)
	if err := p.Produce([]byte("hello"), ProduceOptions{
		Topic:     "t",
		Key:       []byte("k"),
		Partition: &part,
		Headers:   map[string][]byte{"h": []byte("1")},
	}); err != nil {
		t.Fatalf("produce: %v", err)
	}
	if err := p.Produce([]byte("world"), ProduceOptions{Topic: "t"}); err != nil {
		t.Fatalf("produce2: %v", err)
	}
	if err := p.DeliverMessages(); err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if len(mp.produced) != 1 || len(mp.produced[0]) != 2 {
		t.Fatalf("expected one flush of two records, got %v", mp.produced)
	}
	if mp.produced[0][0].Partition != 2 {
		t.Fatalf("explicit partition lost: %d", mp.produced[0][0].Partition)
	}
	if mp.produced[0][1].Partition != -1 {
		t.Fatalf("auto partition should be -1, got %d", mp.produced[0][1].Partition)
	}
	// Deliver again with a fresh message: reuses the cached broker.
	if err := p.Produce([]byte("again"), ProduceOptions{Topic: "t"}); err != nil {
		t.Fatal(err)
	}
	if err := p.DeliverMessages(); err != nil {
		t.Fatalf("deliver reuse: %v", err)
	}
	p.Shutdown()
	if !mp.closed {
		t.Fatal("Shutdown should close broker")
	}
	p.Shutdown() // idempotent
}

func TestProducerDeliverError(t *testing.T) {
	saveSeams(t)
	useProducer(&mockProducer{err: kerr.UnknownTopicOrPartition})
	k := New(Options{SeedBrokers: []string{"x:9092"}})
	p := k.Producer()
	_ = p.Produce([]byte("v"), ProduceOptions{Topic: "t"})
	if err := p.DeliverMessages(); !errors.Is(err, ErrUnknownTopicOrPartition) {
		t.Fatalf("deliver error = %v", err)
	}
}

func TestProducerConnError(t *testing.T) {
	saveSeams(t)
	failProducer(errors.New("no seeds"))
	k := New(Options{})
	p := k.Producer()
	_ = p.Produce([]byte("v"), ProduceOptions{Topic: "t"})
	if err := p.DeliverMessages(); !errors.Is(err, ErrConnection) {
		t.Fatalf("deliver conn error = %v", err)
	}
	p.Shutdown() // never opened
}

func TestDeliverMessageSync(t *testing.T) {
	saveSeams(t)
	mp := &mockProducer{}
	useProducer(mp)
	k := New(Options{SeedBrokers: []string{"x:9092"}})
	if err := k.DeliverMessage([]byte("v"), "t"); err != nil {
		t.Fatalf("DeliverMessage: %v", err)
	}
	if !mp.closed {
		t.Fatal("DeliverMessage should shut down its producer")
	}
	// Empty topic bubbles the produce validation error out.
	if err := k.DeliverMessage([]byte("v"), ""); !errors.Is(err, ErrDeliveryFailed) {
		t.Fatalf("DeliverMessage empty topic = %v", err)
	}
}

// --- consumer.go via mock ---

func rec(topic string, part int32, off int64, val string, headers map[string][]byte) *kgo.Record {
	r := &kgo.Record{Topic: topic, Partition: part, Offset: off, Value: []byte(val)}
	for k, v := range headers {
		r.Headers = append(r.Headers, kgo.RecordHeader{Key: k, Value: v})
	}
	return r
}

func TestConsumerEachMessage(t *testing.T) {
	saveSeams(t)
	mc := &mockConsumer{}
	k := New(Options{SeedBrokers: []string{"x:9092"}})
	c := k.Consumer("g")
	c.Subscribe("t", true)
	// First poll returns two records; the second record's processing stops the
	// consumer, after which the loop's top-of-iteration check returns nil.
	mc.steps = []pollStep{{recs: []*kgo.Record{
		rec("t", 0, 0, "a", map[string][]byte{"h": []byte("x")}),
		rec("t", 0, 1, "b", nil),
	}}}
	useConsumer(mc)

	var got []*Message
	err := c.EachMessage(func(m *Message) error {
		got = append(got, m)
		if err := c.CommitOffsets(); err != nil {
			return err
		}
		if m.Offset == 1 {
			c.Stop()
		}
		return nil
	})
	if err != nil {
		t.Fatalf("EachMessage: %v", err)
	}
	if len(got) != 2 || got[0].Value == nil || got[0].Headers["h"] == nil {
		t.Fatalf("messages: %+v", got)
	}
	if got[1].Headers != nil {
		t.Fatal("record without headers should have nil Headers")
	}
	if len(mc.committed) == 0 {
		t.Fatal("expected commits")
	}
	if !mc.closed {
		t.Fatal("loop should close the broker on return")
	}
	// CommitOffsets after the loop (broker released) is a no-op.
	if err := c.CommitOffsets(); err != nil {
		t.Fatalf("post-loop commit: %v", err)
	}
}

func TestConsumerStopMidBatch(t *testing.T) {
	saveSeams(t)
	useConsumer(&mockConsumer{steps: []pollStep{{recs: []*kgo.Record{
		rec("t", 0, 0, "a", nil),
		rec("t", 0, 1, "b", nil),
	}}}})
	k := New(Options{SeedBrokers: []string{"x:9092"}})
	c := k.Consumer("g")
	c.Subscribe("t", true)
	var seen int
	// Stop on the first record: the second must not be delivered.
	err := c.EachMessage(func(*Message) error {
		seen++
		c.Stop()
		return nil
	})
	if err != nil {
		t.Fatalf("EachMessage: %v", err)
	}
	if seen != 1 {
		t.Fatalf("mid-batch stop should deliver only one message, got %d", seen)
	}
}

func TestConsumerEachBatchStopMid(t *testing.T) {
	saveSeams(t)
	useConsumer(&mockConsumer{steps: []pollStep{{recs: []*kgo.Record{
		rec("t", 0, 0, "a", nil),
		rec("t", 1, 0, "b", nil),
	}}}})
	k := New(Options{SeedBrokers: []string{"x:9092"}})
	c := k.Consumer("g")
	c.Subscribe("t", true)
	var seen int
	err := c.EachBatch(func(*Batch) error {
		seen++
		c.Stop()
		return nil
	})
	if err != nil {
		t.Fatalf("EachBatch: %v", err)
	}
	if seen != 1 {
		t.Fatalf("mid-batch stop should deliver only one batch, got %d", seen)
	}
}

func TestConsumerStopBeforeLoop(t *testing.T) {
	saveSeams(t)
	useConsumer(&mockConsumer{})
	k := New(Options{SeedBrokers: []string{"x:9092"}})
	c := k.Consumer("g")
	c.Subscribe("t", false)
	c.Stop()
	if err := c.EachMessage(func(*Message) error { return errors.New("should not run") }); err != nil {
		t.Fatalf("stopped EachMessage = %v", err)
	}
}

func TestConsumerPollErrorStopped(t *testing.T) {
	saveSeams(t)
	k := New(Options{SeedBrokers: []string{"x:9092"}})
	c := k.Consumer("g")
	c.Subscribe("t", true)
	mc := &mockConsumer{steps: []pollStep{{err: errors.New("boom"), hook: c.Stop}}}
	useConsumer(mc)
	if err := c.EachMessage(func(*Message) error { return nil }); err != nil {
		t.Fatalf("poll error while stopped should return nil, got %v", err)
	}
}

func TestConsumerPollErrorLive(t *testing.T) {
	saveSeams(t)
	useConsumer(&mockConsumer{steps: []pollStep{{err: kerr.OffsetOutOfRange}}})
	k := New(Options{SeedBrokers: []string{"x:9092"}})
	c := k.Consumer("g")
	c.Subscribe("t", true)
	if err := c.EachMessage(func(*Message) error { return nil }); !errors.Is(err, ErrOffsetOutOfRange) {
		t.Fatalf("live poll error = %v", err)
	}
}

func TestConsumerBlockCallbackError(t *testing.T) {
	saveSeams(t)
	useConsumer(&mockConsumer{steps: []pollStep{{recs: []*kgo.Record{rec("t", 0, 0, "a", nil)}}}})
	k := New(Options{SeedBrokers: []string{"x:9092"}})
	c := k.Consumer("g")
	c.Subscribe("t", true)
	want := errors.New("callback failed")
	if err := c.EachMessage(func(*Message) error { return want }); !errors.Is(err, want) {
		t.Fatalf("callback error = %v", err)
	}
}

func TestConsumerEnsureError(t *testing.T) {
	saveSeams(t)
	failConsumer(errors.New("no seeds"))
	k := New(Options{})
	c := k.Consumer("g")
	if err := c.EachMessage(func(*Message) error { return nil }); !errors.Is(err, ErrConnection) {
		t.Fatalf("EachMessage ensure err = %v", err)
	}
	if err := c.EachBatch(func(*Batch) error { return nil }); !errors.Is(err, ErrConnection) {
		t.Fatalf("EachBatch ensure err = %v", err)
	}
}

func TestConsumerCommitError(t *testing.T) {
	saveSeams(t)
	useConsumer(&mockConsumer{
		steps:     []pollStep{{recs: []*kgo.Record{rec("t", 0, 0, "a", nil)}}},
		commitErr: errors.New("commit boom"),
	})
	k := New(Options{SeedBrokers: []string{"x:9092"}})
	c := k.Consumer("g")
	c.Subscribe("t", true)
	err := c.EachMessage(func(*Message) error { return c.CommitOffsets() })
	if !errors.Is(err, ErrOffsetCommit) {
		t.Fatalf("commit error = %v", err)
	}
}

func TestConsumerEachBatch(t *testing.T) {
	saveSeams(t)
	mc := &mockConsumer{steps: []pollStep{{recs: []*kgo.Record{
		rec("t", 0, 0, "a", nil),
		rec("t", 0, 1, "b", nil), // same (topic,partition): existing key
		rec("t", 1, 0, "c", nil), // different partition: new key
	}}}}
	useConsumer(mc)
	k := New(Options{SeedBrokers: []string{"x:9092"}})
	c := k.Consumer("g")
	c.Subscribe("t", true)
	var batches []*Batch
	err := c.EachBatch(func(b *Batch) error {
		batches = append(batches, b)
		if b.Partition == 1 {
			c.Stop()
		}
		return nil
	})
	if err != nil {
		t.Fatalf("EachBatch: %v", err)
	}
	if len(batches) != 2 || len(batches[0].Messages) != 2 || len(batches[1].Messages) != 1 {
		t.Fatalf("batches: %+v", batches)
	}
}

func TestConsumerEachBatchCallbackError(t *testing.T) {
	saveSeams(t)
	useConsumer(&mockConsumer{steps: []pollStep{{recs: []*kgo.Record{rec("t", 0, 0, "a", nil)}}}})
	k := New(Options{SeedBrokers: []string{"x:9092"}})
	c := k.Consumer("g")
	c.Subscribe("t", true)
	want := errors.New("batch failed")
	if err := c.EachBatch(func(*Batch) error { return want }); !errors.Is(err, want) {
		t.Fatalf("batch callback error = %v", err)
	}
}

func TestConsumerEachBatchPollError(t *testing.T) {
	saveSeams(t)
	useConsumer(&mockConsumer{steps: []pollStep{{err: errors.New("boom")}}})
	k := New(Options{SeedBrokers: []string{"x:9092"}})
	c := k.Consumer("g")
	c.Subscribe("t", true)
	if err := c.EachBatch(func(*Batch) error { return nil }); !errors.Is(err, ErrConnection) {
		t.Fatalf("batch poll error = %v", err)
	}
}

func TestConsumerEachBatchStopBefore(t *testing.T) {
	saveSeams(t)
	useConsumer(&mockConsumer{})
	k := New(Options{SeedBrokers: []string{"x:9092"}})
	c := k.Consumer("g")
	c.Subscribe("t", true)
	c.Stop()
	if err := c.EachBatch(func(*Batch) error { return errors.New("nope") }); err != nil {
		t.Fatalf("stopped EachBatch = %v", err)
	}
}

func TestConsumerEachBatchPollErrorStopped(t *testing.T) {
	saveSeams(t)
	k := New(Options{SeedBrokers: []string{"x:9092"}})
	c := k.Consumer("g")
	c.Subscribe("t", true)
	useConsumer(&mockConsumer{steps: []pollStep{{err: errors.New("boom"), hook: c.Stop}}})
	if err := c.EachBatch(func(*Batch) error { return nil }); err != nil {
		t.Fatalf("batch poll error while stopped = %v", err)
	}
}
