package kafka

import (
	"context"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// connConfig is the immutable broker-connection description a Kafka client
// carries: where to dial and how to identify itself.
type connConfig struct {
	seedBrokers []string
	clientID    string
}

// The broker seam. Producing, consuming and administering are each abstracted
// behind a small interface whose real implementation wraps franz-go. The
// constructors below are package variables so tests can inject a deterministic
// mock in place of a live (or kfake) broker.
type (
	producerBroker interface {
		Produce(ctx context.Context, recs []*kgo.Record) error
		Close()
	}
	consumerBroker interface {
		Poll(ctx context.Context, max int) ([]*kgo.Record, error)
		Commit(ctx context.Context, recs []*kgo.Record) error
		Close()
	}
	adminBroker interface {
		CreateTopic(ctx context.Context, name string, partitions int32, replication int16) error
		ListTopics(ctx context.Context) ([]string, error)
		PartitionsFor(ctx context.Context, name string) ([]int32, error)
		DeleteTopic(ctx context.Context, name string) error
		Close()
	}
)

// Injection seams. Production code calls through these; tests swap them.
var (
	newProducerBroker = newFranzProducer
	newConsumerBroker = newFranzConsumer
	newAdminBroker    = newFranzAdmin
)

// dialTimeout and retryTimeout bound how long a connection attempt and a
// retried request may take, so an unreachable broker fails promptly rather than
// blocking forever. They mirror ruby-kafka's connect/socket timeout defaults.
var (
	dialTimeout  = 5 * time.Second
	retryTimeout = 3 * time.Second
)

// baseOpts builds the franz-go options shared by every role.
func baseOpts(cfg connConfig) []kgo.Opt {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.seedBrokers...),
		kgo.DialTimeout(dialTimeout),
		kgo.RetryTimeout(retryTimeout),
	}
	if cfg.clientID != "" {
		opts = append(opts, kgo.ClientID(cfg.clientID))
	}
	return opts
}

// rubyPartitioner reproduces ruby-kafka's partition selection: an explicit
// partition (Record.Partition >= 0) is honoured, otherwise the record is
// balanced by the standard sticky-key partitioner.
type rubyPartitioner struct{ fallback kgo.Partitioner }

func (p rubyPartitioner) ForTopic(t string) kgo.TopicPartitioner {
	return rubyTopicPartitioner{p.fallback.ForTopic(t)}
}

type rubyTopicPartitioner struct{ fallback kgo.TopicPartitioner }

func (p rubyTopicPartitioner) RequiresConsistency(*kgo.Record) bool { return true }

func (p rubyTopicPartitioner) Partition(r *kgo.Record, n int) int {
	if r.Partition >= 0 {
		return int(r.Partition) % n
	}
	return p.fallback.Partition(r, n)
}

// --- real franz-go producer ---

type franzProducer struct{ cl *kgo.Client }

func newFranzProducer(cfg connConfig) (producerBroker, error) {
	opts := append(baseOpts(cfg),
		kgo.RecordPartitioner(rubyPartitioner{kgo.StickyKeyPartitioner(nil)}),
	)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	return &franzProducer{cl}, nil
}

func (p *franzProducer) Produce(ctx context.Context, recs []*kgo.Record) error {
	return p.cl.ProduceSync(ctx, recs...).FirstErr()
}

func (p *franzProducer) Close() { p.cl.Close() }

// --- real franz-go consumer (a group member) ---

type franzConsumer struct{ cl *kgo.Client }

func newFranzConsumer(cfg connConfig, group string, topics []string, fromBeginning bool) (consumerBroker, error) {
	reset := kgo.NewOffset().AtEnd()
	if fromBeginning {
		reset = kgo.NewOffset().AtStart()
	}
	opts := append(baseOpts(cfg),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),
		kgo.ConsumeResetOffset(reset),
		kgo.DisableAutoCommit(),
	)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	return &franzConsumer{cl}, nil
}

func (c *franzConsumer) Poll(ctx context.Context, max int) ([]*kgo.Record, error) {
	fs := c.cl.PollRecords(ctx, max)
	if err := fs.Err0(); err != nil {
		return nil, err
	}
	return fs.Records(), nil
}

func (c *franzConsumer) Commit(ctx context.Context, recs []*kgo.Record) error {
	return c.cl.CommitRecords(ctx, recs...)
}

func (c *franzConsumer) Close() { c.cl.Close() }

// --- real franz-go admin (kadm over a kgo client) ---

type franzAdmin struct {
	cl  *kgo.Client
	adm *kadm.Client
}

func newFranzAdmin(cfg connConfig) (adminBroker, error) {
	cl, err := kgo.NewClient(baseOpts(cfg)...)
	if err != nil {
		return nil, err
	}
	return &franzAdmin{cl, kadm.NewClient(cl)}, nil
}

func (a *franzAdmin) CreateTopic(ctx context.Context, name string, partitions int32, replication int16) error {
	resp, err := a.adm.CreateTopic(ctx, partitions, replication, nil, name)
	if err != nil {
		return err
	}
	return resp.Err
}

func (a *franzAdmin) ListTopics(ctx context.Context) ([]string, error) {
	td, err := a.adm.ListTopics(ctx)
	if err != nil {
		return nil, err
	}
	return td.Names(), nil
}

func (a *franzAdmin) PartitionsFor(ctx context.Context, name string) ([]int32, error) {
	td, err := a.adm.ListTopics(ctx, name)
	if err != nil {
		return nil, err
	}
	// A requested-but-missing topic is returned with an UNKNOWN_TOPIC_OR_PARTITION
	// error on its detail; the zero value's nil Err only occurs for a broker that
	// omits the topic, which real brokers and kfake never do for a named request.
	d := td[name]
	if d.Err != nil {
		return nil, d.Err
	}
	parts := make([]int32, 0, len(d.Partitions))
	for p := range d.Partitions {
		parts = append(parts, p)
	}
	return parts, nil
}

func (a *franzAdmin) DeleteTopic(ctx context.Context, name string) error {
	resp, err := a.adm.DeleteTopic(ctx, name)
	if err != nil {
		return err
	}
	return resp.Err
}

func (a *franzAdmin) Close() { a.cl.Close() }
