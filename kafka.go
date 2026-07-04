package kafka

import "context"

// Options configures a [Kafka] client, mirroring the keyword arguments of the
// gem's Kafka.new(seed_brokers:, client_id:).
type Options struct {
	// SeedBrokers is the list of "host:port" bootstrap brokers.
	SeedBrokers []string
	// ClientID is the client identifier sent to the broker. Optional.
	ClientID string
}

// Kafka is the top-level client, the analogue of the object returned by
// Kafka.new. It is the factory for producers and consumers and carries the
// topic-admin surface. A lazily-created admin connection backs the admin
// methods and is released by [Kafka.Close].
type Kafka struct {
	cfg   connConfig
	admin adminBroker
}

// New builds a Kafka client. It performs no I/O; connections are opened lazily
// by the producer, consumer and admin surfaces.
func New(o Options) *Kafka {
	return &Kafka{cfg: connConfig{
		seedBrokers: append([]string(nil), o.SeedBrokers...),
		clientID:    o.ClientID,
	}}
}

// adminConn returns the shared admin broker, opening it on first use.
func (k *Kafka) adminConn() (adminBroker, error) {
	if k.admin != nil {
		return k.admin, nil
	}
	a, err := newAdminBroker(k.cfg)
	if err != nil {
		return nil, mapError(err, ErrConnection)
	}
	k.admin = a
	return a, nil
}

// CreateTopic creates a topic, mirroring
// kafka.create_topic(name, num_partitions:, replication_factor:).
func (k *Kafka) CreateTopic(name string, numPartitions int32, replicationFactor int16) error {
	a, err := k.adminConn()
	if err != nil {
		return err
	}
	return mapError(a.CreateTopic(context.Background(), name, numPartitions, replicationFactor), ErrConnection)
}

// Topics lists the cluster's topics, mirroring kafka.topics.
func (k *Kafka) Topics() ([]string, error) {
	a, err := k.adminConn()
	if err != nil {
		return nil, err
	}
	names, err := a.ListTopics(context.Background())
	if err != nil {
		return nil, mapError(err, ErrConnection)
	}
	return names, nil
}

// PartitionsFor returns the partition ids of a topic, mirroring
// kafka.partitions_for(name). An unknown topic yields ErrUnknownTopicOrPartition.
func (k *Kafka) PartitionsFor(name string) ([]int32, error) {
	a, err := k.adminConn()
	if err != nil {
		return nil, err
	}
	parts, err := a.PartitionsFor(context.Background(), name)
	if err != nil {
		return nil, mapError(err, ErrConnection)
	}
	return parts, nil
}

// DeleteTopic deletes a topic, mirroring kafka.delete_topic(name).
func (k *Kafka) DeleteTopic(name string) error {
	a, err := k.adminConn()
	if err != nil {
		return err
	}
	return mapError(a.DeleteTopic(context.Background(), name), ErrConnection)
}

// Producer returns a new buffering producer, mirroring kafka.producer.
func (k *Kafka) Producer() *Producer {
	return &Producer{cfg: k.cfg}
}

// Consumer returns a new consumer-group consumer, mirroring
// kafka.consumer(group_id:).
func (k *Kafka) Consumer(groupID string) *Consumer {
	ctx, cancel := context.WithCancel(context.Background())
	return &Consumer{cfg: k.cfg, groupID: groupID, ctx: ctx, cancel: cancel}
}

// DeliverMessage sends a single message synchronously, mirroring
// kafka.deliver_message(value, topic:). It opens a short-lived producer, sends,
// and closes it.
func (k *Kafka) DeliverMessage(value []byte, topic string) error {
	p := k.Producer()
	if err := p.Produce(value, ProduceOptions{Topic: topic}); err != nil {
		return err
	}
	defer p.Shutdown()
	return p.DeliverMessages()
}

// Close releases the shared admin connection, if one was opened.
func (k *Kafka) Close() {
	if k.admin != nil {
		k.admin.Close()
		k.admin = nil
	}
}
