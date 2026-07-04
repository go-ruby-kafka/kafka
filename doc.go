// Package kafka is a pure-Go (CGO-free), MRI-faithful reimplementation of the
// Ruby [ruby-kafka] gem's client surface — the object model a Ruby program
// drives when it calls Kafka.new, kafka.producer, kafka.consumer and the topic
// admin helpers.
//
// It mirrors the gem's method names and semantics:
//
//   - [New] returns a [Kafka] client, the analogue of Kafka.new(seed_brokers:,
//     client_id:).
//   - [Kafka.Producer] returns a buffering [Producer]; #produce buffers a
//     message and #deliver_messages flushes the buffer. [Kafka.DeliverMessage]
//     is the one-shot synchronous send.
//   - [Kafka.Consumer] returns a consumer-group [Consumer]; #subscribe registers
//     topics and #each_message / #each_batch drive the poll loop, with explicit
//     #commit_offsets and #stop.
//   - [Kafka.CreateTopic], [Kafka.Topics], [Kafka.PartitionsFor] and
//     [Kafka.DeleteTopic] are the admin surface.
//
// The Kafka wire protocol is NOT reimplemented here. This package consumes the
// pure-Go [franz-go] client (kgo + kadm) for the protocol and, in its tests, the
// in-process [kfake] broker for deterministic round-trips with no external
// Kafka. The broker connection is a host seam: the constructors that dial a
// broker are package variables, so tests inject either the real franz-go client
// (pointed at kfake) or a deterministic mock.
//
// [ruby-kafka]: https://github.com/zendesk/ruby-kafka
// [franz-go]: https://github.com/twmb/franz-go
// [kfake]: https://pkg.go.dev/github.com/twmb/franz-go/pkg/kfake
package kafka
