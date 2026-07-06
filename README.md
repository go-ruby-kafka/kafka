<p align="center"><img src="https://raw.githubusercontent.com/go-ruby-kafka/brand/main/social/go-ruby-kafka-kafka.png" alt="go-ruby-kafka/kafka" width="720"></p>

# kafka — go-ruby-kafka

[![Docs](https://img.shields.io/badge/docs-mkdocs--material-DC2626)](https://go-ruby-kafka.github.io/docs/)
[![License](https://img.shields.io/badge/license-BSD--3--Clause-blue)](LICENSE)
[![Go](https://img.shields.io/badge/go-1.26.4%2B-00ADD8)](https://go.dev/dl/)
[![Coverage](https://img.shields.io/badge/coverage-100%25-1a7f37)](#tests--coverage)

**A pure-Go (no cgo), MRI-faithful reimplementation of the Ruby
[`ruby-kafka`](https://github.com/zendesk/ruby-kafka) gem's client surface** —
the object model a Ruby program drives when it calls `Kafka.new`,
`kafka.producer`, `kafka.consumer` and the topic-admin helpers. It mirrors the
gem's method names and semantics: a buffering producer with
`produce` / `deliver_messages`, a one-shot `deliver_message`, a consumer-group
consumer with `subscribe` / `each_message` / `each_batch` / `commit_offsets` /
`stop`, and `create_topic` / `topics` / `partitions_for` / `delete_topic`.

It is the Kafka backend for
[go-embedded-ruby](https://github.com/go-embedded-ruby/ruby), but is a
**standalone, reusable** module — a sibling of
[go-ruby-redis](https://github.com/go-ruby-redis/redis) and
[go-ruby-pg](https://github.com/go-ruby-pg/pg).

> **What it is — and what it consumes.** The Kafka wire protocol is **not**
> reimplemented here (the common `rdkafka` gem is a C extension). Instead this
> package binds the pure-Go [franz-go](https://github.com/twmb/franz-go) client
> (`kgo` + `kadm`) for the protocol, and wraps it in ruby-kafka's shape. The
> broker connection is a **host seam**: the constructors that dial a broker are
> injectable, so the test suite runs deterministic round-trips against the
> in-process [`kfake`](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kfake)
> broker — no external Kafka — and drives the error branches through a mock.

## Surface

- **Client** — `New(Options{SeedBrokers, ClientID})` → `*Kafka`, mirroring
  `Kafka.new(seed_brokers:, client_id:)`.
- **Producer** — `kafka.Producer()` → `Producer`; `Produce(value, ProduceOptions{Topic, Key, Partition, Headers})`
  buffers, `DeliverMessages()` flushes, `Shutdown()` closes. The synchronous
  one-shot is `kafka.DeliverMessage(value, topic)`. An explicit `Partition`
  is honoured; otherwise records balance by key hash.
- **Consumer** — `kafka.Consumer(groupID)` → `Consumer`; `Subscribe(topic, startFromBeginning)`,
  the poll loops `EachMessage(func(*Message) error)` and `EachBatch(func(*Batch) error)`,
  explicit `CommitOffsets()`, and `Stop()`. A `Message` carries the
  subject-like `Topic` plus `Partition` / `Offset` / `Key` / `Value` /
  `Headers` / `CreateTime`.
- **Admin** — `CreateTopic(name, numPartitions, replicationFactor)`, `Topics()`,
  `PartitionsFor(name)`, `DeleteTopic(name)`.
- **Errors** — the `Kafka::*Error` tree: `ErrDeliveryFailed`, `ErrConnection`,
  `ErrUnknownTopicOrPartition`, `ErrOffsetCommit`, `ErrOffsetOutOfRange`. Every
  returned error wraps one of these sentinels (classify with `errors.Is`) and
  the underlying franz-go/`kerr` error.

## Usage

```go
import "github.com/go-ruby-kafka/kafka"

k := kafka.New(kafka.Options{SeedBrokers: []string{"localhost:9092"}, ClientID: "app"})
defer k.Close()

k.CreateTopic("orders", 3, 1)

// Producer: buffer, then deliver.
p := k.Producer()
defer p.Shutdown()
p.Produce([]byte(`{"id":1}`), kafka.ProduceOptions{Topic: "orders", Key: []byte("1")})
p.DeliverMessages()

// Or a one-shot synchronous send.
k.DeliverMessage([]byte("ping"), "orders")

// Consumer group: subscribe and iterate.
c := k.Consumer("billing")
c.Subscribe("orders", true) // start_from_beginning
c.EachMessage(func(m *kafka.Message) error {
    process(m.Topic, m.Partition, m.Offset, m.Value)
    return c.CommitOffsets()
})
```

## Tests & coverage

The suite holds **100% statement coverage** with `-race`. Two layers reach it:

- **kfake round-trips** — an in-process fake Kafka cluster
  ([`kfake`](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kfake)) exercises
  the real franz-go producer / consumer / admin: produce→consume delivers the
  correct key / value / headers / partition / offset, a consumer group resumes
  after a committed offset, and topic create / list / partitions / delete are
  verified as real round-trips.
- **Deterministic mock** — the broker seam is replaced by a mock to drive every
  error branch (delivery failure, connection loss, unknown topic, offset-commit
  and offset-out-of-range mapping) without any network.

The kfake round-trips run on the native CI lanes (Linux/macOS/Windows and
amd64/arm64). Under qemu-user emulation a full protocol broker is too heavy, so
the cross-arch lanes set `GO_RUBY_KAFKA_KFAKE=0` and run the mock suite; the
coverage gate is met on the native lanes where kfake executes.

```sh
GOWORK=off go test -race ./...          # full suite (kfake + mock)
GO_RUBY_KAFKA_KFAKE=0 go test ./...     # deterministic mock suite only
```

Validated CGO-free on all six supported 64-bit targets — amd64, arm64, riscv64,
loong64, ppc64le and **s390x** (big-endian) — across Linux, macOS and Windows.

## License

BSD-3-Clause — see [LICENSE](LICENSE). Copyright the go-ruby-kafka/kafka authors.

## WebAssembly

Being pure Go (CGO=0), this library also compiles to **WebAssembly** — both
`GOOS=js GOARCH=wasm` (browser / Node.js) and `GOOS=wasip1 GOARCH=wasm` (WASI).
CI builds both targets on every push, alongside the six 64-bit native/qemu arches.

```sh
GOOS=js     GOARCH=wasm go build ./...   # browser / Node
GOOS=wasip1 GOARCH=wasm go build ./...   # WASI (wasmtime, wasmer, wasmedge, …)
```
