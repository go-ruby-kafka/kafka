package kafka

import (
	"errors"

	"github.com/twmb/franz-go/pkg/kerr"
)

// The Kafka error tree, mirroring ruby-kafka's Kafka::*Error classes. Every
// error returned by this package wraps one of these sentinels, so callers can
// classify failures with errors.Is, e.g.:
//
//	if errors.Is(err, kafka.ErrUnknownTopicOrPartition) { ... }
var (
	// ErrDeliveryFailed is Kafka::DeliveryFailed — a produce did not succeed.
	ErrDeliveryFailed = errors.New("kafka: delivery failed")
	// ErrConnection is Kafka::ConnectionError — the broker could not be reached
	// or a request failed at the transport level.
	ErrConnection = errors.New("kafka: connection error")
	// ErrUnknownTopicOrPartition is Kafka::UnknownTopicOrPartition.
	ErrUnknownTopicOrPartition = errors.New("kafka: unknown topic or partition")
	// ErrOffsetCommit is Kafka::OffsetCommitError — committing consumer-group
	// offsets failed.
	ErrOffsetCommit = errors.New("kafka: offset commit error")
	// ErrOffsetOutOfRange is Kafka::OffsetOutOfRange — a fetch requested an
	// offset the broker no longer holds.
	ErrOffsetOutOfRange = errors.New("kafka: offset out of range")
)

// wrappedError carries both a package sentinel (for errors.Is classification)
// and the underlying franz-go/kerr error (for the human-readable detail).
type wrappedError struct {
	base error
	err  error
}

func (w *wrappedError) Error() string {
	return w.base.Error() + ": " + w.err.Error()
}

// Unwrap returns both wrapped errors so errors.Is matches the package sentinel
// and the underlying kerr error.
func (w *wrappedError) Unwrap() []error {
	return []error{w.base, w.err}
}

// mapError classifies a franz-go/kerr error into the Kafka error tree. A nil
// error maps to nil. Errors carrying a recognised Kafka error code map to their
// specific sentinel; everything else maps to the surface's default sentinel
// (DeliveryFailed for produce, ConnectionError for fetch/admin, and so on).
func mapError(err, dflt error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, kerr.UnknownTopicOrPartition):
		return &wrappedError{ErrUnknownTopicOrPartition, err}
	case errors.Is(err, kerr.OffsetOutOfRange):
		return &wrappedError{ErrOffsetOutOfRange, err}
	default:
		return &wrappedError{dflt, err}
	}
}
