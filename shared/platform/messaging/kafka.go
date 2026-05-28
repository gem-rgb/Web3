package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

// EventEnvelope is the canonical metadata wrapper used for RMS events.
type EventEnvelope struct {
	EventID        string            `json:"event_id"`
	EventType      string            `json:"event_type"`
	AggregateID    string            `json:"aggregate_id,omitempty"`
	TenantID       string            `json:"tenant_id,omitempty"`
	Source         string            `json:"source,omitempty"`
	TraceID        string            `json:"trace_id,omitempty"`
	CorrelationID  string            `json:"correlation_id,omitempty"`
	SchemaVersion  string            `json:"schema_version,omitempty"`
	OccurredAt     time.Time         `json:"occurred_at"`
	Headers        map[string]string `json:"headers,omitempty"`
}

// TopicName returns the canonical RMS topic naming convention.
func TopicName(parts ...string) string {
	cleaned := make([]string, 0, len(parts)+1)
	cleaned = append(cleaned, "rms")
	for _, part := range parts {
		if value := strings.Trim(strings.ToLower(part), "."); value != "" {
			cleaned = append(cleaned, strings.ReplaceAll(value, "_", "-"))
		}
	}
	return strings.Join(cleaned, ".")
}

// DLQTopic returns the dead-letter topic for a source topic.
func DLQTopic(topic string) string {
	return topic + ".dlq"
}

// ReplayTopic returns the replay topic used for controlled reprocessing.
func ReplayTopic(topic string) string {
	return topic + ".replay"
}

// PartitionKey composes a stable key for account and symbol locality.
func PartitionKey(parts ...string) []byte {
	return []byte(strings.Join(parts, "|"))
}

// Producer wraps a kafka.Writer with RMS conventions.
type Producer struct {
	writer      *kafka.Writer
	topicPrefix string
}

// NewProducer returns a Kafka producer configured for idempotent publishing.
func NewProducer(brokers []string, topicPrefix string) *Producer {
	writer := &kafka.Writer{
		Addr:                  kafka.TCP(brokers...),
		Balancer:              &kafka.Hash{},
		AllowAutoTopicCreation: false,
		RequiredAcks:          kafka.RequireAll,
		Async:                 false,
		BatchTimeout:          25 * time.Millisecond,
		BatchSize:             128,
		Compression:           kafka.Snappy,
	}
	return &Producer{writer: writer, topicPrefix: topicPrefix}
}

// PublishJSON encodes payload and envelope into a Kafka message.
func (p *Producer) PublishJSON(ctx context.Context, topic string, key []byte, envelope EventEnvelope, payload any) error {
	if p == nil || p.writer == nil {
		return fmt.Errorf("kafka producer is not configured")
	}
	if p.topicPrefix != "" && !strings.HasPrefix(topic, p.topicPrefix) {
		topic = strings.TrimSuffix(p.topicPrefix, ".") + "." + strings.TrimPrefix(topic, ".")
	}
	if envelope.OccurredAt.IsZero() {
		envelope.OccurredAt = time.Now().UTC()
	}
	envelopeBytes, err := json.Marshal(envelope)
	if err != nil {
		return err
	}
	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	message := kafka.Message{
		Topic: topic,
		Key:   key,
		Value: bodyBytes,
		Headers: []kafka.Header{
			{Key: "event-envelope", Value: envelopeBytes},
		},
		Time: envelope.OccurredAt,
	}
	return p.writer.WriteMessages(ctx, message)
}

// Close closes the underlying Kafka writer.
func (p *Producer) Close() error {
	if p == nil || p.writer == nil {
		return nil
	}
	return p.writer.Close()
}

// Consumer wraps a kafka.Reader.
type Consumer struct {
	reader *kafka.Reader
}

// NewConsumer returns a reader configured for manual offset commits.
func NewConsumer(brokers []string, topic, groupID string, minBytes, maxBytes int) *Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     append([]string(nil), brokers...),
		Topic:       topic,
		GroupID:     groupID,
		MinBytes:    minBytes,
		MaxBytes:    maxBytes,
		StartOffset: kafka.LastOffset,
	})
	return &Consumer{reader: reader}
}

// ReadMessage returns the next message in the stream.
func (c *Consumer) ReadMessage(ctx context.Context) (kafka.Message, error) {
	if c == nil || c.reader == nil {
		return kafka.Message{}, fmt.Errorf("kafka consumer is not configured")
	}
	return c.reader.ReadMessage(ctx)
}

// Close closes the underlying Kafka reader.
func (c *Consumer) Close() error {
	if c == nil || c.reader == nil {
		return nil
	}
	return c.reader.Close()
}

// Deduplicator provides a tiny in-memory idempotency cache for phase 1.
type Deduplicator struct {
	mu   sync.Mutex
	ttl  time.Duration
	seen map[string]time.Time
}

// NewDeduplicator returns a TTL-based deduplication cache.
func NewDeduplicator(ttl time.Duration) *Deduplicator {
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	return &Deduplicator{
		ttl:  ttl,
		seen: map[string]time.Time{},
	}
}

// Seen returns true when the key was observed within the TTL window.
func (d *Deduplicator) Seen(key string) bool {
	if d == nil {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()

	now := time.Now().UTC()
	cutoff := now.Add(-d.ttl)
	for existing, timestamp := range d.seen {
		if timestamp.Before(cutoff) {
			delete(d.seen, existing)
		}
	}
	if _, ok := d.seen[key]; ok {
		return true
	}
	d.seen[key] = now
	return false
}
