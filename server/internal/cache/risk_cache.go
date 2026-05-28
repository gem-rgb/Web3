package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	pb "github.com/example/rms/shared/proto"
	"github.com/example/rms/server/internal/redisx"
)

// RiskCache wraps Redis keys used by the phase 2 risk pipeline.
type RiskCache struct {
	client *redisx.Client
	prefix string
}

// NewRiskCache returns a new cache wrapper.
func NewRiskCache(client *redisx.Client, prefix string) *RiskCache {
	prefix = strings.Trim(strings.TrimSpace(prefix), ":")
	if prefix == "" {
		prefix = "rms"
	}
	return &RiskCache{client: client, prefix: prefix}
}

func (c *RiskCache) enabled() bool {
	return c != nil && c.client != nil
}

func (c *RiskCache) key(parts ...string) string {
	cleaned := make([]string, 0, len(parts)+1)
	cleaned = append(cleaned, c.prefix)
	for _, part := range parts {
		if value := strings.TrimSpace(part); value != "" {
			cleaned = append(cleaned, strings.ReplaceAll(strings.ToLower(value), " ", "-"))
		}
	}
	return strings.Join(cleaned, ":")
}

// ReserveIdempotency attempts to reserve an order idempotency key.
func (c *RiskCache) ReserveIdempotency(ctx context.Context, tenantID, key string, ttl time.Duration) (bool, error) {
	if !c.enabled() {
		return true, nil
	}
	return c.client.SetNX(ctx, c.key("idempotency", tenantID, key), "1", ttl)
}

// StoreFingerprint records an order fingerprint so duplicates can be detected later.
func (c *RiskCache) StoreFingerprint(ctx context.Context, tenantID, accountID, fingerprint string, ttl time.Duration) (bool, error) {
	if !c.enabled() {
		return true, nil
	}
	return c.client.SetNX(ctx, c.key("fingerprint", tenantID, accountID, fingerprint), "1", ttl)
}

// HasFingerprint returns true when the fingerprint already exists.
func (c *RiskCache) HasFingerprint(ctx context.Context, tenantID, accountID, fingerprint string) (bool, error) {
	if !c.enabled() {
		return false, nil
	}
	exists, err := c.client.Exists(ctx, c.key("fingerprint", tenantID, accountID, fingerprint))
	if err != nil {
		return false, err
	}
	return exists, nil
}

// IncrementFrequency increments the per-minute order frequency counter.
func (c *RiskCache) IncrementFrequency(ctx context.Context, tenantID, accountID string, window time.Duration) (int64, error) {
	if !c.enabled() {
		return 0, nil
	}
	key := c.frequencyKey(tenantID, accountID, window, time.Now().UTC())
	pipe := c.client.Pipeline()
	pipe.Add("INCR", key)
	pipe.Add("EXPIRE", key, int64((window+5*time.Second)/time.Second))
	replies, err := pipe.Exec(ctx)
	if err != nil {
		return 0, err
	}
	if len(replies) == 0 {
		return 0, nil
	}
	switch value := replies[0].(type) {
	case int64:
		return value, nil
	case string:
		return strconv.ParseInt(value, 10, 64)
	default:
		return 0, fmt.Errorf("unexpected frequency reply %T", replies[0])
	}
}

// CurrentFrequency reads the current window counter without incrementing it.
func (c *RiskCache) CurrentFrequency(ctx context.Context, tenantID, accountID string, window time.Duration) (int64, bool, error) {
	if !c.enabled() {
		return 0, false, nil
	}
	value, ok, err := c.client.Get(ctx, c.frequencyKey(tenantID, accountID, window, time.Now().UTC()))
	if err != nil {
		return 0, false, err
	}
	if !ok || value == "" {
		return 0, false, nil
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, false, err
	}
	return parsed, true, nil
}

// StoreDecision writes the decision to Redis for fast lookup.
func (c *RiskCache) StoreDecision(ctx context.Context, decision *pb.RiskDecision, ttl time.Duration) error {
	if !c.enabled() || decision == nil {
		return nil
	}
	decisionKey := c.key("decision", decision.TenantID, decision.OrderID)
	correlationKey := c.key("decision", "correlation", decision.TenantID, decision.CorrelationID)
	decisionJSON, err := json.Marshal(decision)
	if err != nil {
		return err
	}
	pipe := c.client.Pipeline()
	pipe.Add("SET", decisionKey, decisionJSON, "PX", int64(ttl/time.Millisecond))
	if decision.CorrelationID != "" {
		pipe.Add("SET", correlationKey, decisionJSON, "PX", int64(ttl/time.Millisecond))
	}
	_, err = pipe.Exec(ctx)
	return err
}

// LoadDecision fetches a decision by order identifier.
func (c *RiskCache) LoadDecision(ctx context.Context, tenantID, orderID string) (*pb.RiskDecision, bool, error) {
	if !c.enabled() {
		return nil, false, nil
	}
	value, ok, err := c.client.Get(ctx, c.key("decision", tenantID, orderID))
	if err != nil {
		return nil, false, err
	}
	if !ok || value == "" {
		return nil, false, nil
	}
	return decodeDecision(value)
}

// LoadDecisionByCorrelation fetches a decision by correlation identifier.
func (c *RiskCache) LoadDecisionByCorrelation(ctx context.Context, tenantID, correlationID string) (*pb.RiskDecision, bool, error) {
	if !c.enabled() {
		return nil, false, nil
	}
	value, ok, err := c.client.Get(ctx, c.key("decision", "correlation", tenantID, correlationID))
	if err != nil {
		return nil, false, err
	}
	if !ok || value == "" {
		return nil, false, nil
	}
	return decodeDecision(value)
}

// StoreAccount persists a hot account snapshot.
func (c *RiskCache) StoreAccount(ctx context.Context, tenantID string, account *pb.Account, ttl time.Duration) error {
	if !c.enabled() || account == nil {
		return nil
	}
	payload, err := json.Marshal(account)
	if err != nil {
		return err
	}
	return c.client.Set(ctx, c.key("account", tenantID, account.AccountId), string(payload), ttl)
}

// LoadAccount returns a cached account snapshot.
func (c *RiskCache) LoadAccount(ctx context.Context, tenantID, accountID string) (*pb.Account, bool, error) {
	if !c.enabled() {
		return nil, false, nil
	}
	value, ok, err := c.client.Get(ctx, c.key("account", tenantID, accountID))
	if err != nil {
		return nil, false, err
	}
	if !ok || value == "" {
		return nil, false, nil
	}
	var account pb.Account
	if err := json.Unmarshal([]byte(value), &account); err != nil {
		return nil, false, err
	}
	return &account, true, nil
}

// StoreMarketPrice persists the most recent market price for a symbol.
func (c *RiskCache) StoreMarketPrice(ctx context.Context, tenantID, symbol string, price float64, ttl time.Duration) error {
	if !c.enabled() {
		return nil
	}
	return c.client.Set(ctx, c.key("market", tenantID, symbol), strconv.FormatFloat(price, 'f', -1, 64), ttl)
}

// LoadMarketPrice reads the most recent market price for a symbol.
func (c *RiskCache) LoadMarketPrice(ctx context.Context, tenantID, symbol string) (float64, bool, error) {
	if !c.enabled() {
		return 0, false, nil
	}
	value, ok, err := c.client.Get(ctx, c.key("market", tenantID, symbol))
	if err != nil {
		return 0, false, err
	}
	if !ok || value == "" {
		return 0, false, nil
	}
	price, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0, false, err
	}
	return price, true, nil
}

// StoreRuleVersion records the active rule snapshot version.
func (c *RiskCache) StoreRuleVersion(ctx context.Context, tenantID, version string, ttl time.Duration) error {
	if !c.enabled() {
		return nil
	}
	return c.client.Set(ctx, c.key("rules", "version", tenantID), version, ttl)
}

// LoadRuleVersion returns the cached active rule version.
func (c *RiskCache) LoadRuleVersion(ctx context.Context, tenantID string) (string, bool, error) {
	if !c.enabled() {
		return "", false, nil
	}
	value, ok, err := c.client.Get(ctx, c.key("rules", "version", tenantID))
	if err != nil {
		return "", false, err
	}
	return value, ok, nil
}

func (c *RiskCache) frequencyKey(tenantID, accountID string, window time.Duration, now time.Time) string {
	bucket := now.UTC().Truncate(window).Unix()
	return c.key("frequency", tenantID, accountID, strconv.FormatInt(bucket, 10))
}

func decodeDecision(raw string) (*pb.RiskDecision, bool, error) {
	var decision pb.RiskDecision
	if err := json.Unmarshal([]byte(raw), &decision); err != nil {
		return nil, false, err
	}
	return &decision, true, nil
}
