package cache

import (
	"context"
	"encoding/json"
	"time"

	pb "github.com/example/rms/shared/proto"
)

// StorePosition persists a hot position snapshot.
func (c *RiskCache) StorePosition(ctx context.Context, tenantID string, position *pb.Position, ttl time.Duration) error {
	if !c.enabled() || position == nil {
		return nil
	}
	payload, err := json.Marshal(position)
	if err != nil {
		return err
	}
	return c.client.Set(ctx, c.key("position", tenantID, position.AccountId, position.Symbol), string(payload), ttl)
}

// LoadPosition returns a hot position snapshot.
func (c *RiskCache) LoadPosition(ctx context.Context, tenantID, accountID, symbol string) (*pb.Position, bool, error) {
	if !c.enabled() {
		return nil, false, nil
	}
	value, ok, err := c.client.Get(ctx, c.key("position", tenantID, accountID, symbol))
	if err != nil {
		return nil, false, err
	}
	if !ok || value == "" {
		return nil, false, nil
	}
	var position pb.Position
	if err := json.Unmarshal([]byte(value), &position); err != nil {
		return nil, false, err
	}
	return &position, true, nil
}

// StorePositions persists an account-level position list.
func (c *RiskCache) StorePositions(ctx context.Context, tenantID, accountID string, positions []*pb.Position, ttl time.Duration) error {
	if !c.enabled() {
		return nil
	}
	payload, err := json.Marshal(positions)
	if err != nil {
		return err
	}
	return c.client.Set(ctx, c.key("positions", tenantID, accountID), string(payload), ttl)
}

// LoadPositions returns the cached account-level position list.
func (c *RiskCache) LoadPositions(ctx context.Context, tenantID, accountID string) ([]*pb.Position, bool, error) {
	if !c.enabled() {
		return nil, false, nil
	}
	value, ok, err := c.client.Get(ctx, c.key("positions", tenantID, accountID))
	if err != nil {
		return nil, false, err
	}
	if !ok || value == "" {
		return nil, false, nil
	}
	var positions []*pb.Position
	if err := json.Unmarshal([]byte(value), &positions); err != nil {
		return nil, false, err
	}
	return positions, true, nil
}

// StorePositionSnapshot writes a materialized position snapshot.
func (c *RiskCache) StorePositionSnapshot(ctx context.Context, snapshot *pb.PositionSnapshot, ttl time.Duration) error {
	if !c.enabled() || snapshot == nil {
		return nil
	}
	payload, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}
	return c.client.Set(ctx, c.key("position", "snapshot", snapshot.TenantID, snapshot.AccountID, snapshot.SnapshotID), string(payload), ttl)
}

// LoadPositionSnapshot fetches a materialized position snapshot.
func (c *RiskCache) LoadPositionSnapshot(ctx context.Context, tenantID, accountID, snapshotID string) (*pb.PositionSnapshot, bool, error) {
	if !c.enabled() {
		return nil, false, nil
	}
	value, ok, err := c.client.Get(ctx, c.key("position", "snapshot", tenantID, accountID, snapshotID))
	if err != nil {
		return nil, false, err
	}
	if !ok || value == "" {
		return nil, false, nil
	}
	var snapshot pb.PositionSnapshot
	if err := json.Unmarshal([]byte(value), &snapshot); err != nil {
		return nil, false, err
	}
	return &snapshot, true, nil
}

// StoreExposure persists a hot exposure snapshot.
func (c *RiskCache) StoreExposure(ctx context.Context, tenantID string, exposure *pb.AggregatedExposure, ttl time.Duration) error {
	if !c.enabled() || exposure == nil {
		return nil
	}
	payload, err := json.Marshal(exposure)
	if err != nil {
		return err
	}
	return c.client.Set(ctx, c.key("exposure", tenantID, exposure.AccountId, exposure.Symbol), string(payload), ttl)
}

// LoadExposure returns a hot exposure snapshot.
func (c *RiskCache) LoadExposure(ctx context.Context, tenantID, accountID, symbol string) (*pb.AggregatedExposure, bool, error) {
	if !c.enabled() {
		return nil, false, nil
	}
	value, ok, err := c.client.Get(ctx, c.key("exposure", tenantID, accountID, symbol))
	if err != nil {
		return nil, false, err
	}
	if !ok || value == "" {
		return nil, false, nil
	}
	var exposure pb.AggregatedExposure
	if err := json.Unmarshal([]byte(value), &exposure); err != nil {
		return nil, false, err
	}
	return &exposure, true, nil
}

// StoreExposures persists an account-level exposure list.
func (c *RiskCache) StoreExposures(ctx context.Context, tenantID, accountID string, exposures []*pb.AggregatedExposure, ttl time.Duration) error {
	if !c.enabled() {
		return nil
	}
	payload, err := json.Marshal(exposures)
	if err != nil {
		return err
	}
	return c.client.Set(ctx, c.key("exposures", tenantID, accountID), string(payload), ttl)
}

// LoadExposures returns the cached account-level exposure list.
func (c *RiskCache) LoadExposures(ctx context.Context, tenantID, accountID string) ([]*pb.AggregatedExposure, bool, error) {
	if !c.enabled() {
		return nil, false, nil
	}
	value, ok, err := c.client.Get(ctx, c.key("exposures", tenantID, accountID))
	if err != nil {
		return nil, false, err
	}
	if !ok || value == "" {
		return nil, false, nil
	}
	var exposures []*pb.AggregatedExposure
	if err := json.Unmarshal([]byte(value), &exposures); err != nil {
		return nil, false, err
	}
	return exposures, true, nil
}

// StoreExposureSnapshot writes a materialized exposure snapshot.
func (c *RiskCache) StoreExposureSnapshot(ctx context.Context, snapshot *pb.ExposureSnapshot, ttl time.Duration) error {
	if !c.enabled() || snapshot == nil {
		return nil
	}
	payload, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}
	return c.client.Set(ctx, c.key("exposure", "snapshot", snapshot.TenantID, snapshot.AccountID, snapshot.Symbol, snapshot.SnapshotID), string(payload), ttl)
}

// LoadExposureSnapshot fetches a materialized exposure snapshot.
func (c *RiskCache) LoadExposureSnapshot(ctx context.Context, tenantID, accountID, symbol, snapshotID string) (*pb.ExposureSnapshot, bool, error) {
	if !c.enabled() {
		return nil, false, nil
	}
	value, ok, err := c.client.Get(ctx, c.key("exposure", "snapshot", tenantID, accountID, symbol, snapshotID))
	if err != nil {
		return nil, false, err
	}
	if !ok || value == "" {
		return nil, false, nil
	}
	var snapshot pb.ExposureSnapshot
	if err := json.Unmarshal([]byte(value), &snapshot); err != nil {
		return nil, false, err
	}
	return &snapshot, true, nil
}

// StorePortfolioRisk writes a portfolio risk summary to Redis.
func (c *RiskCache) StorePortfolioRisk(ctx context.Context, summary *pb.PortfolioRiskSummary, ttl time.Duration) error {
	if !c.enabled() || summary == nil {
		return nil
	}
	payload, err := json.Marshal(summary)
	if err != nil {
		return err
	}
	return c.client.Set(ctx, c.key("portfolio", "risk", summary.TenantID, summary.AccountID), string(payload), ttl)
}

// LoadPortfolioRisk fetches a cached portfolio risk summary.
func (c *RiskCache) LoadPortfolioRisk(ctx context.Context, tenantID, accountID string) (*pb.PortfolioRiskSummary, bool, error) {
	if !c.enabled() {
		return nil, false, nil
	}
	value, ok, err := c.client.Get(ctx, c.key("portfolio", "risk", tenantID, accountID))
	if err != nil {
		return nil, false, err
	}
	if !ok || value == "" {
		return nil, false, nil
	}
	var summary pb.PortfolioRiskSummary
	if err := json.Unmarshal([]byte(value), &summary); err != nil {
		return nil, false, err
	}
	return &summary, true, nil
}
