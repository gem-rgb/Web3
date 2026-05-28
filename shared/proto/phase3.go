package proto

import "time"

// PositionEvent represents an immutable append-only position change.
type PositionEvent struct {
	EventID        string            `json:"event_id,omitempty"`
	TenantID       string            `json:"tenant_id,omitempty"`
	AccountID      string            `json:"account_id,omitempty"`
	Symbol         string            `json:"symbol,omitempty"`
	EventType      string            `json:"event_type,omitempty"`
	QuantityDelta  int32             `json:"quantity_delta,omitempty"`
	Price          float64           `json:"price,omitempty"`
	MarketPrice    float64           `json:"market_price,omitempty"`
	Sector         string            `json:"sector,omitempty"`
	Source         string            `json:"source,omitempty"`
	Sequence       int64             `json:"sequence,omitempty"`
	OccurredAt     int64             `json:"occurred_at,omitempty"`
	CorrelationID  string            `json:"correlation_id,omitempty"`
	TraceID        string            `json:"trace_id,omitempty"`
	Metadata       map[string]string `json:"metadata,omitempty"`
}

// PositionSnapshot captures a materialized position view at a point in time.
type PositionSnapshot struct {
	SnapshotID     string            `json:"snapshot_id,omitempty"`
	TenantID       string            `json:"tenant_id,omitempty"`
	AccountID      string            `json:"account_id,omitempty"`
	Version        int64             `json:"version,omitempty"`
	EventOffset    int64             `json:"event_offset,omitempty"`
	SnapshotVersion string            `json:"snapshot_version,omitempty"`
	Positions      []*Position       `json:"positions,omitempty"`
	Checksum       string            `json:"checksum,omitempty"`
	CreatedAt      int64             `json:"created_at,omitempty"`
	Metadata       map[string]string `json:"metadata,omitempty"`
}

// ExposureSnapshot captures the materialized exposure state.
type ExposureSnapshot struct {
	SnapshotID     string            `json:"snapshot_id,omitempty"`
	TenantID       string            `json:"tenant_id,omitempty"`
	AccountID      string            `json:"account_id,omitempty"`
	Symbol         string            `json:"symbol,omitempty"`
	Sector         string            `json:"sector,omitempty"`
	Version        int64             `json:"version,omitempty"`
	EventOffset    int64             `json:"event_offset,omitempty"`
	SnapshotVersion string            `json:"snapshot_version,omitempty"`
	Exposure       *AggregatedExposure `json:"exposure,omitempty"`
	Checksum       string            `json:"checksum,omitempty"`
	CreatedAt      int64             `json:"created_at,omitempty"`
	Metadata       map[string]string `json:"metadata,omitempty"`
}

// PositionSnapshotRequest fetches the latest materialized position snapshot.
type PositionSnapshotRequest struct {
	TenantID        string `json:"tenant_id,omitempty"`
	AccountID       string `json:"account_id,omitempty"`
	SnapshotVersion string `json:"snapshot_version,omitempty"`
}

// PositionSnapshotResponse returns the stored position snapshot payload.
type PositionSnapshotResponse struct {
	Snapshot  *PositionSnapshot `json:"snapshot,omitempty"`
	Timestamp int64             `json:"timestamp,omitempty"`
}

// PositionReplayRequest asks the position store to replay a sequence range.
type PositionReplayRequest struct {
	TenantID        string            `json:"tenant_id,omitempty"`
	AccountID       string            `json:"account_id,omitempty"`
	FromSequence    int64             `json:"from_sequence,omitempty"`
	ToSequence      int64             `json:"to_sequence,omitempty"`
	SnapshotVersion string            `json:"snapshot_version,omitempty"`
	Reason          string            `json:"reason,omitempty"`
	Metadata        map[string]string `json:"metadata,omitempty"`
}

// PositionReplayResponse reports replay progress for a position aggregate.
type PositionReplayResponse struct {
	Replayed        int64  `json:"replayed,omitempty"`
	Recovered       bool   `json:"recovered,omitempty"`
	SnapshotVersion string `json:"snapshot_version,omitempty"`
	Timestamp       int64  `json:"timestamp,omitempty"`
}

// PositionReconciliationRequest requests a drift check and repair cycle.
type PositionReconciliationRequest struct {
	TenantID  string            `json:"tenant_id,omitempty"`
	AccountID string            `json:"account_id,omitempty"`
	Symbol    string            `json:"symbol,omitempty"`
	Force     bool              `json:"force,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

// PositionReconciliationResponse reports reconciliation results for positions.
type PositionReconciliationResponse struct {
	Reconciled    bool              `json:"reconciled,omitempty"`
	DriftDetected bool              `json:"drift_detected,omitempty"`
	DriftCount    int64             `json:"drift_count,omitempty"`
	Details       map[string]string `json:"details,omitempty"`
	Timestamp     int64             `json:"timestamp,omitempty"`
}

// ExposureSnapshotRequest fetches the latest materialized exposure snapshot.
type ExposureSnapshotRequest struct {
	TenantID        string `json:"tenant_id,omitempty"`
	AccountID       string `json:"account_id,omitempty"`
	Symbol          string `json:"symbol,omitempty"`
	SnapshotVersion string `json:"snapshot_version,omitempty"`
}

// ExposureSnapshotResponse returns the stored exposure snapshot payload.
type ExposureSnapshotResponse struct {
	Snapshot  *ExposureSnapshot `json:"snapshot,omitempty"`
	Timestamp int64             `json:"timestamp,omitempty"`
}

// ExposureReplayRequest asks the exposure store to replay a sequence range.
type ExposureReplayRequest struct {
	TenantID        string            `json:"tenant_id,omitempty"`
	AccountID       string            `json:"account_id,omitempty"`
	Symbol          string            `json:"symbol,omitempty"`
	FromSequence    int64             `json:"from_sequence,omitempty"`
	ToSequence      int64             `json:"to_sequence,omitempty"`
	SnapshotVersion string            `json:"snapshot_version,omitempty"`
	Reason          string            `json:"reason,omitempty"`
	Metadata        map[string]string `json:"metadata,omitempty"`
}

// ExposureReplayResponse reports replay progress for an exposure aggregate.
type ExposureReplayResponse struct {
	Replayed        int64  `json:"replayed,omitempty"`
	Recovered       bool   `json:"recovered,omitempty"`
	SnapshotVersion string `json:"snapshot_version,omitempty"`
	Timestamp       int64  `json:"timestamp,omitempty"`
}

// ExposureReconciliationRequest requests a drift check and repair cycle.
type ExposureReconciliationRequest struct {
	TenantID  string            `json:"tenant_id,omitempty"`
	AccountID string            `json:"account_id,omitempty"`
	Symbol    string            `json:"symbol,omitempty"`
	Force     bool              `json:"force,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

// ExposureReconciliationResponse reports reconciliation results for exposures.
type ExposureReconciliationResponse struct {
	Reconciled    bool              `json:"reconciled,omitempty"`
	DriftDetected bool              `json:"drift_detected,omitempty"`
	DriftCount    int64             `json:"drift_count,omitempty"`
	Details       map[string]string `json:"details,omitempty"`
	Timestamp     int64             `json:"timestamp,omitempty"`
}

// PortfolioRiskSummary aggregates account and tenant risk metrics.
type PortfolioRiskSummary struct {
	TenantID           string             `json:"tenant_id,omitempty"`
	AccountID          string             `json:"account_id,omitempty"`
	GrossExposure      float64            `json:"gross_exposure,omitempty"`
	NetExposure        float64            `json:"net_exposure,omitempty"`
	LongExposure       float64            `json:"long_exposure,omitempty"`
	ShortExposure      float64            `json:"short_exposure,omitempty"`
	UnrealizedPL       float64            `json:"unrealized_pl,omitempty"`
	RealizedPL         float64            `json:"realized_pl,omitempty"`
	Leverage           float64            `json:"leverage,omitempty"`
	MaxPositionExposure float64           `json:"max_position_exposure,omitempty"`
	ConcentrationPct   float64            `json:"concentration_pct,omitempty"`
	SymbolExposure     map[string]float64 `json:"symbol_exposure,omitempty"`
	SectorExposure     map[string]float64 `json:"sector_exposure,omitempty"`
	CrossAccountExposure map[string]float64 `json:"cross_account_exposure,omitempty"`
	PositionCount      int32              `json:"position_count,omitempty"`
	SnapshotVersion    string             `json:"snapshot_version,omitempty"`
	CalculatedAt       int64              `json:"calculated_at,omitempty"`
	DriftDetected      bool               `json:"drift_detected,omitempty"`
	RiskState          string             `json:"risk_state,omitempty"`
	Metadata           map[string]string  `json:"metadata,omitempty"`
}

// PortfolioRiskRequest requests a point-in-time risk calculation.
type PortfolioRiskRequest struct {
	TenantID           string            `json:"tenant_id,omitempty"`
	AccountID          string            `json:"account_id,omitempty"`
	AsOf               int64             `json:"as_of,omitempty"`
	IncludeCrossAccount bool              `json:"include_cross_account,omitempty"`
	IncludeSector      bool              `json:"include_sector,omitempty"`
	Symbols            []string          `json:"symbols,omitempty"`
	SnapshotVersion    string            `json:"snapshot_version,omitempty"`
	Metadata           map[string]string `json:"metadata,omitempty"`
}

// PortfolioRiskResponse returns the calculated portfolio risk summary.
type PortfolioRiskResponse struct {
	Summary   *PortfolioRiskSummary `json:"summary,omitempty"`
	Timestamp int64                 `json:"timestamp,omitempty"`
}

// PortfolioSnapshotRequest fetches the latest portfolio snapshot.
type PortfolioSnapshotRequest struct {
	TenantID        string `json:"tenant_id,omitempty"`
	AccountID       string `json:"account_id,omitempty"`
	SnapshotVersion string `json:"snapshot_version,omitempty"`
}

// PortfolioSnapshotResponse returns the stored portfolio snapshot payload.
type PortfolioSnapshotResponse struct {
	Snapshot  *PositionSnapshot `json:"snapshot,omitempty"`
	Exposure  *ExposureSnapshot `json:"exposure,omitempty"`
	Timestamp int64             `json:"timestamp,omitempty"`
}

// PortfolioReplayRequest asks the state store to replay a sequence range.
type PortfolioReplayRequest struct {
	TenantID        string            `json:"tenant_id,omitempty"`
	AccountID       string            `json:"account_id,omitempty"`
	FromSequence    int64             `json:"from_sequence,omitempty"`
	ToSequence      int64             `json:"to_sequence,omitempty"`
	SnapshotVersion string            `json:"snapshot_version,omitempty"`
	Reason          string            `json:"reason,omitempty"`
	Metadata        map[string]string `json:"metadata,omitempty"`
}

// PortfolioReplayResponse reports replay progress.
type PortfolioReplayResponse struct {
	Replayed       int64  `json:"replayed,omitempty"`
	Recovered      bool   `json:"recovered,omitempty"`
	SnapshotVersion string `json:"snapshot_version,omitempty"`
	Timestamp      int64  `json:"timestamp,omitempty"`
}

// PortfolioReconciliationRequest requests a drift check and repair cycle.
type PortfolioReconciliationRequest struct {
	TenantID  string            `json:"tenant_id,omitempty"`
	AccountID string            `json:"account_id,omitempty"`
	Strict    bool              `json:"strict,omitempty"`
	Force     bool              `json:"force,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

// PortfolioReconciliationResponse reports reconciliation results.
type PortfolioReconciliationResponse struct {
	Reconciled   bool              `json:"reconciled,omitempty"`
	DriftDetected bool              `json:"drift_detected,omitempty"`
	DriftCount   int64             `json:"drift_count,omitempty"`
	Details      map[string]string `json:"details,omitempty"`
	Timestamp    int64             `json:"timestamp,omitempty"`
}

// PortfolioRiskSubscriptionRequest starts a streaming risk feed.
type PortfolioRiskSubscriptionRequest struct {
	TenantID  string            `json:"tenant_id,omitempty"`
	AccountID string            `json:"account_id,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

// PositionEventTTL returns the recommended TTL for event hot caches.
func PositionEventTTL() time.Duration {
	return 30 * time.Minute
}
