package state

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"

	pb "github.com/example/rms/shared/proto"
)

// CalculatePortfolioRisk computes the aggregate risk summary from the current state.
func (r *Repository) CalculatePortfolioRisk(ctx context.Context, req *pb.PortfolioRiskRequest) (*pb.PortfolioRiskSummary, error) {
	if req == nil {
		return nil, fmt.Errorf("portfolio risk request is required")
	}
	tenantID := normalizeTenant(req.TenantID)
	accountID := strings.TrimSpace(req.AccountID)
	positions, err := r.ListPositions(ctx, tenantID, accountID)
	if err != nil {
		return nil, err
	}
	exposures, err := r.GetAccountExposure(ctx, tenantID, accountID)
	if err != nil {
		return nil, err
	}
	if len(exposures) == 0 && len(positions) > 0 {
		exposures, err = r.rebuildAccountExposures(ctx, tenantID, accountID)
		if err != nil {
			return nil, err
		}
	}

	summary := &pb.PortfolioRiskSummary{
		TenantID:            tenantID,
		AccountID:           accountID,
		SymbolExposure:      map[string]float64{},
		SectorExposure:      map[string]float64{},
		CrossAccountExposure: map[string]float64{},
		SnapshotVersion:     firstString(req.SnapshotVersion, "live"),
		CalculatedAt:        time.Now().UTC().UnixMilli(),
		Metadata:            cloneStringMap(req.Metadata),
	}

	var gross float64
	var largest float64
	sectorTotals := map[string]float64{}
	symbolFilter := make(map[string]struct{}, len(req.Symbols))
	for _, symbol := range req.Symbols {
		if normalized := normalizeSymbol(symbol); normalized != "" {
			symbolFilter[normalized] = struct{}{}
		}
	}
	for _, position := range positions {
		if position == nil {
			continue
		}
		symbol := normalizeSymbol(position.Symbol)
		if len(symbolFilter) > 0 {
			if _, ok := symbolFilter[symbol]; !ok {
				continue
			}
		}
		notional := math.Abs(position.MarketValue)
		gross += notional
		summary.NetExposure += position.MarketValue
		if position.MarketValue >= 0 {
			summary.LongExposure += position.MarketValue
		} else {
			summary.ShortExposure += math.Abs(position.MarketValue)
		}
		summary.UnrealizedPL += position.UnrealizedPL
		summary.RealizedPL += position.RealizedPL
		summary.PositionCount++
		summary.SymbolExposure[symbol] += notional
		if req.IncludeSector && position.Sector != "" {
			sectorTotals[strings.TrimSpace(position.Sector)] += notional
		}
		if notional > largest {
			largest = notional
		}
	}

	if req.IncludeSector {
		for sector, value := range sectorTotals {
			summary.SectorExposure[sector] = value
		}
	}
	if req.IncludeCrossAccount {
		byAccount, err := r.crossAccountExposure(ctx, tenantID)
		if err != nil {
			return nil, err
		}
		summary.CrossAccountExposure = byAccount
	}

	summary.GrossExposure = gross
	summary.MaxPositionExposure = largest
	if gross > 0 {
		summary.ConcentrationPct = largest / gross * 100
	}
	equityBase := math.Abs(summary.NetExposure) + math.Abs(summary.UnrealizedPL) + math.Abs(summary.RealizedPL)
	if equityBase <= 0 {
		equityBase = 1
	}
	summary.Leverage = gross / equityBase
	summary.RiskState = classifyRisk(summary)
	if len(symbolFilter) == 0 {
		summary.DriftDetected = !portfolioSnapshotAligned(summary, exposures)
	}
	_ = r.PersistPortfolioRisk(ctx, summary)
	return summary, nil
}

// BuildPortfolioSnapshot returns the latest materialized position and exposure snapshot pair.
func (r *Repository) BuildPortfolioSnapshot(ctx context.Context, tenantID, accountID, snapshotVersion string) (*pb.PositionSnapshot, *pb.ExposureSnapshot, error) {
	tenantID = normalizeTenant(tenantID)
	accountID = strings.TrimSpace(accountID)
	if accountID == "" {
		return nil, nil, fmt.Errorf("account is required")
	}
	posSnapshot, err := r.LoadPositionSnapshot(ctx, tenantID, accountID, snapshotVersion)
	if err != nil && err != sql.ErrNoRows {
		return nil, nil, err
	}
	if posSnapshot == nil {
		positions, err := r.ListPositions(ctx, tenantID, accountID)
		if err != nil {
			return nil, nil, err
		}
		posSnapshot = &pb.PositionSnapshot{
			SnapshotID:     stableSnapshotID("position", tenantID, accountID, time.Now().UTC().UnixMilli()),
			TenantID:       tenantID,
			AccountID:      accountID,
			Version:        int64(len(positions)),
			EventOffset:    latestSequence(positions),
			Positions:      positions,
			Checksum:       checksumPositions(sliceToMap(positions)),
			CreatedAt:      time.Now().UTC().UnixMilli(),
			Metadata:       map[string]string{"snapshot_version": firstString(snapshotVersion, "live")},
		}
	}
	exposures, err := r.GetAccountExposure(ctx, tenantID, accountID)
	if err != nil && err != sql.ErrNoRows {
		return nil, nil, err
	}
	if len(exposures) == 0 && len(posSnapshot.Positions) > 0 {
		exposures, err = r.rebuildAccountExposures(ctx, tenantID, accountID)
		if err != nil {
			return nil, nil, err
		}
	}
	expSnapshot := buildPortfolioExposureSnapshot(tenantID, accountID, exposures, snapshotVersion)
	return posSnapshot, expSnapshot, nil
}

// ReplayPortfolioState rebuilds the underlying position and exposure snapshots.
func (r *Repository) ReplayPortfolioState(ctx context.Context, tenantID, accountID string, fromSequence, toSequence int64, snapshotVersion string) (*pb.PortfolioSnapshotResponse, int64, error) {
	tenantID = normalizeTenant(tenantID)
	accountID = strings.TrimSpace(accountID)
	posSnapshot, replayedPositions, err := r.ReplayPositionState(ctx, tenantID, accountID, fromSequence, toSequence, snapshotVersion)
	if err != nil {
		return nil, 0, err
	}
	exposures, replayedExposures, err := r.ReplayExposureState(ctx, tenantID, accountID, "", snapshotVersion)
	if err != nil {
		return nil, 0, err
	}
	response := &pb.PortfolioSnapshotResponse{
		Snapshot:  posSnapshot,
		Exposure:  buildPortfolioExposureSnapshot(tenantID, accountID, exposures, snapshotVersion),
		Timestamp: time.Now().UTC().UnixMilli(),
	}
	return response, replayedPositions + replayedExposures, nil
}

// ReconcilePortfolioState performs a drift check across positions and exposures.
func (r *Repository) ReconcilePortfolioState(ctx context.Context, tenantID, accountID string, strict, force bool) (bool, bool, int64, error) {
	tenantID = normalizeTenant(tenantID)
	accountID = strings.TrimSpace(accountID)
	summary, err := r.CalculatePortfolioRisk(ctx, &pb.PortfolioRiskRequest{
		TenantID:            tenantID,
		AccountID:           accountID,
		IncludeCrossAccount: true,
		IncludeSector:      true,
	})
	if err != nil {
		return false, false, 0, err
	}
	cached, ok, err := r.loadPortfolioRiskCache(ctx, tenantID, accountID)
	if err != nil {
		return false, false, 0, err
	}
	driftDetected := !ok || !portfolioSummaryEqual(summary, cached)
	var driftCount int64
	if driftDetected {
		driftCount = 1
		refreshed, err := r.CalculatePortfolioRisk(ctx, &pb.PortfolioRiskRequest{
			TenantID:            tenantID,
			AccountID:           accountID,
			IncludeCrossAccount: true,
			IncludeSector:      true,
		}); err != nil {
			return false, true, driftCount, err
		} else {
			summary = refreshed
		}
		_ = r.storePortfolioRiskCache(ctx, summary)
		if _, _, err := r.BuildPortfolioSnapshot(ctx, tenantID, accountID, "reconciled"); err != nil {
			return false, true, driftCount, err
		}
		return true, true, driftCount, nil
	}
	if strict && !ok {
		return false, false, driftCount, nil
	}
	if force {
		_ = r.storePortfolioRiskCache(ctx, summary)
	}
	return true, false, driftCount, nil
}

func (r *Repository) crossAccountExposure(ctx context.Context, tenantID string) (map[string]float64, error) {
	rows, err := r.db.QueryContext(ctx, `
SELECT account_id, SUM(gross_exposure) AS gross_exposure
  FROM exposure_snapshots
 WHERE tenant_id = $1
 GROUP BY account_id
 ORDER BY account_id`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := map[string]float64{}
	for rows.Next() {
		var accountID string
		var gross sql.NullFloat64
		if err := rows.Scan(&accountID, &gross); err != nil {
			return nil, err
		}
		if gross.Valid {
			result[accountID] = gross.Float64
		}
	}
	return result, rows.Err()
}

func classifyRisk(summary *pb.PortfolioRiskSummary) string {
	if summary == nil {
		return "UNKNOWN"
	}
	switch {
	case summary.ConcentrationPct >= 45 || summary.Leverage >= 6:
		return "CRITICAL"
	case summary.ConcentrationPct >= 25 || summary.Leverage >= 4:
		return "HIGH"
	case summary.ConcentrationPct >= 12 || summary.Leverage >= 2:
		return "MEDIUM"
	default:
		return "NORMAL"
	}
}

func portfolioSnapshotAligned(summary *pb.PortfolioRiskSummary, exposures []*pb.AggregatedExposure) bool {
	if summary == nil {
		return true
	}
	if len(exposures) == 0 {
		return summary.GrossExposure == 0
	}
	var gross float64
	for _, exposure := range exposures {
		if exposure == nil {
			continue
		}
		gross += exposure.GrossExposure
	}
	return almostEqual(gross, summary.GrossExposure)
}

func portfolioSummaryEqual(left, right *pb.PortfolioRiskSummary) bool {
	if left == nil || right == nil {
		return left == right
	}
	return left.TenantID == right.TenantID &&
		left.AccountID == right.AccountID &&
		almostEqual(left.GrossExposure, right.GrossExposure) &&
		almostEqual(left.NetExposure, right.NetExposure) &&
		almostEqual(left.LongExposure, right.LongExposure) &&
		almostEqual(left.ShortExposure, right.ShortExposure) &&
		almostEqual(left.UnrealizedPL, right.UnrealizedPL) &&
		almostEqual(left.RealizedPL, right.RealizedPL) &&
		almostEqual(left.Leverage, right.Leverage) &&
		almostEqual(left.ConcentrationPct, right.ConcentrationPct) &&
		left.PositionCount == right.PositionCount &&
		left.RiskState == right.RiskState &&
		left.SnapshotVersion == right.SnapshotVersion &&
		floatMapsEqual(left.SymbolExposure, right.SymbolExposure) &&
		floatMapsEqual(left.SectorExposure, right.SectorExposure) &&
		floatMapsEqual(left.CrossAccountExposure, right.CrossAccountExposure)
}

func latestExposureSequence(exposures []*pb.AggregatedExposure) int64 {
	var latest int64
	for _, exposure := range exposures {
		if exposure != nil && exposure.Sequence > latest {
			latest = exposure.Sequence
		}
	}
	return latest
}

func (r *Repository) storePortfolioRiskCache(ctx context.Context, summary *pb.PortfolioRiskSummary) error {
	if r.cache == nil || summary == nil {
		return nil
	}
	return r.cache.StorePortfolioRisk(ctx, summary, 30*time.Minute)
}

func (r *Repository) loadPortfolioRiskCache(ctx context.Context, tenantID, accountID string) (*pb.PortfolioRiskSummary, bool, error) {
	if r.cache == nil {
		return nil, false, nil
	}
	return r.cache.LoadPortfolioRisk(ctx, tenantID, accountID)
}

// PersistPortfolioRisk stores the latest portfolio risk summary in both PostgreSQL and Redis.
func (r *Repository) PersistPortfolioRisk(ctx context.Context, summary *pb.PortfolioRiskSummary) error {
	if summary == nil {
		return nil
	}
	if err := r.storePortfolioRiskCache(ctx, summary); err != nil {
		return err
	}
	return persistPortfolioRisk(ctx, r.db, summary)
}

func persistPortfolioRisk(ctx context.Context, db *sql.DB, summary *pb.PortfolioRiskSummary) error {
	if db == nil || summary == nil {
		return nil
	}
	payload, err := json.Marshal(summary)
	if err != nil {
		return err
	}
	checksum := checksumPortfolio(summary)
	_, err = db.ExecContext(ctx, `
INSERT INTO exposure_views (
    tenant_id, account_id, symbol, sector, snapshot_version, summary, checksum, updated_at
) VALUES ($1,$2,$3,$4,$5,$6,$7,to_timestamp($8::double precision / 1000.0))
ON CONFLICT (tenant_id, account_id, symbol) DO UPDATE SET
    sector = EXCLUDED.sector,
    snapshot_version = EXCLUDED.snapshot_version,
    summary = EXCLUDED.summary,
    checksum = EXCLUDED.checksum,
    updated_at = EXCLUDED.updated_at`,
		summary.TenantID, summary.AccountID, "_portfolio", "portfolio", summary.SnapshotVersion, payload, checksum, summary.CalculatedAt,
	)
	return err
}

func checksumPortfolio(summary *pb.PortfolioRiskSummary) string {
	if summary == nil {
		return ""
	}
	payload, _ := json.Marshal(summary)
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func buildPortfolioExposureSnapshot(tenantID, accountID string, exposures []*pb.AggregatedExposure, snapshotVersion string) *pb.ExposureSnapshot {
	if len(exposures) == 0 {
		return nil
	}
	now := time.Now().UTC().UnixMilli()
	var (
		gross         float64
		net           float64
		longExposure  float64
		shortExposure float64
		marketValue   float64
		unrealizedPL  float64
		realizedPL    float64
		maxExposure   float64
		positionCount int32
		version       int64
		lastSequence  int64
	)
	for _, exposure := range exposures {
		if exposure == nil {
			continue
		}
		gross += exposure.GrossExposure
		net += exposure.NetExposure
		longExposure += exposure.LongExposure
		shortExposure += exposure.ShortExposure
		marketValue += exposure.MarketValue
		unrealizedPL += exposure.UnrealizedPL
		realizedPL += exposure.RealizedPL
		positionCount += exposure.PositionCount
		if exposure.GrossExposure > maxExposure {
			maxExposure = exposure.GrossExposure
		}
		if exposure.Version > version {
			version = exposure.Version
		}
		if exposure.Sequence > lastSequence {
			lastSequence = exposure.Sequence
		}
	}
	equityBase := math.Abs(net) + math.Abs(unrealizedPL) + math.Abs(realizedPL)
	if equityBase <= 0 {
		equityBase = 1
	}
	leverage := gross / equityBase
	concentration := 0.0
	if gross > 0 {
		concentration = maxExposure / gross * 100
	}
	exposure := &pb.AggregatedExposure{
		TenantId:         tenantID,
		AccountId:        accountID,
		Symbol:           "_PORTFOLIO",
		Sector:           "portfolio",
		NetQuantity:      0,
		GrossExposure:    gross,
		NetExposure:      net,
		LongExposure:     longExposure,
		ShortExposure:    shortExposure,
		MarketValue:      marketValue,
		UnrealizedPL:     unrealizedPL,
		RealizedPL:       realizedPL,
		Leverage:         leverage,
		ConcentrationPct: concentration,
		PositionCount:    positionCount,
		Version:          version,
		Sequence:         lastSequence,
		SnapshotVersion:  firstString(snapshotVersion, "live"),
		UpdatedAt:        now,
		Timestamp:        now,
		Metadata: map[string]string{
			"snapshot_version": firstString(snapshotVersion, "live"),
			"source":           "portfolio-summary",
		},
	}
	return &pb.ExposureSnapshot{
		SnapshotID:     stableSnapshotID("portfolio-exposure", tenantID, accountID, now),
		TenantID:       tenantID,
		AccountID:      accountID,
		Symbol:         exposure.Symbol,
		Sector:         exposure.Sector,
		Version:        exposure.Version,
		EventOffset:    exposure.Sequence,
		SnapshotVersion: exposure.SnapshotVersion,
		Exposure:       exposure,
		Checksum:       checksumExposure(exposure),
		CreatedAt:      now,
		Metadata: map[string]string{
			"snapshot_version": exposure.SnapshotVersion,
			"source":           "portfolio-summary",
		},
	}
}

func floatMapsEqual(left, right map[string]float64) bool {
	if len(left) == 0 && len(right) == 0 {
		return true
	}
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if !almostEqual(value, right[key]) {
			return false
		}
	}
	return true
}
