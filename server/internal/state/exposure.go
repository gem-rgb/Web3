package state

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	pb "github.com/example/rms/shared/proto"
)

// ApplyExposureFromPosition rebuilds the account exposure view for the affected account.
func (r *Repository) ApplyExposureFromPosition(ctx context.Context, position *pb.Position) (*pb.AggregatedExposure, error) {
	if position == nil {
		return nil, fmt.Errorf("position is required")
	}
	tenantID := normalizeTenant(position.TenantId)
	accountID := strings.TrimSpace(position.AccountId)
	if accountID == "" {
		return nil, fmt.Errorf("account is required")
	}
	lease, err := r.acquire(ctx, exposureLockKey(tenantID, accountID, position.Symbol))
	if err != nil {
		return nil, err
	}
	if lease != nil {
		defer func() {
			_ = lease.Release(context.Background())
		}()
	}
	exposures, err := r.rebuildAccountExposures(ctx, tenantID, accountID)
	if err != nil {
		return nil, err
	}
	for _, exposure := range exposures {
		if exposure != nil && normalizeSymbol(exposure.Symbol) == normalizeSymbol(position.Symbol) {
			return cloneExposure(exposure), nil
		}
	}
	return nil, nil
}

// GetAccountExposure returns the current exposure rows for the account.
func (r *Repository) GetAccountExposure(ctx context.Context, tenantID, accountID string) ([]*pb.AggregatedExposure, error) {
	tenantID = normalizeTenant(tenantID)
	accountID = strings.TrimSpace(accountID)
	if accountID == "" {
		return nil, nil
	}
	if exposures, ok, err := r.loadAccountExposuresCache(ctx, tenantID, accountID); err == nil && ok {
		return exposures, nil
	}
	rows, err := r.db.QueryContext(ctx, `
SELECT tenant_id, account_id, symbol, sector, net_quantity, gross_exposure, net_exposure, long_exposure, short_exposure,
       market_value, unrealized_pl, realized_pl, leverage, concentration_pct, position_count, version, sequence,
       snapshot_version, updated_at, metadata
  FROM exposure_snapshots
 WHERE tenant_id = $1 AND account_id = $2
 ORDER BY symbol ASC`, tenantID, accountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	exposures := make([]*pb.AggregatedExposure, 0, 8)
	for rows.Next() {
		exposure, err := scanExposure(rows)
		if err != nil {
			return nil, err
		}
		exposures = append(exposures, exposure)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(exposures) > 0 {
		_ = r.storeAccountExposuresCache(ctx, tenantID, accountID, exposures)
	}
	return exposures, nil
}

// GetSymbolExposure returns the current exposure rows for a symbol across the tenant.
func (r *Repository) GetSymbolExposure(ctx context.Context, tenantID, symbol string) ([]*pb.AggregatedExposure, error) {
	tenantID = normalizeTenant(tenantID)
	symbol = normalizeSymbol(symbol)
	if symbol == "" {
		return nil, nil
	}
	rows, err := r.db.QueryContext(ctx, `
SELECT tenant_id, account_id, symbol, sector, net_quantity, gross_exposure, net_exposure, long_exposure, short_exposure,
       market_value, unrealized_pl, realized_pl, leverage, concentration_pct, position_count, version, sequence,
       snapshot_version, updated_at, metadata
  FROM exposure_snapshots
 WHERE tenant_id = $1 AND symbol = $2
 ORDER BY account_id ASC`, tenantID, symbol)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	exposures := make([]*pb.AggregatedExposure, 0, 8)
	for rows.Next() {
		exposure, err := scanExposure(rows)
		if err != nil {
			return nil, err
		}
		exposures = append(exposures, exposure)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return exposures, nil
}

// SaveExposureSnapshot persists a single materialized exposure row.
func (r *Repository) SaveExposureSnapshot(ctx context.Context, exposure *pb.AggregatedExposure, snapshotVersion string) (*pb.ExposureSnapshot, error) {
	if exposure == nil {
		return nil, fmt.Errorf("exposure is required")
	}
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	snapshot, err := r.saveExposureSnapshotTx(ctx, tx, exposure, snapshotVersion)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	_ = r.storeExposureCache(ctx, exposure)
	return snapshot, nil
}

// LoadExposureSnapshot returns the latest or named exposure snapshot.
func (r *Repository) LoadExposureSnapshot(ctx context.Context, tenantID, accountID, symbol, snapshotVersion string) (*pb.ExposureSnapshot, error) {
	tenantID = normalizeTenant(tenantID)
	accountID = strings.TrimSpace(accountID)
	symbol = normalizeSymbol(symbol)
	if accountID == "" {
		return nil, nil
	}
	query := `
SELECT exposure_id, tenant_id, account_id, symbol, sector, net_quantity, gross_exposure, net_exposure, long_exposure,
       short_exposure, market_value, unrealized_pl, realized_pl, leverage, concentration_pct, position_count, version,
       sequence, snapshot_version, updated_at, metadata
  FROM exposure_snapshots
 WHERE tenant_id = $1 AND account_id = $2`
	args := []any{tenantID, accountID}
	if symbol != "" {
		query += " AND symbol = $3"
		args = append(args, symbol)
	}
	if snapshotVersion != "" {
		query += fmt.Sprintf(" AND snapshot_version = $%d", len(args)+1)
		args = append(args, snapshotVersion)
	} else {
		query += " ORDER BY updated_at DESC LIMIT 1"
	}
	row := r.db.QueryRowContext(ctx, query, args...)
	snapshot, err := scanExposureSnapshot(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return snapshot, err
}

// ReplayExposureState rebuilds the account exposure rows from positions.
func (r *Repository) ReplayExposureState(ctx context.Context, tenantID, accountID, symbol string, snapshotVersion string) ([]*pb.AggregatedExposure, int64, error) {
	tenantID = normalizeTenant(tenantID)
	accountID = strings.TrimSpace(accountID)
	symbol = normalizeSymbol(symbol)
	if accountID == "" {
		return nil, 0, fmt.Errorf("account is required")
	}
	positions, err := r.ListPositions(ctx, tenantID, accountID)
	if err != nil {
		return nil, 0, err
	}
	if len(positions) == 0 {
		return nil, 0, nil
	}
	var replayed int64
	exposures := make([]*pb.AggregatedExposure, 0, len(positions))
	totalGross := totalGrossExposure(positions)
	for _, position := range positions {
		if position == nil {
			continue
		}
		if symbol != "" && !strings.EqualFold(normalizeSymbol(position.Symbol), symbol) {
			continue
		}
		exposure := exposureFromPosition(position, totalGross, len(positions))
		if _, err := r.saveExposureSnapshotTx(ctx, nil, exposure, firstString(snapshotVersion, "replay")); err != nil {
			return nil, 0, err
		}
		if err := r.saveExposureReplayState(ctx, tenantID, accountID, normalizeSymbol(position.Symbol), firstString(snapshotVersion, "replay"), exposure.Sequence, true, map[string]string{
			"position_count": fmt.Sprintf("%d", exposure.PositionCount),
		}); err != nil {
			return nil, 0, err
		}
		exposures = append(exposures, exposure)
		replayed++
	}
	_ = r.storeAccountExposuresCache(ctx, tenantID, accountID, exposures)
	return exposures, replayed, nil
}

// ReconcileExposureState compares materialized exposures with recomputed values.
func (r *Repository) ReconcileExposureState(ctx context.Context, tenantID, accountID, symbol string, force bool) (bool, bool, int64, error) {
	tenantID = normalizeTenant(tenantID)
	accountID = strings.TrimSpace(accountID)
	symbol = normalizeSymbol(symbol)
	exposures, err := r.GetAccountExposure(ctx, tenantID, accountID)
	if err != nil {
		return false, false, 0, err
	}
	if len(exposures) == 0 {
		return true, false, 0, nil
	}
	positions, err := r.ListPositions(ctx, tenantID, accountID)
	if err != nil {
		return false, false, 0, err
	}
	recomputed := make(map[string]*pb.AggregatedExposure, len(exposures))
	totalGross := totalGrossExposure(positions)
	for _, position := range positions {
		if position == nil {
			continue
		}
		if symbol != "" && !strings.EqualFold(normalizeSymbol(position.Symbol), symbol) {
			continue
		}
		recomputed[normalizeSymbol(position.Symbol)] = exposureFromPosition(position, totalGross, len(positions))
	}
	var driftCount int64
	for _, current := range exposures {
		if current == nil {
			continue
		}
		next := recomputed[normalizeSymbol(current.Symbol)]
		if !exposuresEqual(current, next) {
			driftCount++
		}
	}
	if driftCount > 0 || force {
		_, _, err := r.ReplayExposureState(ctx, tenantID, accountID, symbol, "reconciled")
		if err != nil {
			return false, driftCount > 0, driftCount, err
		}
		if len(exposures) > 0 {
			for _, exposure := range exposures {
				if exposure == nil {
					continue
				}
				if err := r.saveExposureReplayState(ctx, tenantID, accountID, normalizeSymbol(exposure.Symbol), "reconciled", exposure.Sequence, true, map[string]string{
					"drift_count": fmt.Sprintf("%d", driftCount),
					"force":       fmt.Sprintf("%t", force),
				}); err != nil {
					return false, driftCount > 0, driftCount, err
				}
			}
		}
		return true, driftCount > 0, driftCount, nil
	}
	return true, false, driftCount, nil
}

func (r *Repository) rebuildAccountExposures(ctx context.Context, tenantID, accountID string) ([]*pb.AggregatedExposure, error) {
	positions, err := r.ListPositions(ctx, tenantID, accountID)
	if err != nil {
		return nil, err
	}
	if len(positions) == 0 {
		_ = r.storeAccountExposuresCache(ctx, tenantID, accountID, nil)
		return nil, nil
	}
	totalGross := totalGrossExposure(positions)
	exposures := make([]*pb.AggregatedExposure, 0, len(positions))
	for _, position := range positions {
		if position == nil {
			continue
		}
		exposure := exposureFromPosition(position, totalGross, len(positions))
		tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
		if err != nil {
			return nil, err
		}
		if _, err := r.saveExposureSnapshotTx(ctx, tx, exposure, "live"); err != nil {
			_ = tx.Rollback()
			return nil, err
		}
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		exposures = append(exposures, exposure)
	}
	sort.Slice(exposures, func(i, j int) bool {
		return exposures[i].Symbol < exposures[j].Symbol
	})
	_ = r.storeAccountExposuresCache(ctx, tenantID, accountID, exposures)
	return exposures, nil
}

func (r *Repository) saveExposureSnapshotTx(ctx context.Context, tx *sql.Tx, exposure *pb.AggregatedExposure, snapshotVersion string) (*pb.ExposureSnapshot, error) {
	if exposure == nil {
		return nil, fmt.Errorf("exposure is required")
	}
	metadata, err := json.Marshal(exposure.Metadata)
	if err != nil {
		return nil, err
	}
	snapshot := &pb.ExposureSnapshot{
		SnapshotID:     stableSnapshotID("exposure", exposure.TenantId, exposure.AccountId, time.Now().UTC().UnixMilli()),
		TenantID:       exposure.TenantId,
		AccountID:      exposure.AccountId,
		Symbol:         exposure.Symbol,
		Sector:         exposure.Sector,
		Version:        firstNonZero(exposure.Version, 0),
		EventOffset:    exposure.Sequence,
		SnapshotVersion: firstString(snapshotVersion, "live"),
		Exposure:       cloneExposure(exposure),
		Checksum:       checksumExposure(exposure),
		CreatedAt:      time.Now().UTC().UnixMilli(),
		Metadata:       map[string]string{"snapshot_version": firstString(snapshotVersion, "live")},
	}
	if tx != nil {
		_, err = tx.ExecContext(ctx, `
INSERT INTO exposure_snapshots (
    exposure_id, tenant_id, account_id, symbol, sector, net_quantity, gross_exposure, net_exposure, long_exposure,
    short_exposure, market_value, unrealized_pl, realized_pl, leverage, concentration_pct, position_count, version,
    sequence, snapshot_version, updated_at, metadata
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,to_timestamp($20::double precision / 1000.0),$21)
ON CONFLICT (tenant_id, account_id, symbol) DO UPDATE SET
    sector = EXCLUDED.sector,
    net_quantity = EXCLUDED.net_quantity,
    gross_exposure = EXCLUDED.gross_exposure,
    net_exposure = EXCLUDED.net_exposure,
    long_exposure = EXCLUDED.long_exposure,
    short_exposure = EXCLUDED.short_exposure,
    market_value = EXCLUDED.market_value,
    unrealized_pl = EXCLUDED.unrealized_pl,
    realized_pl = EXCLUDED.realized_pl,
    leverage = EXCLUDED.leverage,
    concentration_pct = EXCLUDED.concentration_pct,
    position_count = EXCLUDED.position_count,
    version = EXCLUDED.version,
    sequence = EXCLUDED.sequence,
    snapshot_version = EXCLUDED.snapshot_version,
    updated_at = EXCLUDED.updated_at,
    metadata = EXCLUDED.metadata`,
			snapshot.SnapshotID, exposure.TenantId, exposure.AccountId, normalizeSymbol(exposure.Symbol), exposure.Sector, exposure.NetQuantity,
			exposure.GrossExposure, exposure.NetExposure, exposure.LongExposure, exposure.ShortExposure, exposure.MarketValue, exposure.UnrealizedPL,
			exposure.RealizedPL, exposure.Leverage, exposure.ConcentrationPct, exposure.PositionCount, exposure.Version, exposure.Sequence,
			firstString(snapshotVersion, "live"), snapshot.CreatedAt, metadata,
		)
	} else {
		_, err = r.db.ExecContext(ctx, `
INSERT INTO exposure_snapshots (
    exposure_id, tenant_id, account_id, symbol, sector, net_quantity, gross_exposure, net_exposure, long_exposure,
    short_exposure, market_value, unrealized_pl, realized_pl, leverage, concentration_pct, position_count, version,
    sequence, snapshot_version, updated_at, metadata
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,to_timestamp($20::double precision / 1000.0),$21)
ON CONFLICT (tenant_id, account_id, symbol) DO UPDATE SET
    sector = EXCLUDED.sector,
    net_quantity = EXCLUDED.net_quantity,
    gross_exposure = EXCLUDED.gross_exposure,
    net_exposure = EXCLUDED.net_exposure,
    long_exposure = EXCLUDED.long_exposure,
    short_exposure = EXCLUDED.short_exposure,
    market_value = EXCLUDED.market_value,
    unrealized_pl = EXCLUDED.unrealized_pl,
    realized_pl = EXCLUDED.realized_pl,
    leverage = EXCLUDED.leverage,
    concentration_pct = EXCLUDED.concentration_pct,
    position_count = EXCLUDED.position_count,
    version = EXCLUDED.version,
    sequence = EXCLUDED.sequence,
    snapshot_version = EXCLUDED.snapshot_version,
    updated_at = EXCLUDED.updated_at,
    metadata = EXCLUDED.metadata`,
			snapshot.SnapshotID, exposure.TenantId, exposure.AccountId, normalizeSymbol(exposure.Symbol), exposure.Sector, exposure.NetQuantity,
			exposure.GrossExposure, exposure.NetExposure, exposure.LongExposure, exposure.ShortExposure, exposure.MarketValue, exposure.UnrealizedPL,
			exposure.RealizedPL, exposure.Leverage, exposure.ConcentrationPct, exposure.PositionCount, exposure.Version, exposure.Sequence,
			firstString(snapshotVersion, "live"), snapshot.CreatedAt, metadata,
		)
	}
	if err != nil {
		return nil, err
	}
	_ = r.storeExposureCache(ctx, exposure)
	_ = r.cache.StoreExposureSnapshot(ctx, snapshot, 24*time.Hour)
	return snapshot, nil
}

func (r *Repository) storeExposureCache(ctx context.Context, exposure *pb.AggregatedExposure) error {
	if r.cache == nil || exposure == nil {
		return nil
	}
	return r.cache.StoreExposure(ctx, exposure.TenantId, exposure, 12*time.Hour)
}

func (r *Repository) loadAccountExposuresCache(ctx context.Context, tenantID, accountID string) ([]*pb.AggregatedExposure, bool, error) {
	if r.cache == nil {
		return nil, false, nil
	}
	return r.cache.LoadExposures(ctx, tenantID, accountID)
}

func (r *Repository) storeAccountExposuresCache(ctx context.Context, tenantID, accountID string, exposures []*pb.AggregatedExposure) error {
	if r.cache == nil {
		return nil
	}
	return r.cache.StoreExposures(ctx, tenantID, accountID, exposures, 12*time.Hour)
}

func (r *Repository) loadExposureCache(ctx context.Context, tenantID, accountID, symbol string) (*pb.AggregatedExposure, bool, error) {
	if r.cache == nil {
		return nil, false, nil
	}
	return r.cache.LoadExposure(ctx, tenantID, accountID, symbol)
}

func scanExposure(scanner interface {
	Scan(dest ...any) error
}) (*pb.AggregatedExposure, error) {
	var (
		sector, snapshotVersion string
		updatedAt               time.Time
		metadataRaw             []byte
		exposure                pb.AggregatedExposure
	)
	err := scanner.Scan(
		&exposure.TenantId,
		&exposure.AccountId,
		&exposure.Symbol,
		&sector,
		&exposure.NetQuantity,
		&exposure.GrossExposure,
		&exposure.NetExposure,
		&exposure.LongExposure,
		&exposure.ShortExposure,
		&exposure.MarketValue,
		&exposure.UnrealizedPL,
		&exposure.RealizedPL,
		&exposure.Leverage,
		&exposure.ConcentrationPct,
		&exposure.PositionCount,
		&exposure.Version,
		&exposure.Sequence,
		&snapshotVersion,
		&updatedAt,
		&metadataRaw,
	)
	if err != nil {
		return nil, err
	}
	exposure.Sector = sector
	exposure.SnapshotVersion = snapshotVersion
	exposure.UpdatedAt = updatedAt.UTC().UnixMilli()
	exposure.Timestamp = exposure.UpdatedAt
	if len(metadataRaw) > 0 {
		var metadata map[string]string
		if err := json.Unmarshal(metadataRaw, &metadata); err == nil {
			exposure.Metadata = metadata
		}
	}
	return &exposure, nil
}

func scanExposureSnapshot(scanner interface {
	Scan(dest ...any) error
}) (*pb.ExposureSnapshot, error) {
	var (
		sector, snapshotVersion string
		updatedAt               time.Time
		metadataRaw             []byte
		snapshot                pb.ExposureSnapshot
	)
	snapshot.Exposure = &pb.AggregatedExposure{}
	err := scanner.Scan(
		&snapshot.SnapshotID,
		&snapshot.TenantID,
		&snapshot.AccountID,
		&snapshot.Symbol,
		&sector,
		&snapshot.Exposure.NetQuantity,
		&snapshot.Exposure.GrossExposure,
		&snapshot.Exposure.NetExposure,
		&snapshot.Exposure.LongExposure,
		&snapshot.Exposure.ShortExposure,
		&snapshot.Exposure.MarketValue,
		&snapshot.Exposure.UnrealizedPL,
		&snapshot.Exposure.RealizedPL,
		&snapshot.Exposure.Leverage,
		&snapshot.Exposure.ConcentrationPct,
		&snapshot.Exposure.PositionCount,
		&snapshot.Version,
		&snapshot.EventOffset,
		&snapshotVersion,
		&updatedAt,
		&metadataRaw,
	)
	if err != nil {
		return nil, err
	}
	snapshot.Sector = sector
	snapshot.SnapshotVersion = snapshotVersion
	snapshot.Exposure.TenantId = snapshot.TenantID
	snapshot.Exposure.AccountId = snapshot.AccountID
	snapshot.Exposure.Symbol = snapshot.Symbol
	snapshot.Exposure.Sector = sector
	snapshot.Exposure.SnapshotVersion = snapshotVersion
	snapshot.Exposure.UpdatedAt = updatedAt.UTC().UnixMilli()
	snapshot.Exposure.Timestamp = snapshot.Exposure.UpdatedAt
	snapshot.Exposure.Version = snapshot.Version
	snapshot.Exposure.Sequence = snapshot.EventOffset
	if len(metadataRaw) > 0 {
		var metadata map[string]string
		if err := json.Unmarshal(metadataRaw, &metadata); err == nil {
			snapshot.Metadata = metadata
		}
	}
	return &snapshot, nil
}

func exposureFromPosition(position *pb.Position, totalGross float64, positionCount int) *pb.AggregatedExposure {
	if position == nil {
		return nil
	}
	gross := math.Abs(position.MarketValue)
	net := position.MarketValue
	concentration := 0.0
	if totalGross > 0 {
		concentration = gross / totalGross * 100
	}
	return &pb.AggregatedExposure{
		TenantId:         normalizeTenant(position.TenantId),
		AccountId:        strings.TrimSpace(position.AccountId),
		Symbol:           normalizeSymbol(position.Symbol),
		Sector:           position.Sector,
		NetQuantity:      position.Quantity,
		GrossExposure:    gross,
		NetExposure:      net,
		LongExposure:     math.Max(net, 0),
		ShortExposure:    math.Abs(math.Min(net, 0)),
		MarketValue:      position.MarketValue,
		UnrealizedPL:     position.UnrealizedPL,
		RealizedPL:       position.RealizedPL,
		Leverage:         position.Leverage,
		ConcentrationPct: concentration,
		PositionCount:    int32(positionCount),
		Version:          position.Version,
		Sequence:         position.Sequence,
		SnapshotVersion:  position.SnapshotVersion,
		UpdatedAt:        position.UpdatedAt,
		Timestamp:        position.Timestamp,
		Metadata:         cloneStringMap(position.Metadata),
	}
}

func totalGrossExposure(positions []*pb.Position) float64 {
	var total float64
	for _, position := range positions {
		if position == nil {
			continue
		}
		total += math.Abs(position.MarketValue)
	}
	if total <= 0 {
		total = 1
	}
	return total
}

func checksumExposure(exposure *pb.AggregatedExposure) string {
	if exposure == nil {
		return ""
	}
	payload, _ := json.Marshal(exposure)
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func exposuresEqual(left, right *pb.AggregatedExposure) bool {
	if left == nil || right == nil {
		return left == right
	}
	return left.TenantId == right.TenantId &&
		left.AccountId == right.AccountId &&
		normalizeSymbol(left.Symbol) == normalizeSymbol(right.Symbol) &&
		left.Sector == right.Sector &&
		left.NetQuantity == right.NetQuantity &&
		almostEqual(left.GrossExposure, right.GrossExposure) &&
		almostEqual(left.NetExposure, right.NetExposure) &&
		almostEqual(left.LongExposure, right.LongExposure) &&
		almostEqual(left.ShortExposure, right.ShortExposure) &&
		almostEqual(left.MarketValue, right.MarketValue) &&
		almostEqual(left.UnrealizedPL, right.UnrealizedPL) &&
		almostEqual(left.RealizedPL, right.RealizedPL) &&
		almostEqual(left.Leverage, right.Leverage) &&
		almostEqual(left.ConcentrationPct, right.ConcentrationPct) &&
		left.PositionCount == right.PositionCount &&
		left.Version == right.Version &&
		left.SnapshotVersion == right.SnapshotVersion
}

func exposureLockKey(tenantID, accountID, symbol string) string {
	return strings.Join([]string{"exposure", normalizeTenant(tenantID), strings.TrimSpace(accountID), normalizeSymbol(symbol)}, ":")
}

func cloneExposure(exposure *pb.AggregatedExposure) *pb.AggregatedExposure {
	if exposure == nil {
		return nil
	}
	copy := *exposure
	copy.Metadata = cloneStringMap(exposure.Metadata)
	return &copy
}

func cloneStringMap(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	result := make(map[string]string, len(values))
	for k, v := range values {
		result[k] = v
	}
	return result
}

func (r *Repository) saveExposureReplayState(ctx context.Context, tenantID, accountID, symbol, snapshotVersion string, lastSequence int64, recovered bool, details map[string]string) error {
	if r.db == nil {
		return nil
	}
	payload, err := json.Marshal(details)
	if err != nil {
		return err
	}
	_, err = r.db.ExecContext(ctx, `
INSERT INTO exposure_replay_state (
    tenant_id, account_id, symbol, snapshot_version, last_sequence, last_replayed_at, recovered, details, updated_at
) VALUES ($1,$2,$3,$4,$5,now(),$6,$7,now())
ON CONFLICT (tenant_id, account_id, symbol) DO UPDATE SET
    snapshot_version = EXCLUDED.snapshot_version,
    last_sequence = EXCLUDED.last_sequence,
    last_replayed_at = EXCLUDED.last_replayed_at,
    recovered = EXCLUDED.recovered,
    details = EXCLUDED.details,
    updated_at = EXCLUDED.updated_at`,
		normalizeTenant(tenantID), strings.TrimSpace(accountID), normalizeSymbol(symbol), firstString(snapshotVersion, "live"), lastSequence, recovered, payload,
	)
	return err
}
