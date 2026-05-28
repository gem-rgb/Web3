package state

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/example/rms/server/internal/cache"
	"github.com/example/rms/server/internal/lock"
	pb "github.com/example/rms/shared/proto"
)

// Repository owns the durable and hot-state views for positions and exposures.
type Repository struct {
	db            *sql.DB
	cache         *cache.RiskCache
	locks         *lock.Manager
	logger        *slog.Logger
	snapshotEvery int64
}

// NewRepository wires the shared state repository.
func NewRepository(db *sql.DB, cache *cache.RiskCache, locks *lock.Manager, logger *slog.Logger, snapshotEvery int64) *Repository {
	if snapshotEvery <= 0 {
		snapshotEvery = 25
	}
	return &Repository{
		db:            db,
		cache:         cache,
		locks:         locks,
		logger:        logger,
		snapshotEvery: snapshotEvery,
	}
}

// ApplyPositionEvent appends a position event and materializes the current row.
func (r *Repository) ApplyPositionEvent(ctx context.Context, event *pb.PositionEvent) (*pb.Position, error) {
	if event == nil {
		return nil, fmt.Errorf("position event is required")
	}
	tenantID := normalizeTenant(event.TenantID)
	accountID := strings.TrimSpace(event.AccountID)
	symbol := normalizeSymbol(event.Symbol)
	if accountID == "" || symbol == "" {
		return nil, fmt.Errorf("account and symbol are required")
	}
	if event.EventID == "" {
		event.EventID = stableEventID(tenantID, accountID, symbol, event.EventType, event.OccurredAt)
	}
	if event.OccurredAt == 0 {
		event.OccurredAt = time.Now().UTC().UnixMilli()
	}

	lease, err := r.acquire(ctx, positionLockKey(tenantID, accountID, symbol))
	if err != nil {
		return nil, err
	}
	if lease != nil {
		defer func() {
			_ = lease.Release(context.Background())
		}()
	}

	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	sequence, inserted, err := r.insertPositionEventTx(ctx, tx, tenantID, event)
	if err != nil {
		return nil, err
	}
	current, err := r.loadPositionTx(ctx, tx, tenantID, accountID, symbol)
	if err != nil {
		return nil, err
	}
	if !inserted {
		return current, nil
	}

	next := applyPositionEvent(current, event, sequence)
	if next == nil {
		return nil, fmt.Errorf("unable to materialize position for %s/%s", accountID, symbol)
	}
	if err := r.upsertPositionTx(ctx, tx, next); err != nil {
		return nil, err
	}

	if next.Version%r.snapshotEvery == 0 || strings.EqualFold(event.EventType, "SNAPSHOT") {
		if _, err := r.savePositionSnapshotTx(ctx, tx, tenantID, accountID, fmt.Sprintf("v%d", next.Version)); err != nil {
			return nil, err
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	_ = r.storePositionCache(ctx, next)
	return clonePosition(next), nil
}

// GetPosition returns a single position using cache-first lookup.
func (r *Repository) GetPosition(ctx context.Context, tenantID, accountID, symbol string) (*pb.Position, bool, error) {
	tenantID = normalizeTenant(tenantID)
	accountID = strings.TrimSpace(accountID)
	symbol = normalizeSymbol(symbol)
	if accountID == "" || symbol == "" {
		return nil, false, nil
	}
	if pos, ok, err := r.loadPositionCache(ctx, tenantID, accountID, symbol); err == nil && ok {
		return pos, true, nil
	}
	row, err := r.loadPosition(ctx, tenantID, accountID, symbol)
	if err != nil {
		return nil, false, err
	}
	if row == nil {
		return nil, false, nil
	}
	_ = r.storePositionCache(ctx, row)
	return row, true, nil
}

// ListPositions returns all positions for an account.
func (r *Repository) ListPositions(ctx context.Context, tenantID, accountID string) ([]*pb.Position, error) {
	tenantID = normalizeTenant(tenantID)
	accountID = strings.TrimSpace(accountID)
	if accountID == "" {
		return nil, nil
	}
	if positions, ok, err := r.loadAccountPositionsCache(ctx, tenantID, accountID); err == nil && ok {
		return positions, nil
	}
	rows, err := r.db.QueryContext(ctx, `
SELECT tenant_id, account_id, symbol, sector, quantity, average_price, market_price, market_value, cost_basis,
       unrealized_pl, realized_pl, gross_exposure, net_exposure, leverage, side, version, sequence,
       snapshot_version, updated_at, metadata
  FROM positions
 WHERE tenant_id = $1 AND account_id = $2
 ORDER BY symbol ASC`, tenantID, accountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]*pb.Position, 0, 8)
	for rows.Next() {
		pos, err := scanPosition(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, pos)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(result) > 0 {
		_ = r.storeAccountPositionsCache(ctx, tenantID, accountID, result)
	}
	return result, nil
}

// SavePositionSnapshot stores the latest account state as an append-only snapshot.
func (r *Repository) SavePositionSnapshot(ctx context.Context, tenantID, accountID, snapshotVersion string) (*pb.PositionSnapshot, error) {
	tenantID = normalizeTenant(tenantID)
	accountID = strings.TrimSpace(accountID)
	if accountID == "" {
		return nil, fmt.Errorf("account is required")
	}
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	snapshot, err := r.savePositionSnapshotTx(ctx, tx, tenantID, accountID, snapshotVersion)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return snapshot, nil
}

// LoadPositionSnapshot returns the latest or named position snapshot.
func (r *Repository) LoadPositionSnapshot(ctx context.Context, tenantID, accountID, snapshotVersion string) (*pb.PositionSnapshot, error) {
	tenantID = normalizeTenant(tenantID)
	accountID = strings.TrimSpace(accountID)
	if accountID == "" {
		return nil, nil
	}
	query := `
SELECT snapshot_id, tenant_id, account_id, snapshot_version, event_sequence, checksum, positions, created_at, metadata
  FROM position_snapshots
 WHERE tenant_id = $1 AND account_id = $2`
	args := []any{tenantID, accountID}
	if snapshotVersion != "" {
		query += " AND snapshot_version = $3"
		args = append(args, snapshotVersion)
	} else {
		query += " ORDER BY created_at DESC LIMIT 1"
	}
	row := r.db.QueryRowContext(ctx, query, args...)
	return scanPositionSnapshot(row)
}

// ReplayPositionState rebuilds a position aggregate from snapshot plus event log.
func (r *Repository) ReplayPositionState(ctx context.Context, tenantID, accountID string, fromSequence, toSequence int64, snapshotVersion string) (*pb.PositionSnapshot, int64, error) {
	tenantID = normalizeTenant(tenantID)
	accountID = strings.TrimSpace(accountID)
	if accountID == "" {
		return nil, 0, fmt.Errorf("account is required")
	}
	snapshot, err := r.LoadPositionSnapshot(ctx, tenantID, accountID, snapshotVersion)
	if err != nil {
		return nil, 0, err
	}
	positions := map[string]*pb.Position{}
	var replayed int64
	var baseSequence int64
	var lastSequence int64
	if snapshot != nil {
		baseSequence = snapshot.EventOffset
		lastSequence = snapshot.EventOffset
		for _, pos := range snapshot.Positions {
			if pos == nil {
				continue
			}
			positions[positionMapKey(pos.TenantId, pos.AccountId, pos.Symbol)] = clonePosition(pos)
		}
	}
	if fromSequence > baseSequence {
		baseSequence = fromSequence
	}
	query := `
SELECT event_id, tenant_id, account_id, symbol, sector, event_type, quantity_delta, price, market_price, source,
       sequence, correlation_id, trace_id, snapshot_version, occurred_at, payload, created_at
  FROM position_events
 WHERE tenant_id = $1 AND account_id = $2 AND sequence > $3`
	args := []any{tenantID, accountID, baseSequence}
	if toSequence > 0 {
		query += " AND sequence <= $4"
		args = append(args, toSequence)
	}
	query += " ORDER BY sequence ASC"
	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	for rows.Next() {
		event, err := scanPositionEvent(rows)
		if err != nil {
			return nil, 0, err
		}
		key := positionMapKey(event.TenantID, event.AccountID, event.Symbol)
		positions[key] = applyPositionEvent(positions[key], event, event.Sequence)
		if event.Sequence > lastSequence {
			lastSequence = event.Sequence
		}
		replayed++
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	result := &pb.PositionSnapshot{
		SnapshotID:     stableSnapshotID("position", tenantID, accountID, time.Now().UTC().UnixMilli()),
		TenantID:       tenantID,
		AccountID:      accountID,
		Version:        lastSequence,
		EventOffset:    lastSequence,
		SnapshotVersion: firstString(snapshotVersion, "replay"),
		Positions:      make([]*pb.Position, 0, len(positions)),
		Checksum:       checksumPositions(positions),
		CreatedAt:      time.Now().UTC().UnixMilli(),
		Metadata:       map[string]string{"source": "replay", "snapshot_version": firstString(snapshotVersion, "replay")},
	}
	for _, pos := range positions {
		result.Positions = append(result.Positions, clonePosition(pos))
	}
	sort.Slice(result.Positions, func(i, j int) bool {
		return result.Positions[i].Symbol < result.Positions[j].Symbol
	})
	if _, err := r.persistPositionSnapshot(ctx, result); err != nil {
		return nil, 0, err
	}
	if err := r.savePositionReplayState(ctx, tenantID, accountID, result.SnapshotVersion, result.EventOffset, replayed > 0, map[string]string{
		"replayed":  fmt.Sprintf("%d", replayed),
		"from_seq":  fmt.Sprintf("%d", baseSequence),
		"to_seq":    fmt.Sprintf("%d", lastSequence),
	}); err != nil {
		return nil, 0, err
	}
	_ = r.storeAccountPositionsCache(ctx, tenantID, accountID, result.Positions)
	return result, replayed, nil
}

// ReconcilePositionState compares durable and hot state and repairs cache drift.
func (r *Repository) ReconcilePositionState(ctx context.Context, tenantID, accountID, symbol string, force bool) (bool, bool, int64, error) {
	tenantID = normalizeTenant(tenantID)
	accountID = strings.TrimSpace(accountID)
	symbol = normalizeSymbol(symbol)
	positions, err := r.ListPositions(ctx, tenantID, accountID)
	if err != nil {
		return false, false, 0, err
	}
	drift := false
	driftCount := int64(0)
	if symbol != "" {
		for _, pos := range positions {
			if pos == nil || !strings.EqualFold(pos.Symbol, symbol) {
				continue
			}
			stored, ok, err := r.loadPositionCache(ctx, tenantID, accountID, symbol)
			if err != nil {
				return false, false, 0, err
			}
			if !ok || !positionsEqual(pos, stored) {
				drift = true
				driftCount++
			}
		}
	} else {
		for _, pos := range positions {
			if pos == nil {
				continue
			}
			stored, ok, err := r.loadPositionCache(ctx, tenantID, accountID, pos.Symbol)
			if err != nil {
				return false, false, 0, err
			}
			if !ok || !positionsEqual(pos, stored) {
				drift = true
				driftCount++
			}
		}
	}
	if drift || force {
		_ = r.storeAccountPositionsCache(ctx, tenantID, accountID, positions)
		if _, err := r.SavePositionSnapshot(ctx, tenantID, accountID, "reconciled"); err != nil {
			return false, drift, driftCount, err
		}
		if err := r.savePositionReplayState(ctx, tenantID, accountID, "reconciled", latestSequence(positions), true, map[string]string{
			"drift_count": fmt.Sprintf("%d", driftCount),
			"force":       fmt.Sprintf("%t", force),
			"symbol":      symbol,
		}); err != nil {
			return false, drift, driftCount, err
		}
		return true, drift, driftCount, nil
	}
	return true, false, driftCount, nil
}

func (r *Repository) acquire(ctx context.Context, key string) (*lock.Lease, error) {
	if r.locks == nil {
		return nil, nil
	}
	return r.locks.Acquire(ctx, key, 2*time.Second)
}

func (r *Repository) insertPositionEventTx(ctx context.Context, tx *sql.Tx, tenantID string, event *pb.PositionEvent) (int64, bool, error) {
	payload, err := json.Marshal(event)
	if err != nil {
		return 0, false, err
	}
	var sequence int64
	err = tx.QueryRowContext(ctx, `
INSERT INTO position_events (
    event_id, tenant_id, account_id, symbol, sector, event_type, quantity_delta, price, market_price,
    source, correlation_id, trace_id, snapshot_version, occurred_at, payload
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,to_timestamp($14::double precision / 1000.0),$15)
ON CONFLICT (event_id) DO NOTHING
RETURNING sequence`,
		event.EventID, tenantID, event.AccountID, normalizeSymbol(event.Symbol), event.Sector, strings.ToUpper(strings.TrimSpace(event.EventType)),
		event.QuantityDelta, nullFloat(event.Price), nullFloat(event.MarketPrice), firstString(event.Source, "position-tracking-service"),
		event.CorrelationID, event.TraceID, firstString(event.Metadata["snapshot_version"], "live"), event.OccurredAt, payload,
	).Scan(&sequence)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, false, nil
		}
		return 0, false, err
	}
	return sequence, true, nil
}

func (r *Repository) upsertPositionTx(ctx context.Context, tx *sql.Tx, position *pb.Position) error {
	metadata, err := json.Marshal(position.Metadata)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
INSERT INTO positions (
    tenant_id, account_id, symbol, sector, quantity, average_price, market_price, market_value, cost_basis,
    unrealized_pl, realized_pl, gross_exposure, net_exposure, leverage, side, version, sequence,
    snapshot_version, updated_at, metadata
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,to_timestamp($19::double precision / 1000.0),$20)
ON CONFLICT (tenant_id, account_id, symbol) DO UPDATE SET
    sector = EXCLUDED.sector,
    quantity = EXCLUDED.quantity,
    average_price = EXCLUDED.average_price,
    market_price = EXCLUDED.market_price,
    market_value = EXCLUDED.market_value,
    cost_basis = EXCLUDED.cost_basis,
    unrealized_pl = EXCLUDED.unrealized_pl,
    realized_pl = EXCLUDED.realized_pl,
    gross_exposure = EXCLUDED.gross_exposure,
    net_exposure = EXCLUDED.net_exposure,
    leverage = EXCLUDED.leverage,
    side = EXCLUDED.side,
    version = EXCLUDED.version,
    sequence = EXCLUDED.sequence,
    snapshot_version = EXCLUDED.snapshot_version,
    updated_at = EXCLUDED.updated_at,
    metadata = EXCLUDED.metadata`,
		position.TenantId, position.AccountId, normalizeSymbol(position.Symbol), position.Sector, position.Quantity, position.AveragePrice,
		position.MarketPrice, position.MarketValue, position.CostBasis, position.UnrealizedPL, position.RealizedPL, position.GrossExposure,
		position.NetExposure, position.Leverage, position.Side, position.Version, position.Sequence, firstString(position.SnapshotVersion, "live"),
		position.UpdatedAt, metadata,
	)
	return err
}

func (r *Repository) loadPosition(ctx context.Context, tenantID, accountID, symbol string) (*pb.Position, error) {
	if pos, ok, err := r.loadPositionCache(ctx, tenantID, accountID, symbol); err == nil && ok {
		return pos, nil
	}
	row := r.db.QueryRowContext(ctx, `
SELECT tenant_id, account_id, symbol, sector, quantity, average_price, market_price, market_value, cost_basis,
       unrealized_pl, realized_pl, gross_exposure, net_exposure, leverage, side, version, sequence,
       snapshot_version, updated_at, metadata
  FROM positions
 WHERE tenant_id = $1 AND account_id = $2 AND symbol = $3`,
		tenantID, accountID, normalizeSymbol(symbol),
	)
	position, err := scanPosition(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return position, nil
}

func (r *Repository) loadPositionTx(ctx context.Context, tx *sql.Tx, tenantID, accountID, symbol string) (*pb.Position, error) {
	row := tx.QueryRowContext(ctx, `
SELECT tenant_id, account_id, symbol, sector, quantity, average_price, market_price, market_value, cost_basis,
       unrealized_pl, realized_pl, gross_exposure, net_exposure, leverage, side, version, sequence,
       snapshot_version, updated_at, metadata
  FROM positions
 WHERE tenant_id = $1 AND account_id = $2 AND symbol = $3`,
		tenantID, accountID, normalizeSymbol(symbol),
	)
	position, err := scanPosition(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return position, nil
}

func (r *Repository) savePositionSnapshotTx(ctx context.Context, tx *sql.Tx, tenantID, accountID, snapshotVersion string) (*pb.PositionSnapshot, error) {
	positions, err := r.listPositionsTx(ctx, tx, tenantID, accountID)
	if err != nil {
		return nil, err
	}
	snapshot := &pb.PositionSnapshot{
		SnapshotID:     stableSnapshotID("position", tenantID, accountID, time.Now().UTC().UnixMilli()),
		TenantID:       tenantID,
		AccountID:      accountID,
		Version:        latestSequence(positions),
		EventOffset:    latestSequence(positions),
		SnapshotVersion: firstString(snapshotVersion, "live"),
		Positions:      positions,
		Checksum:       checksumPositions(sliceToMap(positions)),
		CreatedAt:      time.Now().UTC().UnixMilli(),
		Metadata:       map[string]string{"snapshot_version": firstString(snapshotVersion, "live")},
	}
	if err := r.persistPositionSnapshot(ctx, tx, snapshot); err != nil {
		return nil, err
	}
	_ = r.storePositionSnapshotCache(ctx, snapshot)
	return snapshot, nil
}

func (r *Repository) persistPositionSnapshot(ctx context.Context, txOrNil any, snapshot *pb.PositionSnapshot) error {
	if snapshot == nil {
		return nil
	}
	payload, err := json.Marshal(snapshot.Positions)
	if err != nil {
		return err
	}
	metadata, err := json.Marshal(snapshot.Metadata)
	if err != nil {
		return err
	}
	stmt := `
INSERT INTO position_snapshots (
    snapshot_id, tenant_id, account_id, snapshot_version, event_sequence, checksum, positions, metadata, created_at
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,to_timestamp($9::double precision / 1000.0))`
if tx, ok := txOrNil.(*sql.Tx); ok && tx != nil {
		_, err = tx.ExecContext(ctx, stmt, snapshot.SnapshotID, snapshot.TenantID, snapshot.AccountID, snapshot.SnapshotVersion, snapshot.EventOffset, snapshot.Checksum, payload, metadata, snapshot.CreatedAt)
		return err
	}
	_, err = r.db.ExecContext(ctx, stmt, snapshot.SnapshotID, snapshot.TenantID, snapshot.AccountID, snapshot.SnapshotVersion, snapshot.EventOffset, snapshot.Checksum, payload, metadata, snapshot.CreatedAt)
	return err
}

func (r *Repository) listPositionsTx(ctx context.Context, tx *sql.Tx, tenantID, accountID string) ([]*pb.Position, error) {
	rows, err := tx.QueryContext(ctx, `
SELECT tenant_id, account_id, symbol, sector, quantity, average_price, market_price, market_value, cost_basis,
       unrealized_pl, realized_pl, gross_exposure, net_exposure, leverage, side, version, sequence,
       snapshot_version, updated_at, metadata
  FROM positions
 WHERE tenant_id = $1 AND account_id = $2
 ORDER BY symbol ASC`, tenantID, accountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	positions := make([]*pb.Position, 0, 8)
	for rows.Next() {
		pos, err := scanPosition(rows)
		if err != nil {
			return nil, err
		}
		positions = append(positions, pos)
	}
	return positions, rows.Err()
}

func (r *Repository) listPositionsFresh(ctx context.Context, tenantID, accountID string) ([]*pb.Position, error) {
	rows, err := r.db.QueryContext(ctx, `
SELECT tenant_id, account_id, symbol, sector, quantity, average_price, market_price, market_value, cost_basis,
       unrealized_pl, realized_pl, gross_exposure, net_exposure, leverage, side, version, sequence,
       snapshot_version, updated_at, metadata
  FROM positions
 WHERE tenant_id = $1 AND account_id = $2
 ORDER BY symbol ASC`, tenantID, accountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	positions := make([]*pb.Position, 0, 8)
	for rows.Next() {
		pos, err := scanPosition(rows)
		if err != nil {
			return nil, err
		}
		positions = append(positions, pos)
	}
	return positions, rows.Err()
}

func (r *Repository) storePositionCache(ctx context.Context, position *pb.Position) error {
	if r.cache == nil || position == nil {
		return nil
	}
	tenantID := normalizeTenant(position.TenantId)
	if err := r.cache.StorePosition(ctx, tenantID, position, 12*time.Hour); err != nil {
		return err
	}
	positions, err := r.listPositionsFresh(ctx, tenantID, position.AccountId)
	if err != nil {
		return err
	}
	return r.cache.StorePositions(ctx, tenantID, position.AccountId, positions, 12*time.Hour)
}

func (r *Repository) loadPositionCache(ctx context.Context, tenantID, accountID, symbol string) (*pb.Position, bool, error) {
	if r.cache == nil {
		return nil, false, nil
	}
	return r.cache.LoadPosition(ctx, tenantID, accountID, symbol)
}

func (r *Repository) loadAccountPositionsCache(ctx context.Context, tenantID, accountID string) ([]*pb.Position, bool, error) {
	if r.cache == nil {
		return nil, false, nil
	}
	return r.cache.LoadPositions(ctx, tenantID, accountID)
}

func (r *Repository) storeAccountPositionsCache(ctx context.Context, tenantID, accountID string, positions []*pb.Position) error {
	if r.cache == nil {
		return nil
	}
	return r.cache.StorePositions(ctx, tenantID, accountID, positions, 12*time.Hour)
}

func (r *Repository) storePositionSnapshotCache(ctx context.Context, snapshot *pb.PositionSnapshot) error {
	if r.cache == nil {
		return nil
	}
	return r.cache.StorePositionSnapshot(ctx, snapshot, 24*time.Hour)
}

func scanPosition(scanner interface {
	Scan(dest ...any) error
}) (*pb.Position, error) {
	var (
		sector, side, snapshotVersion string
		updatedAt                     time.Time
		metadataRaw                   []byte
		position                      pb.Position
	)
	err := scanner.Scan(
		&position.TenantId,
		&position.AccountId,
		&position.Symbol,
		&sector,
		&position.Quantity,
		&position.AveragePrice,
		&position.MarketPrice,
		&position.MarketValue,
		&position.CostBasis,
		&position.UnrealizedPL,
		&position.RealizedPL,
		&position.GrossExposure,
		&position.NetExposure,
		&position.Leverage,
		&side,
		&position.Version,
		&position.Sequence,
		&snapshotVersion,
		&updatedAt,
		&metadataRaw,
	)
	if err != nil {
		return nil, err
	}
	position.Sector = sector
	position.Side = side
	position.SnapshotVersion = snapshotVersion
	position.UpdatedAt = updatedAt.UTC().UnixMilli()
	position.Timestamp = position.UpdatedAt
	if len(metadataRaw) > 0 {
		var metadata map[string]string
		if err := json.Unmarshal(metadataRaw, &metadata); err == nil {
			position.Metadata = metadata
		}
	}
	return &position, nil
}

func scanPositionEvent(scanner interface {
	Scan(dest ...any) error
}) (*pb.PositionEvent, error) {
	var (
		sector, eventType, source, correlationID, traceID, snapshotVersion string
		occurredAt                                                         time.Time
		payloadRaw                                                         []byte
		price                                                              sql.NullFloat64
		marketPrice                                                        sql.NullFloat64
		event                                                              pb.PositionEvent
	)
	err := scanner.Scan(
		&event.EventID,
		&event.TenantID,
		&event.AccountID,
		&event.Symbol,
		&sector,
		&eventType,
		&event.QuantityDelta,
		&price,
		&marketPrice,
		&source,
		&event.Sequence,
		&correlationID,
		&traceID,
		&snapshotVersion,
		&occurredAt,
		&payloadRaw,
		new(time.Time),
	)
	if err != nil {
		return nil, err
	}
	event.Sector = sector
	event.EventType = eventType
	event.Source = source
	event.CorrelationID = correlationID
	event.TraceID = traceID
	if price.Valid {
		event.Price = price.Float64
	}
	if marketPrice.Valid {
		event.MarketPrice = marketPrice.Float64
	}
	event.OccurredAt = occurredAt.UTC().UnixMilli()
	event.Metadata = map[string]string{}
	if snapshotVersion != "" {
		event.Metadata["snapshot_version"] = snapshotVersion
	}
	if len(payloadRaw) > 0 {
		var payload pb.PositionEvent
		if err := json.Unmarshal(payloadRaw, &payload); err == nil {
			if len(payload.Metadata) > 0 {
				event.Metadata = payload.Metadata
			}
		}
	}
	return &event, nil
}

func scanPositionSnapshot(scanner interface {
	Scan(dest ...any) error
}) (*pb.PositionSnapshot, error) {
	var (
		positionsRaw []byte
		metadataRaw  []byte
		createdAt    time.Time
		snapshot     pb.PositionSnapshot
	)
	if err := scanner.Scan(
		&snapshot.SnapshotID,
		&snapshot.TenantID,
		&snapshot.AccountID,
		&snapshot.SnapshotVersion,
		&snapshot.EventOffset,
		&snapshot.Checksum,
		&positionsRaw,
		&createdAt,
		&metadataRaw,
	); err != nil {
		return nil, err
	}
	if len(positionsRaw) > 0 {
		if err := json.Unmarshal(positionsRaw, &snapshot.Positions); err != nil {
			return nil, err
		}
	}
	if len(metadataRaw) > 0 {
		if err := json.Unmarshal(metadataRaw, &snapshot.Metadata); err != nil {
			return nil, err
		}
	}
	snapshot.CreatedAt = createdAt.UTC().UnixMilli()
	snapshot.Version = snapshot.EventOffset
	return &snapshot, nil
}

func clonePosition(position *pb.Position) *pb.Position {
	if position == nil {
		return nil
	}
	copy := *position
	if position.Metadata != nil {
		copy.Metadata = make(map[string]string, len(position.Metadata))
		for k, v := range position.Metadata {
			copy.Metadata[k] = v
		}
	}
	return &copy
}

func applyPositionEvent(previous *pb.Position, event *pb.PositionEvent, sequence int64) *pb.Position {
	next := clonePosition(previous)
	if next == nil {
		next = &pb.Position{
			TenantId: normalizeTenant(event.TenantID),
			AccountId: strings.TrimSpace(event.AccountID),
			Symbol:    normalizeSymbol(event.Symbol),
			Sector:    event.Sector,
			Metadata:  map[string]string{},
		}
	}
	if next.Metadata == nil {
		next.Metadata = map[string]string{}
	}
	mergeStringMap(next.Metadata, event.Metadata)
	next.TenantId = normalizeTenant(firstString(next.TenantId, event.TenantID))
	next.AccountId = strings.TrimSpace(firstString(next.AccountId, event.AccountID))
	next.Symbol = normalizeSymbol(firstString(next.Symbol, event.Symbol))
	if event.Sector != "" {
		next.Sector = event.Sector
	}
	tradePrice := event.Price
	if tradePrice <= 0 {
		if event.MarketPrice > 0 {
			tradePrice = event.MarketPrice
		} else if next.MarketPrice > 0 {
			tradePrice = next.MarketPrice
		} else if next.AveragePrice > 0 {
			tradePrice = next.AveragePrice
		} else {
			tradePrice = 0
		}
	}
	delta := event.QuantityDelta
	currentQty := next.Quantity
	newQty := currentQty + delta
	if currentQty == 0 || sameSign(currentQty, delta) {
		next.AveragePrice = weightedAverage(absInt32(currentQty), next.AveragePrice, absInt32(delta), tradePrice)
	} else if newQty == 0 {
		next.RealizedPL += realizedForClose(currentQty, delta, tradePrice, next.AveragePrice)
		next.AveragePrice = tradePrice
	} else {
		closingQty := minInt32(absInt32(currentQty), absInt32(delta))
		next.RealizedPL += realizedForClose(currentQty, signInt32(delta)*closingQty, tradePrice, next.AveragePrice)
		if sameSign(newQty, delta) {
			next.AveragePrice = tradePrice
		}
	}
	next.Quantity = newQty
	if event.MarketPrice > 0 {
		next.MarketPrice = event.MarketPrice
	} else {
		next.MarketPrice = tradePrice
	}
	next.MarketValue = float64(next.Quantity) * next.MarketPrice
	next.CostBasis = math.Abs(float64(next.Quantity)) * next.AveragePrice
	next.UnrealizedPL = unrealizedPL(next.Quantity, next.MarketPrice, next.AveragePrice)
	next.RealizedPL = next.RealizedPL
	next.GrossExposure = math.Abs(next.MarketValue)
	next.NetExposure = next.MarketValue
	next.Leverage = leverageFromPosition(next)
	next.Side = positionSide(next.Quantity)
	next.Version = firstNonZero(next.Version, 0) + 1
	next.Sequence = sequence
	next.SnapshotVersion = firstString(next.SnapshotVersion, "live")
	next.UpdatedAt = time.Now().UTC().UnixMilli()
	next.Timestamp = next.UpdatedAt
	return next
}

func leverageFromPosition(position *pb.Position) float64 {
	if position == nil {
		return 0
	}
	base := math.Abs(position.CostBasis) + math.Abs(position.RealizedPL) + math.Abs(position.UnrealizedPL)
	if base <= 0 {
		base = 1
	}
	return math.Abs(position.GrossExposure) / base
}

func unrealizedPL(quantity int32, marketPrice, averagePrice float64) float64 {
	if quantity == 0 {
		return 0
	}
	if quantity > 0 {
		return (marketPrice - averagePrice) * float64(quantity)
	}
	return (averagePrice - marketPrice) * float64(-quantity)
}

func realizedForClose(currentQty, deltaQty int32, tradePrice, averagePrice float64) float64 {
	closingQty := math.Min(float64(absInt32(currentQty)), float64(absInt32(deltaQty)))
	if currentQty > 0 {
		return closingQty * (tradePrice - averagePrice)
	}
	return closingQty * (averagePrice - tradePrice)
}

func weightedAverage(currentQty int32, currentAvg float64, deltaQty int32, deltaAvg float64) float64 {
	totalQty := float64(currentQty + deltaQty)
	if totalQty == 0 {
		return deltaAvg
	}
	return ((float64(currentQty) * currentAvg) + (float64(deltaQty) * deltaAvg)) / totalQty
}

func positionSide(quantity int32) string {
	switch {
	case quantity > 0:
		return "LONG"
	case quantity < 0:
		return "SHORT"
	default:
		return "FLAT"
	}
}

func positionsEqual(left, right *pb.Position) bool {
	if left == nil || right == nil {
		return left == right
	}
	return left.TenantId == right.TenantId &&
		left.AccountId == right.AccountId &&
		normalizeSymbol(left.Symbol) == normalizeSymbol(right.Symbol) &&
		left.Quantity == right.Quantity &&
		almostEqual(left.AveragePrice, right.AveragePrice) &&
		almostEqual(left.MarketPrice, right.MarketPrice) &&
		almostEqual(left.MarketValue, right.MarketValue) &&
		almostEqual(left.CostBasis, right.CostBasis) &&
		almostEqual(left.UnrealizedPL, right.UnrealizedPL) &&
		almostEqual(left.RealizedPL, right.RealizedPL) &&
		left.Side == right.Side &&
		left.Version == right.Version &&
		left.SnapshotVersion == right.SnapshotVersion
}

func almostEqual(left, right float64) bool {
	return math.Abs(left-right) < 0.0001
}

func normalizeTenant(raw string) string {
	if value := strings.TrimSpace(raw); value != "" {
		return value
	}
	return "default"
}

func normalizeSymbol(raw string) string {
	return strings.ToUpper(strings.TrimSpace(raw))
}

func positionLockKey(tenantID, accountID, symbol string) string {
	return strings.Join([]string{"position", normalizeTenant(tenantID), strings.TrimSpace(accountID), normalizeSymbol(symbol)}, ":")
}

func stableEventID(tenantID, accountID, symbol, eventType string, occurredAt int64) string {
	if occurredAt == 0 {
		occurredAt = time.Now().UTC().UnixMilli()
	}
	return strings.Join([]string{normalizeTenant(tenantID), strings.TrimSpace(accountID), normalizeSymbol(symbol), strings.ToLower(strings.TrimSpace(eventType)), fmt.Sprintf("%d", occurredAt)}, ":")
}

func stableSnapshotID(prefix, tenantID, accountID string, stamp int64) string {
	return strings.Join([]string{prefix, normalizeTenant(tenantID), strings.TrimSpace(accountID), fmt.Sprintf("%d", stamp)}, ":")
}

func firstString(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func firstNonZero(values ...int64) int64 {
	for _, value := range values {
		if value != 0 {
			return value
		}
	}
	return 0
}

func mergeStringMap(dst map[string]string, src map[string]string) {
	if dst == nil {
		return
	}
	for k, v := range src {
		if strings.TrimSpace(k) == "" {
			continue
		}
		dst[k] = v
	}
}

func absInt32(value int32) int32 {
	if value < 0 {
		return -value
	}
	return value
}

func sameSign(left, right int32) bool {
	if left == 0 || right == 0 {
		return true
	}
	return (left > 0 && right > 0) || (left < 0 && right < 0)
}

func signInt32(value int32) int32 {
	switch {
	case value > 0:
		return 1
	case value < 0:
		return -1
	default:
		return 0
	}
}

func minInt32(left, right int32) int32 {
	if left < right {
		return left
	}
	return right
}

func nullFloat(value float64) any {
	if value == 0 {
		return nil
	}
	return value
}

func snapshotVersionNumber(raw string) int64 {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "live" {
		return 0
	}
	return int64(len(raw))
}

func latestSequence(positions []*pb.Position) int64 {
	var latest int64
	for _, pos := range positions {
		if pos != nil && pos.Sequence > latest {
			latest = pos.Sequence
		}
	}
	return latest
}

func checksumPositions(positions map[string]*pb.Position) string {
	if len(positions) == 0 {
		return ""
	}
	keys := make([]string, 0, len(positions))
	for key := range positions {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	h := sha256.New()
	for _, key := range keys {
		payload, _ := json.Marshal(positions[key])
		_, _ = h.Write([]byte(key))
		_, _ = h.Write(payload)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func sliceToMap(positions []*pb.Position) map[string]*pb.Position {
	result := make(map[string]*pb.Position, len(positions))
	for _, pos := range positions {
		if pos == nil {
			continue
		}
		result[positionMapKey(pos.TenantId, pos.AccountId, pos.Symbol)] = pos
	}
	return result
}

func positionMapKey(tenantID, accountID, symbol string) string {
	return strings.Join([]string{normalizeTenant(tenantID), strings.TrimSpace(accountID), normalizeSymbol(symbol)}, ":")
}

func (r *Repository) savePositionReplayState(ctx context.Context, tenantID, accountID, snapshotVersion string, lastSequence int64, recovered bool, details map[string]string) error {
	if r.db == nil {
		return nil
	}
	payload, err := json.Marshal(details)
	if err != nil {
		return err
	}
	_, err = r.db.ExecContext(ctx, `
INSERT INTO position_replay_state (
    tenant_id, account_id, snapshot_version, last_sequence, last_replayed_at, recovered, details, updated_at
) VALUES ($1,$2,$3,$4,now(),$5,$6,now())
ON CONFLICT (tenant_id, account_id) DO UPDATE SET
    snapshot_version = EXCLUDED.snapshot_version,
    last_sequence = EXCLUDED.last_sequence,
    last_replayed_at = EXCLUDED.last_replayed_at,
    recovered = EXCLUDED.recovered,
    details = EXCLUDED.details,
    updated_at = EXCLUDED.updated_at`,
		normalizeTenant(tenantID), strings.TrimSpace(accountID), firstString(snapshotVersion, "live"), lastSequence, recovered, payload,
	)
	return err
}
