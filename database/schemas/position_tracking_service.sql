-- Position Tracking Service schema
-- Stores canonical positions, immutable position events, and replay state.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS positions (
    position_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id VARCHAR(255) NOT NULL DEFAULT 'default',
    account_id VARCHAR(255) NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    sector VARCHAR(100),
    quantity INTEGER NOT NULL,
    average_price NUMERIC(20, 6) NOT NULL,
    market_price NUMERIC(20, 6) NOT NULL,
    market_value NUMERIC(20, 6) NOT NULL,
    cost_basis NUMERIC(20, 6) NOT NULL,
    unrealized_pl NUMERIC(20, 6) NOT NULL DEFAULT 0,
    realized_pl NUMERIC(20, 6) NOT NULL DEFAULT 0,
    gross_exposure NUMERIC(20, 6) NOT NULL DEFAULT 0,
    net_exposure NUMERIC(20, 6) NOT NULL DEFAULT 0,
    leverage NUMERIC(20, 6) NOT NULL DEFAULT 0,
    side VARCHAR(10) NOT NULL,
    version BIGINT NOT NULL DEFAULT 1,
    sequence BIGINT NOT NULL DEFAULT 1,
    snapshot_version VARCHAR(100) NOT NULL DEFAULT 'live',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata JSONB,
    UNIQUE (tenant_id, account_id, symbol)
);

CREATE TABLE IF NOT EXISTS position_events (
    event_id VARCHAR(255) PRIMARY KEY,
    tenant_id VARCHAR(255) NOT NULL DEFAULT 'default',
    account_id VARCHAR(255) NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    sector VARCHAR(100),
    event_type VARCHAR(50) NOT NULL,
    quantity_delta INTEGER NOT NULL,
    price NUMERIC(20, 6),
    market_price NUMERIC(20, 6),
    source VARCHAR(100),
    sequence BIGSERIAL NOT NULL,
    correlation_id VARCHAR(255),
    trace_id VARCHAR(255),
    snapshot_version VARCHAR(100),
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    payload JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS position_snapshots (
    snapshot_id VARCHAR(255) PRIMARY KEY,
    tenant_id VARCHAR(255) NOT NULL DEFAULT 'default',
    account_id VARCHAR(255) NOT NULL,
    snapshot_version VARCHAR(100) NOT NULL,
    event_sequence BIGINT NOT NULL,
    checksum VARCHAR(128) NOT NULL,
    positions JSONB NOT NULL,
    metadata JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS position_replay_state (
    tenant_id VARCHAR(255) NOT NULL DEFAULT 'default',
    account_id VARCHAR(255) NOT NULL,
    snapshot_version VARCHAR(100) NOT NULL DEFAULT 'live',
    last_sequence BIGINT NOT NULL DEFAULT 0,
    last_replayed_at TIMESTAMPTZ,
    recovered BOOLEAN NOT NULL DEFAULT FALSE,
    details JSONB,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, account_id)
);

CREATE INDEX IF NOT EXISTS idx_positions_account ON positions(tenant_id, account_id);
CREATE INDEX IF NOT EXISTS idx_positions_symbol ON positions(symbol);
CREATE INDEX IF NOT EXISTS idx_positions_tenant_symbol ON positions(tenant_id, symbol);
CREATE INDEX IF NOT EXISTS idx_position_events_account_created ON position_events(tenant_id, account_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_position_events_sequence ON position_events(tenant_id, account_id, sequence);
CREATE INDEX IF NOT EXISTS idx_position_snapshots_account ON position_snapshots(tenant_id, account_id, created_at DESC);
