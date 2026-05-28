-- Exposure Aggregation Service schema
-- Keeps aggregated exposure snapshots, replay state, and materialized views.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS exposure_snapshots (
    exposure_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id VARCHAR(255) NOT NULL DEFAULT 'default',
    account_id VARCHAR(255) NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    sector VARCHAR(100),
    net_quantity INTEGER NOT NULL,
    gross_exposure NUMERIC(20, 6) NOT NULL,
    net_exposure NUMERIC(20, 6) NOT NULL,
    long_exposure NUMERIC(20, 6) NOT NULL DEFAULT 0,
    short_exposure NUMERIC(20, 6) NOT NULL DEFAULT 0,
    market_value NUMERIC(20, 6) NOT NULL,
    unrealized_pl NUMERIC(20, 6) NOT NULL DEFAULT 0,
    realized_pl NUMERIC(20, 6) NOT NULL DEFAULT 0,
    leverage NUMERIC(20, 6) NOT NULL DEFAULT 0,
    concentration_pct NUMERIC(20, 6) NOT NULL DEFAULT 0,
    position_count INTEGER NOT NULL DEFAULT 0,
    version BIGINT NOT NULL DEFAULT 1,
    sequence BIGINT NOT NULL DEFAULT 1,
    snapshot_version VARCHAR(100) NOT NULL DEFAULT 'live',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata JSONB,
    UNIQUE (tenant_id, account_id, symbol)
);

CREATE TABLE IF NOT EXISTS exposure_audit (
    audit_id VARCHAR(255) PRIMARY KEY,
    tenant_id VARCHAR(255) NOT NULL DEFAULT 'default',
    account_id VARCHAR(255) NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    sector VARCHAR(100),
    event_type VARCHAR(50) NOT NULL DEFAULT 'exposure.updated',
    net_quantity INTEGER NOT NULL DEFAULT 0,
    gross_exposure NUMERIC(20, 6) NOT NULL DEFAULT 0,
    net_exposure NUMERIC(20, 6) NOT NULL DEFAULT 0,
    before_snapshot JSONB,
    after_snapshot JSONB,
    sequence BIGSERIAL NOT NULL,
    correlation_id VARCHAR(255),
    trace_id VARCHAR(255),
    snapshot_version VARCHAR(100),
    metadata JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS exposure_views (
    tenant_id VARCHAR(255) NOT NULL DEFAULT 'default',
    account_id VARCHAR(255) NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    sector VARCHAR(100),
    snapshot_version VARCHAR(100) NOT NULL DEFAULT 'live',
    summary JSONB NOT NULL,
    checksum VARCHAR(128) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, account_id, symbol)
);

CREATE TABLE IF NOT EXISTS exposure_replay_state (
    tenant_id VARCHAR(255) NOT NULL DEFAULT 'default',
    account_id VARCHAR(255) NOT NULL,
    symbol VARCHAR(20) NOT NULL DEFAULT '',
    snapshot_version VARCHAR(100) NOT NULL DEFAULT 'live',
    last_sequence BIGINT NOT NULL DEFAULT 0,
    last_replayed_at TIMESTAMPTZ,
    recovered BOOLEAN NOT NULL DEFAULT FALSE,
    details JSONB,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, account_id, symbol)
);

CREATE INDEX IF NOT EXISTS idx_exposure_snapshots_account ON exposure_snapshots(tenant_id, account_id);
CREATE INDEX IF NOT EXISTS idx_exposure_snapshots_symbol ON exposure_snapshots(symbol);
CREATE INDEX IF NOT EXISTS idx_exposure_audit_account_created ON exposure_audit(tenant_id, account_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_exposure_audit_sequence ON exposure_audit(tenant_id, account_id, sequence);
CREATE INDEX IF NOT EXISTS idx_exposure_views_account ON exposure_views(tenant_id, account_id, updated_at DESC);
