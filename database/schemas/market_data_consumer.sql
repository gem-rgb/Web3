-- Market Data Consumer schema
-- Stores latest market data snapshots and history for replay.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS market_data_latest (
    symbol VARCHAR(20) PRIMARY KEY,
    bid_price NUMERIC(20, 6) NOT NULL,
    ask_price NUMERIC(20, 6) NOT NULL,
    last_price NUMERIC(20, 6) NOT NULL,
    volume BIGINT NOT NULL,
    source VARCHAR(100),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS market_data_history (
    record_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    symbol VARCHAR(20) NOT NULL,
    bid_price NUMERIC(20, 6) NOT NULL,
    ask_price NUMERIC(20, 6) NOT NULL,
    last_price NUMERIC(20, 6) NOT NULL,
    volume BIGINT NOT NULL,
    source VARCHAR(100),
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_market_data_history_symbol_time ON market_data_history(symbol, ingested_at DESC);
