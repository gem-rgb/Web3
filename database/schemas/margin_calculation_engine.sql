-- Margin Calculation Engine schema
-- Stores margin requirements and model parameters.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS margin_requirements (
    requirement_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    account_id VARCHAR(255) NOT NULL UNIQUE,
    initial_margin NUMERIC(20, 6) NOT NULL,
    maintenance_margin NUMERIC(20, 6) NOT NULL,
    margin_excess NUMERIC(20, 6) NOT NULL,
    margin_ratio NUMERIC(20, 6) NOT NULL,
    model_name VARCHAR(50) NOT NULL DEFAULT 'reg_t',
    version BIGINT NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS margin_model_parameters (
    parameter_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    model_name VARCHAR(50) NOT NULL,
    parameter_key VARCHAR(100) NOT NULL,
    parameter_value JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (model_name, parameter_key)
);

CREATE INDEX IF NOT EXISTS idx_margin_requirements_updated_at ON margin_requirements(updated_at DESC);
