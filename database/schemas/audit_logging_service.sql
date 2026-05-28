-- Audit Logging Service schema
-- Immutable audit trail for compliance and forensics.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS audit_events (
    event_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    event_type VARCHAR(100) NOT NULL,
    account_id VARCHAR(255),
    user_id VARCHAR(255),
    service_name VARCHAR(100) NOT NULL,
    payload_json JSONB NOT NULL,
    metadata JSONB,
    event_time TIMESTAMPTZ NOT NULL,
    ingest_time TIMESTAMPTZ NOT NULL DEFAULT NOW()
) PARTITION BY RANGE (ingest_time);

CREATE TABLE IF NOT EXISTS audit_events_default PARTITION OF audit_events DEFAULT;

CREATE INDEX IF NOT EXISTS idx_audit_events_account_time ON audit_events(account_id, ingest_time DESC);
CREATE INDEX IF NOT EXISTS idx_audit_events_type_time ON audit_events(event_type, ingest_time DESC);
