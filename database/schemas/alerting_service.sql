-- Alerting Service schema
-- Stores alerts, subscriptions, and delivery state.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS alerts (
    alert_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    account_id VARCHAR(255),
    rule_id VARCHAR(255),
    severity VARCHAR(20) NOT NULL,
    message TEXT NOT NULL,
    metadata JSONB,
    status VARCHAR(20) NOT NULL DEFAULT 'OPEN',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    acknowledged_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS alert_subscriptions (
    subscription_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    account_id VARCHAR(255),
    rule_id VARCHAR(255),
    channel VARCHAR(50) NOT NULL,
    destination TEXT NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_alerts_account_created ON alerts(account_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_rule_created ON alerts(rule_id, created_at DESC);
