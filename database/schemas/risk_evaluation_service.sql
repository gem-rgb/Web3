-- Risk Evaluation Service Database Schema
-- Stores risk rules, evaluation caches, and related metadata

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Risk Rules Table
CREATE TABLE risk_rules (
    rule_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    rule_name VARCHAR(255) NOT NULL,
    rule_description TEXT,
    rule_type VARCHAR(50) NOT NULL, -- PRE_TRADE, POST_TRADE, MONITORING, ALERT
    severity VARCHAR(20) NOT NULL, -- LOW, MEDIUM, HIGH, CRITICAL
    enabled BOOLEAN DEFAULT TRUE,
    condition_expression TEXT NOT NULL, -- e.g., "order.quantity > 10000"
    parameters JSONB, -- rule-specific parameters
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    created_by VARCHAR(255),
    updated_by VARCHAR(255)
);

-- Indexes for risk rules
CREATE INDEX idx_risk_rules_type_enabled ON risk_rules(rule_type, enabled);
CREATE INDEX idx_risk_rules_severity ON risk_rules(severity);

-- Account Cache Table (for fast lookups)
CREATE TABLE account_cache (
    account_id VARCHAR(255) PRIMARY KEY,
    user_id VARCHAR(255),
    status VARCHAR(50), -- ACTIVE, SUSPENDED, CLOSED
    buying_power DECIMAL(20, 4),
    cash_balance DECIMAL(20, 4),
    market_value DECIMAL(20, 4),
    day_trading_buying_power DECIMAL(20, 4),
    maintenance_margin_excess DECIMAL(20, 4),
    metadata JSONB,
    last_updated TIMESTAMPTZ DEFAULT NOW()
);

-- Instrument Cache Table (for symbol metadata)
CREATE TABLE instrument_cache (
    symbol VARCHAR(20) PRIMARY KEY,
    exchange VARCHAR(50),
    currency VARCHAR(10),
    min_tick_size DECIMAL(10, 4),
    lot_size INTEGER,
    margin_requirement DECIMAL(5, 4), -- percentage
    metadata JSONB,
    last_updated TIMESTAMPTZ DEFAULT NOW()
);

-- Risk Evaluation Audit Table (for debugging and analysis)
CREATE TABLE risk_evaluation_audit (
    audit_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    order_id VARCHAR(255) NOT NULL,
    account_id VARCHAR(255) NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    quantity INTEGER NOT NULL,
    price DECIMAL(20, 4),
    side VARCHAR(10), -- BUY, SELL
    approved BOOLEAN NOT NULL,
    reject_reason TEXT,
    evaluated_rules JSONB, -- list of rule IDs that were evaluated
    violated_rules JSONB, -- list of rule IDs that were violated
    evaluation_latency_ms INTEGER, -- time taken to evaluate
    evaluated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for audit table
CREATE INDEX idx_risk_evaluation_audit_account ON risk_evaluation_audit(account_id);
CREATE INDEX idx_risk_evaluation_audit_symbol ON risk_evaluation_audit(symbol);
CREATE INDEX idx_risk_evaluation_audit_evaluated_at ON risk_evaluation_audit(evaluated_at);
CREATE INDEX idx_risk_evaluation_audit_approved ON risk_evaluation_audit(approved);