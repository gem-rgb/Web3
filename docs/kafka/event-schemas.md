# Kafka Event Schemas

This document defines the event envelopes used by the RMS Kafka pipeline.

## Phase 2 Event Schemas

The canonical phase 2 event contracts are:

| Topic | Payload |
| --- | --- |
| `rms.orders.received.v1` | `OrderReceivedEvent` |
| `rms.risk.evaluation.requested.v1` | `RiskEvaluationRequestedEvent` |
| `rms.risk.evaluations.completed.v1` | `RiskEvaluationCompletedEvent` |
| `rms.orders.approved.v1` | `OrderApprovedEvent` |
| `rms.orders.rejected.v1` | `OrderRejectedEvent` |
| `rms.risk.alerts.triggered.v1` | `RiskAlertTriggeredEvent` |

The full payload fields and flow semantics are documented in
[`../phase-2-risk-pipeline.md`](../phase-2-risk-pipeline.md).

### OrderReceivedEvent

```json
{
  "event_id": "ord-123-received",
  "order_id": "ord-123",
  "account_id": "acct-001",
  "tenant_id": "tenant-a",
  "correlation_id": "trace-id",
  "trace_id": "trace-id",
  "idempotency_key": "acct-001|AAPL|100|190.2500|BUY|LIMIT|DAY",
  "received_at": 1730000000000,
  "source": "order-ingestion-service",
  "order": {
    "order_id": "ord-123",
    "account_id": "acct-001",
    "symbol": "AAPL",
    "quantity": 100,
    "price": 190.25,
    "side": "BUY",
    "order_type": "LIMIT",
    "time_in_force": "DAY",
    "timestamp": 1730000000000,
    "strategy_id": "mean-reversion",
    "metadata": {
      "tenant_id": "tenant-a"
    }
  },
  "account": {
    "account_id": "acct-001",
    "user_id": "trader-77",
    "status": "ACTIVE",
    "buying_power": 500000.0,
    "cash_balance": 250000.0,
    "market_value": 180000.0,
    "day_trading_buying_power": 750000.0,
    "maintenance_margin_excess": 50000.0,
    "metadata": {
      "desk": "equities"
    }
  }
}
```

### RiskEvaluationRequestedEvent

```json
{
  "event_id": "ord-123-evaluation-requested",
  "order_id": "ord-123",
  "account_id": "acct-001",
  "tenant_id": "tenant-a",
  "correlation_id": "trace-id",
  "trace_id": "trace-id",
  "idempotency_key": "acct-001|AAPL|100|190.2500|BUY|LIMIT|DAY",
  "requested_at": 1730000000001,
  "reason": "pre-trade order ingestion",
  "order": {
    "order_id": "ord-123",
    "account_id": "acct-001",
    "symbol": "AAPL",
    "quantity": 100,
    "price": 190.25,
    "side": "BUY",
    "order_type": "LIMIT",
    "time_in_force": "DAY",
    "timestamp": 1730000000000,
    "strategy_id": "mean-reversion"
  },
  "account": {
    "account_id": "acct-001",
    "user_id": "trader-77",
    "status": "ACTIVE",
    "buying_power": 500000.0
  }
}
```

### RiskEvaluationCompletedEvent

```json
{
  "event_id": "ord-123-acct-001-AAPL-100-190.2500-BUY-LIMIT-DAY",
  "order_id": "ord-123",
  "account_id": "acct-001",
  "tenant_id": "tenant-a",
  "correlation_id": "trace-id",
  "trace_id": "trace-id",
  "completed_at": 1730000000012,
  "decision": {
    "decision_id": "ord-123-acct-001-AAPL-100-190.2500-BUY-LIMIT-DAY",
    "order_id": "ord-123",
    "account_id": "acct-001",
    "tenant_id": "tenant-a",
    "approved": true,
    "rule_version": "built-in-v1",
    "evaluated_at": 1730000000012,
    "latency_micros": 1287
  }
}
```

### OrderApprovedEvent

```json
{
  "event_id": "ord-123-acct-001-AAPL-100-190.2500-BUY-LIMIT-DAY",
  "order_id": "ord-123",
  "account_id": "acct-001",
  "tenant_id": "tenant-a",
  "correlation_id": "trace-id",
  "trace_id": "trace-id",
  "approved_at": 1730000000012,
  "decision": {
    "decision_id": "ord-123-acct-001-AAPL-100-190.2500-BUY-LIMIT-DAY",
    "approved": true,
    "rule_version": "built-in-v1"
  }
}
```

### OrderRejectedEvent

```json
{
  "event_id": "ord-456-acct-001-AAPL-1000-190.2500-BUY-LIMIT-DAY",
  "order_id": "ord-456",
  "account_id": "acct-001",
  "tenant_id": "tenant-a",
  "correlation_id": "trace-id",
  "trace_id": "trace-id",
  "rejected_at": 1730000000012,
  "decision": {
    "decision_id": "ord-456-acct-001-AAPL-1000-190.2500-BUY-LIMIT-DAY",
    "approved": false,
    "reject_reason": "buying-power, leverage",
    "violations": [
      {
        "rule_id": "buying-power",
        "rule_description": "Order notional exceeds buying power",
        "severity": "CRITICAL"
      }
    ]
  }
}
```

### RiskAlertTriggeredEvent

```json
{
  "event_id": "ord-456-acct-001-AAPL-1000-190.2500-BUY-LIMIT-DAY",
  "alert_id": "alert-ord-456",
  "order_id": "ord-456",
  "account_id": "acct-001",
  "tenant_id": "tenant-a",
  "correlation_id": "trace-id",
  "trace_id": "trace-id",
  "rule_id": "buying-power",
  "severity": "CRITICAL",
  "message": "Order ord-456 rejected: buying-power",
  "triggered_at": 1730000000012,
  "metadata": {
    "symbol": "AAPL"
  }
}
```

## `rms.orders.evaluated.v1`

Produced by the risk evaluation service for every order decision.

```json
{
  "event_id": "ord-123-order-evaluated",
  "event_type": "order.evaluated",
  "aggregate_id": "ord-123",
  "tenant_id": "tenant-a",
  "correlation_id": "trace-id",
  "trace_id": "trace-id",
  "schema_version": "v1",
  "occurred_at": "2026-05-26T00:00:00Z",
  "headers": {
    "source": "risk-evaluation-service",
    "service": "risk-evaluation-service"
  },
  "data": {
    "order": {
      "order_id": "ord-123",
      "account_id": "acct-001",
      "symbol": "AAPL",
      "quantity": 100,
      "price": 190.25,
      "side": "BUY",
      "order_type": "LIMIT",
      "time_in_force": "DAY",
      "timestamp": 1730000000000,
      "strategy_id": "mean-reversion",
      "metadata": {}
    },
    "approved": true,
    "reject_reason": "",
    "violations": [],
    "timestamp": 1730000000123
  }
}
```

## `rms.orders.rejected.v1`

Produced when the risk engine rejects an order.

```json
{
  "event_id": "ord-123-order-rejected",
  "event_type": "order.rejected",
  "aggregate_id": "ord-123",
  "tenant_id": "tenant-a",
  "correlation_id": "trace-id",
  "trace_id": "trace-id",
  "schema_version": "v1",
  "occurred_at": "2026-05-26T00:00:00Z",
  "headers": {
    "source": "risk-evaluation-service",
    "service": "risk-evaluation-service"
  },
  "data": {
    "order": { "...": "same schema as above" },
    "approved": false,
    "reject_reason": "buying-power, leverage",
    "violations": [
      {
        "rule_id": "buying-power",
        "rule_description": "Order notional exceeds buying power",
        "severity": "CRITICAL"
      }
    ],
    "timestamp": 1730000000123
  }
}
```

## `rms.marketdata.tick.v1`

Produced by the market data consumer after feed normalization.

```json
{
  "event_id": "AAPL-1730000000456",
  "event_type": "marketdata.tick",
  "aggregate_id": "AAPL",
  "tenant_id": "global",
  "correlation_id": "trace-id",
  "trace_id": "trace-id",
  "schema_version": "v1",
  "occurred_at": "2026-05-26T00:00:00Z",
  "headers": {
    "source": "market-data-consumer",
    "service": "market-data-consumer"
  },
  "data": {
    "symbol": "AAPL",
    "bid_price": 190.20,
    "ask_price": 190.30,
    "last_price": 190.25,
    "volume": 1042000,
    "timestamp": 1730000000456
  }
}
```

## `rms.positions.updated.v1`

Produced by position tracking when an account position changes.

```json
{
  "event_id": "acct-001-AAPL-position-updated",
  "event_type": "position.updated",
  "aggregate_id": "acct-001:AAPL",
  "tenant_id": "tenant-a",
  "correlation_id": "trace-id",
  "trace_id": "trace-id",
  "schema_version": "v1",
  "occurred_at": "2026-05-26T00:00:00Z",
  "headers": {
    "source": "position-tracking-service",
    "service": "position-tracking-service"
  },
  "data": {
    "account_id": "acct-001",
    "symbol": "AAPL",
    "quantity": 250,
    "average_price": 189.75,
    "market_value": 47437.5,
    "cost_basis": 47437.5,
    "unrealized_pl": 0,
    "realized_pl": 0,
    "side": "LONG",
    "timestamp": 1730000000789
  }
}
```

## `rms.alerts.triggered.v1`

Produced by the alerting service for threshold breaches and suspicious activity.

```json
{
  "event_id": "alert-123-alert-triggered",
  "event_type": "alert.triggered",
  "aggregate_id": "alert-123",
  "tenant_id": "tenant-a",
  "correlation_id": "trace-id",
  "trace_id": "trace-id",
  "schema_version": "v1",
  "occurred_at": "2026-05-26T00:00:00Z",
  "headers": {
    "source": "alerting-service",
    "service": "alerting-service"
  },
  "data": {
    "alert_id": "alert-123",
    "account_id": "acct-001",
    "rule_id": "buying-power",
    "severity": "CRITICAL",
    "message": "Order ord-123 rejected: buying-power",
    "timestamp": 1730000000901,
    "metadata": {
      "symbol": "AAPL",
      "order_id": "ord-123"
    }
  }
}
```

## `rms.audit.logged.v1`

Produced by the audit logging service or mirrored into Kafka for compliance pipelines.

```json
{
  "event_id": "audit-123",
  "event_type": "audit.logged",
  "aggregate_id": "audit-123",
  "tenant_id": "tenant-a",
  "correlation_id": "trace-id",
  "trace_id": "trace-id",
  "schema_version": "v1",
  "occurred_at": "2026-05-26T00:00:00Z",
  "headers": {
    "source": "audit-logging-service",
    "service": "audit-logging-service"
  },
  "data": {
    "event_id": "audit-123",
    "event_type": "ORDER_EVALUATED",
    "account_id": "acct-001",
    "user_id": "trader-77",
    "service_name": "risk-evaluation-service",
    "payload_json": "{\"approved\":true}",
    "timestamp": 1730000000999,
    "ingest_timestamp": 1730000001001,
    "metadata": {
      "symbol": "AAPL"
    }
  }
}
```
