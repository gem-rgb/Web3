# Kafka Topic Design

This platform uses Kafka as the durable event backbone for trading, risk, and compliance workflows.

## Naming Convention

Use the pattern:

```text
rms.<domain>.<event>.v1
```

Examples:

- `rms.orders.received.v1`
- `rms.risk.evaluation.requested.v1`
- `rms.risk.evaluations.completed.v1`
- `rms.orders.approved.v1`
- `rms.risk.alerts.triggered.v1`
- `rms.alerts.triggered.v1`
- `rms.audit.logged.v1`
- `rms.marketdata.tick.v1`
- `rms.rules.changed.v1`

## Topic Categories

| Topic | Purpose | Key | Cleanup | Retention |
| --- | --- | --- | --- | --- |
| `rms.orders.received.v1` | order ingress from gateway | `tenant_id|account_id|order_id` | delete | 24h |
| `rms.risk.evaluation.requested.v1` | queued pre-trade evaluation work | `tenant_id|account_id|order_id` | delete | 24h |
| `rms.risk.evaluations.completed.v1` | terminal risk evaluation lifecycle event | `tenant_id|account_id|order_id` | delete | 7d |
| `rms.orders.approved.v1` | approved order stream | `tenant_id|account_id|order_id` | delete | 7d |
| `rms.orders.rejected.v1` | rejected order stream | `tenant_id|account_id|order_id` | delete | 30d |
| `rms.risk.alerts.triggered.v1` | high-severity risk alerts | `tenant_id|account_id|alert_id` | delete | 30d |
| `rms.positions.updated.v1` | canonical position state changes | `tenant_id|account_id|symbol` | compact | 30d |
| `rms.exposures.updated.v1` | exposure snapshots | `tenant_id|account_id|symbol` | compact | 7d |
| `rms.margin.updated.v1` | margin utilization snapshots | `tenant_id|account_id` | compact | 7d |
| `rms.marketdata.tick.v1` | normalized market data | `symbol` | delete | 1h |
| `rms.rules.changed.v1` | rule changes and rollouts | `tenant_id|rule_id` | compact | 30d |
| `rms.alerts.triggered.v1` | alert events | `tenant_id|account_id|alert_id` | delete | 30d |
| `rms.audit.logged.v1` | compliance/audit ledger | `tenant_id|event_id` | delete | 7y |
| `rms.orders.received.v1.dlq` | dead-letter stream for failed order events | `tenant_id|account_id|order_id` | delete | 30d |

## Partitioning Strategy

- High-throughput topics use 12 to 24 partitions.
- Order and position topics are partitioned by account to preserve ordering where it matters.
- Market data is partitioned by symbol.
- Rules, alerts, and audit are lower-volume and can start at 3 to 6 partitions.
- The partition key must be stable and deterministic.

## Retention And Cleanup

- `delete` for transient event streams and operational commands.
- `compact` for current-state topics such as positions, exposures, margin, and rules.
- Long-term compliance topics retain data for regulatory windows and archive to downstream storage.

## Dead Letter Queues

Every critical topic gets a DLQ topic:

```text
rms.<original-topic>.dlq
```

DLQ messages should contain:

- original topic and partition,
- offset and key,
- failure reason,
- service name,
- trace ID,
- replay eligibility flag,
- original payload and envelope.

DLQ retention is longer than the source topic so operations can investigate and replay.

## Replay Strategy

- Replays are always explicit.
- Operators use a replay consumer group against the source or replay topic.
- Replayed messages must preserve the original event ID and correlation ID.
- Consumers must be idempotent and tolerate at-least-once delivery.

## Idempotency Design

- Every event has an `event_id`.
- Business commands have an `idempotency_key`.
- Producers attach a stable aggregate key.
- Consumers record processed event IDs in Redis or a compacted state table.
- Risk and audit flows must never produce side effects twice for the same event ID.

## Recommended Event Envelope

```json
{
  "event_id": "uuid",
  "event_type": "order.evaluated",
  "aggregate_id": "order-123",
  "tenant_id": "tenant-a",
  "correlation_id": "trace-id",
  "trace_id": "trace-id",
  "schema_version": "v1",
  "occurred_at": "2026-05-26T00:00:00Z",
  "headers": {
    "source": "api-gateway",
    "service": "risk-service"
  }
}
```

## Operational Controls

- Idempotent producers.
- Consumer lag alerting.
- Under-replicated partition alerting.
- Producer and consumer retry policies with bounded backoff.
- Schema evolution through additive changes only.
