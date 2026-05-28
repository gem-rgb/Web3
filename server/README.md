# Server

Canonical phase-2 backend tree for the RMS platform.

## Layout

- `cmd/order-ingestion-service`: client-facing order intake, idempotency, correlation, and synchronous low-latency decisioning.
- `cmd/risk-evaluation-service`: Kafka-driven risk processing, decision persistence, and canonical decision publishing.
- `cmd/rule-engine-service`: dynamic rule catalog, hot reload, tenant/account overrides, and rule evaluation APIs.
- `internal/cache`: Redis-backed hot state and deduplication helpers.
- `internal/redisx`: lightweight RESP client with pipelining support.
- `internal/risk`: evaluation pipeline and decision orchestration.
- `internal/ruleengine`: rule catalog loading, scoping, and ordering.
- `internal/store`: durable decision persistence.
- `internal/worker`: bounded worker pool for Kafka message processing.
- `internal/retry`: bounded exponential retry primitives.

The canonical UI tree now lives under `client/`. The older `frontend/` and top-level legacy service directories remain for compatibility, but new work should target this tree.
