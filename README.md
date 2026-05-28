# Real-Time Risk Management System

Institutional-grade distributed real-time risk management platform with a phase 2 order ingestion and pre-trade risk pipeline.

## Canonical Layout

- `client/`: operator and supervisory UI surfaces
- `server/`: phase 2 backend services
- `shared/`: compatibility libraries used by the current runtime
- `proto/`: source-of-truth protobuf contracts
- `docs/`: architecture, Kafka, contract, and platform documentation

Legacy compatibility trees such as the top-level legacy `admin-dashboard/` and phase 1 service directories remain in the repo for migration continuity, but all canonical work is consolidated under `client/` and `server/`.

## Phase 2 Topology

```text
client/admin-dashboard
        |
        v
   API Gateway
        |
        v
Order Ingestion Service
        |
        +----------------------+
        |                      |
        v                      v
Risk Evaluation Service     Rule Engine Service
        |
        v
Kafka + Redis + Decision Store
```

## What Is Implemented

- Low-latency order ingestion with idempotency, correlation, and request tracing.
- Pre-trade risk evaluation with max size, buying power, leverage, market-hours, duplicate, frequency, restricted symbol, fat-finger, and account-status checks.
- Kafka consumer group processing with worker pools and DLQ handling.
- Redis-backed hot caches for decisions, account snapshots, market prices, idempotency, and frequency windows.
- Dynamic rule loading with hot reload, priorities, chaining, and tenant/account/symbol scoping.
- Structured logging, OpenTelemetry-style trace propagation, gRPC health, and Prometheus metrics endpoints.
- Canonical protobuf contracts in `proto/` plus a compatibility layer in `shared/proto`.

## Local Development

1. Copy `.env.example` to `.env` and adjust local values.
2. Start the stack:

```powershell
docker compose up -d --build
```

3. Open the operator dashboard at `http://localhost:3000`.
4. Call the API gateway at `http://localhost:8080`.
5. Review the phase 2 services under `server/`:
   - order ingestion on `:50062`
   - risk evaluation on `:50063`
   - rule engine on `:50064`

## Documentation

- [Phase 2 Risk Pipeline](docs/phase-2-risk-pipeline.md)
- [Architecture](docs/architecture.md)
- [Kafka Topics](docs/kafka/topics.md)
- [Kafka Event Schemas](docs/kafka/event-schemas.md)
- [Proto Definitions](proto/README.md)
- [Server Tree](server/README.md)

## Notes

- The repository is intentionally split between `client/` and `server/` to keep the runtime boundary obvious.
- The phase 2 backend code is in `server/`; the older top-level services are retained only for compatibility during migration.
- The order ingestion service is the canonical pre-trade entry point. The gateway fails closed if the ingestion path is unavailable.
