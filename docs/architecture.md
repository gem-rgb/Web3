# RMS Architecture

This file keeps the phase 1 platform overview for historical context.
The canonical phase 2 order ingestion and risk pipeline is documented in
[`phase-2-risk-pipeline.md`](phase-2-risk-pipeline.md).

## Design Goals

- Pre-trade risk evaluation at low latency.
- Event-driven service orchestration with Kafka and NATS.
- Database-per-service ownership boundaries.
- gRPC for synchronous internal calls.
- REST for external clients, operators, and browser tooling.
- Observability-first operations with traces, metrics, logs, and health endpoints.
- mTLS, JWT, tenant isolation, and audit logging as first-class controls.

## Topology

```text
                     External Clients / OMS / UI
                                 |
                                 v
                      +----------------------+
                      |     API Gateway      |
                      |  REST + gRPC edge     |
                      +----------+-----------+
                                 |
                +----------------+----------------+
                |                                 |
                v                                 v
        +---------------+                 +----------------+
        |  Auth Service  |                 |  Risk Service  |
        |  JWT issuance  |                 |  pre-trade     |
        +-------+-------+                  |  risk checks   |
                |                          +-------+--------+
                |                                  |
                |                                  v
                |                          Kafka / NATS backbone
                |                                  |
                +----------------+-----------------+----------------+
                                 |                                  |
                                 v                                  v
                    +----------------------+          +-----------------------+
                    | Position Service     |          | Market Data Consumer  |
                    | canonical positions   |          | feed normalization    |
                    +----------+-----------+          +-----------+-----------+
                               |                                  |
                               v                                  v
                    +----------------------+          +-----------------------+
                    | Exposure Service     |          | Margin Engine         |
                    | aggregated exposure   |          | margin calculations   |
                    +----------+-----------+          +-----------+-----------+
                               |                                  |
                               +----------------+-----------------+
                                                |
                                                v
                                    +----------------------+
                                    |  Rule Engine         |
                                    |  policy evaluation    |
                                    +----------+-----------+
                                               |
                                               v
                     +-------------------+  +--------------------+  +-------------------+
                     | Alerting Service   |  | Audit Logging      |  | Observability     |
                     | on-call routing    |  | immutable trail    |  | OTel/Prom/Grafana |
                     +-------------------+  +--------------------+  +-------------------+
```

## Service Boundaries

- `API Gateway`: external entry point, request signing, rate limiting, request routing, tenant-aware headers.
- `Auth Service`: token issuance, introspection, client credential validation, operator roles.
- `Risk Service`: pre-trade checks, kill switches, order throttling, duplicate detection, compliance gates.
- `Position Service`: canonical positions and position event lifecycle.
- `Exposure Service`: aggregated exposure by account, symbol, desk, and tenant.
- `Margin Engine`: initial and maintenance margin calculations and projected utilization.
- `Rule Engine`: dynamic rule definitions, versioning, and rollout control.
- `Alerting Service`: notifications, escalation, deduplication, and alert history.
- `Audit Logging Service`: immutable compliance ledger and forensic query path.
- `Market Data Consumer`: normalization, replay, and market data fan-out.

## Data Ownership

The architecture follows database-per-service ownership:

- PostgreSQL owns durable service-specific state.
- Redis owns hot lookup state, rate limiting, deduplication, and short-lived caches.
- Kafka owns durable event history and replayability.
- NATS handles low-latency ephemeral coordination and fan-out where durability is not required.

Services do not directly read each other's databases.
Cross-service state is exchanged via events or gRPC APIs.

## Synchronous And Asynchronous Paths

- Synchronous:
  - API Gateway -> Risk Service
  - Risk Service -> Rule Service / Position Service / Exposure Service / Margin Engine
  - Operator tooling -> Auth Service
- Asynchronous:
  - Orders, exposures, positions, alerts, market data, and audit events flow on Kafka topics.
  - DLQ topics capture failed messages for investigation and replay.
  - Consumers are idempotent and can replay from offsets without duplicate side effects.

## Operational Guarantees

- Rolling deployments with readiness/liveness probes.
- Pod anti-affinity for HA across nodes.
- HPA for stateless services based on CPU and queue depth.
- StatefulSets for Postgres, Redis, and Kafka.
- Network policies and mTLS for service-to-service segmentation.
- Tracing, metrics, and structured logs on every request path.

## Local Development

The root `docker-compose.yml` starts the local dependency stack and the core services.
The `client/admin-dashboard` workspace provides the operator console.

## Phase 1 Scope

This phase establishes the platform foundation and representative service scaffolding.
Subsequent phases should add:

- real persistence logic and migrations,
- schema registry / contract generation,
- external secret backends,
- stream processing jobs,
- multi-region failover,
- more rigorous policy engines and analytics.
