# Engineering Rationale

## Why This Shape

- **Database-per-service** keeps ownership boundaries clear and prevents cross-service coupling through shared tables.
- **Kafka** provides durable ordered event history, replay, and auditability for trading and risk flows.
- **NATS** is reserved for low-latency ephemeral fan-out where durability is not the primary requirement.
- **gRPC** keeps internal synchronous calls efficient and strongly typed.
- **REST** remains the external edge for browser and operator tooling.
- **JWT + mTLS + request signing** gives the platform a layered security model instead of relying on a single gate.
- **OpenTelemetry-compatible context propagation** allows tracing and metrics to become first-class runtime data.
- **Kubernetes StatefulSets** are appropriate for Postgres, Redis, Kafka, and NATS broker state.
- **Terraform** bootstraps the platform primitives and keeps the cluster add-ons reproducible.

## Phase 1 Boundary

Phase 1 builds the platform skeleton and service boundaries.
The next phases should focus on:

- durable persistence and migrations,
- event consumers and outbox processing,
- risk rule evaluation engine depth,
- exact market data adapters,
- multi-region and DR strategy,
- stronger secret rotation automation.

