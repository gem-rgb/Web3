# Services

This directory is the legacy phase 1 service catalog for the RMS monorepo.

The canonical phase 2 backend tree now lives under [`server/`](../server/README.md).

## Current Coverage

- `auth-service`: JWT issuance and token introspection.
- `api-gateway`: external edge entry point and request orchestration.
- `risk-evaluation-service`: pre-trade risk controls.
- `exposure-aggregation-service`: aggregate exposures by account and symbol.
- `margin-calculation-engine`: initial and maintenance margin calculations.
- `position-tracking-service`: canonical position state and trade lifecycle.
- `rule-engine-service`: dynamic policy evaluation and versioning.
- `alerting-service`: event-driven alert routing.
- `audit-logging-service`: immutable audit trail and compliance logging.
- `market-data-consumer`: feed normalization and market data fan-out.

The root-level service directories remain available for migration continuity, but new backend work should target `server/`.
