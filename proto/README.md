# Proto Definitions

This directory is the canonical source of service contracts for the RMS platform.

## Layout

- `common/v1/common.proto`: shared broker, account, position, margin, alert, and audit messages.
- `auth/v1/auth.proto`: token issuance and introspection.
- `gateway/v1/gateway.proto`: external gateway command surface.
- `risk/v1/risk.proto`: risk evaluation APIs.
- `risk/v1/risk_pipeline.proto`: phase 2 risk pipeline APIs.
- `marketdata/v1/marketdata.proto`: market data fan-out APIs.
- `audit/v1/audit.proto`: audit logging APIs.
- `orders/v1/orders.proto`: phase 2 order ingestion APIs.
- `rules/v1/rules.proto`: dynamic rule catalog and reload APIs.

The current Go services still rely on the hand-written compatibility layer under `shared/proto`.
That layer exists so phase 1 can remain runnable while the generated contract pipeline is introduced.
When code generation is turned on, the output should land under `generated/go/`, not over the compatibility layer.
