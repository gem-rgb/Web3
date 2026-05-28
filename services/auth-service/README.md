# Auth Service

The auth service issues short-lived JWTs for broker-facing and operator-facing clients.

## Responsibilities

- Validate client credentials.
- Issue signed JWTs with tenant and role claims.
- Introspect existing tokens for administrative tooling.
- Provide health and metrics endpoints for platform automation.

## Runtime Interface

- HTTP: `8081`
- gRPC health: `50061`

## Environment

- `RMS_AUTH_SECRET`
- `RMS_AUTH_ISSUER`
- `RMS_AUTH_AUDIENCE`
- `RMS_AUTH_TTL`
- `RMS_AUTH_CLIENTS`

## Example Client Definition

```text
trading-platform:dev-secret:institutional:trader,allocator
ops-console:dev-secret:operations:admin
```

