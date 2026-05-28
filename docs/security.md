# Security

The security model assumes a zero-trust internal network.

## Authentication

- External clients authenticate at the auth service.
- The auth service issues short-lived JWTs.
- The API gateway validates JWTs before forwarding requests.
- Internal services can also validate JWTs for privileged paths.

## Authorization

- Roles are encoded in JWT claims.
- RBAC is enforced at the gateway and in operator-facing services.
- Tenant IDs are part of the token and part of all service-level access checks.

## mTLS

- Service-to-service traffic should use mTLS.
- Each service gets a unique certificate and identity.
- The root CA is rotated through secret management, not embedded in images.
- Phase 1 includes the TLS helper scaffolding, but the local Compose stack still defaults to plaintext gRPC until cert files are mounted.

## Request Signing

- Sensitive commands and internal callbacks should be signed with an HMAC request signature.
- Request signatures protect against replay and tampering in transit.
- Timestamps are validated with a small clock-skew window.

## Rate Limiting

- Gateway rate limiting protects the risk engine during market open spikes.
- Auth service throttles token issuance.
- Downstream services can apply per-tenant or per-account token buckets.

## Secrets Management

- Do not store production secrets in source control.
- Use mounted secrets, Vault, External Secrets, or cloud-native secret managers.
- The repo includes environment templates and secret placeholders only.

## Audit Logging

- Every security-relevant action must emit an audit event.
- Audit events include identity, tenant, account, action, timestamp, and outcome.
- Audit data is immutable and retained according to policy.

## Tenant Isolation

- Tenant ID is propagated through headers, JWT claims, and Kafka envelopes.
- Storage keys include tenant ID where appropriate.
- Shared infrastructure does not imply shared authorization context.
