# Shared Platform Libraries

This module contains the runtime building blocks used across the RMS monorepo.

## Packages

- `config`: environment, file-backed secrets, and endpoint resolution.
- `discovery`: Kubernetes DNS and service-address helpers.
- `grpcx`: gRPC server scaffolding, interceptors, and health wiring.
- `httpx`: HTTP middleware for request IDs, tracing, recovery, and security headers.
- `logging`: structured JSON logging with service metadata.
- `messaging`: Kafka producer/consumer wrappers, event envelopes, and idempotency helpers.
- `observability`: trace propagation and Prometheus-style metrics exposition.
- `security`: JWT signing/verification, request signing, mTLS, and token-bucket rate limiting.
- `storage`: Postgres and Redis DSN helpers for database-per-service deployments.

The root `shared/proto` module remains the canonical location for the hand-written proto-compatible Go types used by the current service demos.

