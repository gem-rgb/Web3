# Shared

Shared code in this repository is split into two modules:

- `shared/proto`: the current hand-written Go proto compatibility layer used by the existing service demos.
- `shared/platform`: the reusable runtime foundation for the Phase 1 monorepo, including config, discovery, logging, observability, security, messaging, gRPC, HTTP, and storage helpers.

The goal of this layout is to keep service implementations thin while moving policy, middleware, and infrastructure-aware code into shared libraries.

