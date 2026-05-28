# Service Discovery

The primary discovery mechanism is Kubernetes DNS.

## Internal Naming

- Services resolve as `service.namespace.svc.cluster.local`.
- Stateful systems use headless services for stable network identities.
- The repo uses explicit environment variables as startup overrides for local development.

## Discovery Strategy

- API Gateway resolves downstream services through stable service names.
- Auth, risk, exposure, margin, position, rules, alerting, audit, and market data services all expose ClusterIP services.
- Kafka and NATS use stable broker endpoints or headless broker sets depending on deployment mode.

## Local Development

- Docker Compose uses service hostnames on the shared bridge network.
- Environment variables point to `localhost` endpoints when services run directly on the developer machine.

## Failure Handling

- If DNS resolution fails or a service is unavailable, callers should fall back to circuit-breaker or degraded-mode logic.
- Risk decisions should fail closed, not open.
- Non-critical read paths may fall back to cached state where policy allows it.

