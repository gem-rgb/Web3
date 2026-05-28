# Observability

The platform is built with observability as a runtime dependency, not an afterthought.

## Tracing

- Use W3C `traceparent` and `tracestate` style propagation.
- Every request gets a request ID and trace ID.
- HTTP and gRPC middleware attach the current trace context to the request.
- Spans should include service name, method/path, tenant ID, account ID, and order ID when available.

### Flow

```text
Client -> API Gateway -> Risk Service -> Position/Exposure/Margin/Rules
                   |                         |
                   +---- traceparent --------+
```

## Metrics

- Expose `/metrics` from every service.
- Track request rates, error rates, latency, queue lag, and business counters.
- Standard metrics:
  - `rms_http_requests_total`
  - `rms_http_request_duration_ms`
  - `rms_grpc_requests_total`
  - `rms_grpc_request_duration_ms`
  - `rms_orders_evaluated_total`
  - `rms_orders_rejected_total`
  - `rms_alerts_triggered_total`
  - `rms_kafka_consumer_lag`

## Logging

- Use structured JSON logs.
- Include service, environment, request ID, trace ID, tenant ID, account ID, and event ID.
- Avoid logging raw secrets or full account PII.

## Dashboards

Recommended Grafana dashboard groups:

- API and gateway health.
- Risk decision latency and rejection rates.
- Kafka consumer lag and DLQ volume.
- Postgres and Redis saturation.
- Alerting and compliance events.

## Alerts

Alert on:

- gateway unavailability,
- elevated risk rejection ratio,
- consumer lag,
- under-replicated partitions,
- error spikes,
- audit write failures,
- storage saturation,
- TLS certificate expiration.

## Runtime Wiring

- OpenTelemetry collector receives traces and metrics from the services.
- Prometheus scrapes `/metrics`.
- Jaeger stores or forwards traces for inspection.
- Structured logs stay local in phase 1, but the format is compatible with centralized ingestion.

