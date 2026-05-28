# RMS Observability

This directory holds the local and Kubernetes-facing observability scaffolding for the RMS stack.

## Contents

- `prometheus/prometheus.yml` for scrape targets and rule loading.
- `prometheus/rms-alerts.yml` for risk-oriented alert rules.
- `grafana/provisioning/datasources/datasource.yml` for Prometheus wiring.
- `otel-collector/config.yaml` for OpenTelemetry ingestion and export.
- `jaeger/values.yaml` for a simple Jaeger all-in-one deployment.
- [Observability Stack Manifest](../infrastructure/k8s/observability-stack.yaml) for the collector and trace backend deployment scaffolding.

## Notes

- The Go services in this repo currently expose a mix of gRPC health and REST endpoints. The gateway exposes the primary `/metrics` endpoint today.
- The Prometheus scrape config is intentionally conservative so it can be mounted into either a local compose workflow or a Kubernetes deployment.
- If you deploy Grafana or Jaeger with a different Helm chart release name, adjust the datasource endpoint and collector service names accordingly.
