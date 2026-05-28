# Terraform

This directory contains the Terraform bootstrap layer for the RMS platform.

## Structure

- `modules/namespace`: namespace, platform config, and shared secret placeholders.
- `modules/observability`: Prometheus/Grafana/Jaeger bootstrap.
- `main.tf`: root composition of the platform modules.

The Terraform layer is intentionally focused on Kubernetes bootstrap and platform add-ons for phase 1.

