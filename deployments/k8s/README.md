# Kubernetes Deployments

This directory is the canonical Kubernetes entrypoint for the RMS platform.

## Layout

- `kustomization.yaml`: production composition root.
- `infra.yaml`: namespace, config, secrets, and stateful dependencies.
- `auth-service.yaml`: the new auth service scaffold.
- `autoscaling.yaml`: HPA and disruption budget examples.
- `ingress.yaml`: edge routing for the gateway and dashboard.

The existing legacy manifests remain under `infrastructure/k8s/` and are still referenced here so the current demo services continue to work while the Phase 1 canonical layout is introduced.

