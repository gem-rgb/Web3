resource "kubernetes_config_map_v1" "otel_collector" {
  metadata {
    name      = "rms-otel-collector-config"
    namespace = var.namespace
  }

  data = {
    "config.yaml" = <<-EOT
    receivers:
      otlp:
        protocols:
          grpc: {}
          http: {}
    processors:
      batch: {}
    exporters:
      debug: {}
      jaeger:
        endpoint: rms-jaeger:14250
        tls:
          insecure: true
      prometheus:
        endpoint: 0.0.0.0:8889
    service:
      pipelines:
        traces:
          receivers: [otlp]
          processors: [batch]
          exporters: [jaeger, debug]
        metrics:
          receivers: [otlp]
          processors: [batch]
          exporters: [prometheus, debug]
    EOT
  }
}

resource "helm_release" "observability" {
  name             = "rms-observability"
  repository       = "https://prometheus-community.github.io/helm-charts"
  chart            = "kube-prometheus-stack"
  namespace        = var.namespace
  create_namespace = false

  values = [
    yamlencode({
      grafana = {
        enabled       = true
        adminPassword = var.grafana_admin_password
      }
      prometheus = {
        prometheusSpec = {
          retention = "15d"
        }
      }
      alertmanager = {
        enabled = true
      }
    })
  ]
}

resource "helm_release" "jaeger" {
  name       = "rms-jaeger"
  repository = "https://jaegertracing.github.io/helm-charts"
  chart      = "jaeger"
  namespace  = var.namespace

  values = [
    yamlencode({
      collector = {
        otlp = {
          enabled = true
        }
      }
    })
  ]
}

