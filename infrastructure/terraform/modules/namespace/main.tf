resource "kubernetes_namespace_v1" "this" {
  metadata {
    name = var.name
    labels = {
      "app.kubernetes.io/name"       = "rms"
      "app.kubernetes.io/part-of"    = "rms"
      "app.kubernetes.io/managed-by" = "terraform"
    }
  }
}

resource "kubernetes_config_map_v1" "platform" {
  metadata {
    name      = "rms-platform-config"
    namespace = kubernetes_namespace_v1.this.metadata[0].name
  }

  data = {
    ENVIRONMENT       = "production"
    LOG_LEVEL         = "info"
    KAFKA_TOPIC_PREFIX = "rms"
    RMS_HTTP_PORT     = "8080"
  }
}

resource "kubernetes_secret_v1" "platform" {
  metadata {
    name      = "rms-platform-secrets"
    namespace = kubernetes_namespace_v1.this.metadata[0].name
  }

  type = "Opaque"

  data = {
    "auth-secret"       = base64encode("change-me")
    "postgres-password" = base64encode("change-me")
    "redis-password"    = base64encode("change-me")
  }
}
