variable "kubeconfig_path" {
  type        = string
  description = "Path to the kubeconfig used for the target cluster."
  default     = "~/.kube/config"
}

variable "namespace" {
  type        = string
  description = "Primary RMS namespace."
  default     = "rms"
}

variable "grafana_admin_password" {
  type        = string
  description = "Grafana admin password."
  sensitive   = true
  default     = "change-me"
}

