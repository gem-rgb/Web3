module "namespace" {
  source   = "./modules/namespace"
  name     = var.namespace
}

module "observability" {
  source                = "./modules/observability"
  namespace             = module.namespace.name
  grafana_admin_password = var.grafana_admin_password
}

