resource "kubernetes_namespace" "wiggle" {
  metadata {
    name = var.wiggle_namespace
  }
}

# The JDBC credentials wiggle connects with, as a Secret the Helm chart consumes via
# storage.jdbc.existingSecret (keys: url / user / password). Keeping it here (not in Helm values)
# keeps the password out of the Helm release and lets Terraform own the one source of truth.
resource "kubernetes_secret" "wiggle_jdbc" {
  metadata {
    name      = "${var.name}-jdbc"
    namespace = kubernetes_namespace.wiggle.metadata[0].name
  }
  data = {
    url      = "jdbc:postgresql://${module.aurora.cluster_endpoint}:5432/${var.database_name}"
    user     = var.database_user
    password = random_password.db.result
  }
  type = "Opaque"
}

locals {
  wiggle_values = yamlencode({
    replicaCount = var.wiggle_replicas
    image = merge({
      registry   = var.wiggle_image_registry
      repository = var.wiggle_image_repository
    }, var.wiggle_image_tag != "" ? { tag = var.wiggle_image_tag } : {})
    storage = {
      jdbc = {
        existingSecret = kubernetes_secret.wiggle_jdbc.metadata[0].name
      }
    }
  })
}

resource "helm_release" "wiggle" {
  name      = var.name
  namespace = kubernetes_namespace.wiggle.metadata[0].name
  chart     = var.wiggle_chart
  version   = var.wiggle_chart_version

  # Base values (image + replicas + the JDBC secret), then any advanced overrides last.
  values = compact([local.wiggle_values, var.wiggle_values])

  depends_on = [module.aurora, kubernetes_secret.wiggle_jdbc]
}
