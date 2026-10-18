output "region" {
  value = var.region
}

output "cluster_name" {
  description = "EKS cluster name."
  value       = module.eks.cluster_name
}

output "kubeconfig_command" {
  description = "Run this to point kubectl at the cluster."
  value       = "aws eks update-kubeconfig --name ${module.eks.cluster_name} --region ${var.region}"
}

output "aurora_endpoint" {
  description = "Aurora writer endpoint (the JDBC host)."
  value       = module.aurora.cluster_endpoint
}

output "aurora_reader_endpoint" {
  description = "Aurora reader endpoint."
  value       = module.aurora.cluster_reader_endpoint
}

output "wiggle_service" {
  description = "In-cluster gRPC address of the wiggle control plane (port-forward or expose to reach it)."
  value       = "${var.name}.${var.wiggle_namespace}.svc:8080"
}

output "database_password_secret" {
  description = "Kubernetes Secret holding the JDBC credentials."
  value       = kubernetes_secret.wiggle_jdbc.metadata[0].name
}
