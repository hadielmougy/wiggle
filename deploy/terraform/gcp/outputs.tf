output "project" {
  value = var.project
}

output "cluster_name" {
  value = google_container_cluster.this.name
}

output "kubeconfig_command" {
  description = "Run this to point kubectl at the cluster."
  value       = "gcloud container clusters get-credentials ${google_container_cluster.this.name} --region ${var.region} --project ${var.project}"
}

output "cloudsql_private_ip" {
  description = "Cloud SQL private IP (the JDBC host)."
  value       = google_sql_database_instance.this.private_ip_address
}

output "cloudsql_instance" {
  value = google_sql_database_instance.this.name
}

output "wiggle_service" {
  description = "In-cluster gRPC address of the wiggle control plane."
  value       = "${var.name}.${var.wiggle_namespace}.svc:8080"
}

output "database_password_secret" {
  value = kubernetes_secret.wiggle_jdbc.metadata[0].name
}
