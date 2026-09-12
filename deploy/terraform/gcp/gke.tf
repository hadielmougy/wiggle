resource "google_container_cluster" "this" {
  name     = "${var.name}-gke"
  location = var.region # regional cluster (control plane + nodes across the region's zones)

  network    = google_compute_network.vpc.id
  subnetwork = google_compute_subnetwork.subnet.id

  # Managed via a separate node pool.
  remove_default_node_pool = true
  initial_node_count       = 1

  networking_mode = "VPC_NATIVE"
  ip_allocation_policy {
    cluster_secondary_range_name  = "pods"
    services_secondary_range_name = "services"
  }

  private_cluster_config {
    enable_private_nodes    = true
    enable_private_endpoint = false # public control-plane endpoint (restrict via authorized networks for prod)
    master_ipv4_cidr_block  = var.master_cidr
  }

  release_channel {
    channel = var.gke_release_channel
  }

  deletion_protection = false # convenient for demos; enable where the cluster matters

  depends_on = [google_project_service.this]
}

resource "google_container_node_pool" "default" {
  name     = "default"
  cluster  = google_container_cluster.this.id
  location = var.region

  # Per-zone; a regional cluster multiplies this across the region's zones.
  initial_node_count = var.node_count

  autoscaling {
    min_node_count = var.node_min_count
    max_node_count = var.node_max_count
  }

  node_config {
    machine_type = var.node_machine_type
    oauth_scopes = ["https://www.googleapis.com/auth/cloud-platform"]

    workload_metadata_config {
      mode = "GKE_METADATA"
    }
  }

  management {
    auto_repair  = true
    auto_upgrade = true
  }
}
