resource "random_password" "db" {
  length           = 24
  special          = true
  override_special = "!#%*_-=+"
}

resource "google_sql_database_instance" "this" {
  name             = "${var.name}-pg"
  region           = var.region
  database_version = var.postgres_version

  deletion_protection = false # convenient for demos; enable where data matters

  # The private IP lives in the peered range, so this must wait for the PSA connection.
  depends_on = [google_service_networking_connection.psa]

  settings {
    tier              = var.db_tier
    availability_type = var.db_ha ? "REGIONAL" : "ZONAL"
    disk_autoresize   = true

    ip_configuration {
      ipv4_enabled    = false
      private_network = google_compute_network.vpc.id
    }

    backup_configuration {
      enabled = true
    }
  }
}

resource "google_sql_database" "wiggle" {
  name     = var.database_name
  instance = google_sql_database_instance.this.name
}

resource "google_sql_user" "wiggle" {
  name     = var.database_user
  instance = google_sql_database_instance.this.name
  password = random_password.db.result
}
