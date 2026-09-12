variable "project" {
  description = "GCP project id."
  type        = string
}

variable "region" {
  description = "GCP region (a regional GKE cluster + Cloud SQL live here)."
  type        = string
  default     = "europe-west1"
}

variable "name" {
  description = "Name prefix for all resources."
  type        = string
  default     = "wiggle"
}

# --- networking (VPC-native GKE + Cloud SQL private IP) ---

variable "subnet_cidr" {
  description = "Primary node subnet CIDR."
  type        = string
  default     = "10.42.0.0/20"
}

variable "pods_cidr" {
  description = "Secondary range for GKE pods."
  type        = string
  default     = "10.44.0.0/14"
}

variable "services_cidr" {
  description = "Secondary range for GKE services."
  type        = string
  default     = "10.48.0.0/20"
}

variable "master_cidr" {
  description = "The /28 for the private GKE control plane."
  type        = string
  default     = "172.16.0.0/28"
}

# --- GKE ---

variable "gke_release_channel" {
  description = "GKE release channel (RAPID / REGULAR / STABLE)."
  type        = string
  default     = "REGULAR"
}

variable "node_machine_type" {
  type    = string
  default = "e2-standard-4"
}

variable "node_count" {
  description = "Nodes PER ZONE (a regional cluster spans the region's zones)."
  type        = number
  default     = 1
}

variable "node_min_count" {
  type    = number
  default = 1
}

variable "node_max_count" {
  type    = number
  default = 3
}

# --- Cloud SQL PostgreSQL ---

variable "postgres_version" {
  description = "Cloud SQL PostgreSQL version, e.g. POSTGRES_16."
  type        = string
  default     = "POSTGRES_16"
}

variable "db_tier" {
  description = "Cloud SQL machine tier."
  type        = string
  default     = "db-custom-2-8192"
}

variable "db_ha" {
  description = "REGIONAL (HA) vs ZONAL availability for Cloud SQL."
  type        = bool
  default     = true
}

variable "database_name" {
  type    = string
  default = "wiggle"
}

variable "database_user" {
  type    = string
  default = "wiggle"
}

# --- wiggle (Helm) ---

variable "wiggle_namespace" {
  type    = string
  default = "wiggle"
}

variable "wiggle_chart" {
  description = "Path or repo reference to the wiggle Helm chart. Defaults to the chart vendored in this repo."
  type        = string
  default     = "../../helm/wiggle"
}

variable "wiggle_chart_version" {
  type    = string
  default = null
}

variable "wiggle_image_registry" {
  type    = string
  default = "ghcr.io"
}

variable "wiggle_image_repository" {
  type    = string
  default = "hadielmougy/wiggle"
}

variable "wiggle_image_tag" {
  type    = string
  default = ""
}

variable "wiggle_replicas" {
  type    = number
  default = 3
}

variable "wiggle_values" {
  description = "Extra Helm values (YAML) merged last."
  type        = string
  default     = ""
}
