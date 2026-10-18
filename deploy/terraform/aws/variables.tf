variable "region" {
  description = "AWS region to deploy into."
  type        = string
  default     = "eu-west-1"
}

variable "name" {
  description = "Name prefix for all resources (cluster, VPC, Aurora, ...)."
  type        = string
  default     = "wiggle"
}

variable "tags" {
  description = "Tags applied to every resource."
  type        = map(string)
  default     = {}
}

# --- networking ---------------------------------------------------------------

variable "vpc_cidr" {
  description = "CIDR for the created VPC."
  type        = string
  default     = "10.42.0.0/16"
}

variable "az_count" {
  description = "Number of availability zones (>= 2 for Aurora + EKS HA)."
  type        = number
  default     = 3
}

# --- EKS ----------------------------------------------------------------------

variable "kubernetes_version" {
  description = "EKS control-plane / node Kubernetes version."
  type        = string
  default     = "1.30"
}

variable "node_instance_types" {
  description = "Instance types for the managed node group."
  type        = list(string)
  default     = ["m6i.large"]
}

variable "node_min_size" {
  type    = number
  default = 2
}

variable "node_max_size" {
  type    = number
  default = 5
}

variable "node_desired_size" {
  type    = number
  default = 3
}

variable "cluster_public_access" {
  description = "Expose the EKS API endpoint publicly (keep false + use a bastion/VPN for production)."
  type        = bool
  default     = true
}

# --- Aurora PostgreSQL --------------------------------------------------------

variable "aurora_engine_version" {
  description = "Aurora PostgreSQL engine version (must be one AWS offers in the region)."
  type        = string
  default     = "16.4"
}

variable "aurora_instance_class" {
  description = "Instance class for the Aurora writer/readers."
  type        = string
  default     = "db.r6g.large"
}

variable "aurora_instance_count" {
  description = "Number of Aurora instances (1 writer + N-1 readers)."
  type        = number
  default     = 2
}

variable "database_name" {
  description = "Initial database created in the Aurora cluster (wiggle's schema lives here)."
  type        = string
  default     = "wiggle"
}

variable "database_user" {
  description = "Aurora master username wiggle connects as."
  type        = string
  default     = "wiggle"
}

# --- wiggle (Helm) ------------------------------------------------------------

variable "wiggle_namespace" {
  description = "Kubernetes namespace for the wiggle release."
  type        = string
  default     = "wiggle"
}

variable "wiggle_chart" {
  description = "Path or repo reference to the wiggle Helm chart. Defaults to the chart vendored in this repo."
  type        = string
  default     = "../../helm/wiggle"
}

variable "wiggle_chart_version" {
  description = "Chart version (only used when wiggle_chart is a repo/OCI reference, not a local path)."
  type        = string
  default     = null
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
  description = "wiggle image tag; empty uses the chart's appVersion."
  type        = string
  default     = ""
}

variable "wiggle_replicas" {
  description = "Number of wiggle server nodes (they cluster on the shared Aurora database)."
  type        = number
  default     = 3
}

variable "wiggle_values" {
  description = "Extra Helm values (YAML) merged last, for advanced overrides."
  type        = string
  default     = ""
}
