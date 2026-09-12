module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 20.24"

  cluster_name    = "${var.name}-eks"
  cluster_version = var.kubernetes_version

  cluster_endpoint_public_access = var.cluster_public_access

  # Give the identity running Terraform admin on the cluster, so the kubernetes/helm
  # providers can deploy the wiggle release in the same apply.
  enable_cluster_creator_admin_permissions = true

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets

  eks_managed_node_groups = {
    default = {
      instance_types = var.node_instance_types
      min_size       = var.node_min_size
      max_size       = var.node_max_size
      desired_size   = var.node_desired_size
    }
  }

  tags = local.tags
}
