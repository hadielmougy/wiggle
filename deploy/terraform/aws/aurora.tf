resource "random_password" "db" {
  length  = 24
  special = true
  # RDS disallows '/', '@', '"' and space in the master password; keep to a safe punctuation set.
  override_special = "!#%*_-=+"
}

module "aurora" {
  source  = "terraform-aws-modules/rds-aurora/aws"
  version = "~> 9.10"

  name           = "${var.name}-aurora"
  engine         = "aurora-postgresql"
  engine_version = var.aurora_engine_version

  vpc_id                 = module.vpc.vpc_id
  subnets                = module.vpc.private_subnets
  create_db_subnet_group = true

  # Only the EKS nodes may reach the database (port 5432 is inferred from the engine).
  security_group_rules = {
    from_eks_nodes = {
      source_security_group_id = module.eks.node_security_group_id
    }
  }

  master_username             = var.database_user
  master_password             = random_password.db.result
  manage_master_user_password = false
  database_name               = var.database_name

  instance_class = var.aurora_instance_class
  instances      = { for i in range(var.aurora_instance_count) : "node-${i}" => {} }

  storage_encrypted   = true
  apply_immediately   = true
  skip_final_snapshot = true

  tags = local.tags
}
