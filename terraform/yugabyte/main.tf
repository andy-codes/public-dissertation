
provider "aws" {
  region = "eu-west-1"
}

module "yugabyte-db-cluster" {
  # The source module used for creating AWS clusters.
  source = "./modules/terraform-aws-yugabyte"


  # The name of the cluster to be created, change as per need.
  cluster_name = "test-cluster"

  # Version of yugabyte
 yb_version = "2025.2.0.0-b131"


  # REQUIRED by module
  region_name        = "eu-west-1"
  availability_zones = ["eu-west-1b"]

  # Existing custom security group to be passed so that we can connect to the instances.
  # Make sure this security group allows your local machine to SSH into these instances.
  custom_security_group_id = aws_security_group.yugabyte_sg.id

  # AWS key pair that you want to use to ssh into the instances.
  # Make sure this key pair is already present in the noted region of your account.
  ssh_keypair     = "yugabyte-test"
  ssh_private_key = "/Users/andrewjones/.ssh/yugabyte_test"

  # Existing vpc and subnet ids where the instances should be spawned.
  vpc_id     = "vpc-89a3a1ed"
  subnet_ids = ["subnet-6d819509"]

  # Replication factor of the YugabyteDB cluster.
  replication_factor = "3"

  # The number of nodes in the cluster, this cannot be lower than the replication factor.
  num_instances = "3"

  instance_type = "t3.large"

  ssh_user = "ec2-user"
}

locals {
  my_ip_cidr = "86.171.101.9/32"
}

resource "aws_security_group" "yugabyte_sg" {
  name        = "yugabyte-db-sg-open"
  description = "OPEN security group for YugabyteDB test cluster"
  vpc_id      = "vpc-89a3a1ed"

  # --------------------
  # SSH (open)
  # --------------------
  ingress {
    description = "SSH from anywhere"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [local.my_ip_cidr]
  }

  # --------------------
  # YugabyteDB client ports
  # --------------------
  ingress {
    description = "YSQL"
    from_port   = 5433
    to_port     = 5433
    protocol    = "tcp"
    cidr_blocks = [local.my_ip_cidr]
  }

  ingress {
    description = "YCQL"
    from_port   = 9042
    to_port     = 9042
    protocol    = "tcp"
    cidr_blocks = [local.my_ip_cidr]
  }

  # --------------------
  # YugabyteDB Web UIs
  # --------------------
  ingress {
    description = "YB Master UI"
    from_port   = 7000
    to_port     = 7000
    protocol    = "tcp"
    cidr_blocks = [local.my_ip_cidr]
  }

  ingress {
    description = "YB TServer UI"
    from_port   = 9000
    to_port     = 9000
    protocol    = "tcp"
    cidr_blocks = [local.my_ip_cidr]
  }

  # --------------------
  # Internal cluster traffic
  # --------------------
  ingress {
    description = "All internal traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    self        = true
  }

  # --------------------
  # Outbound: allow all
  # --------------------
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "yugabyte-db-sg-open"
  }
}
