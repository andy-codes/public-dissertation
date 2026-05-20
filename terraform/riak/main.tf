terraform {
  required_version = ">= 1.3.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }
  }
}

provider "aws" {
  region = "eu-west-1"
}

# ----------------------------
# Inputs (edit if you like)
# ----------------------------
locals {
  cluster_name   = "bench"
  instance_count = 3

  ami_id    = "ami-0408578f3a4e0af2f"
  vpc_id    = "vpc-89a3a1ed"
  subnet_id = "subnet-6d819509"

  instance_type = "t3.large" # adjust as needed
  key_name      = "yugabyte-test"

  # Read user-data as a raw file (avoids Terraform interpreting ${...} inside bash)
  user_data = file("${path.module}/user_data/openriak_userdata.sh")
}

# ----------------------------
# IAM: allow tag-based discovery (bootstrap needs DescribeInstances/DescribeTags)
# ----------------------------
data "aws_iam_policy_document" "assume_ec2" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "riak_role" {
  name               = "openriak-bench-role"
  assume_role_policy = data.aws_iam_policy_document.assume_ec2.json
}

data "aws_iam_policy_document" "riak_ec2_read" {
  statement {
    effect = "Allow"
    actions = [
      "ec2:DescribeInstances",
      "ec2:DescribeTags"
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "riak_ec2_read" {
  name   = "openriak-ec2-read"
  role   = aws_iam_role.riak_role.id
  policy = data.aws_iam_policy_document.riak_ec2_read.json
}

resource "aws_iam_instance_profile" "riak_profile" {
  name = "openriak-bench-profile"
  role = aws_iam_role.riak_role.name
}

# ----------------------------
# Security Group
# ----------------------------
resource "aws_security_group" "riak_sg" {
  name        = "openriak-bench-sg"
  description = "OpenRiak benchmark SG"
  vpc_id      = local.vpc_id

  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    description = "Riak HTTP (8098)"
    from_port   = 8098
    to_port     = 8098
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    description = "Riak PB (8087)"
    from_port   = 8087
    to_port     = 8087
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # ---- ADD THIS: riak handoff (8099) only within the cluster SG ----
  ingress {
    description = "Riak handoff (8099) intra-cluster only"
    from_port   = 8099
    to_port     = 8099
    protocol    = "tcp"
    self        = true
  }

  # Intra-cluster Erlang connectivity
  ingress {
    description = "EPMD (intra-cluster)"
    from_port   = 4369
    to_port     = 4369
    protocol    = "tcp"
    self        = true
  }

  ingress {
    description = "Erlang distribution (intra-cluster)"
    from_port   = 1024
    to_port     = 65535
    protocol    = "tcp"
    self        = true
  }

  # ingress rule for experimentation EC2

  ingress {
  description     = "Riak PB (8087) from load-test SG"
  from_port       = 8087
  to_port         = 8087
  protocol        = "tcp"
  security_groups = ["sg-027eaa3362f1b999e"] # source SG
}

  ingress {
  description     = "Riak HTTP (8098) from load-test SG"
  from_port       = 8098
  to_port         = 8098
  protocol        = "tcp"
  security_groups = ["sg-027eaa3362f1b999e"]
}
 # ingress rule for experimentation EC2
  egress {
    description = "All outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "openriak-bench-sg"
  }
}

# ----------------------------
# Instances (3 nodes)
# ----------------------------
resource "aws_instance" "riak" {
  count         = local.instance_count
  ami           = local.ami_id
  instance_type = local.instance_type
  subnet_id     = local.subnet_id

  key_name = local.key_name

  vpc_security_group_ids      = [aws_security_group.riak_sg.id]
  iam_instance_profile        = aws_iam_instance_profile.riak_profile.name
  associate_public_ip_address = true

  # Apply per-instance configuration at boot (nodename + listeners + start riak)
  user_data = local.user_data
  user_data_replace_on_change = true

  tags = {
    Name               = "openriak-${local.cluster_name}-${count.index}"
    "openriak-cluster" = local.cluster_name
    "openriak-seed"    = count.index == 0 ? "true" : "false"
  }
}

# ----------------------------
# Outputs
# ----------------------------
output "riak_public_ips" {
  value = [for i in aws_instance.riak : i.public_ip]
}

output "riak_private_ips" {
  value = [for i in aws_instance.riak : i.private_ip]
}

output "riak_http_endpoints_public" {
  value = join(
    ",",
    [for i in aws_instance.riak : "http://${i.public_ip}:8098"]
  )
}

output "riak_public_ips_string" {
  value = join(
    ",",
    [for i in aws_instance.riak : i.public_ip]
  )
}


output "riak_http_endpoints_private" {
  value = join(
    ",",
    [for i in aws_instance.riak : "http://${i.private_ip}:8098"]
  )
}

output "riak_private_ips_string" {
  value = join(
    ",",
    [for i in aws_instance.riak : i.private_ip]
  )
}


