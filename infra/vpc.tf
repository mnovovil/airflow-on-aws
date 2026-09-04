# A purpose-built VPC rather than the account's default 172.31.0.0/16, so the CIDR,
# the routing and the security groups are all described in one place.
#
# Everything lives in public subnets and there is no NAT gateway. That is not a
# weaker position than private subnets would be: neither the Airflow box nor the
# GDAL worker accepts any inbound traffic the trigger Lambda's security group does
# not carry, and SSM Run Command works over a connection the agent opens outbound.
# What a private subnet would add is a ~$35/month return path, and nothing else.

data "aws_availability_zones" "available" {
  state = "available"
}

locals {
  azs = slice(data.aws_availability_zones.available.names, 0, 2)
}

resource "aws_vpc" "this" {
  cidr_block           = var.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = { Name = "${local.name}-vpc" }
}

resource "aws_internet_gateway" "this" {
  vpc_id = aws_vpc.this.id
  tags   = { Name = "${local.name}-igw" }
}

# Two AZs, because a Lambda in a VPC and an instance that may need replacing both
# do better with somewhere else to land.
resource "aws_subnet" "public" {
  count = length(local.azs)

  vpc_id                  = aws_vpc.this.id
  availability_zone       = local.azs[count.index]
  cidr_block              = cidrsubnet(var.vpc_cidr, 8, count.index)
  map_public_ip_on_launch = true

  tags = { Name = "${local.name}-public-${local.azs[count.index]}" }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.this.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.this.id
  }

  tags = { Name = "${local.name}-public" }
}

resource "aws_route_table_association" "public" {
  count          = length(aws_subnet.public)
  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

# S3 traffic (raster reads, report writes, ECR layer pulls) leaves through this
# endpoint rather than the internet gateway. It is free, and it matters when the
# payload is elevation rasters.
resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.this.id
  service_name      = "com.amazonaws.${var.aws_region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.public.id]

  tags = { Name = "${local.name}-s3-endpoint" }
}

# ------------------------------------------------------------------ security groups

resource "aws_security_group" "airflow" {
  name = "${local.name}-airflow"
  # ASCII only: EC2 rejects a GroupDescription containing anything else, and the
  # error names the parameter rather than the character, so it is worth not repeating.
  description = "Self-managed Airflow - UI on 8080, reached by the trigger Lambda"
  vpc_id      = aws_vpc.this.id

  tags = { Name = "${local.name}-airflow" }
}

# The Lambda is admitted by security group rather than by CIDR. A Lambda in a VPC
# draws an arbitrary ENI address out of the subnet, so there is no stable address to
# write a CIDR rule against — and a rule wide enough to cover the whole subnet would
# also admit anything else that ever lands in it.
resource "aws_vpc_security_group_ingress_rule" "airflow_from_lambda" {
  security_group_id            = aws_security_group.airflow.id
  referenced_security_group_id = aws_security_group.trigger_lambda.id
  ip_protocol                  = "tcp"
  from_port                    = 8080
  to_port                      = 8080
  description                  = "Trigger Lambda to the Airflow REST API"
}

# Empty unless you opt in with airflow_ui_ingress_cidrs. See the variable's comment
# for why an SSM port-forward is the better way to reach the UI.
resource "aws_vpc_security_group_ingress_rule" "airflow_ui" {
  for_each = toset(var.airflow_ui_ingress_cidrs)

  security_group_id = aws_security_group.airflow.id
  cidr_ipv4         = each.value
  ip_protocol       = "tcp"
  from_port         = 8080
  to_port           = 8080
  description       = "Airflow UI from ${each.value}"
}

resource "aws_vpc_security_group_egress_rule" "airflow_all" {
  security_group_id = aws_security_group.airflow.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
  description       = "Outbound to ECR, S3, SSM, Secrets Manager and SES"
}

# The Lambda's own group. It has no ingress — nothing calls a Lambda over the
# network — and exists so the Airflow group has something specific to admit.
resource "aws_security_group" "trigger_lambda" {
  name        = "${local.name}-trigger-dag"
  description = "Trigger Lambda ENIs"
  vpc_id      = aws_vpc.this.id

  tags = { Name = "${local.name}-trigger-dag" }
}

resource "aws_vpc_security_group_egress_rule" "trigger_lambda_to_airflow" {
  security_group_id            = aws_security_group.trigger_lambda.id
  referenced_security_group_id = aws_security_group.airflow.id
  ip_protocol                  = "tcp"
  from_port                    = 8080
  to_port                      = 8080
  description                  = "To the Airflow REST API"
}

resource "aws_security_group" "worker" {
  name        = "${local.name}-gdal-worker"
  description = "GDAL worker - outbound only, reached exclusively via SSM"
  vpc_id      = aws_vpc.this.id

  tags = { Name = "${local.name}-gdal-worker" }
}

# No ingress rules at all, by design. SSM Run Command works over an outbound
# connection the agent opens, so the worker needs no open ports and no SSH key.
resource "aws_vpc_security_group_egress_rule" "worker_all" {
  security_group_id = aws_security_group.worker.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
  description       = "Outbound to SSM, ECR and S3"
}
