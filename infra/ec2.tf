# The GDAL worker. Normally STOPPED — the DAG starts it, runs one container over
# SSM, and stops it again, so compute is billed by the minute rather than the month.
#
# No SSH key and no inbound security group rules — everything reaches it through the
# SSM agent's outbound connection. It does get a public IP, because that is what lets
# it reach ECR and SSM without a NAT gateway; with zero ingress rules that address
# accepts nothing.

data "aws_ssm_parameter" "al2023" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
}

resource "aws_iam_role" "worker" {
  name = "${local.name}-gdal-worker"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

# Grants the SSM agent its control-plane access — this is what makes Run Command
# work without opening a port.
resource "aws_iam_role_policy_attachment" "worker_ssm" {
  role       = aws_iam_role.worker.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_role_policy" "worker" {
  name = "gdal-report"
  role = aws_iam_role.worker.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "PullImage"
        Effect = "Allow"
        Action = [
          "ecr:GetAuthorizationToken",
          "ecr:BatchCheckLayerAvailability",
          "ecr:GetDownloadUrlForLayer",
          "ecr:BatchGetImage",
        ]
        Resource = "*"
      },
      {
        Sid      = "ReadRasters"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:GetObjectVersion"]
        Resource = "${local.source_bucket_arn}/*"
      },
      {
        Sid      = "ListSourceBucket"
        Effect   = "Allow"
        Action   = ["s3:ListBucket", "s3:GetBucketLocation"]
        Resource = local.source_bucket_arn
      },
      {
        Sid      = "WriteReports"
        Effect   = "Allow"
        Action   = ["s3:PutObject"]
        Resource = "${aws_s3_bucket.artifacts.arn}/reports/*"
      },
    ]
  })
}

resource "aws_iam_instance_profile" "worker" {
  name = "${local.name}-gdal-worker"
  role = aws_iam_role.worker.name
}

resource "aws_instance" "worker" {
  ami           = data.aws_ssm_parameter.al2023.value
  instance_type = var.worker_instance_type
  # Public subnet with a security group that has no ingress rules at all — see the
  # note in vpc.tf for why that is not the weaker position it looks like.
  subnet_id              = aws_subnet.public[0].id
  vpc_security_group_ids = [aws_security_group.worker.id]
  iam_instance_profile   = aws_iam_instance_profile.worker.name

  # A stray `shutdown` from inside the box should stop the instance, not terminate
  # it — terminating would take the Docker image cache with it.
  instance_initiated_shutdown_behavior = "stop"

  metadata_options {
    http_tokens                 = "required" # IMDSv2 only
    http_endpoint               = "enabled"
    http_put_response_hop_limit = 2 # containers need one extra hop for credentials
  }

  root_block_device {
    volume_size           = var.worker_volume_size
    volume_type           = "gp3"
    encrypted             = true
    delete_on_termination = true
  }

  user_data                   = file("${path.module}/user_data.sh")
  user_data_replace_on_change = true

  tags = { Name = "${local.name}-gdal-worker" }

  # Terraform must not fight the DAG over power state. Whether the instance is
  # running or stopped at any given moment is runtime behaviour, not desired state.
  lifecycle {
    ignore_changes = [ami]
  }
}

# ------------------------------------------------------------------ stranded-instance guard

# If a DAG run is killed between start_instance and stop_instance, nothing in
# Airflow will ever stop this box. The built-in EC2 alarm action costs nothing and
# needs no Lambda: idle for 30 minutes means idle.
#
# treat_missing_data = notBreaching is essential — a stopped instance publishes no
# CPU metrics at all, and "missing" must not be read as "idle, stop it again".
resource "aws_cloudwatch_metric_alarm" "worker_idle" {
  alarm_name          = "${local.name}-gdal-worker-idle-stop"
  alarm_description   = "Stop the GDAL worker if it sits idle — catches DAG runs that died before stop_instance"
  namespace           = "AWS/EC2"
  metric_name         = "CPUUtilization"
  dimensions          = { InstanceId = aws_instance.worker.id }
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 6
  threshold           = 5
  comparison_operator = "LessThanThreshold"
  treat_missing_data  = "notBreaching"

  alarm_actions = ["arn:aws:automate:${var.aws_region}:ec2:stop"]
}

# Full container output from SSM Run Command. The API response itself is capped at
# ~24 KB, so anything verbose has to be read here.
resource "aws_cloudwatch_log_group" "ssm_output" {
  name              = "/aws/ssm/${local.name}-gdal-worker"
  retention_in_days = var.log_retention_days
}
