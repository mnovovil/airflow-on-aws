# The hop between S3 and Airflow. S3 notifications can only target SNS, SQS, Lambda
# or EventBridge, and EventBridge API Destinations would need Airflow on a public
# HTTPS endpoint — a domain and a certificate for a test rig. So a small function it
# is, joined to the VPC and posting to Airflow's private address.
#
# The handler has no third-party dependencies — basic auth over HTTP is all it does
# — so the deployment package is just the source file: no build step, no vendored
# wheels.

data "archive_file" "trigger_dag" {
  type        = "zip"
  source_dir  = "${path.module}/../lambda/trigger_dag"
  output_path = "${path.module}/.build/trigger_dag.zip"
  # Both forms: the bare name for the directory itself, the glob for what is inside
  # it. A stale .pyc in the zip is harmless but changes output_base64sha256, which
  # would redeploy the function on every apply run from a machine that had imported
  # the handler locally.
  excludes = ["__pycache__", "__pycache__/*", "requirements.txt"]
}

resource "aws_iam_role" "trigger_dag" {
  name = "${local.name}-trigger-dag"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

# Reaching Airflow needs no AWS permission at all — it is an HTTP call with a
# password — so writing its own logs is the whole policy. The VPC grant it also
# needs is a managed policy, attached separately below.
resource "aws_iam_role_policy" "trigger_dag" {
  name = "trigger-dag"
  role = aws_iam_role.trigger_dag.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid      = "Logs"
      Effect   = "Allow"
      Action   = ["logs:CreateLogStream", "logs:PutLogEvents"]
      Resource = "${aws_cloudwatch_log_group.trigger_dag.arn}:*"
    }]
  })
}

# A Lambda in a VPC manages its own ENIs, and cannot do so without these. The AWS
# managed policy is the documented way to grant it; writing the three ec2:* actions
# by hand buys nothing.
resource "aws_iam_role_policy_attachment" "trigger_dag_vpc" {
  role       = aws_iam_role.trigger_dag.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

resource "aws_cloudwatch_log_group" "trigger_dag" {
  name              = "/aws/lambda/${local.name}-trigger-dag"
  retention_in_days = var.log_retention_days
}

resource "aws_lambda_function" "trigger_dag" {
  function_name = "${local.name}-trigger-dag"
  description   = "Trigger the gdalinfo_notify DAG when a raster lands in ${var.source_bucket}"
  role          = aws_iam_role.trigger_dag.arn

  filename         = data.archive_file.trigger_dag.output_path
  source_code_hash = data.archive_file.trigger_dag.output_base64sha256
  handler          = "handler.handler"
  runtime          = "python3.12"

  # Generous because a cold Airflow web server can be slow to answer, and the POST
  # is synchronous.
  timeout     = 60
  memory_size = 256

  # Airflow is on a private address, so the function has to be inside the VPC to
  # reach it at all.
  vpc_config {
    subnet_ids         = aws_subnet.public[*].id
    security_group_ids = [aws_security_group.trigger_lambda.id]
  }

  environment {
    variables = {
      DAG_ID          = "gdalinfo_notify"
      AIRFLOW_API_URL = "http://${aws_instance.airflow.private_ip}:8080"

      # Lower-cased and compared case-insensitively in the handler, so the
      # mixed-case S3 filter rules all collapse to these.
      ALLOWED_SUFFIXES = ".tif,.tiff,.img,.vrt,.jp2"

      # What the handler drops. The bucket notification matches on suffix only, so
      # the DAG's own archive copy arrives here like any other upload and this is
      # what stops it costing a second run.
      ARCHIVE_PREFIX = local.archive_prefix

      AIRFLOW_USERNAME = var.airflow_admin_user
      # Plain environment, encrypted at rest with the Lambda service key. Reading
      # it from Secrets Manager instead would mean either a NAT gateway or an
      # interface endpoint — both of which cost more per month than this stack
      # spends on compute, to protect a password that only opens a test rig's UI.
      AIRFLOW_PASSWORD = random_password.airflow_admin.result
    }
  }

  depends_on = [aws_cloudwatch_log_group.trigger_dag]
}
