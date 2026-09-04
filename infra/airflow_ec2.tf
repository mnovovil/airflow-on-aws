# Airflow on one EC2 instance, under docker compose with a LocalExecutor and a
# Postgres container.
#
# ~$16/month of t3.small, against ~$120 for the smallest MWAA environment plus the
# ~$35 NAT gateway MWAA's private subnets would have required. What it costs in
# exchange is that scheduler availability is now one instance and one root volume —
# acceptable for a pipeline whose work is idempotent and whose trigger is a durable
# S3 event, and not acceptable for much else.
#
# Everything Airflow-specific is in the bootstrap script and the compose file
# alongside it. This file is the AWS-shaped half: identity, secret, instance.

locals {
  # The plugin is fetched at boot rather than baked, so bumping it is a one-line
  # change here. Pinned deliberately — see the note in the bootstrap script.
  compose_url = "https://github.com/docker/compose/releases/download/v2.29.7/docker-compose-linux-x86_64"

  # Both of these could be read off the resources they name, and that is exactly what
  # they must not be. Anything user_data interpolates from a resource being created in
  # the same run is unknown at plan time, and an unknown user_data on an instance with
  # user_data_replace_on_change is a plan Terraform renders as an in-place update and
  # the provider turns into a destroy-and-create once the real values arrive. That
  # aborts the whole apply with "Provider produced inconsistent final plan", which is
  # how prod's deploy failed with the ECR repository already created and the instance
  # untouched.
  #
  # Spelled out from values that are known before anything is created, the plan says
  # what it is going to do and then does that. Ordering is still enforced, by the
  # depends_on on the instance rather than by an interpolation.
  airflow_image_secret = "${local.name}/airflow-image"
  ecr_registry         = "${local.account_id}.dkr.ecr.${var.aws_region}.amazonaws.com"
}

# ------------------------------------------------------------------------ ui login

# In state, which is why the state bucket is encrypted and private. The alternative —
# generating it on the box — would leave no way to read the password back out
# without an SSM session, and the password is the only way into the UI.
resource "random_password" "airflow_admin" {
  length = 32
  # Airflow's CLI takes the password as an argv value and the bootstrap passes it
  # through a shell, so keep it to characters that cannot terminate a word.
  special          = true
  override_special = "-_.~"
}

resource "aws_secretsmanager_secret" "airflow_admin" {
  name                    = "${local.name}/airflow-admin"
  description             = "Airflow web UI login"
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "airflow_admin" {
  secret_id = aws_secretsmanager_secret.airflow_admin.id
  secret_string = jsonencode({
    username = var.airflow_admin_user
    password = random_password.airflow_admin.result
  })
}

# ------------------------------------------------------------------------- identity

resource "aws_iam_role" "airflow" {
  name = "${local.name}-airflow"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

# Lets you reach this box with `aws ssm start-session` — including the port-forward
# that opens the UI without any security group ingress at all.
resource "aws_iam_role_policy_attachment" "airflow_ssm" {
  role       = aws_iam_role.airflow.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

# Exactly what the DAG does and nothing else: drive the worker, read its results,
# read the secrets that configure it, and archive the raster it has reported on.
resource "aws_iam_role_policy" "airflow" {
  name = "run-the-dag"
  role = aws_iam_role.airflow.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "DriveTheWorker"
        Effect   = "Allow"
        Action   = ["ec2:StartInstances", "ec2:StopInstances"]
        Resource = aws_instance.worker.arn
      },
      {
        # Describe* has no resource-level permissions in EC2 — all or nothing.
        Sid      = "InspectWorkerState"
        Effect   = "Allow"
        Action   = ["ec2:DescribeInstances", "ec2:DescribeInstanceStatus"]
        Resource = "*"
      },
      {
        Sid    = "SendCommands"
        Effect = "Allow"
        Action = ["ssm:SendCommand"]
        Resource = [
          aws_instance.worker.arn,
          "arn:aws:ssm:${var.aws_region}::document/AWS-RunShellScript",
        ]
      },
      {
        Sid      = "ReadCommandResults"
        Effect   = "Allow"
        Action   = ["ssm:GetCommandInvocation", "ssm:ListCommandInvocations", "ssm:DescribeInstanceInformation"]
        Resource = "*"
      },
      {
        Sid    = "ReadAirflowSecrets"
        Effect = "Allow"
        Action = ["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"]
        # Scoped to this environment's prefix, so dev's scheduler cannot read prod's
        # ice_config — which is what holds prod's worker instance id.
        Resource = [
          "arn:aws:secretsmanager:${var.aws_region}:${local.account_id}:secret:${local.secrets_prefix}/connections/*",
          "arn:aws:secretsmanager:${var.aws_region}:${local.account_id}:secret:${local.secrets_prefix}/variables/*",
          aws_secretsmanager_secret.airflow_admin.arn,
          aws_secretsmanager_secret.airflow_image_uri.arn,
        ]
      },
      {
        # Pulling this box's own image. GetAuthorizationToken is account-wide by
        # definition — it mints a registry credential, not a repository one — so the
        # repository scoping is on the two calls that actually read layers.
        Sid      = "AuthenticateToEcr"
        Effect   = "Allow"
        Action   = ["ecr:GetAuthorizationToken"]
        Resource = "*"
      },
      {
        Sid      = "PullTheAirflowImage"
        Effect   = "Allow"
        Action   = ["ecr:BatchGetImage", "ecr:GetDownloadUrlForLayer", "ecr:BatchCheckLayerAvailability"]
        Resource = aws_ecr_repository.airflow.arn
      },
      {
        # DAG source on the way in; gdalinfo.json and summary.json on the way back
        # out for the email task.
        Sid      = "ArtifactsBucket"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:GetObjectVersion"]
        Resource = "${aws_s3_bucket.artifacts.arn}/*"
      },
      {
        Sid      = "ListArtifactsBucket"
        Effect   = "Allow"
        Action   = ["s3:ListBucket", "s3:GetBucketLocation"]
        Resource = aws_s3_bucket.artifacts.arn
      },
      {
        # The STAC catalogue: one item per raster from gdalinfo_notify, plus the
        # collection and root document the stac_publish DAG rolls up.
        #
        # Scoped to the prefix for the same reason the archive grant is: DAG source
        # lives in this bucket, and the scheduler syncs *from* dags/ on a one-minute
        # timer. A bucket-wide PutObject would let a bug in a task overwrite the code
        # that runs it. Every write this role has into the artifacts bucket is named
        # by prefix, here and below; there is deliberately no blanket one.
        Sid      = "WriteTheStacCatalogue"
        Effect   = "Allow"
        Action   = ["s3:PutObject"]
        Resource = "${aws_s3_bucket.artifacts.arn}/${local.stac_prefix}*"
      },
      {
        # The fire DAG's detections: one CSV per country per day, at
        # <prefix>/<source>/<ds>/<country>.csv. The prefix is read from the same
        # local the DAG's own config is built from, in infra/fire.tf, so the grant
        # and the key cannot drift apart.
        #
        # Prefix-scoped like the STAC write above, and for the same reason. Note the
        # explicit "/" — local.fire.prefix carries no trailing slash (store_csv_s3
        # strips one), and without it this would also grant "fires-anything".
        Sid      = "WriteTheFireDetections"
        Effect   = "Allow"
        Action   = ["s3:PutObject"]
        Resource = "${aws_s3_bucket.artifacts.arn}/${local.fire.prefix}/*"
      },
      {
        # move_file archives the raster in place once the report is out. boto3's
        # copy() reads the source object and, above its 8 MiB threshold, does it as a
        # multipart copy — hence the abort, which is what cleans up a failed one.
        # The write half is scoped to the archive prefix below.
        Sid      = "ReadAndRemoveTheSourceRaster"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:GetObjectVersion", "s3:DeleteObject", "s3:AbortMultipartUpload"]
        Resource = "${local.source_bucket_arn}/*"
      },
      {
        # Narrower than the read: the scheduler may create objects under sent/ and
        # nowhere else, so a wrong destination key cannot put a raster back into the
        # watched part of the bucket.
        Sid      = "WriteTheArchiveCopy"
        Effect   = "Allow"
        Action   = ["s3:PutObject"]
        Resource = "${local.source_bucket_arn}/${local.archive_prefix}*"
      },
    ]
  })
}

resource "aws_iam_instance_profile" "airflow" {
  name = "${local.name}-airflow"
  role = aws_iam_role.airflow.name
}

# ------------------------------------------------------------------------- instance

resource "aws_instance" "airflow" {
  ami           = data.aws_ssm_parameter.al2023.value
  instance_type = var.airflow_instance_type
  # Public subnet, so the box can pull from Docker Hub and reach the AWS APIs with
  # no NAT gateway. Ingress is limited to the trigger Lambda's security group.
  subnet_id              = aws_subnet.public[0].id
  vpc_security_group_ids = [aws_security_group.airflow.id]
  iam_instance_profile   = aws_iam_instance_profile.airflow.name

  metadata_options {
    http_tokens                 = "required" # IMDSv2 only
    http_endpoint               = "enabled"
    http_put_response_hop_limit = 2 # containers need one extra hop for credentials
  }

  root_block_device {
    volume_size           = var.airflow_volume_size
    volume_type           = "gp3"
    encrypted             = true
    delete_on_termination = true
  }

  # base64gzip, not a plain string: EC2 caps user_data at 16 KiB and the rendered
  # bootstrap is over it, most of that the base64'd compose file. cloud-init detects
  # the gzip magic bytes and decompresses before running, so this costs nothing at
  # boot and takes the payload to about half the cap.
  #
  # It was already at ~96% of the limit before the image sync was added, which is why
  # a couple of dozen lines tipped it over. If it ever approaches 16 KiB *compressed*,
  # the answer is not more compression — it is that the bootstrap has outgrown
  # user_data and belongs in the image or in an S3 object with its hash wired in here.
  user_data_base64 = base64gzip(templatefile("${path.module}/airflow_user_data.sh.tftpl", {
    region            = var.aws_region
    artifacts_bucket  = aws_s3_bucket.artifacts.id
    admin_secret_name = aws_secretsmanager_secret.airflow_admin.name
    admin_user        = var.airflow_admin_user
    airflow_version   = var.airflow_version
    compose_url       = local.compose_url
    # Both from locals rather than from the resources, so user_data stays known at
    # plan time — see the note beside them. Only the registry host is needed, for
    # `docker login`: the image reference itself comes from the secret at run time,
    # so Terraform is never the thing that decides which tag is running.
    image_secret_name = local.airflow_image_secret
    ecr_registry      = local.ecr_registry
    compose_b64       = base64encode(file("${path.module}/airflow_compose.yml"))
    # Reaches the containers through /opt/ice/.env rather than through the compose
    # file, which is base64'd verbatim precisely so Terraform never templates it.
    secrets_prefix = local.secrets_prefix
  }))

  # Editing the bootstrap or the compose file replaces the instance, which loses the
  # Airflow database along with it. That is the intended trade: DAG run history on a
  # test rig is worth less than knowing the box matches what is in the repository.
  # DAGs themselves are not affected — they are synced from S3, not baked in.
  user_data_replace_on_change = true

  # Both secrets have to hold a value before the bootstrap reads them: the admin
  # password to create the UI login, the image pointer to know what to pull.
  depends_on = [
    aws_secretsmanager_secret_version.airflow_admin,
    aws_secretsmanager_secret_version.airflow_image_uri,
  ]

  tags = { Name = "${local.name}-airflow" }

  lifecycle {
    ignore_changes = [ami]
  }
}
