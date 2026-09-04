variable "aws_region" {
  description = "Region everything is deployed into"
  type        = string
  default     = "eu-north-1"
}

variable "project" {
  description = "Short name prefixed onto every resource"
  type        = string
  default     = "ice"
}

# ------------------------------------------------------------------------ environment

# Deliberately has no default. This root module builds either environment, and which
# one it builds must never be inferred — a missing -var-file has to fail rather than
# quietly pick prod. See infra/envs/ for the two files that set it, and versions.tf
# for how the name mangling keeps prod's resource names unchanged.
variable "env" {
  description = "Which environment this apply builds: prod or dev. Must match the state key selected at init."
  type        = string

  validation {
    condition     = contains(["prod", "dev"], var.env)
    error_message = "env must be 'prod' or 'dev'. Adding a third environment means adding a tfvars file, a backend config and a branch mapping in deploy.yml."
  }
}

# Each environment owns exactly one, and never the other's: aws_s3_bucket_notification
# is authoritative for a whole bucket, so two stacks pointed at one bucket would each
# silently delete the other's triggers on apply. Both buckets predate their definition
# in s3.tf and are adopted by the import blocks in import.tf.
variable "source_bucket" {
  description = "Bucket that rasters land in. Managed by this stack, and adopted on first apply rather than created."
  type        = string
}

# Only dev sets this. Prod's source bucket holds the real rasters and the archive
# move_file writes under sent/, so it gets no expiry rule at all — null removes the
# resource rather than setting some very large number of days, because a rule that
# exists is a rule someone can later shorten by editing one integer.
variable "source_bucket_expiry_days" {
  description = "Days after which objects in the source bucket expire. null means no lifecycle rule. Never set for prod."
  type        = number
  default     = null

  validation {
    condition     = !(var.source_bucket_expiry_days != null && var.env == "prod")
    error_message = "source_bucket_expiry_days must stay null for prod — it would put the archived rasters under sent/ on a deletion timer."
  }
}

# Terraform refuses to delete a bucket with objects in it, so without this a
# `terraform destroy` of dev stops half-way and leaves the rest of the stack behind.
# Prod's buckets are never disposable in that way, which the validation enforces.
variable "force_destroy_buckets" {
  description = "Allow terraform destroy to empty the buckets this stack owns. Never true for prod."
  type        = bool
  default     = false

  validation {
    condition     = !(var.force_destroy_buckets && var.env == "prod")
    error_message = "force_destroy_buckets must stay false for prod — it turns a destroy into unrecoverable data loss."
  }
}

variable "raster_suffixes" {
  description = <<-EOT
    Object suffixes that trigger a DAG run. S3 notification filters are
    case-sensitive and have no wildcards, so each casing needs its own rule.
  EOT
  type        = list(string)
  default     = [".tif", ".TIF", ".tiff", ".TIFF", ".img", ".IMG", ".vrt", ".jp2"]
}

variable "email_to" {
  description = "Recipient of the gdalinfo report"
  type        = string
}

variable "email_from" {
  description = <<-EOT
    Sender address. While SES is in the sandbox this identity AND email_to must
    each be verified by clicking a link before any mail is delivered.
  EOT
  type        = string
}

variable "ses_identities" {
  description = <<-EOT
    Addresses whose SES identity this environment CREATES — not the addresses it is
    allowed to use.

    SES identities are account-global, so two stacks that both declare the same
    address collide on apply, and whichever runs `destroy` first un-verifies it for
    the other. Every address therefore belongs to exactly one environment, and the
    lists must be disjoint.

    Using an address without owning it is fine and is what dev does: SES checks that
    an identity is verified, not which stack verified it. Prod owns both addresses;
    dev's list is empty.
  EOT
  type        = list(string)
}

variable "airflow_version" {
  description = "Airflow version the box runs. Must match the base image and constraint URL in docker/airflow."
  type        = string
  default     = "2.10.3"
}

variable "airflow_instance_type" {
  description = <<-EOT
    EC2 type for the Airflow box.

    t3.small, but only because this account cannot have anything better cheaply.
    Untuned it wedges: the scheduler, the web server, Postgres and a running task
    leave about 400 MiB free on 2 GiB, and a fresh t3 starts with zero CPU credits,
    so the first hour is the worst possible time to ask it for work. Twice it pinned
    the CPU and stopped answering SSM entirely, which presents as ConnectionLost
    while the EC2 status checks still say ok — a starved box looking like a network
    fault. The bootstrap therefore adds swap and the compose file caps the web
    server's workers; with both, 2 GiB holds.

    t3.medium is not an option here. This is a Free plan account, so RunInstances
    refuses any type that is not free-tier eligible, and ModifyInstanceAttribute is
    blocked outright. What is eligible, with eu-north-1 on-demand prices:

      t4g.small        2 GiB   ~$13/mo   (Graviton; needs arm64 images)
      t3.small         2 GiB   ~$16/mo   <- here
      c7i-flex.large   4 GiB   ~$66/mo
      m7i-flex.large   8 GiB   ~$74/mo

    The jump to real headroom is 4x the price for 2x the memory, which is why the
    tuning came first. If this box still misbehaves, c7i-flex.large is the answer
    and it needs no other change.
  EOT
  type        = string
  default     = "t3.small"
}

variable "airflow_volume_size" {
  description = "Root volume in GiB for the Airflow box — holds the Airflow image, Postgres data and task logs"
  type        = number
  default     = 20
}

variable "airflow_admin_user" {
  description = "Username for the Airflow web UI. The password is generated and stored in Secrets Manager."
  type        = string
  default     = "admin"
}

variable "airflow_ui_ingress_cidrs" {
  description = <<-EOT
    CIDRs allowed to reach the Airflow UI on port 8080.

    Empty by default, which is the safe setting: the Lambda reaches Airflow through
    its security group rather than a CIDR, so the UI needs no public ingress to make
    the pipeline work. Add your own address to open the UI:

      terraform apply -var 'airflow_ui_ingress_cidrs=["203.0.113.4/32"]'

    Do not put 0.0.0.0/0 here. Airflow's UI is a login form over plain HTTP on this
    box, so a public rule exposes the admin password to anyone watching the wire.
    For casual access prefer an SSM port-forward, which needs no ingress at all:

      aws ssm start-session --target <id> \
        --document-name AWS-StartPortForwardingSession \
        --parameters '{"portNumber":["8080"],"localPortNumber":["8080"]}'
  EOT
  type        = list(string)
  default     = []
}

variable "worker_instance_type" {
  description = "EC2 type running the GDAL container"
  type        = string
  default     = "t3.small"
}

variable "worker_volume_size" {
  description = "Root volume in GiB — must hold the GDAL image (~1.5 GiB) plus scratch"
  type        = number
  default     = 20
}

variable "vpc_cidr" {
  description = <<-EOT
    CIDR for the VPC this stack creates.

    The environments are not peered, so overlapping ranges would in fact work.
    They are kept distinct anyway: it makes a flow log or an instance address
    self-identifying, and it is the cheap half of never having to renumber if the
    two are ever connected.
  EOT
  type        = string
}

variable "log_retention_days" {
  description = "Retention for the Lambda and SSM output log groups"
  type        = number
  default     = 14
}
