terraform {
  required_version = ">= 1.11.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.60"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.4"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }

  # State locking uses a lock file in the bucket itself (Terraform >= 1.11), which
  # is why there is no DynamoDB table anywhere in this stack.
  #
  # PARTIAL on purpose: `key` is supplied at init time from infra/backends/<env>.hcl.
  # Hard-coding it here would mean one environment's state file is the default, and
  # the failure mode of applying dev's variables over prod's state is not one worth
  # leaving available:
  #
  #     terraform init -backend-config=backends/dev.hcl
  #
  # `terraform init` with no -backend-config now prompts for the key rather than
  # silently choosing, and CI derives both the key and the tfvars from one branch
  # name so the pair cannot drift.
  backend "s3" {
    bucket       = "ice-tfstate-123456789012"
    region       = "eu-north-1"
    encrypt      = true
    use_lockfile = true
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project   = var.project
      Env       = var.env
      ManagedBy = "terraform"
    }
  }
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id

  # Prod is unsuffixed and dev is "ice-dev". Prod deliberately keeps the names it
  # already has: nearly every one of them (IAM roles, instance profiles, security
  # groups, the ECR repository, the buckets) is immutable, so renaming prod would
  # mean destroying and recreating the entire live stack — including an artifacts
  # bucket that cannot be deleted while it holds objects. A one-off asymmetry in
  # one expression is a far better trade than that.
  name = var.env == "prod" ? var.project : "${var.project}-${var.env}"

  artifacts_bucket = "${local.name}-artifacts-${local.account_id}"

  # Airflow resolves Connections and Variables from Secrets Manager under these
  # prefixes (see AIRFLOW__SECRETS__BACKEND_KWARGS in airflow_compose.yml). They have
  # to differ per environment — ice_config carries the worker instance id and the
  # bucket names, so a shared prefix would have dev's scheduler driving prod's worker.
  #
  # Prod stays on the bare "airflow" prefix because its secrets already exist under
  # it and the DAG looks connections up by the fixed id smtp_default.
  secrets_prefix = var.env == "prod" ? "airflow" : "airflow-${var.env}"

  # Both environments manage their own source bucket now, so there is one origin
  # rather than two. These stay because everything downstream already reads them, and
  # a single place to look is worth keeping even when the expression is trivial.
  source_bucket_id  = aws_s3_bucket.source.id
  source_bucket_arn = aws_s3_bucket.source.arn

  # Where move_file archives a raster once its report has been emailed. Three things
  # have to agree on this string: the DAG that writes there, the IAM policy that
  # allows it, and the Lambda that ignores the event the write produces. The DAG
  # holds its own copy as a module constant (ARCHIVE_PREFIX); the other two read
  # this, and tests/test_archive_wiring.py fails if the three ever drift apart.
  archive_prefix = "sent/"

  # Where the STAC catalogue lives in the artifacts bucket. Two things read this: the
  # IAM statement that lets the scheduler write there, and the lifecycle rule that
  # expires old versions of an item. The DAGs hold their own copy in
  # common/stac.py (CATALOG_PREFIX), and tests/test_stac_wiring.py fails if the two
  # drift — the failure mode otherwise is AccessDenied on every publish.
  stac_prefix = "stac/"
}
