# Bootstrap: the handful of resources that must exist before the main stack can be
# applied from CI. Deliberately a separate root module with LOCAL state, because the
# main stack's S3 backend cannot create the bucket it stores its own state in.
#
# Run once, by hand, with admin credentials:
#     cd infra/bootstrap && terraform init && terraform apply
#
# Losing this state file is survivable — everything here is either trivially
# re-importable or safe to recreate.

terraform {
  required_version = ">= 1.11.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.60"
    }
  }
}

provider "aws" {
  region = var.aws_region
  default_tags {
    tags = {
      Project   = var.project
      ManagedBy = "terraform"
      Component = "bootstrap"
    }
  }
}

data "aws_caller_identity" "current" {}

locals {
  state_bucket = "${var.project}-tfstate-${data.aws_caller_identity.current.account_id}"

  # The GitHub OIDC subjects allowed to assume the deploy role, spelled out rather
  # than wildcarded. `ref:refs/heads/<branch>` is what deploy.yml sends on a push or
  # a workflow_dispatch against that branch; `pull_request` is what pr.yml sends,
  # whatever branch the PR is from. See the trust policy below for what each costs.
  oidc_subjects = [
    "ref:refs/heads/main",
    "ref:refs/heads/dev",
    "pull_request",
  ]
}

# --------------------------------------------------------------- terraform state

resource "aws_s3_bucket" "state" {
  bucket = local.state_bucket

  # State is the one thing whose accidental deletion is genuinely painful.
  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_s3_bucket_versioning" "state" {
  bucket = aws_s3_bucket.state.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "state" {
  bucket = aws_s3_bucket.state.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "state" {
  bucket                  = aws_s3_bucket.state.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ------------------------------------------------------------- github oidc access

# Lets GitHub Actions assume a role with a short-lived token instead of storing
# long-lived AWS keys as repository secrets.
resource "aws_iam_openid_connect_provider" "github" {
  url            = "https://token.actions.githubusercontent.com"
  client_id_list = ["sts.amazonaws.com"]
  thumbprint_list = [
    "6938fd4d98bab03faadb97b34396831e3780aea1",
    "1c58a3a8518e8759bf075b76b750d4f2df264fcd",
  ]
}

data "aws_iam_policy_document" "github_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.github.arn]
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }

    # Scoped to this repository and to the three subjects that actually run: the
    # two deploy branches, and `pull_request` for the plan in pr.yml. A fork's
    # token carries the fork's own `sub`, so a PR from a fork cannot assume this
    # role either way.
    #
    # What this replaces was a trailing `:*`, matching every subject GitHub issues
    # for the repo. Naming the three is tighter but not dramatically so, and it is
    # worth being precise about the difference: it rules out tag refs, `environment:`
    # subjects, and a workflow triggered by a push to some other branch. It does not
    # rule out a pull request, because pr.yml plans against real state and needs the
    # role to do it — so anyone who can open a PR from a branch IN this repository
    # can still reach a role holding AdministratorAccess. That is collaborators only,
    # never the public, but it is the reason to keep write access to this repository
    # narrow now that it is readable by everyone.
    #
    # Drop the "pull_request" entries below to close that, at the cost of the plan
    # on every PR — the job fails at its Authenticate step rather than being skipped.
    #
    # Two forms of each, because GitHub now issues the subject with immutable
    # numeric IDs appended to the owner and repository names:
    #
    #   repo:your-org/ice:ref:refs/heads/main                   (classic)
    #   repo:your-org@1234567/ice@12345678:ref:refs/heads/main  (what it sends)
    #
    # Matching only the classic form fails with "Not authorized to perform
    # sts:AssumeRoleWithWebIdentity", which reads like a missing permission rather
    # than a claim that does not match. The IDs are wildcarded rather than pinned so
    # this keeps working if GitHub changes them; the owner and repository names are
    # still fixed, and a GitHub username cannot contain '@', so nothing else matches.
    condition {
      test     = "StringLike"
      variable = "token.actions.githubusercontent.com:sub"
      values = flatten([
        for subject in local.oidc_subjects : [
          "repo:${var.github_repo}:${subject}",
          "repo:${split("/", var.github_repo)[0]}@*/${split("/", var.github_repo)[1]}@*:${subject}",
        ]
      ])
    }
  }
}

resource "aws_iam_role" "github_actions" {
  name               = "${var.project}-github-actions-deploy"
  description        = "Assumed by GitHub Actions to apply the ${var.project} stack"
  assume_role_policy = data.aws_iam_policy_document.github_assume_role.json
}

# The stack this role manages spans VPC, EC2, ECR, IAM, Lambda, SES and
# Secrets Manager, and Terraform needs to read and tag everything it creates.
# Hand-writing a least-privilege policy for that surface is a project in itself;
# admin on a single-purpose account is the honest trade. Narrow it if this account
# ever hosts anything else.
resource "aws_iam_role_policy_attachment" "github_actions_admin" {
  role       = aws_iam_role.github_actions.name
  policy_arn = "arn:aws:iam::aws:policy/AdministratorAccess"
}
