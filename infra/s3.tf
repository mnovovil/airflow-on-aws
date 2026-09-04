# Artifacts bucket: the DAG source the Airflow box syncs from, AND the destination
# for gdalinfo reports.
#
# Reports deliberately do NOT go back into example-dem. Writing outputs into the
# bucket that triggers the pipeline is how you get a notification loop; keeping them
# separate makes that structurally impossible rather than merely unlikely.

resource "aws_s3_bucket" "artifacts" {
  bucket = local.artifacts_bucket

  # dev only — see the variable. Prod's reports and DAG source are not disposable.
  force_destroy = var.force_destroy_buckets
}

# Versioned so that a bad DAG sync is recoverable, and so the lifecycle rules below
# have old versions to expire rather than live objects.
resource "aws_s3_bucket_versioning" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Nothing here is ever meant to be world-readable: DAG source on the way in, raster
# metadata on the way out.
resource "aws_s3_bucket_public_access_block" "artifacts" {
  bucket                  = aws_s3_bucket.artifacts.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id

  rule {
    id     = "expire-old-report-versions"
    status = "Enabled"

    filter {
      prefix = "reports/"
    }

    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }

  # The DAG sync writes a new version on every deploy, so this prefix accumulates
  # faster than the reports do.
  rule {
    id     = "expire-old-dag-versions"
    status = "Enabled"

    filter {
      prefix = "dags/"
    }

    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }

  # collection.json is rewritten in full on every stac_publish run — daily, whether
  # or not anything changed — so without this the catalogue's own history outgrows
  # the catalogue within a couple of months.
  rule {
    id     = "expire-old-stac-versions"
    status = "Enabled"

    filter {
      prefix = local.stac_prefix
    }

    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }
}

# There was an aws_s3_object.requirements here, uploading dags/requirements.txt for
# the bootstrap to fetch and build an image from. It is gone with the on-box build:
# the file now lives in docker/airflow and is baked into the image by CI. Updating
# an S3 object changed nothing about a box that had already built from it, which is
# how a DAG shipped to dev without the dependency it imported.

# ------------------------------------------------------------------- source bucket
#
# Where rasters land, and the one bucket the two environments must not share.
#
# aws_s3_bucket_notification is authoritative for an entire bucket: it describes the
# complete notification configuration, not a contribution to it. Two stacks pointed
# at one bucket would therefore each wipe the other's triggers on every apply, and
# the symptom is an upload that silently produces nothing rather than an error. So
# each environment keeps a bucket of its own — prod example-dem, dev
# example-dem-dev — and both are described here.
#
# Both predate their definition in this file and are adopted by the import blocks in
# import.tf rather than created. Terraform therefore owns their configuration from
# the first apply onwards, which is the point: before this, prod's bucket was a
# `data` lookup, so the only thing the stack could say about example-dem was its
# name, and every setting on it was whatever the console last left there.
#
# Adoption is not free. A managed bucket is a bucket `terraform destroy` will try to
# delete. Prod's protection is force_destroy = false — a destroy stops on a bucket
# holding objects rather than emptying it — which is the same catch the artifacts
# bucket has always relied on, and it is a weaker guarantee than "Terraform cannot
# touch it at all". An empty prod source bucket is now destroyable where it was not
# before.
resource "aws_s3_bucket" "source" {
  bucket = var.source_bucket

  # dev only — see the variable. Prod's rasters are not disposable.
  force_destroy = var.force_destroy_buckets
}

# bucket_key_enabled is set explicitly because example-dem already has it on:
# leaving it unset makes the provider send a rule without the field, which S3 reads
# as false, and adopting a bucket should not quietly turn one of its settings off.
resource "aws_s3_bucket_server_side_encryption_configuration" "source" {
  bucket = aws_s3_bucket.source.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "source" {
  bucket                  = aws_s3_bucket.source.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Test rasters are large and land here by the handful. Left alone this is the one
# part of a dev environment whose cost grows without anyone deciding it should.
#
# Gated on the retention variable rather than on which environment owns the bucket,
# and that distinction is the whole reason this resource is not simply uncounted like
# the three above. Prod's example-dem holds the real rasters and everything
# move_file has archived under sent/; an expiry rule inherited from dev would delete
# all of it on a 30-day delay, silently and after the apply that caused it had
# scrolled away. Prod passes null, which removes the rule entirely.
resource "aws_s3_bucket_lifecycle_configuration" "source" {
  count  = var.source_bucket_expiry_days == null ? 0 : 1
  bucket = aws_s3_bucket.source.id

  rule {
    id     = "expire-test-rasters"
    status = "Enabled"

    filter {}

    expiration {
      days = var.source_bucket_expiry_days
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}
