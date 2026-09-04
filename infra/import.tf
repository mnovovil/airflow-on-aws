# Adopting the buckets that predate — or now outlive — the stack that describes them.
#
# example-dem predates the project outright. example-dem-dev was created by hand
# just before the dev environment was written, and the first dev apply failed with
# BucketAlreadyOwnedByYou — S3's CreateBucket is not idempotent for a bucket you
# already own, and Terraform has no way to tell "someone made this for us" from "this
# name is taken". Importing is the reconciliation.
#
# The gate on env is not cosmetic, and it is newer than the blocks themselves. The
# 2026-09-02 teardown left the two environments starting from genuinely different
# places: dev's buckets were deleted outright (force_destroy_buckets = true), while
# prod's two were removed from state rather than deleted, because force_destroy is
# refused for prod and a destroy that reached them would have stripped their
# encryption and public-access-block configuration before failing on BucketNotEmpty.
#
# So prod has two buckets to adopt and dev has none. An unconditional import of a
# bucket that does not exist does not quietly create it — it fails the plan with
# "Cannot import non-existent remote object", which would block every dev apply from
# here on. for_each over an empty set is how an import block is made conditional;
# there is no count for import.
#
# Unlike the one-shot version this replaces, these do not become dead weight after the
# first apply: an import block naming a resource already in state is a no-op, so they
# cost prod nothing on subsequent applies and stay correct if prod is ever torn down
# and rebuilt again. See the Rebuilding section of the README.

import {
  for_each = var.env == "prod" ? toset([var.source_bucket]) : toset([])

  to = aws_s3_bucket.source
  id = each.value
}

import {
  for_each = var.env == "prod" ? toset([local.artifacts_bucket]) : toset([])

  to = aws_s3_bucket.artifacts
  id = each.value
}
