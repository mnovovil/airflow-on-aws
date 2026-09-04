# Development — deployed from the `dev` branch.
#
#     terraform init -backend-config=backends/dev.hcl
#     terraform apply -var-file=envs/dev.tfvars
#
# Identical to prod except for the four things that have to differ: the name prefix
# (derived from env), the bucket it watches, its VPC range, and the addresses it
# sends from and to. Everything else — instance types, tuning, retention, the DAG,
# the image — is deliberately the same, because a dev environment that differs in
# those is not rehearsing the deploy that matters.

env = "dev"

# Owned here, and prod's is now owned by prod — both are adopted by the import block
# in import.tf. Two stacks still cannot share one source bucket:
# aws_s3_bucket_notification describes a bucket's complete notification config, so
# they would take turns deleting each other's triggers. See the note in s3.tf.
source_bucket = "example-dem-dev"

# Test rasters are large and land here by the handful, and nothing uploaded to dev is
# worth keeping. Prod leaves this null, which removes the rule rather than lengthening
# it — see the note in prod.tfvars.
source_bucket_expiry_days = 30

# dev is meant to be disposable — `terraform destroy` should actually finish rather
# than stop at a bucket holding last week's test rasters.
force_destroy_buckets = true

# Distinct from prod's 10.20.0.0/16. The two VPCs are not peered, so this is about
# being able to tell them apart, not about routing.
vpc_cidr = "10.21.0.0/16"

# The same addresses prod uses, on purpose: dev's reports land in the ordinary
# inbox, and there is no verification step to do before dev can send anything.
#
# The trade is that a dev report is not distinguishable from a real one by its
# headers — same From:, same To:, and the subject is built from the raster's name in
# both environments. The bucket named in the report body is the only tell.
email_from = "sender@example.com"
email_to   = "recipient@example.com"

# Empty, and it has to be. SES identities are account-global, so this list is
# "addresses this stack CREATES", not "addresses it may use" — declaring an address
# prod already owns fails the apply with AlreadyExists, and a later `terraform
# destroy` of dev would un-verify a sender prod is still sending with.
#
# Both addresses above are verified by prod, which is enough: SES checks that an
# identity is verified, not which stack verified it. The dependency runs one way and
# is worth knowing about — dev cannot send mail into an account where prod's
# identities have been destroyed.
ses_identities = []
