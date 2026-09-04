# Production — deployed from the `main` branch.
#
#     terraform init -backend-config=backends/prod.hcl
#     terraform apply -var-file=envs/prod.tfvars
#
# The pairing of this file with backends/prod.hcl is not optional and not checked by
# Terraform: applying these values over dev's state would rename every resource in
# it. CI derives both from the branch name so the two cannot drift; do the same by
# hand, or use `terraform workspace`-style discipline and check `terraform output env`
# before applying.

env = "prod"

# Predates this stack, and adopted by the import block in import.tf rather than
# created. Whatever else writes to example-dem keeps working — adoption changes who
# describes the bucket's configuration, not who may put objects in it.
source_bucket = "example-dem"

# Unset, and the variable's validation refuses it for prod. This bucket holds the
# real rasters and everything move_file has archived under sent/; dev's 30-day expiry
# rule applied here would delete all of it on a delay.
source_bucket_expiry_days = null

# Never true here. A destroy of prod should stop at a bucket holding reports rather
# than empty it — the variable's own validation enforces this.
force_destroy_buckets = false

vpc_cidr = "10.20.0.0/16"

email_from = "sender@example.com"
email_to   = "recipient@example.com"

# Both plain addresses belong to prod. Verified long ago; the moved blocks in
# moved.tf are what keep them that way through the switch to for_each.
ses_identities = [
  "sender@example.com",
  "recipient@example.com",
]
