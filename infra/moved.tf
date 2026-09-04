# Every resource here used to carry `count = <orchestrator was ec2> ? 1 : 0`, which
# put it in state as `resource[0]`. Dropping the count changes the address to a bare
# `resource`, and Terraform reads that as "destroy the old one, create a new one" —
# which for the Airflow instance means losing the box and its database on the next
# apply, and for the security groups means a dependency-ordered replacement cascade.
#
# These blocks say "same resource, new address" instead. They are pure state
# bookkeeping: after one successful apply has migrated the state, this file can be
# deleted with no effect on the plan.

moved {
  from = aws_instance.airflow[0]
  to   = aws_instance.airflow
}

moved {
  from = random_password.airflow_admin[0]
  to   = random_password.airflow_admin
}

moved {
  from = aws_secretsmanager_secret.airflow_admin[0]
  to   = aws_secretsmanager_secret.airflow_admin
}

moved {
  from = aws_secretsmanager_secret_version.airflow_admin[0]
  to   = aws_secretsmanager_secret_version.airflow_admin
}

moved {
  from = aws_iam_role.airflow[0]
  to   = aws_iam_role.airflow
}

moved {
  from = aws_iam_role_policy.airflow[0]
  to   = aws_iam_role_policy.airflow
}

moved {
  from = aws_iam_role_policy_attachment.airflow_ssm[0]
  to   = aws_iam_role_policy_attachment.airflow_ssm
}

moved {
  from = aws_iam_instance_profile.airflow[0]
  to   = aws_iam_instance_profile.airflow
}

# ------------------------------------------------------------------ security groups

moved {
  from = aws_security_group.airflow[0]
  to   = aws_security_group.airflow
}

moved {
  from = aws_vpc_security_group_ingress_rule.airflow_from_lambda[0]
  to   = aws_vpc_security_group_ingress_rule.airflow_from_lambda
}

moved {
  from = aws_vpc_security_group_egress_rule.airflow_all[0]
  to   = aws_vpc_security_group_egress_rule.airflow_all
}

moved {
  from = aws_security_group.trigger_lambda[0]
  to   = aws_security_group.trigger_lambda
}

moved {
  from = aws_vpc_security_group_egress_rule.trigger_lambda_to_airflow[0]
  to   = aws_vpc_security_group_egress_rule.trigger_lambda_to_airflow
}

# ----------------------------------------------------------------- the trigger Lambda

moved {
  from = aws_lambda_function.trigger_dag[0]
  to   = aws_lambda_function.trigger_dag
}

moved {
  from = aws_iam_role.trigger_dag[0]
  to   = aws_iam_role.trigger_dag
}

moved {
  from = aws_iam_role_policy.trigger_dag[0]
  to   = aws_iam_role_policy.trigger_dag
}

moved {
  from = aws_iam_role_policy_attachment.trigger_dag_vpc[0]
  to   = aws_iam_role_policy_attachment.trigger_dag_vpc
}

moved {
  from = aws_cloudwatch_log_group.trigger_dag[0]
  to   = aws_cloudwatch_log_group.trigger_dag
}

moved {
  from = aws_lambda_permission.allow_s3[0]
  to   = aws_lambda_permission.allow_s3
}

moved {
  from = aws_s3_bucket_notification.source[0]
  to   = aws_s3_bucket_notification.source
}

# ------------------------------------------------------------------- ses identities
#
# A fixed sender/recipient pair became a for_each over var.ses_identities when the
# dev environment arrived — see the note in ses.tf. Without these, Terraform reads
# the address change as destroy-then-create, and destroying an SES identity throws
# away its verification: prod would stop sending mail until both links were clicked
# again, in an account where SES is still in the sandbox.
#
# The keys are prod's addresses, so these blocks are a no-op in every other
# environment. Terraform ignores a moved block whose source address does not exist.

moved {
  from = aws_sesv2_email_identity.sender
  to   = aws_sesv2_email_identity.this["sender@example.com"]
}

moved {
  from = aws_sesv2_email_identity.recipient
  to   = aws_sesv2_email_identity.this["recipient@example.com"]
}

# --------------------------------------------------------------------- source bucket
#
# These carried `count = var.create_source_bucket ? 1 : 0` while prod's bucket was a
# data lookup, so dev's state holds them at `[0]`. Now that both environments manage
# their bucket the count is gone, and without these blocks Terraform reads the address
# change as destroy-then-create — on a dev bucket whose force_destroy is true, which
# means it would empty and delete example-dem-dev rather than fail.
#
# No-ops in prod, whose state has never held any of the three.

moved {
  from = aws_s3_bucket.source[0]
  to   = aws_s3_bucket.source
}

moved {
  from = aws_s3_bucket_server_side_encryption_configuration.source[0]
  to   = aws_s3_bucket_server_side_encryption_configuration.source
}

moved {
  from = aws_s3_bucket_public_access_block.source[0]
  to   = aws_s3_bucket_public_access_block.source
}

# aws_s3_bucket_lifecycle_configuration.source deliberately has no block here. It kept
# its count — gated on var.source_bucket_expiry_days now instead of on which
# environment owns the bucket — so dev's address is still `[0]` and prod's is still
# absent. See the note on that resource in s3.tf.
