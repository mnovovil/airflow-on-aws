output "state_bucket" {
  description = "Set this as the backend bucket in infra/versions.tf"
  value       = aws_s3_bucket.state.id
}

output "github_actions_role_arn" {
  description = "Set as the AWS_DEPLOY_ROLE_ARN repository variable in GitHub"
  value       = aws_iam_role.github_actions.arn
}
