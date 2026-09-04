# The private address is the one that matters: it is what the trigger Lambda posts
# to. Reaching the UI is a port-forward to this instance rather than a public
# address — see the airflow_ui_ingress_cidrs variable.
output "airflow_ui" {
  description = "Airflow web UI, on the VPC-internal address"
  value       = "http://${aws_instance.airflow.private_ip}:8080"
}

output "airflow_instance_id" {
  description = "The Airflow box — port-forward to this to open the UI"
  value       = aws_instance.airflow.id
}

output "airflow_admin_secret" {
  description = "Secrets Manager secret holding the Airflow UI login"
  value       = aws_secretsmanager_secret.airflow_admin.name
}

output "env" {
  description = "Which environment this state describes — prod or dev"
  value       = var.env
}

output "artifacts_bucket" {
  description = "DAG source and gdalinfo report destination"
  value       = aws_s3_bucket.artifacts.id
}

output "source_bucket" {
  description = "Bucket to upload a raster to in order to trigger this environment"
  value       = local.source_bucket_id
}

# The deploy workflow overwrites this secret with the image it just built. Reading
# the name from the stack rather than assembling it in YAML means the environments'
# prefixes are defined in exactly one place.
output "gdal_image_secret_id" {
  description = "Secrets Manager entry holding the image the worker runs — written by CI on each deploy"
  value       = aws_secretsmanager_secret.gdal_image_uri.name
}

output "ecr_repository_url" {
  description = "Push the GDAL image here"
  value       = aws_ecr_repository.gdal.repository_url
}

output "airflow_ecr_repository_url" {
  description = "Push the Airflow image here"
  value       = aws_ecr_repository.airflow.repository_url
}

# Read by the deploy workflow for the same reason as gdal_image_secret_id: the
# prefix differs per environment and belongs in one place.
output "airflow_image_secret_id" {
  description = "Secrets Manager entry holding the image the Airflow box runs — written by CI on each deploy"
  value       = aws_secretsmanager_secret.airflow_image_uri.name
}

output "worker_instance_id" {
  description = "GDAL worker — normally stopped, started per DAG run"
  value       = aws_instance.worker.id
}

output "trigger_lambda_log_group" {
  description = "First place to look when an upload does not produce a DAG run"
  value       = aws_cloudwatch_log_group.trigger_dag.name
}

output "email_to" {
  description = "Where this environment's reports are sent"
  value       = var.email_to
}

output "ses_verification_pending" {
  description = "Identities this environment creates — each needs its verification link clicked before mail flows"
  value       = sort(var.ses_identities)
}
