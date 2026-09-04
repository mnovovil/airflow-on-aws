# Wires this environment's source bucket to its trigger Lambda.
#
# S3 suffix filters are case-sensitive and support no wildcards, so ".tif" and
# ".TIF" need separate rules — hence the loop over var.raster_suffixes. Rules with
# distinct suffixes and an empty prefix do not overlap, which is what S3 rejects.
#
# NOTE: aws_s3_bucket_notification is authoritative for the whole bucket. If
# anything else ever needs a notification on that bucket, it has to be added here or
# this resource will silently remove it — which is also why each environment gets a
# bucket of its own. See the note in s3.tf.

resource "aws_lambda_permission" "allow_s3" {
  statement_id   = "AllowExecutionFromS3"
  action         = "lambda:InvokeFunction"
  function_name  = aws_lambda_function.trigger_dag.function_name
  principal      = "s3.amazonaws.com"
  source_arn     = local.source_bucket_arn
  source_account = local.account_id
}

resource "aws_s3_bucket_notification" "source" {
  bucket = local.source_bucket_id

  dynamic "lambda_function" {
    for_each = var.raster_suffixes

    content {
      id                  = "raster${replace(lambda_function.value, ".", "-")}"
      lambda_function_arn = aws_lambda_function.trigger_dag.arn
      events              = ["s3:ObjectCreated:*"]
      filter_suffix       = lambda_function.value
    }
  }

  # S3 validates that it can invoke the function at configuration time, so the
  # permission has to exist first.
  depends_on = [aws_lambda_permission.allow_s3]
}
