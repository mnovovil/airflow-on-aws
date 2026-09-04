resource "aws_ecr_repository" "gdal" {
  name                 = "${local.name}/gdal-report"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }
}

# The GDAL base image is ~1.5 GiB per tag and every push adds one. Without this,
# storage cost grows linearly with commits for no benefit.
resource "aws_ecr_lifecycle_policy" "gdal" {
  repository = aws_ecr_repository.gdal.name

  policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Keep the 10 most recent images"
        selection = {
          tagStatus   = "any"
          countType   = "imageCountMoreThan"
          countNumber = 10
        }
        action = { type = "expire" }
      }
    ]
  })
}

# --------------------------------------------------------------------------- airflow

# The Airflow box's own image, built by CI from docker/airflow and pulled by tag.
#
# Separate from the GDAL repository rather than sharing one with a tag prefix: the
# lifecycle rule below counts images, and two unrelated build cadences sharing a
# retention window means a busy week of DAG changes can expire the GDAL image the
# worker is pinned to.
resource "aws_ecr_repository" "airflow" {
  name                 = "${local.name}/airflow"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }
}

# Smaller than the GDAL image but pushed more often — every deploy produces one,
# whether or not the requirements changed. Ten is the same window for the same
# reason: enough to roll back through, not enough to pay for indefinitely.
resource "aws_ecr_lifecycle_policy" "airflow" {
  repository = aws_ecr_repository.airflow.name

  policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Keep the 10 most recent images"
        selection = {
          tagStatus   = "any"
          countType   = "imageCountMoreThan"
          countNumber = 10
        }
        action = { type = "expire" }
      }
    ]
  })
}
