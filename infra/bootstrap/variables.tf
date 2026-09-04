variable "aws_region" {
  description = "Region for the state bucket"
  type        = string
  default     = "eu-north-1"
}

variable "project" {
  description = "Short name prefixed onto every resource"
  type        = string
  default     = "ice"
}

variable "github_repo" {
  description = "owner/repo allowed to assume the deploy role"
  type        = string
  default     = "your-org/ice"
}
