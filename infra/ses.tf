# SES is in the sandbox on this account (ProductionAccessEnabled = false), which
# means 200 messages/day, 1/second, and BOTH the sender and every recipient must be
# verified. Terraform creates the identities; the verification links still have to
# be clicked in the respective inboxes. Until that happens, SES accepts nothing.
#
# `terraform apply` will not wait for or detect verification — check with:
#   aws sesv2 get-email-identity --email-identity <address>

# One resource per address this environment owns, rather than a fixed sender and
# recipient pair. SES identities are account-global: if both environments declared
# the same address, the second apply would fail with AlreadyExists, and importing
# one stack's identity into the other's state would leave whichever ran `destroy`
# first free to un-verify the survivor's sender.
#
# So the environments hold disjoint sets — prod the plain addresses, dev the
# plus-addressed ones — and each verifies its own. See var.ses_identities.
resource "aws_sesv2_email_identity" "this" {
  for_each = toset(var.ses_identities)

  email_identity = each.value
}

# ------------------------------------------------------------------ smtp credentials

# Airflow talks to SES over SMTP, which needs an IAM user rather than a role —
# SMTP credentials are a static username/password pair and cannot come from STS.
resource "aws_iam_user" "ses_smtp" {
  name = "${local.name}-ses-smtp"
  path = "/service/"
}

resource "aws_iam_user_policy" "ses_smtp" {
  name = "send-email"
  user = aws_iam_user.ses_smtp.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["ses:SendRawEmail", "ses:SendEmail"]
        Resource = "*"
        Condition = {
          StringEquals = {
            "ses:FromAddress" = var.email_from
          }
        }
      }
    ]
  })
}

# ses_smtp_password_v4 is the secret key run through SES's SigV4 derivation — the
# provider does that conversion, so the password never has to be produced by hand.
resource "aws_iam_access_key" "ses_smtp" {
  user = aws_iam_user.ses_smtp.name
}
