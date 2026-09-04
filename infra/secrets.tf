# Airflow reads connections and variables straight out of Secrets Manager (see the
# secrets.backend config in airflow_compose.yml), so anything the DAG needs at
# runtime is defined here rather than clicked into the Airflow UI.
#
# Every name here is under local.secrets_prefix — "airflow" for prod, "airflow-dev"
# for dev. The DAG asks for the same connection id and variable names in both
# environments; the prefix its scheduler was configured with is the only thing that
# decides which set it gets. That is what keeps one DAG file correct on both boxes.

# ------------------------------------------------------------------- smtp connection

resource "aws_secretsmanager_secret" "smtp" {
  name                    = "${local.secrets_prefix}/connections/smtp_default"
  description             = "SES SMTP credentials used by the gdalinfo_notify DAG"
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "smtp" {
  secret_id = aws_secretsmanager_secret.smtp.id

  # Port 465 (implicit TLS) rather than 587 (STARTTLS): the smtp provider picks
  # SMTP_SSL when disable_ssl is false, which is unambiguous. 587 would need
  # disable_ssl=true and relies on the STARTTLS upgrade.
  #
  # disable_ssl is not sufficient on its own — see the extra below.
  secret_string = jsonencode({
    conn_type = "smtp"
    host      = "email-smtp.${var.aws_region}.amazonaws.com"
    port      = 465
    login     = aws_iam_access_key.ses_smtp.id
    password  = aws_iam_access_key.ses_smtp.ses_smtp_password_v4
    extra = jsonencode({
      from_email  = var.email_from
      disable_ssl = false
      # Two independent switches, and both have to be set. disable_ssl=false picks
      # SMTP_SSL, which is right for 465 — but the hook then applies STARTTLS on top
      # regardless, driven by disable_tls alone. On an already-encrypted connection
      # SES answers "STARTTLS extension not supported by server" and the task fails.
      disable_tls = true
      timeout     = 30
    })
  })
}

# ---------------------------------------------------------------- airflow variables

# One JSON blob of stable configuration, read by the DAG through
# Variable.get("ice_config", deserialize_json=True).
resource "aws_secretsmanager_secret" "ice_config" {
  name                    = "${local.secrets_prefix}/variables/ice_config"
  description             = "Runtime configuration for the gdalinfo_notify DAG"
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "ice_config" {
  secret_id = aws_secretsmanager_secret.ice_config.id

  secret_string = jsonencode({
    instance_id      = aws_instance.worker.id
    artifacts_bucket = aws_s3_bucket.artifacts.id
    source_bucket    = local.source_bucket_id
    report_prefix    = "reports"
    email_to         = var.email_to
    filepath_txt     = "/tmp/trigger.txt"
    LOW              = 1
    HIGH             = 100
    # The stocks the `stock` DAG reports on, one email per entry and in this order. A
    # trigger-with-config run can name a single different one for that run; this is
    # what every scheduled run uses, and the DAG falls back to its own constant if
    # this key is missing or empty.
    stocks = [
      { ticker = "SGO.PA", name = "Saint-Gobain", currency = "€" },
    ]
    # The weather stations the `temperature` DAG reports on, same arrangement as the
    # stocks above: one email per entry, in this order, and the DAG falls back to its
    # own constant if this key is missing or empty.
    #
    # ``id`` is a Meteostat station id, not an ICAO or WMO code — 72503 is LaGuardia,
    # which Meteostat also answers to as KLGA. Every task downstream looks a station
    # up by this key, so an entry spelled ``station`` or ``station_id`` is not a
    # failed run: it is a run that quietly reports the DAG's default station instead.
    #
    # ``timezone`` is what makes the report's "day" a local one. Meteostat stamps its
    # observations UTC unless it is told otherwise, so leaving this off a station
    # outside New York gives it a day that starts at the wrong hour and a chart whose
    # hours are labelled as somebody else's.
    temperature = [
      { id = "72503", name = "LaGuardia Airport", timezone = "America/New_York" },
    ]
    # Everything the `fire` DAG runs on, defined in fire.tf so that the FIRMS
    # credential sits beside the note on how it is supplied. Its shape is documented
    # there rather than here.
    fire          = local.fire
    region        = var.aws_region
    ssm_log_group = aws_cloudwatch_log_group.ssm_output.name
    start_timeout = 300
    gdal_timeout  = 1800
    # How long wait_for_object polls for the raster before failing the run. Short on
    # purpose: the object is already there in the normal case — the notification is
    # what raced ahead of it — so this only has to cover the tail of an upload, and
    # every second of it is a second before the worker is started.
    object_timeout = 60
  })
}

# The image tag changes on every deploy, so this one lives on its own and is
# updated by the deploy workflow rather than by Terraform. Terraform seeds it with
# :latest so the DAG is runnable the moment the environment comes up, then stops
# tracking the value.
resource "aws_secretsmanager_secret" "gdal_image_uri" {
  name                    = "${local.secrets_prefix}/variables/gdal_image_uri"
  description             = "ECR image the GDAL worker runs — updated by CI on each deploy"
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "gdal_image_uri" {
  secret_id     = aws_secretsmanager_secret.gdal_image_uri.id
  secret_string = "${aws_ecr_repository.gdal.repository_url}:latest"

  # CI overwrites this with an immutable SHA tag after every image build. Without
  # this, every terraform apply would drag the worker back to :latest and undo the
  # deploy that just happened.
  lifecycle {
    ignore_changes = [secret_string]
  }
}

# The same arrangement for the Airflow box's own image, and it is what replaced
# building that image on the box. Note the name: this one is NOT under
# ${local.secrets_prefix}/variables/, because nothing in a DAG reads it — the
# bootstrap's image timer does, the way it reads the admin login.
resource "aws_secretsmanager_secret" "airflow_image_uri" {
  name                    = local.airflow_image_secret
  description             = "ECR image the Airflow box runs — updated by CI on each deploy"
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "airflow_image_uri" {
  secret_id     = aws_secretsmanager_secret.airflow_image_uri.id
  secret_string = "${aws_ecr_repository.airflow.repository_url}:latest"

  # Seeded with :latest and then left alone, exactly as above. A fresh environment
  # therefore comes up pointing at a tag that does not exist yet — see the wait in
  # the bootstrap, which is the deliberate cost of the box no longer building its
  # own image.
  lifecycle {
    ignore_changes = [secret_string]
  }
}
