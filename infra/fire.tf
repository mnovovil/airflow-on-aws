# Everything the `fire` DAG runs on. secrets.tf folds local.fire into ice_config, and
# dags/fire.py carries a fallback for every value here so a DAG deployed ahead of an
# apply still runs — the fallbacks are not the configuration; these are.

# NASA FIRMS issues a MAP_KEY per email address from
# https://firms.modaps.eosdis.nasa.gov/api/map_key/. It is a credential: anyone holding
# it can spend the account's request quota. No value here and none in envs/*.tfvars,
# both of which are committed — supply it out of band:
#
#     export TF_VAR_fire_key=...                 # locally, from your password manager
#     TF_VAR_fire_key: ${{ secrets.FIRE_KEY }}   # in the deploy workflow
#
# Defaulted to "" rather than left required so an apply that does not touch this DAG
# still works; store_csv_s3 raises a message naming this file when it finds it empty.
variable "fire_key" {
  description = "NASA FIRMS MAP_KEY used by the fire DAG. Supplied via TF_VAR_fire_key, never committed."
  type        = string
  default     = ""
  sensitive   = true
}

locals {
  fire = {
    map_key = var.fire_key

    # Source of the bounding box the FIRMS query takes and of the outline the emailed
    # chart is drawn on. 110m is the coarsest of the three files and shows it at chart
    # size; swapping it for 50m in this URL is the whole change.
    countries_url = "https://naciscdn.org/naturalearth/110m/cultural/ne_110m_admin_0_countries.zip"

    # Objects land at <prefix>/<source>/<ds>/<country>.csv inside the artifacts bucket.
    # Not "sent/", which is what gdalinfo_notify archives rasters under in the *source*
    # bucket — different bucket, different lifecycle, and one name invites one rule.
    prefix = "fires"

    # Run-wide defaults for the FIRMS query; either can be overridden per country below.
    # VIIRS_SNPP_NRT is a second satellite about 50 minutes apart, and MODIS_NRT is
    # older with 1km pixels against VIIRS's 375m.
    source = "VIIRS_NOAA20_NRT"
    days   = 2

    # One email per entry, in this order. `country` is matched against Natural Earth's
    # SOV_A3 — the sovereignty code, so France and its overseas departments share FRA.
    # `timezone` is carried but not read yet (see country_timezone in dags/fire.py).
    countries = [
      { country = "ESP", timezone = "Europe/Madrid" },
    ]
  }
}
