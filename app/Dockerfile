# GDAL with the Python bindings already built — do not try to pip install GDAL,
# the wheel has to match the system libgdal and that fight is not worth having.
FROM ghcr.io/osgeo/gdal:ubuntu-small-3.9.2

WORKDIR /app

COPY requirements.txt .
# The ubuntu-small GDAL images carry the Python bindings but no pip at all, so it
# has to come from apt first.
#
# Ubuntu 24.04 then marks its Python as externally managed (PEP 668); this image has
# no venv and is single-purpose, so installing into the system interpreter is
# correct and --break-system-packages is the way to say so.
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3-pip \
    && rm -rf /var/lib/apt/lists/* \
    && python3 -m pip install --no-cache-dir --break-system-packages -r requirements.txt

# Two entry points share this image because they share the expensive part of it —
# libgdal and its Python bindings — and a second image would mean a second ECR
# repository, a second build in the deploy workflow and a second secret to keep
# pinned. gfs_rain.py imports helpers from gdal_report.py, so they also have to sit
# on the same sys.path, which /app is.
COPY gdal_report.py gfs_rain.py ./

# Sensible GDAL defaults for reading single objects out of S3: don't list the
# whole prefix on open, and cap how long a stuck read can hang.
#
# PAM off matters more than it looks: computing statistics makes GDAL try to persist
# them to a `<raster>.aux.xml` sidecar beside the source, which over /vsis3 means a
# PutObject back into the trigger bucket. The worker has no write access there, so it
# would only ever produce AccessDenied noise — and if it were ever granted, writing
# into the bucket that triggers the pipeline is the loop this design avoids.
ENV GDAL_DISABLE_READDIR_ON_OPEN=EMPTY_DIR \
    GDAL_HTTP_TIMEOUT=60 \
    GDAL_HTTP_CONNECTTIMEOUT=15 \
    CPL_VSIL_CURL_CHUNK_SIZE=1048576 \
    GDAL_PAM_ENABLED=NO \
    PYTHONUNBUFFERED=1

# The elevation pipeline's entry point, because it is the one that runs on every
# upload. The gdal_weather DAG overrides it with `--entrypoint python3` and names
# /app/gfs_rain.py explicitly.
ENTRYPOINT ["python3", "/app/gdal_report.py"]
