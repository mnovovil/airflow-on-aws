# The image the Airflow box runs.
#
# Built by CI and pushed to this environment's ECR repository, exactly like the GDAL
# image in app/ — not built on the box. That distinction is the whole point of this
# directory: the previous arrangement fetched requirements.txt from S3 during
# user_data and ran `docker compose up --build` on a t3.small at first boot, which
# meant a dependency change could only reach the box by replacing the instance. It
# silently did not, and a DAG that imported yfinance failed to parse in dev with the
# package sitting in the repository.
#
# Keep this in step with the constraint URL in requirements.txt: the Airflow version
# in the tag and the version the constraints describe have to be the same one.
FROM apache/airflow:2.10.3-python3.11

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# matplotlib, which sgo.py imports at module scope to draw the intraday chart, is
# installed by the line above like everything else — its manylinux wheel bundles
# freetype and its own fonts, so there is no apt-get to go with it.
#
# What it does need is somewhere to put its font cache. Unset, it writes to $HOME and
# logs a warning and rebuilds the cache on every miss; the DAG file imports pyplot on
# every parse, so that is the scheduler's inner loop. Baked in here rather than left
# to the box: this is a property of the image, not of the environment it runs in.
ENV MPLCONFIGDIR=/tmp/matplotlib
