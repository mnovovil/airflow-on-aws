"""Shared setup for the test suite.

Airflow puts the DAGs folder itself on ``sys.path``, which is what makes the DAG's
``from common import ec2_ssm`` resolve there. Nothing does that under pytest, so it
has to happen before any test imports a DAG module.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DAGS_DIR = ROOT / "dags"
LAMBDA_DIR = ROOT / "lambda" / "trigger_dag"

sys.path.insert(0, str(DAGS_DIR))

# The DAG resolves configuration from Secrets Manager at run time, not import time,
# so no AWS credentials are needed — but Airflow wants a home, and boto3 clients
# constructed at import time want a region.
os.environ.setdefault("AIRFLOW_HOME", "/tmp/airflow-test")
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")
os.environ.setdefault("AWS_DEFAULT_REGION", "eu-north-1")


# One sample report, shared by the three files that need one: the STAC mapping, the
# DAG task that writes an item, and the rebuild DAG that backfills them. Here rather
# than in whichever of them was written first, because a second copy is a copy that
# can be updated on its own — and the whole point of these fixtures is that all three
# are looking at the same raster.
#
# Trimmed to the fields the mapping reads. The geotransform and size are the
# interesting part: proj:transform and proj:shape are derived from them, and both
# derivations reorder their input.
GDALINFO = {
    "driverShortName": "GTiff",
    "driverLongName": "GeoTIFF",
    "size": [1200, 900],
    "geoTransform": [400000.0, 30.0, 0.0, 6800000.0, 0.0, -30.0],
    "coordinateSystem": {"wkt": 'PROJCRS["WGS 84 / UTM zone 33N",BASEGEOGCRS["WGS 84"]]'},
    "cornerCoordinates": {"upperLeft": [400000.0, 6800000.0]},
    "wgs84Extent": {
        "type": "Polygon",
        "coordinates": [
            [[10.0, 61.3], [10.0, 61.0], [10.7, 61.0], [10.7, 61.3], [10.0, 61.3]],
        ],
    },
}

SUMMARY = {
    "source": "s3://example-dem/scenes/n61e010.tif",
    "file_name": "n61e010.tif",
    "size_bytes": 48_234_112,
    "size_human": "46.0 MiB",
    "driver": "GTiff (GeoTIFF)",
    "crs": {"name": "WGS 84 / UTM zone 33N", "epsg": 32633, "units": "metre"},
    "width": 1200,
    "height": 900,
    "band_count": 1,
    "bands": [
        {
            "index": 1,
            "type": "Int16",
            "color_interpretation": "Gray",
            "nodata": -32768.0,
            "min": 12.0,
            "max": 2469.0,
            "mean": 843.2,
            "stddev": 401.7,
        }
    ],
    "processed_at": "2026-08-11T09:15:00+00:00",
}
