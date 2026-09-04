"""Turn a gdalinfo report into STAC.

Pure functions over dictionaries: no Airflow, no boto3, no network. That is what
lets the whole mapping be exercised from a fixture in the test suite, and it is why
the STAC objects are built as plain dicts rather than with pystac — a library that
would have to be added to ``dags/requirements.txt``, which is a Terraform-managed
object wired into the Airflow box's user data. Adding a dependency there replaces
the instance. pystac is a *test* dependency instead, used to validate what these
functions produce against the published schemas, which is the stricter check.

The input is the full ``gdalinfo.json``, not ``summary.json``. The summary drops the
raw geotransform and the CRS WKT, and reconstructing them from pixel size and origin
is lossy for a rotated raster.

Everything is written under one prefix in the artifacts bucket:

    stac/catalog.json
    stac/<collection>/collection.json
    stac/<collection>/items/<item_id>.json

Links between them are relative, so the tree can be copied between buckets — or
pulled down and served from somewhere else — without a rewrite.
"""

from __future__ import annotations

import posixpath
import re
from typing import Any

# STAC 1.0.0 rather than 1.1.0. 1.1.0 moves band metadata into a core ``bands``
# field and deprecates the raster extension; tooling has not uniformly followed, and
# nothing here needs what 1.1.0 adds.
STAC_VERSION = "1.0.0"

PROJ_EXTENSION = "https://stac-extensions.github.io/projection/v1.1.0/schema.json"
RASTER_EXTENSION = "https://stac-extensions.github.io/raster/v1.1.0/schema.json"
FILE_EXTENSION = "https://stac-extensions.github.io/file/v2.1.0/schema.json"

CATALOG_PREFIX = "stac"
COLLECTION_ID = "elevation"

CATALOG_ID = "ice"
CATALOG_DESCRIPTION = "Rasters processed by the ice pipeline"
COLLECTION_TITLE = "Elevation rasters"
COLLECTION_DESCRIPTION = (
    "Rasters uploaded to the source bucket, inspected with gdalinfo and archived. "
    "One item per raster that passed validation."
)

# Every asset here is an object in a bucket with public access blocked, so there is
# no href that a reader can fetch anonymously. s3:// is the honest choice: a client
# with credentials can resolve it, and one without fails at the URI rather than on a
# 403 from a link that looked public.
LICENSE = "proprietary"

# raster:bands names its statistics differently from the report container's summary.
STATISTICS_FIELDS = (("minimum", "min"), ("maximum", "max"), ("mean", "mean"), ("stddev", "stddev"))

MEDIA_TYPES = {
    ".tif": "image/tiff; application=geotiff",
    ".tiff": "image/tiff; application=geotiff",
    ".jp2": "image/jp2",
    ".img": "application/octet-stream",
    ".vrt": "application/xml",
}


class StacError(ValueError):
    """The report cannot be expressed as a STAC item."""


# ------------------------------------------------------------------------ identity


def item_id(key: str, archive_prefix: str = "") -> str:
    """A stable id for the raster at ``key``.

    ``archive_prefix`` is stripped first, which is what keeps the id the same before
    and after ``move_file`` relocates the object. Without that, re-running the DAG by
    hand against an already-archived key would mint a second item for one raster.

    The prefix is a parameter rather than a constant here because the DAG, the IAM
    policy and the trigger Lambda already have to agree on that string and
    tests/test_archive_wiring.py enforces it; a fourth copy is a fourth thing to
    drift.
    """
    if archive_prefix and key.startswith(archive_prefix):
        key = key[len(archive_prefix) :]

    stem, _, extension = key.rpartition(".")
    # rpartition returns ("", "", key) when there is no dot at all, and a bare
    # extension check is not enough: "a.b/c" has a dot before the last slash.
    if not stem or "/" in extension:
        stem = key

    slug = re.sub(r"[^A-Za-z0-9._-]+", "_", stem).strip("_.")
    if not slug:
        raise StacError(f"no usable item id can be derived from {key!r}")
    return slug


def item_key(identifier: str, collection_id: str = COLLECTION_ID) -> str:
    return f"{CATALOG_PREFIX}/{collection_id}/items/{identifier}.json"


def collection_key(collection_id: str = COLLECTION_ID) -> str:
    return f"{CATALOG_PREFIX}/{collection_id}/collection.json"


def catalog_key() -> str:
    return f"{CATALOG_PREFIX}/catalog.json"


# -------------------------------------------------------------------------- pieces


def _bbox(geometry: dict) -> list[float]:
    def coordinates(node: Any):
        if isinstance(node, int | float):
            return
        if node and isinstance(node[0], int | float):
            yield node
            return
        for child in node:
            yield from coordinates(child)

    points = list(coordinates(geometry.get("coordinates") or []))
    if not points:
        raise StacError("geometry carries no coordinates")

    xs = [point[0] for point in points]
    ys = [point[1] for point in points]
    return [min(xs), min(ys), max(xs), max(ys)]


def _transform(geotransform: list[float] | None) -> list[float] | None:
    """GDAL's six-element geotransform as the nine-element affine STAC wants.

    GDAL orders it (origin_x, pixel_width, row_rotation, origin_y, column_rotation,
    pixel_height); ``proj:transform`` is the first two rows of the affine matrix in
    row-major order, followed by the constant third row. The two are not a reordering
    of each other by accident — writing GDAL's tuple straight through produces a
    matrix that is wrong everywhere except an untranslated north-up raster.
    """
    if not geotransform or len(geotransform) < 6:
        return None
    origin_x, pixel_width, row_rotation, origin_y, column_rotation, pixel_height = geotransform[:6]
    return [pixel_width, row_rotation, origin_x, column_rotation, pixel_height, origin_y, 0.0, 0.0, 1.0]


def _media_type(key: str) -> str:
    _, extension = posixpath.splitext(key.lower())
    return MEDIA_TYPES.get(extension, "application/octet-stream")


def _raster_bands(summary: dict) -> list[dict]:
    """``raster:bands``, one entry per band of the source.

    Statistics come from ``ComputeStatistics(True)`` in the report container — an
    approximation over overviews rather than a full pass. They are published because
    an order of magnitude is useful, and flagged as approximate on each band because
    a consumer treating them as exact would be wrong about the tails in particular.
    """
    bands = []
    for band in summary.get("bands") or []:
        entry: dict[str, Any] = {}
        if band.get("type"):
            entry["data_type"] = str(band["type"]).lower()
        if band.get("nodata") is not None:
            entry["nodata"] = band["nodata"]
        statistics = {
            name: band[source] for name, source in STATISTICS_FIELDS if band.get(source) is not None
        }
        if statistics:
            entry["statistics"] = statistics
            entry["ice:statistics_are_approximate"] = True
        elif band.get("stats_error"):
            entry["ice:statistics_error"] = band["stats_error"]
        bands.append(entry)
    return bands


# --------------------------------------------------------------------------- items


def build_item(
    gdalinfo: dict,
    summary: dict,
    *,
    source_bucket: str,
    raster_key: str,
    artifacts_bucket: str,
    report_key: str,
    archive_prefix: str = "",
    collection_id: str = COLLECTION_ID,
) -> dict:
    """One STAC Item for one raster.

    ``raster_key`` is where the object lives *now* — after ``move_file``, under the
    archive prefix — because that is what the data asset has to point at. The item id
    is derived from it with the prefix stripped, so it does not change when the object
    moves. ``report_key`` is the ``reports/<key>`` prefix holding the two JSON
    documents this was built from, which are published as assets in their own right:
    the item is a summary of them, and a consumer that wants the full band table
    should be able to find it.
    """
    geometry = gdalinfo.get("wgs84Extent")
    if not geometry:
        # gdalinfo omits this when it cannot transform the raster's corners into
        # WGS84, which is exactly the case an item cannot describe: STAC requires a
        # geometry in a geographic CRS. validate_report already rejects a raster with
        # no EPSG code, so reaching this means something rarer — a CRS that identifies
        # but does not transform.
        raise StacError(f"gdalinfo reported no wgs84Extent for {raster_key}; cannot place the item in space")

    identifier = item_id(raster_key, archive_prefix)
    crs = summary.get("crs") or {}
    size = gdalinfo.get("size") or [None, None]

    properties: dict[str, Any] = {
        # The pipeline never learns when the raster was captured — nothing upstream
        # carries an acquisition time — so this is the moment the report was written.
        # Recorded rather than implied: a consumer filtering by datetime is filtering
        # on processing order, and ice:datetime_source is how they can tell.
        "datetime": summary["processed_at"],
        "ice:datetime_source": "processed_at",
        # Written unconditionally, and null is the extension's way of saying "no
        # authority code for this CRS" rather than a missing field. Not expected here
        # — validate_report deletes a raster without an EPSG — but a hand-triggered
        # run reaches this code without passing that branch.
        "proj:epsg": crs.get("epsg"),
    }

    if summary.get("driver"):
        properties["ice:driver"] = summary["driver"]
    if crs.get("name"):
        properties["ice:crs_name"] = crs["name"]

    wkt = (gdalinfo.get("coordinateSystem") or {}).get("wkt")
    if wkt:
        properties["proj:wkt2"] = wkt
    if size[0] and size[1]:
        properties["proj:shape"] = [size[1], size[0]]  # row-major: height, then width
    transform = _transform(gdalinfo.get("geoTransform"))
    if transform:
        properties["proj:transform"] = transform

    data_asset: dict[str, Any] = {
        "href": f"s3://{source_bucket}/{raster_key}",
        "type": _media_type(raster_key),
        "title": summary.get("file_name") or posixpath.basename(raster_key),
        "roles": ["data"],
    }
    if summary.get("size_bytes") is not None:
        data_asset["file:size"] = summary["size_bytes"]
    bands = _raster_bands(summary)
    if bands:
        data_asset["raster:bands"] = bands

    reports = f"s3://{artifacts_bucket}/{report_key.rstrip('/')}"

    return {
        "type": "Feature",
        "stac_version": STAC_VERSION,
        "stac_extensions": [PROJ_EXTENSION, RASTER_EXTENSION, FILE_EXTENSION],
        "id": identifier,
        "collection": collection_id,
        "geometry": geometry,
        "bbox": _bbox(geometry),
        "properties": properties,
        "assets": {
            "data": data_asset,
            "gdalinfo": {
                "href": f"{reports}/gdalinfo.json",
                "type": "application/json",
                "title": "Full gdalinfo output",
                "roles": ["metadata"],
            },
            "summary": {
                "href": f"{reports}/summary.json",
                "type": "application/json",
                "title": "Flattened report",
                "roles": ["metadata"],
            },
        },
        # Relative, and no self link: the tree describes its own shape and nothing
        # else, so it survives being copied to another bucket or another prefix.
        "links": [
            {"rel": "root", "href": "../../catalog.json", "type": "application/json"},
            {"rel": "parent", "href": "../collection.json", "type": "application/json"},
            {"rel": "collection", "href": "../collection.json", "type": "application/json"},
        ],
    }


# ---------------------------------------------------------------- collection & root


def build_collection(items: list[dict], collection_id: str = COLLECTION_ID) -> dict:
    """The collection, with its extent unioned over ``items``.

    An empty collection is legal and is what the first run of the rebuild DAG writes
    against an empty catalog. STAC spells an unknown extent as a list of nulls rather
    than an omitted field, so the shape stays the same either way.
    """
    bboxes = [item["bbox"] for item in items if item.get("bbox")]
    datetimes = sorted(
        item["properties"]["datetime"] for item in items if (item.get("properties") or {}).get("datetime")
    )

    if bboxes:
        spatial = [
            min(box[0] for box in bboxes),
            min(box[1] for box in bboxes),
            max(box[2] for box in bboxes),
            max(box[3] for box in bboxes),
        ]
    else:
        spatial = [-180.0, -90.0, 180.0, 90.0]

    temporal = [datetimes[0], datetimes[-1]] if datetimes else [None, None]

    return {
        "type": "Collection",
        "stac_version": STAC_VERSION,
        "id": collection_id,
        "title": COLLECTION_TITLE,
        "description": COLLECTION_DESCRIPTION,
        "license": LICENSE,
        "extent": {"spatial": {"bbox": [spatial]}, "temporal": {"interval": [temporal]}},
        "links": [
            {"rel": "root", "href": "../catalog.json", "type": "application/json"},
            {"rel": "parent", "href": "../catalog.json", "type": "application/json"},
            *(
                {
                    "rel": "item",
                    "href": f"./items/{item['id']}.json",
                    "type": "application/geo+json",
                    "title": item["assets"]["data"].get("title"),
                }
                for item in sorted(items, key=lambda item: item["id"])
            ),
        ],
    }


def build_catalog(collection_ids: list[str] | None = None) -> dict:
    collection_ids = collection_ids or [COLLECTION_ID]
    return {
        "type": "Catalog",
        "stac_version": STAC_VERSION,
        "id": CATALOG_ID,
        "description": CATALOG_DESCRIPTION,
        "links": [
            {"rel": "root", "href": "./catalog.json", "type": "application/json"},
            *(
                {
                    "rel": "child",
                    "href": f"./{identifier}/collection.json",
                    "type": "application/json",
                }
                for identifier in sorted(collection_ids)
            ),
        ],
    }
