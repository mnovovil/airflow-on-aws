"""The gdalinfo-to-STAC mapping, checked field by field and then against the schemas.

Two different jobs here, and both are needed. The field assertions pin down decisions
a schema cannot see — that the item id survives the archive move, that the geotransform
is reordered rather than copied — and the pystac validation catches everything a
hand-built dictionary gets wrong that nobody thought to assert.

pystac is a test dependency on purpose: adding it to dags/requirements.txt would
replace the Airflow instance, and nothing at run time needs it. See common/stac.py.
"""

from __future__ import annotations

import json
import os

import pytest

from common import stac
from conftest import GDALINFO, SUMMARY

BUILD = {
    "source_bucket": "example-dem",
    "raster_key": "sent/scenes/n61e010.tif",
    "artifacts_bucket": "ice-artifacts-123456789012",
    "report_key": "reports/scenes/n61e010.tif",
    "archive_prefix": "sent/",
}


@pytest.fixture
def item() -> dict:
    return stac.build_item(GDALINFO, SUMMARY, **BUILD)


# ----------------------------------------------------------------------- item ids


@pytest.mark.parametrize(
    ("key", "expected"),
    [
        ("scenes/n61e010.tif", "scenes_n61e010"),
        ("n61e010.tif", "n61e010"),
        ("scenes/2026/a b.tiff", "scenes_2026_a_b"),
        # A dot in a directory name is not an extension.
        ("v1.2/raster", "v1.2_raster"),
        ("scenes/odd#name.tif", "scenes_odd_name"),
    ],
)
def test_item_id_is_derived_from_the_key(key: str, expected: str) -> None:
    assert stac.item_id(key) == expected


def test_item_id_survives_the_archive_move() -> None:
    """The whole reason the prefix is stripped.

    move_file relocates the raster after the report is written, and a hand-triggered
    re-run then sees the archived key. If that produced a different id, one raster
    would end up as two items — one of them pointing at an object that is gone.
    """
    assert stac.item_id("sent/scenes/a.tif", "sent/") == stac.item_id("scenes/a.tif", "sent/")


def test_item_id_rejects_a_key_with_nothing_usable_in_it() -> None:
    with pytest.raises(stac.StacError):
        stac.item_id("sent/", "sent/")


# --------------------------------------------------------------------- the mapping


def test_the_item_is_placed_in_space(item: dict) -> None:
    assert item["geometry"] == GDALINFO["wgs84Extent"]
    assert item["bbox"] == [10.0, 61.0, 10.7, 61.3]


def test_the_item_carries_the_collection_and_a_derived_id(item: dict) -> None:
    assert item["id"] == "scenes_n61e010"
    assert item["collection"] == stac.COLLECTION_ID


def test_the_datetime_is_the_processing_time_and_says_so(item: dict) -> None:
    """Nothing upstream carries an acquisition time, so this is when the report ran.

    ice:datetime_source is what stops a consumer reading it as anything else: filtering
    these items by datetime filters by processing order, not by when the ground was
    measured.
    """
    assert item["properties"]["datetime"] == SUMMARY["processed_at"]
    assert item["properties"]["ice:datetime_source"] == "processed_at"


def test_the_projection_is_described_in_the_units_stac_expects(item: dict) -> None:
    """proj:shape is (height, width) where gdalinfo's size is (width, height), and
    proj:transform is the affine matrix in row-major order where GDAL's geotransform
    interleaves the two rows. Copying either straight through is wrong in a way that
    only shows up when something tries to georeference the item."""
    properties = item["properties"]
    assert properties["proj:epsg"] == 32633
    assert properties["proj:shape"] == [900, 1200]
    assert properties["proj:transform"] == [30.0, 0.0, 400000.0, 0.0, -30.0, 6800000.0, 0.0, 0.0, 1.0]
    assert properties["proj:wkt2"] == GDALINFO["coordinateSystem"]["wkt"]


def test_a_rotated_raster_keeps_its_rotation_terms() -> None:
    """The north-up case hides a reordering error, because four of the six values are
    zero or on the diagonal. This one does not."""
    gdalinfo = {**GDALINFO, "geoTransform": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]}
    item = stac.build_item(gdalinfo, SUMMARY, **BUILD)
    assert item["properties"]["proj:transform"] == [2.0, 3.0, 1.0, 5.0, 6.0, 4.0, 0.0, 0.0, 1.0]


def test_a_crs_without_an_epsg_code_is_published_as_null(item: dict) -> None:
    """validate_report deletes such a raster, so this is only reachable by hand — but
    the extension wants the field present, and an absent one reads as 'not projected'
    rather than 'no authority code'."""
    summary = {**SUMMARY, "crs": {"name": "custom", "epsg": None}}
    built = stac.build_item(GDALINFO, summary, **BUILD)
    assert built["properties"]["proj:epsg"] is None


def test_a_raster_that_cannot_be_placed_is_refused() -> None:
    """No wgs84Extent means gdalinfo could not transform the corners into geographic
    coordinates. STAC has no way to express that, and an item with a made-up geometry
    is worse than no item."""
    gdalinfo = {key: value for key, value in GDALINFO.items() if key != "wgs84Extent"}
    with pytest.raises(stac.StacError, match="wgs84Extent"):
        stac.build_item(gdalinfo, SUMMARY, **BUILD)


# -------------------------------------------------------------------------- assets


def test_the_data_asset_points_at_the_archived_object(item: dict) -> None:
    """Not at the key the raster was uploaded under: by the time publish_stac runs,
    move_file has already relocated it, and the original key 404s."""
    data = item["assets"]["data"]
    assert data["href"] == "s3://example-dem/sent/scenes/n61e010.tif"
    assert data["type"] == "image/tiff; application=geotiff"
    assert data["roles"] == ["data"]
    assert data["file:size"] == 48_234_112


def test_the_reports_are_assets_too(item: dict) -> None:
    """The item is a summary of them; a consumer wanting the full band table needs a
    way to get from one to the other."""
    assets = item["assets"]
    base = "s3://ice-artifacts-123456789012/reports/scenes/n61e010.tif"
    assert assets["gdalinfo"]["href"] == f"{base}/gdalinfo.json"
    assert assets["summary"]["href"] == f"{base}/summary.json"
    assert assets["gdalinfo"]["roles"] == ["metadata"]


def test_the_band_statistics_are_published_and_flagged_approximate(item: dict) -> None:
    """gdal_report.py computes them with ComputeStatistics(True) — sampled, not a full
    pass. Useful to an order of magnitude and wrong about the tails."""
    band = item["assets"]["data"]["raster:bands"][0]
    assert band["data_type"] == "int16"
    assert band["nodata"] == -32768.0
    assert band["statistics"] == {"minimum": 12.0, "maximum": 2469.0, "mean": 843.2, "stddev": 401.7}
    assert band["ice:statistics_are_approximate"] is True


def test_a_band_with_no_statistics_records_why() -> None:
    """An all-nodata band legitimately has none. The report keeps the GDAL message
    rather than dropping the band, and so does the item."""
    summary = {**SUMMARY, "bands": [{"index": 1, "type": "Byte", "stats_error": "no valid pixels found"}]}
    item = stac.build_item(GDALINFO, summary, **BUILD)
    band = item["assets"]["data"]["raster:bands"][0]
    assert "statistics" not in band
    assert band["ice:statistics_error"] == "no valid pixels found"


def test_the_links_are_relative_so_the_tree_can_be_copied(item: dict) -> None:
    """No self link and no bucket name anywhere in the links: the catalogue describes
    its own shape and nothing else, so `aws s3 sync` to somewhere new is enough to
    move it."""
    hrefs = {link["rel"]: link["href"] for link in item["links"]}
    assert hrefs == {
        "root": "../../catalog.json",
        "parent": "../collection.json",
        "collection": "../collection.json",
    }


# ---------------------------------------------------------------------- collection


def test_the_collection_extent_is_the_union_of_its_items() -> None:
    east = stac.build_item(
        {
            **GDALINFO,
            "wgs84Extent": {
                "type": "Polygon",
                "coordinates": [[[12.0, 62.0], [12.0, 61.5], [12.5, 61.5], [12.5, 62.0], [12.0, 62.0]]],
            },
        },
        {**SUMMARY, "processed_at": "2026-08-12T09:15:00+00:00"},
        **{**BUILD, "raster_key": "sent/scenes/n62e012.tif"},
    )
    collection = stac.build_collection([stac.build_item(GDALINFO, SUMMARY, **BUILD), east])

    assert collection["extent"]["spatial"]["bbox"] == [[10.0, 61.0, 12.5, 62.0]]
    assert collection["extent"]["temporal"]["interval"] == [
        ["2026-08-11T09:15:00+00:00", "2026-08-12T09:15:00+00:00"]
    ]


def test_the_collection_links_every_item_relatively(item: dict) -> None:
    collection = stac.build_collection([item])
    items = [link for link in collection["links"] if link["rel"] == "item"]
    assert [link["href"] for link in items] == ["./items/scenes_n61e010.json"]


def test_an_empty_collection_is_still_a_valid_one() -> None:
    """What the first rebuild against an empty catalogue writes. STAC spells an unknown
    extent as nulls rather than an absent field, so the shape does not change."""
    collection = stac.build_collection([])
    assert collection["extent"]["temporal"]["interval"] == [[None, None]]
    assert [link for link in collection["links"] if link["rel"] == "item"] == []


def test_the_catalog_points_at_the_collection() -> None:
    catalog = stac.build_catalog()
    children = [link["href"] for link in catalog["links"] if link["rel"] == "child"]
    assert children == [f"./{stac.COLLECTION_ID}/collection.json"]


# ---------------------------------------------------------------- against the spec
#
# Two checks of different strength, because they have different costs.
#
# Parsing with pystac is offline and runs everywhere: it catches a missing required
# field, a datetime that is not a datetime, a bbox that does not parse. Validating
# against the published JSON schemas is stricter — it is what notices an extension
# declared in stac_extensions but not satisfied — and it fetches those schemas over
# the network, so it runs in CI and is skipped elsewhere rather than turning a
# no-egress environment into a hung test run.


def _reserialised(document: dict) -> dict:
    """Through JSON and back, which is how the DAG writes these — floats to strings via
    ``default=str`` included."""
    return json.loads(json.dumps(document, default=str))


def test_the_item_parses_as_a_stac_item(item: dict) -> None:
    import pystac

    parsed = pystac.Item.from_dict(_reserialised(item))
    assert parsed.id == "scenes_n61e010"
    assert parsed.bbox == [10.0, 61.0, 10.7, 61.3]
    assert parsed.datetime is not None


def test_the_collection_parses_as_a_stac_collection(item: dict) -> None:
    import pystac

    parsed = pystac.Collection.from_dict(_reserialised(stac.build_collection([item])))
    assert parsed.id == stac.COLLECTION_ID
    assert parsed.extent.spatial.bboxes == [[10.0, 61.0, 10.7, 61.3]]


def test_the_catalog_parses_as_a_stac_catalog() -> None:
    import pystac

    assert pystac.Catalog.from_dict(_reserialised(stac.build_catalog())).id == stac.CATALOG_ID


@pytest.mark.skipif(not os.environ.get("CI"), reason="fetches the STAC schemas over the network")
def test_the_item_validates_against_the_published_schemas(item: dict) -> None:
    """Everything above pins down a decision. This catches the rest — a misspelled
    field name, a role that should have been a list, an extension declared but not
    satisfied."""
    import pystac

    pystac.Item.from_dict(_reserialised(item)).validate()


@pytest.mark.skipif(not os.environ.get("CI"), reason="fetches the STAC schemas over the network")
def test_the_collection_validates_against_the_published_schemas(item: dict) -> None:
    import pystac

    pystac.Collection.from_dict(_reserialised(stac.build_collection([item]))).validate()
