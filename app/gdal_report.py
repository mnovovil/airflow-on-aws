#!/usr/bin/env python3
"""Inspect a raster with GDAL and publish a metadata report.

Reads the raster in place over ``/vsis3`` so a multi-gigabyte elevation model is
never downloaded just to read its header. Writes two objects:

    <out-prefix>/<key>/gdalinfo.json   full `gdalinfo -json` output
    <out-prefix>/<key>/summary.json    the flat fields the notification email renders

The report goes to S3 rather than back through stdout because SSM Run Command
truncates command output at ~24 KB, which `gdalinfo -json` exceeds easily on a
multi-band raster.

Usage:
    gdal_report.py --bucket B --key K --out-bucket O [--out-prefix reports]
    gdal_report.py --local /data/some.tif          # offline, no AWS calls
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import UTC, datetime

from osgeo import gdal, osr

gdal.UseExceptions()
osr.UseExceptions()


def log(message: str) -> None:
    """Progress output. Always stderr — stdout is reserved for the summary JSON."""
    print(message, file=sys.stderr)


def human_bytes(n: int | None) -> str:
    if n is None:
        return "unknown"
    step = 1024.0
    value = float(n)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if value < step or unit == "TiB":
            return f"{value:,.1f} {unit}" if unit != "B" else f"{int(value):,} B"
        value /= step
    return f"{value:,.1f} TiB"


def describe_crs(info: dict) -> dict:
    """Pull a human-readable CRS name and EPSG code out of the gdalinfo payload."""
    wkt = info.get("coordinateSystem", {}).get("wkt")
    if not wkt:
        return {"name": None, "epsg": None, "units": None}

    srs = osr.SpatialReference()
    try:
        srs.ImportFromWkt(wkt)
    except RuntimeError:
        return {"name": None, "epsg": None, "units": None}

    # AutoIdentifyEPSG only sets the authority when it is confident; a null EPSG
    # here means the raster carries a custom projection, not that parsing failed.
    try:
        srs.AutoIdentifyEPSG()
    except RuntimeError:
        pass

    epsg = srs.GetAuthorityCode(None)
    units = srs.GetAttrValue("UNIT") if srs.IsProjected() or srs.IsGeographic() else None
    return {
        "name": srs.GetName(),
        "epsg": int(epsg) if epsg and epsg.isdigit() else None,
        "units": units,
    }


def describe_bands(dataset: gdal.Dataset, info: dict) -> list[dict]:
    """Per-band metadata, with statistics computed approximately.

    Approximate stats sample the overviews / a subset of pixels instead of every
    pixel. On a full Landsat scene that is the difference between milliseconds and
    tens of seconds, and the email only needs the order of magnitude.
    """
    bands: list[dict] = []
    for i, band_info in enumerate(info.get("bands", []), start=1):
        band = dataset.GetRasterBand(i)
        entry = {
            "index": i,
            "type": band_info.get("type"),
            "color_interpretation": band_info.get("colorInterpretation"),
            "nodata": band_info.get("noDataValue"),
            "block_size": band_info.get("block"),
            "min": None,
            "max": None,
            "mean": None,
            "stddev": None,
        }
        try:
            minimum, maximum, mean, stddev = band.ComputeStatistics(True)
            entry.update({"min": minimum, "max": maximum, "mean": mean, "stddev": stddev})
        except RuntimeError as exc:
            # An all-nodata band legitimately has no statistics. Record why rather
            # than failing the whole report over it.
            entry["stats_error"] = str(exc)
        bands.append(entry)
    return bands


def build_summary(source: str, info: dict, dataset: gdal.Dataset, size_bytes: int | None) -> dict:
    geotransform = info.get("geoTransform") or [None] * 6
    corners = info.get("cornerCoordinates", {})
    wgs84 = info.get("wgs84Extent")

    return {
        "source": source,
        "file_name": os.path.basename(source.rstrip("/")),
        "size_bytes": size_bytes,
        "size_human": human_bytes(size_bytes),
        "driver": f"{info.get('driverShortName')} ({info.get('driverLongName')})",
        "crs": describe_crs(info),
        "width": info.get("size", [None, None])[0],
        "height": info.get("size", [None, None])[1],
        "pixel_size": {"x": geotransform[1], "y": geotransform[5]},
        "origin": {"x": geotransform[0], "y": geotransform[3]},
        "corner_coordinates": corners,
        "wgs84_extent": wgs84,
        "band_count": dataset.RasterCount,
        "bands": describe_bands(dataset, info),
        "metadata": info.get("metadata", {}).get("", {}),
        "processed_at": datetime.now(UTC).isoformat(timespec="seconds"),
    }


def s3_object_size(bucket: str, key: str) -> int | None:
    import boto3

    try:
        head = boto3.client("s3").head_object(Bucket=bucket, Key=key)
        return head["ContentLength"]
    except Exception as exc:  # noqa: BLE001 — size is cosmetic, never fail the run for it
        log(f"warning: could not HEAD s3://{bucket}/{key}: {exc}")
        return None


def upload_reports(out_bucket: str, out_prefix: str, key: str, info: dict, summary: dict) -> str:
    import boto3

    s3 = boto3.client("s3")
    base = f"{out_prefix.rstrip('/')}/{key}"
    for name, payload in (("gdalinfo.json", info), ("summary.json", summary)):
        s3.put_object(
            Bucket=out_bucket,
            Key=f"{base}/{name}",
            Body=json.dumps(payload, indent=2, default=str).encode(),
            ContentType="application/json",
        )
    return f"s3://{out_bucket}/{base}"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--bucket", help="S3 bucket holding the raster")
    source.add_argument("--local", help="Local raster path, for offline development")
    parser.add_argument("--key", help="S3 object key (required with --bucket)")
    parser.add_argument("--out-bucket", help="Bucket to write the reports to")
    parser.add_argument("--out-prefix", default="reports", help="Key prefix for reports (default: reports)")
    parser.add_argument("--region", default=os.environ.get("AWS_REGION", "eu-north-1"))

    args = parser.parse_args(argv)
    if args.bucket and not args.key:
        parser.error("--key is required when --bucket is given")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    if args.local:
        path, size_bytes, source = args.local, os.path.getsize(args.local), args.local
    else:
        gdal.SetConfigOption("AWS_REGION", args.region)
        gdal.SetConfigOption("AWS_DEFAULT_REGION", args.region)
        path = f"/vsis3/{args.bucket}/{args.key}"
        size_bytes = s3_object_size(args.bucket, args.key)
        source = f"s3://{args.bucket}/{args.key}"

    # Progress goes to stderr so stdout stays a single JSON document and
    # `gdal_report.py --local x.tif > summary.json` is directly usable. SSM Run
    # Command captures both streams, so nothing is lost on the worker.
    log(f"opening {path}")
    try:
        dataset = gdal.Open(path, gdal.GA_ReadOnly)
    except RuntimeError as exc:
        log(f"ERROR: GDAL could not open {source}: {exc}")
        return 2
    if dataset is None:
        log(f"ERROR: GDAL returned no dataset for {source}")
        return 2

    try:
        info = gdal.Info(dataset, format="json")
    except RuntimeError as exc:
        log(f"ERROR: gdalinfo failed on {source}: {exc}")
        return 3

    summary = build_summary(source, info, dataset, size_bytes)
    print(json.dumps(summary, indent=2, default=str))

    if args.out_bucket and not args.local:
        location = upload_reports(args.out_bucket, args.out_prefix, args.key, info, summary)
        log(f"reports written to {location}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
