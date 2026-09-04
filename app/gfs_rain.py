#!/usr/bin/env python3
"""Turn a GFS precipitation forecast into a map, and publish it with its metadata.

Runs on the GDAL worker, driven over SSM by the ``gdal_weather`` DAG. Given a NOMADS
grib filter URL it downloads the subset, picks the accumulation record that was asked
for, and writes five objects under ``<out-prefix>/``:

    <name>.grib2      the bytes NOMADS served, untouched
    <name>.tif        the chosen band alone, as GeoTIFF, tagged EPSG:4326
    <name>.png        that band run through a rainfall colour ramp
    gdalinfo.json     full `gdalinfo -json` output for the GeoTIFF
    summary.json      the flat fields the notification email renders

The URL is built by the DAG rather than here — see ``dags/common/gfs.py`` for why.
This script only checks that it points at NOMADS before fetching it.

Usage:
    gfs_rain.py --url URL --out-bucket B --out-prefix reports/weather/2026-08-19
    gfs_rain.py --url URL --local-dir ./out          # offline, no AWS calls
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from datetime import UTC, datetime, timedelta

# Sibling module in the same image. Reused rather than re-derived so a raster
# inspected by this pipeline is described the same way whichever DAG produced it —
# the notification email renders both from one set of field names.
from gdal_report import describe_bands, describe_crs, human_bytes, log
from osgeo import gdal

gdal.UseExceptions()

NOMADS_HOST = "nomads.ncep.noaa.gov"

# Millimetres of accumulated precipitation — GFS reports APCP in kg/m², which over
# water is the same number. Fully transparent below 0.1 mm so the map shows where it
# rained rather than a solid rectangle of "almost nothing".
#
# The `nv` line draws nodata as transparent. GFS APCP carries no nodata value, so GDAL
# logs "Input dataset has no nodata value. Ignoring 'nv' entry" on every run — that
# warning is expected. The line stays because dropping it would colour nodata as if it
# were 0 mm of rain the moment a product that does carry one comes through here.
RAIN_RAMP = """\
0      255 255 255 0
0.1    220 240 255 255
1      173 216 230 255
2.5    100 180 255 255
5      30 144 255 255
10     0 100 200 255
25     0 60 160 255
50     90 0 130 255
100    180 0 180 255
nv     0 0 0 0
"""

# The subset is 241x105 pixels, which is honest at the model's 0.25° resolution and
# postage-stamp sized in an email client. The PNG is enlarged from the *data* before
# the ramp is applied, not after: interpolating millimetres and then colouring them
# keeps the ramp's alpha step meaningful, where interpolating finished RGBA pixels
# would blend the transparent 0 mm colour into its neighbours and halo every shower.
UPSCALE_PERCENT = 300


def fetch(url: str, dest: str, timeout: int = 120, attempts: int = 3) -> int:
    """Download the GRIB2 subset, or raise saying what came back instead.

    NOMADS answers a cycle it has not published with 200 and a short HTML apology, so
    the status code cannot be trusted on its own — the magic bytes are the check that
    matters. The DAG's ``wait_for_cycle`` should have made that impossible by the time
    this runs; this catches the case where the cycle expired between the two.
    """
    host = urllib.parse.urlparse(url).hostname
    if host != NOMADS_HOST:
        raise ValueError(f"refusing to fetch from {host!r}; this script only talks to {NOMADS_HOST}")

    request = urllib.request.Request(url, headers={"User-Agent": "ice-gdal-worker/gfs_rain"})
    last_error: Exception | None = None

    for attempt in range(1, attempts + 1):
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response, open(dest, "wb") as handle:
                payload = response.read()
                handle.write(payload)
        except (urllib.error.URLError, TimeoutError) as exc:
            last_error = exc
            log(f"attempt {attempt}/{attempts} failed: {exc}")
            continue

        if not payload.startswith(b"GRIB"):
            # Not retried: a body that is not GRIB is an answer, not a glitch, and
            # repeating the request will get the same answer more slowly.
            raise RuntimeError(
                f"NOMADS did not return GRIB2 data ({len(payload)} bytes). First 300 bytes: {payload[:300]!r}"
            )

        log(f"fetched {human_bytes(len(payload))} from NOMADS")
        return len(payload)

    raise RuntimeError(f"could not reach NOMADS after {attempts} attempts: {last_error}")


def accumulation_hours(band: gdal.Band) -> int | None:
    """Length of the window an accumulated GRIB2 band covers, in hours.

    ``GRIB_FORECAST_SECONDS`` is the offset from the reference time to the *start* of
    the accumulation, not to its end, so the window is what is left after taking it off
    the lead time. Verified against a real gfs.t12z.pgrb2.0p25.f024, which carries:

        band 1  APCP06  ref+64800 .. ref+86400   ->  6 h   (the last six hours)
        band 2  APCP24  ref+0     .. ref+86400   -> 24 h   (the whole forecast)

    Both bands share a valid time and a level, so nothing shallower than this separates
    them — and picking the wrong one silently produces a map of the wrong period.

    Returns None for a band that is not an accumulation, which is not an error: the
    caller reports what it found rather than guessing.
    """
    metadata = band.GetMetadata()
    try:
        seconds = (
            int(metadata["GRIB_VALID_TIME"])
            - int(metadata["GRIB_REF_TIME"])
            - int(metadata["GRIB_FORECAST_SECONDS"])
        )
    except (KeyError, ValueError):
        return None
    return seconds // 3600 if seconds > 0 and seconds % 3600 == 0 else None


def select_band(dataset: gdal.Dataset, hours: int) -> int:
    """Index of the band accumulating over exactly ``hours``.

    Raises rather than falling back to band 1. Band order in these files is not the
    order you would guess — the 6-hour bucket comes first — so a default would be a
    coin flip dressed up as a decision, and the resulting map would look entirely
    plausible while describing the wrong window.
    """
    found = []
    for index in range(1, dataset.RasterCount + 1):
        band = dataset.GetRasterBand(index)
        window = accumulation_hours(band)
        found.append((index, band.GetMetadata().get("GRIB_ELEMENT", "?"), window))
        if window == hours:
            return index

    catalogue = ", ".join(f"band {i} {element} ({w}h)" for i, element, w in found) or "no bands"
    raise RuntimeError(f"no {hours}-hour accumulation in this file. Found: {catalogue}")


def render(grib_path: str, band_index: int, workdir: str, name: str) -> tuple[str, str]:
    """Write the single-band GeoTIFF and the coloured PNG. Returns both paths.

    The GeoTIFF is tagged EPSG:4326 rather than carrying the CRS GDAL reads out of the
    GRIB. That CRS is a bare geographic system on a 6371229 m sphere with no authority
    code at all — ``AutoIdentifyEPSG`` raises "Unsupported SRS" on it — which leaves the
    product unusable in anything that wants an EPSG code. The coordinates themselves are
    unchanged; only the label is. The original WKT is preserved in summary.json so the
    substitution is visible rather than silent.
    """
    tif = os.path.join(workdir, f"{name}.tif")
    gdal.Translate(
        tif,
        grib_path,
        bandList=[band_index],
        outputSRS="EPSG:4326",
        format="GTiff",
        creationOptions=["COMPRESS=DEFLATE", "TILED=YES"],
    )

    enlarged = os.path.join(workdir, f"{name}.display.tif")
    gdal.Translate(enlarged, tif, widthPct=UPSCALE_PERCENT, heightPct=UPSCALE_PERCENT, resampleAlg="bilinear")

    ramp = os.path.join(workdir, "rain_colors.txt")
    with open(ramp, "w") as handle:
        handle.write(RAIN_RAMP)

    coloured = os.path.join(workdir, f"{name}.coloured.tif")
    gdal.DEMProcessing(coloured, enlarged, "color-relief", colorFilename=ramp, addAlpha=True)

    png = os.path.join(workdir, f"{name}.png")
    gdal.Translate(png, coloured, format="PNG")

    log(f"rendered {png}")
    return tif, png


def build_summary(
    *,
    url: str,
    grib_path: str,
    tif_path: str,
    info: dict,
    dataset: gdal.Dataset,
    grib: gdal.Dataset,
    band_index: int,
    hours: int,
) -> dict:
    """The flat document the email renders, in the same shape gdal_report.py emits.

    Shares its field names with the elevation pipeline's summary so one set of render
    helpers covers both, and adds a ``forecast`` block for the things only a forecast
    has: which run produced it and what period it covers.
    """
    band = dataset.GetRasterBand(1)
    source = grib.GetRasterBand(band_index).GetMetadata()
    geotransform = info.get("geoTransform") or [None] * 6

    reference = datetime.fromtimestamp(int(source["GRIB_REF_TIME"]), tz=UTC)
    valid = datetime.fromtimestamp(int(source["GRIB_VALID_TIME"]), tz=UTC)

    statistics = dict(zip(("min", "max", "mean", "stddev"), band.ComputeStatistics(False), strict=False))

    return {
        "source": url,
        "file_name": os.path.basename(tif_path),
        "size_bytes": os.path.getsize(grib_path),
        "size_human": human_bytes(os.path.getsize(grib_path)),
        "driver": f"{info.get('driverShortName')} ({info.get('driverLongName')})",
        "crs": describe_crs(info),
        "width": info.get("size", [None, None])[0],
        "height": info.get("size", [None, None])[1],
        "pixel_size": {"x": geotransform[1], "y": geotransform[5]},
        "origin": {"x": geotransform[0], "y": geotransform[3]},
        "corner_coordinates": info.get("cornerCoordinates", {}),
        "wgs84_extent": info.get("wgs84Extent"),
        "band_count": dataset.RasterCount,
        "bands": describe_bands(dataset, info),
        "metadata": info.get("metadata", {}).get("", {}),
        "processed_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "forecast": {
            "model": "GFS 0.25°",
            "element": source.get("GRIB_ELEMENT"),
            "description": source.get("GRIB_COMMENT"),
            "units": source.get("GRIB_UNIT"),
            "cycle": reference.isoformat(timespec="seconds"),
            "valid_from": (valid - timedelta(hours=hours)).isoformat(timespec="seconds"),
            "valid_to": valid.isoformat(timespec="seconds"),
            "accumulation_hours": hours,
            "grib_band": band_index,
            "grib_band_count": grib.RasterCount,
            "grib_crs_wkt": grib.GetProjection(),
            "max_mm": statistics.get("max"),
            "mean_mm": statistics.get("mean"),
        },
    }


def upload(out_bucket: str, out_prefix: str, paths: dict[str, str], documents: dict[str, dict]) -> str:
    import boto3

    s3 = boto3.client("s3")
    base = out_prefix.strip("/")

    types = {".grib2": "application/wmo-grib2", ".tif": "image/tiff", ".png": "image/png"}
    for path in paths.values():
        name = os.path.basename(path)
        with open(path, "rb") as handle:
            s3.put_object(
                Bucket=out_bucket,
                Key=f"{base}/{name}",
                Body=handle.read(),
                ContentType=types.get(os.path.splitext(name)[1], "application/octet-stream"),
            )

    for name, payload in documents.items():
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
    parser.add_argument("--url", required=True, help="NOMADS grib filter URL, built by the DAG")
    parser.add_argument(
        "--accumulation",
        type=int,
        default=24,
        help="Accumulation window to map, in hours (default: 24). The band is chosen by "
        "this, never by position.",
    )
    parser.add_argument("--name", default="usa_rain", help="Basename for the products (default: usa_rain)")
    parser.add_argument("--out-bucket", help="Bucket to write the products to")
    parser.add_argument("--out-prefix", default="reports/weather", help="Key prefix for the products")
    parser.add_argument("--local-dir", help="Write products here instead of S3, for offline development")
    parser.add_argument("--region", default=os.environ.get("AWS_REGION", "eu-north-1"))

    args = parser.parse_args(argv)
    if not args.out_bucket and not args.local_dir:
        parser.error("one of --out-bucket or --local-dir is required")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    with tempfile.TemporaryDirectory(prefix="gfs-rain-") as scratch:
        workdir = args.local_dir or scratch
        os.makedirs(workdir, exist_ok=True)

        grib_path = os.path.join(workdir, f"{args.name}.grib2")
        try:
            fetch(args.url, grib_path)
        except (RuntimeError, ValueError) as exc:
            log(f"ERROR: {exc}")
            return 4

        try:
            grib = gdal.Open(grib_path, gdal.GA_ReadOnly)
        except RuntimeError as exc:
            log(f"ERROR: GDAL could not open the downloaded GRIB2: {exc}")
            return 2

        try:
            band_index = select_band(grib, args.accumulation)
        except RuntimeError as exc:
            log(f"ERROR: {exc}")
            return 5
        log(f"using band {band_index} ({grib.GetRasterBand(band_index).GetMetadata().get('GRIB_ELEMENT')})")

        tif_path, png_path = render(grib_path, band_index, workdir, args.name)

        product = gdal.Open(tif_path, gdal.GA_ReadOnly)
        try:
            info = gdal.Info(product, format="json")
        except RuntimeError as exc:
            log(f"ERROR: gdalinfo failed on the rendered GeoTIFF: {exc}")
            return 3

        summary = build_summary(
            url=args.url,
            grib_path=grib_path,
            tif_path=tif_path,
            info=info,
            dataset=product,
            grib=grib,
            band_index=band_index,
            hours=args.accumulation,
        )
        print(json.dumps(summary, indent=2, default=str))

        if args.out_bucket:
            gdal.SetConfigOption("AWS_REGION", args.region)
            location = upload(
                args.out_bucket,
                args.out_prefix,
                {"grib": grib_path, "tif": tif_path, "png": png_path},
                {"gdalinfo.json": info, "summary.json": summary},
            )
            log(f"products written to {location}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
