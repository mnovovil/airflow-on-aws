"""The email is the only output a human ever sees, and it is rendered from whatever
gdalinfo happened to report. A raster with no CRS, no statistics or no corner
coordinates is unusual but perfectly legal, and it must still produce an email
rather than fail the task after the GDAL work has already been paid for.
"""

from __future__ import annotations

from common.email_render import dummy_email, render, render_failure, render_stock, subject

FULL = {
    "source": "s3://example-dem/scenes/a.tif",
    "file_name": "a.tif",
    "size_human": "4.3 MiB",
    "driver": "GTiff (GeoTIFF)",
    "crs": {"name": "WGS 84 / UTM zone 29N", "epsg": 32629, "units": "metre"},
    "width": 8011,
    "height": 6991,
    "band_count": 1,
    "pixel_size": {"x": 30.0, "y": -30.0},
    "corner_coordinates": {"upperLeft": [383685.0, 2343915.0], "lowerRight": [624015.0, 2134185.0]},
    "bands": [{"index": 1, "type": "Byte", "min": 0.0, "max": 255.0, "mean": 12.5, "stddev": 3.25}],
    "processed_at": "2026-07-25T10:00:00+00:00",
}


def test_subject_carries_the_facts_worth_scanning():
    assert subject(FULL) == "[GDAL] a.tif — 8011×6991 · EPSG:32629"


def test_subject_without_epsg_omits_it_rather_than_saying_none():
    line = subject({"file_name": "a.tif", "width": 10, "height": 20, "crs": {}})
    assert line == "[GDAL] a.tif — 10×20"


def test_render_includes_the_overview_and_the_band_row():
    html = render(FULL, report_location="s3://ice-artifacts/reports/scenes/a.tif/")
    assert "WGS 84 / UTM zone 29N" in html
    assert "EPSG:32629" in html
    assert "s3://ice-artifacts/reports/scenes/a.tif/" in html
    assert "<td>Byte</td>" in html


def test_render_survives_an_empty_summary():
    """Nothing here should raise — a degraded email beats a failed task."""
    html = render({})
    assert "No bands reported." in html


def test_render_escapes_values_that_came_from_an_object_key():
    """The file name is chosen by whoever uploaded the raster, so it reaches the
    email as untrusted input and must not be able to inject markup."""
    html = render({**FULL, "file_name": "<script>alert(1)</script>.tif"})
    assert "<script>" not in html
    assert "&lt;script&gt;" in html


def test_failure_email_shows_the_error_escaped():
    html = render_failure({"Object": "s3://b/k.tif"}, "boom <&> happened")
    assert "s3://b/k.tif" in html
    assert "boom &lt;&amp;&gt; happened" in html


def test_dummy_email_carries_the_number_in_both_the_subject_and_the_body():
    """The whole payload is one integer, so if it does not survive the round trip
    into the MIME tree there is nothing else in the message to notice it by."""
    message = dummy_email(42, mail_from="ice@example.com", to=["a@example.com", "b@example.com"])

    assert message["Subject"] == "[test] 42"
    assert message["From"] == "ice@example.com"
    assert message["To"] == "a@example.com, b@example.com"
    # The HTML part is base64-encoded on the way into the tree (MIMEText with a utf-8
    # charset always is), so this has to look at the decoded payload rather than at
    # as_string(), where the number is not searchable text.
    body = next(p for p in message.walk() if p.get_content_type() == "text/html")
    assert "<h2>42</h2>" in body.get_payload(decode=True).decode()


def test_dummy_email_has_no_attachments():
    """Its value as a smoke test is that nothing but the SMTP path can break it —
    an attachment would put a file read back in front of the send."""
    payloads = [part.get_content_type() for part in dummy_email(7, mail_from="a@b.c", to=["d@e.f"]).walk()]
    assert payloads == ["multipart/mixed", "multipart/related", "text/html"]


# ------------------------------------------------------------------- the stock chart


def _bar(close: float = 42.0):
    import pandas as pd

    return pd.DataFrame(
        [[close - 1, close + 1, close - 2, close, 1_000]],
        index=pd.DatetimeIndex([pd.Timestamp("2026-08-19")], name="Date"),
        columns=["Open", "High", "Low", "Close", "Volume"],
    )


def test_the_chart_is_referenced_by_cid_and_never_inlined():
    """A data: URI here would arrive as a broken-image icon: Gmail and Outlook both
    strip them out of image sources. The angle brackets belong on the Content-ID
    header and not on this reference, which is the commonest way it breaks."""
    html = render_stock(_bar(), "SGO.PA", chart_cid="chart.SGO.PA.2026-08-19")
    assert 'src="cid:chart.SGO.PA.2026-08-19"' in html
    assert "data:image" not in html
    assert "cid:<" not in html


def test_a_session_without_a_chart_renders_no_image_tag_at_all():
    """The chart is optional — no intraday bars, or a Yahoo call that failed — and an
    <img> with an empty src is a broken image in every client."""
    assert "<img" not in render_stock(_bar(), "SGO.PA")


def test_an_empty_frame_ignores_the_chart_rather_than_pointing_at_nothing():
    """The no-data body is a different email, and nothing drew a chart for a session
    that never printed. A cid: reference with no matching part is a broken image."""
    import pandas as pd

    empty = pd.DataFrame(columns=["Open", "High", "Low", "Close", "Volume"])
    assert "<img" not in render_stock(empty, "SGO.PA", chart_cid="chart.SGO.PA.2026-08-19")


def test_the_cid_is_escaped_because_the_ticker_reaches_it_from_the_form():
    """The Content-ID is built from the typed ticker, so it is attacker-adjacent in the
    same way the heading is."""
    html = render_stock(_bar(), 'X"><script>', chart_cid='chart."><script>')
    assert "<script>" not in html
    assert "&quot;&gt;&lt;script&gt;" in html
