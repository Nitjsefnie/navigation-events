"""Shared fixtures: one local SparkSession, an events builder and tiny source files."""

import bz2
import copy
import json
from datetime import datetime, timedelta, timezone

import pytest
from pyspark.sql import types as T

from navigation.spark import get_spark

# The shape load_events() returns.
EVENTS_SCHEMA = T.StructType([
    T.StructField("navigation", T.LongType()),
    T.StructField("serial_id", T.LongType()),
    T.StructField("relative_time_ms", T.LongType()),
    T.StructField("event_time", T.TimestampType()),
    T.StructField("event_type", T.StringType()),
    T.StructField("hw_type", T.StringType()),
    T.StructField("has_request", T.BooleanType()),
])

# Start of the hour the hand-built extracts live in.
T0 = datetime(2022, 5, 11, 21, 0, 0, tzinfo=timezone.utc)


@pytest.fixture(scope="session")
def spark():
    session = get_spark(app_name="navigation-tests", master="local[2]")
    yield session
    session.stop()


@pytest.fixture
def make_events(spark):
    """Build an events DataFrame from ``(navigation, serial_id, event_type, ...)`` rows.

    Optional trailing fields: ``hw_type`` (default "phone"; ``None`` means the
    navigation has no request row), ``relative_time_ms`` (default
    ``serial_id`` seconds) and an arrival delay in seconds (default 0).
    Navigation ``n`` starts ``n`` seconds after ``T0``, and an event arrives
    at ``start + relative_time_ms + delay``.
    """

    def build(rows):
        built = []
        for row in rows:
            navigation, serial_id, event_type = row[:3]
            hw_type = row[3] if len(row) > 3 else "phone"
            relative_ms = row[4] if len(row) > 4 else serial_id * 1000
            delay_s = row[5] if len(row) > 5 else 0.0
            start = T0 + timedelta(seconds=navigation)
            event_time = start + timedelta(milliseconds=relative_ms, seconds=delay_s)
            built.append((navigation, serial_id, relative_ms, event_time, event_type,
                          hw_type or "unknown", hw_type is not None))
        return spark.createDataFrame(built, EVENTS_SCHEMA)

    return build


# A handful of hand-written rows in the shape of the five source files.
SOURCE_ROWS = {
    "request": [
        {"eventTime": "2022-05-11T21:00:01.000000001Z", "hwType": "phone", "navigation": 1},
        {"eventTime": "2022-05-11T21:00:02.000000001Z", "hwType": "desktop", "navigation": 2},
        # navigation 3 has client events but no request row
    ],
    "page-change": [
        {"eventTime": "2022-05-11T21:00:01.5Z", "relativeTimeMs": 400,
         "clientSize": {"height": 800, "width": 400}, "serialId": 0, "navigation": 1},
        {"eventTime": "2022-05-11T21:00:02.500000000Z", "relativeTimeMs": 300,
         "clientSize": {"height": 900, "width": 1600}, "serialId": 0, "navigation": 2},
        {"eventTime": "2022-05-11T21:00:03.500000000Z", "relativeTimeMs": 200,
         "relativeLayoutSize": {"height": 6.58, "width": 1}, "serialId": 0, "navigation": 3},
    ],
    "box-create": [
        {"eventTime": "2022-05-11T21:00:01.600000000Z", "boxIndex": 411652303654651,
         "checkVisibility": True, "relativeTimeMs": 500, "serialId": 1, "navigation": 1},
    ],
    "box-change": [
        {"eventTime": "2022-05-11T21:00:01.700000000Z", "boxIndex": 19, "visibility": False,
         "relativeTimeMs": 600, "serialId": 2, "navigation": 1},
        # the same event re-delivered 8 s later: a duplicate, not a new event
        {"eventTime": "2022-05-11T21:00:09.700000000Z", "boxIndex": 19, "visibility": False,
         "relativeTimeMs": 600, "serialId": 2, "navigation": 1},
        {"eventTime": "2022-05-11T21:00:03.700000000Z", "boxIndex": 29,
         "relativePosition": {"x": 0.191, "y": 2.01}, "relativeSize": {"height": 0.141, "width": 0.554},
         "visibility": True, "relativeTimeMs": 900, "serialId": 2, "navigation": 3},
    ],
    "mouse-down": [
        {"eventTime": "2022-05-11T21:00:04.000000000Z", "boxIndex": 18, "relativeTimeMs": 2000,
         "activeElement": True, "serialId": 1, "navigation": 2},
    ],
}


@pytest.fixture
def write_sources(tmp_path):
    """Write ``{stem: rows}`` as ``<stem>.json.bz2`` files; return the directory."""

    def write(rows_by_stem, directory=tmp_path / "data"):
        directory.mkdir(exist_ok=True)
        for stem, rows in rows_by_stem.items():
            with bz2.open(directory / f"{stem}.json.bz2", "wt") as fh:
                fh.write("".join(json.dumps(row) + "\n" for row in rows))
        return str(directory)

    return write


@pytest.fixture
def source_rows():
    """A fresh copy of SOURCE_ROWS that a test may modify."""
    return copy.deepcopy(SOURCE_ROWS)


@pytest.fixture
def data_dir(write_sources, source_rows):
    return write_sources(source_rows)
