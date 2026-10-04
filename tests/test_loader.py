"""Loader, device join and device filter."""

import pytest
from pyspark.sql import functions as F

from navigation.loader import attach_device, filter_by_device, load_events


def test_load_events_unions_the_client_streams(spark, data_dir):
    events = load_events(spark, data_dir)
    assert events.columns == [
        "navigation", "serial_id", "relative_time_ms", "event_time", "event_type", "hw_type", "has_request",
    ]
    rows = events.collect()
    assert len(rows) == 8  # the re-delivered box-change is kept ...
    by_key = {(r["navigation"], r["serial_id"]): r for r in rows}
    assert len(by_key) == 7  # ... and is the only repeated key
    assert by_key[(1, 1)]["event_type"] == "box-create"
    assert by_key[(2, 1)]["event_type"] == "mouse-down"
    assert by_key[(2, 1)]["hw_type"] == "desktop"
    assert by_key[(2, 1)]["relative_time_ms"] == 2000


def test_event_time_parses_variable_fraction_utc(spark, data_dir):
    # Rendered by Spark in the UTC session zone: collect() would convert the
    # value to the zone of the machine running the test.
    rendered = {
        r["navigation"]: r["t"]
        for r in load_events(spark, data_dir)
        .filter("serial_id = 0")
        .select("navigation", F.date_format("event_time", "HH:mm:ss.SSSSSS").alias("t"))
        .collect()
    }
    assert rendered == {1: "21:00:01.500000", 2: "21:00:02.500000", 3: "21:00:03.500000"}


def test_navigation_without_request_is_kept_as_unknown(spark, data_dir):
    nav3 = load_events(spark, data_dir).filter("navigation = 3").collect()
    assert len(nav3) == 2
    assert {(r["hw_type"], r["has_request"]) for r in nav3} == {("unknown", False)}


def test_repeated_request_rows_do_not_multiply_events(spark):
    events = spark.createDataFrame([(1, 0), (1, 1), (2, 0)], "navigation long, serial_id long")
    requests = spark.createDataFrame([(1, "phone"), (1, "phone")], "navigation long, hw_type string")
    attached = {(r["navigation"], r["serial_id"]): r for r in attach_device(events, requests).collect()}
    assert len(attached) == 3 and attach_device(events, requests).count() == 3
    assert (attached[(1, 0)]["hw_type"], attached[(1, 0)]["has_request"]) == ("phone", True)
    assert (attached[(2, 0)]["hw_type"], attached[(2, 0)]["has_request"]) == ("unknown", False)


def test_record_not_matching_the_schema_fails_the_job(spark, write_sources, source_rows):
    source_rows["mouse-down"][0]["serialId"] = "not-a-number"
    events = load_events(spark, write_sources(source_rows))
    with pytest.raises(Exception, match="(?i)malformed|parse"):
        events.collect()


def test_unparseable_event_time_fails_the_job(spark, write_sources, source_rows):
    source_rows["mouse-down"][0]["eventTime"] = "yesterday"
    events = load_events(spark, write_sources(source_rows))
    with pytest.raises(Exception, match="(?i)parse|timestamp"):
        events.collect()


@pytest.mark.parametrize("device, navigations", [
    ("all", [1, 2, 3]),  # "all" includes the request-less navigation 3
    ("phone", [1]),
    ("desktop", [2]),
    ("tablet", []),
])
def test_device_filter(spark, data_dir, device, navigations):
    events = filter_by_device(load_events(spark, data_dir), device)
    assert sorted({r["navigation"] for r in events.collect()}) == navigations


def test_device_filter_rejects_unknown_device(spark, data_dir):
    with pytest.raises(ValueError):
        filter_by_device(load_events(spark, data_dir), "unknown")
