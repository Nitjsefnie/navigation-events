"""Read the source files into one client-side events DataFrame.

Output of ``load_events``, one row per *received* client-side event::

    navigation        long       navigation id (``navigation`` in the files)
    serial_id         long       client-side position of the event in the navigation
    relative_time_ms  long       ms since the navigation started, client clock
    event_time        timestamp  arrival at the collecting servers, UTC
    event_type        string     page-change | box-create | box-change | mouse-down
    hw_type           string     phone | desktop | tablet, or "unknown" without a request row
    has_request       boolean    whether the navigation's server-side request row exists

Re-delivered events (the same ``(navigation, serial_id)`` twice) are kept:
they are a data-quality signal that task 1 reports. Every measure counts
distinct serials, so a duplicate can neither hide a gap nor inflate a count.
"""

from functools import reduce
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from navigation import schemas

DEVICES = ("phone", "desktop", "tablet")
DEVICE_CHOICES = ("all",) + DEVICES
UNKNOWN_DEVICE = "unknown"


def read_source(spark: SparkSession, data_dir: str, stem: str, schema) -> DataFrame:
    """Read ``<data_dir>/<stem>.json.bz2`` with its declared schema, unmodified."""
    # FAILFAST: a record that does not fit the schema aborts the job instead
    # of silently becoming a row of nulls (PERMISSIVE, the default).
    return (
        spark.read.schema(schema)
        .option("mode", "FAILFAST")
        .json(str(Path(data_dir) / f"{stem}.json.bz2"))
    )


def _event_time():
    # eventTime has nanoseconds with a variable number of fractional digits
    # (``…T21:14:15.473634374Z``). to_timestamp parses that and truncates to
    # Spark's microseconds; no result depends on the lost digits (the closest
    # re-delivery pair is 36 ms apart). Under ANSI mode, the Spark 4 default,
    # an unparseable value raises instead of becoming null.
    return F.to_timestamp("eventTime")


def load_requests(spark: SparkSession, data_dir: str) -> DataFrame:
    """Server-side request rows: one per navigation, the only carrier of the device."""
    return read_source(spark, data_dir, "request", schemas.REQUEST).select(
        "navigation",
        F.col("hwType").alias("hw_type"),
        _event_time().alias("request_time"),
    )


def load_client_events(spark: SparkSession, data_dir: str) -> DataFrame:
    """Union of the four browser-side streams, projected to their shared columns.

    The payloads differ per stream, but every task works on the shared
    columns, so the union is narrow: a wide ``allowMissingColumns`` union
    would carry mostly-null payload columns through every shuffle.
    """
    frames = [
        read_source(spark, data_dir, stem, schema).select(
            "navigation",
            F.col("serialId").alias("serial_id"),
            F.col("relativeTimeMs").alias("relative_time_ms"),
            _event_time().alias("event_time"),
            F.lit(stem).alias("event_type"),
        )
        for stem, schema in schemas.CLIENT_EVENT_SCHEMAS.items()
    ]
    return reduce(DataFrame.unionByName, frames)


def attach_device(events: DataFrame, requests: DataFrame) -> DataFrame:
    """Attach ``hw_type`` and ``has_request`` to every client event.

    The device is only known server-side, so it reaches client events through
    a join on ``navigation``. It is a *left* join: 785 of the 10 454
    navigations have client events but no request row, and an inner join
    would silently drop their ~23k events, including most of the navigations
    that do not start at serial 0. They are kept as ``hw_type = "unknown"``:
    they count under ``--device all`` and under no concrete device, because
    their device cannot be known.

    The request side is one small row per navigation, so it is broadcast and
    the much larger events side is not shuffled for the join. Requests are
    deduplicated on ``navigation`` first so a re-delivered request could
    never multiply events (none is duplicated in this extract).
    """
    devices = (
        requests.select("navigation", "hw_type", F.lit(True).alias("has_request"))
        .dropDuplicates(["navigation"])
    )
    return (
        events.join(F.broadcast(devices), on="navigation", how="left")
        .fillna({"hw_type": UNKNOWN_DEVICE, "has_request": False})
    )


def load_events(spark: SparkSession, data_dir: str) -> DataFrame:
    """Client events with the device attached: the input of every task."""
    return attach_device(load_client_events(spark, data_dir), load_requests(spark, data_dir))


def filter_by_device(events: DataFrame, device: str) -> DataFrame:
    """Restrict the events to one device type; ``"all"`` keeps everything.

    Applied once, before any task, so every output of a run describes the
    same population. Filtering event rows is safe because ``hw_type`` is
    constant within a navigation: a navigation is kept or dropped whole.
    """
    if device not in DEVICE_CHOICES:
        raise ValueError(f"device must be one of {DEVICE_CHOICES}, got {device!r}")
    if device == "all":
        return events
    return events.filter(F.col("hw_type") == device)
