"""The five monitoring tasks and the per-navigation summary they share.

Every task takes the events DataFrame from ``loader.load_events`` (already
restricted by ``loader.filter_by_device``) and returns a small DataFrame.
The definitions and their evidence are in README.md; the comments here cover
what a reviewer is likely to question.

Facts from a full scan of the data that the definitions rely on
(reproduced by ``python -m navigation.profile``):

* ``serialId`` is a per-navigation counter starting at **0**. 10 291 of the
  10 454 navigations have a serial 0, and every serial-0 row is a
  ``page-change`` carrying ``clientSize``: the initial render of the results
  page. The server-side request has no ``serialId`` and is not part of the
  sequence.
* ``relativeTimeMs`` never decreases as ``serialId`` grows, so both describe
  the same client-side order. ``eventTime`` is the *arrival* time and goes
  backwards against ``serialId`` 21 813 times (events are sent in batches).
* A duplicate is a second row with the same ``(navigation, serialId)``: 85
  of them, each with the same type and payload as its original and a later
  ``eventTime`` (0.04-30 s). They are re-deliveries, not new events.
* The extract is one hour of ``eventTime`` (21:00-22:00 UTC), so
  navigations that began before 21:00 lost their start to the cut.
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def navigation_summary(events: DataFrame) -> DataFrame:
    """One row per navigation with the completeness facts tasks 1-4 use.

    * ``first_serial_id`` / ``last_serial_id``: lowest / highest received serial.
    * ``received_events``: distinct serials received; duplicates count once.
    * ``duplicate_events``: copies beyond the first.
    * ``missing_prefix``: serials ``0 .. first-1``, never received
      (= ``first_serial_id``, because sequences start at 0).
    * ``missing_inner``: holes inside ``[first, last]``.
    * ``lost_events`` = prefix + inner = ``last_serial_id + 1 - received_events``.
      Events lost *after* the last received serial cannot be seen: the data
      has no end marker and no "events sent" counter, so this is a lower bound.
    * ``first_event_type`` / ``last_event_type``: type at the min / max serial.
    * ``implied_start`` / ``started_before_window``: see below.

    An event happens ``relativeTimeMs`` after the navigation starts and can
    only arrive after it happens, so ``eventTime - relativeTimeMs`` of any
    event is an upper bound of the navigation's start. If the smallest such
    bound is before the extract window opens, the navigation provably began
    before the window, and its missing start was cut off by the extract
    rather than lost in delivery. The window start is the hour containing the
    earliest ``eventTime``: the earliest arrival itself would move by a
    fraction of a second with the device filter and flip borderline
    navigations, while the hour is the same for every filter (the extract is
    assumed to be hourly).
    """
    window_start = events.agg(F.date_trunc("hour", F.min("event_time")).alias("window_start"))
    # Microsecond arithmetic keeps the bound exact and avoids interval types.
    implied_start_us = F.unix_micros("event_time") - F.col("relative_time_ms") * 1000
    summary = events.groupBy("navigation").agg(
        # Both come from the request join and are constant within a navigation.
        F.first("hw_type").alias("hw_type"),
        F.first("has_request").alias("has_request"),
        F.min("serial_id").alias("first_serial_id"),
        F.max("serial_id").alias("last_serial_id"),
        F.count_distinct("serial_id").alias("received_events"),
        (F.count(F.lit(1)) - F.count_distinct("serial_id")).alias("duplicate_events"),
        # min_by/max_by take the type at the extreme serial in the same
        # aggregation, without a window or a self-join. A re-delivered serial
        # has the same type as its original, so duplicates cannot make the
        # choice ambiguous.
        F.min_by("event_type", "serial_id").alias("first_event_type"),
        F.max_by("event_type", "serial_id").alias("last_event_type"),
        F.timestamp_micros(F.min(implied_start_us)).alias("implied_start"),
    )
    # window_start is a one-row frame: the cross join is a broadcast, not a shuffle.
    return (
        summary.crossJoin(window_start)
        .withColumn("missing_prefix", F.col("first_serial_id"))
        .withColumn(
            "missing_inner",
            F.col("last_serial_id") - F.col("first_serial_id") + 1 - F.col("received_events"),
        )
        .withColumn("lost_events", F.col("missing_prefix") + F.col("missing_inner"))
        .withColumn("started_before_window", F.col("implied_start") < F.col("window_start"))
        .drop("window_start")
    )


def incomplete_navigations(events: DataFrame) -> DataFrame:
    """Task 1: navigations with at least one detectably lost client event.

    Incomplete means ``lost_events > 0``: a missing prefix or a hole inside
    the received serial range. A navigation that is merely short is not
    incomplete, and duplicates alone are not either (nothing is missing);
    their count is a column because it is a signal of the same delivery path.

    A missing request row is not part of the definition. It is a different
    collection path (server side), and because the device comes from the
    request, such a navigation could only ever be counted under ``all``,
    which would make ``all`` and the device runs measure different things.
    ``has_request`` is a column, and the run summary counts those navigations.
    """
    return (
        navigation_summary(events)
        .filter(F.col("lost_events") > 0)
        .select(
            "navigation", "hw_type", "has_request",
            "first_serial_id", "last_serial_id", "received_events",
            "missing_prefix", "missing_inner", "lost_events", "duplicate_events",
            "started_before_window",
        )
        .orderBy("navigation")
    )


def navigations_not_starting_from_zero(events: DataFrame) -> DataFrame:
    """Task 2: navigations whose lowest received ``serial_id`` is above 0.

    Serial 0 exists in the data (the initial ``page-change``), so "from zero"
    is taken literally. ``started_before_window`` separates navigations whose
    start lies before the extract from those whose start was really lost.
    """
    return (
        navigation_summary(events)
        .filter(F.col("first_serial_id") > 0)
        .select(
            "navigation", "hw_type", "has_request",
            "first_serial_id", "first_event_type", "received_events",
            "implied_start", "started_before_window",
        )
        .orderBy("navigation")
    )


def lost_events_histogram(events: DataFrame) -> DataFrame:
    """Task 3: number of navigations per count of lost events, over all navigations.

    ``lost_events`` is the measure of task 1. The zero bucket is included:
    "all navigations" includes the complete ones, and their share is the
    headline. Each bucket is split by ``started_before_window`` so losses
    caused by the extract boundary can be told apart from delivery losses.
    """
    return (
        navigation_summary(events)
        .groupBy("lost_events")
        .agg(
            F.count(F.lit(1)).alias("navigations"),
            F.sum(F.col("started_before_window").cast("long")).alias("navigations_started_before_window"),
        )
        .orderBy("lost_events")
    )


def _rank_by(counts: DataFrame, count_col: str, share_col: str) -> DataFrame:
    """Add the share of the total and a rank by ``count_col``; equal counts share a rank.

    Both windows have no partition key, so Spark moves the rows to one
    partition. That is right here: the input is an aggregate with one row
    per event type, four rows.
    """
    return (
        counts.withColumn(
            share_col, F.round(F.col(count_col) / F.sum(count_col).over(Window.partitionBy()), 4)
        )
        .withColumn("rank", F.rank().over(Window.orderBy(F.col(count_col).desc())))
        .orderBy("rank", "event_type")
    )


def last_event_types(events: DataFrame) -> DataFrame:
    """Task 4: types of the navigations' last events, ranked; ``rank = 1`` is the answer.

    "Last" is the highest received ``serial_id``, the client's own order.
    ``relativeTimeMs`` agrees with it but ties at the maximum in 1 061
    navigations (five of them between two types), and breaking those ties by
    serial is the same rule. ``eventTime`` is arrival order and picks a
    different last event in 13 % of navigations, so it would measure the
    network rather than the user.
    """
    counts = (
        navigation_summary(events)
        .groupBy(F.col("last_event_type").alias("event_type"))
        .agg(F.count(F.lit(1)).alias("navigations"))
    )
    return _rank_by(counts, "navigations", "share_of_navigations")


def event_type_frequency(events: DataFrame) -> DataFrame:
    """Task 5: event types ranked by number of events; ``rank = 1`` is the answer.

    An event is a distinct ``(navigation, serial_id)``, so a re-delivery is
    counted once. Two navigation-level readings of "most frequent in
    navigations" are added as columns:

    * ``navigations_where_most_frequent``: navigations in which the type is
      the most frequent one (a tie credits every tied type). It picks the
      same winner as the event count, box-change;
    * ``navigations_with_type``: navigations containing the type at all.
      It picks a different winner, page-change, because nearly every
      navigation has its serial-0 render.

    The request is not counted: it is not part of the client stream and
    occurs once per navigation, so it could never lead.
    """
    # Which copy of a re-delivered event survives does not matter: the
    # copies share their type, and nothing else is read here.
    per_navigation = (
        events.dropDuplicates(["navigation", "serial_id"])
        .groupBy("navigation", "event_type")
        .agg(F.count(F.lit(1)).alias("events"))
        .withColumn(
            "is_most_frequent",
            F.col("events") == F.max("events").over(Window.partitionBy("navigation")),
        )
    )
    counts = per_navigation.groupBy("event_type").agg(
        F.sum("events").alias("events"),
        F.count(F.lit(1)).alias("navigations_with_type"),
        F.sum(F.col("is_most_frequent").cast("long")).alias("navigations_where_most_frequent"),
    )
    return _rank_by(counts, "events", "share_of_events")
