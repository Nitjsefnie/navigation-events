"""Pin each task's definition on small hand-built navigations."""

from datetime import timedelta

from pyspark.sql import functions as F

from conftest import EVENTS_SCHEMA, T0
from navigation import tasks


def _by_navigation(df):
    return {r["navigation"]: r for r in df.collect()}


# --------------------------------------------------------------------------
# navigation_summary: the shared definition of a lost event
# --------------------------------------------------------------------------
def test_summary_separates_inner_gaps_prefix_and_duplicates(make_events):
    events = make_events([
        # nav 1: complete 0..3
        (1, 0, "page-change"), (1, 1, "box-create"), (1, 2, "box-change"), (1, 3, "mouse-down"),
        # nav 2: serials 2 and 3 missing inside the range
        (2, 0, "page-change"), (2, 1, "box-change"), (2, 4, "box-change"),
        # nav 3: serials 0 and 1 missing before the first received one
        (3, 2, "box-change"), (3, 3, "mouse-down"),
        # nav 4: serial 1 delivered twice, the copy 5 s later
        (4, 0, "page-change"), (4, 1, "box-change"), (4, 1, "box-change", "phone", 1000, 5.0),
    ])
    s = _by_navigation(tasks.navigation_summary(events))
    assert (s[1]["lost_events"], s[1]["duplicate_events"]) == (0, 0)
    assert (s[2]["missing_prefix"], s[2]["missing_inner"], s[2]["lost_events"]) == (0, 2, 2)
    assert (s[3]["missing_prefix"], s[3]["missing_inner"], s[3]["lost_events"]) == (2, 0, 2)
    # A duplicate is not a lost event, but it is counted.
    assert (s[4]["received_events"], s[4]["lost_events"], s[4]["duplicate_events"]) == (2, 0, 1)


def test_trailing_loss_is_not_detectable(make_events):
    # Whether this navigation ended at serial 1 or lost serials 2.. cannot be
    # told from the data, so lost_events is a lower bound.
    events = make_events([(1, 0, "page-change"), (1, 1, "mouse-down")])
    assert tasks.navigation_summary(events).first()["lost_events"] == 0


def test_started_before_window_uses_arrival_minus_relative_time(spark):
    rows = [
        # nav 1 arrives 2 s after the hour but is 5 s into the navigation:
        # it began 3 s before the hour at the latest.
        (1, 5, 5000, T0 + timedelta(seconds=2), "box-change", "phone", True),
        # nav 2 began 10 s after the hour.
        (2, 0, 100, T0 + timedelta(seconds=10, milliseconds=100), "page-change", "phone", True),
        (2, 1, 900, T0 + timedelta(seconds=10, milliseconds=900), "box-change", "phone", True),
    ]
    s = _by_navigation(tasks.navigation_summary(spark.createDataFrame(rows, EVENTS_SCHEMA)))
    assert s[1]["started_before_window"] is True
    assert s[2]["started_before_window"] is False


def test_start_bound_is_the_earliest_over_all_events(spark):
    # Serial 1 arrives right after it happens and bounds the start at 3 s
    # before the hour. Serial 2 arrives 6 s late and alone would bound it at
    # 6 s after. The tighter (smaller) bound is the one that holds.
    rows = [
        (1, 1, 5000, T0 + timedelta(seconds=2), "box-change", "phone", True),
        (1, 2, 6000, T0 + timedelta(seconds=12), "mouse-down", "phone", True),
    ]
    summary = tasks.navigation_summary(spark.createDataFrame(rows, EVENTS_SCHEMA))
    row = summary.select(F.unix_micros("implied_start").alias("start_us"), "started_before_window").first()
    assert row["start_us"] == int((T0 - timedelta(seconds=3)).timestamp()) * 1_000_000
    assert row["started_before_window"] is True


def test_window_starts_at_the_hour_not_at_the_earliest_arrival(spark):
    # The earliest arrival is 30 s after the hour; the navigation began 10 s
    # after it. It is inside the window, although it precedes the earliest
    # arrival of this (for example device-filtered) frame.
    rows = [(1, 3, 20000, T0 + timedelta(seconds=30), "box-change", "phone", True)]
    s = _by_navigation(tasks.navigation_summary(spark.createDataFrame(rows, EVENTS_SCHEMA)))
    assert s[1]["started_before_window"] is False


# --------------------------------------------------------------------------
# Task 1
# --------------------------------------------------------------------------
def test_incomplete_means_a_lost_client_event(make_events):
    events = make_events([
        (1, 0, "page-change"), (1, 1, "box-change"),                         # complete
        (2, 0, "page-change"), (2, 2, "box-change"),                         # inner gap
        (3, 1, "box-change"),                                                # prefix lost
        (4, 0, "page-change", None), (4, 1, "box-change", None),             # no request, complete
        (5, 0, "page-change", None), (5, 2, "box-change", None),             # no request, gap
        (6, 0, "page-change"), (6, 0, "page-change", "phone", 0, 3.0),       # duplicate only
    ])
    result = _by_navigation(tasks.incomplete_navigations(events))
    assert sorted(result) == [2, 3, 5]
    assert (result[2]["missing_prefix"], result[2]["missing_inner"]) == (0, 1)
    assert (result[3]["missing_prefix"], result[3]["missing_inner"]) == (1, 0)
    assert (result[5]["has_request"], result[5]["hw_type"]) == (False, "unknown")


# --------------------------------------------------------------------------
# Task 2
# --------------------------------------------------------------------------
def test_not_starting_from_zero_is_literal(make_events):
    events = make_events([
        (1, 0, "page-change"), (1, 1, "box-change"),
        (2, 1, "box-create"), (2, 2, "box-change"),
        (3, 0, "page-change"), (3, 3, "mouse-down"),   # gap, but starts at 0
        (4, 7, "mouse-down"),                          # a single late event
    ])
    result = _by_navigation(tasks.navigations_not_starting_from_zero(events))
    assert sorted(result) == [2, 4]
    assert (result[2]["first_serial_id"], result[2]["first_event_type"]) == (1, "box-create")


# --------------------------------------------------------------------------
# Task 3
# --------------------------------------------------------------------------
def test_histogram_covers_every_navigation(make_events):
    events = make_events([
        (1, 0, "page-change"),
        (2, 0, "page-change"),
        (3, 0, "page-change"), (3, 2, "box-change"),                       # 1 lost
        (4, 3, "box-change"),                                              # 3 lost (prefix)
        (5, 0, "page-change"), (5, 0, "page-change", "phone", 0, 1.0),     # duplicate: 0 lost
    ])
    hist = {r["lost_events"]: r["navigations"] for r in tasks.lost_events_histogram(events).collect()}
    assert hist == {0: 3, 1: 1, 3: 1}


def test_histogram_splits_buckets_by_window_start(spark):
    # Both navigations lost serials 0-2. Navigation 1 began before the hour
    # (as in the window test above), navigation 2 inside it.
    rows = [
        (1, 3, 5000, T0 + timedelta(seconds=2), "box-change", "phone", True),
        (2, 3, 500, T0 + timedelta(seconds=10), "box-change", "phone", True),
    ]
    hist = tasks.lost_events_histogram(spark.createDataFrame(rows, EVENTS_SCHEMA)).collect()
    assert [(r["lost_events"], r["navigations"], r["navigations_started_before_window"]) for r in hist] \
        == [(3, 2, 1)]


# --------------------------------------------------------------------------
# Task 4
# --------------------------------------------------------------------------
def test_last_event_is_highest_serial_not_latest_arrival(make_events):
    # The mouse-down (serial 1) is the last client event but arrives first:
    # serial 0 is delayed by 10 s. Arrival order would call page-change last.
    events = make_events([(1, 0, "page-change", "phone", 0, 10.0), (1, 1, "mouse-down")])
    result = tasks.last_event_types(events).collect()
    assert [(r["event_type"], r["navigations"], r["rank"]) for r in result] == [("mouse-down", 1, 1)]


def test_last_event_is_highest_serial_when_relative_times_tie(make_events):
    # Two events of different types share the maximum relativeTimeMs (1 061
    # navigations tie there, five of them across types). The higher serial
    # is the later event. The rows come in both orders, so no tie-breaking
    # by input order can pass by accident.
    events = make_events([
        (1, 0, "page-change", "phone", 100), (1, 1, "mouse-down", "phone", 100),
        (2, 1, "mouse-down", "phone", 100), (2, 0, "page-change", "phone", 100),
    ])
    result = tasks.last_event_types(events).collect()
    assert [(r["event_type"], r["navigations"], r["rank"]) for r in result] == [("mouse-down", 2, 1)]


def test_last_event_ties_share_a_rank(make_events):
    events = make_events([
        (1, 0, "page-change"), (1, 1, "mouse-down"),
        (2, 0, "page-change"), (2, 1, "box-change"),
        (3, 0, "page-change"), (3, 1, "box-change"), (3, 2, "mouse-down"),
        (4, 0, "page-change"),
    ])
    result = [(r["event_type"], r["navigations"], r["share_of_navigations"], r["rank"])
              for r in tasks.last_event_types(events).collect()]
    assert result == [
        ("mouse-down", 2, 0.5, 1),
        ("box-change", 1, 0.25, 2),
        ("page-change", 1, 0.25, 2),
    ]


# --------------------------------------------------------------------------
# Task 5
# --------------------------------------------------------------------------
def test_event_types_are_counted_once_per_serial(make_events):
    events = make_events([
        # nav 1: two box-change and a page-change; one box-change re-delivered
        (1, 0, "page-change"), (1, 1, "box-change"), (1, 2, "box-change"),
        (1, 2, "box-change", "phone", 2000, 4.0),
        # nav 2: one event of each of three types, a three-way tie
        (2, 0, "page-change"), (2, 1, "mouse-down"), (2, 2, "box-change"),
    ])
    rows = {r["event_type"]: r for r in tasks.event_type_frequency(events).collect()}
    assert rows["box-change"]["events"] == 3  # not 4: the re-delivery counts once
    assert rows["box-change"]["rank"] == 1
    assert rows["page-change"]["events"] == 2
    assert rows["page-change"]["navigations_with_type"] == 2
    # nav 1: box-change leads; nav 2: every type ties and is credited.
    assert rows["box-change"]["navigations_where_most_frequent"] == 2
    assert rows["page-change"]["navigations_where_most_frequent"] == 1
    assert rows["mouse-down"]["navigations_where_most_frequent"] == 1


def test_event_types_rank_by_volume_not_by_coverage(make_events):
    # box-change has more events (3 against 2), page-change appears in more
    # navigations (2 against 1). The ranking and the shares follow volume.
    events = make_events([
        (1, 0, "page-change"), (1, 1, "box-change"), (1, 2, "box-change"), (1, 3, "box-change"),
        (2, 0, "page-change"),
    ])
    result = [
        (r["event_type"], r["events"], r["share_of_events"], r["navigations_with_type"],
         r["navigations_where_most_frequent"], r["rank"])
        for r in tasks.event_type_frequency(events).collect()
    ]
    assert result == [
        ("box-change", 3, 0.6, 1, 1, 1),
        ("page-change", 2, 0.4, 2, 1, 2),
    ]


def test_event_type_ties_share_a_rank(make_events):
    events = make_events([(1, 0, "page-change"), (1, 1, "box-change")])
    ranks = {r["event_type"]: r["rank"] for r in tasks.event_type_frequency(events).collect()}
    assert ranks == {"page-change": 1, "box-change": 1}
