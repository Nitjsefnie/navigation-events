"""Data profile: the evidence behind the definitions in README.md.

    python -m navigation.profile --data-dir DIR --out results/data_profile.json

Every number the README quotes as evidence comes from here, so a reviewer
can re-derive it instead of trusting prose. Each check is one aggregation;
only aggregated values reach the driver.
"""

import argparse
import json

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from navigation import schemas, tasks
from navigation.loader import load_events, load_requests, read_source
from navigation.spark import get_spark


def _leaf_columns(schema, prefix=""):
    for field in schema.fields:
        if hasattr(field.dataType, "fields"):
            yield from _leaf_columns(field.dataType, f"{prefix}{field.name}.")
        else:
            yield f"{prefix}{field.name}"


def _counts(df: DataFrame, *keys) -> dict:
    """``{"k1=v1, k2=v2": rows}`` for a small group-by, in key order."""
    rows = df.groupBy(*keys).count().orderBy(*keys).collect()
    return {", ".join(f"{k}={r[k]}" for k in keys): r["count"] for r in rows}


def _files(spark: SparkSession, data_dir: str) -> dict:
    """Rows and absent (null) values per declared field, per file."""
    out = {}
    for stem, schema in {"request": schemas.REQUEST, **schemas.CLIENT_EVENT_SCHEMAS}.items():
        df = read_source(spark, data_dir, stem, schema)
        cols = list(_leaf_columns(schema))
        row = df.agg(F.count(F.lit(1)).alias("_rows"),
                     *[F.sum(F.col(c).isNull().cast("long")).alias(c) for c in cols]).first()
        out[stem] = {"rows": row["_rows"], "absent": {c: row[c] for c in cols if row[c]}}
    return out


def _duplicates(spark: SparkSession, data_dir: str) -> dict:
    """Repeated (navigation, serialId) within each file, and whether the copies differ."""
    out = {}
    for stem, schema in schemas.CLIENT_EVENT_SCHEMAS.items():
        df = read_source(spark, data_dir, stem, schema)
        payload = F.to_json(F.struct(*[c for c in df.columns if c != "eventTime"]))
        arrival = F.unix_micros(F.to_timestamp("eventTime"))
        out[stem] = df.groupBy("navigation", "serialId").agg(
            F.count(F.lit(1)).alias("copies"),
            F.count_distinct(payload).alias("payloads"),
            ((F.max(arrival) - F.min(arrival)) / 1e6).alias("spread_s"),
        ).filter("copies > 1").agg(
            F.count(F.lit(1)).alias("repeated_serials"),
            F.count_distinct("navigation").alias("navigations"),
            F.max("payloads").alias("max_distinct_payloads"),
            F.min("spread_s").alias("min_arrival_spread_s"),
            F.max("spread_s").alias("max_arrival_spread_s"),
        ).first().asDict()
    return out


def profile(spark: SparkSession, data_dir: str) -> dict:
    out = {"files": _files(spark, data_dir), "duplicates_within_file": _duplicates(spark, data_dir)}

    out["boolean_values"] = {
        f"{stem}.{col}": _counts(read_source(spark, data_dir, stem, schemas.CLIENT_EVENT_SCHEMAS[stem]), col)
        for stem, col in [("box-create", "checkVisibility"), ("box-change", "visibility"),
                          ("mouse-down", "activeElement")]
    }
    page = read_source(spark, data_dir, "page-change", schemas.PAGE_CHANGE)
    out["page_change_size_fields"] = _counts(
        page.select((F.col("serialId") == 0).alias("serial_0"),
                    F.col("clientSize").isNotNull().alias("has_clientSize")),
        "serial_0", "has_clientSize",
    )

    events = load_events(spark, data_dir).cache()
    requests = load_requests(spark, data_dir)

    def time_range(df, col):
        return df.agg(F.min(col).cast("string").alias("min"), F.max(col).cast("string").alias("max")).first().asDict()

    out["time_range_utc"] = {"request": time_range(requests, "request_time"),
                             "client_events": time_range(events, "event_time")}
    out["requests_per_5_minutes"] = _counts(
        requests.select(F.date_format(F.window("request_time", "5 minutes").start, "HH:mm").alias("from")), "from"
    )
    out["hw_type"] = _counts(requests, "hw_type")

    client_navs = events.select("navigation").distinct()
    request_navs = requests.select("navigation").distinct()
    out["navigations"] = {
        "with_client_events": client_navs.count(),
        "with_request": request_navs.count(),
        "client_events_but_no_request": client_navs.join(request_navs, "navigation", "left_anti").count(),
        "request_but_no_client_events": request_navs.join(client_navs, "navigation", "left_anti").count(),
        "request_rows_beyond_one_per_navigation": requests.count() - request_navs.count(),
    }

    # Where does the serial sequence start, and what sits at serial 0?
    first = events.groupBy("navigation").agg(F.min("serial_id").alias("first_serial_id"))
    out["navigations_by_first_serial_id"] = dict(list(_counts(first, "first_serial_id").items())[:10])
    out["navigations_by_first_serial_id_above_9"] = first.filter("first_serial_id > 9").count()
    out["serial_0_rows_by_event_type"] = _counts(events.filter("serial_id = 0"), "event_type")

    # Ordering checks on deduplicated events: does relativeTimeMs / eventTime
    # follow serial_id? The earliest arrival of a re-delivered event is kept.
    deduped = events.groupBy("navigation", "serial_id", "event_type", "relative_time_ms").agg(
        F.min("event_time").alias("event_time"))
    by_serial = Window.partitionBy("navigation").orderBy("serial_id")
    steps = deduped.select(
        "navigation",
        (F.col("relative_time_ms") < F.lag("relative_time_ms").over(by_serial)).alias("relative_back"),
        (F.col("event_time") < F.lag("event_time").over(by_serial)).alias("arrival_back"),
        (F.col("serial_id") - F.lag("serial_id").over(by_serial) - 1).alias("gap_run"),
    ).cache()
    out["ordering_against_serial_id"] = steps.agg(
        F.sum(F.col("relative_back").cast("long")).alias("relative_time_decreases"),
        F.sum(F.col("arrival_back").cast("long")).alias("event_time_decreases"),
        F.count_distinct(F.when(F.col("arrival_back"), F.col("navigation"))).alias("navigations_with_event_time_decrease"),
    ).first().asDict()
    # Inner gaps as runs of consecutive missing serials: single drops vs lost batches.
    out["inner_gap_runs_by_length"] = _counts(steps.filter("gap_run > 0"), "gap_run")

    # Last event: does the choice of ordering change the answer to task 4?
    last = deduped.groupBy("navigation").agg(
        F.max_by("event_type", "serial_id").alias("type_by_serial_id"),
        F.max_by("event_type", "event_time").alias("type_by_event_time"),
        F.max("relative_time_ms").alias("max_relative_time_ms"),
    )
    at_max_relative = (
        deduped.join(last, "navigation").filter(F.col("relative_time_ms") == F.col("max_relative_time_ms"))
        .groupBy("navigation").agg(F.count(F.lit(1)).alias("events"), F.count_distinct("event_type").alias("types"))
    )
    out["last_event"] = {
        "by_serial_id": _counts(last, "type_by_serial_id"),
        "by_event_time": _counts(last, "type_by_event_time"),
        "navigations_where_event_time_picks_another_type": last.filter(
            "type_by_serial_id <> type_by_event_time").count(),
        "navigations_tied_at_max_relative_time": at_max_relative.filter("events > 1").count(),
        "of_which_between_two_types": at_max_relative.filter("types > 1").count(),
    }

    # The extract boundary, with the same definition the tasks use.
    summary = tasks.navigation_summary(events).cache()
    out["window_boundary"] = {
        key: dict(zip(["navigations", "lost_events"], values))
        for key, values in (
            (f"has_request={r['has_request']}, started_before_window={r['started_before_window']}, "
             f"has_serial_0={r['has_serial_0']}", (r["navigations"], r["lost_events"]))
            for r in summary.groupBy("has_request", "started_before_window",
                                     (F.col("first_serial_id") == 0).alias("has_serial_0"))
            .agg(F.count(F.lit(1)).alias("navigations"), F.sum("lost_events").alias("lost_events"))
            .orderBy("has_request", "started_before_window", "has_serial_0").collect()
        )
    }
    # How tight is eventTime - relativeTimeMs as a start bound? For navigations
    # with a request, compare it with the request's arrival.
    lead_ms = (F.unix_micros("request_time") - F.unix_micros("implied_start")) / 1000
    quantiles = summary.join(requests, "navigation").agg(
        F.percentile_approx(lead_ms, [0.01, 0.5, 0.99], 10000).alias("q")).first()["q"]
    out["request_time_minus_implied_start_ms"] = dict(zip(["p01", "p50", "p99"], quantiles))
    # Navigations whose last arrival is in the window's final minute may have
    # lost their tail to the cut at the end, which no serial check can see.
    window_end = F.date_trunc("hour", F.min("event_time")) + F.expr("INTERVAL 1 HOUR")
    end = events.agg(window_end.alias("end")).first()["end"]
    out["navigations_last_arrival_in_final_minute"] = (
        events.groupBy("navigation").agg(F.max("event_time").alias("last_arrival"))
        .filter(F.col("last_arrival") >= F.lit(end) - F.expr("INTERVAL 1 MINUTE")).count()
    )
    for df in (summary, steps, events):
        df.unpersist()
    return out


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(prog="python -m navigation.profile", description=__doc__.splitlines()[0])
    parser.add_argument("--data-dir", required=True)
    parser.add_argument("--out", required=True, help="path of the JSON file to write")
    args = parser.parse_args(argv)
    spark = get_spark()
    try:
        result = profile(spark, args.data_dir)
        with open(args.out, "w") as fh:
            json.dump(result, fh, indent=2)
            fh.write("\n")
        print(json.dumps(result, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
