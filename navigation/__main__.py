"""Run all five tasks for one device setting and write their outputs.

    python -m navigation --data-dir DIR --device {all,phone,desktop,tablet} --out DIR

Writes into ``--out``: one CSV per task, ``task3_lost_events_histogram.png``
and ``summary.json`` with the headline numbers of the run.
"""

import argparse
import json
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from navigation import output, tasks
from navigation.loader import DEVICE_CHOICES, filter_by_device, load_events
from navigation.spark import get_spark

TASKS = {
    "task1_incomplete_navigations": tasks.incomplete_navigations,
    "task2_not_starting_from_zero": tasks.navigations_not_starting_from_zero,
    "task3_lost_events_histogram": tasks.lost_events_histogram,
    "task4_last_event_types": tasks.last_event_types,
    "task5_event_type_frequency": tasks.event_type_frequency,
}


def _summary(events: DataFrame, results: dict, device: str) -> dict:
    """Headline numbers of a run: one aggregation over the navigation summary."""

    def total(condition):
        return F.coalesce(F.sum(condition.cast("long")), F.lit(0))

    nav = tasks.navigation_summary(events)
    lost, before = F.col("lost_events"), F.col("started_before_window")
    row = nav.agg(
        F.count(F.lit(1)).alias("navigations"),
        total(~F.col("has_request")).alias("navigations_without_request"),
        total(lost > 0).alias("incomplete_navigations"),
        total((lost > 0) & before).alias("incomplete_started_before_window"),
        total(F.col("first_serial_id") > 0).alias("not_starting_from_zero"),
        total((F.col("first_serial_id") > 0) & before).alias("not_starting_from_zero_started_before_window"),
        F.coalesce(F.sum("lost_events"), F.lit(0)).alias("lost_events"),
        F.coalesce(F.sum("missing_prefix"), F.lit(0)).alias("lost_in_prefix"),
        F.coalesce(F.sum("missing_inner"), F.lit(0)).alias("lost_in_inner_gaps"),
        F.coalesce(F.sum(F.when(before, lost)), F.lit(0)).alias("lost_in_navigations_started_before_window"),
        F.coalesce(F.max("lost_events"), F.lit(0)).alias("max_lost_in_one_navigation"),
        total(F.col("duplicate_events") > 0).alias("navigations_with_duplicates"),
        F.coalesce(F.sum("duplicate_events"), F.lit(0)).alias("duplicate_events"),
    ).first()

    def winners(name):
        result = results[name]
        return result.loc[result["rank"] == 1, "event_type"].tolist()

    return {
        "device": device,
        **row.asDict(),
        "most_common_last_event_type": winners("task4_last_event_types"),
        "most_frequent_event_type": winners("task5_event_type_frequency"),
    }


def run(spark: SparkSession, data_dir: str, device: str, out_dir: str) -> dict:
    """Load, filter once by device, run the five tasks and write their outputs."""
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    # The filtered events feed every task; caching them means the bzip2
    # files are decompressed and parsed once per run instead of once per task.
    events = filter_by_device(load_events(spark, data_dir), device).cache()
    try:
        results = {name: output.write_csv(task(events), out / f"{name}.csv") for name, task in TASKS.items()}
        output.plot_lost_events_histogram(
            results["task3_lost_events_histogram"], device, out / "task3_lost_events_histogram.png"
        )
        summary = _summary(events, results, device)
        output.write_json(summary, out / "summary.json")
    finally:
        events.unpersist()
    return summary


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(prog="python -m navigation", description=__doc__.splitlines()[0])
    parser.add_argument("--data-dir", required=True, help="directory holding the five *.json.bz2 files")
    parser.add_argument("--device", choices=DEVICE_CHOICES, default="all",
                        help="restrict every task to one device type (default: all)")
    parser.add_argument("--out", required=True, help="output directory of this run")
    args = parser.parse_args(argv)
    spark = get_spark()
    try:
        print(json.dumps(run(spark, args.data_dir, args.device, args.out), indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
