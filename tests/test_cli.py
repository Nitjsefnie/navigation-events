"""End to end: the CLI's run() on tiny source files writes every output."""

import csv
import json

import pytest

from navigation.__main__ import main, run

OUTPUTS = [
    "summary.json",
    "task1_incomplete_navigations.csv",
    "task2_not_starting_from_zero.csv",
    "task3_lost_events_histogram.csv",
    "task3_lost_events_histogram.png",
    "task4_last_event_types.csv",
    "task5_event_type_frequency.csv",
]


def test_run_writes_every_output(spark, data_dir, tmp_path):
    out = tmp_path / "all"
    summary = run(spark, data_dir, "all", str(out))
    assert sorted(p.name for p in out.iterdir()) == OUTPUTS
    assert json.loads((out / "summary.json").read_text()) == summary
    assert (out / "task3_lost_events_histogram.png").read_bytes().startswith(b"\x89PNG")
    # nav 1 (phone) is complete with one re-delivery, nav 2 (desktop) is
    # complete, nav 3 has no request row and lost serial 1.
    assert summary["navigations"] == 3
    assert summary["navigations_without_request"] == 1
    assert (summary["incomplete_navigations"], summary["lost_events"]) == (1, 1)
    assert (summary["navigations_with_duplicates"], summary["duplicate_events"]) == (1, 1)
    assert summary["most_common_last_event_type"] == ["box-change"]
    assert summary["most_frequent_event_type"] == ["page-change"]
    with open(out / "task1_incomplete_navigations.csv") as fh:
        assert [row["navigation"] for row in csv.DictReader(fh)] == ["3"]


def test_device_filter_reaches_every_output(spark, data_dir, tmp_path):
    out = tmp_path / "desktop"
    summary = run(spark, data_dir, "desktop", str(out))
    assert summary["navigations"] == 1
    assert summary["incomplete_navigations"] == 0
    assert summary["most_common_last_event_type"] == ["mouse-down"]
    with open(out / "task3_lost_events_histogram.csv") as fh:
        assert [(row["lost_events"], row["navigations"]) for row in csv.DictReader(fh)] == [("0", "1")]
    with open(out / "task5_event_type_frequency.csv") as fh:
        assert sorted(row["event_type"] for row in csv.DictReader(fh)) == ["mouse-down", "page-change"]


def test_device_without_navigations_still_writes_every_output(spark, data_dir, tmp_path):
    out = tmp_path / "tablet"
    summary = run(spark, data_dir, "tablet", str(out))
    assert sorted(p.name for p in out.iterdir()) == OUTPUTS
    assert (summary["navigations"], summary["lost_events"]) == (0, 0)


def test_cli_rejects_an_unknown_device():
    with pytest.raises(SystemExit) as error:
        main(["--data-dir", "data", "--device", "mobile", "--out", "out"])
    assert error.value.code == 2
