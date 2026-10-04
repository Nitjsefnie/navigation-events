"""Write task results: one CSV per task, a PNG of the histogram, a JSON summary.

Every task result is an aggregate or a short list of navigations (at most a
few hundred rows here), so it is brought to the driver with ``toPandas`` and
written as one plain file a reviewer can open. Event-level data never leaves
Spark. A production job with large outputs would use ``DataFrame.write`` to
distributed storage instead.
"""

import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # headless: the CLI runs without a display
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402
from pyspark.sql import DataFrame  # noqa: E402

# Buckets for the chart only; the CSV keeps every exact count. One
# navigation loses 733 events while 97.6 % lose none, so equal-width bins
# would show one bar and noise.
_BUCKETS = [(1, 1), (2, 2), (3, 5), (6, 10), (11, 20), (21, 50), (51, 100), (101, None)]
_INSIDE, _BEFORE = "#2a78d6", "#eb6834"  # colour-vision-deficiency-safe pair
_SURFACE, _TEXT, _MUTED = "#fcfcfb", "#0b0b0b", "#52514e"


def write_csv(df: DataFrame, path: Path) -> pd.DataFrame:
    """Collect a small result and write it as a single CSV; return it for the summary."""
    result = df.toPandas()
    result.to_csv(path, index=False)
    return result


def write_json(data: dict, path: Path) -> None:
    path.write_text(json.dumps(data, indent=2) + "\n")


def plot_lost_events_histogram(histogram: pd.DataFrame, device: str, path: Path) -> None:
    """Stacked bars of navigations per lost-events bucket, split by window start.

    The zero bucket is stated in the title instead of drawn: at ~98 % of the
    navigations it would flatten every other bar, and a log axis would
    misrepresent the stacked split.
    """
    total = int(histogram["navigations"].sum())
    complete = int(histogram.loc[histogram["lost_events"] == 0, "navigations"].sum())
    labels, inside, before = [], [], []
    for low, high in _BUCKETS:
        lost = histogram["lost_events"]
        rows = histogram[lost >= low if high is None else lost.between(low, high)]
        n_before = int(rows["navigations_started_before_window"].sum())
        labels.append(f"{low}+" if high is None else str(low) if low == high else f"{low}-{high}")
        before.append(n_before)
        inside.append(int(rows["navigations"].sum()) - n_before)

    fig, ax = plt.subplots(figsize=(9, 5), facecolor=_SURFACE)
    ax.set_facecolor(_SURFACE)
    x = range(len(labels))
    bar = dict(width=0.6, edgecolor=_SURFACE, linewidth=2)
    ax.bar(x, inside, color=_INSIDE, label="started inside the extract window", **bar)
    ax.bar(x, before, bottom=inside, color=_BEFORE, label="started before the window (start cut off)", **bar)
    totals = [a + b for a, b in zip(inside, before)]
    for xi, height in zip(x, totals):
        if height:
            ax.annotate(str(height), (xi, height), xytext=(0, 3), textcoords="offset points",
                        ha="center", fontsize=9, color=_TEXT)
    ax.set_ylim(0, max(totals + [1]) * 1.15)
    ax.set_xticks(list(x), labels)
    ax.set_xlabel("lost events per navigation (missing prefix + inner gaps)", color=_MUTED)
    ax.set_ylabel("navigations", color=_MUTED)
    ax.tick_params(colors=_MUTED)
    ax.spines[["top", "right"]].set_visible(False)
    ax.grid(axis="y", color="#e6e5e0", linewidth=0.8)
    ax.set_axisbelow(True)
    share = complete / total if total else 0.0
    ax.set_title(
        f"Lost events per navigation, device = {device}\n"
        f"{total} navigations; {complete} ({share:.1%}) lost none and are not drawn",
        loc="left", color=_TEXT, fontsize=11, pad=24,
    )
    # The legend sits in its own band under the title, so it can never cover a bar.
    ax.legend(loc="lower left", bbox_to_anchor=(0, 1.0), ncol=2, frameon=False, labelcolor=_TEXT)
    fig.tight_layout()
    fig.savefig(path, dpi=120, facecolor=_SURFACE)
    plt.close(fig)
