"""SparkSession factory shared by the CLI, the data profile and the tests."""

import os
import sys

from pyspark.sql import SparkSession


def get_spark(app_name: str = "navigation-monitoring", master: str = "local[*]") -> SparkSession:
    """Return a SparkSession configured for this job.

    * ``spark.sql.session.timeZone=UTC``: ``eventTime`` is UTC (``Z`` suffix),
      and hour truncation or timestamp rendering must not depend on the zone
      of the machine running the job.
    * Shuffle partitions stay at the default on purpose: adaptive query
      execution (on by default since Spark 3.2) coalesces the 200 post-shuffle
      partitions to a handful for ~400k rows, and the same code then runs
      unchanged on a cluster.
    * Arrow is off: only small aggregated results reach pandas, so the
      ``pyarrow`` dependency would buy nothing.
    """
    # Python workers must run the driver's interpreter. Otherwise PySpark
    # starts whatever ``python3`` is on PATH and refuses a minor-version
    # mismatch with the driver (PYTHON_VERSION_MISMATCH).
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    spark = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark
