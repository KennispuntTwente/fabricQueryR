"""Deterministic Fabric Livy batch fixture used by integration tests."""

import sys
import time

from pyspark.sql import SparkSession


mode = sys.argv[1] if len(sys.argv) > 1 else "success"
spark = SparkSession.builder.appName(f"fabricqueryr-batch-{mode}").getOrCreate()


def write_marker(marker_mode, row_count):
    marker = spark.createDataFrame(
        [(marker_mode, int(row_count))],
        "mode string, row_count long",
    )
    (
        marker.write.format("delta")
        .mode("overwrite")
        .saveAsTable("dbo.fabricqueryr_livy_batch_result")
    )


if mode == "success":
    row_count = spark.sql(
        "SELECT COUNT(*) FROM dbo.fabricqueryr_basic"
    ).first()[0]
    write_marker(mode, row_count)
    print(f"FABRICQUERYR_BATCH_ROW_COUNT={row_count}", flush=True)
elif mode == "failure":
    write_marker(mode, -1)
    raise RuntimeError("FABRICQUERYR_INTENTIONAL_BATCH_FAILURE")
elif mode == "slow":
    write_marker(mode, -1)
    print("FABRICQUERYR_BATCH_READY_FOR_CANCELLATION", flush=True)
    time.sleep(600)
else:
    raise ValueError(f"Unknown fabricQueryR batch fixture mode: {mode}")
