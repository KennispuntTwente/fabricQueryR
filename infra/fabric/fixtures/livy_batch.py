"""Deterministic Fabric Livy batch fixture used by integration tests."""

import sys
import time

from pyspark.sql import SparkSession


mode = sys.argv[1] if len(sys.argv) > 1 else "success"
spark = SparkSession.builder.appName(f"fabricqueryr-batch-{mode}").getOrCreate()

if mode == "success":
    row_count = spark.sql(
        "SELECT COUNT(*) FROM dbo.fabricqueryr_basic"
    ).first()[0]
    print(f"FABRICQUERYR_BATCH_ROW_COUNT={row_count}", flush=True)
elif mode == "failure":
    raise RuntimeError("FABRICQUERYR_INTENTIONAL_BATCH_FAILURE")
elif mode == "slow":
    print("FABRICQUERYR_BATCH_READY_FOR_CANCELLATION", flush=True)
    time.sleep(600)
else:
    raise ValueError(f"Unknown fabricQueryR batch fixture mode: {mode}")
