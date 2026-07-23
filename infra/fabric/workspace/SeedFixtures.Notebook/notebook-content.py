# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "00000000-0000-0000-0000-000000000001",
# META       "default_lakehouse_name": "TestLakehouse",
# META       "default_lakehouse_workspace_id": "00000000-0000-0000-0000-000000000002"
# META     }
# META   }
# META }

# CELL ********************

import traceback

from pyspark.sql import functions as F

stage = "read uploaded CSV fixture"
try:
    fixture = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv("/lakehouse/default/Files/fixtures/basic.csv")
    )

    stage = "write basic Delta table"
    (
        fixture.withColumn(
            "loaded_at", F.lit("2026-01-01T00:00:00Z").cast("timestamp")
        )
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", True)
        .saveAsTable("dbo.fabricqueryr_basic")
    )

    stage = "write partitioned Delta table"
    (
        fixture.write.format("delta")
        .mode("overwrite")
        .partitionBy("category")
        .option("overwriteSchema", True)
        .saveAsTable("dbo.fabricqueryr_partitioned")
    )

    stage = "generate Delta checkpoint"
    for _ in range(10):
        fixture.limit(1).write.format("delta").mode("append").saveAsTable(
            "dbo.fabricqueryr_partitioned"
        )
except Exception:
    mssparkutils.notebook.exit(
        f"fabricqueryr-seed-error: {stage}\n{traceback.format_exc()}"
    )

mssparkutils.notebook.exit("fabricqueryr-seed-success")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }