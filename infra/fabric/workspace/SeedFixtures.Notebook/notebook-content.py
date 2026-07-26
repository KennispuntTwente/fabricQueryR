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

workspace_id = "00000000-0000-0000-0000-000000000002"
lakehouse_id = "00000000-0000-0000-0000-000000000001"
fixture_path = (
    f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/"
    f"{lakehouse_id}/Files/fixtures/basic.csv"
)

stage = "read uploaded CSV fixture"
try:
    fixture = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(fixture_path)
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

    stage = "write empty Delta table"
    (
        fixture.limit(0)
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", True)
        .saveAsTable("dbo.fabricqueryr_empty")
    )

    stage = "write partitioned Delta table"
    (
        fixture.write.format("delta")
        .mode("overwrite")
        .partitionBy("category")
        .option("overwriteSchema", True)
        .saveAsTable("dbo.fabricqueryr_partitioned")
    )

    stage = "write typed and null partition Delta table"
    typed_partitions = (
        fixture.select("id", "name", "amount")
        .withColumn(
            "event_date",
            F.when(
                F.col("id") == 3,
                F.lit(None).cast("date"),
            ).otherwise(
                F.date_add(
                    F.lit("2026-01-01").cast("date"),
                    F.col("id") - 1,
                )
            ),
        )
        .withColumn(
            "active",
            F.when(
                F.col("id") == 3,
                F.lit(None).cast("boolean"),
            ).otherwise((F.col("id") % 2) == 1),
        )
    )
    (
        typed_partitions.write.format("delta")
        .mode("overwrite")
        .partitionBy("event_date", "active")
        .option("overwriteSchema", True)
        .saveAsTable("dbo.fabricqueryr_typed_partitions")
    )

    stage = "generate Delta checkpoint"
    for _ in range(10):
        fixture.limit(1).write.format("delta").mode("append").saveAsTable(
            "dbo.fabricqueryr_partitioned"
        )

    stage = "replace a partition after the Delta checkpoint"
    replacement = (
        fixture.filter(F.col("category") == "B")
        .withColumn("name", F.lit("beta-updated"))
        .withColumn("amount", F.lit(21.0))
    )
    (
        replacement.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", "category = 'B'")
        .saveAsTable("dbo.fabricqueryr_partitioned")
    )

    stage = "write schema-evolution Delta table"
    (
        fixture.filter(F.col("id") < 3)
        .select("id", "name")
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", True)
        .saveAsTable("dbo.fabricqueryr_schema_evolved")
    )
    (
        fixture.filter(F.col("id") == 3)
        .select("id", "name")
        .withColumn("evolved_value", F.lit("introduced"))
        .write.format("delta")
        .mode("append")
        .option("mergeSchema", True)
        .saveAsTable("dbo.fabricqueryr_schema_evolved")
    )

    stage = "write column-mapping Delta table"
    (
        fixture.write.format("delta")
        .mode("overwrite")
        .option("delta.columnMapping.mode", "name")
        .saveAsTable("dbo.fabricqueryr_column_mapped")
    )

    stage = "write deletion-vector Delta table"
    (
        fixture.write.format("delta")
        .mode("overwrite")
        .option("delta.enableDeletionVectors", "true")
        .saveAsTable("dbo.fabricqueryr_deletion_vectors")
    )
    spark.sql(
        "DELETE FROM dbo.fabricqueryr_deletion_vectors WHERE id = 1"
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
