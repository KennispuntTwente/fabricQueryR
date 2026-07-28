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

    stage = "write Delta void columns"
    (
        spark.range(0, 3)
        .select(
            F.col("id"),
            F.lit(None).alias("always_null"),
            F.struct(
                F.col("id").cast("int").alias("value"),
                F.lit(None).alias("pending"),
            ).alias("details"),
        )
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", True)
        .saveAsTable("dbo.fabricqueryr_void")
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

    stage = "write exact scalar Delta types"
    spark.sql("DROP TABLE IF EXISTS dbo.fabricqueryr_exact_types")
    spark.sql(
        """
        CREATE TABLE dbo.fabricqueryr_exact_types
        USING DELTA
        AS SELECT
          1 AS row_id,
          CAST('9007199254740993' AS BIGINT) AS above_double_limit,
          CAST('9223372036854775807' AS BIGINT) AS maximum_long,
          CAST(
            '12345678901234567890123456789012345678'
            AS DECIMAL(38, 0)
          ) AS whole_decimal,
          CAST(
            '123456789012345678901234567890123456.78'
            AS DECIMAL(38, 2)
          ) AS scaled_decimal,
          CAST('2026-07-28 12:34:56.123456' AS TIMESTAMP_NTZ)
            AS observed_at,
          X'00FF10' AS payload,
          'café-数据-🙂' AS unicode_text,
          CAST('NaN' AS DOUBLE) AS not_a_number,
          CAST('Infinity' AS DOUBLE) AS positive_infinity
        """
    )

    stage = "write nested Delta types"
    spark.sql("DROP TABLE IF EXISTS dbo.fabricqueryr_complex_types")
    spark.sql(
        """
        CREATE TABLE dbo.fabricqueryr_complex_types
        USING DELTA
        TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
        AS SELECT
          1 AS id,
          named_struct(
            'label', 'nested',
            'amount',
            CAST('1234567890123456789012345678901234.56'
              AS DECIMAL(38, 2))
          ) AS profile,
          array(1, 2, 3) AS scores,
          map(
            'large',
            CAST('9007199254740993' AS BIGINT),
            'small',
            CAST('2' AS BIGINT)
          ) AS counts,
          'display café-数据' AS `display name`
        """
    )

    stage = "write column-mapping Delta table"
    spark.sql("DROP TABLE IF EXISTS dbo.fabricqueryr_column_mapped")
    (
        fixture.filter(F.col("id") < 3)
        .select("id", "name", "amount")
        .write.format("delta")
        .mode("overwrite")
        .option("delta.columnMapping.mode", "name")
        .saveAsTable("dbo.fabricqueryr_column_mapped")
    )
    spark.sql(
        """
        ALTER TABLE dbo.fabricqueryr_column_mapped
        RENAME COLUMN name TO display_name
        """
    )
    spark.sql(
        """
        ALTER TABLE dbo.fabricqueryr_column_mapped
        DROP COLUMN amount
        """
    )
    (
        fixture.filter(F.col("id") == 3)
        .select("id", F.col("name").alias("display_name"))
        .write.format("delta")
        .mode("append")
        .saveAsTable("dbo.fabricqueryr_column_mapped")
    )

    stage = "write ID column-mapping Delta table"
    spark.sql("DROP TABLE IF EXISTS dbo.fabricqueryr_column_mapped_id")
    (
        fixture.filter(F.col("id") < 3)
        .select("id", "name")
        .write.format("delta")
        .mode("overwrite")
        .option("delta.columnMapping.mode", "id")
        .saveAsTable("dbo.fabricqueryr_column_mapped_id")
    )
    spark.sql(
        """
        ALTER TABLE dbo.fabricqueryr_column_mapped_id
        RENAME COLUMN name TO display_name
        """
    )
    (
        fixture.filter(F.col("id") == 3)
        .select("id", F.col("name").alias("display_name"))
        .write.format("delta")
        .mode("append")
        .saveAsTable("dbo.fabricqueryr_column_mapped_id")
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

    stage = "write deletion-vector stress Delta table"
    (
        spark.range(0, 5000)
        .repartition(4)
        .select(
            F.col("id"),
            F.concat(F.lit("row-"), F.col("id")).alias("label"),
        )
        .write.format("delta")
        .mode("overwrite")
        .option("delta.enableDeletionVectors", "true")
        .option("maxRecordsPerFile", 500)
        .saveAsTable("dbo.fabricqueryr_deletion_vectors_stress")
    )
    spark.sql(
        """
        DELETE FROM dbo.fabricqueryr_deletion_vectors_stress
        WHERE id % 10 = 0
        """
    )
    spark.sql(
        """
        UPDATE dbo.fabricqueryr_deletion_vectors_stress
        SET label = 'updated'
        WHERE id % 13 = 0
        """
    )
    spark.createDataFrame(
        [(1, "merged"), (5000, "inserted")],
        ["id", "label"],
    ).createOrReplaceTempView("fabricqueryr_dv_merge_source")
    spark.sql(
        """
        MERGE INTO dbo.fabricqueryr_deletion_vectors_stress AS target
        USING fabricqueryr_dv_merge_source AS source
        ON target.id = source.id
        WHEN MATCHED THEN UPDATE SET label = source.label
        WHEN NOT MATCHED THEN INSERT (id, label)
        VALUES (source.id, source.label)
        """
    )

    stage = "write deletion-vector V2 checkpoint table"
    spark.sql("DROP TABLE IF EXISTS dbo.fabricqueryr_deletion_vectors_checkpoint")
    spark.sql(
        """
        CREATE TABLE dbo.fabricqueryr_deletion_vectors_checkpoint (
          id BIGINT,
          label STRING
        )
        USING DELTA
        TBLPROPERTIES (
          'delta.enableDeletionVectors' = 'true',
          'delta.checkpointPolicy' = 'v2',
          'delta.checkpointInterval' = '1'
        )
        """
    )
    (
        spark.range(0, 1000)
        .coalesce(1)
        .select(
            F.col("id"),
            F.concat(F.lit("row-"), F.col("id")).alias("label"),
        )
        .write.format("delta")
        .mode("append")
        .saveAsTable("dbo.fabricqueryr_deletion_vectors_checkpoint")
    )
    spark.sql(
        """
        DELETE FROM dbo.fabricqueryr_deletion_vectors_checkpoint
        WHERE id % 10 = 0
        """
    )
    spark.sql(
        """
        DELETE FROM dbo.fabricqueryr_deletion_vectors_checkpoint
        WHERE id % 10 = 1
        """
    )

    stage = "write type-widening Delta table"
    spark.sql("DROP TABLE IF EXISTS dbo.fabricqueryr_type_widened")
    spark.sql(
        """
        CREATE TABLE dbo.fabricqueryr_type_widened (
          id TINYINT,
          label STRING
        )
        USING DELTA
        TBLPROPERTIES ('delta.enableTypeWidening' = 'true')
        """
    )
    spark.sql(
        """
        INSERT INTO dbo.fabricqueryr_type_widened
        VALUES (1, 'before'), (127, 'tinyint-limit')
        """
    )
    spark.sql(
        """
        ALTER TABLE dbo.fabricqueryr_type_widened
        ALTER COLUMN id TYPE SMALLINT
        """
    )
    spark.sql(
        """
        INSERT INTO dbo.fabricqueryr_type_widened
        VALUES (128, 'after')
        """
    )

    stage = "write exact type-widening Delta table"
    spark.sql("DROP TABLE IF EXISTS dbo.fabricqueryr_type_widened_exact")
    spark.sql(
        """
        CREATE TABLE dbo.fabricqueryr_type_widened_exact (
          id INT,
          amount DECIMAL(10, 2),
          occurred DATE,
          label STRING
        )
        USING DELTA
        TBLPROPERTIES (
          'delta.enableTypeWidening' = 'true',
          'delta.feature.timestampNtz' = 'supported'
        )
        """
    )
    spark.sql(
        """
        INSERT INTO dbo.fabricqueryr_type_widened_exact
        VALUES (1, 12.34, DATE '2026-01-01', 'before')
        """
    )
    spark.sql(
        """
        ALTER TABLE dbo.fabricqueryr_type_widened_exact
        ALTER COLUMN id TYPE BIGINT
        """
    )
    spark.sql(
        """
        ALTER TABLE dbo.fabricqueryr_type_widened_exact
        ALTER COLUMN amount TYPE DECIMAL(14, 4)
        """
    )
    spark.sql(
        """
        ALTER TABLE dbo.fabricqueryr_type_widened_exact
        ALTER COLUMN occurred TYPE TIMESTAMP_NTZ
        """
    )
    spark.sql(
        """
        INSERT INTO dbo.fabricqueryr_type_widened_exact
        VALUES (
          CAST('9007199254740993' AS BIGINT),
          CAST('1234567890.1234' AS DECIMAL(14, 4)),
          CAST('2026-07-28 12:34:56.123456' AS TIMESTAMP_NTZ),
          'after'
        )
        """
    )

    stage = "write V2 checkpoint Delta table"
    spark.sql("DROP TABLE IF EXISTS dbo.fabricqueryr_v2_checkpoint")
    spark.sql(
        """
        CREATE TABLE dbo.fabricqueryr_v2_checkpoint (
          id BIGINT,
          batch INT
        )
        USING DELTA
        TBLPROPERTIES (
          'delta.checkpointPolicy' = 'v2',
          'delta.checkpointInterval' = '1'
        )
        """
    )
    for batch in range(4):
        (
            spark.range(batch * 250, (batch + 1) * 250)
            .repartition(2)
            .withColumn("batch", F.lit(batch))
            .write.format("delta")
            .mode("append")
            .saveAsTable("dbo.fabricqueryr_v2_checkpoint")
        )

    stage = "write shallow clone Delta table"
    (
        fixture.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", True)
        .saveAsTable("dbo.fabricqueryr_clone_source")
    )
    spark.sql("DROP TABLE IF EXISTS dbo.fabricqueryr_shallow_clone")
    spark.sql(
        """
        CREATE TABLE dbo.fabricqueryr_shallow_clone
        SHALLOW CLONE dbo.fabricqueryr_clone_source
        """
    )

    stage = "write Variant Delta table"
    spark.sql("DROP TABLE IF EXISTS dbo.fabricqueryr_variant")
    spark.sql(
        """
        CREATE TABLE dbo.fabricqueryr_variant (
          event_id BIGINT,
          data VARIANT
        )
        USING DELTA
        """
    )
    spark.sql(
        """
        INSERT INTO dbo.fabricqueryr_variant
        SELECT
          1,
          PARSE_JSON(
            '{"user_id":4471,"action":"checkout","items":[1,2,3]}'
          )
        """
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
