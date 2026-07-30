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

import json
import re
import traceback
import uuid

from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, MapType, StructField, StructType

workspace_id = "00000000-0000-0000-0000-000000000002"
lakehouse_id = "00000000-0000-0000-0000-000000000001"
spark.conf.set("spark.sql.session.timeZone", "UTC")
fixture_path = (
    f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/"
    f"{lakehouse_id}/Files/fixtures/basic.csv"
)

stage = "read uploaded CSV fixture"
try:
    stage = "verify and record Fabric Spark runtime"
    delta_class = spark._jvm.java.lang.Class.forName(
        "org.apache.spark.sql.delta.DeltaLog"
    )
    delta_version = delta_class.getPackage().getImplementationVersion()
    delta_source = str(
        delta_class.getProtectionDomain().getCodeSource().getLocation()
    )
    if not delta_version:
        delta_match = re.search(
            r"delta-(?:spark|core)_[^-/]+[-_](\d+\.\d+(?:\.\d+)?)",
            delta_source,
        )
        delta_version = delta_match.group(1) if delta_match else None
    if not spark.version.startswith("4.1."):
        raise RuntimeError(
            f"Expected Fabric Runtime 2.0 Spark 4.1, got {spark.version}"
        )
    if not delta_version or not delta_version.startswith("4.2."):
        raise RuntimeError(
            "Expected Fabric Runtime 2.0 Delta Lake 4.2, got "
            f"{delta_version!r} from {delta_source!r}"
        )
    (
        spark.createDataFrame(
            [("2.0", spark.version, delta_version)],
            ["fabric_runtime", "spark_version", "delta_version"],
        )
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", True)
        .saveAsTable("dbo.fabricqueryr_runtime")
    )

    stage = "read uploaded CSV fixture"
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
        .withColumn(
            "integer_part",
            F.when(
                F.col("id") == 3,
                F.lit(None).cast("integer"),
            ).otherwise((F.col("id") * 10).cast("integer")),
        )
        .withColumn(
            "decimal_part",
            F.when(
                F.col("id") == 1,
                F.lit("12.30").cast("decimal(8,2)"),
            )
            .when(
                F.col("id") == 2,
                F.lit("-0.50").cast("decimal(8,2)"),
            )
            .otherwise(F.lit(None).cast("decimal(8,2)")),
        )
        .withColumn(
            "timestamp_part",
            F.when(
                F.col("id") == 1,
                F.lit("2026-01-01 12:34:56.123456").cast("timestamp"),
            )
            .when(
                F.col("id") == 2,
                F.lit("1969-12-31 23:59:59.000001").cast("timestamp"),
            )
            .otherwise(F.lit(None).cast("timestamp")),
        )
        .withColumn(
            "timestamp_ntz_part",
            F.when(
                F.col("id") == 1,
                F.lit("2026-07-28 09:08:07.654321").cast("timestamp_ntz"),
            )
            .when(
                F.col("id") == 2,
                F.lit("1900-01-01 00:00:00.000001").cast("timestamp_ntz"),
            )
            .otherwise(F.lit(None).cast("timestamp_ntz")),
        )
        .withColumn(
            "binary_part",
            F.when(
                F.col("id") == 1,
                F.unhex(F.lit("010203")),
            )
            .when(
                F.col("id") == 2,
                F.unhex(F.lit("7f")),
            )
            .otherwise(F.lit(None).cast("binary")),
        )
    )
    (
        typed_partitions.write.format("delta")
        .mode("overwrite")
        .partitionBy(
            "event_date",
            "active",
            "integer_part",
            "decimal_part",
            "timestamp_part",
            "timestamp_ntz_part",
            "binary_part",
        )
        .option("overwriteSchema", True)
        .saveAsTable("dbo.fabricqueryr_typed_partitions")
    )

    stage = "write protocol-valid binary partition edge-case Delta table"
    binary_table_path = (
        f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/"
        f"{lakehouse_id}/Tables/dbo/fabricqueryr_binary_partitions"
    )
    binary_root = spark._jvm.org.apache.hadoop.fs.Path(binary_table_path)
    binary_fs = binary_root.getFileSystem(
        spark._jsc.hadoopConfiguration()
    )
    spark.sql("DROP TABLE IF EXISTS dbo.fabricqueryr_binary_partitions")
    binary_fs.delete(binary_root, True)

    # Spark's directory partition writer cannot safely materialize every byte
    # in a partition path. Delta does not require partition values in paths:
    # AddFile.partitionValues is authoritative, so use safe data-file paths.
    binary_partition_values = [
        (1, "\u0000"),
        (2, "\u0080"),
        (3, "\u00ff"),
        (4, None),
    ]
    binary_adds = []
    for row_id, binary_value in binary_partition_values:
        relative_dir = f"data/row-{row_id}"
        absolute_dir = f"{binary_table_path}/{relative_dir}"
        (
            spark.createDataFrame([(row_id,)], "id INT")
            .coalesce(1)
            .write.mode("error")
            .parquet(absolute_dir)
        )
        parquet_files = [
            status
            for status in binary_fs.listStatus(
                spark._jvm.org.apache.hadoop.fs.Path(absolute_dir)
            )
            if (
                status.isFile()
                and str(status.getPath().getName()).startswith("part-")
                and str(status.getPath().getName()).endswith(".parquet")
            )
        ]
        if len(parquet_files) != 1:
            raise RuntimeError(
                f"Expected one binary fixture Parquet file, got "
                f"{len(parquet_files)} for row {row_id}"
            )
        parquet_file = parquet_files[0]
        relative_file = (
            f"{relative_dir}/{str(parquet_file.getPath().getName())}"
        )
        binary_adds.append(
            {
                "add": {
                    "path": relative_file,
                    "partitionValues": {"binary_part": binary_value},
                    "size": parquet_file.getLen(),
                    "modificationTime": parquet_file.getModificationTime(),
                    "dataChange": True,
                    "stats": json.dumps(
                        {
                            "numRecords": 1,
                            "minValues": {"id": row_id},
                            "maxValues": {"id": row_id},
                            "nullCount": {"id": 0},
                        },
                        separators=(",", ":"),
                    ),
                }
            }
        )

    binary_schema = {
        "type": "struct",
        "fields": [
            {
                "name": "id",
                "type": "integer",
                "nullable": True,
                "metadata": {},
            },
            {
                "name": "binary_part",
                "type": "binary",
                "nullable": True,
                "metadata": {},
            },
        ],
    }
    binary_actions = [
        {"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}},
        {
            "metaData": {
                "id": str(uuid.uuid4()),
                "format": {"provider": "parquet", "options": {}},
                "schemaString": json.dumps(
                    binary_schema,
                    separators=(",", ":"),
                ),
                "partitionColumns": ["binary_part"],
                "configuration": {},
                "createdTime": (
                    spark._jvm.java.lang.System.currentTimeMillis()
                ),
            }
        },
        *binary_adds,
    ]
    binary_log_staging = f"{binary_table_path}/_fabricqueryr_log_staging"
    (
        spark.createDataFrame(
            [
                (
                    json.dumps(
                        action,
                        ensure_ascii=True,
                        separators=(",", ":"),
                    ),
                )
                for action in binary_actions
            ],
            "value STRING",
        )
        .coalesce(1)
        .write.mode("error")
        .text(binary_log_staging)
    )
    staged_logs = [
        status
        for status in binary_fs.listStatus(
            spark._jvm.org.apache.hadoop.fs.Path(binary_log_staging)
        )
        if (
            status.isFile()
            and str(status.getPath().getName()).startswith("part-")
        )
    ]
    if len(staged_logs) != 1:
        raise RuntimeError(
            f"Expected one staged Delta log, got {len(staged_logs)}"
        )
    binary_log_dir = spark._jvm.org.apache.hadoop.fs.Path(
        f"{binary_table_path}/_delta_log"
    )
    binary_fs.mkdirs(binary_log_dir)
    binary_log = spark._jvm.org.apache.hadoop.fs.Path(
        f"{binary_table_path}/_delta_log/00000000000000000000.json"
    )
    if not binary_fs.rename(staged_logs[0].getPath(), binary_log):
        raise RuntimeError("Could not publish binary fixture Delta log")
    binary_fs.delete(
        spark._jvm.org.apache.hadoop.fs.Path(binary_log_staging),
        True,
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
          array(
            named_struct('label', 'first', 'score', 10),
            named_struct('label', 'second', 'score', 20)
          ) AS items,
          map(
            'primary',
            named_struct('label', 'mapped', 'enabled', true)
          ) AS attributes,
          'display café-数据' AS `display name`
        """
    )

    stage = "write delta-rs compatible parity tables"
    spark.sql("DROP TABLE IF EXISTS dbo.fabricqueryr_oracle_basic")
    (
        fixture.withColumn(
            "loaded_at", F.lit("2026-01-01T00:00:00Z").cast("timestamp")
        )
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", True)
        .option("delta.enableDeletionVectors", "false")
        .saveAsTable("dbo.fabricqueryr_oracle_basic")
    )

    spark.sql("DROP TABLE IF EXISTS dbo.fabricqueryr_oracle_empty")
    (
        fixture.limit(0)
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", True)
        .option("delta.enableDeletionVectors", "false")
        .saveAsTable("dbo.fabricqueryr_oracle_empty")
    )

    spark.sql("DROP TABLE IF EXISTS dbo.fabricqueryr_oracle_typed_partitions")
    oracle_typed_partitions = typed_partitions.withColumn(
        "decimal_part",
        F.when(
            F.col("id") == 2,
            F.lit("0.50").cast("decimal(8,2)"),
        ).otherwise(F.col("decimal_part")),
    )
    (
        oracle_typed_partitions.write.format("delta")
        .mode("overwrite")
        .partitionBy(
            "event_date",
            "active",
            "integer_part",
            "decimal_part",
            "timestamp_part",
            "timestamp_ntz_part",
            "binary_part",
        )
        .option("overwriteSchema", True)
        .option("delta.enableDeletionVectors", "false")
        .saveAsTable("dbo.fabricqueryr_oracle_typed_partitions")
    )

    spark.sql("DROP TABLE IF EXISTS dbo.fabricqueryr_oracle_partitioned")
    (
        fixture.write.format("delta")
        .mode("overwrite")
        .partitionBy("category")
        .option("overwriteSchema", True)
        .option("delta.enableDeletionVectors", "false")
        .saveAsTable("dbo.fabricqueryr_oracle_partitioned")
    )
    for _ in range(10):
        fixture.limit(1).write.format("delta").mode("append").saveAsTable(
            "dbo.fabricqueryr_oracle_partitioned"
        )
    (
        replacement.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", "category = 'B'")
        .saveAsTable("dbo.fabricqueryr_oracle_partitioned")
    )

    spark.sql("DROP TABLE IF EXISTS dbo.fabricqueryr_oracle_schema_evolved")
    (
        fixture.filter(F.col("id") < 3)
        .select("id", "name")
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", True)
        .option("delta.enableDeletionVectors", "false")
        .saveAsTable("dbo.fabricqueryr_oracle_schema_evolved")
    )
    (
        fixture.filter(F.col("id") == 3)
        .select("id", "name")
        .withColumn("evolved_value", F.lit("introduced"))
        .write.format("delta")
        .mode("append")
        .option("mergeSchema", True)
        .saveAsTable("dbo.fabricqueryr_oracle_schema_evolved")
    )

    spark.sql("DROP TABLE IF EXISTS dbo.fabricqueryr_oracle_exact_types")
    spark.sql(
        """
        CREATE TABLE dbo.fabricqueryr_oracle_exact_types
        USING DELTA
        TBLPROPERTIES ('delta.enableDeletionVectors' = 'false')
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

    spark.sql("DROP TABLE IF EXISTS dbo.fabricqueryr_oracle_complex_types")
    spark.sql(
        """
        CREATE TABLE dbo.fabricqueryr_oracle_complex_types
        USING DELTA
        TBLPROPERTIES ('delta.enableDeletionVectors' = 'false')
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
          array(
            named_struct('label', 'first', 'score', 10),
            named_struct('label', 'second', 'score', 20)
          ) AS items,
          map(
            'primary',
            named_struct('label', 'mapped', 'enabled', true)
          ) AS attributes,
          'display café-数据' AS display_name
        """
    )

    stage = "write column-mapping Delta table"
    spark.sql("DROP TABLE IF EXISTS dbo.fabricqueryr_column_mapped")
    (
        fixture.filter(F.col("id") < 3)
        .select("id", "name", "amount")
        .withColumn(
            "profile",
            F.struct(
                F.col("name").alias("label"),
                F.col("amount").alias("obsolete"),
            ),
        )
        .withColumn(
            "items",
            F.array(
                F.struct(
                    F.col("name").alias("label"),
                    F.col("id").cast("integer").alias("rank"),
                )
            ),
        )
        .withColumn(
            "attributes",
            F.create_map(
                F.lit("primary"),
                F.struct(
                    F.col("name").alias("label"),
                    F.col("id").cast("integer").alias("rank"),
                ),
            ),
        )
        .write.format("delta")
        .mode("overwrite")
        .option("delta.columnMapping.mode", "name")
        .saveAsTable("dbo.fabricqueryr_column_mapped")
    )
    spark.sql(
        """
        ALTER TABLE dbo.fabricqueryr_column_mapped
        ADD COLUMNS (profile.metadata_only STRING)
        """
    )

    stage = "write struct parent-validity Delta table"
    spark.sql("DROP TABLE IF EXISTS dbo.fabricqueryr_struct_validity")
    spark.sql(
        """
        CREATE TABLE dbo.fabricqueryr_struct_validity
        USING DELTA
        AS SELECT
          1 AS id,
          CAST(NULL AS STRUCT<number: INT, text: STRING>) AS profile,
          array(
            CAST(NULL AS STRUCT<number: INT, text: STRING>),
            named_struct(
              'number', CAST(NULL AS INT),
              'text', CAST(NULL AS STRING)
            )
          ) AS items,
          map(
            'null', CAST(NULL AS STRUCT<number: INT, text: STRING>),
            'present', named_struct(
              'number', CAST(NULL AS INT),
              'text', CAST(NULL AS STRING)
            )
          ) AS attributes,
          map(
            named_struct(
              'marker', 1,
              'nested', CAST(NULL AS STRUCT<number: INT, text: STRING>)
            ), 'null',
            named_struct(
              'marker', 2,
              'nested', named_struct(
                'number', CAST(NULL AS INT),
                'text', CAST(NULL AS STRING)
              )
            ), 'present'
          ) AS keyed
        UNION ALL
        SELECT
          2,
          named_struct(
            'number', CAST(NULL AS INT),
            'text', CAST(NULL AS STRING)
          ),
          CAST(NULL AS ARRAY<STRUCT<number: INT, text: STRING>>),
          CAST(NULL AS MAP<STRING, STRUCT<number: INT, text: STRING>>),
          CAST(
            NULL AS MAP<
              STRUCT<
                marker: INT,
                nested: STRUCT<number: INT, text: STRING>
              >,
              STRING
            >
          )
        UNION ALL
        SELECT
          3,
          named_struct('number', 3, 'text', 'present'),
          array(),
          map(),
          map()
        """
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
    spark.sql(
        """
        ALTER TABLE dbo.fabricqueryr_column_mapped
        RENAME COLUMN profile.label TO display_label
        """
    )
    spark.sql(
        """
        ALTER TABLE dbo.fabricqueryr_column_mapped
        DROP COLUMN profile.obsolete
        """
    )
    (
        fixture.filter(F.col("id") == 3)
        .select("id", F.col("name").alias("display_name"))
        .withColumn(
            "profile",
            F.struct(F.col("display_name").alias("display_label")),
        )
        .withColumn(
            "items",
            F.array(
                F.struct(
                    F.col("display_name").alias("label"),
                    F.col("id").cast("integer").alias("rank"),
                )
            ),
        )
        .withColumn(
            "attributes",
            F.create_map(
                F.lit("primary"),
                F.struct(
                    F.col("display_name").alias("label"),
                    F.col("id").cast("integer").alias("rank"),
                ),
            ),
        )
        .write.format("delta")
        .mode("append")
        .saveAsTable("dbo.fabricqueryr_column_mapped")
    )

    stage = "write ID column-mapping Delta table"
    spark.sql("DROP TABLE IF EXISTS dbo.fabricqueryr_column_mapped_id")
    (
        fixture.filter(F.col("id") < 3)
        .select("id", "name")
        .withColumn(
            "profile",
            F.struct(F.col("name").alias("label")),
        )
        .withColumn(
            "items",
            F.array(
                F.struct(
                    F.col("name").alias("label"),
                    F.col("id").cast("integer").alias("rank"),
                )
            ),
        )
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
    spark.sql(
        """
        ALTER TABLE dbo.fabricqueryr_column_mapped_id
        RENAME COLUMN profile.label TO display_label
        """
    )
    (
        fixture.filter(F.col("id") == 3)
        .select("id", F.col("name").alias("display_name"))
        .withColumn(
            "profile",
            F.struct(F.col("display_name").alias("display_label")),
        )
        .withColumn(
            "items",
            F.array(
                F.struct(
                    F.col("display_name").alias("label"),
                    F.col("id").cast("integer").alias("rank"),
                )
            ),
        )
        .write.format("delta")
        .mode("append")
        .saveAsTable("dbo.fabricqueryr_column_mapped_id")
    )

    stage = "write partitioned ID-mapped deletion-vector Delta table"
    spark.sql(
        "DROP TABLE IF EXISTS dbo.fabricqueryr_column_mapped_id_partitioned_dv"
    )
    spark.sql(
        """
        CREATE TABLE dbo.fabricqueryr_column_mapped_id_partitioned_dv (
          id INT,
          display_name STRING,
          profile STRUCT<label: STRING>,
          category STRING
        )
        USING DELTA
        PARTITIONED BY (category)
        TBLPROPERTIES (
          'delta.columnMapping.mode' = 'id',
          'delta.enableDeletionVectors' = 'true'
        )
        """
    )
    spark.sql(
        """
        INSERT INTO dbo.fabricqueryr_column_mapped_id_partitioned_dv
        SELECT
          CAST(id AS INT),
          concat('row-', id),
          named_struct('label', concat('profile-', id)),
          CASE WHEN id % 2 = 0 THEN 'even' ELSE 'odd' END
        FROM range(0, 1000)
        """
    )
    spark.sql(
        """
        ALTER TABLE dbo.fabricqueryr_column_mapped_id_partitioned_dv
        RENAME COLUMN display_name TO name
        """
    )
    spark.sql(
        """
        ALTER TABLE dbo.fabricqueryr_column_mapped_id_partitioned_dv
        RENAME COLUMN profile.label TO display_label
        """
    )
    spark.sql(
        """
        DELETE FROM dbo.fabricqueryr_column_mapped_id_partitioned_dv
        WHERE id % 10 = 0
        """
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

    stage = "write deletion-vector file-row-number collision table"
    spark.sql(
        "DROP TABLE IF EXISTS dbo.fabricqueryr_file_row_number_collision"
    )
    spark.sql(
        """
        CREATE TABLE dbo.fabricqueryr_file_row_number_collision
        USING DELTA
        TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')
        AS SELECT
          CAST(id AS INT) AS id,
          CAST(id + 1000 AS BIGINT) AS file_row_number
        FROM range(0, 5000)
        """
    )
    spark.sql(
        """
        DELETE FROM dbo.fabricqueryr_file_row_number_collision
        WHERE id IN (3, 2047, 2048, 4095, 4096, 4999)
        """
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

    stage = "write dense deletion-vector Delta table"
    (
        spark.range(0, 100000)
        .coalesce(1)
        .write.format("delta")
        .mode("overwrite")
        .option("delta.enableDeletionVectors", "true")
        .option("maxRecordsPerFile", 100000)
        .saveAsTable("dbo.fabricqueryr_deletion_vectors_dense")
    )
    spark.sql(
        """
        DELETE FROM dbo.fabricqueryr_deletion_vectors_dense
        WHERE (id < 10000 AND id % 2 = 0)
           OR (id >= 70000 AND id < 80000)
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

    stage = "write nested type-widening Delta table"
    spark.sql("DROP TABLE IF EXISTS dbo.fabricqueryr_type_widened_nested")
    spark.sql(
        """
        CREATE TABLE dbo.fabricqueryr_type_widened_nested (
          id INT,
          nested STRUCT<count: INT, ratio: FLOAT>,
          readings ARRAY<INT>,
          lookup MAP<STRING, INT>,
          decimal_value INT,
          double_value SMALLINT
        )
        USING DELTA
        TBLPROPERTIES ('delta.enableTypeWidening' = 'true')
        """
    )
    spark.sql(
        """
        INSERT INTO dbo.fabricqueryr_type_widened_nested
        SELECT
          1,
          named_struct(
            'count', CAST(10 AS INT),
            'ratio', CAST(1.5 AS FLOAT)
          ),
          array(CAST(1 AS INT), CAST(2 AS INT)),
          map('before', CAST(3 AS INT)),
          CAST(123 AS INT),
          CAST(7 AS SMALLINT)
        """
    )
    wide_nested = spark.sql(
        """
        SELECT
          2 AS id,
          named_struct(
            'count', CAST('9007199254740993' AS BIGINT),
            'ratio', CAST(2.5 AS DOUBLE)
          ) AS nested,
          array(
            CAST('9007199254740993' AS BIGINT),
            CAST('4' AS BIGINT)
          ) AS readings,
          map('after', CAST('9007199254740993' AS BIGINT)) AS lookup,
          CAST(456 AS INT) AS decimal_value,
          CAST(8 AS SMALLINT) AS double_value
        """
    )
    (
        wide_nested.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable("dbo.fabricqueryr_type_widened_nested")
    )
    spark.sql(
        """
        ALTER TABLE dbo.fabricqueryr_type_widened_nested
        ALTER COLUMN decimal_value TYPE DECIMAL(12, 2)
        """
    )
    spark.sql(
        """
        ALTER TABLE dbo.fabricqueryr_type_widened_nested
        ALTER COLUMN double_value TYPE DOUBLE
        """
    )
    spark.sql(
        """
        INSERT INTO dbo.fabricqueryr_type_widened_nested
        SELECT
          3,
          named_struct(
            'count', CAST(11 AS BIGINT),
            'ratio', CAST(3.5 AS DOUBLE)
          ),
          array(CAST(5 AS BIGINT), CAST(6 AS BIGINT)),
          map('manual', CAST(7 AS BIGINT)),
          CAST('9007199254.25' AS DECIMAL(12, 2)),
          CAST(9.5 AS DOUBLE)
        """
    )

    stage = "write map-key type-widening Delta table"
    spark.sql("DROP TABLE IF EXISTS dbo.fabricqueryr_type_widened_map_key")
    spark.sql(
        """
        CREATE TABLE dbo.fabricqueryr_type_widened_map_key (
          id INT,
          keyed MAP<FLOAT, STRING>
        )
        USING DELTA
        TBLPROPERTIES ('delta.enableTypeWidening' = 'true')
        """
    )
    spark.sql(
        """
        INSERT INTO dbo.fabricqueryr_type_widened_map_key
        SELECT 1, map(CAST(1.25 AS FLOAT), 'before')
        """
    )
    widened_map_key = spark.sql(
        """
        SELECT
          2 AS id,
          map(CAST(16777217.5 AS DOUBLE), 'after') AS keyed
        """
    )
    (
        widened_map_key.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable("dbo.fabricqueryr_type_widened_map_key")
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
        TBLPROPERTIES ('delta.enableVariantShredding' = 'false')
        """
    )
    spark.sql(
        """
        INSERT INTO dbo.fabricqueryr_variant
        SELECT 1, PARSE_JSON(
          '{"user_id":4471,"action":"checkout","items":[1,2,3]}'
        )
        UNION ALL
        SELECT 2, CAST(NULL AS VARIANT)
        UNION ALL
        SELECT 3, PARSE_JSON('null')
        UNION ALL
        SELECT 4, PARSE_JSON('9007199254740993')
        """
    )
    spark.sql(
        """
        ALTER TABLE dbo.fabricqueryr_variant
        SET TBLPROPERTIES ('delta.enableVariantShredding' = 'true')
        """
    )
    spark.sql(
        """
        INSERT INTO dbo.fabricqueryr_variant
        SELECT 5, PARSE_JSON(
          '{"user_id":9007199254740993,"action":"refund","items":[]}'
        )
        UNION ALL
        SELECT 6, PARSE_JSON('[1,"two",true,null,{"nested":3}]')
        UNION ALL
        SELECT 7, PARSE_JSON('"root string"')
        UNION ALL
        SELECT 8,
          PARSE_JSON(
            '{"unicode":"café-数据-🙂","decimal":1234567890.125}'
          )
        UNION ALL
        SELECT 9,
          PARSE_JSON('123456789012345678901234567890123456.78')
        """
    )

    stage = "write ID-mapped Variant deletion-vector Delta table"
    spark.sql("DROP TABLE IF EXISTS dbo.fabricqueryr_variant_id_dv")
    spark.sql(
        """
        CREATE TABLE dbo.fabricqueryr_variant_id_dv (
          event_id BIGINT,
          data VARIANT
        )
        USING DELTA
        TBLPROPERTIES (
          'delta.columnMapping.mode' = 'id',
          'delta.enableDeletionVectors' = 'true',
          'delta.enableVariantShredding' = 'true'
        )
        """
    )
    spark.sql(
        """
        INSERT INTO dbo.fabricqueryr_variant_id_dv
        SELECT 1, PARSE_JSON('{"kind":"object","value":1}')
        UNION ALL SELECT 2, PARSE_JSON('{"deleted":true}')
        UNION ALL SELECT 3, PARSE_JSON('null')
        UNION ALL SELECT 4, PARSE_JSON('"mapped string"')
        """
    )
    spark.sql(
        """
        ALTER TABLE dbo.fabricqueryr_variant_id_dv
        RENAME COLUMN event_id TO id
        """
    )
    spark.sql(
        """
        ALTER TABLE dbo.fabricqueryr_variant_id_dv
        RENAME COLUMN data TO payload
        """
    )
    spark.sql(
        """
        INSERT INTO dbo.fabricqueryr_variant_id_dv
        SELECT
          5,
          PARSE_JSON('123456789012345678901234567890123456.78')
        """
    )
    spark.sql("DELETE FROM dbo.fabricqueryr_variant_id_dv WHERE id = 2")

    stage = "materialize independent Spark Delta reader oracles"

    def without_delta_metadata(data_type):
        if isinstance(data_type, StructType):
            return StructType(
                [
                    StructField(
                        field.name,
                        without_delta_metadata(field.dataType),
                        field.nullable,
                        {},
                    )
                    for field in data_type.fields
                ]
            )
        if isinstance(data_type, ArrayType):
            return ArrayType(
                without_delta_metadata(data_type.elementType),
                data_type.containsNull,
            )
        if isinstance(data_type, MapType):
            return MapType(
                without_delta_metadata(data_type.keyType),
                without_delta_metadata(data_type.valueType),
                data_type.valueContainsNull,
            )
        return data_type

    def write_reader_oracle(source, target, query=None):
        source_frame = spark.sql(query) if query else spark.table(source)
        clean_frame = spark.createDataFrame(
            source_frame.rdd,
            without_delta_metadata(source_frame.schema),
        )
        spark.sql(f"DROP TABLE IF EXISTS dbo.{target}")
        (
            clean_frame.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", True)
            .option("delta.enableDeletionVectors", "false")
            .option("delta.checkpointPolicy", "classic")
            .saveAsTable(f"dbo.{target}")
        )

    reader_oracles = {
        "fabricqueryr_column_mapped": "fabricqueryr_spark_oracle_column_mapped",
        "fabricqueryr_column_mapped_id": (
            "fabricqueryr_spark_oracle_column_mapped_id"
        ),
        "fabricqueryr_column_mapped_id_partitioned_dv": (
            "fabricqueryr_spark_oracle_column_mapped_id_partitioned_dv"
        ),
        "fabricqueryr_struct_validity": (
            "fabricqueryr_spark_oracle_struct_validity"
        ),
        "fabricqueryr_deletion_vectors": (
            "fabricqueryr_spark_oracle_deletion_vectors"
        ),
        "fabricqueryr_file_row_number_collision": (
            "fabricqueryr_spark_oracle_file_row_number_collision"
        ),
        "fabricqueryr_deletion_vectors_stress": (
            "fabricqueryr_spark_oracle_deletion_vectors_stress"
        ),
        "fabricqueryr_deletion_vectors_checkpoint": (
            "fabricqueryr_spark_oracle_deletion_vectors_checkpoint"
        ),
        "fabricqueryr_deletion_vectors_dense": (
            "fabricqueryr_spark_oracle_deletion_vectors_dense"
        ),
        "fabricqueryr_type_widened": (
            "fabricqueryr_spark_oracle_type_widened"
        ),
        "fabricqueryr_type_widened_exact": (
            "fabricqueryr_spark_oracle_type_widened_exact"
        ),
        "fabricqueryr_type_widened_nested": (
            "fabricqueryr_spark_oracle_type_widened_nested"
        ),
        "fabricqueryr_type_widened_map_key": (
            "fabricqueryr_spark_oracle_type_widened_map_key"
        ),
        "fabricqueryr_v2_checkpoint": (
            "fabricqueryr_spark_oracle_v2_checkpoint"
        ),
        "fabricqueryr_shallow_clone": (
            "fabricqueryr_spark_oracle_shallow_clone"
        ),
    }
    for source, target in reader_oracles.items():
        write_reader_oracle(f"dbo.{source}", target)
    write_reader_oracle(
        "dbo.fabricqueryr_variant",
        "fabricqueryr_spark_oracle_variant",
        """
        SELECT
          event_id,
          CAST(data AS STRING) AS data_display
        FROM dbo.fabricqueryr_variant
        """,
    )
    write_reader_oracle(
        "dbo.fabricqueryr_variant_id_dv",
        "fabricqueryr_spark_oracle_variant_id_dv",
        """
        SELECT
          id,
          CAST(payload AS STRING) AS payload_display
        FROM dbo.fabricqueryr_variant_id_dv
        """,
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
