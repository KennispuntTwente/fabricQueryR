from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("fabricqueryr-spark-job").getOrCreate()
row_count = spark.sql(
    "SELECT COUNT(*) FROM dbo.fabricqueryr_basic"
).first()[0]
marker = spark.createDataFrame(
    [("success", int(row_count))],
    "mode string, row_count long",
)
(
    marker.write.format("delta")
    .mode("overwrite")
    .saveAsTable("dbo.fabricqueryr_spark_job_result")
)
print(f"fabricqueryr-spark-job-success:{row_count}", flush=True)
