# Eventhouse writer rejects unsafe multi-file idempotency

    Code
      fabric_kql_write_table("https://ingest-cluster.kusto.fabric.microsoft.com",
        "Raw", data.frame(id = 1:3), database = "Telemetry", ingest_if_not_exists = "batch-1",
        skip_batching = TRUE, max_rows_per_file = 1, token = "test-token")
    Condition
      Error in `fabric_kql_write_table()`:
      ! Cannot safely apply one idempotency key to independently ingested files
      x `skip_batching` is `TRUE` and staging produced 3 Parquet files
      i Disable `skip_batching`, stage one file, or omit `ingest_if_not_exists`

