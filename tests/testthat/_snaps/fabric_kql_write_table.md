# Eventhouse writer requires a Storage credential for fixed tokens

    Code
      fabric_kql_write_table("https://ingest-cluster.kusto.fabric.microsoft.com",
        "Raw", data.frame(id = 1L), database = "Telemetry", token = "kusto-token")
    Condition
      Error in `fabric_kql_write_table()`:
      ! OneLake staging requires an audience-aware token provider or a separate storage_token

# Eventhouse writer rejects unsafe multi-file idempotency

    Code
      fabric_kql_write_table("https://ingest-cluster.kusto.fabric.microsoft.com",
        "Raw", data.frame(id = 1:3), database = "Telemetry", ingest_if_not_exists = "batch-1",
        max_rows_per_file = 1, token = "test-token", storage_token = "storage-token")
    Condition
      Error in `fabric_kql_write_table()`:
      ! Cannot safely apply shared idempotency keys to multiple staged files
      x Staging produced 3 Parquet files
      i Stage exactly one file per write or omit `ingest_if_not_exists`

---

    Code
      fabric_kql_write_table("https://ingest-cluster.kusto.fabric.microsoft.com",
        "Raw", data.frame(id = 1:3), database = "Telemetry", ingest_if_not_exists = "batch-1",
        skip_batching = TRUE, max_rows_per_file = 1, token = "test-token",
        storage_token = "storage-token")
    Condition
      Error in `fabric_kql_write_table()`:
      ! Cannot safely apply shared idempotency keys to multiple staged files
      x Staging produced 3 Parquet files
      i Stage exactly one file per write or omit `ingest_if_not_exists`

