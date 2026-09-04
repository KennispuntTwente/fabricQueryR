# fabric_kql_ingest rejects shared keys for multiple sources

    Code
      fabric_kql_ingest("https://ingest-cluster.kusto.fabric.microsoft.com", table = "Raw",
        sources = c("https://example.test/a.parquet",
          "https://example.test/b.parquet"), database = "Telemetry", format = "parquet",
        ingest_if_not_exists = "batch-1", token = "test-token")
    Condition
      Error in `fabric_kql_ingest()`:
      ! Cannot safely apply shared idempotency keys to multiple sources
      x `sources` contains 2 files
      i Submit each source separately with its own stable key or omit `ingest_if_not_exists`

