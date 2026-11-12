test_that("lightweight SQL records explain how to retrieve missing endpoints", {
  for (type in c(
    "Lakehouse", "Warehouse", "WarehouseSnapshot", "MirroredDatabase", "SQLDatabase"
  )) {
    item <- r6_test_record(type)
    expect_error(
      item$sql_connection_info(),
      "retrieve workload details",
      class = "fabric_sql_target_error"
    )
    expect_error(
      fabric_sql_connection_info(item$as_list()),
      "retrieve workload details",
      class = "fabric_sql_target_error"
    )
  }
})

test_that("lightweight KQL records explain which endpoint details are missing", {
  for (type in c("Eventhouse", "KQLDatabase")) {
    item <- r6_test_record(type)
    expect_error(
      item$query("print value = 1", database = "Telemetry"),
      "query-service URI.*retrieve workload details"
    )
    expect_error(
      item$ingest(
        "Events", sources = "https://storage.test/data.csv", format = "csv",
        database = "Telemetry"
      ),
      "ingestion-service URI.*retrieve workload details"
    )
    expect_error(
      kusto_resolve_target(item$as_list(), "Telemetry"),
      "query-service URI.*retrieve workload details"
    )
    expect_error(
      kusto_resolve_ingestion_target(item$as_list(), "Telemetry", "Events"),
      "ingestion-service URI.*retrieve workload details"
    )
  }
})
