# Fabric integration coverage: exact unsigned Parquet ingestion boundaries

test_that("KQL ingests unsigned int64 boundaries as exact decimals", {
  manifest <- fabric_test_manifest()
  fabric_test_require_package("arrow")
  database <- fabric_test_manifest_item(manifest, "TestKQLDatabase")
  token <- fabric_test_token_provider()
  target <- kusto_resolve_target(database)
  credential <- fabric_credential(token = token)
  table <- paste0("fabricqueryr_unsigned_", .fabric_lakehouse_staging_id())
  on.exit(
    try(
      kusto_export_management(
        target,
        paste(
          ".drop table",
          kusto_write_identifier(table, "table"),
          "ifexists"
        ),
        credential,
        deadline = Sys.time() + 60,
        idempotent = TRUE,
        operation = "UnsignedTestCleanup"
      ),
      silent = TRUE
    ),
    add = TRUE
  )
  expected <- c("9223372036854775808", "18446744073709551615", NA_character_)
  data <- arrow::Table$create(
    id = 1:3,
    value = arrow::Array$create(expected)$cast(arrow::uint64())
  )
  written <- fabric_kql_write_table(
    database,
    table,
    data,
    create_if_missing = TRUE,
    skip_batching = TRUE,
    timeout = 600,
    token = token
  )
  expect_identical(written$status$state, "Succeeded")
  rows <- fabric_kql_query(
    database,
    paste(
      kusto_write_identifier(table, "table"),
      "| order by id asc | project id, value=tostring(value), is_null=isnull(value)"
    ),
    token = token
  )
  expect_equal(as.integer(rows$id), 1:3)
  expect_identical(rows$value, c(expected[1:2], ""))
  expect_identical(rows$is_null, c(FALSE, FALSE, TRUE))
})
