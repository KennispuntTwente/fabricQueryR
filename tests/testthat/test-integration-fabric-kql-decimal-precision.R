test_that("KQL decimal ingestion rejects narrowing before creating a table", {
  manifest <- fabric_test_manifest()
  fabric_test_require_package("arrow")
  database <- fabric_test_manifest_item(manifest, "TestKQLDatabase")
  token <- fabric_test_token_provider()
  table <- paste0(
    "fabricqueryr_decimal_",
    gsub("-", "", kusto_ingestion_source_id())
  )
  target <- kusto_resolve_target(database)
  credential <- fabric_credential(token = token)
  withr::defer(kusto_export_management(
    target,
    paste(".drop table", kusto_write_identifier(table, "table"), "ifexists"),
    credential,
    deadline = Sys.time() + 60,
    idempotent = TRUE,
    operation = "DropDecimalPrecisionTest"
  ))
  safe_text <- "1234567890123456789.123456789012345"
  unsafe_text <- "12345678901234567890.123456789012345"
  make_data <- function(ids, values) {
    arrow::Table$create(
      id = ids,
      value = arrow::Array$create(values)$cast(arrow::decimal128(38, 15))
    )
  }
  expect_error(
    fabric_kql_write_table(
      database,
      table,
      make_data(1:3, c(safe_text, unsafe_text, NA_character_)),
      create_if_missing = TRUE,
      max_rows_per_file = 1,
      token = token
    ),
    class = "fabric_kql_decimal_precision_error"
  )
  tables <- fabric_kql_tables(database, token = token)
  expect_false(table %in% tables$name)

  written <- fabric_kql_write_table(
    database,
    table,
    make_data(1:2, c(safe_text, NA_character_)),
    create_if_missing = TRUE,
    max_rows_per_file = 1,
    skip_batching = TRUE,
    timeout = 600,
    token = token
  )
  expect_identical(written$status$state, "Succeeded")
  expect_identical(written$staging_retained, FALSE)
  rows <- fabric_test_eventually(function() {
    value <- fabric_kql_query(
      database,
      paste(
        table,
        "| project id, value, value_is_null=isnull(value) | order by id asc"
      ),
      token = token
    )
    if (nrow(value) == 2L) value else NULL
  })
  expect_identical(rows$value, c(safe_text, NA_character_))
  expect_identical(rows$value_is_null, c(FALSE, TRUE))

  service <- fabric_kql_write_table(
    database,
    table,
    make_data(3L, unsafe_text),
    numeric_policy = "service",
    skip_batching = TRUE,
    timeout = 600,
    token = token
  )
  expect_identical(service$status$state, "Succeeded")
  expect_identical(service$staging_retained, FALSE)
  # This intentionally delegates the conversion: do not equate a successful
  # ingestion with preservation of the original 35-digit value.
  rows <- fabric_test_eventually(function() {
    value <- fabric_kql_query(
      database,
      paste(table, "| where id == 3"),
      token = token
    )
    if (nrow(value) == 1L) value else NULL
  })
  expect_identical(rows$value, NA_character_)
})
