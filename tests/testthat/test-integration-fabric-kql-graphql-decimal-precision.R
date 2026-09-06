# Fabric integration coverage: KQL decimal ingestion and Parquet export
# The tests exercise exact decimal validation and service-selected conversion
# against the live Eventhouse fixture, including empty export results

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

test_that("KQL Parquet export protects decimals and supports an exact text projection", {
  manifest <- fabric_test_manifest()
  fabric_test_require_package("arrow")
  database <- fabric_test_manifest_item(manifest, "TestKQLDatabase")
  lakehouse <- fabric_test_manifest_item(manifest, "TestLakehouse")
  token <- fabric_test_token_provider()
  root <- paste0(
    "Files/fabricqueryr-decimal-export/",
    kusto_ingestion_source_id()
  )
  created <- FALSE
  withr::defer(
    if (created) {
      fabric_onelake_delete(
        manifest$workspace_id,
        lakehouse$id,
        root,
        recursive = TRUE,
        confirm = TRUE,
        token = token
      )
    }
  )
  query <- paste(
    "union (print id=1,value=decimal(1234567890123456789012345678)),",
    "(print id=2,value=decimal(0.0000000000000000001)),",
    "(print id=3,value=decimal(0.123456789012345678901234567890)),",
    "(print id=4,value=decimal(null))"
  )
  for (input in c(
    paste0(query, "; // trailing comment"),
    paste(query, "| take 0")
  )) {
    error <- rlang::catch_cnd(
      fabric_kql_export(
        database,
        input,
        lakehouse,
        path = paste0(root, "/refused"),
        token = token
      ),
      classes = "error"
    )
    expect_s3_class(error, "fabric_kql_export_decimal_error")
  }
  absent <- rlang::catch_cnd(
    fabric_onelake_list(
      manifest$workspace_id,
      lakehouse$id,
      root,
      token = token
    ),
    classes = "error"
  )
  expect_identical(absent$status, 404L)
  read_parts <- function(path) {
    files <- fabric_test_eventually(function() {
      value <- fabric_onelake_list(
        manifest$workspace_id,
        lakehouse$id,
        path,
        recursive = TRUE,
        token = token
      )
      paths <- value$path[
        !value$is_directory & grepl("[.]parquet$", value$path)
      ]
      if (length(paths)) paths else NULL
    })
    rows <- dplyr::bind_rows(lapply(files, function(file) {
      fabric_onelake_read_file(
        manifest$workspace_id,
        lakehouse$id,
        file,
        format = "parquet",
        token = token
      )
    }))
    rows[order(rows$id), , drop = FALSE]
  }
  expected_text <- c(
    "1234567890123456789012345678",
    "1E-19",
    "0.12345678901234567890123456789",
    ""
  )
  created <- TRUE
  exact <- fabric_kql_export(
    database,
    paste(
      query,
      "| project id, value_text=tostring(value), value_is_null=isnull(value)"
    ),
    lakehouse,
    path = paste0(root, "/text"),
    timeout = 600,
    token = token
  )
  rows <- read_parts(paste0(root, "/text"))
  expect_identical(exact$numeric_policy, "exact")
  expect_identical(rows$value_text, expected_text)
  expect_identical(rows$value_is_null, c(FALSE, FALSE, FALSE, TRUE))

  service <- fabric_kql_export(
    database,
    paste(
      query,
      "| project id, value, value_text=tostring(value), value_is_null=isnull(value)"
    ),
    lakehouse,
    path = paste0(root, "/service"),
    numeric_policy = "service",
    timeout = 600,
    token = token
  )
  rows <- read_parts(paste0(root, "/service"))
  expect_identical(service$state, "Completed")
  expect_identical(service$numeric_policy, "service")
  expect_identical(rows$value_text, expected_text)
  expect_identical(
    rows$value,
    c(
      "0.000000000000000000",
      "0.000000000000000000",
      "0.123456789012345678",
      NA_character_
    )
  )
})
