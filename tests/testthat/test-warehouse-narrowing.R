test_that("Warehouse rejects decimal and temporal narrowing before modifying tables", {
  skip_if_not_installed("arrow")
  cases <- list(
    list(arrow::decimal128(24, 4), "decimal", 24L, 2L),
    list(arrow::decimal128(24, 4), "decimal", 24L, 5L),
    list(arrow::decimal128(24, 4), "float", 53L, 0L),
    list(arrow::timestamp("us"), "datetime2", 23L, 3L),
    list(arrow::time64("us"), "time", 12L, 3L)
  )
  for (case in cases) {
    local_mocked_bindings(.fabric_warehouse_query = function(...) {
      data.frame(
        column_name = "value",
        type_name = case[[2L]],
        precision = case[[3L]],
        scale = case[[4L]]
      )
    })
    for (type in list(
      case[[1L]],
      arrow::dictionary(arrow::int32(), case[[1L]])
    )) {
      error <- rlang::catch_cnd(.fabric_warehouse_validate_destination_columns(
        NULL,
        "dbo",
        "target",
        "value",
        arrow::schema(value = type)
      ))
      expect_s3_class(error, "fabric_warehouse_column_error")
    }
  }
  local_mocked_bindings(.fabric_warehouse_query = function(...) {
    data.frame(
      column_name = c("value", "when"),
      type_name = c("decimal", "datetime2"),
      precision = c(26L, 27L),
      scale = c(6L, 6L)
    )
  })
  expect_no_error(.fabric_warehouse_validate_destination_columns(
    NULL,
    "dbo",
    "target",
    c("value", "when"),
    arrow::schema(
      value = arrow::decimal128(24, 4),
      when = arrow::timestamp("us")
    )
  ))
})

test_that("Warehouse append and overwrite reject narrowing before SQL mutation", {
  skip_if_not_installed("arrow")
  local_mocked_bindings(
    onelake_upload_target = function(...) NULL,
    .fabric_warehouse_connect = function(...) NULL,
    .fabric_warehouse_disconnect = function(...) TRUE,
    .fabric_warehouse_remove_staging = function(...) TRUE,
    .fabric_warehouse_query = function(...) {
      data.frame(
        column_name = "value",
        type_name = "decimal",
        precision = 24L,
        scale = 2L
      )
    },
    .fabric_warehouse_begin = function(...) stop("Unexpected BEGIN"),
    .fabric_warehouse_execute = function(...) stop("Unexpected SQL mutation")
  )
  value <- arrow::Array$create("1.2399")$cast(arrow::decimal128(24, 4))
  dictionary <- arrow::DictionaryArray$create(arrow::Array$create(0L), value)
  for (value in list(value, dictionary)) {
    data <- arrow::Table$create(value = value)
    for (mode in c("Append", "Overwrite")) {
      error <- expect_error(
        fabric_warehouse_write_table(
          warehouse_write_test_warehouse(),
          "narrowing",
          data,
          staging_lakehouse = warehouse_write_test_lakehouse(),
          mode = mode,
          token = "fabric",
          storage_token = "storage",
          sql_token = "sql",
          keep_staging_on_failure = FALSE,
          verbose = FALSE
        ),
        class = "fabric_warehouse_write_error"
      )
      expect_s3_class(error$parent, "fabric_warehouse_column_error")
    }
  }
})

test_that("Warehouse timestamp normalization retains input precision for validation", {
  skip_if_not_installed("arrow")
  data <- arrow::Table$create(
    value = arrow::Array$create(
      as.POSIXct("2026-01-02 03:04:05", tz = "UTC")
    )$cast(arrow::timestamp("ms"))
  )
  prepared <- .fabric_warehouse_prepare_data(data)
  local_mocked_bindings(.fabric_warehouse_query = function(...) {
    data.frame(
      column_name = "value",
      type_name = "datetime2",
      precision = 23L,
      scale = 3L
    )
  })
  expect_no_error(.fabric_warehouse_validate_destination_columns(
    NULL,
    "dbo",
    "target",
    "value",
    prepared$input_schema
  ))
  expect_error(
    .fabric_warehouse_validate_destination_columns(
      NULL,
      "dbo",
      "target",
      "value",
      prepared$schema
    ),
    class = "fabric_warehouse_column_error"
  )
})
