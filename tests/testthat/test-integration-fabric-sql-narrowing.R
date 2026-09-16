# Fabric integration coverage: sql narrowing
test_that("live Warehouse rejects narrowing without changing existing rows", {
  manifest <- fabric_test_manifest()
  fabric_test_require_package("arrow")
  token <- fabric_test_token_provider()
  item <- function(name, type) {
    fabric_item(
      manifest$workspace_id,
      fabric_test_manifest_item(manifest, name)$id,
      type = type,
      token = token
    )
  }
  warehouse <- item("TestWarehouse", "Warehouse")
  lake <- item("TestLakehouse", "Lakehouse")
  con <- fabric_sql_connect(warehouse, token = token, verbose = FALSE)
  withr::defer(DBI::dbDisconnect(con))
  table <- paste0("fabricqueryr_narrowing_", Sys.getpid())
  sql <- paste0("[dbo].[", table, "]")
  withr::defer(DBI::dbExecute(con, paste("DROP TABLE IF EXISTS", sql)))
  cases <- list(
    list(
      "decimal(24,2)",
      "1.23",
      arrow::Array$create("1.2399")$cast(arrow::decimal128(24, 4))
    ),
    list(
      "datetime2(3)",
      "'2026-01-02T03:04:05.123'",
      arrow::Array$create(as.POSIXct("2026-01-02 03:04:05.123456", tz = "UTC"))
    )
  )
  cases <- c(
    cases,
    lapply(cases, function(case) {
      case[[3L]] <- arrow::DictionaryArray$create(
        arrow::Array$create(0L),
        case[[3L]]
      )
      case
    })
  )
  for (case in cases) {
    DBI::dbExecute(con, paste("CREATE TABLE", sql, "(value", case[[1L]], ")"))
    DBI::dbExecute(con, paste("INSERT INTO", sql, "VALUES (", case[[2L]], ")"))
    before <- DBI::dbGetQuery(con, paste("SELECT * FROM", sql))
    for (mode in c("Append", "Overwrite")) {
      error <- expect_error(
        fabric_warehouse_write_table(
          warehouse,
          table,
          arrow::Table$create(value = case[[3L]]),
          staging_lakehouse = lake,
          mode = mode,
          token = token,
          keep_staging_on_failure = FALSE,
          verbose = FALSE
        ),
        class = "fabric_warehouse_write_error"
      )
      expect_s3_class(error$parent, "fabric_warehouse_column_error")
      expect_equal(DBI::dbGetQuery(con, paste("SELECT * FROM", sql)), before)
    }
    if (case[[1L]] == "datetime2(3)") {
      safe <- arrow::Table$create(
        value = case[[3L]]$cast(arrow::timestamp("ms"), safe = FALSE)
      )
      expect_no_error(fabric_warehouse_write_table(
        warehouse,
        table,
        safe,
        staging_lakehouse = lake,
        mode = "Overwrite",
        token = token,
        verbose = FALSE
      ))
      expect_equal(DBI::dbGetQuery(con, paste("SELECT * FROM", sql)), before)
    }
    DBI::dbExecute(con, paste("DROP TABLE", sql))
  }
})

test_that("Warehouse CTAS and COPY decode dictionary values without changing types", {
  manifest <- fabric_test_manifest()
  fabric_test_require_package("arrow")
  token <- fabric_test_token_provider()
  warehouse <- fabric_item(
    manifest$workspace_id,
    fabric_test_manifest_item(manifest, "TestWarehouse")$id,
    token = token
  )
  lake <- fabric_item(
    manifest$workspace_id,
    fabric_test_manifest_item(manifest, "TestLakehouse")$id,
    token = token
  )
  con <- fabric_sql_connect(warehouse, token = token, verbose = FALSE)
  withr::defer(DBI::dbDisconnect(con))
  table <- paste0("fabricqueryr_dictionary_", Sys.getpid())
  sql <- paste0("[dbo].[", table, "]")
  withr::defer(DBI::dbExecute(con, paste("DROP TABLE IF EXISTS", sql)))
  values <- list(
    moment = arrow::Array$create("1788613200123456")$cast(arrow::int64())$cast(
      arrow::timestamp("us")
    ),
    amount = arrow::Array$create("123.45")$cast(arrow::decimal128(5, 2)),
    byte = arrow::Array$create(255L)$cast(arrow::uint8())
  )
  columns <- lapply(values, function(value) {
    arrow::DictionaryArray$create(
      arrow::Array$create(c(0L, NA_integer_)),
      value
    )
  })
  data <- do.call(arrow::Table$create, columns)
  for (iteration in seq_len(2L)) {
    written <- fabric_warehouse_write_table(
      warehouse,
      table,
      data,
      staging_lakehouse = lake,
      create_if_missing = iteration == 1L,
      mode = "Append",
      token = token,
      verbose = FALSE
    )
    expect_equal(written$rows, 2)
    rows <- DBI::dbGetQuery(
      con,
      paste0(
        "SELECT CONVERT(varchar(40), moment, 126) AS moment, ",
        "CONVERT(varchar(40), amount) AS amount, byte FROM ",
        sql
      )
    )
    expect_equal(nrow(rows), 2L * iteration)
    expect_identical(
      sort(rows$amount, na.last = TRUE),
      c(rep("123.45", iteration), rep(NA_character_, iteration))
    )
    expect_identical(unique(rows$byte[!is.na(rows$byte)]), 255L)
    expect_identical(
      all(grepl("123456$", rows$moment[!is.na(rows$moment)])),
      TRUE
    )
  }
  types <- DBI::dbGetQuery(
    con,
    paste0(
      "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS ",
      "WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = '",
      table,
      "'"
    )
  )
  expect_identical(types$DATA_TYPE[types$COLUMN_NAME == "moment"], "datetime2")
  expect_true(
    types$DATA_TYPE[types$COLUMN_NAME == "amount"] %in% c("decimal", "numeric")
  )
  expect_identical(types$DATA_TYPE[types$COLUMN_NAME == "byte"], "smallint")
})
