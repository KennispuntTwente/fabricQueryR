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
