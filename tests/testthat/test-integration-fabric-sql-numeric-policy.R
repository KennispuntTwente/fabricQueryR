# Fabric integration coverage: SQL numeric policies and precision warnings
# Exercise default and explicit policies through live Warehouse queries and
# table reads with ODBC and ADBC, including tibble and Arrow stream results.

test_that("SQL defaults work on Fabric with one ODBC precision warning", {
  manifest <- fabric_test_manifest()
  backends <- fabric_test_sql_backends()
  token <- fabric_test_token_provider()
  provisioned <- fabric_test_manifest_item(manifest, "TestWarehouse")
  warehouse <- fabric_item(
    manifest$workspace_id,
    provisioned$id,
    type = "Warehouse",
    token = token
  )
  withr::local_options(rlib_warning_verbosity = "default")
  warning_id <- "fabricQueryR.sql.odbc_precision"
  rlang::reset_warning_verbosity(warning_id)
  withr::defer(rlang::reset_warning_verbosity(warning_id))
  warnings <- list()
  sql <- paste(
    "SELECT CAST(1 AS int) AS id,",
    "CAST('12345678901234567890.1234' AS decimal(24,4)) AS amount,",
    "CAST('9007199254740993' AS bigint) AS large"
  )

  withCallingHandlers(
    {
      for (backend in backends) {
        for (shape in c("tibble", "arrow_stream")) {
          result <- warehouse$sql_query(
            sql,
            backend = backend,
            result = shape,
            verbose = FALSE
          )
          if (shape == "arrow_stream") {
            stream <- result
            result <- .fabric_arrow_exact_tibble(stream)
            nanoarrow::nanoarrow_pointer_release(stream)
          }
          expect_identical(as.integer(result$id), 1L, info = backend)
          expect_identical(as.character(result$large), "9007199254740993")
          if (backend == "adbc") {
            expect_identical(result$amount, "12345678901234567890.1234")
          } else {
            expect_equal(as.numeric(result$amount), 12345678901234567890)
          }
        }
        rows <- warehouse$read_table(
          provisioned$tables$types,
          columns = c("id", "amount"),
          limit = 1L,
          backend = backend,
          verbose = FALSE
        )
        expect_identical(nrow(rows), 1L, info = backend)
        expect_named(rows, c("id", "amount"))
      }
    },
    fabric_sql_precision_warning = function(cnd) {
      warnings[[length(warnings) + 1L]] <<- cnd
      rlang::cnd_muffle(cnd)
    }
  )
  expect_length(warnings, as.integer("odbc" %in% backends))

  if ("odbc" %in% backends) {
    rlang::reset_warning_verbosity(warning_id)
    accepted <- expect_silent(
      warehouse$sql_query(
        sql,
        numeric_policy = "driver",
        verbose = FALSE
      )
    )
    expect_identical(accepted$id, 1L)
    for (shape in c("tibble", "arrow_stream")) {
      error <- rlang::catch_cnd(warehouse$sql_query(
        sql,
        numeric_policy = "exact",
        result = shape,
        verbose = FALSE
      ))
      expect_s3_class(error, "fabric_sql_execution_error")
      expect_s3_class(error$parent, "fabric_sql_precision_error")
    }
  }
})
