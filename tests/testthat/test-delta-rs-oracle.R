test_that("R Delta snapshots agree with deterministic delta-rs fixtures", {
  fabric_test_require_package("DBI")
  fabric_test_require_package("duckdb")
  fabric_test_require_package("fs")
  fabric_test_require_delta_oracle()

  directory <- tempfile("delta-rs-fixtures-")
  dir.create(directory)
  on.exit(unlink(directory, recursive = TRUE, force = TRUE), add = TRUE)
  fabric_test_delta_oracle_run(c(
    "write-fixtures",
    "--directory",
    shQuote(directory)
  ))
  manifest <- jsonlite::fromJSON(
    file.path(directory, "manifest.json"),
    simplifyVector = FALSE
  )

  expect_match(manifest$deltalake_version, "^1[.]6[.]")
  expect_length(manifest$cases, 6L)
  for (case in manifest$cases) {
    table <- file.path(directory, case$table)
    version <- case$version %||% NULL
    columns <- unlist(case$columns %||% list(), use.names = FALSE)
    if (!length(columns)) {
      columns <- NULL
    }
    limit <- case$limit %||% NULL
    actual <- fabric_delta_read_staged(
      table,
      version = version,
      columns = columns,
      limit = limit
    )
    oracle <- fabric_test_delta_oracle_read(
      table,
      version = version,
      columns = columns,
      limit = limit
    )
    fabric_test_expect_delta_oracle_equal(actual, oracle, case$name)
  }
})
