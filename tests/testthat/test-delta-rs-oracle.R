fabric_test_delta_runtime_python <- function(root) {
  relative <- if (.Platform$OS.type == "windows") {
    file.path(".venv", "Scripts", "python.exe")
  } else {
    file.path(".venv", "bin", "python")
  }
  python <- file.path(root, relative)
  # On Unix, the venv executable is commonly a symlink to the base Python
  # Resolving that final component makes reticulate bypass the venv packages
  file.path(
    normalizePath(dirname(python), winslash = "/", mustWork = TRUE),
    basename(python)
  )
}

fabric_test_select_delta_runtime <- function() {
  oracle <- fabric_test_require_delta_oracle()
  python <- fabric_test_delta_runtime_python(oracle$root)
  expect_true(startsWith(python, paste0(oracle$root, "/.venv/")))
  if (reticulate::py_available(initialize = FALSE)) {
    selected <- normalizePath(
      reticulate::py_config()$python,
      winslash = "/",
      mustWork = TRUE
    )
    fabric_test_skip_or_fail(
      !identical(tolower(selected), tolower(python)),
      "reticulate was initialized with a different Python interpreter"
    )
  } else {
    Sys.setenv(RETICULATE_PYTHON = python)
  }
  oracle
}

test_that("fabric_delta_config reports the initialized locked runtime", {
  fabric_test_select_delta_runtime()

  config <- fabric_delta_config(initialize = TRUE)

  expect_true(config$initialized)
  expect_true(all(config$available))
  expect_identical(config$versions$deltalake, "1.6.2")
  expect_identical(config$versions$nanoarrow, "0.8.0")
  expect_match(config$python, "tools/fabric-sandbox/[.]venv", fixed = FALSE)
})

test_that("the production reader consumes deterministic delta-rs fixtures", {
  oracle <- fabric_test_select_delta_runtime()

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

  expect_identical(manifest$deltalake_version, "1.6.2")
  for (case in manifest$cases) {
    if (identical(case$table, "nested")) {
      next
    }
    columns <- unlist(case$columns %||% list(), use.names = FALSE)
    if (!length(columns)) {
      columns <- NULL
    }
    actual <- fabric_delta_read_uri(
      file.path(directory, case$table),
      version = case$version %||% NULL,
      columns = columns,
      limit = case$limit %||% NULL,
      result = "tibble"
    )
    expect_s3_class(actual, "tbl_df")
    expect_equal(
      nrow(actual),
      as.integer(case$expected_rows),
      info = case$name
    )
    expect_named(
      actual,
      unlist(case$expected_columns, use.names = FALSE),
      info = case$name
    )
  }

  deletion_vectors <- fabric_delta_read_uri(
    file.path(directory, "large_deletion_vector")
  )
  deletion_vector_ids <- as.character(deletion_vectors$id)
  expect_equal(nrow(deletion_vectors), 99997L)
  expect_identical(
    base::match(
      c("0", "65536", "99999"),
      deletion_vector_ids,
      nomatch = 0L
    ),
    rep(0L, 3L)
  )
  expect_true(
    all(
      base::match(
        c("1", "65537", "99998"),
        deletion_vector_ids,
        nomatch = 0L
      ) >
        0L
    )
  )

  row_tracking <- fabric_delta_read_uri(
    file.path(directory, "row_tracking_capable"),
    result = "tibble"
  )
  row_tracking <- row_tracking[order(row_tracking$id), ]
  expect_named(row_tracking, c("id", "label"))
  expect_identical(as.character(row_tracking$id), c("1", "2"))
  expect_identical(row_tracking$label, c("one", "two"))

  invalid_warehouse <- file.path(directory, "warehouse_invalid_columns")
  warehouse <- fabric_delta_read_uri(
    invalid_warehouse,
    result = "tibble"
  )
  expect_named(warehouse, c("id", "display name"))
  projected_warehouse <- fabric_delta_read_uri(
    invalid_warehouse,
    columns = "id",
    result = "tibble"
  )
  expect_identical(as.character(projected_warehouse$id), "1")
})

test_that("the production bridge has a narrow tibble contract", {
  oracle <- fabric_test_require_delta_oracle()
  python <- fabric_test_delta_runtime_python(oracle$root)
  if (!reticulate::py_available(initialize = FALSE)) {
    Sys.setenv(RETICULATE_PYTHON = python)
  }

  directory <- tempfile("delta-rs-scalar-fixtures-")
  dir.create(directory)
  on.exit(unlink(directory, recursive = TRUE, force = TRUE), add = TRUE)
  fabric_test_delta_oracle_run(c(
    "write-fixtures",
    "--directory",
    shQuote(directory)
  ))

  primitive <- fabric_delta_read_uri(
    file.path(directory, "primitive"),
    result = "tibble"
  )
  expect_type(primitive$id, "character")
  expect_true("9007199254740993" %in% primitive$id)
  expect_true("12.3000" %in% primitive$amount)
  expect_type(primitive$local_at, "character")
  expect_true("2026-07-28T09:08:07.654321" %in% primitive$local_at)

  boundaries <- fabric_delta_read_uri(
    file.path(directory, "scalar_boundaries"),
    result = "tibble"
  )
  expect_identical(
    boundaries$regular,
    c(-2147483648, 2147483647, NA_real_)
  )
  expect_identical(
    boundaries$large,
    c("-9223372036854775808", "9223372036854775807", NA)
  )
  expect_identical(
    boundaries$scaled_decimal,
    c(
      "-99999999999999999999.999999999999999999",
      "99999999999999999999.999999999999999999",
      NA
    )
  )
  expect_identical(
    boundaries$event_date,
    as.Date(c("1900-01-01", "2038-01-19", NA))
  )
  expect_s3_class(boundaries$observed_at, "POSIXct")
  expect_identical(
    boundaries$local_at,
    c(
      "1900-01-01T00:00:00.000001",
      "2038-01-19T03:14:07.999999",
      NA
    )
  )

  nested <- tryCatch(
    fabric_delta_read_uri(
      file.path(directory, "nested"),
      result = "tibble"
    ),
    error = identity
  )
  expect_s3_class(nested, "fabric_delta_nested_collection_error")
  expect_true(all(c("profile", "scores", "items") %in% nested$delta_columns))

  empty <- fabric_delta_read_uri(
    file.path(directory, "empty"),
    result = "tibble"
  )
  expect_equal(nrow(empty), 0L)
  expect_named(
    empty,
    c(
      "id",
      "name",
      "category",
      "amount",
      "active",
      "event_date",
      "observed_at",
      "local_at",
      "payload"
    )
  )
})


test_that("the production Arrow stream is lazy and R Arrow compatible", {
  oracle <- fabric_test_require_delta_oracle()
  python <- fabric_test_delta_runtime_python(oracle$root)
  if (!reticulate::py_available(initialize = FALSE)) {
    Sys.setenv(RETICULATE_PYTHON = python)
  }

  directory <- tempfile("delta-rs-stream-fixtures-")
  dir.create(directory)
  on.exit(unlink(directory, recursive = TRUE, force = TRUE), add = TRUE)
  fabric_test_delta_oracle_run(c(
    "write-fixtures",
    "--directory",
    shQuote(directory)
  ))

  stream <- fabric_delta_read_uri(
    file.path(directory, "primitive"),
    columns = c("id", "name", "amount", "local_at"),
    limit = 2,
    result = "arrow_stream"
  )
  expect_s3_class(stream, "nanoarrow_array_stream")
  expect_identical(
    attr(stream, "fabric_delta_snapshot_version", exact = TRUE),
    1
  )
  table <- arrow::as_record_batch_reader(stream)$read_table()
  expect_equal(table$num_rows, 2L)
  expect_named(
    as.data.frame(table),
    c("id", "name", "amount", "local_at")
  )
  expect_identical(table$schema$GetFieldByName("id")$type$ToString(), "int64")
  expect_identical(
    table$schema$GetFieldByName("amount")$type$ToString(),
    "string"
  )
  expect_match(
    table$schema$GetFieldByName("local_at")$type$ToString(),
    "timestamp\\[us\\]"
  )

  cases <- list(
    list(table = "primitive", version = 0, rows = 2L),
    list(table = "empty", rows = 0L),
    list(table = "schema_evolved", version = 0, rows = 2L),
    list(table = "schema_evolved", rows = 3L),
    list(table = "nested", rows = 3L),
    list(table = "scalar_boundaries", rows = 3L)
  )
  for (case in cases) {
    stream <- fabric_delta_read_uri(
      file.path(directory, case$table),
      version = case$version %||% NULL,
      result = "arrow_stream"
    )
    expect_s3_class(stream, "nanoarrow_array_stream")
    streamed <- arrow::as_record_batch_reader(stream)$read_table()
    expect_equal(streamed$num_rows, case$rows, label = case$table)
    expect_gt(streamed$num_columns, 0L, label = case$table)
    if ("id" %in% streamed$ColumnNames() && case$rows) {
      streamed_id <- as.data.frame(streamed["id"])$id
      expect_length(streamed_id, case$rows)
    }
  }

  sys <- reticulate::import("sys", convert = FALSE)
  loaded_modules <- reticulate::py_to_r(
    .delta_python$builtins$list(sys$modules$keys())
  )
  expect_false(any(startsWith(loaded_modules, "pyarrow")))
})
