fabric_test_delta_runtime_python <- function(root) {
  relative <- if (.Platform$OS.type == "windows") {
    file.path(".venv", "Scripts", "python.exe")
  } else {
    file.path(".venv", "bin", "python")
  }
  python <- file.path(root, relative)
  # On Unix, the venv executable is commonly a symlink to the base Python.
  # Resolving that final component makes reticulate bypass the venv packages.
  file.path(
    normalizePath(dirname(python), winslash = "/", mustWork = TRUE),
    basename(python)
  )
}

test_that("the production reader consumes deterministic delta-rs fixtures", {
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

  capable_table <- .delta_python$deltalake$DeltaTable(
    file.path(directory, "deletion_vector_capable")
  )
  expect_true(
    "deletionvectors" %in%
      tolower(fabric_delta_reader_features(capable_table))
  )
  expect_gt(
    max(fabric_delta_active_file_rows(capable_table)),
    .fabric_delta_max_deletion_vector_rows
  )
  expect_length(fabric_delta_deletion_vector_rows(capable_table), 0L)
})

test_that("the production bridge preserves exact and nested values", {
  oracle <- fabric_test_require_delta_oracle()
  python <- fabric_test_delta_runtime_python(oracle$root)
  if (!reticulate::py_available(initialize = FALSE)) {
    Sys.setenv(RETICULATE_PYTHON = python)
  }

  directory <- tempfile("delta-rs-exact-fixtures-")
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
  primitive_table <- .delta_python$deltalake$DeltaTable(
    file.path(directory, "primitive")
  )
  expect_false(
    "deletionvectors" %in%
      tolower(fabric_delta_reader_features(primitive_table))
  )
  expect_equal(sum(fabric_delta_active_file_rows(primitive_table)), 5)
  expect_s3_class(primitive$id, "integer64")
  expect_true("9007199254740993" %in% as.character(primitive$id))
  expect_true("12.3000" %in% primitive$amount)
  expect_s3_class(primitive$local_at, "fabric_delta_timestamp_ntz")
  expect_true(
    "2026-07-28 09:08:07.654321" %in% unclass(primitive$local_at)
  )

  boundaries <- fabric_delta_read_uri(
    file.path(directory, "scalar_boundaries"),
    result = "tibble"
  )
  expect_type(boundaries$regular, "double")
  expect_identical(
    boundaries$regular,
    c(-2147483648, 2147483647, NA_real_)
  )
  expect_s3_class(boundaries$large, "fabric_delta_integer64")
  expect_identical(
    as.character(boundaries$large),
    c("-9223372036854775808", "9223372036854775807", NA)
  )
  expect_identical(boundaries$row_id, 1:3)
  expect_identical(boundaries$tiny, c(-128L, 127L, NA_integer_))
  expect_identical(boundaries$small, c(-32768L, 32767L, NA_integer_))
  expect_true(is.nan(boundaries$single[[1L]]))
  expect_identical(boundaries$single[[2L]], -Inf)
  expect_true(is.na(boundaries$single[[3L]]))
  expect_identical(boundaries$double[[1L]], Inf)
  expect_identical(boundaries$double[[2L]], 0)
  expect_true(is.nan(boundaries$double[[3L]]))
  expect_identical(
    boundaries$whole_decimal,
    c(
      "-99999999999999999999999999999999999999",
      "99999999999999999999999999999999999999",
      NA
    )
  )
  expect_identical(
    boundaries$scaled_decimal,
    c(
      "-99999999999999999999.999999999999999999",
      "99999999999999999999.999999999999999999",
      NA
    )
  )
  expect_identical(boundaries$text, c("", "café-数据-🙂", NA))
  expect_identical(boundaries$payload[[1L]], raw())
  expect_identical(boundaries$payload[[2L]], as.raw(c(0L, 255L)))
  expect_null(boundaries$payload[[3L]])
  expect_identical(
    boundaries$event_date,
    as.Date(c("1900-01-01", "2038-01-19", NA))
  )
  expect_s3_class(boundaries$observed_at, "POSIXct")
  expected_observed_at <- as.POSIXct(
    c("1900-01-01 00:00:00", "2038-01-19 03:14:07", NA),
    tz = "UTC"
  )
  expected_observed_at[1L] <- expected_observed_at[1L] + 0.000001
  expected_observed_at[2L] <- expected_observed_at[2L] + 0.999999
  expect_equal(boundaries$observed_at, expected_observed_at, tolerance = 1e-6)
  expect_identical(
    unclass(boundaries$local_at),
    c(
      "1900-01-01 00:00:00.000001",
      "2038-01-19 03:14:07.999999",
      NA
    )
  )
  expect_identical(boundaries$active, c(FALSE, TRUE, NA))

  nested <- fabric_delta_read_uri(
    file.path(directory, "nested"),
    result = "tibble"
  )
  expect_identical(
    nested$profile$amount[[1L]],
    "123456789012345678.90"
  )
  expect_s3_class(nested$counts[[1L]]$value, "integer64")
  expect_s3_class(nested$items[[1L]]$code, "integer64")
  expect_identical(
    nested$scores[[1L]],
    c(-2147483648, 2147483647)
  )
  expect_s3_class(nested$longs[[1L]], "fabric_delta_integer64")
  expect_identical(
    as.character(nested$longs[[1L]]),
    c("-9223372036854775808", "9223372036854775807")
  )
  expect_s3_class(nested$profile, "fabric_delta_struct_column")
  expect_identical(is.na(nested$profile), c(FALSE, TRUE, FALSE))
  expect_identical(
    is.na(nested$profile[c(3L, 2L), , drop = FALSE]),
    c(FALSE, TRUE)
  )
  expect_s3_class(nested$items[[1L]], "fabric_delta_struct_column")
  expect_identical(
    is.na(nested$items[[1L]]),
    c(FALSE, FALSE, TRUE, FALSE)
  )
  expect_s3_class(
    nested$attributes[[1L]]$value,
    "fabric_delta_struct_column"
  )
  expect_identical(
    is.na(nested$attributes[[1L]]$value),
    c(FALSE, TRUE, FALSE)
  )
  expect_s3_class(
    nested$keyed[[1L]]$key$nested,
    "fabric_delta_struct_column"
  )
  expect_identical(
    is.na(nested$keyed[[1L]]$key$nested),
    c(TRUE, FALSE)
  )

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
    list(table = "scalar_boundaries", rows = 3L),
    list(table = "deletion_vector_capable", rows = 65537L)
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
