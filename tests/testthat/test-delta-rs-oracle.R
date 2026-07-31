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
  expect_s3_class(primitive$id, "integer64")
  expect_true("9007199254740993" %in% as.character(primitive$id))
  expect_true("12.3000" %in% primitive$amount)
  expect_s3_class(primitive$local_at, "fabric_delta_timestamp_ntz")
  expect_true(
    "2026-07-28 09:08:07.654321" %in% unclass(primitive$local_at)
  )

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

  sys <- reticulate::import("sys", convert = FALSE)
  loaded_modules <- reticulate::py_to_r(
    .delta_python$builtins$list(sys$modules$keys())
  )
  expect_false(any(startsWith(loaded_modules, "pyarrow")))
})
