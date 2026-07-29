test_that("Delta reader defaults to delta-rs and dispatches explicitly", {
  calls <- character()
  context_engine <- NULL
  context <- list(
    cleanup = FALSE,
    dest_dir = tempfile("delta-dispatch-"),
    table_dir = "Tables/table"
  )
  local_mocked_bindings(
    fabric_delta_read_context = function(..., engine) {
      context_engine <<- engine
      context
    },
    fabric_onelake_read_delta_table_delta_rs = function(context) {
      calls <<- c(calls, "delta-rs")
      data.frame(id = 1L)
    },
    fabric_onelake_read_delta_table_r = function(context) {
      calls <<- c(calls, "R")
      data.frame(id = 2L)
    }
  )

  native <- fabric_onelake_read_delta_table(
    "table",
    "workspace",
    "lakehouse",
    verbose = FALSE
  )
  expect_identical(context_engine, "delta-rs")
  expect_identical(calls, "delta-rs")
  expect_identical(native$id, 1L)

  legacy <- fabric_onelake_read_delta_table(
    "table",
    "workspace",
    "lakehouse",
    engine = "R",
    verbose = FALSE
  )
  expect_identical(context_engine, "R")
  expect_identical(calls, c("delta-rs", "R"))
  expect_identical(legacy$id, 2L)

  expect_error(
    fabric_onelake_read_delta_table(
      "table",
      "workspace",
      "lakehouse",
      engine = "automatic",
      verbose = FALSE
    ),
    class = "rlang_error"
  )
  expect_error(
    fabric_onelake_read_delta_table(
      "table",
      "workspace",
      "lakehouse",
      result = "data.frame",
      verbose = FALSE
    ),
    class = "rlang_error"
  )
})

test_that("Delta result formatting supports tibbles and Arrow streams", {
  skip_if_not_installed("arrow")
  skip_if_not_installed("nanoarrow")
  values <- list(
    R = data.frame(id = 1:2, name = c("alpha", "beta")),
    `delta-rs` = arrow::Table$create(
      id = 1:2,
      name = c("alpha", "beta")
    )
  )

  for (engine in names(values)) {
    tibble <- fabric_delta_format_result(values[[engine]], "tibble")
    expect_s3_class(tibble, "tbl_df")
    expect_equal(tibble$id, 1:2, info = engine)

    stream <- fabric_delta_format_result(values[[engine]], "arrow_stream")
    expect_s3_class(stream, "nanoarrow_array_stream")
    materialized <- arrow::as_record_batch_reader(stream)$read_table()
    expect_equal(as.data.frame(materialized)$id, 1:2, info = engine)
  }
})

test_that("delta-rs backend passes OneLake options through the native bridge", {
  skip_if_not_installed("arrow")
  captured <- NULL
  work_dir <- tempfile("delta-rs-bridge-")
  dir.create(work_dir)
  on.exit(unlink(work_dir, recursive = TRUE, force = TRUE), add = TRUE)
  context <- list(
    target = list(
      dfs_base = "https://onelake.dfs.fabric.microsoft.com",
      workspace = "workspace id",
      item = "item id",
      path = "Tables/dbo/a table"
    ),
    credential = structure(list(), class = "test-credential"),
    version = 3,
    dest_dir = work_dir,
    verbose = FALSE,
    columns = c("name", "id"),
    limit = 2
  )
  local_mocked_bindings(
    fabric_get_token = function(credential, audience) {
      expect_identical(audience, .fabric_audience$storage)
      "secret-token"
    },
    fabric_delta_rs_read_to_ipc = function(
      uri,
      bearer_token,
      version,
      columns,
      limit,
      ipc_path
    ) {
      captured <<- as.list(environment())
      arrow::write_ipc_file(
        data.frame(name = c("alpha", "beta"), id = 1:2),
        ipc_path
      )
      list(version = version, rows = 2, path = ipc_path)
    }
  )

  result <- fabric_onelake_read_delta_table_delta_rs(context)

  expect_identical(
    captured$uri,
    paste0(
      "abfss://workspace%20id@onelake.dfs.fabric.microsoft.com/",
      "item%20id/Tables/dbo/a%20table"
    )
  )
  expect_identical(captured$bearer_token, "secret-token")
  expect_identical(captured$version, 3)
  expect_identical(captured$columns, c("name", "id"))
  expect_identical(captured$limit, 2)
  expect_true(inherits(result, "ArrowTabular"))
  materialized <- as.data.frame(result)
  expect_named(materialized, c("name", "id"))
  expect_equal(materialized$id, 1:2)
})

test_that("delta-rs backend exposes stable unsupported-feature errors", {
  skip_if_not_installed("arrow")
  work_dir <- tempfile("delta-rs-error-")
  dir.create(work_dir)
  on.exit(unlink(work_dir, recursive = TRUE, force = TRUE), add = TRUE)
  context <- list(
    target = list(
      dfs_base = "https://onelake.dfs.fabric.microsoft.com",
      workspace = "workspace",
      item = "item",
      path = "Tables/table"
    ),
    credential = list(),
    version = NULL,
    dest_dir = work_dir,
    verbose = FALSE,
    columns = NULL,
    limit = NULL
  )
  local_mocked_bindings(
    fabric_get_token = function(...) "secret-token",
    fabric_delta_rs_read_to_ipc = function(...) {
      stop("Unsupported table features: typeWidening")
    }
  )

  expect_error(
    fabric_onelake_read_delta_table_delta_rs(context),
    class = "fabric_delta_rs_unsupported_error"
  )
})

test_that("native delta-rs bridge reads a local Delta snapshot", {
  skip_if_not_installed("arrow")
  table_dir <- tempfile("delta-rs-local-")
  log_dir <- file.path(table_dir, "_delta_log")
  dir.create(log_dir, recursive = TRUE)
  on.exit(unlink(table_dir, recursive = TRUE, force = TRUE), add = TRUE)

  parquet_path <- file.path(table_dir, "part-00000.parquet")
  exact <- arrow::Array$create(
    c(123.45, -0.5, NA_real_),
    type = arrow::decimal128(8, 2)
  )
  arrow::write_parquet(
    arrow::Table$create(
      id = 1:3,
      name = c("alpha", "beta", "gamma"),
      exact = exact
    ),
    parquet_path
  )
  empty_object <- structure(list(), names = character())
  schema <- list(
    type = "struct",
    fields = list(
      list(
        name = "id",
        type = "integer",
        nullable = TRUE,
        metadata = empty_object
      ),
      list(
        name = "name",
        type = "string",
        nullable = TRUE,
        metadata = empty_object
      ),
      list(
        name = "exact",
        type = "decimal(8,2)",
        nullable = TRUE,
        metadata = empty_object
      )
    )
  )
  actions <- list(
    list(protocol = list(minReaderVersion = 1, minWriterVersion = 2)),
    list(
      metaData = list(
        id = "11111111-1111-1111-1111-111111111111",
        format = list(provider = "parquet", options = empty_object),
        schemaString = jsonlite::toJSON(schema, auto_unbox = TRUE),
        partitionColumns = list(),
        configuration = empty_object,
        createdTime = 0
      )
    ),
    list(
      add = list(
        path = basename(parquet_path),
        partitionValues = empty_object,
        size = unname(file.info(parquet_path)$size),
        modificationTime = 0,
        dataChange = TRUE
      )
    )
  )
  log_path <- file.path(log_dir, "00000000000000000000.json")
  writeLines(
    vapply(
      actions,
      jsonlite::toJSON,
      character(1),
      auto_unbox = TRUE,
      null = "null"
    ),
    log_path,
    useBytes = TRUE
  )
  ipc_path <- tempfile(fileext = ".arrow")
  on.exit(unlink(ipc_path, force = TRUE), add = TRUE)
  local_path <- normalizePath(table_dir, winslash = "/", mustWork = TRUE)
  uri <- paste0("file:///", sub("^/", "", local_path))

  metadata <- fabric_delta_rs_read_to_ipc(
    uri,
    "",
    -1,
    c("name", "id", "exact"),
    2,
    ipc_path
  )
  result <- arrow::read_ipc_file(ipc_path, as_data_frame = TRUE)

  expect_identical(metadata$version, 0)
  expect_identical(metadata$rows, 2)
  expect_named(result, c("name", "id", "exact"))
  expect_equal(nrow(result), 2L)
  expect_true(all(result$name %in% c("alpha", "beta", "gamma")))
  expect_type(result$exact, "character")
  expect_true(all(result$exact %in% c("123.45", "-0.50", NA_character_)))
  expect_error(
    fabric_delta_rs_read_to_ipc(uri, "", 1.5, character(), -1, ipc_path),
    "version must be -1 or a non-negative whole number",
    fixed = TRUE
  )
  expect_error(
    fabric_delta_rs_read_to_ipc(uri, "", -1, character(), -2, ipc_path),
    "limit must be -1 or a non-negative whole number",
    fixed = TRUE
  )
})
