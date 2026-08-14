# Fabric integration coverage: schema-aware table metadata and managed
# CSV/Parquet loads through the Lakehouse preview APIs

fabric_test_use_table_delta_runtime <- function() {
  if (
    reticulate::py_available(initialize = FALSE) ||
      nzchar(Sys.getenv("RETICULATE_PYTHON"))
  ) {
    return(invisible(NULL))
  }
  root <- fabric_test_delta_oracle_root()
  relative <- if (.Platform$OS.type == "windows") {
    file.path(".venv", "Scripts", "python.exe")
  } else {
    file.path(".venv", "bin", "python")
  }
  python <- file.path(root, relative)
  fabric_test_skip_or_fail(
    !file.exists(python),
    "The locked delta-rs Python environment has not been installed"
  )
  Sys.setenv(
    RETICULATE_PYTHON = normalizePath(
      python,
      winslash = "/",
      mustWork = TRUE
    )
  )
  invisible(NULL)
}

fabric_test_lakehouse_table_target <- function(manifest, lakehouse) {
  structure(
    utils::modifyList(
      lakehouse,
      list(
        workspaceId = manifest$workspace_id,
        properties = list(defaultSchema = lakehouse$schema)
      )
    ),
    class = "fabric_item"
  )
}

test_that("Lakehouse tables list and load CSV and Parquet end to end", {
  fabric_test_require_package("arrow")
  fabric_test_require_package("DBI")
  fabric_test_require_package("odbc")
  manifest <- fabric_test_manifest()
  fabric_test_use_table_delta_runtime()
  lakehouse <- fabric_test_manifest_item(manifest, "TestLakehouse")
  target <- fabric_test_lakehouse_table_target(manifest, lakehouse)
  token <- fabric_test_token_provider()
  schema <- lakehouse$schema
  csv_table <- "fabricqueryr_csv_load"
  parquet_table <- "fabricqueryr_r_load"

  # Force multiple metadata pages and retain the schema record associated with
  # every table row
  initial <- fabric_lakehouse_tables(
    target,
    detail = FALSE,
    page_size = 1L,
    token = token
  )
  expect_s3_class(initial, "tbl_df")
  expect_true(all(nzchar(initial$name)))
  expect_true(all(nzchar(initial$schema)))
  if (nrow(initial)) {
    expect_true(all(vapply(
      seq_len(nrow(initial)),
      function(index) {
        identical(
          initial$schema_metadata[[index]]$name,
          initial$schema[[index]]
        )
      },
      logical(1)
    )))
  }

  # The staged CSV fixture covers both overwrite and append through the direct
  # file-loading API
  csv_overwrite <- fabric_lakehouse_load_table(
    target,
    table = csv_table,
    path = "Files/fixtures/basic.csv",
    format = "Csv",
    mode = "Overwrite",
    header = TRUE,
    delimiter = ",",
    token = token
  )
  csv_overwrite_state <- fabric_operation_wait(csv_overwrite, timeout = 900)
  expect_equal(csv_overwrite_state$status, "Succeeded")

  csv_append <- fabric_lakehouse_load_table(
    target,
    table = csv_table,
    path = "Files/fixtures/basic.csv",
    format = "Csv",
    mode = "Append",
    token = token
  )
  csv_append_state <- fabric_operation_wait(csv_append, timeout = 900)
  expect_equal(csv_append_state$status, "Succeeded")

  csv_rows <- fabric_test_eventually(function() {
    value <- fabric_onelake_read_delta_table(
      table_path = csv_table,
      workspace_name = manifest$workspace_id,
      lakehouse_name = lakehouse$id,
      schema = schema,
      token = token,
      verbose = FALSE
    )
    if (nrow(value) != 6L) {
      return(NULL)
    }
    value
  })
  expect_equal(as.integer(sort(csv_rows$id)), sort(rep(1:3, 2L)))
  expect_equal(sum(is.na(csv_rows$amount)), 2L)

  # The R workflow covers Unicode names, nulls, exact 64-bit integers, dates,
  # timestamps, overwrite, append, and confirmed staging cleanup
  first <- data.frame(
    id = 1:2,
    whole = bit64::as.integer64(c("9007199254740993", NA)),
    café_数据 = c("één", NA),
    amount = c(10.5, NA),
    active = c(TRUE, FALSE),
    event_date = as.Date(c("2026-01-01", "2026-01-02")),
    event_time = as.POSIXct(
      c("2026-01-01 10:00:00", "2026-01-02 11:30:00"),
      tz = "UTC"
    ),
    stringsAsFactors = FALSE
  )
  overwrite <- fabric_lakehouse_write_table(
    target,
    table = parquet_table,
    data = first,
    mode = "Overwrite",
    timeout = 900,
    token = token
  )
  expect_equal(overwrite$operation_status$status, "Succeeded")
  expect_false(overwrite$staging_retained)

  second <- data.frame(
    id = 3L,
    whole = bit64::as.integer64("9007199254740995"),
    café_数据 = "drie",
    amount = 30,
    active = TRUE,
    event_date = as.Date("2026-01-03"),
    event_time = as.POSIXct("2026-01-03 12:45:00", tz = "UTC"),
    stringsAsFactors = FALSE
  )
  append <- fabric_lakehouse_write_table(
    target,
    table = parquet_table,
    data = second,
    mode = "Append",
    timeout = 900,
    token = token
  )
  expect_equal(append$operation_status$status, "Succeeded")
  expect_false(append$staging_retained)

  delta_rows <- fabric_test_eventually(function() {
    value <- fabric_onelake_read_delta_table(
      table_path = parquet_table,
      workspace_name = manifest$workspace_id,
      lakehouse_name = lakehouse$id,
      schema = schema,
      token = token,
      verbose = FALSE
    )
    if (nrow(value) != 3L) {
      return(NULL)
    }
    value[order(value$id), ]
  })
  expect_named(delta_rows, names(first), ignore.order = TRUE)
  expect_equal(delta_rows$id, 1:3)
  expect_equal(
    as.character(delta_rows$whole),
    c("9007199254740993", NA, "9007199254740995")
  )
  expect_equal(delta_rows$café_数据, c("één", NA, "drie"))
  expect_equal(delta_rows$amount, c(10.5, NA, 30))
  expect_identical(delta_rows$active, c(TRUE, FALSE, TRUE))
  expect_equal(
    as.Date(delta_rows$event_date),
    as.Date(c(
      "2026-01-01",
      "2026-01-02",
      "2026-01-03"
    ))
  )
  expect_equal(
    as.numeric(delta_rows$event_time),
    as.numeric(c(first$event_time, second$event_time))
  )

  # The SQL analytics endpoint is eventually consistent with Lakehouse Delta
  # metadata, so retry only the read-only verification query
  sql_rows <- fabric_test_eventually(function() {
    value <- fabric_sql_query(
      server = lakehouse$sql_endpoint,
      database = lakehouse$display_name,
      sql = paste0(
        "SELECT COUNT_BIG(*) AS row_count, ",
        "SUM(CASE WHEN [café_数据] IS NULL THEN 1 ELSE 0 END) AS null_count, ",
        "CAST(MAX(whole) AS varchar(30)) AS max_whole ",
        "FROM [",
        schema,
        "].[",
        parquet_table,
        "]"
      ),
      token = token,
      verbose = FALSE
    )
    if (as.numeric(value$row_count[[1L]]) != 3) {
      return(NULL)
    }
    value
  })
  expect_equal(as.numeric(sql_rows$row_count), 3)
  expect_equal(as.numeric(sql_rows$null_count), 1)
  expect_equal(sql_rows$max_whole, "9007199254740995")

  discovered <- fabric_lakehouse_tables(
    target,
    schema = schema,
    detail = FALSE,
    page_size = 1L,
    token = token
  )
  expect_true(all(c(csv_table, parquet_table) %in% discovered$name))
  expect_true(all(
    toupper(discovered$format[
      discovered$name %in%
        c(
          csv_table,
          parquet_table
        )
    ]) ==
      "DELTA"
  ))
})

test_that("Lakehouse writer retains a recoverable staging path on failure", {
  fabric_test_require_package("arrow")
  manifest <- fabric_test_manifest()
  lakehouse <- fabric_test_manifest_item(manifest, "TestLakehouse")
  target <- fabric_test_lakehouse_table_target(manifest, lakehouse)
  token <- fabric_test_token_provider()
  failed_table <- "fabricqueryr_expected_failure"

  seeded <- fabric_lakehouse_write_table(
    target,
    table = failed_table,
    data = data.frame(id = 1L),
    mode = "Overwrite",
    timeout = 900,
    token = token
  )
  expect_equal(seeded$operation_status$status, "Succeeded")

  failure <- expect_error(
    fabric_lakehouse_write_table(
      target,
      table = failed_table,
      data = data.frame(id = "not-an-integer"),
      mode = "Append",
      timeout = 300,
      token = token
    ),
    class = "fabric_lakehouse_write_error"
  )
  expect_true(failure$staging_retained)
  expect_match(failure$staging_path, "^Files/fabricqueryr-staging/")
  retained <- fabric_onelake_list(
    manifest$workspace_id,
    lakehouse$id,
    failure$staging_path,
    recursive = TRUE,
    token = token
  )
  expect_equal(sum(!retained$is_directory), 1L)
  expect_gt(sum(retained$content_length, na.rm = TRUE), 0)

  # A rejected append must leave the committed destination unchanged
  destination_rows <- fabric_test_eventually(function() {
    value <- fabric_onelake_read_delta_table(
      table_path = failed_table,
      workspace_name = manifest$workspace_id,
      lakehouse_name = lakehouse$id,
      schema = lakehouse$schema,
      token = token,
      verbose = FALSE
    )
    if (nrow(value) != 1L) {
      return(NULL)
    }
    value
  })
  expect_equal(destination_rows$id, 1L)

  # The retained source is complete and can be submitted again after fixing
  # the destination problem
  recovered <- fabric_lakehouse_load_table(
    target,
    table = "fabricqueryr_recovered_load",
    path = failure$staging_path,
    path_type = "Folder",
    format = "Parquet",
    mode = "Overwrite",
    file_extension = "parquet",
    token = token
  )
  recovered_state <- fabric_operation_wait(recovered, timeout = 900)
  expect_equal(recovered_state$status, "Succeeded")
  recovered_rows <- fabric_test_eventually(function() {
    value <- fabric_onelake_read_delta_table(
      table_path = "fabricqueryr_recovered_load",
      workspace_name = manifest$workspace_id,
      lakehouse_name = lakehouse$id,
      schema = lakehouse$schema,
      token = token,
      verbose = FALSE
    )
    if (nrow(value) != 1L) {
      return(NULL)
    }
    value
  })
  expect_equal(recovered_rows$id, "not-an-integer")

  expect_true(fabric_onelake_delete(
    manifest$workspace_id,
    lakehouse$id,
    failure$staging_path,
    recursive = TRUE,
    confirm = TRUE,
    token = token
  ))
})

test_that("Lakehouse writer streams a lazy Arrow Dataset end to end", {
  fabric_test_require_package("arrow")
  manifest <- fabric_test_manifest()
  fabric_test_use_table_delta_runtime()
  lakehouse <- fabric_test_manifest_item(manifest, "TestLakehouse")
  target <- fabric_test_lakehouse_table_target(manifest, lakehouse)
  token <- fabric_test_token_provider()
  table <- "fabricqueryr_lazy_arrow_load"
  dataset_path <- tempfile("fabricqueryr-lazy-lakehouse-")
  dir.create(dataset_path)
  on.exit(unlink(dataset_path, recursive = TRUE, force = TRUE), add = TRUE)
  arrow::write_parquet(
    data.frame(id = 1:2, label = c("a", "b")),
    file.path(dataset_path, "part-1.parquet")
  )
  arrow::write_parquet(
    data.frame(id = 3:5, label = c("c", "d", "e")),
    file.path(dataset_path, "part-2.parquet")
  )

  result <- fabric_lakehouse_write_table(
    target,
    table = table,
    data = arrow::open_dataset(dataset_path),
    mode = "Overwrite",
    max_rows_per_file = 2,
    timeout = 900,
    token = token
  )
  expect_equal(result$operation_status$status, "Succeeded")
  expect_equal(result$rows, 5)
  expect_equal(result$file_count, 3L)
  expect_false(result$staging_retained)

  rows <- fabric_test_eventually(function() {
    value <- fabric_onelake_read_delta_table(
      table_path = table,
      workspace_name = manifest$workspace_id,
      lakehouse_name = lakehouse$id,
      schema = lakehouse$schema,
      token = token,
      verbose = FALSE
    )
    if (nrow(value) != 5L) {
      return(NULL)
    }
    value[order(value$id), ]
  })
  expect_equal(rows$id, 1:5)
  expect_equal(rows$label, letters[1:5])
})
