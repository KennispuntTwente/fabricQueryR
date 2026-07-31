test_that("Lakehouse item identifiers are normalized for OneLake", {
  id <- "ac3c729b-c131-46d2-adff-aec92a1a3217"

  expect_equal(fabric_normalize_lakehouse_item(id), id)
  expect_equal(
    fabric_normalize_lakehouse_item("TestLakehouse"),
    "TestLakehouse.Lakehouse"
  )
  expect_equal(
    fabric_normalize_lakehouse_item("TestLakehouse.lakehouse"),
    "TestLakehouse.lakehouse"
  )
})

test_that("Delta staging excludes directories from storage listings", {
  files <- data.frame(
    name = c(
      "Lakehouse/Tables/dbo/table/_delta_log",
      "Lakehouse/Tables/dbo/table/_delta_log/00000000000000000000.json",
      "Lakehouse/Tables/dbo/table/category=A",
      "Lakehouse/Tables/dbo/table/category=A/part.parquet"
    ),
    isdir = c(TRUE, FALSE, TRUE, FALSE)
  )

  downloadable <- fabric_delta_file_rows(files)

  expect_equal(
    downloadable$name,
    c(
      "Lakehouse/Tables/dbo/table/_delta_log/00000000000000000000.json",
      "Lakehouse/Tables/dbo/table/category=A/part.parquet"
    )
  )
  expect_false(any(downloadable$isdir))

  shared <- fabric_delta_file_rows(tibble::tibble(
    path = c(
      "Tables/dbo/table/_delta_log",
      "Tables/dbo/table/_delta_log/00000000000000000000.json"
    ),
    is_directory = c(TRUE, FALSE)
  ))
  expect_equal(
    shared$name,
    "Tables/dbo/table/_delta_log/00000000000000000000.json"
  )
})

test_that("Delta reads consume the shared OneLake filesystem transport", {
  listed_target <- NULL
  downloaded <- list()
  local_mocked_bindings(
    fabric_delta_last_checkpoint_version = function(...) NULL,
    onelake_list_target = function(
      target,
      credential,
      recursive,
      page_size,
      begin_from = NULL
    ) {
      listed_target <<- target
      expect_false(recursive)
      expect_equal(page_size, 5000L)
      expect_null(begin_from)
      tibble::tibble(
        path = "Tables/dbo/table/_delta_log/00000000000000000000.json",
        is_directory = FALSE
      )
    },
    onelake_download_target = function(
      target,
      credential,
      dest,
      overwrite,
      ...
    ) {
      downloaded[[length(downloaded) + 1L]] <<- list(
        path = target$path,
        dest = dest,
        overwrite = overwrite
      )
      writeBin(raw(), dest)
      invisible(dest)
    },
    fabric_delta_resolve_snapshot = function(table_dir, version = NULL) {
      expect_null(version)
      expect_true(dir.exists(table_dir))
      list(
        active = "category=A/part.parquet",
        files = list(
          "category=A/part.parquet" = list(
            deletionVector = list(
              storageType = "u",
              pathOrInlineDv = "ab^-aqEH.-t@S}K{vb[*k^"
            )
          )
        ),
        version = 0
      )
    },
    fabric_delta_read_staged = function(
      table_dir,
      version = NULL,
      columns = NULL,
      limit = NULL
    ) {
      expect_null(version)
      expect_equal(columns, "id")
      expect_equal(limit, 1)
      expect_true(dir.exists(table_dir))
      data.frame(id = 1L)
    }
  )
  dest <- tempfile("delta-shared-transport-")
  on.exit(if (fs::dir_exists(dest)) fs::dir_delete(dest), add = TRUE)

  result <- fabric_onelake_read_delta_table(
    table_path = "table",
    workspace_name = data.frame(
      id = "11111111-1111-1111-1111-111111111111"
    ),
    lakehouse_name = tibble::tibble(
      id = "22222222-2222-2222-2222-222222222222",
      type = "Lakehouse",
      workspaceId = "11111111-1111-1111-1111-111111111111",
      properties = list(list(defaultSchema = "dbo"))
    ),
    token = "token",
    dest_dir = dest,
    verbose = FALSE,
    columns = "id",
    limit = 1
  )

  expect_equal(listed_target$item, "22222222-2222-2222-2222-222222222222")
  expect_equal(listed_target$path, "Tables/dbo/table/_delta_log")
  expect_equal(
    vapply(downloaded, `[[`, character(1), "path"),
    c(
      "Tables/dbo/table/_delta_log/00000000000000000000.json",
      "Tables/dbo/table/category=A/part.parquet",
      paste0(
        "Tables/dbo/table/ab/deletion_vector_",
        "d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin"
      )
    )
  )
  expect_true(all(vapply(downloaded, `[[`, logical(1), "overwrite")))
  expect_equal(result$id, 1L)
})

test_that("Delta staging fetches an older checkpoint after a failed latest one", {
  list_calls <- 0L
  downloaded <- character()
  checkpoint <- function(version) {
    paste0(
      "Tables/dbo/table/_delta_log/",
      sprintf("%020.0f", version),
      ".checkpoint.parquet"
    )
  }
  commit <- function(version) {
    paste0(
      "Tables/dbo/table/_delta_log/",
      sprintf("%020.0f", version),
      ".json"
    )
  }
  local_mocked_bindings(
    fabric_delta_last_checkpoint_version = function(...) 10,
    onelake_list_target = function(
      target,
      credential,
      recursive,
      page_size,
      begin_from = NULL
    ) {
      list_calls <<- list_calls + 1L
      paths <- if (list_calls == 1L) {
        expect_identical(begin_from, "00000000000000000010")
        c(checkpoint(10), commit(11))
      } else {
        expect_null(begin_from)
        c(checkpoint(5), checkpoint(10), vapply(6:11, commit, character(1)))
      }
      tibble::tibble(path = paths, is_directory = FALSE)
    },
    onelake_download_target = function(
      target,
      credential,
      dest,
      overwrite,
      ...
    ) {
      downloaded <<- c(downloaded, target$path)
      writeBin(raw(), dest)
      invisible(dest)
    },
    fabric_delta_checkpoint_sidecar_paths = function(...) character(),
    fabric_delta_resolve_snapshot = function(table_dir, version = NULL) {
      older <- fs::path(
        table_dir,
        "_delta_log",
        basename(checkpoint(5))
      )
      if (!fs::file_exists(older)) {
        rlang::abort(
          "newest checkpoint is corrupt",
          class = "fabric_delta_checkpoint_error"
        )
      }
      list(active = character(), files = list(), version = 11)
    },
    fabric_delta_read_staged = function(...) data.frame(id = integer())
  )
  dest <- tempfile("delta-checkpoint-retry-")
  on.exit(if (fs::dir_exists(dest)) fs::dir_delete(dest), add = TRUE)

  result <- fabric_onelake_read_delta_table(
    table_path = "table",
    workspace_name = "workspace",
    lakehouse_name = "lakehouse",
    schema = "dbo",
    token = "token",
    dest_dir = dest,
    verbose = FALSE
  )

  expect_equal(list_calls, 2L)
  expect_true(checkpoint(10) %in% downloaded)
  expect_true(checkpoint(5) %in% downloaded)
  expect_true(all(vapply(6:11, commit, character(1)) %in% downloaded))
  expect_equal(nrow(result), 0L)
})

test_that("Delta public projection and limit arguments are validated", {
  read_table <- function(...) {
    fabric_onelake_read_delta_table(
      table_path = "table",
      workspace_name = "workspace",
      lakehouse_name = "lakehouse",
      token = "token",
      verbose = FALSE,
      ...
    )
  }

  expect_error(
    read_table(columns = character()),
    "columns must be NULL",
    fixed = TRUE
  )
  expect_error(
    read_table(columns = c("id", "id")),
    "unique",
    fixed = TRUE
  )
  expect_error(read_table(limit = -1), "limit must be NULL", fixed = TRUE)
  expect_error(read_table(limit = 1.5), "limit must be NULL", fixed = TRUE)
  expect_error(read_table(limit = Inf), "limit must be NULL", fixed = TRUE)
  expect_error(
    read_table(timestamp_partition_timezone = ""),
    "timestamp_partition_timezone must be NULL",
    fixed = TRUE
  )
  expect_error(
    read_table(timestamp_partition_timezone = c("UTC", "Europe/Amsterdam")),
    "timestamp_partition_timezone must be NULL",
    fixed = TRUE
  )
  expect_error(read_table(result = "data.frame"), class = "rlang_error")
})

test_that("Delta results can be returned as tibbles or Arrow streams", {
  skip_if_not_installed("arrow")
  skip_if_not_installed("nanoarrow")
  values <- list(
    data.frame(id = 1:2, name = c("alpha", "beta")),
    data.frame(id = integer(), name = character())
  )

  for (value in values) {
    tibble <- fabric_delta_format_result(value, "tibble")
    expect_s3_class(tibble, "tbl_df")
    expect_named(tibble, c("id", "name"))

    stream <- fabric_delta_format_result(value, "arrow_stream")
    expect_s3_class(stream, "nanoarrow_array_stream")
    restored <- as.data.frame(
      arrow::as_record_batch_reader(stream)$read_table()
    )
    expect_named(restored, c("id", "name"))
    expect_equal(nrow(restored), nrow(value))
    expect_equal(restored$id, value$id)
  }
})

test_that("Delta Variant restoration distinguishes SQL and Variant null", {
  physical <- data.frame(row.names = 1:2)
  physical$metadata <- I(list(as.raw(c(17L, 0L, 0L)), as.raw(c(17L, 0L, 0L))))
  physical$value <- I(list(as.raw(0L), as.raw(0L)))
  payload <- data.frame(
    type = c("VARIANT_NULL", "VARIANT_NULL"),
    display = c(NA_character_, NA_character_)
  )
  payload$physical <- physical
  result <- data.frame(
    fabric_delta_source_path_internal = rep("part.parquet", 2),
    fabric_delta_row_index_internal = 0:1,
    check.names = FALSE
  )
  result$payload <- payload

  restored <- fabric_delta_restore_variants(
    result,
    fields = list(list(name = "payload")),
    masks = list(payload = list("part.parquet" = c(TRUE, FALSE))),
    source_column = "fabric_delta_source_path_internal",
    row_column = "fabric_delta_row_index_internal"
  )

  expect_null(restored$payload[[1L]])
  expect_s3_class(restored$payload[[2L]], "fabric_delta_variant")
  expect_identical(restored$payload[[2L]]$type, "VARIANT_NULL")
  expect_identical(restored$payload[[2L]]$value, as.raw(0L))
  expect_identical(format(restored$payload[[2L]]), "null")
  expect_false("fabric_delta_source_path_internal" %in% names(restored))
  expect_false("fabric_delta_row_index_internal" %in% names(restored))

  skip_if_not_installed("arrow")
  skip_if_not_installed("nanoarrow")
  stream <- fabric_delta_format_result(restored, "arrow_stream")
  expect_s3_class(stream, "nanoarrow_array_stream")
  arrow_result <- as.data.frame(
    arrow::as_record_batch_reader(stream)$read_table()
  )
  expect_s3_class(arrow_result$payload, "data.frame")
  expect_named(
    arrow_result$payload,
    c("type", "display", "metadata", "value")
  )
  expect_true(all(vapply(
    arrow_result$payload[1L, ],
    function(value) {
      is.null(value[[1L]]) || isTRUE(is.na(value[[1L]]))
    },
    logical(1)
  )))
  expect_identical(arrow_result$payload$type[[2L]], "VARIANT_NULL")
  expect_identical(arrow_result$payload$value[[2L]], as.raw(0L))

  empty_variant <- data.frame(id = integer())
  empty_variant$payload <- structure(
    list(),
    class = c("fabric_delta_variant_column", "list")
  )
  empty_stream <- fabric_delta_format_result(empty_variant, "arrow_stream")
  empty_result <- as.data.frame(
    arrow::as_record_batch_reader(empty_stream)$read_table()
  )
  expect_equal(nrow(empty_result), 0L)
  expect_s3_class(empty_result$payload, "data.frame")
  expect_named(empty_result$payload, c("type", "display", "metadata", "value"))
})

test_that("Delta records validate workspace ownership", {
  workspace <- data.frame(
    id = "11111111-1111-1111-1111-111111111111"
  )
  lakehouse <- tibble::tibble(
    id = "22222222-2222-2222-2222-222222222222",
    type = "Lakehouse",
    workspaceId = "33333333-3333-3333-3333-333333333333",
    properties = list(list(defaultSchema = "dbo"))
  )

  expect_error(
    fabric_onelake_read_delta_table(
      table_path = "table",
      workspace_name = workspace,
      lakehouse_name = lakehouse,
      schema = "curated",
      token = "token",
      verbose = FALSE
    ),
    "different workspace",
    fixed = TRUE
  )
})

test_that("Delta reads accept Warehouse discovery records", {
  warehouse <- tibble::tibble(
    id = "22222222-2222-2222-2222-222222222222",
    type = "Warehouse",
    workspaceId = "11111111-1111-1111-1111-111111111111"
  )
  local_mocked_bindings(
    onelake_resolve_target = function(
      workspace,
      item,
      path,
      item_type,
      dfs_base
    ) {
      expect_equal(item_type, "Warehouse")
      expect_equal(fabric_record_value(item, "id"), warehouse$id)
      rlang::abort("target captured")
    }
  )

  expect_error(
    fabric_onelake_read_delta_table(
      table_path = "table",
      workspace_name = warehouse$workspaceId,
      lakehouse_name = warehouse,
      schema = "dbo",
      token = "token",
      verbose = FALSE
    ),
    "target captured",
    fixed = TRUE
  )
})

test_that("Delta reads do not download tombstoned or historical data files", {
  downloaded <- character()
  local_mocked_bindings(
    fabric_delta_last_checkpoint_version = function(...) NULL,
    onelake_list_target = function(
      target,
      credential,
      recursive,
      page_size,
      begin_from = NULL
    ) {
      tibble::tibble(
        path = c(
          "Tables/table/_delta_log/00000000000000000000.json",
          "Tables/table/_delta_log/00000000000000000001.json"
        ),
        is_directory = FALSE
      )
    },
    onelake_download_target = function(
      target,
      credential,
      dest,
      overwrite,
      ...
    ) {
      downloaded <<- c(downloaded, target$path)
      fs::dir_create(fs::path_dir(dest))
      writeBin(raw(), dest)
      invisible(dest)
    },
    fabric_delta_resolve_snapshot = function(table_dir, version = NULL) {
      list(active = "active.parquet", version = 1)
    },
    fabric_delta_read_staged = function(
      table_dir,
      version = NULL,
      columns = NULL,
      limit = NULL
    ) {
      expect_null(columns)
      expect_null(limit)
      data.frame(id = 1L)
    }
  )
  dest <- tempfile("delta-active-only-")
  on.exit(if (fs::dir_exists(dest)) fs::dir_delete(dest), add = TRUE)

  fabric_onelake_read_delta_table(
    table_path = "table",
    workspace_name = "workspace",
    lakehouse_name = "lakehouse",
    token = "token",
    dest_dir = dest,
    verbose = FALSE
  )

  expect_setequal(
    downloaded,
    c(
      "Tables/table/_delta_log/00000000000000000000.json",
      "Tables/table/_delta_log/00000000000000000001.json",
      "Tables/table/active.parquet"
    )
  )
  expect_false("Tables/table/tombstoned.parquet" %in% downloaded)
})

test_that("Delta staging preserves paths beneath the table root", {
  staged <- fabric_delta_stage_paths(
    c(
      "Lakehouse/Tables/dbo/table/_delta_log/00000000000000000010.checkpoint.parquet",
      "Lakehouse/Tables/dbo/table/category=A/part.parquet",
      "Lakehouse/Tables/dbo/table/category=B/part.parquet"
    ),
    "Lakehouse/Tables/dbo/table",
    "stage"
  )

  expect_equal(
    staged$relative,
    c(
      "_delta_log/00000000000000000010.checkpoint.parquet",
      "category=A/part.parquet",
      "category=B/part.parquet"
    )
  )
  expect_equal(
    gsub("\\\\", "/", as.character(staged$destination)),
    paste0("stage/", staged$relative)
  )
  expect_equal(sum(basename(staged$destination) == "part.parquet"), 2L)
})

test_that("Delta staging accepts paths longer than PATH_MAX", {
  # Writers spell a NULL partition value `__HIVE_DEFAULT_PARTITION__` in the
  # directory name, so a handful of partition columns pushes a staged path past
  # 260 characters. `fs::path()` refuses to build those even where the file
  # system stores them happily.
  partitions <- paste(rep("__HIVE_DEFAULT_PARTITION__", 6L), collapse = "/")
  relative <- paste0(
    partitions,
    "/part-00000-963863a1-5943-43a4-83a5-f831d44e159e-c000.snappy.parquet"
  )
  staged <- fabric_delta_stage_paths(
    paste0("Lakehouse/Tables/dbo/table/", relative),
    "Lakehouse/Tables/dbo/table",
    "stage"
  )
  expect_equal(staged$relative, relative)
  expect_equal(
    gsub("\\\\", "/", as.character(staged$destination)),
    paste0("stage/", relative)
  )
  expect_gt(nchar(fabric_delta_local_file(strrep("d", 200L), relative)), 260L)
})

test_that("Delta reads tolerate staged paths longer than PATH_MAX", {
  table_dir <- fs::path_temp(paste0("delta-long-", sample.int(1e9, 1)))
  relative <- paste0(
    paste(rep("__HIVE_DEFAULT_PARTITION__", 6L), collapse = "/"),
    "/part-00000-963863a1-5943-43a4-83a5-f831d44e159e-c000.snappy.parquet"
  )
  parquet <- file.path(table_dir, relative, fsep = "/")
  created <- tryCatch(
    {
      dir.create(dirname(parquet), recursive = TRUE)
      file.exists(dirname(parquet))
    },
    error = function(error) FALSE,
    warning = function(warning) FALSE
  )
  skip_if(
    !isTRUE(created) || nchar(parquet) <= 260L,
    "this file system cannot store a path longer than PATH_MAX"
  )
  on.exit(unlink(table_dir, recursive = TRUE, force = TRUE), add = TRUE)
  log_dir <- file.path(table_dir, "_delta_log", fsep = "/")
  dir.create(log_dir, recursive = TRUE)
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  DBI::dbExecute(
    con,
    paste0(
      "COPY (SELECT CAST(7 AS INTEGER) AS id) TO ",
      as.character(DBI::dbQuoteString(con, parquet)),
      " (FORMAT PARQUET)"
    )
  )
  schema <- jsonlite::toJSON(
    list(
      type = "struct",
      fields = list(
        list(name = "id", type = "integer", nullable = TRUE, metadata = list()),
        list(
          name = "part",
          type = "string",
          nullable = TRUE,
          metadata = list()
        )
      )
    ),
    auto_unbox = TRUE
  )
  writeLines(
    c(
      '{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}',
      jsonlite::toJSON(
        list(
          metaData = list(
            id = "table",
            format = list(provider = "parquet", options = list()),
            schemaString = schema,
            partitionColumns = list("part"),
            configuration = list()
          )
        ),
        auto_unbox = TRUE
      ),
      jsonlite::toJSON(
        list(
          add = list(
            path = relative,
            partitionValues = stats::setNames(list(NULL), "part")
          )
        ),
        auto_unbox = TRUE,
        null = "null"
      )
    ),
    file.path(log_dir, "00000000000000000000.json", fsep = "/"),
    useBytes = TRUE
  )

  result <- fabric_delta_read_staged(table_dir)

  expect_equal(nrow(result), 1L)
  expect_identical(result$id, 7L)
  expect_identical(result$part, NA_character_)
})

test_that("Delta staging rejects paths outside the requested table", {
  expect_error(
    fabric_delta_stage_paths(
      "Lakehouse/Tables/dbo/other/part.parquet",
      "Lakehouse/Tables/dbo/table",
      "stage"
    ),
    "outside the requested Delta table",
    fixed = TRUE
  )
  expect_error(
    fabric_delta_stage_paths(
      "Lakehouse/Tables/dbo/table/../other/part.parquet",
      "Lakehouse/Tables/dbo/table",
      "stage"
    ),
    "unsafe relative Delta table path",
    fixed = TRUE
  )
})

test_that("Delta staging preserves encoded binary partition paths", {
  target <- onelake_resolve_target(
    "11111111-1111-1111-1111-111111111111",
    "22222222-2222-2222-2222-222222222222",
    "Tables/dbo/table",
    dfs_base = "https://onelake.dfs.fabric.microsoft.com"
  )
  path <- paste0(
    "event_date=2026-01-01/",
    "timestamp_part=2026-01-01%2012%3A34%3A56.123456/",
    "binary_part=%01%02%03/",
    "part.parquet"
  )
  staged <- fabric_delta_stage_files(
    path,
    target,
    "Tables/dbo/table",
    "stage"
  )

  expect_identical(staged$relative, utils::URLdecode(path))
  expect_match(
    onelake_path_url(staged$target[[1L]]),
    paste0(
      "Tables/dbo/table/event_date%3D2026-01-01/",
      "timestamp_part%3D2026-01-01%2012%3A34%3A56.123456/",
      "binary_part%3D%01%02%03/part.parquet$"
    )
  )
  expect_false(grepl(
    "[\001\002\003]",
    onelake_path_url(staged$target[[1L]])
  ))

  expect_error(
    fabric_delta_stage_files(
      "%2E%2E/part.parquet",
      target,
      "Tables/dbo/table",
      "stage"
    ),
    "unsafe data-file path",
    fixed = TRUE
  )
  expect_error(
    fabric_delta_encode_uri_path("category=bad%escape/part.parquet"),
    "invalid percent-encoded path",
    fixed = TRUE
  )
})

test_that("Delta binary partitions preserve NUL and high-bit bytes", {
  values <- c("\\u0000", "\\u0080", "\\u00ff", "\\u0001\\u00ff")
  actions <- lapply(seq_along(values), function(index) {
    line <- paste0(
      '{"add":{"path":"part-',
      index,
      '.parquet","partitionValues":{"binary_part":"',
      values[[index]],
      '"}}}'
    )
    action <- jsonlite::fromJSON(line, simplifyVector = FALSE)
    fabric_delta_preserve_partition_tokens(action, line)
  })
  state <- fabric_delta_apply_actions(
    list(
      active = character(),
      files = list(),
      has_deletion_vectors = FALSE
    ),
    actions
  )
  schema <- list(
    fields = list(list(name = "binary_part", type = "binary")),
    partitionColumns = "binary_part",
    columnMappingMode = "none"
  )
  paths <- paste0("C:/staged/part-", seq_along(values), ".parquet")

  mapping <- fabric_delta_partition_mapping(state, paths, schema)

  expect_identical(
    unclass(mapping$fabric_delta_partition_1),
    list(
      as.raw(0L),
      as.raw(128L),
      as.raw(255L),
      as.raw(c(1L, 255L))
    )
  )
  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  DBI::dbWriteTable(con, "binary_partitions", mapping, temporary = TRUE)
  roundtrip <- DBI::dbGetQuery(
    con,
    "SELECT fabric_delta_partition_1 FROM binary_partitions"
  )
  expect_identical(
    roundtrip$fabric_delta_partition_1,
    unclass(mapping$fabric_delta_partition_1)
  )
})

test_that("Delta stages and reads absolute OneLake AddFile paths", {
  current_target <- onelake_resolve_target(
    "11111111-1111-1111-1111-111111111111",
    "22222222-2222-2222-2222-222222222222",
    "Tables/current",
    dfs_base = "https://onelake.dfs.fabric.microsoft.com"
  )
  absolute <- paste0(
    "abfss://33333333-3333-3333-3333-333333333333",
    "@onelake.dfs.fabric.microsoft.com/",
    "44444444-4444-4444-4444-444444444444/",
    "Tables/source/part.parquet"
  )
  staged <- fabric_delta_stage_files(
    absolute,
    current_target,
    "Tables/current",
    "stage"
  )
  expect_equal(
    staged$target[[1L]]$workspace,
    "33333333-3333-3333-3333-333333333333"
  )
  expect_equal(
    staged$target[[1L]]$item,
    "44444444-4444-4444-4444-444444444444"
  )
  expect_match(
    gsub("\\\\", "/", staged$relative),
    "^_delta_log/\\.fabricqueryr-external/"
  )
  expect_lt(nchar(staged$relative), 120L)
  second <- sub("part\\.parquet$", "nested/part.parquet", absolute)
  second_staged <- fabric_delta_stage_files(
    second,
    current_target,
    "Tables/current",
    "stage"
  )
  expect_false(identical(staged$relative, second_staged$relative))
  expect_match(staged$relative, "\\.parquet$")

  table_dir <- fs::path_temp(
    paste0("delta-absolute-", sample.int(1e9, 1))
  )
  log_dir <- fs::path(table_dir, "_delta_log")
  parquet <- fabric_delta_local_file(table_dir, absolute)
  fs::dir_create(log_dir, recurse = TRUE)
  fs::dir_create(fs::path_dir(parquet), recurse = TRUE)
  on.exit(fs::dir_delete(table_dir), add = TRUE)
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  DBI::dbExecute(
    con,
    paste0(
      "COPY (SELECT 42::INTEGER AS id) TO ",
      as.character(DBI::dbQuoteString(con, gsub("\\\\", "/", parquet))),
      " (FORMAT PARQUET)"
    )
  )
  schema <- jsonlite::toJSON(
    list(
      type = "struct",
      fields = list(list(
        name = "id",
        type = "integer",
        nullable = FALSE,
        metadata = list()
      ))
    ),
    auto_unbox = TRUE
  )
  actions <- list(
    list(protocol = list(minReaderVersion = 1L, minWriterVersion = 2L)),
    list(
      metaData = list(
        id = "absolute-table",
        format = list(provider = "parquet", options = list()),
        schemaString = schema,
        partitionColumns = list(),
        configuration = list()
      )
    ),
    list(add = list(path = absolute, partitionValues = list()))
  )
  writeLines(
    vapply(actions, jsonlite::toJSON, character(1), auto_unbox = TRUE),
    fs::path(log_dir, "00000000000000000000.json"),
    useBytes = TRUE
  )

  result <- fabric_delta_read_staged(table_dir)

  expect_equal(result$id, 42L)
})

test_that("automatic Delta staging is unique and cleaned after each read", {
  staging_dirs <- character()
  local_mocked_bindings(
    fabric_delta_last_checkpoint_version = function(...) NULL,
    onelake_list_target = function(...) {
      tibble::tibble(
        path = "Tables/table/_delta_log/00000000000000000000.json",
        is_directory = FALSE
      )
    },
    onelake_download_target = function(target, credential, dest, ...) {
      fs::dir_create(fs::path_dir(dest), recurse = TRUE)
      writeBin(raw(), dest)
      invisible(dest)
    },
    fabric_delta_resolve_snapshot = function(table_dir, version = NULL) {
      staging_dirs <<- c(staging_dirs, as.character(table_dir))
      list(active = character(), version = 0)
    },
    fabric_delta_read_staged = function(...) data.frame(id = integer())
  )

  for (index in 1:2) {
    result <- fabric_onelake_read_delta_table(
      table_path = "table",
      workspace_name = "workspace",
      lakehouse_name = "lakehouse",
      token = "token",
      verbose = FALSE
    )
    expect_equal(nrow(result), 0L)
  }

  expect_length(unique(staging_dirs), 2L)
  expect_false(any(fs::dir_exists(staging_dirs)))
})

test_that("Delta reads reject retained files in a supplied staging directory", {
  dest <- fs::path_temp(paste0("delta-stale-", sample.int(1e9, 1)))
  stale_log <- fs::path(
    dest,
    "_delta_log",
    "00000000000000000099.json"
  )
  fs::dir_create(fs::path_dir(stale_log), recurse = TRUE)
  writeLines(
    '{"add":{"path":"wrong-table.parquet"}}',
    stale_log,
    useBytes = TRUE
  )
  on.exit(fs::dir_delete(dest), add = TRUE)
  listed <- FALSE
  local_mocked_bindings(
    onelake_list_target = function(...) {
      listed <<- TRUE
      data.frame()
    }
  )

  expect_error(
    fabric_onelake_read_delta_table(
      table_path = "other_table",
      workspace_name = "workspace",
      lakehouse_name = "lakehouse",
      token = "token",
      dest_dir = dest,
      verbose = FALSE
    ),
    "dest_dir must be a new or empty directory",
    fixed = TRUE
  )
  expect_false(listed)
  expect_true(fs::file_exists(stale_log))
})

test_that("Delta log selection stages only the newest checkpoint tail", {
  old_commits <- sprintf(
    "Tables/table/_delta_log/%020.0f.json",
    0:9999
  )
  checkpoint <- paste0(
    "Tables/table/_delta_log/",
    "00000000000000010000.checkpoint.parquet"
  )
  recent_commits <- sprintf(
    "Tables/table/_delta_log/%020.0f.json",
    10001:10004
  )
  ignored <- c(
    "Tables/table/_delta_log/_last_checkpoint",
    "Tables/table/_delta_log/00000000000000010004.crc"
  )

  selected <- fabric_delta_select_log_paths(
    c(old_commits, checkpoint, recent_commits, ignored)
  )

  expect_equal(selected$checkpoint_version, 10000)
  expect_equal(selected$target, 10004)
  expect_setequal(selected$paths, c(checkpoint, recent_commits))
  expect_length(selected$paths, 5L)
})

test_that("Delta log selection respects historical versions", {
  paths <- c(
    sprintf("Tables/table/_delta_log/%020.0f.json", 0:12),
    paste0(
      "Tables/table/_delta_log/",
      "00000000000000000010.checkpoint.parquet"
    )
  )

  selected <- fabric_delta_select_log_paths(paths, version = 11)

  expect_equal(selected$checkpoint_version, 10)
  expect_equal(
    selected$paths,
    c(
      paste0(
        "Tables/table/_delta_log/",
        "00000000000000000010.checkpoint.parquet"
      ),
      "Tables/table/_delta_log/00000000000000000011.json"
    )
  )
  expect_error(
    fabric_delta_select_log_paths(paths, version = 13),
    "does not exist",
    fixed = TRUE
  )
})

test_that("Delta reads the last-checkpoint pointer directly", {
  calls <- 0L
  httr2::local_mocked_responses(function(req) {
    calls <<- calls + 1L
    httr2::response(
      status_code = if (calls == 1L) 200L else 404L,
      url = req$url,
      headers = list("content-type" = "application/json"),
      body = if (calls == 1L) {
        charToRaw('{"version":42,"size":7}')
      } else {
        raw()
      }
    )
  })
  target <- onelake_resolve_target(
    "workspace",
    "lakehouse.Lakehouse",
    "Tables/table/_delta_log"
  )
  credential <- fabric_credential(token = "token")

  expect_equal(
    fabric_delta_last_checkpoint_version(target, credential),
    42
  )
  expect_null(fabric_delta_last_checkpoint_version(target, credential))
})

test_that("Delta versions must be non-negative integers", {
  for (version in list(-1, 1.5, NA_real_, c(1, 2), "1", 2^53 + 2)) {
    expect_error(
      fabric_onelake_read_delta_table(
        table_path = "table",
        workspace_name = "workspace",
        lakehouse_name = "lakehouse",
        token = "token",
        version = version,
        verbose = FALSE
      ),
      "version must be one exactly representable non-negative integer",
      fixed = TRUE
    )
  }
})

test_that("Delta versions are not narrowed to 32-bit integers", {
  local_mocked_bindings(
    fabric_credential = function(...) {
      structure(
        list(),
        class = "fabric_credential"
      )
    },
    onelake_resolve_target = function(...) {
      rlang::abort("version validation completed")
    }
  )

  expect_error(
    fabric_onelake_read_delta_table(
      table_path = "table",
      workspace_name = "workspace",
      lakehouse_name = "lakehouse",
      token = "token",
      version = 3000000000,
      verbose = FALSE
    ),
    "version validation completed",
    fixed = TRUE
  )
  expect_equal(
    fabric_delta_versions_from_text(c(
      "00000000003000000000",
      "00009007199254740992"
    )),
    c(3000000000, 2^53)
  )
  expect_error(
    fabric_delta_versions_from_text("00009007199254740993"),
    class = "fabric_delta_unsupported_error"
  )
})

test_that("Delta JSON logs resolve latest and versioned snapshots", {
  table_dir <- fs::path_temp(paste0("delta-json-", sample.int(1e9, 1)))
  log_dir <- fs::path(table_dir, "_delta_log")
  fs::dir_create(log_dir, recurse = TRUE)
  on.exit(fs::dir_delete(table_dir), add = TRUE)

  writeLines(
    c(
      paste0(
        '{"protocol":{"minReaderVersion":3,"minWriterVersion":7,',
        '"readerFeatures":["deletionVectors"],',
        '"writerFeatures":["deletionVectors"]}}'
      ),
      paste0(
        '{"metaData":{"id":"table",',
        '"format":{"provider":"parquet","options":{}},',
        '"schemaString":"{\\"type\\":\\"struct\\",\\"fields\\":[]}",',
        '"partitionColumns":[],"configuration":{}}}'
      ),
      '{"add":{"path":"category=A/part.parquet"}}',
      '{"add":{"path":"category=B/part.parquet"}}'
    ),
    fs::path(log_dir, "00000000000000000000.json"),
    useBytes = TRUE
  )
  writeLines(
    c(
      paste0(
        '{"add":{"path":"category=B/part.parquet",',
        '"deletionVector":{"storageType":"i","pathOrInlineDv":"00000"}}}'
      ),
      '{"remove":{"path":"category=B/part.parquet"}}'
    ),
    fs::path(log_dir, "00000000000000000001.json"),
    useBytes = TRUE
  )

  latest <- fabric_delta_resolve_snapshot(table_dir)
  original <- fabric_delta_resolve_snapshot(table_dir, version = 0)

  expect_equal(latest$version, 1)
  expect_setequal(
    latest$active,
    c("category=A/part.parquet", "category=B/part.parquet")
  )
  expect_equal(
    latest$files[["category=B/part.parquet"]]$deletionVector$storageType,
    "i"
  )
  expect_setequal(
    original$active,
    c("category=A/part.parquet", "category=B/part.parquet")
  )
})

test_that("Delta reads preserve duplicate basenames in separate partitions", {
  table_dir <- fs::path_temp(
    paste0("delta-duplicate-basenames-", sample.int(1e9, 1))
  )
  log_dir <- fs::path(table_dir, "_delta_log")
  partitions <- fs::path(table_dir, c("category=A", "category=B"))
  fs::dir_create(log_dir, recurse = TRUE)
  purrr::walk(partitions, fs::dir_create, recurse = TRUE)
  on.exit(fs::dir_delete(table_dir), add = TRUE)

  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  for (index in seq_along(partitions)) {
    parquet <- fs::path(partitions[[index]], "part.parquet")
    literal <- as.character(DBI::dbQuoteString(
      con,
      gsub("\\", "/", parquet, fixed = TRUE)
    ))
    DBI::dbExecute(
      con,
      paste0(
        "COPY (SELECT ",
        index,
        "::BIGINT AS id, 'row-",
        index,
        "' AS value) TO ",
        literal,
        " (FORMAT PARQUET)"
      )
    )
  }

  schema <- jsonlite::toJSON(
    list(
      type = "struct",
      fields = list(
        list(name = "id", type = "long", nullable = FALSE, metadata = list()),
        list(
          name = "value",
          type = "string",
          nullable = FALSE,
          metadata = list()
        ),
        list(
          name = "category",
          type = "string",
          nullable = FALSE,
          metadata = list()
        )
      )
    ),
    auto_unbox = TRUE
  )
  writeLines(
    c(
      '{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}',
      jsonlite::toJSON(
        list(
          metaData = list(
            id = "table",
            format = list(provider = "parquet", options = list()),
            schemaString = schema,
            partitionColumns = list("category"),
            configuration = list()
          )
        ),
        auto_unbox = TRUE
      ),
      '{"add":{"path":"category=A/part.parquet","partitionValues":{"category":"A"}}}',
      '{"add":{"path":"category=B/part.parquet","partitionValues":{"category":"B"}}}'
    ),
    fs::path(log_dir, "00000000000000000000.json"),
    useBytes = TRUE
  )

  result <- fabric_delta_read_staged(table_dir)
  result <- result[order(result$id), ]

  expect_equal(result$id, c(1, 2))
  expect_equal(result$value, c("row-1", "row-2"))
  expect_equal(result$category, c("A", "B"))

  projected <- fabric_delta_read_staged(
    table_dir,
    columns = c("category", "id"),
    limit = 1
  )
  expect_named(projected, c("category", "id"))
  expect_equal(nrow(projected), 1L)
  expect_true(projected$id %in% c(1, 2))
  expect_true(projected$category %in% c("A", "B"))
})

test_that("Delta reader preserves the logical schema for empty tables", {
  table_dir <- fs::path_temp(paste0("delta-empty-", sample.int(1e9, 1)))
  log_dir <- fs::path(table_dir, "_delta_log")
  fs::dir_create(log_dir, recurse = TRUE)
  on.exit(fs::dir_delete(table_dir), add = TRUE)
  schema <- jsonlite::toJSON(
    list(
      type = "struct",
      fields = list(
        list(
          name = "id",
          type = "long",
          nullable = FALSE,
          metadata = list()
        ),
        list(
          name = "label",
          type = "string",
          nullable = TRUE,
          metadata = list()
        )
      )
    ),
    auto_unbox = TRUE
  )
  writeLines(
    c(
      '{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}',
      jsonlite::toJSON(
        list(
          metaData = list(
            id = "table",
            format = list(provider = "parquet", options = list()),
            schemaString = schema,
            partitionColumns = list(),
            configuration = list()
          )
        ),
        auto_unbox = TRUE
      )
    ),
    fs::path(log_dir, "00000000000000000000.json"),
    useBytes = TRUE
  )

  result <- fabric_delta_read_staged(table_dir)

  expect_equal(nrow(result), 0L)
  expect_equal(names(result), c("id", "label"))
  expect_s3_class(result$id, "integer64")
  expect_type(result$label, "character")

  projected <- fabric_delta_read_staged(
    table_dir,
    columns = "label",
    limit = 0
  )
  expect_named(projected, "label")
  expect_equal(nrow(projected), 0L)
  expect_type(projected$label, "character")
  expect_error(
    fabric_delta_read_staged(table_dir, columns = "missing"),
    "not present in the selected snapshot",
    fixed = TRUE
  )
})

test_that("Delta logical schemas cover primitive and nested types", {
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  primitives <- c(
    string = "VARCHAR",
    long = "BIGINT",
    integer = "INTEGER",
    short = "SMALLINT",
    byte = "TINYINT",
    float = "FLOAT",
    double = "DOUBLE",
    boolean = "BOOLEAN",
    binary = "BLOB",
    date = "DATE",
    timestamp = "TIMESTAMPTZ",
    timestamp_ntz = "TIMESTAMP",
    void = "BOOLEAN",
    "decimal(18,4)" = "DECIMAL(18,4)"
  )
  converted <- vapply(
    names(primitives),
    function(type) fabric_delta_duckdb_type(con, type),
    character(1)
  )
  expect_equal(unname(converted), unname(primitives))

  complex <- list(
    type = "struct",
    fields = list(
      list(
        name = "labels",
        type = list(
          type = "array",
          elementType = "string"
        )
      ),
      list(
        name = "counts",
        type = list(
          type = "map",
          keyType = "string",
          valueType = "long"
        )
      )
    )
  )
  complex_type <- fabric_delta_duckdb_type(con, complex)
  expect_match(complex_type, "^STRUCT\\(")
  expect_match(complex_type, "labels VARCHAR\\[\\]")
  expect_match(complex_type, "counts MAP\\(VARCHAR, BIGINT\\)")

  expect_equal(
    fabric_delta_duckdb_type(con, "variant"),
    "VARIANT"
  )
  expect_error(
    fabric_delta_duckdb_type(con, list(type = "struct", fields = list())),
    "Empty Delta struct fields are not supported",
    fixed = TRUE
  )
  expect_error(
    fabric_delta_duckdb_type(con, list(type = "interval")),
    "Unsupported Delta complex schema type: interval",
    fixed = TRUE
  )

  exact_result_type <- fabric_delta_duckdb_result_type(
    con,
    list(
      type = "struct",
      fields = list(
        list(name = "amount", type = "decimal(38,2)"),
        list(
          name = "history",
          type = list(type = "array", elementType = "decimal(20,0)")
        ),
        list(name = "id", type = "long")
      )
    )
  )
  expect_equal(
    exact_result_type,
    "STRUCT(amount VARCHAR, history VARCHAR[], id BIGINT)"
  )

  mapped_field <- function(name, type, id, physical) {
    list(
      name = name,
      type = type,
      metadata = list(
        "delta.columnMapping.id" = id,
        "delta.columnMapping.physicalName" = physical
      )
    )
  }
  id_schema <- list(
    partitionColumns = character(),
    fields = list(
      mapped_field(
        "outer",
        list(
          type = "struct",
          fields = list(mapped_field(
            "inner",
            "integer",
            2L,
            "physical-inner"
          ))
        ),
        1L,
        "physical-outer"
      ),
      mapped_field(
        "items",
        list(
          type = "array",
          elementType = list(
            type = "struct",
            fields = list(mapped_field(
              "label",
              "string",
              4L,
              "physical-label"
            ))
          )
        ),
        3L,
        "physical-items"
      ),
      mapped_field(
        "attributes",
        list(
          type = "map",
          keyType = "string",
          valueType = list(
            type = "struct",
            fields = list(mapped_field(
              "enabled",
              "boolean",
              6L,
              "physical-enabled"
            ))
          )
        ),
        5L,
        "physical-attributes"
      )
    )
  )
  id_projection <- fabric_delta_id_file_projection(
    con,
    id_schema,
    c(
      "1" = "file-outer",
      "2" = "file-inner",
      "3" = "file-items",
      "4" = "file-label",
      "5" = "file-attributes",
      "6" = "file-enabled"
    )
  )
  expect_match(id_projection, '"file-outer"."file-inner"', fixed = TRUE)
  expect_match(id_projection, '"physical-inner" :=', fixed = TRUE)
  expect_match(id_projection, 'AS "physical-outer"', fixed = TRUE)
  expect_match(id_projection, "CASE WHEN", fixed = TRUE)
  expect_match(id_projection, "list_transform(", fixed = TRUE)
  expect_match(id_projection, '"file-label"', fixed = TRUE)
  expect_match(id_projection, "map_values(", fixed = TRUE)
  expect_match(id_projection, '"file-enabled"', fixed = TRUE)

  name_expression <- fabric_delta_type_expression(
    con,
    id_schema$fields[[2L]]$type,
    '"physical-items"',
    "name"
  )
  expect_match(name_expression, "list_transform(", fixed = TRUE)
  expect_match(
    name_expression,
    'CAST(fabric_delta_element AS STRUCT("physical-label" VARCHAR))',
    fixed = TRUE
  )
})

test_that("name mapping fills nested metadata-only fields with NULL", {
  table_dir <- fs::path_temp(paste0(
    "delta-nested-name-evolution-",
    sample.int(1e9, 1)
  ))
  log_dir <- fs::path(table_dir, "_delta_log")
  fs::dir_create(log_dir, recurse = TRUE)
  on.exit(fs::dir_delete(table_dir), add = TRUE)
  parquet <- fs::path(table_dir, "part.parquet")
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  DBI::dbExecute(
    con,
    paste0(
      "COPY (SELECT ",
      "struct_pack(p_existing := 7::INTEGER) AS p_profile, ",
      "[struct_pack(p_label := 'first')] AS p_items, ",
      "map(['key'], [struct_pack(p_enabled := true)]) AS p_attributes",
      ") TO ",
      as.character(DBI::dbQuoteString(con, gsub("\\\\", "/", parquet))),
      " (FORMAT PARQUET)"
    )
  )
  mapped_field <- function(name, type, id, physical, nullable = TRUE) {
    list(
      name = name,
      type = type,
      nullable = nullable,
      metadata = list(
        "delta.columnMapping.id" = id,
        "delta.columnMapping.physicalName" = physical
      )
    )
  }
  schema <- jsonlite::toJSON(
    list(
      type = "struct",
      fields = list(
        mapped_field(
          "profile",
          list(
            type = "struct",
            fields = list(
              mapped_field("existing", "integer", 2L, "p_existing"),
              mapped_field("added", "string", 3L, "p_added")
            )
          ),
          1L,
          "p_profile"
        ),
        mapped_field(
          "items",
          list(
            type = "array",
            elementType = list(
              type = "struct",
              fields = list(
                mapped_field("label", "string", 5L, "p_label"),
                mapped_field("added", "long", 6L, "p_added")
              )
            ),
            containsNull = TRUE
          ),
          4L,
          "p_items"
        ),
        mapped_field(
          "attributes",
          list(
            type = "map",
            keyType = "string",
            valueType = list(
              type = "struct",
              fields = list(
                mapped_field("enabled", "boolean", 8L, "p_enabled"),
                mapped_field("added", "double", 9L, "p_added")
              )
            ),
            valueContainsNull = TRUE
          ),
          7L,
          "p_attributes"
        )
      )
    ),
    auto_unbox = TRUE
  )
  actions <- list(
    list(protocol = list(minReaderVersion = 2L, minWriterVersion = 5L)),
    list(
      metaData = list(
        id = "nested-name-evolution",
        format = list(provider = "parquet", options = list()),
        schemaString = schema,
        partitionColumns = list(),
        configuration = list("delta.columnMapping.mode" = "name")
      )
    ),
    list(add = list(path = "part.parquet", partitionValues = list()))
  )
  writeLines(
    vapply(actions, jsonlite::toJSON, character(1), auto_unbox = TRUE),
    fs::path(log_dir, "00000000000000000000.json"),
    useBytes = TRUE
  )

  result <- fabric_delta_read_staged(table_dir)

  expect_identical(result$profile$existing, 7L)
  expect_true(is.na(result$profile$added))
  expect_identical(result$items[[1L]]$label, "first")
  expect_true(is.na(result$items[[1L]]$added))
  expect_identical(result$attributes[[1L]]$key, "key")
  expect_true(result$attributes[[1L]]$value$enabled)
  expect_true(is.na(result$attributes[[1L]]$value$added))
})

test_that("Delta structs preserve parent validity separately from children", {
  table_dir <- fs::path_temp(paste0(
    "delta-struct-validity-",
    sample.int(1e9, 1)
  ))
  log_dir <- fs::path(table_dir, "_delta_log")
  fs::dir_create(log_dir, recurse = TRUE)
  on.exit(fs::dir_delete(table_dir), add = TRUE)
  parquet <- fs::path(table_dir, "part.parquet")
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  struct_type <- "STRUCT(p_number INTEGER, p_text VARCHAR)"
  key_type <- paste0(
    "STRUCT(p_marker INTEGER, p_nested ",
    struct_type,
    ")"
  )
  DBI::dbExecute(
    con,
    paste0(
      "COPY (",
      "SELECT 1::INTEGER AS p_id, NULL::",
      struct_type,
      " AS p_profile, [NULL::",
      struct_type,
      ", struct_pack(p_number := NULL::INTEGER, ",
      "p_text := NULL::VARCHAR)] AS p_items, ",
      "map(['null', 'present'], [NULL::",
      struct_type,
      ", struct_pack(p_number := NULL::INTEGER, ",
      "p_text := NULL::VARCHAR)]) AS p_attributes, ",
      "map([",
      "struct_pack(p_marker := 1::INTEGER, p_nested := NULL::",
      struct_type,
      "), ",
      "struct_pack(p_marker := 2::INTEGER, p_nested := ",
      "struct_pack(p_number := NULL::INTEGER, p_text := NULL::VARCHAR))",
      "], ['null', 'present']) AS p_keyed ",
      "UNION ALL SELECT 2::INTEGER, ",
      "struct_pack(p_number := NULL::INTEGER, p_text := NULL::VARCHAR), ",
      "NULL::",
      struct_type,
      "[], NULL::MAP(VARCHAR, ",
      struct_type,
      "), NULL::MAP(",
      key_type,
      ", VARCHAR) ",
      "UNION ALL SELECT 3::INTEGER, ",
      "struct_pack(p_number := 3::INTEGER, p_text := 'value'), ",
      "[]::",
      struct_type,
      "[], map([]::VARCHAR[], []::",
      struct_type,
      "[]), map([]::",
      key_type,
      "[], []::VARCHAR[])",
      ") TO ",
      as.character(DBI::dbQuoteString(con, gsub("\\\\", "/", parquet))),
      " (FORMAT PARQUET)"
    )
  )
  mapped_field <- function(name, type, id, physical) {
    list(
      name = name,
      type = type,
      nullable = TRUE,
      metadata = list(
        "delta.columnMapping.id" = id,
        "delta.columnMapping.physicalName" = physical
      )
    )
  }
  nested_type <- function(number_id, text_id) {
    list(
      type = "struct",
      fields = list(
        mapped_field("number", "integer", number_id, "p_number"),
        mapped_field("text", "string", text_id, "p_text")
      )
    )
  }
  schema <- jsonlite::toJSON(
    list(
      type = "struct",
      fields = list(
        mapped_field("id", "integer", 1L, "p_id"),
        mapped_field("profile", nested_type(3L, 4L), 2L, "p_profile"),
        mapped_field(
          "items",
          list(
            type = "array",
            elementType = nested_type(6L, 7L),
            containsNull = TRUE
          ),
          5L,
          "p_items"
        ),
        mapped_field(
          "attributes",
          list(
            type = "map",
            keyType = "string",
            valueType = nested_type(9L, 10L),
            valueContainsNull = TRUE
          ),
          8L,
          "p_attributes"
        ),
        mapped_field(
          "keyed",
          list(
            type = "map",
            keyType = list(
              type = "struct",
              fields = list(
                mapped_field("marker", "integer", 12L, "p_marker"),
                mapped_field(
                  "nested",
                  nested_type(14L, 15L),
                  13L,
                  "p_nested"
                )
              )
            ),
            valueType = "string",
            valueContainsNull = TRUE
          ),
          11L,
          "p_keyed"
        )
      )
    ),
    auto_unbox = TRUE
  )
  actions <- list(
    list(protocol = list(minReaderVersion = 2L, minWriterVersion = 5L)),
    list(
      metaData = list(
        id = "struct-validity",
        format = list(provider = "parquet", options = list()),
        schemaString = schema,
        partitionColumns = list(),
        configuration = list("delta.columnMapping.mode" = "name")
      )
    ),
    list(add = list(path = "part.parquet", partitionValues = list()))
  )
  writeLines(
    vapply(actions, jsonlite::toJSON, character(1), auto_unbox = TRUE),
    fs::path(log_dir, "00000000000000000000.json"),
    useBytes = TRUE
  )

  result <- fabric_delta_read_staged(table_dir)

  expect_s3_class(result$profile, "fabric_delta_struct_column")
  expect_identical(is.na(result$profile), c(TRUE, FALSE, FALSE))
  expect_true(is.na(result$profile$number[[2L]]))
  expect_true(is.na(result$profile$text[[2L]]))
  expect_identical(
    is.na(result$profile[c(2L, 1L), , drop = FALSE]),
    c(FALSE, TRUE)
  )
  expect_identical(is.na(result$items[[1L]]), c(TRUE, FALSE))
  expect_identical(
    is.na(result$attributes[[1L]]$value),
    c(TRUE, FALSE)
  )
  expect_s3_class(
    result$keyed[[1L]]$key$nested,
    "fabric_delta_struct_column"
  )
  expect_identical(
    is.na(result$keyed[[1L]]$key$nested),
    c(TRUE, FALSE)
  )
  expect_null(result$items[[2L]])
  expect_equal(nrow(result$items[[3L]]), 0L)
  expect_null(result$attributes[[2L]])
  expect_equal(nrow(result$attributes[[3L]]), 0L)
  expect_null(result$keyed[[2L]])
  expect_equal(nrow(result$keyed[[3L]]), 0L)
  tibble_result <- fabric_delta_format_result(result, "tibble")
  expect_identical(is.na(tibble_result$profile), c(TRUE, FALSE, FALSE))

  skip_if_not_installed("arrow")
  skip_if_not_installed("nanoarrow")
  stream <- fabric_delta_format_result(result, "arrow_stream")
  table <- arrow::as_record_batch_reader(stream)$read_table()
  profile <- table$GetColumnByName("profile")
  expect_equal(profile$null_count, 1L)
  expect_true(profile$chunk(0L)$IsNull(0L))
  expect_false(profile$chunk(0L)$IsNull(1L))
  items <- table$GetColumnByName("items")$chunk(0L)
  expect_equal(items$values()$null_count, 1L)
  expect_true(items$values()$IsNull(0L))
  expect_false(items$values()$IsNull(1L))
  attributes <- table$GetColumnByName("attributes")$chunk(0L)
  attribute_values <- attributes$values()$GetFieldByName("value")
  expect_equal(attribute_values$null_count, 1L)
  expect_true(attribute_values$IsNull(0L))
  expect_false(attribute_values$IsNull(1L))
  keyed <- table$GetColumnByName("keyed")$chunk(0L)
  keyed_keys <- keyed$values()$GetFieldByName("key")
  keyed_nested <- keyed_keys$GetFieldByName("nested")
  expect_equal(keyed_nested$null_count, 1L)
  expect_true(keyed_nested$IsNull(0L))
  expect_false(keyed_nested$IsNull(1L))
})

test_that("Delta reader reconstructs top-level and nested void fields", {
  table_dir <- fs::path_temp(paste0("delta-void-", sample.int(1e9, 1)))
  log_dir <- fs::path(table_dir, "_delta_log")
  fs::dir_create(log_dir, recurse = TRUE)
  on.exit(fs::dir_delete(table_dir), add = TRUE)
  parquet <- fs::path(table_dir, "part.parquet")
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  DBI::dbExecute(
    con,
    paste0(
      "COPY (SELECT 1::INTEGER AS id, ",
      "struct_pack(value := 2::INTEGER) AS details) TO ",
      as.character(DBI::dbQuoteString(con, gsub("\\\\", "/", parquet))),
      " (FORMAT PARQUET)"
    )
  )
  schema <- jsonlite::toJSON(
    list(
      type = "struct",
      fields = list(
        list(
          name = "id",
          type = "integer",
          nullable = FALSE,
          metadata = list()
        ),
        list(
          name = "always_null",
          type = "void",
          nullable = TRUE,
          metadata = list()
        ),
        list(
          name = "details",
          type = list(
            type = "struct",
            fields = list(
              list(
                name = "value",
                type = "integer",
                nullable = FALSE,
                metadata = list()
              ),
              list(
                name = "pending",
                type = "void",
                nullable = TRUE,
                metadata = list()
              )
            )
          ),
          nullable = FALSE,
          metadata = list()
        )
      )
    ),
    auto_unbox = TRUE
  )
  actions <- list(
    list(protocol = list(minReaderVersion = 1L, minWriterVersion = 2L)),
    list(
      metaData = list(
        id = "void-table",
        format = list(provider = "parquet", options = list()),
        schemaString = schema,
        partitionColumns = list(),
        configuration = list()
      )
    ),
    list(add = list(path = "part.parquet", partitionValues = list()))
  )
  writeLines(
    vapply(actions, jsonlite::toJSON, character(1), auto_unbox = TRUE),
    fs::path(log_dir, "00000000000000000000.json"),
    useBytes = TRUE
  )

  result <- fabric_delta_read_staged(table_dir)

  expect_named(result, c("id", "always_null", "details"))
  expect_identical(result$id, 1L)
  expect_type(result$always_null, "logical")
  expect_true(is.na(result$always_null))
  expect_s3_class(result$details, "data.frame")
  expect_identical(result$details$value, 2L)
  expect_type(result$details$pending, "logical")
  expect_true(is.na(result$details$pending))
})

test_that("Delta reader preserves exact BIGINT and DECIMAL values", {
  table_dir <- fs::path_temp(paste0("delta-exact-", sample.int(1e9, 1)))
  log_dir <- fs::path(table_dir, "_delta_log")
  fs::dir_create(log_dir, recurse = TRUE)
  on.exit(fs::dir_delete(table_dir), add = TRUE)
  parquet <- fs::path(table_dir, "part.parquet")
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  DBI::dbExecute(
    con,
    paste0(
      "COPY (SELECT ",
      "CAST('9007199254740993' AS BIGINT) AS above_double_limit, ",
      "CAST('9223372036854775807' AS BIGINT) AS maximum_long, ",
      "CAST('12345678901234567890123456789012345678' ",
      "AS DECIMAL(38,0)) AS whole_decimal, ",
      "CAST('123456789012345678901234567890123456.78' ",
      "AS DECIMAL(38,2)) AS scaled_decimal) TO ",
      as.character(DBI::dbQuoteString(con, gsub("\\\\", "/", parquet))),
      " (FORMAT PARQUET)"
    )
  )
  schema <- jsonlite::toJSON(
    list(
      type = "struct",
      fields = list(
        list(
          name = "above_double_limit",
          type = "long",
          nullable = FALSE,
          metadata = list()
        ),
        list(
          name = "maximum_long",
          type = "long",
          nullable = FALSE,
          metadata = list()
        ),
        list(
          name = "whole_decimal",
          type = "decimal(38,0)",
          nullable = FALSE,
          metadata = list()
        ),
        list(
          name = "scaled_decimal",
          type = "decimal(38,2)",
          nullable = FALSE,
          metadata = list()
        )
      )
    ),
    auto_unbox = TRUE
  )
  writeLines(
    c(
      '{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}',
      jsonlite::toJSON(
        list(
          metaData = list(
            id = "table",
            format = list(provider = "parquet", options = list()),
            schemaString = schema,
            partitionColumns = list(),
            configuration = list()
          )
        ),
        auto_unbox = TRUE
      ),
      '{"add":{"path":"part.parquet","partitionValues":{}}}'
    ),
    fs::path(log_dir, "00000000000000000000.json"),
    useBytes = TRUE
  )

  result <- fabric_delta_read_staged(table_dir)

  expect_s3_class(result$above_double_limit, "integer64")
  expect_s3_class(result$maximum_long, "integer64")
  expect_identical(
    as.character(result$above_double_limit),
    "9007199254740993"
  )
  expect_identical(
    as.character(result$maximum_long),
    "9223372036854775807"
  )
  expect_identical(
    result$whole_decimal,
    "12345678901234567890123456789012345678"
  )
  expect_identical(
    result$scaled_decimal,
    "123456789012345678901234567890123456.78"
  )
})

test_that("Delta timestamps carry exact instants in an inexact POSIXct text", {
  # `timestamp` is returned as POSIXct, a binary double counting seconds since
  # the Unix epoch. The documented guarantee is on the *value*: it is the
  # closest double to the stored microsecond. Rendering those microseconds back
  # as text is a known POSIXct weakness, so the documentation warns about it and
  # this test pins both halves of that contract.
  table_dir <- fs::path_temp(paste0("delta-instant-", sample.int(1e9, 1)))
  log_dir <- fs::path(table_dir, "_delta_log")
  fs::dir_create(log_dir, recurse = TRUE)
  on.exit(fs::dir_delete(table_dir), add = TRUE)
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  instants <- c(
    "1900-01-01 00:00:00.000001",
    "1950-06-01 00:00:00.000001",
    "1970-01-01 00:00:00.000001",
    "2037-01-01 00:00:00.000001",
    "2038-01-19 03:14:07.999999",
    "9999-12-31 23:59:59.999999"
  )
  values <- paste(
    sprintf("SELECT TIMESTAMPTZ '%s+00' AS observed_at", instants),
    collapse = " UNION ALL "
  )
  DBI::dbExecute(
    con,
    paste0(
      "COPY (",
      values,
      ") TO ",
      as.character(DBI::dbQuoteString(
        con,
        gsub("\\\\", "/", fs::path(table_dir, "part.parquet"))
      )),
      " (FORMAT PARQUET)"
    )
  )
  schema <- jsonlite::toJSON(
    list(
      type = "struct",
      fields = list(
        list(
          name = "observed_at",
          type = "timestamp",
          nullable = TRUE,
          metadata = list()
        )
      )
    ),
    auto_unbox = TRUE
  )
  writeLines(
    c(
      '{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}',
      jsonlite::toJSON(
        list(
          metaData = list(
            id = "table",
            format = list(provider = "parquet", options = list()),
            schemaString = schema,
            partitionColumns = list(),
            configuration = list()
          )
        ),
        auto_unbox = TRUE
      ),
      '{"add":{"path":"part.parquet","partitionValues":{}}}'
    ),
    fs::path(log_dir, "00000000000000000000.json"),
    useBytes = TRUE
  )

  result <- fabric_delta_read_staged(table_dir)
  expect_s3_class(result$observed_at, "POSIXct")

  # The guarantee: every instant is the closest double to the stored
  # microsecond, so the numeric value is accurate to well under one microsecond
  # across the whole representable range.
  expected <- as.POSIXct(
    instants,
    tz = "UTC",
    format = "%Y-%m-%d %H:%M:%OS"
  )
  expect_identical(as.numeric(result$observed_at), as.numeric(expected))
  expect_true(all(
    abs(as.numeric(result$observed_at) - as.numeric(expected)) < 1e-6
  ))

  # The documented limitation: POSIXct text rendering splits off the fraction by
  # subtraction, which drops low digits away from 1970. Both of these hold today
  # and are the reason the reader documents `timestamp` as text-inexact.
  rendered <- format(result$observed_at, "%Y-%m-%d %H:%M:%OS6", tz = "UTC")
  expect_identical(rendered[[3L]], "1970-01-01 00:00:00.000001")
  expect_identical(rendered[[5L]], "2038-01-19 03:14:07.999999")
  expect_false(identical(rendered[[2L]], instants[[2L]]))
  expect_false(identical(rendered[[4L]], instants[[4L]]))
})

test_that("Delta reader exposes native and shredded Variant values", {
  table_dir <- fs::path_temp(paste0("delta-variant-", sample.int(1e9, 1)))
  log_dir <- fs::path(table_dir, "_delta_log")
  fs::dir_create(log_dir, recurse = TRUE)
  on.exit(fs::dir_delete(table_dir), add = TRUE)
  unshredded <- fs::path(table_dir, "unshredded.parquet")
  shredded <- fs::path(table_dir, "shredded.parquet")
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  DBI::dbExecute(
    con,
    paste0(
      "COPY (",
      "SELECT 1::BIGINT AS event_id, ",
      "{'kind': 'object', 'large': 9007199254740993::BIGINT}::VARIANT ",
      "AS payload UNION ALL ",
      "SELECT 2::BIGINT, NULL::VARIANT UNION ALL ",
      "SELECT 3::BIGINT, ",
      "[1::VARIANT, 'two'::VARIANT, true::VARIANT, NULL::VARIANT]",
      "::VARIANT UNION ALL ",
      "SELECT 4::BIGINT, 9007199254740993::BIGINT::VARIANT",
      " UNION ALL SELECT 6::BIGINT, ",
      "123456789012345678901234567890123456.78::DECIMAL(38,2)::VARIANT",
      ") TO ",
      as.character(DBI::dbQuoteString(
        con,
        gsub("\\\\", "/", unshredded)
      )),
      " (FORMAT PARQUET)"
    )
  )
  DBI::dbExecute(
    con,
    paste0(
      "COPY (SELECT 5::BIGINT AS event_id, ",
      "{'kind': 'shredded', 'large': 7::BIGINT}::VARIANT AS payload) TO ",
      as.character(DBI::dbQuoteString(con, gsub("\\\\", "/", shredded))),
      " (FORMAT PARQUET, SHREDDING {",
      "'payload': 'STRUCT(kind VARCHAR, large BIGINT)'",
      "})"
    )
  )
  alias_dir <- fs::path(table_dir, "path-alias")
  fs::dir_create(alias_dir)
  aliased_unshredded <- file.path(
    alias_dir,
    "..",
    "unshredded.parquet"
  )
  canonical_unshredded <- gsub(
    "\\\\",
    "/",
    normalizePath(unshredded, mustWork = TRUE)
  )
  expect_false(identical(
    gsub("\\\\", "/", aliased_unshredded),
    canonical_unshredded
  ))
  aliased_masks <- fabric_delta_variant_null_masks(
    aliased_unshredded,
    fields = list(list(
      name = "payload",
      type = "variant",
      metadata = list()
    )),
    schema = list(columnMappingMode = "none")
  )
  expect_named(aliased_masks$payload, canonical_unshredded)
  id_masks <- fabric_delta_variant_null_masks(
    unshredded,
    fields = list(list(
      name = "payload",
      type = "variant",
      metadata = list(
        "delta.columnMapping.id" = 17L,
        "delta.columnMapping.physicalName" = "not-the-file-name"
      )
    )),
    schema = list(columnMappingMode = "id"),
    id_mappings = list(c("17" = "payload"))
  )
  expect_identical(
    unname(id_masks$payload[[canonical_unshredded]]),
    rep(FALSE, 5L)
  )
  missing_id_masks <- fabric_delta_variant_null_masks(
    unshredded,
    fields = list(list(
      name = "payload",
      type = "variant",
      metadata = list(
        "delta.columnMapping.id" = 17L,
        "delta.columnMapping.physicalName" = "payload"
      )
    )),
    schema = list(columnMappingMode = "id"),
    id_mappings = list(c("17" = "missing-from-file"))
  )
  expect_true(all(missing_id_masks$payload[[canonical_unshredded]]))

  schema <- jsonlite::toJSON(
    list(
      type = "struct",
      fields = list(
        list(
          name = "event_id",
          type = "long",
          nullable = FALSE,
          metadata = list()
        ),
        list(
          name = "payload",
          type = "variant",
          nullable = TRUE,
          metadata = list()
        )
      )
    ),
    auto_unbox = TRUE
  )
  actions <- list(
    list(
      protocol = list(
        minReaderVersion = 3L,
        minWriterVersion = 7L,
        readerFeatures = list(
          "variantType",
          "variantShredding-preview"
        ),
        writerFeatures = list(
          "variantType",
          "variantShredding-preview"
        )
      )
    ),
    list(
      metaData = list(
        id = "variant-table",
        format = list(provider = "parquet", options = list()),
        schemaString = schema,
        partitionColumns = list(),
        configuration = list()
      )
    ),
    list(add = list(path = "unshredded.parquet", partitionValues = list())),
    list(add = list(path = "shredded.parquet", partitionValues = list()))
  )
  writeLines(
    vapply(actions, jsonlite::toJSON, character(1), auto_unbox = TRUE),
    fs::path(log_dir, "00000000000000000000.json"),
    useBytes = TRUE
  )

  result <- fabric_delta_read_staged(table_dir)
  result <- result[order(as.numeric(result$event_id)), ]

  expect_type(result$payload, "list")
  expect_true(all(vapply(
    result$payload,
    inherits,
    logical(1),
    "fabric_delta_variant"
  )))
  expect_match(result$payload[[1L]]$type, "^OBJECT")
  expect_match(result$payload[[1L]]$display, "9007199254740993", fixed = TRUE)
  expect_identical(result$payload[[2L]]$type, "VARIANT_NULL")
  expect_identical(result$payload[[2L]]$value, as.raw(0L))
  expect_match(result$payload[[3L]]$type, "^ARRAY")
  expect_identical(result$payload[[4L]]$type, "INT64")
  expect_identical(result$payload[[4L]]$display, "9007199254740993")
  expect_match(result$payload[[5L]]$type, "^OBJECT")
  expect_match(result$payload[[5L]]$display, "shredded", fixed = TRUE)
  expect_identical(result$payload[[6L]]$type, "DECIMAL(38, 2)")
  expect_identical(
    result$payload[[6L]]$display,
    "123456789012345678901234567890123456.78"
  )
  expect_type(result$payload[[6L]]$metadata, "raw")
  expect_type(result$payload[[6L]]$value, "raw")
})

test_that("Delta partition serialization treats empty strings as null", {
  snapshot <- list(
    active = c("missing.parquet", "present.parquet"),
    files = list(
      "missing.parquet" = list(
        partitionValues = list(
          event_date = "",
          active = ""
        )
      ),
      "present.parquet" = list(
        partitionValues = list(
          event_date = "2026-01-02",
          active = "false"
        )
      )
    )
  )
  schema <- list(partitionColumns = c("event_date", "active"))

  mapping <- fabric_delta_partition_mapping(
    snapshot,
    c("missing.parquet", "present.parquet"),
    schema
  )

  expect_equal(
    mapping$fabric_delta_partition_1,
    c(NA_character_, "2026-01-02")
  )
  expect_equal(
    mapping$fabric_delta_partition_2,
    c(NA_character_, "false")
  )
})

test_that("Delta timestamp partitions use the writer timezone", {
  table_dir <- fs::path_temp(paste0(
    "delta-timestamp-partition-",
    sample.int(1e9, 1)
  ))
  log_dir <- fs::path(table_dir, "_delta_log")
  fs::dir_create(log_dir, recurse = TRUE)
  on.exit(fs::dir_delete(table_dir), add = TRUE)
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  for (index in 1:2) {
    path <- fs::path(table_dir, paste0("part-", index, ".parquet"))
    DBI::dbExecute(
      con,
      paste0(
        "COPY (SELECT ",
        index,
        "::INTEGER AS id) TO ",
        as.character(DBI::dbQuoteString(con, gsub("\\\\", "/", path))),
        " (FORMAT PARQUET)"
      )
    )
  }
  schema <- jsonlite::toJSON(
    list(
      type = "struct",
      fields = list(
        list(
          name = "id",
          type = "integer",
          nullable = FALSE,
          metadata = list()
        ),
        list(
          name = "recorded_at",
          type = "timestamp",
          nullable = FALSE,
          metadata = list()
        )
      )
    ),
    auto_unbox = TRUE
  )
  actions <- list(
    list(protocol = list(minReaderVersion = 1L, minWriterVersion = 2L)),
    list(
      metaData = list(
        id = "timestamp-partition-table",
        format = list(provider = "parquet", options = list()),
        schemaString = schema,
        partitionColumns = list("recorded_at"),
        configuration = list()
      )
    ),
    list(
      add = list(
        path = "part-1.parquet",
        partitionValues = list(recorded_at = "2026-01-01 12:00:00")
      )
    ),
    list(
      add = list(
        path = "part-2.parquet",
        partitionValues = list(recorded_at = "2026-07-01 12:00:00")
      )
    )
  )
  writeLines(
    vapply(actions, jsonlite::toJSON, character(1), auto_unbox = TRUE),
    fs::path(log_dir, "00000000000000000000.json"),
    useBytes = TRUE
  )

  expect_error(
    fabric_delta_read_staged(table_dir),
    "require timestamp_partition_timezone",
    fixed = TRUE
  )
  expect_error(
    fabric_delta_read_staged(
      table_dir,
      timestamp_partition_timezone = "Not/A-Timezone"
    ),
    "not a recognized IANA timezone",
    fixed = TRUE
  )

  result <- fabric_delta_read_staged(
    table_dir,
    timestamp_partition_timezone = "Europe/Amsterdam"
  )
  result <- result[order(result$id), ]
  expect_s3_class(result$recorded_at, "POSIXct")
  expect_equal(
    as.numeric(result$recorded_at),
    as.numeric(as.POSIXct(
      c("2026-01-01 11:00:00", "2026-07-01 10:00:00"),
      tz = "UTC"
    ))
  )
})

test_that("Delta metadata schemas reject malformed and ambiguous fields", {
  expect_error(
    fabric_delta_schema(list(schemaString = "{not-json")),
    "Could not parse the Delta metadata schemaString",
    fixed = TRUE
  )
  expect_error(
    fabric_delta_schema(list(schemaString = '{"type":"array","fields":[]}')),
    "must describe a struct",
    fixed = TRUE
  )

  duplicate <- jsonlite::toJSON(
    list(
      type = "struct",
      fields = list(
        list(name = "Value", type = "string"),
        list(name = "value", type = "string")
      )
    ),
    auto_unbox = TRUE
  )
  expect_error(
    fabric_delta_schema(list(schemaString = duplicate)),
    "missing or duplicate field names",
    fixed = TRUE
  )

  valid <- jsonlite::toJSON(
    list(
      type = "struct",
      fields = list(list(name = "id", type = "long"))
    ),
    auto_unbox = TRUE
  )
  expect_error(
    fabric_delta_schema(list(
      schemaString = valid,
      partitionColumns = list("missing")
    )),
    "unknown partition column(s): missing",
    fixed = TRUE
  )

  nested_duplicate <- jsonlite::toJSON(
    list(
      type = "struct",
      fields = list(list(
        name = "profile",
        type = list(
          type = "struct",
          fields = list(
            list(name = "Value", type = "string"),
            list(name = "value", type = "string")
          )
        )
      ))
    ),
    auto_unbox = TRUE
  )
  expect_error(
    fabric_delta_schema(list(schemaString = nested_duplicate)),
    "struct at <root>.profile contains missing or duplicate field names",
    fixed = TRUE
  )

  missing_element <- jsonlite::toJSON(
    list(
      type = "struct",
      fields = list(list(
        name = "items",
        type = list(type = "array", containsNull = TRUE)
      ))
    ),
    auto_unbox = TRUE
  )
  expect_error(
    fabric_delta_schema(list(schemaString = missing_element)),
    "array at <root>.items has no elementType",
    fixed = TRUE
  )

  missing_map_value <- jsonlite::toJSON(
    list(
      type = "struct",
      fields = list(list(
        name = "lookup",
        type = list(type = "map", keyType = "string")
      ))
    ),
    auto_unbox = TRUE
  )
  expect_error(
    fabric_delta_schema(list(schemaString = missing_map_value)),
    "map at <root>.lookup has no key/value type",
    fixed = TRUE
  )

  expect_error(
    fabric_delta_schema(list(
      schemaString = valid,
      partitionColumns = list("id", "ID")
    )),
    "partitionColumns contains invalid or duplicate names",
    fixed = TRUE
  )
})

test_that("Delta metadata accepts only supported Parquet data formats", {
  expect_invisible(fabric_delta_validate_metadata_format(list(
    format = list(provider = "parquet", options = list())
  )))
  expect_error(
    fabric_delta_validate_metadata_format(list()),
    "valid data format",
    fixed = TRUE
  )
  expect_error(
    fabric_delta_validate_metadata_format(list(
      format = list(provider = "orc", options = list())
    )),
    "Delta data format orc",
    fixed = TRUE,
    class = "fabric_delta_unsupported_error"
  )
  expect_error(
    fabric_delta_validate_metadata_format(list(
      format = list(
        provider = "parquet",
        options = list(compression = "gzip")
      )
    )),
    "Delta Parquet format options",
    fixed = TRUE,
    class = "fabric_delta_unsupported_error"
  )
})

test_that("Delta commits reject mutually reconciling actions", {
  path <- "00000000000000000001.json"
  protocol <- list(
    protocol = list(
      minReaderVersion = 1L,
      minWriterVersion = 2L
    )
  )
  metadata <- list(metaData = list(id = "table"))
  expect_error(
    fabric_delta_validate_commit_actions(list(protocol, protocol), path),
    "multiple protocol actions",
    fixed = TRUE
  )
  expect_error(
    fabric_delta_validate_commit_actions(list(metadata, metadata), path),
    "multiple metadata actions",
    fixed = TRUE
  )
  transaction <- list(txn = list(appId = "writer", version = 1L))
  expect_error(
    fabric_delta_validate_commit_actions(
      list(transaction, transaction),
      path
    ),
    "duplicate transaction actions",
    fixed = TRUE
  )
  expect_error(
    fabric_delta_validate_commit_actions(
      list(
        list(add = list(path = "part.parquet")),
        list(remove = list(path = "part.parquet"))
      ),
      path
    ),
    "conflicting file actions",
    fixed = TRUE
  )
  expect_invisible(fabric_delta_validate_commit_actions(
    list(
      list(
        add = list(
          path = "part.parquet",
          deletionVector = list(
            storageType = "i",
            pathOrInlineDv = "first"
          )
        )
      ),
      list(
        remove = list(
          path = "part.parquet",
          deletionVector = list(
            storageType = "i",
            pathOrInlineDv = "second"
          )
        )
      )
    ),
    path
  ))
  for (action_name in c("add", "remove")) {
    actions <- lapply(c("first", "second"), function(dv) {
      action <- list(
        path = "part.parquet",
        deletionVector = list(
          storageType = "i",
          pathOrInlineDv = dv
        )
      )
      stats::setNames(list(action), action_name)
    })
    expect_error(
      fabric_delta_validate_commit_actions(actions, path),
      paste0("multiple ", action_name, " actions for one file path"),
      fixed = TRUE
    )
  }
  expect_error(
    fabric_delta_validate_checkpoint_actions(list(protocol, protocol)),
    "multiple protocol actions",
    fixed = TRUE
  )
})

test_that("Delta reader applies schema projection and log partition values", {
  table_dir <- fs::path_temp(paste0("delta-schema-", sample.int(1e9, 1)))
  log_dir <- fs::path(table_dir, "_delta_log")
  data_dir <- fs::path(table_dir, "not-a-hive-partition")
  fs::dir_create(log_dir, recurse = TRUE)
  fs::dir_create(data_dir, recurse = TRUE)
  on.exit(fs::dir_delete(table_dir), add = TRUE)
  parquet <- fs::path(data_dir, "part.parquet")
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  parquet_literal <- as.character(DBI::dbQuoteString(
    con,
    gsub("\\\\", "/", parquet)
  ))
  DBI::dbExecute(
    con,
    paste0(
      "COPY (SELECT 1::BIGINT AS id, 'obsolete' AS dropped) TO ",
      parquet_literal,
      " (FORMAT PARQUET)"
    )
  )
  schema <- jsonlite::toJSON(
    list(
      type = "struct",
      fields = list(
        list(name = "id", type = "long", nullable = FALSE, metadata = list()),
        list(
          name = "added",
          type = "string",
          nullable = TRUE,
          metadata = list()
        ),
        list(
          name = "category",
          type = "string",
          nullable = TRUE,
          metadata = list()
        )
      )
    ),
    auto_unbox = TRUE
  )
  writeLines(
    c(
      '{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}',
      jsonlite::toJSON(
        list(
          metaData = list(
            id = "table",
            format = list(provider = "parquet", options = list()),
            schemaString = schema,
            partitionColumns = list("category"),
            configuration = list()
          )
        ),
        auto_unbox = TRUE
      ),
      jsonlite::toJSON(
        list(
          add = list(
            path = "not-a-hive-partition/part.parquet",
            partitionValues = list(category = "from-log")
          )
        ),
        auto_unbox = TRUE
      )
    ),
    fs::path(log_dir, "00000000000000000000.json"),
    useBytes = TRUE
  )

  result <- fabric_delta_read_staged(table_dir)

  expect_equal(names(result), c("id", "added", "category"))
  expect_equal(as.numeric(result$id), 1)
  expect_true(is.na(result$added))
  expect_equal(result$category, "from-log")
  expect_false("dropped" %in% names(result))
})

test_that("Delta reader decodes typed partition values in UTC", {
  table_dir <- fs::path_temp(paste0(
    "delta-typed-partitions-",
    sample.int(1e9, 1)
  ))
  log_dir <- fs::path(table_dir, "_delta_log")
  fs::dir_create(log_dir, recurse = TRUE)
  on.exit(fs::dir_delete(table_dir), add = TRUE)
  parquet <- fs::path(table_dir, "part.parquet")
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  DBI::dbExecute(
    con,
    paste0(
      "COPY (SELECT 1::INTEGER AS id) TO ",
      as.character(DBI::dbQuoteString(con, gsub("\\\\", "/", parquet))),
      " (FORMAT PARQUET)"
    )
  )
  fields <- list(
    list(name = "id", type = "integer", nullable = FALSE, metadata = list()),
    list(
      name = "event_time",
      type = "timestamp",
      nullable = TRUE,
      metadata = list()
    ),
    list(
      name = "local_time",
      type = "timestamp_ntz",
      nullable = TRUE,
      metadata = list()
    ),
    list(
      name = "amount",
      type = "decimal(8,2)",
      nullable = TRUE,
      metadata = list()
    ),
    list(
      name = "payload",
      type = "binary",
      nullable = TRUE,
      metadata = list()
    )
  )
  schema <- jsonlite::toJSON(
    list(type = "struct", fields = fields),
    auto_unbox = TRUE
  )
  actions <- list(
    list(
      protocol = list(
        minReaderVersion = 3L,
        minWriterVersion = 7L,
        readerFeatures = list("timestampNtz"),
        writerFeatures = list("timestampNtz")
      )
    ),
    list(
      metaData = list(
        id = "table",
        format = list(provider = "parquet", options = list()),
        schemaString = schema,
        partitionColumns = list(
          "event_time",
          "local_time",
          "amount",
          "payload"
        ),
        configuration = list()
      )
    ),
    list(
      add = list(
        path = "part.parquet",
        partitionValues = list(
          event_time = "2026-01-01T12:34:56.123456Z",
          local_time = "2026-07-28 09:08:07.654321",
          amount = "12.30",
          payload = rawToChar(as.raw(c(1L, 2L, 3L)))
        )
      )
    )
  )
  writeLines(
    vapply(
      actions,
      jsonlite::toJSON,
      character(1),
      auto_unbox = TRUE
    ),
    fs::path(log_dir, "00000000000000000000.json"),
    useBytes = TRUE
  )
  old_timezone <- Sys.getenv("TZ", unset = NA_character_)
  Sys.setenv(TZ = "Pacific/Auckland")
  on.exit(
    if (is.na(old_timezone)) {
      Sys.unsetenv("TZ")
    } else {
      Sys.setenv(TZ = old_timezone)
    },
    add = TRUE
  )

  result <- fabric_delta_read_staged(table_dir)

  expect_equal(
    as.numeric(result$event_time),
    as.numeric(as.POSIXct("2026-01-01 12:34:56.123456", tz = "UTC")),
    tolerance = 1e-6
  )
  expect_s3_class(result$local_time, "fabric_delta_timestamp_ntz")
  expect_identical(
    unclass(result$local_time),
    "2026-07-28 09:08:07.654321"
  )
  expect_identical(
    format(result$local_time, tz = "America/Los_Angeles"),
    "2026-07-28 09:08:07.654321"
  )
  localized <- as.POSIXct(result$local_time, tz = "Pacific/Auckland")
  expect_identical(
    format(localized, "%Y-%m-%d %H:%M", tz = "Pacific/Auckland"),
    "2026-07-28 09:08"
  )
  expect_equal(
    as.numeric(localized) %% 60,
    7.654321,
    tolerance = 1e-6
  )
  expect_identical(result$amount, "12.30")
  expect_identical(result$payload[[1L]], as.raw(c(1L, 2L, 3L)))

  skip_if_not_installed("arrow")
  skip_if_not_installed("nanoarrow")
  stream <- fabric_delta_format_result(result["local_time"], "arrow_stream")
  table <- arrow::as_record_batch_reader(stream)$read_table()
  expect_identical(
    table$schema$GetFieldByName("local_time")$type$ToString(),
    "timestamp[us]"
  )
})

test_that("Delta reader supports a physical filename column", {
  table_dir <- fs::path_temp(paste0("delta-filename-", sample.int(1e9, 1)))
  log_dir <- fs::path(table_dir, "_delta_log")
  fs::dir_create(log_dir, recurse = TRUE)
  on.exit(fs::dir_delete(table_dir), add = TRUE)
  parquet <- fs::path(table_dir, "part.parquet")
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  parquet_literal <- as.character(DBI::dbQuoteString(
    con,
    gsub("\\\\", "/", parquet)
  ))
  DBI::dbExecute(
    con,
    paste0(
      "COPY (SELECT 'value.csv' AS filename) TO ",
      parquet_literal,
      " (FORMAT PARQUET)"
    )
  )
  schema <- jsonlite::toJSON(
    list(
      type = "struct",
      fields = list(list(
        name = "filename",
        type = "string",
        nullable = FALSE,
        metadata = list()
      ))
    ),
    auto_unbox = TRUE
  )
  writeLines(
    c(
      '{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}',
      jsonlite::toJSON(
        list(
          metaData = list(
            id = "table",
            format = list(provider = "parquet", options = list()),
            schemaString = schema,
            partitionColumns = list(),
            configuration = list()
          )
        ),
        auto_unbox = TRUE
      ),
      '{"add":{"path":"part.parquet","partitionValues":{}}}'
    ),
    fs::path(log_dir, "00000000000000000000.json"),
    useBytes = TRUE
  )

  result <- fabric_delta_read_staged(table_dir)

  expect_equal(result$filename, "value.csv")
})

test_that("Delta checkpoints allow earlier JSON commits to be absent", {
  table_dir <- fs::path_temp(paste0("delta-checkpoint-", sample.int(1e9, 1)))
  log_dir <- fs::path(table_dir, "_delta_log")
  fs::dir_create(log_dir, recurse = TRUE)
  on.exit(fs::dir_delete(table_dir), add = TRUE)
  checkpoint_path <- fs::path(
    log_dir,
    "00000000000000000010.checkpoint.parquet"
  )
  writeBin(raw(), checkpoint_path)
  writeLines(
    '{"add":{"path":"category=B/after-checkpoint.parquet"}}',
    fs::path(log_dir, "00000000000000000011.json"),
    useBytes = TRUE
  )

  local_mocked_bindings(
    fabric_delta_read_checkpoint = function(paths) {
      expect_equal(as.character(paths), as.character(checkpoint_path))
      list(
        add = list(
          path = "category=A/from-checkpoint.parquet",
          deletionVector = list(storageType = NA_character_)
        ),
        remove = list(path = "category=A/from-checkpoint.parquet"),
        protocol = list(
          minReaderVersion = 1L,
          minWriterVersion = 2L
        ),
        metaData = list(
          id = "table-id",
          format = list(provider = "parquet", options = list()),
          schemaString = '{"type":"struct","fields":[]}',
          partitionColumns = list(list()),
          configuration = list(data.frame(
            key = character(),
            value = character()
          ))
        )
      )
    }
  )

  snapshot <- fabric_delta_resolve_snapshot(table_dir)
  expect_equal(snapshot$checkpoint_version, 10)
  expect_equal(snapshot$version, 11)
  expect_setequal(
    snapshot$active,
    c(
      "category=A/from-checkpoint.parquet",
      "category=B/after-checkpoint.parquet"
    )
  )
})

test_that("Delta multipart checkpoints require every declared part", {
  log_dir <- fs::path_temp(
    paste0("delta-multipart-", sample.int(1e9, 1))
  )
  fs::dir_create(log_dir, recurse = TRUE)
  on.exit(fs::dir_delete(log_dir), add = TRUE)

  complete <- fs::path(
    log_dir,
    sprintf(
      "00000000000000000010.checkpoint.%010d.0000000003.parquet",
      c(3L, 1L, 2L)
    )
  )
  purrr::walk(complete, function(path) writeBin(raw(), path))
  incomplete <- fs::path(
    log_dir,
    c(
      "00000000000000000011.checkpoint.0000000001.0000000003.parquet",
      "00000000000000000011.checkpoint.0000000003.0000000003.parquet"
    )
  )
  purrr::walk(incomplete, function(path) writeBin(raw(), path))

  sets <- fabric_delta_checkpoint_sets(c(incomplete, complete))

  expect_length(sets, 1L)
  expect_equal(sets[[1L]]$version, 10)
  expect_equal(
    basename(sets[[1L]]$paths),
    sprintf(
      "00000000000000000010.checkpoint.%010d.0000000003.parquet",
      1:3
    )
  )
})

test_that("Delta reader replays a real multipart Parquet checkpoint", {
  table_dir <- fs::path_temp(paste0(
    "delta-multipart-read-",
    sample.int(1e9, 1)
  ))
  log_dir <- fs::path(table_dir, "_delta_log")
  fs::dir_create(log_dir, recurse = TRUE)
  on.exit(fs::dir_delete(table_dir), add = TRUE)
  parts <- fs::path(
    log_dir,
    sprintf(
      "00000000000000000010.checkpoint.%010d.0000000002.parquet",
      1:2
    )
  )
  schema <- jsonlite::toJSON(
    list(
      type = "struct",
      fields = list(list(
        name = "id",
        type = "integer",
        nullable = FALSE,
        metadata = list()
      ))
    ),
    auto_unbox = TRUE
  )
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  quote_string <- function(value) {
    as.character(DBI::dbQuoteString(con, value))
  }
  DBI::dbExecute(
    con,
    paste0(
      "COPY (SELECT ",
      "struct_pack(minReaderVersion := 1, minWriterVersion := 2) ",
      "AS protocol, ",
      "struct_pack(id := 'multipart-table', schemaString := ",
      quote_string(schema),
      ", format := struct_pack(provider := 'parquet', ",
      "options := map([]::VARCHAR[], []::VARCHAR[]))",
      ", partitionColumns := []::VARCHAR[], ",
      "configuration := map([]::VARCHAR[], []::VARCHAR[])) AS metaData) TO ",
      quote_string(gsub("\\\\", "/", parts[[1L]])),
      " (FORMAT PARQUET)"
    )
  )
  DBI::dbExecute(
    con,
    paste0(
      "COPY (SELECT struct_pack(path := 'part.parquet', ",
      "partitionValues := map([]::VARCHAR[], []::VARCHAR[])) AS add) TO ",
      quote_string(gsub("\\\\", "/", parts[[2L]])),
      " (FORMAT PARQUET)"
    )
  )

  snapshot <- fabric_delta_resolve_snapshot(table_dir)

  expect_equal(snapshot$version, 10)
  expect_equal(snapshot$checkpoint_version, 10)
  expect_identical(snapshot$active, "part.parquet")
  expect_identical(snapshot$metadata$format$provider, "parquet")
  expect_identical(snapshot$metadata$schemaString, as.character(schema))
})

test_that("Delta Parquet checkpoints preserve binary partition bytes", {
  checkpoint <- tempfile("binary-partitions-", fileext = ".checkpoint.parquet")
  on.exit(unlink(checkpoint), add = TRUE)
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  quoted_checkpoint <- as.character(DBI::dbQuoteString(
    con,
    gsub("\\\\", "/", checkpoint)
  ))
  DBI::dbExecute(
    con,
    paste0(
      "COPY (",
      "SELECT struct_pack(path := 'zero.parquet', ",
      "partitionValues := map(['binary_part'], [chr(0)])) AS add ",
      "UNION ALL SELECT struct_pack(path := 'high.parquet', ",
      "partitionValues := map(['binary_part'], [chr(128)])) AS add ",
      "UNION ALL SELECT struct_pack(path := 'max.parquet', ",
      "partitionValues := map(['binary_part'], [chr(255)])) AS add",
      ") TO ",
      quoted_checkpoint,
      " (FORMAT PARQUET)"
    )
  )

  checkpoint_actions <- fabric_delta_read_checkpoint(checkpoint)
  state <- fabric_delta_apply_checkpoint(
    list(
      active = character(),
      files = list(),
      has_deletion_vectors = FALSE
    ),
    checkpoint_actions
  )
  schema <- list(
    fields = list(list(name = "binary_part", type = "binary")),
    partitionColumns = "binary_part",
    columnMappingMode = "none"
  )
  paths <- fs::path_temp(state$active)

  mapping <- fabric_delta_partition_mapping(state, paths, schema)

  expect_identical(
    unclass(mapping$fabric_delta_partition_1),
    list(as.raw(0L), as.raw(128L), as.raw(255L))
  )
})

test_that("Delta UUID checkpoints replay V2 Parquet sidecars", {
  table_dir <- fs::path_temp(paste0("delta-v2-", sample.int(1e9, 1)))
  log_dir <- fs::path(table_dir, "_delta_log")
  sidecar_dir <- fs::path(log_dir, "_sidecars")
  fs::dir_create(sidecar_dir, recurse = TRUE)
  on.exit(fs::dir_delete(table_dir), add = TRUE)
  uuid <- "80a083e8-7026-4e79-81be-64bd76c43a11"
  sidecar_name <- "016ae953-37a9-438e-8683-9a9a4a79a395.parquet"
  checkpoint <- fs::path(
    log_dir,
    paste0("00000000000000000010.checkpoint.", uuid, ".json")
  )
  schema <- jsonlite::toJSON(
    list(
      type = "struct",
      fields = list(list(
        name = "id",
        type = "integer",
        nullable = FALSE,
        metadata = list()
      ))
    ),
    auto_unbox = TRUE
  )
  actions <- list(
    list(checkpointMetadata = list(version = 10L, tags = list())),
    list(
      protocol = list(
        minReaderVersion = 3L,
        minWriterVersion = 7L,
        readerFeatures = list("v2Checkpoint"),
        writerFeatures = list("v2Checkpoint")
      )
    ),
    list(
      metaData = list(
        id = "v2-table",
        format = list(provider = "parquet", options = list()),
        schemaString = schema,
        partitionColumns = list(),
        configuration = list()
      )
    ),
    list(
      sidecar = list(
        path = sidecar_name,
        sizeInBytes = 1L,
        modificationTime = 1L,
        tags = list()
      )
    )
  )
  writeLines(
    vapply(actions, jsonlite::toJSON, character(1), auto_unbox = TRUE),
    checkpoint,
    useBytes = TRUE
  )
  sidecar <- fs::path(sidecar_dir, sidecar_name)
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  DBI::dbExecute(
    con,
    paste0(
      "COPY (SELECT struct_pack(",
      "path := 'part.parquet', ",
      "partitionValues := map([], [])) AS add) TO ",
      as.character(DBI::dbQuoteString(con, gsub("\\\\", "/", sidecar))),
      " (FORMAT PARQUET)"
    )
  )

  sets <- fabric_delta_checkpoint_sets(checkpoint)
  expect_length(sets, 1L)
  expect_true(sets[[1L]]$v2_named)
  expect_equal(
    fabric_delta_checkpoint_sidecar_paths(checkpoint),
    sidecar_name
  )
  actions[[4L]]$sidecar$path <- paste0(
    "_delta_log%2F_sidecars%2F",
    sidecar_name
  )
  writeLines(
    vapply(actions, jsonlite::toJSON, character(1), auto_unbox = TRUE),
    checkpoint,
    useBytes = TRUE
  )
  expect_identical(
    fabric_delta_checkpoint_sidecar_paths(checkpoint),
    sidecar_name
  )
  workspace <- "11111111-1111-1111-1111-111111111111"
  item <- "22222222-2222-2222-2222-222222222222"
  actions[[4L]]$sidecar$path <- paste0(
    "abfss://",
    workspace,
    "@onelake.dfs.fabric.microsoft.com/",
    item,
    "/Tables/dbo/table/_delta_log/_sidecars/",
    sidecar_name
  )
  writeLines(
    vapply(actions, jsonlite::toJSON, character(1), auto_unbox = TRUE),
    checkpoint,
    useBytes = TRUE
  )
  expect_identical(
    fabric_delta_checkpoint_sidecar_paths(
      checkpoint,
      target = list(workspace = workspace, item = item),
      table_dir = "Tables/dbo/table"
    ),
    sidecar_name
  )
  expect_error(
    fabric_delta_checkpoint_sidecar_paths(
      checkpoint,
      target = list(
        workspace = workspace,
        item = "33333333-3333-3333-3333-333333333333"
      ),
      table_dir = "Tables/dbo/table"
    ),
    "outside its table",
    fixed = TRUE
  )
  actions[[4L]]$sidecar$path <- sidecar_name
  writeLines(
    vapply(actions, jsonlite::toJSON, character(1), auto_unbox = TRUE),
    checkpoint,
    useBytes = TRUE
  )
  snapshot <- fabric_delta_resolve_snapshot(table_dir)
  expect_equal(snapshot$version, 10)
  expect_equal(snapshot$checkpoint_version, 10)
  expect_equal(snapshot$active, "part.parquet")
  expect_identical(snapshot$metadata$format$provider, "parquet")
  expect_identical(
    unlist(snapshot$protocol$writerFeatures, use.names = FALSE),
    "v2Checkpoint"
  )

  actions[[2L]]$protocol$readerFeatures <- list()
  actions[[2L]]$protocol$writerFeatures <- list()
  writeLines(
    vapply(actions, jsonlite::toJSON, character(1), auto_unbox = TRUE),
    checkpoint,
    useBytes = TRUE
  )
  expect_error(
    fabric_delta_resolve_snapshot(table_dir),
    "requires the v2Checkpoint reader feature",
    fixed = TRUE
  )
  actions[[2L]]$protocol$readerFeatures <- list("v2Checkpoint")
  actions[[2L]]$protocol$writerFeatures <- list("v2Checkpoint")
  actions[[1L]]$checkpointMetadata$version <- 9L
  writeLines(
    vapply(actions, jsonlite::toJSON, character(1), auto_unbox = TRUE),
    checkpoint,
    useBytes = TRUE
  )
  expect_error(
    fabric_delta_resolve_snapshot(table_dir),
    "exactly one matching checkpointMetadata",
    fixed = TRUE
  )
})

test_that("V2 sidecars may carry unpopulated non-file action columns", {
  # The protocol restricts which *actions* a sidecar carries, not its Parquet
  # schema. A writer that emits the full checkpoint schema with the non-file
  # columns left null is conformant and must still be read.
  sidecar_dir <- fs::path_temp(paste0("delta-sidecar-", sample.int(1e9, 1)))
  fs::dir_create(fs::path(sidecar_dir, "_sidecars"), recurse = TRUE)
  on.exit(fs::dir_delete(sidecar_dir), add = TRUE)
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  write_sidecar <- function(name, select) {
    path <- fs::path(sidecar_dir, "_sidecars", name)
    DBI::dbExecute(
      con,
      paste0(
        "COPY (",
        select,
        ") TO ",
        as.character(DBI::dbQuoteString(con, gsub("\\\\", "/", path))),
        " (FORMAT PARQUET)"
      )
    )
    path
  }

  add_action <- paste0(
    "struct_pack(path := 'part.parquet', ",
    "partitionValues := map([], [])) AS add"
  )
  null_columns <- paste0(
    "CAST(NULL AS STRUCT(minReaderVersion INTEGER)) AS protocol, ",
    "CAST(NULL AS STRUCT(id VARCHAR)) AS metaData, ",
    "CAST(NULL AS STRUCT(version BIGINT)) AS checkpointMetadata"
  )
  tolerated <- write_sidecar(
    "tolerated.parquet",
    paste0("SELECT ", add_action, ", ", null_columns)
  )
  checkpoint <- fabric_delta_read_checkpoint(tolerated)
  expect_identical(checkpoint$add$path, "part.parquet")

  populated <- write_sidecar(
    "populated.parquet",
    paste0(
      "SELECT ",
      add_action,
      ", CAST(NULL AS STRUCT(minReaderVersion INTEGER)) AS protocol, ",
      "CAST(NULL AS STRUCT(id VARCHAR)) AS metaData, ",
      "struct_pack(version := 10::BIGINT) AS checkpointMetadata"
    )
  )
  expect_error(
    fabric_delta_read_checkpoint(populated),
    "sidecar contains non-file actions",
    fixed = TRUE
  )
})

test_that("Delta reader falls back across same-version UUID checkpoints", {
  table_dir <- fs::path_temp(paste0("delta-v2-fallback-", sample.int(1e9, 1)))
  log_dir <- fs::path(table_dir, "_delta_log")
  fs::dir_create(log_dir, recurse = TRUE)
  on.exit(fs::dir_delete(table_dir), add = TRUE)
  schema <- jsonlite::toJSON(
    list(
      type = "struct",
      fields = list(list(
        name = "id",
        type = "integer",
        nullable = FALSE,
        metadata = list()
      ))
    ),
    auto_unbox = TRUE
  )
  common <- list(
    list(checkpointMetadata = list(version = 10L, tags = list())),
    list(
      protocol = list(
        minReaderVersion = 3L,
        minWriterVersion = 7L,
        readerFeatures = list("v2Checkpoint"),
        writerFeatures = list("v2Checkpoint")
      )
    ),
    list(
      metaData = list(
        id = "fallback-table",
        format = list(provider = "parquet", options = list()),
        schemaString = schema,
        partitionColumns = list(),
        configuration = list()
      )
    )
  )
  broken <- c(
    common,
    list(list(
      sidecar = list(
        path = "missing.parquet",
        sizeInBytes = 1L,
        modificationTime = 1L
      )
    ))
  )
  valid <- c(
    common,
    list(list(
      add = list(
        path = "part.parquet",
        partitionValues = list()
      )
    ))
  )
  broken_path <- fs::path(
    log_dir,
    paste0(
      "00000000000000000010.checkpoint.",
      "00000000-0000-0000-0000-000000000001.json"
    )
  )
  valid_path <- fs::path(
    log_dir,
    paste0(
      "00000000000000000010.checkpoint.",
      "ffffffff-ffff-ffff-ffff-ffffffffffff.json"
    )
  )
  writeLines(
    vapply(broken, jsonlite::toJSON, character(1), auto_unbox = TRUE),
    broken_path,
    useBytes = TRUE
  )
  writeLines(
    vapply(valid, jsonlite::toJSON, character(1), auto_unbox = TRUE),
    valid_path,
    useBytes = TRUE
  )

  sets <- fabric_delta_checkpoint_sets(c(broken_path, valid_path))
  expect_length(sets, 1L)
  expect_length(sets[[1L]]$alternatives, 2L)

  snapshot <- fabric_delta_resolve_snapshot(table_dir)

  expect_equal(snapshot$checkpoint_version, 10)
  expect_identical(snapshot$active, "part.parquet")
})

test_that("Delta reader falls back to an older complete checkpoint", {
  table_dir <- fs::path_temp(paste0(
    "delta-older-checkpoint-fallback-",
    sample.int(1e9, 1)
  ))
  log_dir <- fs::path(table_dir, "_delta_log")
  fs::dir_create(log_dir, recurse = TRUE)
  on.exit(fs::dir_delete(table_dir), add = TRUE)
  older <- fs::path(
    log_dir,
    "00000000000000000005.checkpoint.parquet"
  )
  newest <- fs::path(
    log_dir,
    "00000000000000000010.checkpoint.parquet"
  )
  writeBin(raw(), older)
  writeBin(raw(), newest)
  for (version in 6:11) {
    writeLines(
      "{}",
      fs::path(log_dir, sprintf("%020.0f.json", version)),
      useBytes = TRUE
    )
  }
  local_mocked_bindings(
    fabric_delta_read_checkpoint = function(paths) {
      if (identical(as.character(paths), as.character(newest))) {
        rlang::abort("corrupt newest checkpoint")
      }
      expect_identical(as.character(paths), as.character(older))
      list(
        protocol = list(
          minReaderVersion = 1L,
          minWriterVersion = 2L
        ),
        metaData = list(
          id = "fallback-table",
          format = list(provider = "parquet", options = list()),
          schemaString = '{"type":"struct","fields":[]}',
          partitionColumns = list(),
          configuration = list()
        ),
        add = list(
          path = "from-older-checkpoint.parquet",
          partitionValues = list()
        )
      )
    }
  )

  snapshot <- fabric_delta_resolve_snapshot(table_dir)

  expect_equal(snapshot$version, 11)
  expect_equal(snapshot$checkpoint_version, 5)
  expect_identical(
    snapshot$active,
    "from-older-checkpoint.parquet"
  )
})

test_that("Delta snapshot ignores an incomplete multipart checkpoint", {
  table_dir <- fs::path_temp(
    paste0("delta-incomplete-checkpoint-", sample.int(1e9, 1))
  )
  log_dir <- fs::path(table_dir, "_delta_log")
  fs::dir_create(log_dir, recurse = TRUE)
  on.exit(fs::dir_delete(table_dir), add = TRUE)

  writeLines(
    c(
      '{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}',
      paste0(
        '{"metaData":{"id":"table",',
        '"format":{"provider":"parquet","options":{}},',
        '"schemaString":"{\\"type\\":\\"struct\\",\\"fields\\":[]}",',
        '"partitionColumns":[],"configuration":{}}}'
      ),
      '{"add":{"path":"part.parquet"}}'
    ),
    fs::path(log_dir, "00000000000000000000.json"),
    useBytes = TRUE
  )
  parts <- fs::path(
    log_dir,
    c(
      "00000000000000000010.checkpoint.0000000001.0000000003.parquet",
      "00000000000000000010.checkpoint.0000000003.0000000003.parquet"
    )
  )
  purrr::walk(parts, function(path) writeBin(raw(), path))

  snapshot <- fabric_delta_resolve_snapshot(table_dir)

  expect_equal(snapshot$version, 0)
  expect_null(snapshot$checkpoint_version)
  expect_equal(snapshot$active, "part.parquet")
})

test_that("Delta type widening validates stable and preview transitions", {
  schema <- list(
    fields = list(list(
      name = "id",
      type = "long",
      metadata = list(
        "delta.typeChanges" = list(list(
          fromType = "integer",
          toType = "long"
        ))
      )
    ))
  )
  expect_invisible(
    fabric_delta_validate_type_widening(schema, "typeWidening")
  )
  # Delta's `TypeWidening.isTypeChangeSupported()` does not branch on which of
  # the two feature names a table carries, so a preview-named table supports
  # exactly the same transitions.
  expect_invisible(
    fabric_delta_validate_type_widening(schema, "typeWidening-preview")
  )
  expect_true(
    fabric_delta_supported_type_change(
      "decimal(8,2)",
      "decimal(12,4)"
    )
  )
  expect_false(
    fabric_delta_supported_type_change(
      "decimal(8,2)",
      "decimal(9,4)"
    )
  )
  expect_true(
    fabric_delta_supported_type_change(
      "date",
      "timestamp_ntz"
    )
  )
  expect_false(fabric_delta_supported_type_change("long", "double"))
  expect_true(
    fabric_delta_supported_type_change("integer", "decimal(12,2)")
  )
  expect_false(
    fabric_delta_supported_type_change("integer", "decimal(10,1)")
  )
  expect_true(
    fabric_delta_supported_type_change("long", "decimal(23,3)")
  )
  expect_false(
    fabric_delta_supported_type_change("long", "decimal(20,1)")
  )
  expect_false(
    fabric_delta_supported_type_change("long", "decimal(39,0)")
  )
  expect_true(fabric_delta_supported_type_change("byte", "double"))

  # Delta accepts a recorded change that leaves the type alone, and tolerates
  # char/varchar/string changes rather than blocking a readable table.
  expect_true(fabric_delta_supported_type_change("long", "long"))
  expect_true(fabric_delta_supported_type_change("string", "string"))
  expect_true(fabric_delta_supported_type_change(
    "timestamp_ntz",
    "timestamp_ntz"
  ))
  expect_true(fabric_delta_supported_type_change(
    "decimal(8,2)",
    "decimal(8,2)"
  ))
  expect_true(fabric_delta_supported_type_change("char(5)", "string"))
  expect_true(fabric_delta_supported_type_change("varchar(10)", "char(5)"))
  expect_false(fabric_delta_supported_type_change("string", "integer"))
  expect_false(fabric_delta_supported_type_change("integer", "varchar(4)"))
  expect_invisible(
    fabric_delta_validate_type_widening(
      list(
        fields = list(list(
          name = "label",
          type = "string",
          metadata = list(
            "delta.typeChanges" = list(list(
              fromType = "varchar(10)",
              toType = "string"
            ))
          )
        ))
      ),
      "typeWidening"
    )
  )

  # Every transition the stable feature supports must also be accepted under
  # the preview feature name.
  preview_transitions <- list(
    c("byte", "short"),
    c("byte", "integer"),
    c("byte", "long"),
    c("byte", "double"),
    c("short", "integer"),
    c("short", "long"),
    c("short", "double"),
    c("integer", "long"),
    c("integer", "double"),
    c("float", "double"),
    c("date", "timestamp_ntz"),
    c("integer", "decimal(12,2)"),
    c("long", "decimal(23,3)"),
    c("decimal(8,2)", "decimal(12,4)")
  )
  for (transition in preview_transitions) {
    field <- list(
      name = "value",
      type = transition[[2L]],
      metadata = list(
        "delta.typeChanges" = list(list(
          fromType = transition[[1L]],
          toType = transition[[2L]]
        ))
      )
    )
    expect_true(
      is.list(fabric_delta_validate_type_widening(
        list(fields = list(field)),
        "typeWidening-preview"
      )),
      info = paste(transition, collapse = " -> ")
    )
  }
  expect_error(
    fabric_delta_validate_type_widening(
      list(
        fields = list(list(
          name = "value",
          type = "integer",
          metadata = list(
            "delta.typeChanges" = list(list(
              fromType = "double",
              toType = "integer"
            ))
          )
        ))
      ),
      "typeWidening-preview"
    ),
    "double -> integer",
    fixed = TRUE,
    class = "fabric_delta_unsupported_error"
  )

  nested <- list(
    fields = list(list(
      name = "values",
      type = list(
        type = "array",
        elementType = list(
          type = "map",
          keyType = "string",
          valueType = "decimal(14,4)"
        )
      ),
      metadata = list(
        "delta.typeChanges" = list(
          list(
            fromType = "decimal(8,2)",
            toType = "decimal(10,2)",
            fieldPath = "element.value"
          ),
          list(
            fromType = "decimal(10,2)",
            toType = "decimal(14,4)",
            fieldPath = "element.value"
          )
        )
      )
    ))
  )
  expect_invisible(
    fabric_delta_validate_type_widening(nested, "typeWidening")
  )

  invalid_path <- nested
  invalid_path$fields[[1L]]$metadata[["delta.typeChanges"]][[1L]]$fieldPath <-
    "element.element"
  invalid_path$fields[[1L]]$metadata[["delta.typeChanges"]][[2L]]$fieldPath <-
    "element.element"
  expect_error(
    fabric_delta_validate_type_widening(invalid_path, "typeWidening"),
    "does not resolve through the current schema",
    fixed = TRUE,
    class = "fabric_delta_unsupported_error"
  )

  broken_history <- nested
  broken_history$fields[[1L]]$metadata[["delta.typeChanges"]][[2L]]$fromType <-
    "decimal(11,2)"
  expect_error(
    fabric_delta_validate_type_widening(broken_history, "typeWidening"),
    "is not contiguous",
    fixed = TRUE,
    class = "fabric_delta_unsupported_error"
  )

  stale_history <- nested
  stale_history$fields[[1L]]$type$elementType$valueType <- "decimal(16,4)"
  expect_error(
    fabric_delta_validate_type_widening(stale_history, "typeWidening"),
    "current schema type is decimal(16,4)",
    fixed = TRUE,
    class = "fabric_delta_unsupported_error"
  )
})

test_that("Delta type widening converts files written before the change", {
  # Every data file here predates the recorded type change, so DuckDB's
  # `UNION ALL BY NAME` cannot promote the column to the widened type. The
  # reader must convert to the current logical Delta type on its own.
  table_dir <- fs::path_temp(paste0("delta-widen-", sample.int(1e9, 1)))
  log_dir <- fs::path(table_dir, "_delta_log")
  fs::dir_create(log_dir, recurse = TRUE)
  on.exit(fs::dir_delete(table_dir), add = TRUE)
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  DBI::dbExecute(
    con,
    paste0(
      "COPY (SELECT ",
      "CAST(5 AS INTEGER) AS amount, ",
      "DATE '2024-03-05' AS occurred, ",
      "CAST(7 AS TINYINT) AS counted, ",
      "CAST(0.5 AS FLOAT) AS ratio, ",
      "{'inner': CAST(3 AS INTEGER)} AS nested, ",
      "[CAST(4 AS INTEGER)] AS listed, ",
      "MAP {'k': CAST(6 AS INTEGER)} AS keyed) TO ",
      as.character(DBI::dbQuoteString(
        con,
        gsub("\\\\", "/", fs::path(table_dir, "part.parquet"))
      )),
      " (FORMAT PARQUET)"
    )
  )
  widened <- function(name, type, from) {
    list(
      name = name,
      type = type,
      nullable = TRUE,
      metadata = list(
        "delta.typeChanges" = list(list(fromType = from, toType = type))
      )
    )
  }
  schema <- jsonlite::toJSON(
    list(
      type = "struct",
      fields = list(
        widened("amount", "decimal(12,2)", "integer"),
        widened("occurred", "timestamp_ntz", "date"),
        widened("counted", "integer", "byte"),
        widened("ratio", "double", "float"),
        list(
          name = "nested",
          type = list(
            type = "struct",
            fields = list(widened("inner", "decimal(13,3)", "integer"))
          ),
          nullable = TRUE,
          metadata = list()
        ),
        list(
          name = "listed",
          type = list(type = "array", elementType = "decimal(11,1)"),
          nullable = TRUE,
          metadata = list(
            "delta.typeChanges" = list(list(
              fromType = "integer",
              toType = "decimal(11,1)",
              fieldPath = "element"
            ))
          )
        ),
        list(
          name = "keyed",
          type = list(
            type = "map",
            keyType = "string",
            valueType = "decimal(14,4)"
          ),
          nullable = TRUE,
          metadata = list(
            "delta.typeChanges" = list(list(
              fromType = "integer",
              toType = "decimal(14,4)",
              fieldPath = "value"
            ))
          )
        )
      )
    ),
    auto_unbox = TRUE
  )
  writeLines(
    c(
      paste0(
        '{"protocol":{"minReaderVersion":3,"minWriterVersion":7,',
        '"readerFeatures":["typeWidening","timestampNtz"],',
        '"writerFeatures":["typeWidening","timestampNtz"]}}'
      ),
      jsonlite::toJSON(
        list(
          metaData = list(
            id = "table",
            format = list(provider = "parquet", options = list()),
            schemaString = schema,
            partitionColumns = list(),
            configuration = list()
          )
        ),
        auto_unbox = TRUE
      ),
      '{"add":{"path":"part.parquet","partitionValues":{}}}'
    ),
    fs::path(log_dir, "00000000000000000000.json"),
    useBytes = TRUE
  )

  result <- fabric_delta_read_staged(table_dir)

  expect_identical(result$amount, "5.00")
  expect_s3_class(result$occurred, "fabric_delta_timestamp_ntz")
  expect_identical(unclass(result$occurred), "2024-03-05 00:00:00.000000")
  expect_identical(
    as.POSIXct(result$occurred, tz = "UTC"),
    as.POSIXct("2024-03-05 00:00:00", tz = "UTC")
  )
  expect_identical(result$counted, 7L)
  expect_identical(result$ratio, 0.5)
  expect_identical(result$nested$inner, "3.000")
  expect_identical(result$listed[[1L]], "4.0")
  expect_identical(result$keyed[[1L]]$value, "6.0000")
})

test_that("Delta type widening keeps mixed narrow and widened files exact", {
  table_dir <- fs::path_temp(paste0("delta-widen-mixed-", sample.int(1e9, 1)))
  log_dir <- fs::path(table_dir, "_delta_log")
  fs::dir_create(log_dir, recurse = TRUE)
  on.exit(fs::dir_delete(table_dir), add = TRUE)
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  copy <- function(name, select) {
    DBI::dbExecute(
      con,
      paste0(
        "COPY (SELECT ",
        select,
        ") TO ",
        as.character(DBI::dbQuoteString(
          con,
          gsub("\\\\", "/", fs::path(table_dir, name))
        )),
        " (FORMAT PARQUET)"
      )
    )
  }
  copy(
    "narrow.parquet",
    "CAST(1 AS INTEGER) AS id, CAST(5 AS INTEGER) AS amount, DATE '2024-03-05' AS occurred"
  )
  copy(
    "wide.parquet",
    paste0(
      "CAST(2 AS INTEGER) AS id, ",
      "CAST('1234567890.12' AS DECIMAL(12,2)) AS amount, ",
      "TIMESTAMP '2026-07-28 12:34:56.123456' AS occurred"
    )
  )
  schema <- jsonlite::toJSON(
    list(
      type = "struct",
      fields = list(
        list(name = "id", type = "integer", nullable = TRUE, metadata = list()),
        list(
          name = "amount",
          type = "decimal(12,2)",
          nullable = TRUE,
          metadata = list(
            "delta.typeChanges" = list(list(
              fromType = "integer",
              toType = "decimal(12,2)"
            ))
          )
        ),
        list(
          name = "occurred",
          type = "timestamp_ntz",
          nullable = TRUE,
          metadata = list(
            "delta.typeChanges" = list(list(
              fromType = "date",
              toType = "timestamp_ntz"
            ))
          )
        )
      )
    ),
    auto_unbox = TRUE
  )
  writeLines(
    c(
      paste0(
        '{"protocol":{"minReaderVersion":3,"minWriterVersion":7,',
        '"readerFeatures":["typeWidening","timestampNtz"],',
        '"writerFeatures":["typeWidening","timestampNtz"]}}'
      ),
      jsonlite::toJSON(
        list(
          metaData = list(
            id = "table",
            format = list(provider = "parquet", options = list()),
            schemaString = schema,
            partitionColumns = list(),
            configuration = list()
          )
        ),
        auto_unbox = TRUE
      ),
      '{"add":{"path":"narrow.parquet","partitionValues":{}}}',
      '{"add":{"path":"wide.parquet","partitionValues":{}}}'
    ),
    fs::path(log_dir, "00000000000000000000.json"),
    useBytes = TRUE
  )

  result <- fabric_delta_read_staged(table_dir)
  result <- result[order(result$id), ]

  expect_identical(result$amount, c("5.00", "1234567890.12"))
  expect_identical(
    unclass(result$occurred),
    c("2024-03-05 00:00:00.000000", "2026-07-28 12:34:56.123456")
  )
})

test_that("Delta reader accepts supported features and rejects unsafe ones", {
  state <- list(
    protocol = list(
      minReaderVersion = 3L,
      minWriterVersion = 7L,
      readerFeatures = list(
        "columnMapping",
        "deletionVectors",
        "timestampNtz",
        "typeWidening",
        "vacuumProtocolCheck",
        "variantType",
        "variantShredding"
      ),
      writerFeatures = list(
        "columnMapping",
        "deletionVectors",
        "timestampNtz",
        "typeWidening",
        "vacuumProtocolCheck",
        "variantType",
        "variantShredding"
      )
    ),
    metadata = list(
      format = list(provider = "parquet", options = list()),
      schemaString = '{"type":"struct","fields":[]}',
      partitionColumns = list(),
      configuration = list(
        "delta.columnMapping.mode" = "name"
      )
    ),
    has_deletion_vectors = TRUE,
    active = "part.parquet",
    files = list(
      "part.parquet" = list(
        deletionVector = list(storageType = "i")
      )
    )
  )
  expect_invisible(fabric_delta_validate_reader(state))

  preview_variant <- state
  preview_variant$protocol$readerFeatures <- as.list(c(
    setdiff(
      unlist(preview_variant$protocol$readerFeatures),
      "variantShredding"
    ),
    "variantShredding-preview"
  ))
  preview_variant$protocol$writerFeatures <- preview_variant$protocol$readerFeatures
  expect_invisible(fabric_delta_validate_reader(preview_variant))

  invalid_variant <- state
  invalid_variant$protocol$readerFeatures <- list("variantShredding")
  invalid_variant$protocol$writerFeatures <- list("variantShredding")
  expect_error(
    fabric_delta_validate_reader(invalid_variant),
    "without its required variantType",
    fixed = TRUE,
    class = "fabric_delta_unsupported_error"
  )
  invalid_preview_variant <- state
  invalid_preview_variant$protocol$readerFeatures <- list(
    "variantShredding-preview"
  )
  invalid_preview_variant$protocol$writerFeatures <- list(
    "variantShredding-preview"
  )
  expect_error(
    fabric_delta_validate_reader(invalid_preview_variant),
    "without its required variantType",
    fixed = TRUE,
    class = "fabric_delta_unsupported_error"
  )

  state$protocol <- list(minReaderVersion = 1L, minWriterVersion = 1L)
  expect_error(
    fabric_delta_validate_reader(state),
    "column mapping without matching protocol support",
    fixed = TRUE,
    class = "fabric_delta_unsupported_error"
  )
  state$protocol <- list(minReaderVersion = 2L, minWriterVersion = 5L)
  state$has_deletion_vectors <- FALSE
  state$files[["part.parquet"]]$deletionVector <- NULL
  expect_invisible(
    fabric_delta_validate_reader(state)
  )

  state$metadata$configuration[["delta.columnMapping.mode"]] <- "id"
  expect_invisible(fabric_delta_validate_reader(state))
  state$metadata$configuration[["delta.columnMapping.mode"]] <- "none"
  state$protocol <- list(
    minReaderVersion = 3L,
    minWriterVersion = 7L,
    readerFeatures = list("variant"),
    writerFeatures = list("variant")
  )
  expect_error(
    fabric_delta_validate_reader(state),
    "reader feature(s): variant",
    fixed = TRUE,
    class = "fabric_delta_unsupported_error"
  )
  state$protocol$readerFeatures <- list("deletionVectors")
  state$protocol$writerFeatures <- list("deletionVectors")
  state$has_deletion_vectors <- TRUE
  state$files[["part.parquet"]]$deletionVector <- list(
    storageType = "p",
    pathOrInlineDv = paste0(
      "abfss://11111111-1111-1111-1111-111111111111",
      "@onelake.dfs.fabric.microsoft.com/",
      "22222222-2222-2222-2222-222222222222/",
      "Tables/source/deletion-vector.bin"
    )
  )
  expect_invisible(fabric_delta_validate_reader(state))

  state$protocol <- list(
    minReaderVersion = 3L,
    minWriterVersion = 7L,
    writerFeatures = list()
  )
  expect_error(
    fabric_delta_validate_reader(state),
    "without readerFeatures",
    fixed = TRUE,
    class = "fabric_delta_unsupported_error"
  )
})

test_that("Delta reader validates protocol field shape and feature invariants", {
  state <- list(
    protocol = list(minReaderVersion = 1L, minWriterVersion = 2L),
    metadata = list(
      format = list(provider = "parquet", options = list()),
      schemaString = '{"type":"struct","fields":[]}',
      partitionColumns = list(),
      configuration = list()
    ),
    active = character(),
    files = list()
  )
  expect_invisible(fabric_delta_validate_reader(state))

  invalid <- state
  invalid$protocol$minWriterVersion <- NULL
  expect_error(
    fabric_delta_validate_reader(invalid),
    "minWriterVersion must be one whole number",
    fixed = TRUE
  )
  invalid <- state
  invalid$protocol$minReaderVersion <- 1.5
  expect_error(
    fabric_delta_validate_reader(invalid),
    "minReaderVersion must be one whole number",
    fixed = TRUE
  )
  invalid <- state
  invalid$protocol$readerFeatures <- list()
  expect_error(
    fabric_delta_validate_reader(invalid),
    "reader protocol version 1 with a readerFeatures field",
    fixed = TRUE
  )
  invalid <- state
  invalid$protocol$writerFeatures <- list()
  expect_error(
    fabric_delta_validate_reader(invalid),
    "writer protocol version 2 with a writerFeatures field",
    fixed = TRUE
  )

  feature_state <- state
  feature_state$protocol <- list(
    minReaderVersion = 3L,
    minWriterVersion = 7L,
    readerFeatures = list("timestampNtz"),
    writerFeatures = list("timestampNtz")
  )
  expect_invisible(fabric_delta_validate_reader(feature_state))

  invalid <- feature_state
  invalid$protocol$writerFeatures <- NULL
  expect_error(
    fabric_delta_validate_reader(invalid),
    "writer protocol version 7 without writerFeatures",
    fixed = TRUE
  )
  invalid <- feature_state
  invalid$protocol$writerFeatures <- list()
  expect_error(
    fabric_delta_validate_reader(invalid),
    "reader feature(s) absent from writerFeatures: timestampNtz",
    fixed = TRUE
  )
  invalid <- feature_state
  invalid$protocol$readerFeatures <- list("timestampNtz", "timestampNtz")
  expect_error(
    fabric_delta_validate_reader(invalid),
    "readerFeatures must contain unique non-empty strings",
    fixed = TRUE
  )
  invalid <- feature_state
  invalid$protocol$writerFeatures <- list("timestampNtz", 1L)
  expect_error(
    fabric_delta_validate_reader(invalid),
    "writerFeatures must contain unique non-empty strings",
    fixed = TRUE
  )
})

test_that("Delta reader enforces schema feature dependencies", {
  schema <- jsonlite::toJSON(
    list(
      type = "struct",
      fields = list(
        list(
          name = "observed_at",
          type = "timestamp_ntz",
          nullable = TRUE,
          metadata = list()
        ),
        list(
          name = "payload",
          type = "variant",
          nullable = TRUE,
          metadata = list()
        ),
        list(
          name = "id",
          type = "long",
          nullable = TRUE,
          metadata = list(
            "delta.typeChanges" = list(list(
              fromType = "integer",
              toType = "long"
            ))
          )
        )
      )
    ),
    auto_unbox = TRUE
  )
  state <- list(
    protocol = list(
      minReaderVersion = 3L,
      minWriterVersion = 7L,
      readerFeatures = list(
        "timestampNtz",
        "variantType",
        "typeWidening"
      ),
      writerFeatures = list(
        "timestampNtz",
        "variantType",
        "typeWidening"
      )
    ),
    metadata = list(
      format = list(provider = "parquet", options = list()),
      schemaString = schema,
      partitionColumns = list(),
      configuration = list()
    ),
    active = character(),
    files = list()
  )
  expect_invisible(fabric_delta_validate_reader(state))

  nested_variant <- state
  nested_schema <- jsonlite::fromJSON(schema, simplifyVector = FALSE)
  nested_schema$fields[[2L]]$type <- list(
    type = "array",
    elementType = "variant",
    containsNull = TRUE
  )
  nested_variant$metadata$schemaString <- jsonlite::toJSON(
    nested_schema,
    auto_unbox = TRUE
  )
  expect_error(
    fabric_delta_validate_reader(nested_variant),
    "Variant fields nested inside another complex field",
    fixed = TRUE,
    class = "fabric_delta_unsupported_error"
  )

  without <- function(feature) {
    candidate <- state
    candidate$protocol$readerFeatures <- setdiff(
      unlist(candidate$protocol$readerFeatures),
      feature
    )
    candidate
  }
  expect_error(
    fabric_delta_validate_reader(without("timestampNtz")),
    "without matching timestampNtz",
    fixed = TRUE,
    class = "fabric_delta_unsupported_error"
  )
  expect_error(
    fabric_delta_validate_reader(without("variantType")),
    "without matching variantType",
    fixed = TRUE,
    class = "fabric_delta_unsupported_error"
  )
  expect_error(
    fabric_delta_validate_reader(without("typeWidening")),
    "without matching type-widening",
    fixed = TRUE,
    class = "fabric_delta_unsupported_error"
  )
})

test_that("Delta reader fails safely for incomplete snapshots", {
  table_dir <- fs::path_temp(paste0("delta-incomplete-", sample.int(1e9, 1)))
  log_dir <- fs::path(table_dir, "_delta_log")
  fs::dir_create(log_dir, recurse = TRUE)
  on.exit(fs::dir_delete(table_dir), add = TRUE)
  writeLines(
    '{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}',
    fs::path(log_dir, "00000000000000000000.json")
  )
  writeLines(
    '{"add":{"path":"part.parquet"}}',
    fs::path(log_dir, "00000000000000000002.json")
  )
  expect_error(
    fabric_delta_resolve_snapshot(table_dir),
    "required commit is missing",
    fixed = TRUE
  )
})

test_that("Delta deletion vectors decode inline and persisted storage", {
  descriptor <- list(
    storageType = "i",
    pathOrInlineDv = paste0(
      "^Bg9^0rr910000000000iXQKl0rr91000f55c8Xg0",
      "@@D72lkbi5=-{L"
    ),
    sizeInBytes = 44L,
    cardinality = 6L
  )
  expect_equal(
    fabric_delta_read_deletion_vector(descriptor, tempdir()),
    c(3, 4, 7, 11, 18, 29)
  )

  relative_descriptor <- descriptor
  relative_descriptor$storageType <- "u"
  relative_descriptor$pathOrInlineDv <- "ab^-aqEH.-t@S}K{vb[*k^"
  relative_descriptor$offset <- 1L
  expected_path <- paste0(
    "ab/deletion_vector_",
    "d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin"
  )
  expect_equal(
    fabric_delta_deletion_vector_path(relative_descriptor),
    expected_path
  )

  table_dir <- fs::path_temp(paste0("delta-dv-", sample.int(1e9, 1)))
  path <- fs::path(table_dir, expected_path)
  fs::dir_create(fs::path_dir(path), recurse = TRUE)
  on.exit(fs::dir_delete(table_dir), add = TRUE)
  bitmap <- fabric_delta_z85_decode(descriptor$pathOrInlineDv)
  uint32_be <- function(value) {
    as.raw(floor(value / 256^(3:0)) %% 256)
  }
  writeBin(
    c(
      as.raw(1L),
      uint32_be(length(bitmap)),
      bitmap,
      uint32_be(fabric_delta_crc32(bitmap))
    ),
    path
  )
  expect_equal(
    fabric_delta_read_deletion_vector(relative_descriptor, table_dir),
    c(3, 4, 7, 11, 18, 29)
  )
  damaged <- readBin(path, "raw", n = fs::file_size(path))
  damaged[[10L]] <- as.raw(bitwXor(as.integer(damaged[[10L]]), 1L))
  writeBin(damaged, path)
  expect_error(
    fabric_delta_read_deletion_vector(relative_descriptor, table_dir),
    "checksum",
    fixed = TRUE
  )
})

test_that("Delta inline deletion vectors tolerate Z85 alignment padding", {
  # Delta's Base85Codec pads unaligned input to a 4-byte boundary before
  # encoding and truncates to `sizeInBytes` on decode. An array container uses
  # two bytes per row index, so any odd cardinality is unaligned.
  little_endian <- function(value, width) {
    as.raw(floor(value / 256^(seq_len(width) - 1L)) %% 256)
  }
  roaring_array <- function(values) {
    c(
      little_endian(1681511377, 4L),
      little_endian(1, 4L),
      little_endian(0, 4L),
      little_endian(0, 4L),
      little_endian(12346, 4L),
      little_endian(1, 4L),
      little_endian(0, 2L),
      little_endian(length(values) - 1L, 2L),
      little_endian(0, 4L),
      unlist(lapply(values, little_endian, width = 2L))
    )
  }
  z85_encode <- function(bytes) {
    alphabet <- strsplit(
      paste0(
        "0123456789abcdefghijklmnopqrstuvwxyz",
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#"
      ),
      "",
      fixed = TRUE
    )[[1L]]
    remainder <- length(bytes) %% 4L
    if (remainder) {
      bytes <- c(bytes, as.raw(rep(0L, 4L - remainder)))
    }
    starts <- seq.int(1L, length(bytes), by = 4L)
    paste(
      vapply(
        starts,
        function(start) {
          number <- sum(
            as.numeric(as.integer(bytes[start + 0:3])) * 256^(3:0)
          )
          digits <- numeric(5L)
          for (index in 5:1) {
            digits[[index]] <- number %% 85
            number <- floor(number / 85)
          }
          paste(alphabet[digits + 1L], collapse = "")
        },
        character(1)
      ),
      collapse = ""
    )
  }

  for (values in list(c(0), c(3, 4, 7), c(3, 4, 7, 11, 18))) {
    bitmap <- roaring_array(values)
    expect_true(length(bitmap) %% 4L != 0L)
    descriptor <- list(
      storageType = "i",
      pathOrInlineDv = z85_encode(bitmap),
      sizeInBytes = length(bitmap),
      cardinality = length(values)
    )
    expect_equal(
      fabric_delta_read_deletion_vector(descriptor, tempdir()),
      as.numeric(values),
      info = paste("cardinality", length(values))
    )
  }

  aligned <- roaring_array(c(3, 4, 7, 11, 18, 29))
  expect_equal(length(aligned) %% 4L, 0L)
  expect_equal(
    fabric_delta_read_deletion_vector(
      list(
        storageType = "i",
        pathOrInlineDv = z85_encode(aligned),
        sizeInBytes = length(aligned),
        cardinality = 6L
      ),
      tempdir()
    ),
    c(3, 4, 7, 11, 18, 29)
  )

  # More than three bytes of slack is a genuine descriptor mismatch.
  expect_error(
    fabric_delta_read_deletion_vector(
      list(
        storageType = "i",
        pathOrInlineDv = z85_encode(aligned),
        sizeInBytes = length(aligned) - 4L,
        cardinality = 6L
      ),
      tempdir()
    ),
    "size does not match its descriptor",
    fixed = TRUE
  )
  expect_error(
    fabric_delta_read_deletion_vector(
      list(
        storageType = "i",
        pathOrInlineDv = z85_encode(aligned),
        sizeInBytes = length(aligned) + 4L,
        cardinality = 6L
      ),
      tempdir()
    ),
    "size does not match its descriptor",
    fixed = TRUE
  )
})

test_that("Delta deletion vectors decode portable Roaring golden vectors", {
  # Generated with pyroaring (CRoaring portable serialization), then wrapped
  # in Delta's RoaringBitmapArray framing and gzip-compressed for readability.
  golden <- list(
    bitmap = paste0(
      "H4sIAAAAAAACCu3HQQ2AMAAEwasDEoz0Cz7qAC/V12CIJjzwUGYem+y4j6vk",
      "c9bk/b5vsw1gWQEAAAAAAIDfeAA3Mc+MICAAAA=="
    ),
    run = paste0(
      "H4sIAAAAAAACCrt42TKFkQEBrA0YGID81WqMDClAEgDiPXjoHwAAAA=="
    ),
    buckets = paste0(
      "H4sIAAAAAAACCrt42TKFiQEBrAwYGBjBLCYGAQYQm5mBFSyCLsPEwMLAx",
      "gAAMstVqUAAAAA="
    )
  )
  decode <- function(value) {
    bytes <- memDecompress(jsonlite::base64_dec(value), type = "gzip")
    magic <- fabric_delta_raw_uint32(bytes, 1L, endian = "little")
    expect_identical(as.numeric(magic), 1681511377)
    fabric_delta_roaring64(bytes[-seq_len(4L)])
  }

  expect_identical(decode(golden$bitmap), as.numeric(seq(0, 9998, 2)))
  expect_identical(decode(golden$run), as.numeric(100:9999))
  expect_identical(
    decode(golden$buckets),
    c(1, 3, 5, 4294967296 + c(2, 4, 6))
  )
})

test_that("Delta file reconciliation distinguishes deletion-vector identities", {
  state <- list(
    active = character(),
    files = list(),
    protocol = NULL,
    metadata = NULL,
    has_deletion_vectors = FALSE
  )
  dv_a <- list(
    storageType = "u",
    pathOrInlineDv = "aa^-aqEH.-t@S}K{vb[*k^",
    offset = 1L
  )
  dv_b <- list(
    storageType = "u",
    pathOrInlineDv = "bb^-aqEH.-t@S}K{vb[*k^",
    offset = 17L
  )
  expect_identical(
    fabric_delta_deletion_vector_id(dv_a),
    "uaa^-aqEH.-t@S}K{vb[*k^@1"
  )
  expect_identical(
    fabric_delta_deletion_vector_id(list(
      storageType = "i",
      pathOrInlineDv = "inline"
    )),
    "iinline"
  )
  expect_true(is.na(fabric_delta_deletion_vector_id(NULL)))

  state <- fabric_delta_apply_actions(
    state,
    list(list(
      add = list(
        path = "part.parquet",
        partitionValues = list(),
        deletionVector = dv_a
      )
    ))
  )
  state <- fabric_delta_apply_actions(
    state,
    list(
      list(remove = list(path = "part.parquet", deletionVector = dv_a)),
      list(
        add = list(
          path = "part.parquet",
          partitionValues = list(),
          deletionVector = dv_b
        )
      )
    )
  )

  # A later tombstone for the old logical file must not remove the current DV.
  state <- fabric_delta_apply_actions(
    state,
    list(list(
      remove = list(
        path = "part.parquet",
        deletionVector = dv_a
      )
    ))
  )
  expect_identical(state$active, "part.parquet")
  expect_identical(
    state$files[["part.parquet"]]$deletionVectorId,
    fabric_delta_deletion_vector_id(dv_b)
  )

  # Checkpoint sidecars are independent and unordered. A stale tombstone in a
  # later sidecar must likewise leave the current logical file active.
  stale_sidecar <- list(
    remove = data.frame(
      path = "part.parquet",
      deletionVector = I(list(dv_a))
    )
  )
  state <- fabric_delta_apply_checkpoint(state, stale_sidecar)
  expect_identical(state$active, "part.parquet")

  current_sidecar <- list(
    remove = data.frame(
      path = "part.parquet",
      deletionVector = I(list(dv_b))
    )
  )
  state <- fabric_delta_apply_checkpoint(state, current_sidecar)
  expect_length(state$active, 0L)
})

test_that("persisted deletion vectors support legacy and sidecar offsets", {
  descriptor <- list(
    storageType = "u",
    pathOrInlineDv = "ab^-aqEH.-t@S}K{vb[*k^",
    sizeInBytes = 44L,
    cardinality = 6L
  )
  expected <- c(3, 4, 7, 11, 18, 29)
  expected_path <- paste0(
    "ab/deletion_vector_",
    "d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin"
  )
  table_dir <- fs::path_temp(paste0("delta-dv-offset-", sample.int(1e9, 1)))
  path <- fs::path(table_dir, expected_path)
  fs::dir_create(fs::path_dir(path), recurse = TRUE)
  on.exit(fs::dir_delete(table_dir), add = TRUE)
  bitmap <- fabric_delta_z85_decode(
    paste0(
      "^Bg9^0rr910000000000iXQKl0rr91000f55c8Xg0",
      "@@D72lkbi5=-{L"
    )
  )
  uint32_be <- function(value) {
    as.raw(floor(value / 256^(3:0)) %% 256)
  }
  block <- c(
    uint32_be(length(bitmap)),
    bitmap,
    uint32_be(fabric_delta_crc32(bitmap))
  )

  writeBin(block, path)
  expect_equal(
    fabric_delta_read_deletion_vector(descriptor, table_dir),
    expected
  )
  zero_descriptor <- descriptor
  zero_descriptor$offset <- 0L
  expect_equal(
    fabric_delta_read_deletion_vector(zero_descriptor, table_dir),
    expected
  )

  writeBin(c(as.raw(1L), block, block), path)
  first_descriptor <- descriptor
  first_descriptor$offset <- 1L
  expect_equal(
    fabric_delta_read_deletion_vector(first_descriptor, table_dir),
    expected
  )
  second_descriptor <- descriptor
  second_descriptor$offset <- 1L + length(block)
  expect_equal(
    fabric_delta_read_deletion_vector(second_descriptor, table_dir),
    expected
  )

  # A descriptor that omits `offset` is documented as meaning 0, but byte 0 of a
  # version 1 file is the format version. `sizeInBytes` separates the layouts,
  # so the header is skipped rather than decoded as part of the size.
  expect_equal(
    fabric_delta_read_deletion_vector(descriptor, table_dir),
    expected
  )
  expect_equal(
    fabric_delta_read_deletion_vector(zero_descriptor, table_dir),
    expected
  )

  # A version header is only tolerated when it declares a format this reader
  # understands, whether the offset is explicit or inferred.
  writeBin(c(as.raw(2L), block, block), path)
  expect_error(
    fabric_delta_read_deletion_vector(first_descriptor, table_dir),
    "unsupported version",
    fixed = TRUE
  )
  expect_error(
    fabric_delta_read_deletion_vector(descriptor, table_dir),
    "unsupported version",
    fixed = TRUE
  )

  writeBin(c(as.raw(1L), block), path)
  mismatched <- descriptor
  mismatched$offset <- 1L
  mismatched$sizeInBytes <- 40L
  expect_error(
    fabric_delta_read_deletion_vector(mismatched, table_dir),
    "size does not match its descriptor",
    fixed = TRUE
  )

  absolute <- paste0(
    "abfss://11111111-1111-1111-1111-111111111111",
    "@onelake.dfs.fabric.microsoft.com/",
    "22222222-2222-2222-2222-222222222222/",
    "Tables/source/deletion-vector.bin"
  )
  absolute_path <- fabric_delta_local_file(table_dir, absolute)
  fs::dir_create(fs::path_dir(absolute_path), recurse = TRUE)
  writeBin(block, absolute_path)
  absolute_descriptor <- descriptor
  absolute_descriptor$storageType <- "p"
  absolute_descriptor$pathOrInlineDv <- absolute
  expect_equal(
    fabric_delta_read_deletion_vector(absolute_descriptor, table_dir),
    expected
  )
})

test_that("absolute deletion-vector URIs are decoded exactly once", {
  # A blob literally named `deletion%20vector.bin` is written into the log as
  # `deletion%2520vector.bin`. Decoding that twice yields `deletion vector.bin`,
  # which re-encodes to a different blob and downloads nothing.
  encoded <- paste0(
    "abfss://11111111-1111-1111-1111-111111111111",
    "@onelake.dfs.fabric.microsoft.com/",
    "22222222-2222-2222-2222-222222222222/",
    "Tables/source/deletion%2520vector.bin"
  )
  descriptor <- list(
    storageType = "p",
    pathOrInlineDv = encoded,
    sizeInBytes = 44L,
    cardinality = 6L
  )
  expect_identical(fabric_delta_deletion_vector_path(descriptor), encoded)

  target <- onelake_resolve_target(
    "11111111-1111-1111-1111-111111111111",
    "22222222-2222-2222-2222-222222222222",
    "Tables/dbo/table"
  )
  staged <- fabric_delta_stage_files(
    fabric_delta_deletion_vector_paths(list(
      active = "part.parquet",
      files = list("part.parquet" = list(deletionVector = descriptor))
    )),
    target,
    "Tables/dbo/table",
    "stage"
  )
  expect_equal(nrow(staged), 1L)
  expect_identical(
    staged$target[[1L]]$path,
    "Tables/source/deletion%20vector.bin"
  )
  expect_true(endsWith(
    onelake_path_url(staged$target[[1L]]),
    "Tables/source/deletion%2520vector.bin"
  ))
  # Staging and the later local lookup must agree on the same staged file.
  expect_identical(
    as.character(staged$destination),
    as.character(fabric_delta_local_file(
      "stage",
      fabric_delta_deletion_vector_path(descriptor)
    ))
  )
})

test_that("Delta reader applies name mapping and deletion vectors", {
  table_dir <- fs::path_temp(paste0("delta-modern-", sample.int(1e9, 1)))
  log_dir <- fs::path(table_dir, "_delta_log")
  fs::dir_create(log_dir, recurse = TRUE)
  on.exit(fs::dir_delete(table_dir), add = TRUE)
  parquet <- fs::path(table_dir, "part.parquet")
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  DBI::dbExecute(
    con,
    paste0(
      "COPY (SELECT range::INTEGER AS \"physical-id\", ",
      "'row-' || range::VARCHAR AS \"physical-name\" ",
      "FROM range(30)) TO ",
      as.character(DBI::dbQuoteString(con, gsub("\\\\", "/", parquet))),
      " (FORMAT PARQUET)"
    )
  )
  schema <- jsonlite::toJSON(
    list(
      type = "struct",
      fields = list(
        list(
          name = "id",
          type = "integer",
          nullable = FALSE,
          metadata = list(
            "delta.columnMapping.id" = 1L,
            "delta.columnMapping.physicalName" = "physical-id"
          )
        ),
        list(
          name = "display name",
          type = "string",
          nullable = TRUE,
          metadata = list(
            "delta.columnMapping.id" = 2L,
            "delta.columnMapping.physicalName" = "physical-name"
          )
        )
      )
    ),
    auto_unbox = TRUE
  )
  deletion_vector <- list(
    storageType = "i",
    pathOrInlineDv = paste0(
      "^Bg9^0rr910000000000iXQKl0rr91000f55c8Xg0",
      "@@D72lkbi5=-{L"
    ),
    sizeInBytes = 44L,
    cardinality = 6L
  )
  actions <- list(
    list(
      protocol = list(
        minReaderVersion = 3L,
        minWriterVersion = 7L,
        readerFeatures = list("columnMapping", "deletionVectors"),
        writerFeatures = list("columnMapping", "deletionVectors")
      )
    ),
    list(
      metaData = list(
        id = "table",
        format = list(provider = "parquet", options = list()),
        schemaString = schema,
        partitionColumns = list(),
        configuration = list("delta.columnMapping.mode" = "name")
      )
    ),
    list(
      add = list(
        path = "part.parquet",
        partitionValues = list(),
        deletionVector = deletion_vector
      )
    )
  )
  writeLines(
    vapply(
      actions,
      jsonlite::toJSON,
      character(1),
      auto_unbox = TRUE
    ),
    fs::path(log_dir, "00000000000000000000.json"),
    useBytes = TRUE
  )

  result <- fabric_delta_read_staged(table_dir)

  expect_named(result, c("id", "display name"))
  expect_equal(
    result$id,
    setdiff(0:29, c(3, 4, 7, 11, 18, 29))
  )
  expect_equal(result[["display name"]], paste0("row-", result$id))
})

test_that("Delta row tracking allows a physical file_row_number column", {
  table_dir <- fs::path_temp(paste0(
    "delta-file-row-number-",
    sample.int(1e9, 1)
  ))
  log_dir <- fs::path(table_dir, "_delta_log")
  fs::dir_create(log_dir, recurse = TRUE)
  on.exit(fs::dir_delete(table_dir), add = TRUE)
  parquet <- fs::path(table_dir, "part.parquet")
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  DBI::dbExecute(
    con,
    paste0(
      "COPY (SELECT range::INTEGER AS id, ",
      "1000::BIGINT + range AS file_row_number FROM range(5000)) TO ",
      as.character(DBI::dbQuoteString(con, gsub("\\\\", "/", parquet))),
      " (FORMAT PARQUET, ROW_GROUP_SIZE 2048)"
    )
  )
  parquet_groups <- DBI::dbGetQuery(
    con,
    paste0(
      "SELECT COUNT(DISTINCT row_group_id) AS groups FROM parquet_metadata(",
      as.character(DBI::dbQuoteString(con, gsub("\\\\", "/", parquet))),
      ")"
    )
  )
  expect_gt(parquet_groups$groups[[1L]], 1L)
  schema <- jsonlite::toJSON(
    list(
      type = "struct",
      fields = list(
        list(
          name = "id",
          type = "integer",
          nullable = FALSE,
          metadata = list()
        ),
        list(
          name = "file_row_number",
          type = "long",
          nullable = FALSE,
          metadata = list()
        )
      )
    ),
    auto_unbox = TRUE
  )
  deletion_vector <- list(
    storageType = "i",
    pathOrInlineDv = paste0(
      "^Bg9^0rr910000000000iXQKl0rr91000f55c8Xg0",
      "@@D72lkbi5=-{L"
    ),
    sizeInBytes = 44L,
    cardinality = 6L
  )
  actions <- list(
    list(
      protocol = list(
        minReaderVersion = 3L,
        minWriterVersion = 7L,
        readerFeatures = list("deletionVectors"),
        writerFeatures = list("deletionVectors")
      )
    ),
    list(
      metaData = list(
        id = "file-row-number",
        format = list(provider = "parquet", options = list()),
        schemaString = schema,
        partitionColumns = list(),
        configuration = list()
      )
    ),
    list(
      add = list(
        path = "part.parquet",
        partitionValues = list(),
        deletionVector = deletion_vector
      )
    )
  )
  writeLines(
    vapply(actions, jsonlite::toJSON, character(1), auto_unbox = TRUE),
    fs::path(log_dir, "00000000000000000000.json"),
    useBytes = TRUE
  )

  result <- fabric_delta_read_staged(table_dir)
  expected_ids <- setdiff(0:4999, c(3, 4, 7, 11, 18, 29))

  expect_identical(result$id, expected_ids)
  expect_s3_class(result$file_row_number, "integer64")
  expect_equal(as.numeric(result$file_row_number), 1000 + expected_ids)
})

test_that("Variant restoration allows a physical file_row_number column", {
  skip_if_not_installed("arrow")
  table_dir <- fs::path_temp(paste0(
    "delta-variant-file-row-number-",
    sample.int(1e9, 1)
  ))
  log_dir <- fs::path(table_dir, "_delta_log")
  fs::dir_create(log_dir, recurse = TRUE)
  on.exit(fs::dir_delete(table_dir), add = TRUE)
  parquet <- fs::path(table_dir, "part.parquet")
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  DBI::dbExecute(
    con,
    paste0(
      "COPY (SELECT 10::BIGINT AS file_row_number, ",
      "{'value': 1}::VARIANT AS payload UNION ALL ",
      "SELECT 20::BIGINT, NULL::VARIANT) TO ",
      as.character(DBI::dbQuoteString(con, gsub("\\\\", "/", parquet))),
      " (FORMAT PARQUET)"
    )
  )
  schema <- jsonlite::toJSON(
    list(
      type = "struct",
      fields = list(
        list(
          name = "file_row_number",
          type = "long",
          nullable = FALSE,
          metadata = list()
        ),
        list(
          name = "payload",
          type = "variant",
          nullable = TRUE,
          metadata = list()
        )
      )
    ),
    auto_unbox = TRUE
  )
  actions <- list(
    list(
      protocol = list(
        minReaderVersion = 3L,
        minWriterVersion = 7L,
        readerFeatures = list("variantType"),
        writerFeatures = list("variantType")
      )
    ),
    list(
      metaData = list(
        id = "variant-file-row-number",
        format = list(provider = "parquet", options = list()),
        schemaString = schema,
        partitionColumns = list(),
        configuration = list()
      )
    ),
    list(add = list(path = "part.parquet", partitionValues = list()))
  )
  writeLines(
    vapply(actions, jsonlite::toJSON, character(1), auto_unbox = TRUE),
    fs::path(log_dir, "00000000000000000000.json"),
    useBytes = TRUE
  )

  result <- fabric_delta_read_staged(table_dir)

  expect_equal(as.numeric(result$file_row_number), c(10, 20))
  expect_s3_class(result$payload[[1L]], "fabric_delta_variant")
  expect_s3_class(result$payload[[2L]], "fabric_delta_variant")
  expect_identical(result$payload[[2L]]$type, "VARIANT_NULL")
})
