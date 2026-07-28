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
        '"readerFeatures":["deletionVectors"]}}'
      ),
      '{"metaData":{"id":"table","configuration":{}}}',
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

  id_schema <- list(
    partitionColumns = character(),
    fields = list(list(
      name = "outer",
      type = list(
        type = "struct",
        fields = list(list(
          name = "inner",
          type = "integer",
          metadata = list(
            "delta.columnMapping.id" = 2L,
            "delta.columnMapping.physicalName" = "physical-inner"
          )
        ))
      ),
      metadata = list(
        "delta.columnMapping.id" = 1L,
        "delta.columnMapping.physicalName" = "physical-outer"
      )
    ))
  )
  id_projection <- fabric_delta_id_file_projection(
    con,
    id_schema,
    c("1" = "file-outer", "2" = "file-inner")
  )
  expect_match(id_projection, '"file-outer"."file-inner"', fixed = TRUE)
  expect_match(id_projection, '"physical-inner" :=', fixed = TRUE)
  expect_match(id_projection, 'AS "physical-outer"', fixed = TRUE)
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
    list(metaData = list(
      id = "void-table",
      format = list(provider = "parquet", options = list()),
      schemaString = schema,
      partitionColumns = list(),
      configuration = list()
    )),
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

test_that("Delta reader exposes native and shredded Variant values", {
  table_dir <- fs::path_temp(paste0("delta-variant-", sample.int(1e9, 1)))
  log_dir <- fs::path(table_dir, "_delta_log")
  fs::dir_create(log_dir, recurse = TRUE)
  on.exit(fs::dir_delete(table_dir), add = TRUE)
  parquet <- fs::path(table_dir, "part.parquet")
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  DBI::dbExecute(
    con,
    paste0(
      "COPY (SELECT {'action': 'checkout', 'items': [1, 2, 3], ",
      "'user_id': 4471}::VARIANT AS payload) TO ",
      as.character(DBI::dbQuoteString(con, gsub("\\\\", "/", parquet))),
      " (FORMAT PARQUET, SHREDDING {",
      "'payload': 'STRUCT(action VARCHAR, items INTEGER[], user_id INTEGER)'",
      "})"
    )
  )
  schema <- jsonlite::toJSON(
    list(
      type = "struct",
      fields = list(list(
        name = "payload",
        type = "variant",
        nullable = FALSE,
        metadata = list()
      ))
    ),
    auto_unbox = TRUE
  )
  actions <- list(
    list(
      protocol = list(
        minReaderVersion = 3L,
        minWriterVersion = 7L,
        readerFeatures = list("variantType", "variantShredding"),
        writerFeatures = list("variantType", "variantShredding")
      )
    ),
    list(
      metaData = list(
        id = "variant-table",
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

  expect_type(result$payload, "list")
  expect_s3_class(result$payload[[1L]], "data.frame")
  expect_identical(result$payload[[1L]]$action, "checkout")
  expect_identical(result$payload[[1L]]$items[[1L]], 1:3)
  expect_identical(result$payload[[1L]]$user_id, 4471L)
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
          readerFeatures = list(NULL)
        ),
        metaData = list(
          id = "table-id",
          schemaString = "{}",
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
  snapshot <- fabric_delta_resolve_snapshot(table_dir)
  expect_equal(snapshot$version, 10)
  expect_equal(snapshot$checkpoint_version, 10)
  expect_equal(snapshot$active, "part.parquet")

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
      '{"metaData":{"id":"table","configuration":{}}}',
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
  expect_error(
    fabric_delta_validate_type_widening(
      schema,
      "typeWidening-preview"
    ),
    "integer -> long",
    fixed = TRUE,
    class = "fabric_delta_unsupported_error"
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
  expect_false(
    fabric_delta_supported_type_change("byte", "double", preview = TRUE)
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
      )
    ),
    metadata = list(
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

  invalid_variant <- state
  invalid_variant$protocol$readerFeatures <- list("variantShredding")
  expect_error(
    fabric_delta_validate_reader(invalid_variant),
    "without its required variantType",
    fixed = TRUE,
    class = "fabric_delta_unsupported_error"
  )

  state$protocol <- list(minReaderVersion = 1L)
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
    readerFeatures = list("variant")
  )
  expect_error(
    fabric_delta_validate_reader(state),
    "reader feature(s): variant",
    fixed = TRUE,
    class = "fabric_delta_unsupported_error"
  )
  state$protocol$readerFeatures <- list("deletionVectors")
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
    minWriterVersion = 7L
  )
  expect_error(
    fabric_delta_validate_reader(state),
    "without readerFeatures",
    fixed = TRUE,
    class = "fabric_delta_unsupported_error"
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
          name = "payloads",
          type = list(
            type = "array",
            elementType = "variant",
            containsNull = TRUE
          ),
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
      )
    ),
    metadata = list(
      schemaString = schema,
      partitionColumns = list(),
      configuration = list()
    ),
    active = character(),
    files = list()
  )
  expect_invisible(fabric_delta_validate_reader(state))

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
    list(list(add = list(
      path = "part.parquet",
      partitionValues = list(),
      deletionVector = dv_a
    )))
  )
  state <- fabric_delta_apply_actions(
    state,
    list(
      list(remove = list(path = "part.parquet", deletionVector = dv_a)),
      list(add = list(
        path = "part.parquet",
        partitionValues = list(),
        deletionVector = dv_b
      ))
    )
  )

  # A later tombstone for the old logical file must not remove the current DV.
  state <- fabric_delta_apply_actions(
    state,
    list(list(remove = list(
      path = "part.parquet",
      deletionVector = dv_a
    )))
  )
  expect_identical(state$active, "part.parquet")
  expect_identical(
    state$files[["part.parquet"]]$deletionVectorId,
    fabric_delta_deletion_vector_id(dv_b)
  )

  # Checkpoint sidecars are independent and unordered. A stale tombstone in a
  # later sidecar must likewise leave the current logical file active.
  stale_sidecar <- list(remove = data.frame(
    path = "part.parquet",
    deletionVector = I(list(dv_a))
  ))
  state <- fabric_delta_apply_checkpoint(state, stale_sidecar)
  expect_identical(state$active, "part.parquet")

  current_sidecar <- list(remove = data.frame(
    path = "part.parquet",
    deletionVector = I(list(dv_b))
  ))
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
