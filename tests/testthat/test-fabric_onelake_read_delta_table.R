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
    onelake_list_target = function(target, credential, recursive, page_size) {
      listed_target <<- target
      expect_true(recursive)
      expect_equal(page_size, 5000L)
      tibble::tibble(
        path = c(
          "Tables/dbo/table/_delta_log/00000000000000000000.json",
          "Tables/dbo/table/category=A/part.parquet"
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
      downloaded[[length(downloaded) + 1L]] <<- list(
        path = target$path,
        dest = dest,
        overwrite = overwrite
      )
      writeBin(raw(), dest)
      invisible(dest)
    },
    fabric_delta_read_staged = function(table_dir, version = NULL) {
      expect_null(version)
      expect_true(dir.exists(table_dir))
      data.frame(id = 1L)
    }
  )
  dest <- tempfile("delta-shared-transport-")
  on.exit(if (fs::dir_exists(dest)) fs::dir_delete(dest), add = TRUE)

  result <- fabric_onelake_read_delta_table(
    table_path = "table",
    workspace_name = "Analytics",
    lakehouse_name = "Curated",
    schema = "dbo",
    access_token = "token",
    dest_dir = dest,
    verbose = FALSE
  )

  expect_equal(listed_target$item, "Curated.Lakehouse")
  expect_equal(listed_target$path, "Tables/dbo/table")
  expect_equal(
    vapply(downloaded, `[[`, character(1), "path"),
    c(
      "Tables/dbo/table/_delta_log/00000000000000000000.json",
      "Tables/dbo/table/category=A/part.parquet"
    )
  )
  expect_true(all(vapply(downloaded, `[[`, logical(1), "overwrite")))
  expect_equal(result$id, 1L)
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

test_that("Delta versions must be non-negative integers", {
  for (version in list(-1, 1.5, NA_real_, c(1, 2), "1")) {
    expect_error(
      fabric_onelake_read_delta_table(
        table_path = "table",
        workspace_name = "workspace",
        lakehouse_name = "lakehouse",
        access_token = "token",
        version = version,
        verbose = FALSE
      ),
      "version must be a single non-negative integer",
      fixed = TRUE
    )
  }
})

test_that("Delta JSON logs resolve latest and versioned snapshots", {
  table_dir <- fs::path_temp(paste0("delta-json-", sample.int(1e9, 1)))
  log_dir <- fs::path(table_dir, "_delta_log")
  fs::dir_create(log_dir, recurse = TRUE)
  on.exit(fs::dir_delete(table_dir), add = TRUE)

  writeLines(
    c(
      '{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}',
      '{"metaData":{"id":"table","configuration":{}}}',
      '{"add":{"path":"category=A/part.parquet"}}',
      '{"add":{"path":"category=B/part.parquet"}}'
    ),
    fs::path(log_dir, "00000000000000000000.json"),
    useBytes = TRUE
  )
  writeLines(
    c(
      '{"remove":{"path":"category=B/part.parquet"}}',
      '{"add":{"path":"category=B/replacement.parquet"}}'
    ),
    fs::path(log_dir, "00000000000000000001.json"),
    useBytes = TRUE
  )

  latest <- fabric_delta_resolve_snapshot(table_dir)
  original <- fabric_delta_resolve_snapshot(table_dir, version = 0)

  expect_equal(latest$version, 1)
  expect_setequal(
    latest$active,
    c("category=A/part.parquet", "category=B/replacement.parquet")
  )
  expect_setequal(
    original$active,
    c("category=A/part.parquet", "category=B/part.parquet")
  )
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
        remove = list(path = NA_character_),
        protocol = list(
          minReaderVersion = 1L,
          readerFeatures = list(NULL)
        ),
        metaData = list(
          id = "table-id",
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

test_that("Delta reader fails safely for incomplete or unsupported snapshots", {
  state <- list(
    protocol = list(
      minReaderVersion = 3L,
      readerFeatures = list("deletionVectors")
    ),
    metadata = list(configuration = list()),
    has_deletion_vectors = FALSE
  )
  expect_error(
    fabric_delta_validate_reader(state),
    "Unsupported Delta reader protocol version 3",
    fixed = TRUE
  )

  state$protocol <- list(minReaderVersion = 1L)
  state$metadata$configuration <- list(
    "delta.columnMapping.mode" = "name"
  )
  expect_error(
    fabric_delta_validate_reader(state),
    "column mapping mode",
    fixed = TRUE
  )

  state$metadata$configuration <- list()
  state$has_deletion_vectors <- TRUE
  expect_error(
    fabric_delta_validate_reader(state),
    "deletion vectors",
    fixed = TRUE
  )

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
