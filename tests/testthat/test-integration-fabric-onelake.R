# Fabric integration coverage: OneLake files and Delta Lake tables.
# OneLake is Fabric's shared storage layer; these tests use the sandbox
# lakehouse to check file operations and reading current, typed Delta data.

test_that("fabric_onelake_read_delta_table reads schema-enabled Delta data", {
  fabric_test_require_package("duckdb")
  fabric_test_require_package("fs")
  manifest <- fabric_test_manifest()
  lakehouse <- manifest$items$TestLakehouse
  token <- fabric_test_token_provider()
  discovered <- fabric_item(
    manifest$workspace_id,
    lakehouse$id,
    type = "Lakehouse",
    token = fabric_test_token("FABRIC_TEST_API_TOKEN")
  )

  result <- fabric_onelake_read_delta_table(
    table_path = lakehouse$tables$basic,
    workspace_name = manifest$workspace_id,
    lakehouse_name = discovered,
    tenant_id = "",
    client_id = "",
    token = token,
    verbose = FALSE
  )
  result <- result[order(result$id), ]

  expect_s3_class(result, "tbl_df")
  expect_named(
    result,
    c("id", "name", "category", "amount", "loaded_at"),
    ignore.order = TRUE
  )
  expect_equal(nrow(result), 3L)
  expect_equal(result$id, c(1L, 2L, 3L))
  expect_equal(result$name, c("alpha", "beta", "gamma"))
  expect_equal(result$category, c("A", "B", "A"))
  expect_equal(result$amount, c(10.5, 20, NA))
  expect_true(inherits(result$loaded_at, "POSIXct"))
  expect_equal(
    as.numeric(result$loaded_at),
    rep(as.numeric(as.POSIXct("2026-01-01", tz = "UTC")), 3)
  )
})

test_that("Delta reader preserves empty schemas and typed log partitions", {
  fabric_test_require_package("duckdb")
  fabric_test_require_package("fs")
  manifest <- fabric_test_manifest()
  lakehouse <- fabric_test_manifest_item(manifest, "TestLakehouse")
  token <- fabric_test_token("FABRIC_TEST_STORAGE_TOKEN")
  read_table <- function(table) {
    fabric_onelake_read_delta_table(
      table_path = table,
      workspace_name = manifest$workspace_id,
      lakehouse_name = lakehouse$id,
      schema = lakehouse$schema,
      token = token,
      verbose = FALSE
    )
  }

  empty <- read_table(lakehouse$tables$empty)
  expect_s3_class(empty, "tbl_df")
  expect_equal(nrow(empty), 0L)
  expect_named(empty, c("id", "name", "category", "amount"))
  expect_type(empty$id, "integer")
  expect_type(empty$name, "character")
  expect_type(empty$category, "character")
  expect_type(empty$amount, "double")

  partitioned <- read_table(lakehouse$tables$typed_partitions)
  partitioned <- partitioned[order(partitioned$id), ]
  expect_equal(partitioned$id, 1:3)
  expect_equal(partitioned$name, c("alpha", "beta", "gamma"))
  expect_equal(partitioned$amount, c(10.5, 20, NA))
  expect_s3_class(partitioned$event_date, "Date")
  expect_equal(
    partitioned$event_date,
    as.Date(c("2026-01-01", "2026-01-02", NA))
  )
  expect_type(partitioned$active, "logical")
  expect_equal(partitioned$active, c(TRUE, FALSE, NA))
})

test_that("OneLake file helpers cover hierarchy, ranges, conflicts, and Unicode", {
  manifest <- fabric_test_manifest()
  lakehouse <- fabric_test_manifest_item(manifest, "TestLakehouse")
  token <- fabric_test_token_provider()

  fixtures <- fabric_onelake_list(
    manifest$workspace_id,
    lakehouse$id,
    path = "Files/fixtures/nested",
    recursive = TRUE,
    page_size = 2L,
    token = token
  )
  expect_setequal(
    fixtures$path[!fixtures$is_directory],
    c(
      "Files/fixtures/nested/a/duplicate.txt",
      "Files/fixtures/nested/b/duplicate.txt",
      "Files/fixtures/nested/unicode/café-数据.txt"
    )
  )
  duplicate_paths <- fixtures$path[fixtures$name == "duplicate.txt"]
  expect_setequal(
    duplicate_paths,
    c(
      "Files/fixtures/nested/a/duplicate.txt",
      "Files/fixtures/nested/b/duplicate.txt"
    )
  )
  unicode_path <- "Files/fixtures/nested/unicode/café-数据.txt"
  expect_true(unicode_path %in% fixtures$path)

  unicode_metadata <- fabric_onelake_metadata(
    paste0(
      "abfss://",
      manifest$workspace_id,
      "@onelake.dfs.fabric.microsoft.com/",
      lakehouse$id,
      "/",
      unicode_path
    ),
    token = token
  )
  expect_false(unicode_metadata$is_directory)
  expect_true(nzchar(unicode_metadata$etag))
  expect_gt(unicode_metadata$content_length, 0)
  unicode_contents <- fabric_onelake_download(
    manifest$workspace_id,
    lakehouse$id,
    unicode_path,
    token = token
  )
  expect_identical(
    trimws(rawToChar(unicode_contents)),
    "OneLake Unicode fixture"
  )

  ranged <- fabric_onelake_download(
    manifest$workspace_id,
    lakehouse$id,
    "Files/fixtures/nested/a/duplicate.txt",
    range = c(0, 4),
    token = token
  )
  expect_identical(rawToChar(ranged), "alpha")

  test_root <- paste0(
    "Files/fabricqueryr-tests/one-lake-",
    gsub("[^A-Za-z0-9]", "", manifest$workspace_id),
    "-",
    format(Sys.time(), "%Y%m%d%H%M%S"),
    "-",
    Sys.getpid()
  )
  test_path <- paste0(test_root, "/nested/échantillon-数据.txt")
  on.exit(
    try(
      fabric_onelake_delete(
        manifest$workspace_id,
        lakehouse$id,
        test_root,
        recursive = TRUE,
        confirm = TRUE,
        token = token
      ),
      silent = TRUE
    ),
    add = TRUE
  )

  fabric_onelake_upload(
    manifest$workspace_id,
    lakehouse$id,
    test_path,
    source = charToRaw("first-version"),
    chunk_size = 3,
    content_type = "text/plain; charset=utf-8",
    token = token
  )
  first <- fabric_onelake_metadata(
    manifest$workspace_id,
    lakehouse$id,
    test_path,
    token = token
  )
  expect_equal(first$content_length, nchar("first-version", type = "bytes"))
  expect_true(nzchar(first$etag))
  expect_match(first$content_type, "^text/plain")

  expect_error(
    fabric_onelake_upload(
      manifest$workspace_id,
      lakehouse$id,
      test_path,
      source = charToRaw("conflict"),
      token = token
    ),
    "HTTP (409|412)"
  )
  fabric_onelake_upload(
    manifest$workspace_id,
    lakehouse$id,
    test_path,
    source = charToRaw("second-version"),
    overwrite = TRUE,
    if_match = first$etag,
    token = token
  )
  second <- fabric_onelake_metadata(
    manifest$workspace_id,
    lakehouse$id,
    test_path,
    token = token
  )
  expect_true(nzchar(second$etag))
  expect_false(identical(second$etag, first$etag))
  expect_error(
    fabric_onelake_upload(
      manifest$workspace_id,
      lakehouse$id,
      test_path,
      source = charToRaw("stale-write"),
      overwrite = TRUE,
      if_match = first$etag,
      token = token
    ),
    "HTTP 412"
  )
  expect_identical(
    rawToChar(fabric_onelake_download(
      manifest$workspace_id,
      lakehouse$id,
      test_path,
      token = token
    )),
    "second-version"
  )
  expect_identical(
    rawToChar(fabric_onelake_download(
      manifest$workspace_id,
      lakehouse$id,
      test_path,
      range = c(7, 13),
      token = token
    )),
    "version"
  )

  empty_path <- paste0(test_root, "/empty.bin")
  empty_metadata <- fabric_onelake_upload(
    manifest$workspace_id,
    lakehouse$id,
    empty_path,
    source = raw(),
    token = token
  )
  expect_equal(empty_metadata$content_length, 0)
  expect_length(
    fabric_onelake_download(
      manifest$workspace_id,
      lakehouse$id,
      empty_path,
      token = token
    ),
    0L
  )

  expect_error(
    fabric_onelake_delete(
      manifest$workspace_id,
      lakehouse$id,
      test_root,
      recursive = TRUE,
      token = token
    ),
    "disabled by default",
    fixed = TRUE
  )
  expect_true(fabric_onelake_delete(
    manifest$workspace_id,
    lakehouse$id,
    test_root,
    recursive = TRUE,
    confirm = TRUE,
    token = token
  ))
})

test_that("fabric_onelake_read_delta_table resolves Delta removals and partitions", {
  fabric_test_require_package("duckdb")
  fabric_test_require_package("fs")
  manifest <- fabric_test_manifest()
  lakehouse <- manifest$items$TestLakehouse
  dest_dir <- tempfile("fabricqueryr-integration-")
  on.exit(
    if (fs::dir_exists(dest_dir)) {
      fs::dir_delete(dest_dir)
    },
    add = TRUE
  )

  result <- fabric_onelake_read_delta_table(
    table_path = lakehouse$tables$partitioned,
    workspace_name = manifest$workspace_id,
    lakehouse_name = lakehouse$id,
    schema = lakehouse$schema,
    tenant_id = "",
    client_id = "",
    token = fabric_test_token("FABRIC_TEST_STORAGE_TOKEN"),
    dest_dir = dest_dir,
    verbose = FALSE
  )
  id_counts <- table(result$id)
  replaced <- result[result$id == 2L, ]

  expect_s3_class(result, "tbl_df")
  expect_true("category" %in% names(result))
  expect_equal(nrow(result), 13L)
  expect_equal(as.integer(id_counts[c("1", "2", "3")]), c(11L, 1L, 1L))
  expect_equal(sort(unique(result$category)), c("A", "B"))
  expect_equal(replaced$name, "beta-updated")
  expect_equal(replaced$amount, 21)
  expect_true(fs::dir_exists(fs::path(dest_dir, "category=A")))
  expect_true(fs::dir_exists(fs::path(dest_dir, "category=B")))
  expect_gt(
    length(fs::dir_ls(dest_dir, recurse = TRUE, regexp = "\\.parquet$")),
    0L
  )
  expect_true(
    any(fs::file_exists(
      fs::dir_ls(
        fs::path(dest_dir, "_delta_log"),
        regexp = "checkpoint.*\\.parquet$"
      )
    ))
  )

  historical <- fabric_onelake_read_delta_table(
    table_path = lakehouse$tables$partitioned,
    workspace_name = manifest$workspace_id,
    lakehouse_name = lakehouse$id,
    schema = lakehouse$schema,
    token = fabric_test_token("FABRIC_TEST_STORAGE_TOKEN"),
    version = 10,
    verbose = FALSE
  )
  historical_beta <- historical[historical$id == 2L, ]
  expect_equal(historical_beta$name, "beta")
  expect_equal(historical_beta$amount, 20)
})

test_that("Delta reader covers current Fabric Delta reader features", {
  fabric_test_require_package("duckdb")
  fabric_test_require_package("fs")
  manifest <- fabric_test_manifest()
  lakehouse <- manifest$items$TestLakehouse
  token <- fabric_test_token("FABRIC_TEST_STORAGE_TOKEN")
  read_table <- function(table) {
    fabric_onelake_read_delta_table(
      table_path = table,
      workspace_name = manifest$workspace_id,
      lakehouse_name = lakehouse$id,
      schema = lakehouse$schema,
      token = token,
      verbose = FALSE
    )
  }

  evolved <- read_table(lakehouse$tables$schema_evolved)
  evolved <- evolved[order(evolved$id), ]
  expect_equal(evolved$id, 1:3)
  expect_true("evolved_value" %in% names(evolved))
  expect_true(all(is.na(evolved$evolved_value[1:2])))
  expect_equal(evolved$evolved_value[[3L]], "introduced")

  column_mapped <- read_table(lakehouse$tables$column_mapped)
  column_mapped <- column_mapped[order(column_mapped$id), ]
  expect_equal(column_mapped$id, 1:3)
  expect_equal(column_mapped$name, c("alpha", "beta", "gamma"))

  deletion_vectors <- read_table(lakehouse$tables$deletion_vectors)
  deletion_vectors <- deletion_vectors[order(deletion_vectors$id), ]
  expect_equal(deletion_vectors$id, 2:3)
  expect_equal(deletion_vectors$name, c("beta", "gamma"))

  type_widened <- read_table(lakehouse$tables$type_widened)
  type_widened <- type_widened[order(type_widened$id), ]
  expect_equal(type_widened$id, c(1L, 127L, 128L))
  expect_equal(
    type_widened$label,
    c("before", "tinyint-limit", "after")
  )
})
