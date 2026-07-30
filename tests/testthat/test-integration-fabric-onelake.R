# Fabric integration coverage: OneLake files and Delta Lake tables.
# OneLake is Fabric's shared storage layer; these tests use the sandbox
# lakehouse to check file operations and reading current, typed Delta data.

test_that("fabric_onelake_read_delta_table reads schema-enabled Delta data", {
  fabric_test_require_package("duckdb")
  fabric_test_require_package("fs")
  fabric_test_require_package("arrow")
  fabric_test_require_package("nanoarrow")
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

  projected <- fabric_onelake_read_delta_table(
    table_path = lakehouse$tables$basic,
    workspace_name = manifest$workspace_id,
    lakehouse_name = discovered,
    token = token,
    columns = c("name", "id"),
    limit = 2,
    result = "arrow_stream",
    verbose = FALSE
  )
  expect_s3_class(projected, "nanoarrow_array_stream")
  projected <- as.data.frame(
    arrow::as_record_batch_reader(projected)$read_table()
  )
  expect_named(projected, c("name", "id"))
  expect_equal(nrow(projected), 2L)
  expect_true(all(projected$id %in% 1:3))
  expect_identical(
    projected$name,
    c("alpha", "beta", "gamma")[projected$id]
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
      timestamp_partition_timezone = "UTC",
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

  void <- read_table(lakehouse$tables$void)
  void <- void[order(void$id), ]
  expect_equal(void$id, 0:2)
  expect_type(void$always_null, "logical")
  expect_true(all(is.na(void$always_null)))
  expect_s3_class(void$details, "data.frame")
  expect_equal(void$details$value, 0:2)
  expect_type(void$details$pending, "logical")
  expect_true(all(is.na(void$details$pending)))

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
  expect_equal(partitioned$integer_part, c(10L, 20L, NA_integer_))
  expect_identical(
    partitioned$decimal_part,
    c("12.30", "-0.50", NA_character_)
  )
  expect_s3_class(partitioned$timestamp_part, "POSIXct")
  expect_equal(
    as.numeric(partitioned$timestamp_part),
    as.numeric(as.POSIXct(
      c(
        "2026-01-01 12:34:56.123456",
        "1969-12-31 23:59:59.000001",
        NA
      ),
      tz = "UTC"
    )),
    tolerance = 1e-6
  )
  expect_s3_class(partitioned$timestamp_ntz_part, "POSIXct")
  expect_equal(
    as.numeric(partitioned$timestamp_ntz_part),
    as.numeric(as.POSIXct(
      c(
        "2026-07-28 09:08:07.654321",
        "1900-01-01 00:00:00.000001",
        NA
      ),
      tz = "UTC"
    )),
    tolerance = 2e-6
  )
  expect_identical(
    partitioned$binary_part,
    list(as.raw(c(1L, 2L, 3L)), as.raw(127L), NULL)
  )
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
  expect_error(
    fabric_onelake_read_delta_table(
      table_path = lakehouse$tables$basic,
      workspace_name = manifest$workspace_id,
      lakehouse_name = lakehouse$id,
      schema = lakehouse$schema,
      token = fabric_test_token("FABRIC_TEST_STORAGE_TOKEN"),
      dest_dir = dest_dir,
      verbose = FALSE
    ),
    "dest_dir must be a new or empty directory",
    fixed = TRUE
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
  expect_named(
    column_mapped,
    c("id", "display_name", "profile", "items", "attributes")
  )
  expect_equal(
    column_mapped$display_name,
    c("alpha", "beta", "gamma")
  )
  expect_s3_class(column_mapped$profile, "data.frame")
  expect_identical(
    column_mapped$profile$display_label,
    c("alpha", "beta", "gamma")
  )
  expect_false("obsolete" %in% names(column_mapped$profile))
  expect_identical(
    lapply(column_mapped$items, function(value) value$label),
    list("alpha", "beta", "gamma")
  )
  expect_identical(
    lapply(
      column_mapped$attributes,
      function(value) value$value$label
    ),
    list("alpha", "beta", "gamma")
  )

  column_mapped_id <- read_table(lakehouse$tables$column_mapped_id)
  column_mapped_id <- column_mapped_id[order(column_mapped_id$id), ]
  expect_equal(column_mapped_id$id, 1:3)
  expect_named(
    column_mapped_id,
    c("id", "display_name", "profile", "items")
  )
  expect_equal(
    column_mapped_id$display_name,
    c("alpha", "beta", "gamma")
  )
  expect_identical(
    column_mapped_id$profile$display_label,
    c("alpha", "beta", "gamma")
  )
  expect_identical(
    lapply(column_mapped_id$items, function(value) value$label),
    list("alpha", "beta", "gamma")
  )

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

test_that("Delta reader preserves exact and complex Fabric values", {
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

  exact <- read_table(lakehouse$tables$exact_types)
  expect_s3_class(exact$above_double_limit, "integer64")
  expect_s3_class(exact$maximum_long, "integer64")
  expect_identical(
    as.character(exact$above_double_limit),
    "9007199254740993"
  )
  expect_identical(
    as.character(exact$maximum_long),
    "9223372036854775807"
  )
  expect_identical(
    exact$whole_decimal,
    "12345678901234567890123456789012345678"
  )
  expect_identical(
    exact$scaled_decimal,
    "123456789012345678901234567890123456.78"
  )
  expect_s3_class(exact$observed_at, "POSIXct")
  expect_equal(
    as.numeric(exact$observed_at),
    as.numeric(as.POSIXct("2026-07-28 12:34:56.123456", tz = "UTC")),
    tolerance = 1e-6
  )
  expect_identical(exact$payload[[1L]], as.raw(c(0L, 255L, 16L)))
  expect_identical(exact$unicode_text, "café-数据-🙂")
  expect_true(is.nan(exact$not_a_number))
  expect_identical(exact$positive_infinity, Inf)

  complex <- read_table(lakehouse$tables$complex_types)
  expect_named(
    complex,
    c(
      "id",
      "profile",
      "scores",
      "counts",
      "items",
      "attributes",
      "display name"
    )
  )
  expect_s3_class(complex$profile, "data.frame")
  expect_identical(complex$profile$label, "nested")
  expect_identical(
    complex$profile$amount,
    "1234567890123456789012345678901234.56"
  )
  expect_identical(complex$scores[[1L]], 1:3)
  expect_identical(complex$counts[[1L]]$key, c("large", "small"))
  expect_s3_class(complex$counts[[1L]]$value, "integer64")
  expect_identical(
    as.character(complex$counts[[1L]]$value),
    c("9007199254740993", "2")
  )
  expect_identical(complex$items[[1L]]$label, c("first", "second"))
  expect_identical(complex$items[[1L]]$score, c(10L, 20L))
  expect_identical(complex$attributes[[1L]]$key, "primary")
  expect_identical(complex$attributes[[1L]]$value$label, "mapped")
  expect_identical(complex$attributes[[1L]]$value$enabled, TRUE)
  expect_identical(complex[["display name"]], "display café-数据")
})

test_that("R Delta results agree with delta-rs on Fabric tables", {
  fabric_test_require_package("duckdb")
  fabric_test_require_package("fs")
  fabric_test_require_delta_oracle()
  manifest <- fabric_test_manifest()
  lakehouse <- manifest$items$TestLakehouse
  token <- fabric_test_token("FABRIC_TEST_STORAGE_TOKEN")

  cases <- list(
    list(
      name = "basic",
      key = "oracle_basic",
      item = lakehouse,
      table = lakehouse$tables$oracle_basic,
      expected_rows = 3
    ),
    list(
      name = "basic_projection",
      key = "oracle_basic",
      item = lakehouse,
      table = lakehouse$tables$oracle_basic,
      columns = c("name", "id", "amount"),
      expected_rows = 3
    ),
    list(
      name = "empty",
      key = "oracle_empty",
      item = lakehouse,
      table = lakehouse$tables$oracle_empty,
      expected_rows = 0,
      min_active_files = 0
    ),
    list(
      name = "typed_partitions",
      key = "oracle_typed_partitions",
      item = lakehouse,
      table = lakehouse$tables$oracle_typed_partitions,
      expected_rows = 3,
      reader_features = "timestampNtz",
      timestamp_partition_timezone = "UTC",
      partition_columns = c(
        "event_date",
        "active",
        "integer_part",
        "decimal_part",
        "timestamp_part",
        "timestamp_ntz_part",
        "binary_part"
      )
    ),
    list(
      name = "partitioned",
      key = "oracle_partitioned",
      item = lakehouse,
      table = lakehouse$tables$oracle_partitioned,
      expected_rows = 13,
      min_active_files = 2,
      partition_columns = "category"
    ),
    list(
      name = "partitioned_version_10",
      key = "oracle_partitioned",
      item = lakehouse,
      table = lakehouse$tables$oracle_partitioned,
      version = 10,
      expected_rows = 13,
      min_active_files = 2,
      partition_columns = "category"
    ),
    list(
      name = "schema_evolved",
      key = "oracle_schema_evolved",
      item = lakehouse,
      table = lakehouse$tables$oracle_schema_evolved,
      expected_rows = 3,
      min_active_files = 2
    ),
    list(
      name = "schema_evolved_version_0",
      key = "oracle_schema_evolved",
      item = lakehouse,
      table = lakehouse$tables$oracle_schema_evolved,
      version = 0,
      expected_rows = 2
    ),
    list(
      name = "exact_types",
      key = "oracle_exact_types",
      item = lakehouse,
      table = lakehouse$tables$oracle_exact_types,
      expected_rows = 1,
      reader_features = "timestampNtz"
    ),
    list(
      name = "complex_types",
      key = "oracle_complex_types",
      item = lakehouse,
      table = lakehouse$tables$oracle_complex_types,
      expected_rows = 1
    )
  )

  exclusions <- c(
    basic = paste(
      "Fabric Runtime 2.0 enables deletionVectors by default;",
      "the equivalent oracle_basic table disables it for delta-rs parity"
    ),
    empty = paste(
      "Fabric Runtime 2.0 enables deletionVectors by default;",
      "the equivalent oracle_empty table disables it for delta-rs parity"
    ),
    typed_partitions = paste(
      "Fabric Runtime 2.0 enables deletionVectors by default and delta-rs",
      "misparses negative decimal partition paths; the oracle mirror covers",
      "all other typed partition parity while direct R assertions cover -0.50"
    ),
    partitioned = paste(
      "Fabric Runtime 2.0 enables deletionVectors by default;",
      "the equivalent oracle_partitioned table covers parity and history"
    ),
    schema_evolved = paste(
      "Fabric Runtime 2.0 enables deletionVectors by default;",
      "the equivalent oracle_schema_evolved table covers parity and history"
    ),
    column_mapped = paste(
      "Fabric documents column mapping as unsupported by its pinned",
      "delta-rs reader; direct R assertions cover nested renames and drops"
    ),
    column_mapped_id = paste(
      "Fabric documents column mapping as unsupported by its pinned",
      "delta-rs reader; direct R assertions cover ID mapping"
    ),
    exact_types = paste(
      "Fabric Runtime 2.0 enables deletionVectors by default;",
      "the equivalent oracle_exact_types table covers parity"
    ),
    complex_types = paste(
      "the Fabric table combines default deletionVectors with name mapping;",
      "oracle_complex_types covers the same nested logical value kinds"
    ),
    shallow_clone = paste(
      "the clone inherits a Runtime 2.0 deletionVectors protocol;",
      "the R reader has direct clone assertions"
    ),
    void = paste(
      "Spark legacy void has no portable delta-rs/PyArrow value type;",
      "the R reader has direct unit and Fabric assertions"
    ),
    deletion_vectors_checkpoint = paste(
      "this combines deletion vectors with V2 checkpoint sidecars;",
      "delta-rs 1.6 does not claim V2 checkpoint support"
    ),
    deletion_vectors = paste(
      "delta-rs 1.6 rejects the deletionVectors reader feature;",
      "the R reader has direct exact-row Fabric assertions"
    ),
    deletion_vectors_stress = paste(
      "delta-rs 1.6 rejects the deletionVectors reader feature;",
      "the R reader has direct mutation and exact-row Fabric assertions"
    ),
    deletion_vectors_dense = paste(
      "delta-rs 1.6 rejects the deletionVectors reader feature;",
      "the R reader has a direct 85,000-row Fabric assertion"
    ),
    type_widened = paste(
      "delta-rs 1.6 does not claim Delta typeWidening support;",
      "the R reader has direct unit and Fabric assertions"
    ),
    type_widened_exact = paste(
      "delta-rs 1.6 does not claim exact/date typeWidening support;",
      "the R reader has direct unit and Fabric assertions"
    ),
    type_widened_nested = paste(
      "delta-rs 1.6 does not claim nested typeWidening support;",
      "the R reader has direct unit and Fabric assertions"
    ),
    v2_checkpoint = paste(
      "delta-rs 1.6 does not claim UUID V2 checkpoint-sidecar support;",
      "the R reader replays this in direct unit and Fabric tests"
    ),
    variant = paste(
      "delta-rs 1.6 has preliminary Variant support but does not claim",
      "Fabric mixed shredded/unshredded Variant parity"
    ),
    livy_batch_result = "created by the Livy job suite, not the seed matrix",
    spark_job_result = "created by the item-job suite, not the seed matrix"
  )
  non_lakehouse_exclusions <- c(
    warehouse_export = paste(
      "Fabric Warehouse exports require deletionVectors and name mapping;",
      "the R reader has direct publication and exact-row assertions"
    )
  )
  compared_keys <- unique(vapply(cases, `[[`, character(1), "key"))
  compared_keys <- compared_keys[!is.na(compared_keys)]
  expect_setequal(
    names(lakehouse$tables),
    c(compared_keys, names(exclusions))
  )
  expect_true(all(nzchar(exclusions)))
  expect_true(all(nzchar(non_lakehouse_exclusions)))

  for (case in cases) {
    version <- case$version %||% NULL
    columns <- case$columns %||% NULL
    schema <- case$schema %||% case$item$schema %||% "dbo"
    actual <- fabric_onelake_read_delta_table(
      table_path = case$table,
      workspace_name = manifest$workspace_id,
      lakehouse_name = case$item$id,
      schema = schema,
      token = token,
      version = version,
      columns = columns,
      timestamp_partition_timezone =
        case$timestamp_partition_timezone %||% NULL,
      verbose = FALSE
    )
    oracle <- fabric_test_delta_oracle_read(
      fabric_test_delta_oracle_uri(
        manifest,
        case$item,
        case$table,
        schema = schema
      ),
      version = version,
      columns = columns
    )
    fabric_test_expect_delta_oracle_equal(
      actual,
      oracle,
      info = case$name
    )
    expect_equal(
      nrow(actual),
      case$expected_rows,
      info = case$name
    )
    fabric_test_expect_delta_oracle_profile(
      oracle,
      version = version,
      reader_features = case$reader_features %||% character(),
      partition_columns = case$partition_columns %||% NULL,
      column_mapping_mode = case$column_mapping_mode %||% NULL,
      min_active_files = case$min_active_files %||%
        if (case$expected_rows == 0) 0 else 1,
      info = case$name
    )
    expect_false(
      "deletionVectors" %in%
        unlist(
          fabric_test_delta_oracle_metadata(oracle)$reader_features,
          use.names = FALSE
        ),
      info = case$name
    )
  }
})

test_that("Delta reader handles DV stress and exact widening", {
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

  deletion_vectors <- read_table(
    lakehouse$tables$deletion_vectors_stress
  )
  ids <- as.numeric(deletion_vectors$id)
  expect_equal(nrow(deletion_vectors), 4501L)
  expect_false(any(ids %% 10 == 0 & ids < 5000))
  expect_true(all(c(1, 4999, 5000) %in% ids))
  expect_identical(
    deletion_vectors$label[ids == 1],
    "merged"
  )
  expect_identical(
    deletion_vectors$label[ids == 5000],
    "inserted"
  )
  updated <- ids %% 13 == 0 & ids %% 10 != 0 & ids < 5000
  expect_true(all(deletion_vectors$label[updated] == "updated"))

  checkpoint_stage <- tempfile("fabricqueryr-dv-checkpoint-")
  on.exit(
    if (fs::dir_exists(checkpoint_stage)) fs::dir_delete(checkpoint_stage),
    add = TRUE
  )
  checkpoint_dv <- fabric_onelake_read_delta_table(
    table_path = lakehouse$tables$deletion_vectors_checkpoint,
    workspace_name = manifest$workspace_id,
    lakehouse_name = lakehouse$id,
    schema = lakehouse$schema,
    token = token,
    dest_dir = checkpoint_stage,
    verbose = FALSE
  )
  checkpoint_ids <- as.numeric(checkpoint_dv$id)
  expect_equal(nrow(checkpoint_dv), 800L)
  expect_equal(
    sort(checkpoint_ids),
    setdiff(0:999, c(seq(0, 990, 10), seq(1, 991, 10)))
  )
  checkpoint_snapshot <- fabric_delta_resolve_snapshot(checkpoint_stage)
  expect_true(
    "v2Checkpoint" %in%
      unlist(checkpoint_snapshot$protocol$readerFeatures)
  )
  expect_true(checkpoint_snapshot$has_deletion_vectors)

  dense_stage <- tempfile("fabricqueryr-dv-dense-")
  on.exit(
    if (fs::dir_exists(dense_stage)) fs::dir_delete(dense_stage),
    add = TRUE
  )
  dense <- fabric_onelake_read_delta_table(
    table_path = lakehouse$tables$deletion_vectors_dense,
    workspace_name = manifest$workspace_id,
    lakehouse_name = lakehouse$id,
    schema = lakehouse$schema,
    token = token,
    dest_dir = dense_stage,
    verbose = FALSE
  )
  dense_ids <- sort(as.numeric(dense$id))
  expected_dense <- setdiff(
    0:99999,
    c(seq(0, 9998, 2), 70000:79999)
  )
  expect_equal(nrow(dense), 85000L)
  expect_equal(dense_ids, expected_dense)
  dense_snapshot <- fabric_delta_resolve_snapshot(dense_stage)
  dense_descriptors <- Filter(
    Negate(is.null),
    lapply(
      dense_snapshot$active,
      function(path) dense_snapshot$files[[path]]$deletionVector
    )
  )
  expect_length(dense_descriptors, 1L)
  expect_equal(as.numeric(dense_descriptors[[1L]]$cardinality), 15000)

  widened <- read_table(lakehouse$tables$type_widened_exact)
  widened <- widened[order(widened$label), ]
  expect_s3_class(widened$id, "integer64")
  expect_identical(
    sort(as.character(widened$id)),
    c("1", "9007199254740993")
  )
  expect_identical(
    widened$amount,
    c("1234567890.1234", "12.3400")
  )
  expect_s3_class(widened$occurred, "POSIXct")
  expect_equal(
    as.numeric(widened$occurred),
    c(
      as.numeric(as.POSIXct("2026-07-28 12:34:56.123456", tz = "UTC")),
      as.numeric(as.POSIXct("2026-01-01", tz = "UTC"))
    ),
    tolerance = 1e-6
  )

  nested <- read_table(lakehouse$tables$type_widened_nested)
  nested <- nested[order(nested$id), ]
  expect_s3_class(nested$nested$count, "integer64")
  expect_identical(
    as.character(nested$nested$count),
    c("10", "9007199254740993", "11")
  )
  expect_equal(nested$nested$ratio, c(1.5, 2.5, 3.5))
  expect_identical(
    lapply(nested$readings, as.character),
    list(
      c("1", "2"),
      c("9007199254740993", "4"),
      c("5", "6")
    )
  )
  expect_identical(
    lapply(nested$lookup, function(value) as.character(value$value)),
    list("3", "9007199254740993", "7")
  )
  expect_identical(
    nested$decimal_value,
    c("123.00", "456.00", "9007199254.25")
  )
  expect_equal(nested$double_value, c(7, 8, 9.5))
})

test_that("Delta reader handles Fabric V2 checkpoints and shallow clones", {
  fabric_test_require_package("duckdb")
  fabric_test_require_package("fs")
  manifest <- fabric_test_manifest()
  lakehouse <- manifest$items$TestLakehouse
  token <- fabric_test_token("FABRIC_TEST_STORAGE_TOKEN")

  v2_stage <- tempfile("fabricqueryr-v2-")
  on.exit(
    if (fs::dir_exists(v2_stage)) fs::dir_delete(v2_stage),
    add = TRUE
  )
  v2 <- fabric_onelake_read_delta_table(
    table_path = lakehouse$tables$v2_checkpoint,
    workspace_name = manifest$workspace_id,
    lakehouse_name = lakehouse$id,
    schema = lakehouse$schema,
    token = token,
    dest_dir = v2_stage,
    verbose = FALSE
  )
  expect_equal(nrow(v2), 1000L)
  expect_equal(sort(unique(v2$batch)), 0:3)
  checkpoint_files <- fs::dir_ls(
    fs::path(v2_stage, "_delta_log"),
    recurse = TRUE,
    type = "file"
  )
  expect_true(any(grepl("\\.checkpoint\\.", basename(checkpoint_files))))
  expect_true(
    any(grepl("[/\\\\]_sidecars[/\\\\]", checkpoint_files))
  )
  expect_true(
    any(grepl(
      "checkpoint\\.[0-9a-f-]{36}\\.(json|parquet)$",
      basename(checkpoint_files)
    ))
  )
  snapshot <- fabric_delta_resolve_snapshot(v2_stage)
  expect_gt(snapshot$checkpoint_version, 0)
  expect_identical(
    tolower(snapshot$metadata$format$provider),
    "parquet"
  )
  expect_length(
    fabric_delta_partition_values(snapshot$metadata$format$options),
    0L
  )
  expect_true(
    "v2Checkpoint" %in% unlist(snapshot$protocol$readerFeatures)
  )
  checkpoint_sets <- fabric_delta_checkpoint_sets(checkpoint_files)
  selected_set <- checkpoint_sets[[
    which(vapply(
      checkpoint_sets,
      function(candidate) {
        identical(candidate$version, snapshot$checkpoint_version)
      },
      logical(1)
    ))[[1L]]
  ]]
  expect_gte(length(selected_set$alternatives), 1L)
  referenced_sidecars <- unique(unlist(lapply(
    selected_set$alternatives,
    function(candidate) {
      fabric_delta_checkpoint_sidecar_paths(candidate$paths)
    }
  )))
  expect_gt(length(referenced_sidecars), 0L)
  staged_sidecars <- basename(checkpoint_files[
    grepl("[/\\\\]_sidecars[/\\\\]", checkpoint_files)
  ])
  expect_true(all(referenced_sidecars %in% staged_sidecars))

  clone <- fabric_onelake_read_delta_table(
    table_path = lakehouse$tables$shallow_clone,
    workspace_name = manifest$workspace_id,
    lakehouse_name = lakehouse$id,
    schema = lakehouse$schema,
    token = token,
    verbose = FALSE
  )
  clone <- clone[order(clone$id), ]
  expect_equal(clone$id, 1:3)
  expect_identical(clone$name, c("alpha", "beta", "gamma"))
})

test_that("Delta reader exposes Fabric Variant physical values", {
  fabric_test_require_package("duckdb")
  fabric_test_require_package("fs")
  manifest <- fabric_test_manifest()
  lakehouse <- manifest$items$TestLakehouse
  token <- fabric_test_token("FABRIC_TEST_STORAGE_TOKEN")
  result <- fabric_onelake_read_delta_table(
    table_path = lakehouse$tables$variant,
    workspace_name = manifest$workspace_id,
    lakehouse_name = lakehouse$id,
    schema = lakehouse$schema,
    token = token,
    verbose = FALSE
  )

  result <- result[order(as.numeric(result$event_id)), ]
  expect_s3_class(result$event_id, "integer64")
  expect_identical(as.character(result$event_id), as.character(1:9))
  expect_type(result$data, "list")
  expect_s3_class(result$data[[1L]], "fabric_delta_variant")
  expect_match(result$data[[1L]]$type, "^OBJECT")
  expect_match(result$data[[1L]]$display, "checkout", fixed = TRUE)
  expect_null(result$data[[2L]])
  expect_s3_class(result$data[[3L]], "fabric_delta_variant")
  expect_identical(result$data[[3L]]$type, "VARIANT_NULL")
  expect_identical(result$data[[3L]]$value, as.raw(0L))
  expect_identical(result$data[[4L]]$type, "INT64")
  expect_identical(result$data[[4L]]$display, "9007199254740993")
  expect_match(result$data[[5L]]$display, "refund", fixed = TRUE)
  expect_match(result$data[[5L]]$display, "9007199254740993", fixed = TRUE)
  expect_match(result$data[[6L]]$type, "^ARRAY")
  expect_identical(result$data[[7L]]$type, "VARCHAR")
  expect_identical(result$data[[7L]]$display, "root string")
  expect_match(result$data[[8L]]$display, "café-数据-🙂", fixed = TRUE)
  expect_match(result$data[[8L]]$display, "1234567890.125", fixed = TRUE)
  expect_identical(result$data[[9L]]$type, "DECIMAL(38, 2)")
  expect_identical(
    result$data[[9L]]$display,
    "123456789012345678901234567890123456.78"
  )
  expect_type(result$data[[9L]]$metadata, "raw")
  expect_type(result$data[[9L]]$value, "raw")

  stream <- fabric_onelake_read_delta_table(
    table_path = lakehouse$tables$variant,
    workspace_name = manifest$workspace_id,
    lakehouse_name = lakehouse$id,
    schema = lakehouse$schema,
    token = token,
    result = "arrow_stream",
    verbose = FALSE
  )
  arrow_result <- as.data.frame(
    arrow::as_record_batch_reader(stream)$read_table()
  )
  arrow_result <- arrow_result[order(as.numeric(arrow_result$event_id)), ]
  expect_s3_class(arrow_result$data, "data.frame")
  expect_named(
    arrow_result$data,
    c("type", "display", "metadata", "value")
  )
  expect_true(all(vapply(
    arrow_result$data[2L, ],
    function(value) {
      is.null(value[[1L]]) || isTRUE(is.na(value[[1L]]))
    },
    logical(1)
  )))
  expect_identical(arrow_result$data$type[[3L]], "VARIANT_NULL")
  expect_identical(
    arrow_result$data$display[[9L]],
    "123456789012345678901234567890123456.78"
  )
})

test_that("Delta reader reads the Fabric Warehouse export profile", {
  fabric_test_require_package("duckdb")
  fabric_test_require_package("fs")
  manifest <- fabric_test_manifest()
  warehouse <- fabric_test_manifest_item(manifest, "TestWarehouse")
  result <- fabric_onelake_read_delta_table(
    table_path = warehouse$tables$types,
    workspace_name = manifest$workspace_id,
    lakehouse_name = warehouse$id,
    schema = "dbo",
    token = fabric_test_token("FABRIC_TEST_STORAGE_TOKEN"),
    verbose = FALSE
  )
  result <- result[order(result$id), ]

  expect_equal(result$id, 1:3)
  expect_identical(result$name, c("alpha", "beta", "gamma"))
  expect_identical(result$amount, c("10.50", "20.00", NA_character_))
  expect_identical(result$active, c(TRUE, FALSE, NA))
  expect_s3_class(result$event_date, "Date")
  expect_s3_class(result$loaded_at, "POSIXct")
})
