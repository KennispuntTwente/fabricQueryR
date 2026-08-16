test_that("Delta oracle differences are bounded for large tables", {
  actual <- data.frame(id = seq_len(100000L))
  expected <- actual
  expected$id[10001:90000] <- rev(expected$id[10001:90000])

  differences <- fabric_test_delta_differences(
    actual,
    expected,
    "large_fixture"
  )

  expect_lte(length(differences), 10L)
  expect_lt(nchar(paste(differences, collapse = "\n")), 5000L)
  failure <- tryCatch(
    fabric_test_expect_no_delta_differences(differences),
    expectation_failure = identity
  )
  expect_s3_class(failure, "expectation_failure")
  expect_lt(nchar(conditionMessage(failure)), 5000L)
})

test_that("Delta row ordering treats exact integer text numerically", {
  value <- data.frame(
    id = c("1001", "9007199254740993", "2", "1"),
    label = letters[1:4]
  )

  ordered <- fabric_test_order_delta_rows(value, "exact integer fixture")

  expect_identical(
    ordered$id,
    c("1", "2", "1001", "9007199254740993")
  )
})

test_that("deep Arrow comparison preserves row order and binary values", {
  fabric_test_require_package("arrow")
  expected <- arrow::Table$create(
    id = c(1L, 2L),
    payload = arrow::Array$create(list(as.raw(0:2), as.raw(c(255L, 0L))))
  )
  actual <- expected$Take(arrow::Array$create(c(1L, 0L)))

  actual <- fabric_test_order_arrow_rows(actual, "local Arrow fixture")
  expected <- fabric_test_order_arrow_rows(expected, "local Arrow reference")

  expect_true(actual$Equals(expected))

  nan_actual <- arrow::Table$create(value = c(NaN, 1))
  nan_expected <- arrow::Table$create(value = c(NaN, 1))
  expect_false(nan_actual$Equals(nan_expected))
  expect_true(fabric_test_arrow_column_equals(
    nan_actual$GetColumnByName("value"),
    nan_expected$GetColumnByName("value")
  ))

  timestamp_value <- "2026-07-28 09:08:07.654321"
  timestamp_table <- arrow::Table$create(
    id = 1L,
    local_at = arrow::Array$create(timestamp_value)$cast(
      arrow::timestamp("us")
    )
  )
  expect_no_error(fabric_test_expect_arrow_scalar_values(
    timestamp_table,
    tibble::tibble(
      id = 1L,
      local_at = "2026-07-28T09:08:07.654321"
    ),
    "local timestamp fixture"
  ))

  map_type <- arrow::map_of(arrow::utf8(), arrow::int32())
  map_actual <- arrow::Table$create(
    keyed = arrow::Array$create(
      list(data.frame(key = c("a", "b"), value = c(1L, 2L))),
      type = map_type
    )
  )
  map_expected <- arrow::Table$create(
    keyed = arrow::Array$create(
      list(data.frame(key = c("b", "a"), value = c(2L, 1L))),
      type = map_type
    )
  )
  expect_false(map_actual$Equals(map_expected))
  expect_true(fabric_test_arrow_column_equals(
    map_actual$GetColumnByName("keyed"),
    map_expected$GetColumnByName("keyed")
  ))
})

test_that("the delta-rs reader handles schema-enabled Fabric tables", {
  fabric_test_require_package("arrow")
  manifest <- fabric_test_manifest()
  fabric_test_use_delta_runtime()
  lakehouse <- fabric_test_manifest_item(manifest, "TestLakehouse")
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
    token = fabric_test_token_provider(),
    verbose = FALSE
  )
  result <- result[order(result$id), ]

  expect_s3_class(result, "tbl_df")
  expect_named(
    result,
    c("id", "name", "category", "amount", "loaded_at"),
    ignore.order = TRUE
  )
  expect_equal(result$id, 1:3)
  expect_equal(result$name, c("alpha", "beta", "gamma"))
  expect_equal(result$category, c("A", "B", "A"))
  expect_equal(result$amount, c(10.5, 20, NA))
  expect_s3_class(result$loaded_at, "POSIXct")

  stream <- fabric_test_read_delta(
    manifest,
    lakehouse,
    lakehouse$tables$basic,
    columns = c("name", "id"),
    limit = 2,
    result = "arrow_stream"
  )
  expect_s3_class(stream, "nanoarrow_array_stream")
  projected <- as.data.frame(
    arrow::as_record_batch_reader(stream)$read_table()
  )
  expect_named(projected, c("name", "id"))
  expect_equal(nrow(projected), 2L)
  expect_true(all(projected$id %in% 1:3))
})

test_that("the Lakehouse reader provides a discovered-item round trip", {
  fabric_test_require_package("arrow")
  manifest <- fabric_test_manifest()
  fabric_test_use_delta_runtime()
  lakehouse <- fabric_test_manifest_item(manifest, "TestLakehouse")
  discovered <- fabric_item(
    manifest$workspace_id,
    lakehouse$id,
    type = "Lakehouse",
    token = fabric_test_token("FABRIC_TEST_API_TOKEN")
  )

  result <- fabric_lakehouse_read_table(
    discovered,
    lakehouse$tables$basic,
    columns = c("id", "name"),
    token = fabric_test_token_provider(),
    verbose = FALSE
  )
  result <- result[order(result$id), ]

  expect_named(result, c("id", "name"))
  expect_identical(result$id, c(1, 2, 3))
  expect_identical(result$name, c("alpha", "beta", "gamma"))
})

test_that("the delta-rs reader handles a schema-disabled Fabric Lakehouse", {
  manifest <- fabric_test_manifest()
  fabric_test_use_delta_runtime()
  lakehouse <- fabric_test_manifest_item(
    manifest,
    "TestLakehouseNoSchemas"
  )
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
    token = fabric_test_token_provider(),
    verbose = FALSE
  )
  result <- result[order(result$id), ]

  expect_identical(result$id, c(1, 2, 3))
  expect_identical(result$name, c("alpha", "beta", "gamma"))
  expect_identical(result$category, c("A", "B", "A"))
  expect_identical(result$amount, c(10.5, 20, NA_real_))
})

test_that("a refreshable credential retries and reads live OneLake data", {
  manifest <- fabric_test_manifest()
  fabric_test_use_delta_runtime()
  lakehouse <- fabric_test_manifest_item(manifest, "TestLakehouse")
  calls <- logical()
  provider <- function(audience, force_refresh = FALSE) {
    calls <<- c(calls, force_refresh)
    expect_identical(audience, "https://storage.azure.com/.default")
    if (!force_refresh) {
      stop("HTTP 401: token expired")
    }
    fabric_test_token("FABRIC_TEST_STORAGE_TOKEN")
  }

  result <- fabric_onelake_read_delta_table(
    table_path = lakehouse$tables$basic,
    workspace_name = manifest$workspace_id,
    lakehouse_name = lakehouse$id,
    schema = lakehouse$schema,
    token = provider,
    verbose = FALSE
  )
  result <- result[order(result$id), ]

  expect_identical(calls, c(FALSE, TRUE))
  expect_identical(result$id, c(1, 2, 3))
  expect_identical(result$name, c("alpha", "beta", "gamma"))
})

test_that("the delta-rs reader preserves empty and exact Fabric values", {
  manifest <- fabric_test_manifest()
  fabric_test_use_delta_runtime()
  lakehouse <- fabric_test_manifest_item(manifest, "TestLakehouse")

  empty <- fabric_test_read_delta(
    manifest,
    lakehouse,
    lakehouse$tables$empty
  )
  expect_s3_class(empty, "tbl_df")
  expect_equal(nrow(empty), 0L)
  expect_named(empty, c("id", "name", "category", "amount"))

  partitions <- fabric_test_read_delta(
    manifest,
    lakehouse,
    lakehouse$tables$typed_partitions
  )
  partitions <- partitions[order(partitions$id), ]
  expect_equal(partitions$id, 1:3)
  expect_s3_class(partitions$event_date, "Date")
  expect_type(partitions$active, "logical")
  expect_identical(
    partitions$decimal_part,
    c("12.30", "-0.50", NA_character_)
  )
  expect_type(partitions$timestamp_ntz_part, "character")
  expect_identical(
    partitions$timestamp_ntz_part,
    c(
      "2026-07-28T09:08:07.654321",
      "1900-01-01T00:00:00.000001",
      NA_character_
    )
  )

  exact <- fabric_test_read_delta(
    manifest,
    lakehouse,
    lakehouse$tables$exact_types
  )
  expect_named(
    exact,
    c(
      "row_id",
      "minimum_integer",
      "minimum_long",
      "above_double_limit",
      "maximum_long",
      "whole_decimal",
      "scaled_decimal",
      "observed_at",
      "payload",
      "unicode_text",
      "not_a_number",
      "positive_infinity"
    )
  )
  expect_identical(exact$row_id, 1)
  expect_type(exact$minimum_integer, "double")
  expect_identical(exact$minimum_integer, -2147483648)
  expect_identical(exact$minimum_long, "-9223372036854775808")
  expect_identical(exact$above_double_limit, "9007199254740993")
  expect_identical(exact$maximum_long, "9223372036854775807")
  expect_identical(
    exact$whole_decimal,
    "12345678901234567890123456789012345678"
  )
  expect_identical(
    exact$scaled_decimal,
    "123456789012345678901234567890123456.78"
  )
  expect_identical(
    exact$observed_at,
    "2026-07-28T12:34:56.123456"
  )
  expect_identical(exact$payload[[1L]], as.raw(c(0L, 255L, 16L)))
  expect_identical(exact$unicode_text, "café-数据-🙂")
  expect_true(is.nan(exact$not_a_number))
  expect_identical(exact$positive_infinity, Inf)

  nested_tables <- c(
    lakehouse$tables$complex_types,
    lakehouse$tables$oracle_complex_types,
    lakehouse$tables$struct_validity,
    lakehouse$tables$spark_oracle_struct_validity
  )
  for (table in nested_tables) {
    expect_error(
      fabric_test_read_delta(manifest, lakehouse, table),
      class = "fabric_delta_nested_collection_error"
    )
  }
})

test_that("OneLake access failures receive an actionable error class", {
  manifest <- fabric_test_manifest()
  fabric_test_use_delta_runtime()
  lakehouse <- fabric_test_manifest_item(manifest, "TestLakehouse")
  uri <- fabric_delta_target_uri(list(
    dfs_base = "https://onelake.dfs.fabric.microsoft.com",
    workspace = manifest$workspace_id,
    item = lakehouse$id,
    path = paste("Tables/dbo", lakehouse$tables$basic, sep = "/")
  ))
  raw_error <- tryCatch(
    fabric_delta_read_uri(uri, bearer_token = "invalid-integration-token"),
    error = identity
  )
  expect_s3_class(raw_error, "error")
  classified <- tryCatch(
    fabric_delta_abort_python(
      raw_error,
      bearer_token = "invalid-integration-token"
    ),
    error = identity
  )
  expect_s3_class(classified, "fabric_delta_access_error")
  expect_false(
    grepl(
      "invalid-integration-token",
      conditionMessage(classified),
      fixed = TRUE
    )
  )
})

test_that("Arrow streams cover representative Fabric Delta snapshots", {
  fabric_test_require_package("arrow")
  manifest <- fabric_test_manifest()
  fabric_test_use_delta_runtime()
  lakehouse <- fabric_test_manifest_item(manifest, "TestLakehouse")
  cases <- list(
    list(item = lakehouse, table = lakehouse$tables$empty),
    list(item = lakehouse, table = lakehouse$tables$typed_partitions),
    list(
      item = lakehouse,
      table = lakehouse$tables$schema_evolved,
      version = 0
    )
  )

  for (case in cases) {
    expected <- fabric_test_read_delta(
      manifest,
      case$item,
      case$table,
      version = case$version %||% NULL
    )
    stream <- fabric_test_read_delta(
      manifest,
      case$item,
      case$table,
      version = case$version %||% NULL,
      result = "arrow_stream"
    )
    expect_s3_class(stream, "nanoarrow_array_stream")
    actual <- arrow::as_record_batch_reader(stream)$read_table()
    expect_equal(actual$num_rows, nrow(expected), label = case$table)
    expect_identical(actual$ColumnNames(), names(expected), label = case$table)
    fabric_test_expect_arrow_scalar_values(actual, expected, case$table)
  }
})

test_that("Arrow streams deeply match Spark-neutral Fabric references", {
  fabric_test_require_package("arrow")
  manifest <- fabric_test_manifest()
  fabric_test_use_delta_runtime()
  lakehouse <- fabric_test_manifest_item(manifest, "TestLakehouse")
  tables <- lakehouse$tables
  cases <- list(
    column_mapped = list(
      source = tables$column_mapped,
      reference = tables$spark_oracle_column_mapped
    ),
    column_mapped_id = list(
      source = tables$column_mapped_id,
      reference = tables$spark_oracle_column_mapped_id
    ),
    column_mapped_id_partitioned_dv = list(
      source = tables$column_mapped_id_partitioned_dv,
      reference = tables$spark_oracle_column_mapped_id_partitioned_dv
    ),
    exact_types = list(
      source = tables$exact_types,
      reference = tables$oracle_exact_types
    ),
    struct_validity = list(
      source = tables$struct_validity,
      reference = tables$spark_oracle_struct_validity
    ),
    shallow_clone = list(
      source = tables$shallow_clone,
      reference = tables$spark_oracle_shallow_clone
    ),
    complex_types = list(
      source = tables$complex_types,
      reference = tables$oracle_complex_types,
      columns = c("id", "profile", "scores", "counts", "items", "attributes")
    )
  )

  for (feature in names(cases)) {
    case <- cases[[feature]]
    fabric_test_expect_arrow_matches_reference(
      manifest,
      lakehouse,
      case$source,
      case$reference,
      feature,
      columns = case$columns %||% NULL
    )
  }
})

test_that("core Fabric Delta features are handled by the table provider", {
  manifest <- fabric_test_manifest()
  fabric_test_use_delta_runtime()
  lakehouse <- fabric_test_manifest_item(manifest, "TestLakehouse")
  tables <- lakehouse$tables
  required <- list(
    basic = c(
      tables$basic,
      tables$oracle_basic
    ),
    partitioned_classic_checkpoint = c(
      tables$partitioned,
      tables$oracle_partitioned
    ),
    schema_evolved = c(
      tables$schema_evolved,
      tables$oracle_schema_evolved
    ),
    exact_types = c(
      tables$exact_types,
      tables$oracle_exact_types
    ),
    shallow_clone = c(
      tables$shallow_clone,
      tables$spark_oracle_shallow_clone
    )
  )

  for (feature in names(required)) {
    pair <- required[[feature]]
    expect_length(pair, 2L)
    fabric_test_expect_delta_matches_reference(
      manifest,
      lakehouse,
      pair[[1L]],
      pair[[2L]],
      feature
    )
  }
})

test_that("Delta reference ordering accepts both fixture key conventions", {
  by_id <- fabric_test_order_delta_rows(
    tibble::tibble(id = c(2L, 1L)),
    "id fixture"
  )
  by_row_id <- fabric_test_order_delta_rows(
    tibble::tibble(row_id = c(2L, 1L)),
    "row_id fixture"
  )

  expect_identical(by_id$id, 1:2)
  expect_identical(by_row_id$row_id, 1:2)
})

test_that("remaining supported Fabric Delta fixtures cover edge cases", {
  fabric_test_require_package("arrow")
  manifest <- fabric_test_manifest()
  fabric_test_use_delta_runtime()
  lakehouse <- fabric_test_manifest_item(manifest, "TestLakehouse")
  tables <- lakehouse$tables

  runtime <- fabric_test_read_delta(manifest, lakehouse, tables$runtime)
  expect_equal(nrow(runtime), 1L)
  expect_named(
    runtime,
    c("fabric_runtime", "spark_version", "delta_version")
  )
  expect_identical(manifest$runtime$lane, "preview")
  expect_identical(manifest$runtime$fabric_runtime, "2.0")
  expect_match(manifest$runtime$spark_version, "^4[.]1[.]")
  expect_match(manifest$runtime$delta_version, "^4[.]2[.]")
  expect_identical(runtime$fabric_runtime, "2.0")
  expect_identical(runtime$spark_version, manifest$runtime$spark_version)
  expect_identical(runtime$delta_version, manifest$runtime$delta_version)

  void_arrow <- fabric_test_read_arrow_table(
    manifest,
    lakehouse,
    tables$void
  )
  expect_equal(void_arrow$GetColumnByName("details")$null_count, 0L)
  void <- as.data.frame(void_arrow)
  void <- void[order(void$id), ]
  expect_named(void, c("id", "always_null", "details"))
  expect_identical(as.character(void$id), as.character(0:2))
  expect_true(all(is.na(void$always_null)))
  expect_identical(void$details$value, 0:2)
  expect_true(all(is.na(void$details$pending)))

  binary <- fabric_test_read_delta(
    manifest,
    lakehouse,
    tables$binary_partitions
  )
  binary <- binary[order(binary$id), ]
  expect_identical(binary$id, c(1, 2, 3, 4))
  expect_identical(binary$binary_part[[1L]], as.raw(0L))
  expect_identical(binary$binary_part[[2L]], as.raw(c(194L, 128L)))
  expect_identical(binary$binary_part[[3L]], as.raw(c(195L, 191L)))
  expect_null(binary$binary_part[[4L]])

  evolved <- fabric_test_read_delta(
    manifest,
    lakehouse,
    tables$schema_evolved
  )
  evolved <- evolved[order(evolved$id), ]
  expect_named(evolved, c("id", "name", "evolved_value"))
  expect_identical(evolved$id, c(1, 2, 3))
  expect_identical(evolved$name, c("alpha", "beta", "gamma"))
  expect_identical(evolved$evolved_value, c(NA, NA, "introduced"))

  original <- fabric_test_read_delta(
    manifest,
    lakehouse,
    tables$schema_evolved,
    version = 0
  )
  original <- original[order(original$id), ]
  expect_named(original, c("id", "name"))
  expect_identical(original$id, c(1, 2))
  expect_identical(original$name, c("alpha", "beta"))

  typed_reference <- fabric_test_read_delta(
    manifest,
    lakehouse,
    tables$oracle_typed_partitions
  )
  typed_reference <- typed_reference[order(typed_reference$id), ]
  expect_identical(typed_reference$id, c(1, 2, 3))
  expect_identical(
    typed_reference$decimal_part,
    c("12.30", "0.50", NA_character_)
  )

  empty_reference <- fabric_test_read_delta(
    manifest,
    lakehouse,
    tables$oracle_empty
  )
  expect_equal(nrow(empty_reference), 0L)
  expect_named(empty_reference, c("id", "name", "category", "amount"))
})

test_that("unsupported Fabric Delta features fail with actionable errors", {
  manifest <- fabric_test_manifest()
  fabric_test_use_delta_runtime()
  lakehouse <- fabric_test_manifest_item(manifest, "TestLakehouse")
  tables <- lakehouse$tables
  unsupported <- c(
    type_widened = "TypeWidening",
    type_widened_exact = "TypeWidening",
    type_widened_pending = "TypeWidening",
    type_widened_nested = "TypeWidening",
    type_widened_map_key = "TypeWidening",
    deletion_vectors_checkpoint = "V2Checkpoint",
    v2_checkpoint = "V2Checkpoint"
  )

  for (feature in names(unsupported)) {
    condition <- tryCatch(
      fabric_test_read_delta(
        manifest,
        lakehouse,
        tables[[feature]]
      ),
      error = identity
    )
    expect_s3_class(
      condition,
      "fabric_delta_unsupported_feature_error"
    )
    expect_match(
      conditionMessage(condition),
      unsupported[[feature]],
      label = feature
    )
  }
})

test_that("neutral references for unsupported Fabric features fully scan", {
  fabric_test_require_package("arrow")
  manifest <- fabric_test_manifest()
  fabric_test_use_delta_runtime()
  lakehouse <- fabric_test_manifest_item(manifest, "TestLakehouse")
  tables <- lakehouse$tables
  references <- c(
    "spark_oracle_column_mapped_id_partitioned_dv",
    "spark_oracle_deletion_vectors",
    "spark_oracle_file_row_number_collision",
    "spark_oracle_deletion_vectors_stress",
    "spark_oracle_deletion_vectors_dense",
    "spark_oracle_row_tracking",
    "spark_oracle_deletion_vectors_checkpoint",
    "spark_oracle_type_widened",
    "spark_oracle_type_widened_exact",
    "spark_oracle_type_widened_pending",
    "spark_oracle_type_widened_nested",
    "spark_oracle_type_widened_map_key",
    "spark_oracle_v2_checkpoint",
    "spark_oracle_variant",
    "spark_oracle_variant_id_dv"
  )

  for (reference in references) {
    value <- fabric_test_read_arrow_table(
      manifest,
      lakehouse,
      tables[[reference]]
    )
    expect_true(inherits(value, "Table"), label = reference)
    expect_gt(value$num_rows, 0L, label = reference)
    expect_gt(value$num_columns, 0L, label = reference)
    key <- intersect(c("id", "row_id", "event_id"), value$ColumnNames())
    expect_true(
      length(key) == 1L,
      label = paste(reference, "has one stable key")
    )
    key_values <- value$GetColumnByName(key[[1L]])$as_vector()
    expect_false(
      anyDuplicated(key_values) > 0L,
      label = paste(reference, "unique stable key")
    )
  }
})

test_that("supported Delta rows match the independent Spark logical oracle", {
  manifest <- fabric_test_manifest()
  fabric_test_use_delta_runtime()
  lakehouse <- fabric_test_manifest_item(manifest, "TestLakehouse")
  tables <- lakehouse$tables
  oracle <- fabric_test_spark_logical_oracle(manifest, lakehouse)
  expect_identical(oracle$format_version, 2L)
  expect_identical(oracle$canonicalization, "spark-logical-v1")

  sources <- c(
    tables$shallow_clone,
    tables$deletion_vectors,
    tables$file_row_number_collision,
    tables$deletion_vectors_stress,
    tables$deletion_vectors_dense,
    tables$row_tracking
  )
  expect_true(all(sources %in% names(oracle$tables)))
  for (source in sources) {
    expected <- oracle$tables[[source]]
    actual <- fabric_test_read_delta(
      manifest,
      lakehouse,
      source
    )
    key <- expected$key
    expect_named(
      actual,
      unlist(expected$columns, use.names = FALSE),
      info = source
    )
    expect_equal(nrow(actual), expected$row_count, info = source)
    expect_false(anyDuplicated(actual[[key]]) > 0L, info = source)
    actual <- fabric_test_order_delta_rows(actual, source)
    actual_rows <- fabric_test_spark_canonical_rows(
      actual,
      expected$schema
    )
    differences <- fabric_test_delta_differences(
      actual_rows,
      expected$rows,
      source
    )
    fabric_test_expect_no_delta_differences(differences)
  }
})

test_that("every discovered Delta fixture has an integration-test disposition", {
  manifest <- fabric_test_manifest()
  lakehouse <- fabric_test_manifest_item(manifest, "TestLakehouse")
  exact_values <- c(
    "runtime",
    "basic",
    "empty",
    "typed_partitions",
    "binary_partitions",
    "schema_evolved",
    "exact_types",
    "oracle_empty",
    "oracle_typed_partitions"
  )
  reference_comparison <- c(
    "partitioned",
    "deletion_vectors",
    "file_row_number_collision",
    "deletion_vectors_stress",
    "deletion_vectors_dense",
    "row_tracking",
    "shallow_clone",
    "oracle_basic",
    "oracle_partitioned",
    "oracle_schema_evolved",
    "oracle_exact_types",
    "spark_oracle_shallow_clone"
  )
  stream_only <- c(
    "void",
    "complex_types",
    "oracle_complex_types",
    "struct_validity",
    "spark_oracle_struct_validity",
    "column_mapped",
    "spark_oracle_column_mapped",
    "column_mapped_id",
    "spark_oracle_column_mapped_id",
    "column_mapped_id_partitioned_dv",
    "spark_oracle_column_mapped_id_partitioned_dv"
  )
  unsupported_error <- c(
    "deletion_vectors_checkpoint",
    "type_widened",
    "type_widened_exact",
    "type_widened_pending",
    "type_widened_nested",
    "type_widened_map_key",
    "v2_checkpoint",
    "variant",
    "variant_id_dv"
  )
  full_scan <- c(
    "spark_oracle_deletion_vectors",
    "spark_oracle_file_row_number_collision",
    "spark_oracle_deletion_vectors_stress",
    "spark_oracle_deletion_vectors_dense",
    "spark_oracle_row_tracking",
    "spark_oracle_deletion_vectors_checkpoint",
    "spark_oracle_type_widened",
    "spark_oracle_type_widened_exact",
    "spark_oracle_type_widened_pending",
    "spark_oracle_type_widened_nested",
    "spark_oracle_type_widened_map_key",
    "spark_oracle_v2_checkpoint",
    "spark_oracle_variant",
    "spark_oracle_variant_id_dv"
  )
  covered_by_job_workflows <- c("livy_batch_result", "spark_job_result")
  disposition <- c(
    stats::setNames(rep("exact_values", length(exact_values)), exact_values),
    stats::setNames(
      rep("reference_comparison", length(reference_comparison)),
      reference_comparison
    ),
    stats::setNames(rep("stream_only", length(stream_only)), stream_only),
    stats::setNames(
      rep("unsupported_error", length(unsupported_error)),
      unsupported_error
    ),
    stats::setNames(rep("full_scan", length(full_scan)), full_scan),
    stats::setNames(
      rep("job_workflow", length(covered_by_job_workflows)),
      covered_by_job_workflows
    )
  )
  expect_false(anyDuplicated(names(disposition)) > 0L)
  expect_setequal(names(lakehouse$tables), names(disposition))
  expect_setequal(
    unname(disposition),
    c(
      "exact_values",
      "reference_comparison",
      "unsupported_error",
      "stream_only",
      "full_scan",
      "job_workflow"
    )
  )
})

test_that("Fabric Variant tables fail before exposing physical fields", {
  manifest <- fabric_test_manifest()
  fabric_test_use_delta_runtime()
  lakehouse <- fabric_test_manifest_item(manifest, "TestLakehouse")

  tables <- c(
    lakehouse$tables$variant,
    lakehouse$tables$variant_id_dv
  )

  for (table in tables) {
    for (result in c("tibble", "arrow_stream")) {
      condition <- tryCatch(
        fabric_test_read_delta(
          manifest,
          lakehouse,
          table,
          result = result
        ),
        error = identity
      )
      expect_s3_class(
        condition,
        "fabric_delta_unsupported_feature_error"
      )
      expect_true(
        length(condition$delta_features) > 0L &&
          all(
            condition$delta_features %in%
              c("VariantType", "VariantShredding")
          ),
        label = table
      )
      expect_match(conditionMessage(condition), "Variant(Type|Shredding)")
    }
  }
})

test_that("seeded Fabric Warehouse Delta exports remain readable", {
  manifest <- fabric_test_manifest()
  fabric_test_use_delta_runtime()
  warehouse <- fabric_test_manifest_item(manifest, "TestWarehouse")

  expected <- list(
    types = data.frame(
      id = c(1, 2, 3),
      name = c("alpha", "beta", "gamma")
    ),
    mutations = data.frame(
      id = c(2, 3, 4),
      name = c("beta-updated", "gamma", "alpha-replacement")
    )
  )
  expect_setequal(names(warehouse$tables), names(expected))

  for (fixture in names(expected)) {
    table <- warehouse$tables[[fixture]]
    result <- fabric_test_read_delta(
      manifest,
      warehouse,
      table,
      schema = "dbo"
    )
    result <- result[order(result$id), , drop = FALSE]
    expected_result <- expected[[fixture]]

    expect_s3_class(result, "tbl_df")
    expect_identical(as.numeric(result$id), expected_result$id, label = table)
    expect_identical(
      as.character(result$name),
      expected_result$name,
      label = table
    )
  }
})

test_that("OneLake file helpers cover hierarchy, ranges, and Unicode", {
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
  resumed <- fabric_onelake_list(
    manifest$workspace_id,
    lakehouse$id,
    path = "Files/fixtures/nested",
    recursive = TRUE,
    begin_from = "b/duplicate.txt",
    token = token
  )
  expect_false(any(grepl("/a/duplicate[.]txt$", resumed$path)))
  expect_true(any(grepl("/b/duplicate[.]txt$", resumed$path)))

  unicode_path <- "Files/fixtures/nested/unicode/café-数据.txt"
  expect_identical(
    trimws(rawToChar(fabric_onelake_download(
      manifest$workspace_id,
      lakehouse$id,
      unicode_path,
      token = token
    ))),
    "OneLake Unicode fixture"
  )
  disk_destination <- tempfile("fabricqueryr-onelake-", fileext = ".txt")
  on.exit(unlink(disk_destination, force = TRUE), add = TRUE)
  downloaded_path <- fabric_onelake_download(
    manifest$workspace_id,
    lakehouse$id,
    unicode_path,
    dest = disk_destination,
    token = token
  )
  expect_identical(
    downloaded_path,
    normalizePath(disk_destination, winslash = "/", mustWork = TRUE)
  )
  expect_identical(
    trimws(readChar(disk_destination, file.info(disk_destination)$size)),
    "OneLake Unicode fixture"
  )
  expect_identical(
    rawToChar(fabric_onelake_download(
      manifest$workspace_id,
      lakehouse$id,
      "Files/fixtures/nested/a/duplicate.txt",
      range = c(0, 4),
      token = token
    )),
    "alpha"
  )

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

  fabric_test_require_package("arrow")
  object_path <- paste0(test_root, "/objects/orders.parquet")
  object_data <- data.frame(
    id = 1:3,
    amount = c(10.5, NA, 30),
    loaded_on = as.Date(c("2026-08-12", "2026-08-13", "2026-08-14"))
  )
  object_write <- fabric_onelake_write_file(
    manifest$workspace_id,
    lakehouse$id,
    object_path,
    object_data,
    token = token
  )
  expect_s3_class(object_write, "fabric_onelake_file_write_result")
  expect_identical(object_write$format, "parquet")
  expect_equal(object_write$rows, 3)
  object_read <- fabric_onelake_read_file(
    manifest$workspace_id,
    lakehouse$id,
    object_path,
    token = token
  )
  expect_equal(object_read$id, object_data$id)
  expect_equal(object_read$amount, object_data$amount)
  expect_equal(object_read$loaded_on, object_data$loaded_on)

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
  expect_true(nzchar(first$etag))
  expect_equal(first$content_length, nchar("first-version", type = "bytes"))

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
  expect_identical(
    rawToChar(fabric_onelake_download(
      manifest$workspace_id,
      lakehouse$id,
      test_path,
      token = token
    )),
    "second-version"
  )

  file_delete_path <- paste0(test_root, "/individual-file.txt")
  fabric_onelake_upload(
    manifest$workspace_id,
    lakehouse$id,
    file_delete_path,
    source = charToRaw("delete-me"),
    token = token
  )
  expect_true(fabric_onelake_delete(
    manifest$workspace_id,
    lakehouse$id,
    file_delete_path,
    confirm = TRUE,
    token = token
  ))

  reserved_path <- paste0(test_root, "/reserved/a?b#c.txt")
  fabric_onelake_upload(
    manifest$workspace_id,
    lakehouse$id,
    reserved_path,
    source = charToRaw("reserved-characters"),
    token = token
  )
  expect_identical(
    rawToChar(fabric_onelake_download(
      manifest$workspace_id,
      lakehouse$id,
      reserved_path,
      token = token
    )),
    "reserved-characters"
  )
  expect_true(fabric_onelake_delete(
    manifest$workspace_id,
    lakehouse$id,
    reserved_path,
    confirm = TRUE,
    token = token
  ))

  empty_directory_file <- paste0(test_root, "/empty-directory/file.txt")
  fabric_onelake_upload(
    manifest$workspace_id,
    lakehouse$id,
    empty_directory_file,
    source = raw(),
    token = token
  )
  expect_true(fabric_onelake_delete(
    manifest$workspace_id,
    lakehouse$id,
    empty_directory_file,
    confirm = TRUE,
    token = token
  ))
  expect_true(fabric_onelake_delete(
    manifest$workspace_id,
    lakehouse$id,
    paste0(test_root, "/empty-directory"),
    confirm = TRUE,
    token = token
  ))

  expect_true(fabric_onelake_delete(
    manifest$workspace_id,
    lakehouse$id,
    test_root,
    recursive = TRUE,
    confirm = TRUE,
    token = token
  ))
})

test_that("OneLake shortcuts complete a live create/read/delete lifecycle", {
  manifest <- fabric_test_manifest()
  lakehouse <- fabric_test_manifest_item(manifest, "TestLakehouse")
  token <- fabric_test_token_provider()
  item <- fabric_item(
    manifest$workspace_id,
    lakehouse$id,
    type = "Lakehouse",
    token = token
  )
  name <- "fabricqueryr_shortcut_live"
  parent <- "Files"
  created <- FALSE
  on.exit(
    if (created) {
      try(
        fabric_onelake_shortcut_delete(
          item,
          parent,
          name,
          confirm = TRUE,
          token = token
        ),
        silent = TRUE
      )
    },
    add = TRUE
  )
  source_path <- "Files/fixtures/nested/unicode/café-数据.txt"
  expected <- fabric_onelake_download(
    manifest$workspace_id,
    lakehouse$id,
    source_path,
    token = token
  )

  shortcut <- fabric_onelake_shortcut_create(
    item,
    path = parent,
    name = name,
    target = item,
    target_path = "Files/fixtures/nested",
    conflict_policy = "CreateOrOverwrite",
    token = token
  )
  created <- TRUE
  expect_equal(shortcut$name, name)
  expect_equal(shortcut$target_type, "OneLake")
  expect_equal(shortcut$one_lake_item_id, lakehouse$id)

  observed <- fabric_test_eventually(function() {
    fabric_onelake_download(
      manifest$workspace_id,
      lakehouse$id,
      paste0("Files/", name, "/unicode/café-数据.txt"),
      token = token
    )
  })
  expect_identical(observed, expected)
  listed <- fabric_onelake_shortcuts(
    item,
    parent_path = parent,
    token = token
  )
  expect_true(name %in% listed$name)
  inspected <- fabric_onelake_shortcut_get(
    item,
    parent,
    name,
    token = token
  )
  expect_equal(inspected$one_lake_path, "Files/fixtures/nested")

  expect_true(fabric_onelake_shortcut_delete(
    item,
    parent,
    name,
    confirm = TRUE,
    token = token
  ))
  created <- FALSE
  expect_error(
    fabric_onelake_shortcut_get(item, parent, name, token = token),
    "HTTP 404"
  )
  expect_identical(
    fabric_onelake_download(
      manifest$workspace_id,
      lakehouse$id,
      source_path,
      token = token
    ),
    expected
  )
})
