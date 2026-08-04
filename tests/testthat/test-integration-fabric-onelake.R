# Fabric integration coverage: OneLake files and the production delta-rs path.

fabric_test_use_delta_runtime <- function() {
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

fabric_test_read_delta <- function(
  manifest,
  item,
  table,
  schema = item$schema %||% "dbo",
  ...
) {
  testthat::expect_no_warning(
    fabric_onelake_read_delta_table(
      table_path = table,
      workspace_name = manifest$workspace_id,
      lakehouse_name = item$id,
      schema = schema,
      token = fabric_test_token_provider(),
      verbose = FALSE,
      ...
    )
  )
}

fabric_test_order_delta_rows <- function(value, feature) {
  key <- intersect(c("id", "row_id"), names(value))
  expect_true(
    length(key) == 1L,
    label = paste(feature, "has one stable id or row_id column")
  )
  value[order(value[[key]], na.last = TRUE), , drop = FALSE]
}

fabric_test_canonicalize_delta_maps <- function(value) {
  if (is.data.frame(value)) {
    if (identical(names(value), c("key", "value")) && nrow(value) > 1L) {
      labels <- vapply(
        seq_len(nrow(value)),
        function(index) {
          key <- value$key
          key <- if (is.data.frame(key)) {
            key[index, , drop = FALSE]
          } else {
            key[[index]]
          }
          if (is.data.frame(key)) {
            rownames(key) <- NULL
          }
          paste(capture.output(dput(key)), collapse = "\n")
        },
        character(1)
      )
      value <- value[order(labels), , drop = FALSE]
    }
    for (name in names(value)) {
      value[[name]] <- fabric_test_canonicalize_delta_maps(value[[name]])
    }
    rownames(value) <- NULL
    return(value)
  }
  if (is.list(value)) {
    return(lapply(value, fabric_test_canonicalize_delta_maps))
  }
  value
}

fabric_test_arrow_scalar_text <- function(value) {
  if (inherits(value, "POSIXt")) {
    return(format(value, "%Y-%m-%d %H:%M:%OS6", tz = "UTC"))
  }
  as.character(value)
}

fabric_test_expect_arrow_scalar_values <- function(
  actual,
  expected,
  feature
) {
  key <- intersect(c("id", "row_id"), names(expected))
  if (length(key) == 1L && nrow(expected)) {
    key_values <- actual$GetColumnByName(key[[1L]])$as_vector()
    indices <- order(key_values, na.last = TRUE) - 1L
    actual <- actual$Take(arrow::Array$create(as.integer(indices)))
    expected <- expected[
      order(expected[[key]], na.last = TRUE),
      ,
      drop = FALSE
    ]
  }
  actual_frame <- as.data.frame(actual)
  scalar <- names(expected)[vapply(
    expected,
    function(column) is.atomic(column) && !is.raw(column),
    logical(1)
  )]
  expect_gt(length(scalar), 0L, label = paste(feature, "scalar columns"))
  for (name in scalar) {
    actual_value <- if (
      inherits(expected[[name]], "fabric_delta_timestamp_ntz")
    ) {
      actual$GetColumnByName(name)$cast(arrow::utf8())$as_vector()
    } else {
      actual_frame[[name]]
    }
    expect_identical(
      fabric_test_arrow_scalar_text(actual_value),
      fabric_test_arrow_scalar_text(expected[[name]]),
      label = paste(feature, name)
    )
  }
  invisible(actual_frame)
}

fabric_test_read_arrow_table <- function(
  manifest,
  item,
  table,
  ...
) {
  stream <- fabric_test_read_delta(
    manifest,
    item,
    table,
    result = "arrow_stream",
    ...
  )
  expect_s3_class(stream, "nanoarrow_array_stream")
  arrow::as_record_batch_reader(stream)$read_table()
}

fabric_test_order_arrow_rows <- function(value, feature) {
  key <- intersect(c("id", "row_id"), value$ColumnNames())
  if (!value$num_rows) {
    return(value)
  }
  expect_true(
    length(key) == 1L,
    label = paste(feature, "Arrow table has one stable key")
  )
  key_values <- value$GetColumnByName(key[[1L]])$as_vector()
  indices <- order(key_values, na.last = TRUE) - 1L
  value$Take(arrow::Array$create(as.integer(indices)))
}

fabric_test_arrow_column_equals <- function(actual, expected) {
  if (!identical(actual$type$ToString(), expected$type$ToString())) {
    return(FALSE)
  }
  if (isTRUE(actual$Equals(expected))) {
    return(TRUE)
  }
  identical(actual$as_vector(), expected$as_vector())
}

fabric_test_expect_arrow_matches_reference <- function(
  manifest,
  lakehouse,
  source,
  reference,
  feature,
  columns = NULL
) {
  actual <- fabric_test_read_arrow_table(
    manifest,
    lakehouse,
    source,
    columns = columns
  )
  expected <- fabric_test_read_arrow_table(
    manifest,
    lakehouse,
    reference,
    columns = columns
  )
  expect_identical(
    actual$ColumnNames(),
    expected$ColumnNames(),
    label = paste(feature, "Arrow columns")
  )
  expect_identical(
    actual$num_rows,
    expected$num_rows,
    label = paste(feature, "Arrow rows")
  )
  actual <- fabric_test_order_arrow_rows(actual, feature)
  expected <- fabric_test_order_arrow_rows(
    expected,
    paste(feature, "feature-neutral reference")
  )
  for (name in actual$ColumnNames()) {
    expect_true(
      fabric_test_arrow_column_equals(
        actual$GetColumnByName(name),
        expected$GetColumnByName(name)
      ),
      label = paste(feature, name, "logical Arrow equality")
    )
  }
  invisible(actual)
}

fabric_test_delta_differences <- function(actual, expected, feature) {
  waldo::compare(
    actual,
    expected,
    x_arg = feature,
    y_arg = paste(feature, "feature-neutral reference"),
    max_diffs = 10L
  )
}

test_that("Delta oracle comparisons canonicalize unordered map entries", {
  nested <- structure(
    data.frame(number = c(NA_integer_, NA_integer_)),
    class = c("fabric_delta_struct_column", "data.frame"),
    fabric_delta_struct_validity = c(FALSE, TRUE)
  )
  key <- data.frame(marker = 1:2)
  key$nested <- nested
  map <- structure(
    list(key = key, value = c("null", "present")),
    class = "data.frame",
    row.names = c(NA, -2L)
  )

  actual <- fabric_test_canonicalize_delta_maps(map)
  expected <- fabric_test_canonicalize_delta_maps(map[2:1, , drop = FALSE])

  expect_equal(actual, expected)
  expect_identical(
    attr(actual$key$nested, "fabric_delta_struct_validity", exact = TRUE),
    c(FALSE, TRUE)
  )
})

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

  timestamp_value <- "2026-07-28 09:08:07.654321"
  timestamp_table <- arrow::Table$create(
    id = 1L,
    local_at = arrow::Array$create(timestamp_value)$cast(
      arrow::timestamp("us")
    )
  )
  timestamp_expected <- tibble::tibble(
    id = 1L,
    local_at = fabric_delta_timestamp_ntz(timestamp_value)
  )

  expect_no_error(fabric_test_expect_arrow_scalar_values(
    timestamp_table,
    timestamp_expected,
    "local timestamp fixture"
  ))

  nan_actual <- arrow::Table$create(value = c(NaN, 1))
  nan_expected <- arrow::Table$create(value = c(NaN, 1))
  expect_false(nan_actual$Equals(nan_expected))
  expect_true(fabric_test_arrow_column_equals(
    nan_actual$GetColumnByName("value"),
    nan_expected$GetColumnByName("value")
  ))
})

# Both sides use the production bridge. This comparison isolates protocol
# feature handling by comparing a feature-bearing table with a Spark-materialized
# neutral table; independent static assertions below cover bridge conversion.
fabric_test_expect_delta_matches_reference <- function(
  manifest,
  lakehouse,
  source,
  reference,
  feature
) {
  actual <- fabric_test_read_delta(manifest, lakehouse, source)
  expected <- fabric_test_read_delta(manifest, lakehouse, reference)

  expect_s3_class(actual, "tbl_df")
  expect_s3_class(expected, "tbl_df")
  expect_named(actual, names(expected), label = feature)
  expect_equal(nrow(actual), nrow(expected), label = feature)
  expect_gt(nrow(actual), 0L, label = feature)

  actual <- fabric_test_order_delta_rows(actual, feature)
  expected <- fabric_test_order_delta_rows(
    expected,
    paste(feature, "feature-neutral reference")
  )
  actual <- fabric_test_canonicalize_delta_maps(actual)
  expected <- fabric_test_canonicalize_delta_maps(expected)
  rownames(actual) <- NULL
  rownames(expected) <- NULL
  differences <- fabric_test_delta_differences(actual, expected, feature)
  if (length(differences)) {
    fail(
      paste(
        c(
          paste(
            "Delta result differs from its feature-neutral reference:",
            feature
          ),
          differences
        ),
        collapse = "\n"
      )
    )
  }
  invisible(actual)
}

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

  expect_identical(result$id, 1:3)
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
  expect_identical(result$id, 1:3)
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
  expect_s3_class(
    partitions$timestamp_ntz_part,
    "fabric_delta_timestamp_ntz"
  )
  expect_identical(
    unclass(partitions$timestamp_ntz_part),
    c(
      "2026-07-28 09:08:07.654321",
      "1900-01-01 00:00:00.000001",
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
  expect_identical(exact$row_id, 1L)
  expect_type(exact$minimum_integer, "double")
  expect_identical(exact$minimum_integer, -2147483648)
  expect_s3_class(exact$minimum_long, "fabric_delta_integer64")
  expect_identical(as.character(exact$minimum_long), "-9223372036854775808")
  expect_s3_class(exact$above_double_limit, "integer64")
  expect_s3_class(exact$maximum_long, "integer64")
  expect_identical(as.character(exact$above_double_limit), "9007199254740993")
  expect_identical(as.character(exact$maximum_long), "9223372036854775807")
  expect_identical(
    exact$whole_decimal,
    "12345678901234567890123456789012345678"
  )
  expect_identical(
    exact$scaled_decimal,
    "123456789012345678901234567890123456.78"
  )
  expect_s3_class(exact$observed_at, "fabric_delta_timestamp_ntz")
  expect_identical(
    unclass(exact$observed_at),
    "2026-07-28 12:34:56.123456"
  )
  expect_identical(exact$payload[[1L]], as.raw(c(0L, 255L, 16L)))
  expect_identical(exact$unicode_text, "café-数据-🙂")
  expect_true(is.nan(exact$not_a_number))
  expect_identical(exact$positive_infinity, Inf)

  complex <- fabric_test_read_delta(
    manifest,
    lakehouse,
    lakehouse$tables$complex_types
  )
  expect_s3_class(complex, "tbl_df")
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
  expect_identical(complex$id, 1L)
  expect_identical(complex$profile$label, "nested")
  expect_identical(
    complex$profile$amount,
    "1234567890123456789012345678901234.56"
  )
  expect_identical(complex$scores[[1L]], 1:3)
  counts <- complex$counts[[1L]]
  count_values <- stats::setNames(as.character(counts$value), counts$key)
  expect_identical(
    count_values[c("large", "small")],
    c(large = "9007199254740993", small = "2")
  )
  expect_identical(complex$items[[1L]]$label, c("first", "second"))
  expect_identical(complex$items[[1L]]$score, c(10L, 20L))
  expect_identical(complex$attributes[[1L]]$key, "primary")
  expect_identical(complex$attributes[[1L]]$value$label, "mapped")
  expect_identical(complex$attributes[[1L]]$value$enabled, TRUE)
  expect_identical(complex[["display name"]], "display café-数据")
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
    grepl("invalid-integration-token", conditionMessage(classified), fixed = TRUE)
  )
})

test_that("Arrow streams cover representative Fabric Delta snapshots", {
  fabric_test_require_package("arrow")
  manifest <- fabric_test_manifest()
  fabric_test_use_delta_runtime()
  lakehouse <- fabric_test_manifest_item(manifest, "TestLakehouse")
  warehouse <- fabric_test_manifest_item(manifest, "TestWarehouse")
  cases <- list(
    list(item = lakehouse, table = lakehouse$tables$empty),
    list(item = lakehouse, table = lakehouse$tables$typed_partitions),
    list(
      item = lakehouse,
      table = lakehouse$tables$column_mapped_id_partitioned_dv
    ),
    list(item = lakehouse, table = lakehouse$tables$complex_types),
    list(
      item = lakehouse,
      table = lakehouse$tables$schema_evolved,
      version = 0
    ),
    list(item = warehouse, table = warehouse$tables$types)
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
    exact_types = list(
      source = tables$exact_types,
      reference = tables$oracle_exact_types
    ),
    column_mapped_id_partitioned_dv = list(
      source = tables$column_mapped_id_partitioned_dv,
      reference = tables$spark_oracle_column_mapped_id_partitioned_dv
    ),
    struct_validity = list(
      source = tables$struct_validity,
      reference = tables$spark_oracle_struct_validity
    ),
    deletion_vectors = list(
      source = tables$deletion_vectors,
      reference = tables$spark_oracle_deletion_vectors
    ),
    deletion_vectors_stress = list(
      source = tables$deletion_vectors_stress,
      reference = tables$spark_oracle_deletion_vectors_stress
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
    column_mapped = c(
      tables$column_mapped,
      tables$spark_oracle_column_mapped
    ),
    column_mapped_id = c(
      tables$column_mapped_id,
      tables$spark_oracle_column_mapped_id
    ),
    column_mapped_id_partitioned_dv = c(
      tables$column_mapped_id_partitioned_dv,
      tables$spark_oracle_column_mapped_id_partitioned_dv
    ),
    struct_validity = c(
      tables$struct_validity,
      tables$spark_oracle_struct_validity
    ),
    deletion_vectors = c(
      tables$deletion_vectors,
      tables$spark_oracle_deletion_vectors
    ),
    file_row_number_collision = c(
      tables$file_row_number_collision,
      tables$spark_oracle_file_row_number_collision
    ),
    deletion_vectors_stress = c(
      tables$deletion_vectors_stress,
      tables$spark_oracle_deletion_vectors_stress
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
  expect_identical(runtime$fabric_runtime, "2.0")

  void <- fabric_test_read_delta(manifest, lakehouse, tables$void)
  void <- void[order(void$id), ]
  expect_named(void, c("id", "always_null", "details"))
  expect_identical(as.character(void$id), as.character(0:2))
  expect_true(all(is.na(void$always_null)))
  expect_identical(void$details$value, 0:2)
  expect_true(all(is.na(void$details$pending)))
  expect_false(any(is.na(void$details)))

  binary <- fabric_test_read_delta(
    manifest,
    lakehouse,
    tables$binary_partitions
  )
  binary <- binary[order(binary$id), ]
  expect_identical(binary$id, 1:4)
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
  expect_identical(evolved$id, 1:3)
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
  expect_identical(original$id, 1:2)
  expect_identical(original$name, c("alpha", "beta"))

  typed_reference <- fabric_test_read_delta(
    manifest,
    lakehouse,
    tables$oracle_typed_partitions
  )
  typed_reference <- typed_reference[order(typed_reference$id), ]
  expect_identical(typed_reference$id, 1:3)
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
    deletion_vectors_dense = "65,536 rows",
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
  manifest <- fabric_test_manifest()
  fabric_test_use_delta_runtime()
  lakehouse <- fabric_test_manifest_item(manifest, "TestLakehouse")
  tables <- lakehouse$tables
  references <- c(
    "oracle_complex_types",
    "spark_oracle_deletion_vectors_checkpoint",
    "spark_oracle_deletion_vectors_dense",
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
    value <- fabric_test_read_delta(
      manifest,
      lakehouse,
      tables[[reference]]
    )
    expect_s3_class(value, "tbl_df")
    expect_gt(nrow(value), 0L, label = reference)
    expect_gt(ncol(value), 0L, label = reference)
    key <- intersect(c("id", "row_id", "event_id"), names(value))
    expect_length(key, 1L, label = paste(reference, "stable key"))
    expect_false(
      anyDuplicated(value[[key]]) > 0L,
      label = paste(reference, "unique stable key")
    )
  }
})

test_that("every discovered Delta fixture has an integration-test disposition", {
  manifest <- fabric_test_manifest()
  lakehouse <- fabric_test_manifest_item(manifest, "TestLakehouse")
  exact_values <- c(
    "runtime", "basic", "empty", "void", "typed_partitions",
    "binary_partitions", "schema_evolved", "exact_types", "complex_types",
    "oracle_empty", "oracle_typed_partitions"
  )
  reference_comparison <- c(
    "partitioned", "column_mapped", "column_mapped_id",
    "column_mapped_id_partitioned_dv", "struct_validity",
    "deletion_vectors", "file_row_number_collision",
    "deletion_vectors_stress", "shallow_clone", "oracle_basic",
    "oracle_partitioned", "oracle_schema_evolved", "oracle_exact_types",
    "oracle_complex_types", "spark_oracle_column_mapped",
    "spark_oracle_column_mapped_id",
    "spark_oracle_column_mapped_id_partitioned_dv",
    "spark_oracle_struct_validity", "spark_oracle_deletion_vectors",
    "spark_oracle_file_row_number_collision",
    "spark_oracle_deletion_vectors_stress", "spark_oracle_shallow_clone"
  )
  unsupported_error <- c(
    "deletion_vectors_checkpoint", "deletion_vectors_dense",
    "type_widened", "type_widened_exact", "type_widened_pending",
    "type_widened_nested", "type_widened_map_key", "v2_checkpoint",
    "variant", "variant_id_dv"
  )
  full_scan <- c(
    "spark_oracle_deletion_vectors_checkpoint",
    "spark_oracle_deletion_vectors_dense", "spark_oracle_type_widened",
    "spark_oracle_type_widened_exact",
    "spark_oracle_type_widened_pending",
    "spark_oracle_type_widened_nested",
    "spark_oracle_type_widened_map_key", "spark_oracle_v2_checkpoint",
    "spark_oracle_variant", "spark_oracle_variant_id_dv"
  )
  covered_by_job_workflows <- c("livy_batch_result", "spark_job_result")
  disposition <- c(
    stats::setNames(rep("exact_values", length(exact_values)), exact_values),
    stats::setNames(
      rep("reference_comparison", length(reference_comparison)),
      reference_comparison
    ),
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
      "exact_values", "reference_comparison", "unsupported_error",
      "full_scan", "job_workflow"
    )
  )
})

test_that("Fabric Variant preview tables fail before exposing physical fields", {
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
      expect_match(
        conditionMessage(condition),
        "VariantShreddingPreview"
      )
    }
  }
})

test_that("the delta-rs reader reads the Fabric Warehouse export profile", {
  manifest <- fabric_test_manifest()
  fabric_test_use_delta_runtime()
  warehouse <- fabric_test_manifest_item(manifest, "TestWarehouse")

  types <- fabric_test_read_delta(
    manifest,
    warehouse,
    warehouse$tables$types,
    schema = "dbo"
  )
  expect_s3_class(types, "tbl_df")
  types <- types[order(types$id), ]
  expect_named(
    types,
    c(
      "id",
      "name",
      "category",
      "amount",
      "active",
      "event_date",
      "loaded_at",
      "nullable_value"
    )
  )
  expect_equal(types$id, 1:3)
  expect_identical(types$name, c("alpha", "beta", "gamma"))
  expect_identical(types$category, c("A", "B", "A"))
  expect_identical(types$amount, c("10.50", "20.00", NA_character_))
  expect_identical(types$active, c(TRUE, FALSE, NA))
  expect_identical(
    types$event_date,
    as.Date(c("2026-01-01", "2026-01-02", NA_character_))
  )
  expect_s3_class(types$loaded_at, "POSIXct")
  expect_identical(
    types$nullable_value,
    c(NA_character_, "present", NA_character_)
  )

  mutations <- fabric_test_read_delta(
    manifest,
    warehouse,
    warehouse$tables$mutations,
    schema = "dbo"
  )
  expect_s3_class(mutations, "tbl_df")
  mutations <- mutations[order(mutations$id), ]
  expect_named(mutations, names(types))
  expect_equal(mutations$id, c(2L, 3L, 4L))
  expect_identical(
    mutations$name,
    c("beta-updated", "gamma", "alpha-replacement")
  )
  expect_identical(mutations$amount, c("20.00", NA_character_, "10.50"))

  named_types <- fabric_onelake_read_delta_table(
    table_path = warehouse$tables$types,
    workspace_name = manifest$workspace_name,
    lakehouse_name = warehouse$display_name,
    item_type = "Warehouse",
    token = fabric_test_token_provider(),
    verbose = FALSE
  )
  named_types <- named_types[order(named_types$id), ]
  rownames(types) <- NULL
  rownames(named_types) <- NULL
  expect_equal(named_types, types)
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
  expect_true(fabric_onelake_delete(
    manifest$workspace_id,
    lakehouse$id,
    test_root,
    recursive = TRUE,
    confirm = TRUE,
    token = token
  ))
})
