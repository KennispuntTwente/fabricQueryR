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
  fabric_onelake_read_delta_table(
    table_path = table,
    workspace_name = manifest$workspace_id,
    lakehouse_name = item$id,
    schema = schema,
    token = fabric_test_token_provider(),
    verbose = FALSE,
    ...
  )
}

fabric_test_order_delta_rows <- function(value, feature) {
  expect_true(
    "id" %in% names(value),
    label = paste(feature, "has a stable id column")
  )
  value[order(value$id, na.last = TRUE), , drop = FALSE]
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

fabric_test_delta_differences <- function(actual, expected, feature) {
  waldo::compare(
    actual,
    expected,
    x_arg = feature,
    y_arg = paste(feature, "Spark oracle"),
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

fabric_test_expect_delta_matches_oracle <- function(
  manifest,
  lakehouse,
  source,
  oracle,
  feature
) {
  actual <- fabric_test_read_delta(manifest, lakehouse, source)
  expected <- fabric_test_read_delta(manifest, lakehouse, oracle)

  expect_s3_class(actual, "tbl_df")
  expect_s3_class(expected, "tbl_df")
  expect_named(actual, names(expected), label = feature)
  expect_equal(nrow(actual), nrow(expected), label = feature)
  expect_gt(nrow(actual), 0L, label = feature)

  actual <- fabric_test_order_delta_rows(actual, feature)
  expected <- fabric_test_order_delta_rows(expected, paste(feature, "oracle"))
  actual <- fabric_test_canonicalize_delta_maps(actual)
  expected <- fabric_test_canonicalize_delta_maps(expected)
  rownames(actual) <- NULL
  rownames(expected) <- NULL
  differences <- fabric_test_delta_differences(actual, expected, feature)
  if (length(differences)) {
    fail(
      paste(
        c(
          paste("Delta result differs from its Spark oracle:", feature),
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
  integer64_columns <- names(exact)[vapply(
    exact,
    inherits,
    logical(1),
    what = "integer64"
  )]
  expect_gt(length(integer64_columns), 0L)
  character_columns <- names(exact)[vapply(exact, is.character, logical(1))]
  expect_gt(length(character_columns), 0L)

  complex <- fabric_test_read_delta(
    manifest,
    lakehouse,
    lakehouse$tables$complex_types
  )
  expect_s3_class(complex, "tbl_df")
  expect_true(any(vapply(complex, is.list, logical(1))))
})

test_that("core Fabric Delta features are handled by the table provider", {
  manifest <- fabric_test_manifest()
  fabric_test_use_delta_runtime()
  lakehouse <- fabric_test_manifest_item(manifest, "TestLakehouse")
  tables <- lakehouse$tables
  required <- list(
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
    fabric_test_expect_delta_matches_oracle(
      manifest,
      lakehouse,
      pair[[1L]],
      pair[[2L]],
      feature
    )
  }
})

test_that("unsupported Fabric Delta features fail with actionable errors", {
  manifest <- fabric_test_manifest()
  fabric_test_use_delta_runtime()
  lakehouse <- fabric_test_manifest_item(manifest, "TestLakehouse")
  tables <- lakehouse$tables
  unsupported <- c(
    type_widened = "TypeWidening",
    type_widened_exact = "TypeWidening",
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
    schema = "dbo",
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
