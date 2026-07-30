test_that("delta-rs parity assertions retain empty schemas and value kinds", {
  integer_schema <- fabric_test_delta_column_signatures(
    data.frame(value = integer())
  )
  character_schema <- fabric_test_delta_column_signatures(
    data.frame(value = character())
  )
  expect_false(identical(integer_schema, character_schema))
  expect_identical(integer_schema, c(value = "integer"))
  expect_identical(
    fabric_test_delta_canonical_scalar(raw()),
    list(type = "binary", value = "")
  )
  expect_identical(
    fabric_test_delta_canonical_scalar(NULL),
    list(type = "null")
  )
  expect_identical(
    fabric_test_delta_canonical_scalar(NaN),
    list(type = "double", value = "NaN")
  )

  map_forward <- data.frame(
    key = c("alpha", "beta"),
    value = c(1L, 2L)
  )
  map_reverse <- map_forward[2:1, , drop = FALSE]
  expect_identical(
    fabric_test_delta_canonical_value(map_forward),
    fabric_test_delta_canonical_value(map_reverse)
  )
  struct_forward <- data.frame(
    name = c("alpha", "beta"),
    value = c(1L, 2L)
  )
  expect_false(identical(
    fabric_test_delta_canonical_value(struct_forward),
    fabric_test_delta_canonical_value(struct_forward[2:1, , drop = FALSE])
  ))
  parent_null <- data.frame(value = NA_integer_)
  class(parent_null) <- c("fabric_delta_struct_column", "data.frame")
  attr(parent_null, "fabric_delta_struct_validity") <- FALSE
  present_all_null <- parent_null
  attr(present_all_null, "fabric_delta_struct_validity") <- TRUE
  expect_false(identical(
    fabric_test_delta_canonical_value(parent_null),
    fabric_test_delta_canonical_value(present_all_null)
  ))
  expect_identical(
    fabric_test_delta_canonical_value(parent_null),
    list(list(type = "struct_null"))
  )

  oracle <- data.frame(id = 1L)
  attr(oracle, "fabric_delta_oracle_metadata") <- list(
    version = 0,
    min_reader_version = 1,
    row_count = 1,
    column_names = list("id"),
    reader_features = list(),
    partition_columns = list("region", "date"),
    active_file_count = 1
  )
  expect_invisible(fabric_test_expect_delta_oracle_profile(
    oracle,
    partition_columns = c("date", "region"),
    info = "partition profile compatibility"
  ))
})

test_that("delta-rs oracle URIs cover Lakehouse and Warehouse items", {
  manifest <- list(workspace_id = "workspace id")
  lakehouse <- list(
    id = "lakehouse id",
    type = "Lakehouse",
    schema = "curated"
  )
  warehouse <- list(id = "warehouse id", type = "Warehouse")

  expect_identical(
    fabric_test_delta_oracle_uri(manifest, lakehouse, "café table"),
    paste0(
      "abfss://workspace%20id@onelake.dfs.fabric.microsoft.com/",
      "lakehouse%20id.Lakehouse/Tables/curated/caf%C3%A9%20table"
    )
  )
  expect_identical(
    fabric_test_delta_oracle_uri(manifest, warehouse, "fact table"),
    paste0(
      "abfss://workspace%20id@onelake.dfs.fabric.microsoft.com/",
      "warehouse%20id.Warehouse/Tables/dbo/fact%20table"
    )
  )

  guid_manifest <- list(
    workspace_id = "11111111-1111-1111-1111-111111111111"
  )
  guid_lakehouse <- list(
    id = "22222222-2222-2222-2222-222222222222",
    type = "Lakehouse",
    schema = "dbo"
  )
  expect_identical(
    fabric_test_delta_oracle_uri(
      guid_manifest,
      guid_lakehouse,
      "fact table"
    ),
    paste0(
      "abfss://11111111-1111-1111-1111-111111111111",
      "@onelake.dfs.fabric.microsoft.com/",
      "22222222-2222-2222-2222-222222222222/",
      "Tables/dbo/fact%20table"
    )
  )
})

test_that("every row-affecting reader feature has an independent oracle", {
  delta_rs_parity <- "timestampNtz"
  spark_materialized_parity <- c(
    "columnMapping",
    "deletionVectors",
    "typeWidening",
    "typeWidening-preview",
    "v2Checkpoint",
    "variantType",
    "variantShredding",
    "variantShredding-preview"
  )
  non_scan_features <- "vacuumProtocolCheck"

  expect_setequal(
    .fabric_delta_supported_reader_features,
    c(delta_rs_parity, spark_materialized_parity, non_scan_features)
  )
})

test_that("Spark Variant oracle displays normalize to DuckDB semantics", {
  object <- paste0(
    '{"action":"checkout","items":[1,2,3],',
    '"user_id":4471}'
  )
  mixed <- '[1,"two",true,null,{"nested":3}]'
  unicode <- '{"decimal":1234567890.125,"unicode":"café-数据-🙂"}'

  expect_identical(
    fabric_test_delta_variant_spark_display(object),
    "{'action': checkout, 'items': [1, 2, 3], 'user_id': 4471}"
  )
  expect_identical(
    fabric_test_delta_variant_spark_display(mixed),
    "[1, two, true, NULL, {'nested': 3}]"
  )
  expect_identical(
    fabric_test_delta_variant_spark_display(unicode),
    "{'decimal': 1234567890.125, 'unicode': café-数据-🙂}"
  )
  expect_identical(
    unname(vapply(
      c(NA_character_, "root string", "9007199254740993"),
      fabric_test_delta_variant_spark_display,
      character(1)
    )),
    c(NA_character_, "root string", "9007199254740993")
  )

  expect_identical(
    fabric_test_delta_variant_display(NULL),
    NA_character_
  )
  expect_identical(
    fabric_test_delta_variant_display(list(
      type = "VARIANT_NULL",
      display = "null"
    )),
    NA_character_
  )
  expect_identical(
    fabric_test_delta_variant_display(list(
      type = "VARCHAR",
      display = "root string"
    )),
    "root string"
  )
})

test_that("R Delta snapshots agree with deterministic delta-rs fixtures", {
  fabric_test_require_package("DBI")
  fabric_test_require_package("duckdb")
  fabric_test_require_package("fs")
  fabric_test_require_delta_oracle()

  directory <- tempfile("delta-rs-fixtures-")
  dir.create(directory)
  on.exit(unlink(directory, recursive = TRUE, force = TRUE), add = TRUE)
  fabric_test_delta_oracle_run(c(
    "write-fixtures",
    "--directory",
    shQuote(directory)
  ))
  manifest <- jsonlite::fromJSON(
    file.path(directory, "manifest.json"),
    simplifyVector = FALSE
  )

  expect_match(manifest$deltalake_version, "^1[.]6[.]")
  expect_setequal(
    vapply(manifest$cases, `[[`, character(1), "name"),
    c(
      "primitive_latest",
      "primitive_version_0",
      "primitive_projection",
      "empty",
      "schema_evolved",
      "schema_evolved_version_0",
      "nested",
      "scalar_boundaries",
      "mutated_latest",
      "mutated_version_0"
    )
  )
  for (case in manifest$cases) {
    table <- file.path(directory, case$table)
    version <- case$version %||% NULL
    columns <- unlist(case$columns %||% list(), use.names = FALSE)
    if (!length(columns)) {
      columns <- NULL
    }
    limit <- case$limit %||% NULL
    actual <- fabric_delta_read_staged(
      table,
      version = version,
      columns = columns,
      limit = limit
    )
    oracle <- fabric_test_delta_oracle_read(
      table,
      version = version,
      columns = columns,
      limit = limit
    )
    fabric_test_expect_delta_oracle_equal(actual, oracle, case$name)
    expect_equal(nrow(actual), as.integer(case$expected_rows), info = case$name)
    expect_named(
      actual,
      unlist(case$expected_columns, use.names = FALSE),
      info = case$name
    )
    fabric_test_expect_delta_oracle_profile(
      oracle,
      version = as.numeric(case$expected_version),
      min_active_files = if (identical(case$name, "empty")) 0 else 1,
      info = case$name
    )
  }
})
