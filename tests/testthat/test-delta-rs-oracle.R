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

test_that("every supported Delta reader feature has an oracle strategy", {
  delta_rs_parity <- c(
    "columnMapping",
    "timestampNtz"
  )
  direct_r_coverage <- c(
    deletionVectors = "delta-rs 1.6 rejects deletion-vector table features",
    typeWidening = "delta-rs 1.6 has no type-widening scan support",
    `typeWidening-preview` = "covered with the stable widening implementation",
    vacuumProtocolCheck = "has no row-scan semantics to compare",
    v2Checkpoint = "delta-rs 1.6 has no UUID sidecar-checkpoint support",
    variantType = "only preliminary unshredded Variant support is available",
    variantShredding = "mixed shredded files are not a supported oracle profile",
    `variantShredding-preview` = "the Fabric preview alias uses the same direct Variant coverage"
  )

  expect_setequal(
    .fabric_delta_supported_reader_features,
    c(delta_rs_parity, names(direct_r_coverage))
  )
  expect_true(all(nzchar(direct_r_coverage)))
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
