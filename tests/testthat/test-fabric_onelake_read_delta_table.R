test_that("Delta targets preserve Fabric discovery and ABFSS addressing", {
  workspace_id <- "11111111-1111-1111-1111-111111111111"
  item_id <- "22222222-2222-2222-2222-222222222222"
  workspace <- data.frame(id = workspace_id)
  lakehouse <- tibble::tibble(
    id = item_id,
    type = "Lakehouse",
    workspaceId = workspace_id,
    properties = list(list(defaultSchema = "dbo"))
  )

  resolved <- fabric_delta_resolve_public_target(
    table_path = "nested/path/patients",
    workspace_name = workspace,
    lakehouse_name = lakehouse,
    schema = NULL,
    dfs_base = "https://westeurope-onelake.dfs.fabric.microsoft.com"
  )

  expect_equal(resolved$table_dir, "Tables/dbo/patients")
  expect_equal(resolved$item_type, "Lakehouse")
  expect_identical(
    fabric_delta_target_uri(resolved$target),
    paste0(
      "abfss://",
      workspace_id,
      "@westeurope-onelake.dfs.fabric.microsoft.com/",
      item_id,
      "/Tables/dbo/patients"
    )
  )

  named <- onelake_resolve_target(
    "Research Workspace",
    "Clinical Lakehouse",
    path = "Tables/dbo/café data",
    item_type = "Lakehouse"
  )
  expect_error(
    fabric_delta_target_uri(named),
    "paired workspace and item GUIDs",
    class = "fabric_delta_invalid_target"
  )

  named$workspace <- "ResearchWorkspace"
  expect_match(
    fabric_delta_target_uri(named),
    paste0(
      "^abfss://ResearchWorkspace@onelake[.]dfs[.]fabric[.]microsoft[.]com/",
      "Clinical%20Lakehouse[.]Lakehouse/Tables/dbo/caf%C3%A9%20data$"
    )
  )
})

test_that("Delta discovery records enforce type and workspace ownership", {
  workspace <- data.frame(
    id = "11111111-1111-1111-1111-111111111111"
  )
  wrong_workspace <- tibble::tibble(
    id = "22222222-2222-2222-2222-222222222222",
    type = "Lakehouse",
    workspaceId = "33333333-3333-3333-3333-333333333333"
  )
  expect_error(
    fabric_delta_resolve_public_target(
      "table",
      workspace,
      wrong_workspace,
      schema = "dbo",
      dfs_base = "https://onelake.dfs.fabric.microsoft.com"
    ),
    "different workspace",
    fixed = TRUE
  )

  warehouse <- tibble::tibble(
    id = "22222222-2222-2222-2222-222222222222",
    type = "Warehouse",
    workspaceId = workspace$id
  )
  resolved <- fabric_delta_resolve_public_target(
    "table",
    workspace,
    warehouse,
    schema = "dbo",
    dfs_base = "https://onelake.dfs.fabric.microsoft.com"
  )
  expect_equal(resolved$item_type, "Warehouse")
  expect_equal(resolved$target$item, warehouse$id)

  named_warehouse <- fabric_delta_resolve_public_target(
    "table",
    "Workspace",
    "Sales",
    schema = "dbo",
    dfs_base = "https://onelake.dfs.fabric.microsoft.com",
    item_type = "Warehouse"
  )
  expect_identical(named_warehouse$item_type, "Warehouse")
  expect_identical(named_warehouse$target$item, "Sales.Warehouse")

  expect_error(
    fabric_delta_resolve_public_target(
      "table",
      workspace,
      warehouse,
      schema = "dbo",
      dfs_base = "https://onelake.dfs.fabric.microsoft.com",
      item_type = "Lakehouse"
    ),
    "conflicts with the item discovery record"
  )

  notebook <- warehouse
  notebook$type <- "Notebook"
  expect_error(
    fabric_delta_resolve_public_target(
      "table",
      workspace,
      notebook,
      schema = "dbo",
      dfs_base = "https://onelake.dfs.fabric.microsoft.com"
    ),
    "must be a Lakehouse or Warehouse"
  )
})

test_that("Delta public projection, limit, version, and compatibility arguments validate", {
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

  expect_error(read_table(columns = character()), "columns must be NULL")
  expect_error(read_table(columns = c("id", "id")), "unique")
  expect_error(read_table(limit = -1), "limit must be NULL")
  expect_error(read_table(limit = 1.5), "limit must be NULL")
  expect_error(read_table(limit = Inf), "limit must be NULL")
  expect_error(read_table(version = -1), "version must be NULL")
  expect_error(read_table(version = 1.5), "version must be NULL")
  expect_error(read_table(version = 2^53 + 2), "2\\^53")
  expect_error(read_table(result = "data.frame"), class = "rlang_error")
  expect_error(
    read_table(timestamp_partition_timezone = "UTC"),
    class = "fabric_delta_unsupported_error"
  )

  local_mocked_bindings(
    fabric_delta_read_uri = function(...) tibble::tibble(id = 1L)
  )
  expect_warning(
    value <- read_table(dest_dir = tempfile("retired-staging-")),
    class = "fabric_delta_deprecated_argument"
  )
  expect_equal(value$id, 1L)
})

test_that("DataFusion queries quote identifiers and exact whole numbers", {
  expect_identical(
    fabric_delta_query(c("normal", "quote\"inside", "select"), 2^53),
    paste0(
      "SELECT \"normal\", \"quote\"\"inside\", \"select\" ",
      "FROM \"fabric_delta_table\" LIMIT 9007199254740992"
    )
  )
  expect_identical(
    fabric_delta_query(NULL, NULL),
    "SELECT * FROM \"fabric_delta_table\""
  )
  expect_identical(
    fabric_delta_whole_number_text(2147483648),
    "2147483648"
  )
})

test_that("the public reader passes Fabric auth and query options to delta-rs", {
  captured <- NULL
  provider_calls <- list()
  provider <- function(audience, force_refresh = FALSE) {
    provider_calls[[length(provider_calls) + 1L]] <<- list(
      audience = audience,
      force_refresh = force_refresh
    )
    "storage-token"
  }
  local_mocked_bindings(
    fabric_delta_read_uri = function(...) {
      captured <<- list(...)
      tibble::tibble(id = bit64::as.integer64("9007199254740993"))
    }
  )

  result <- fabric_onelake_read_delta_table(
    table_path = "table",
    workspace_name = "workspace",
    lakehouse_name = "lakehouse",
    schema = "dbo",
    token = provider,
    version = 2147483648,
    columns = c("name", "id"),
    limit = 10,
    verbose = FALSE
  )

  expect_s3_class(result, "tbl_df")
  expect_s3_class(result$id, "integer64")
  expect_identical(provider_calls[[1L]]$audience, .fabric_audience$storage)
  expect_false(provider_calls[[1L]]$force_refresh)
  expect_identical(captured$bearer_token, "storage-token")
  expect_equal(captured$version, 2147483648)
  expect_identical(captured$columns, c("name", "id"))
  expect_equal(captured$limit, 10)
  expect_identical(captured$result, "tibble")
  expect_match(
    captured$table_uri,
    "^abfss://workspace@onelake[.]dfs[.]fabric[.]microsoft[.]com/"
  )
  expect_match(captured$table_uri, "/Tables/dbo/table$", perl = TRUE)
})

test_that("Delta reads refresh once after a pre-return authentication failure", {
  calls <- 0L
  refresh <- logical()
  provider <- function(audience, force_refresh = FALSE) {
    refresh <<- c(refresh, force_refresh)
    if (force_refresh) "fresh-token" else "expired-token"
  }
  local_mocked_bindings(
    fabric_delta_read_uri = function(bearer_token, result, ...) {
      calls <<- calls + 1L
      if (identical(bearer_token, "expired-token")) {
        rlang::abort("HTTP 401 Unauthorized")
      }
      if (identical(result, "arrow_stream")) {
        return(nanoarrow::as_nanoarrow_array_stream(data.frame(id = 1L)))
      }
      tibble::tibble(id = 1L)
    }
  )

  for (result in c("tibble", "arrow_stream")) {
    calls <- 0L
    refresh <- logical()
    value <- fabric_onelake_read_delta_table(
      "table",
      "workspace",
      "lakehouse",
      token = provider,
      verbose = FALSE,
      result = result
    )
    if (identical(result, "tibble")) {
      expect_equal(value$id, 1L)
    } else {
      expect_s3_class(value, "nanoarrow_array_stream")
    }
    expect_equal(calls, 2L, label = result)
    expect_identical(refresh, c(FALSE, TRUE), label = result)
  }
})

test_that("Arrow schemas normalize DataFusion view and exact decimal types", {
  skip_if_not_installed("arrow")
  schema <- arrow::schema(
    id = arrow::int64(),
    amount = arrow::decimal128(20, 4),
    local_at = arrow::timestamp("us")
  )
  schema <- nanoarrow::as_nanoarrow_schema(schema)
  normalized <- fabric_delta_normalize_schema(schema, collect = FALSE)
  expect_identical(normalized$children$amount$format, "u")
  expect_identical(normalized$children$local_at$format, "tsu:")

  collected <- fabric_delta_normalize_schema(schema, collect = TRUE)
  expect_identical(collected$children$amount$format, "u")
  expect_identical(collected$children$local_at$format, "u")

  ptype <- fabric_delta_collect_ptype(schema, collected)
  expect_s3_class(ptype$id, "integer64")
  expect_type(ptype$amount, "character")
  expect_type(ptype$local_at, "character")
})

test_that("Variant extensions are preserved as streams and rejected for tibbles", {
  variant <- nanoarrow::na_extension(
    nanoarrow::na_struct(list(
      metadata = nanoarrow::na_binary(nullable = FALSE),
      value = nanoarrow::na_binary()
    )),
    "arrow.parquet.variant"
  )
  schema <- nanoarrow::na_struct(list(
    id = nanoarrow::na_int32(nullable = FALSE),
    payload = variant
  ))

  expect_identical(fabric_delta_variant_paths(schema), "payload")
  error <- tryCatch(
    fabric_delta_validate_collect_schema(schema),
    error = identity
  )
  expect_s3_class(error, "fabric_delta_variant_collection_error")
  expect_s3_class(error, "fabric_delta_unsupported_feature_error")
  expect_identical(error$delta_features, "Variant")
  expect_identical(error$variant_paths, "payload")
  expect_match(conditionMessage(error), 'result = "arrow_stream"', fixed = TRUE)
})

test_that("Delta timestamp_ntz values retain exact wall-clock text", {
  value <- fabric_delta_timestamp_ntz(c(
    "2026-07-28T09:08:07.654321",
    "1900-01-01T00:00:00.000001",
    "2000-02-29T00:00:00",
    NA_character_
  ))
  expect_s3_class(value, "fabric_delta_timestamp_ntz")
  expect_identical(
    unclass(value),
    c(
      "2026-07-28 09:08:07.654321",
      "1900-01-01 00:00:00.000001",
      "2000-02-29 00:00:00.000000",
      NA_character_
    )
  )
  expect_identical(format(value), unclass(value))
  expect_s3_class(value[c(1L, 4L)], "fabric_delta_timestamp_ntz")
  expect_equal(
    as.POSIXct(value[[1L]], tz = "Europe/Amsterdam"),
    as.POSIXct("2026-07-28 09:08:07.654321", tz = "Europe/Amsterdam")
  )
})

test_that("Python failures are classified and bearer tokens are redacted", {
  token <- "eyJheader.payload.signature"
  expect_error(
    fabric_delta_abort_python(
      simpleError(paste("request failed with", token)),
      bearer_token = token
    ),
    class = "fabric_delta_python_error"
  )
  error <- tryCatch(
    fabric_delta_abort_python(
      simpleError(paste("request failed with", token)),
      bearer_token = token
    ),
    error = identity
  )
  expect_false(grepl(token, conditionMessage(error), fixed = TRUE))
  expect_match(conditionMessage(error), "<redacted>", fixed = TRUE)

  expect_error(
    fabric_delta_abort_python(
      simpleError("ModuleNotFoundError: No module named 'deltalake'")
    ),
    class = "fabric_delta_environment_error"
  )
  expect_error(
    fabric_delta_abort_python(simpleError("DeltaProtocolError: unsupported")),
    class = "fabric_delta_unsupported_error"
  )

  unsupported <- tryCatch(
    fabric_delta_abort_python(simpleError(
      paste0(
        "DeltaProtocolError: Unsupported table features required: ",
        "[TypeWidening, V2Checkpoint]"
      )
    )),
    error = identity
  )
  expect_s3_class(unsupported, "fabric_delta_unsupported_feature_error")
  expect_s3_class(unsupported, "fabric_delta_unsupported_error")
  expect_identical(
    unsupported$delta_features,
    c("TypeWidening", "V2Checkpoint")
  )
  expect_match(conditionMessage(unsupported), "Fabric PySpark notebook")
})

test_that("oversized deletion vectors fail closed before table scans", {
  expect_no_error(
    fabric_delta_validate_deletion_vectors(lengths = c(100L, 65536L))
  )
  error <- tryCatch(
    fabric_delta_validate_deletion_vectors(lengths = c(10L, 100000L)),
    error = identity
  )

  expect_s3_class(error, "fabric_delta_unsupported_feature_error")
  expect_identical(error$delta_features, "LargeDeletionVector")
  expect_identical(error$deletion_vector_rows, 100000L)
  expect_identical(error$deletion_vector_row_limit, 65536)
  expect_match(conditionMessage(error), "100,000 rows", fixed = TRUE)
  expect_match(conditionMessage(error), "65,536 rows", fixed = TRUE)
})

test_that("Delta runtime requirements are declared without forcing initialization", {
  requirements <- reticulate::py_require()
  expect_true("deltalake>=1.6.2,<2" %in% requirements$packages)
  expect_true("nanoarrow>=0.8.0" %in% requirements$packages)
  expect_identical(requirements$python_version, ">=3.10")

  config <- fabric_delta_config(initialize = FALSE)
  expect_type(config, "list")
  expect_named(
    config,
    c(
      "initialized",
      "python",
      "python_version",
      "requirements",
      "available",
      "versions"
    )
  )
  expect_error(fabric_delta_config(initialize = NA), "TRUE or FALSE")
})

test_that("Delta runtime compatibility is validated before querying", {
  required <- c("DeltaTable", "QueryBuilder")
  expect_invisible(fabric_delta_validate_runtime("1.6.2", required))
  expect_invisible(fabric_delta_validate_runtime("1.99.0", required))
  expect_error(
    fabric_delta_validate_runtime("1.6.1", required),
    ">=1.6.2,<2",
    class = "fabric_delta_environment_error"
  )
  expect_error(
    fabric_delta_validate_runtime("2.0.0", required),
    ">=1.6.2,<2",
    class = "fabric_delta_environment_error"
  )
  expect_error(
    fabric_delta_validate_runtime("1.6.2", "DeltaTable"),
    "QueryBuilder",
    class = "fabric_delta_environment_error"
  )
})
