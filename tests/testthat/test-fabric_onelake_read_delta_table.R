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
    schema = NULL,
    dfs_base = "https://onelake.dfs.fabric.microsoft.com"
  )
  expect_equal(resolved$item_type, "Warehouse")
  expect_equal(resolved$target$item, warehouse$id)
  expect_equal(resolved$table_dir, "Tables/dbo/table")

  named_warehouse <- fabric_delta_resolve_public_target(
    "table",
    "Workspace",
    "Sales",
    schema = NULL,
    dfs_base = "https://onelake.dfs.fabric.microsoft.com",
    item_type = "Warehouse"
  )
  expect_identical(named_warehouse$item_type, "Warehouse")
  expect_identical(named_warehouse$target$item, "Sales.Warehouse")
  expect_identical(named_warehouse$table_dir, "Tables/dbo/table")

  suffixed_warehouse <- fabric_delta_resolve_public_target(
    "table",
    "Workspace",
    "Sales.Warehouse",
    schema = NULL,
    dfs_base = "https://onelake.dfs.fabric.microsoft.com",
    item_type = "Warehouse"
  )
  expect_identical(suffixed_warehouse$target$item, "Sales.Warehouse")

  expect_error(
    fabric_delta_resolve_public_target(
      "table",
      "Workspace",
      "Sales.Lakehouse",
      schema = NULL,
      dfs_base = "https://onelake.dfs.fabric.microsoft.com",
      item_type = "Warehouse"
    ),
    "conflicts with the .Lakehouse/.Warehouse item suffix",
    fixed = TRUE
  )

  expect_error(
    fabric_delta_resolve_public_target(
      "sales-orders",
      "Workspace",
      "Sales",
      schema = NULL,
      dfs_base = "https://onelake.dfs.fabric.microsoft.com",
      item_type = "Warehouse"
    ),
    "ASCII letters, digits, and underscores",
    class = "fabric_delta_invalid_target"
  )

  non_schema_lakehouse <- warehouse
  non_schema_lakehouse$type <- "Lakehouse"
  resolved_lakehouse <- fabric_delta_resolve_public_target(
    "table",
    workspace,
    non_schema_lakehouse,
    schema = NULL,
    dfs_base = "https://onelake.dfs.fabric.microsoft.com"
  )
  expect_identical(resolved_lakehouse$table_dir, "Tables/table")

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
  limited <- fabric_delta_query(NULL, 2)
  expect_identical(
    limited,
    "SELECT * FROM \"fabric_delta_table\" LIMIT 2"
  )
  expect_false(grepl("ORDER BY", limited, fixed = TRUE))
  expect_identical(
    fabric_delta_whole_number_text(2147483648),
    "2147483648"
  )
  expect_identical(
    fabric_delta_query(
      columns = c("id", "__fabric_delta_limit_row_number__"),
      limit = 2,
      has_deletion_vectors = TRUE,
      all_columns = c("id", "__fabric_delta_limit_row_number__")
    ),
    paste0(
      "SELECT \"id\", \"__fabric_delta_limit_row_number__\" ",
      "FROM (SELECT \"id\", \"__fabric_delta_limit_row_number__\", ",
      "ROW_NUMBER() OVER () AS \"__fabric_delta_limit_row_number___\" ",
      "FROM \"fabric_delta_table\") AS \"__fabric_delta_limited__\" ",
      "WHERE \"__fabric_delta_limit_row_number___\" <= 2"
    )
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
  expect_identical(captured$item_type, "Lakehouse")
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

test_that("Arrow schemas normalize exact scalar types for safe collection", {
  skip_if_not_installed("arrow")
  schema <- arrow::schema(
    regular = arrow::int32(),
    id = arrow::int64(),
    amount = arrow::decimal128(20, 4),
    local_at = arrow::timestamp("us")
  )
  schema <- nanoarrow::as_nanoarrow_schema(schema)
  normalized <- fabric_delta_normalize_schema(schema, collect = FALSE)
  expect_identical(normalized$children$amount$format, "u")
  expect_identical(normalized$children$local_at$format, "tsu:")

  collected <- fabric_delta_normalize_schema(schema, collect = TRUE)
  expect_identical(collected$children$regular$format, "g")
  expect_identical(collected$children$id$format, "u")
  expect_identical(collected$children$amount$format, "u")
  expect_identical(collected$children$local_at$format, "u")

  ptype <- fabric_delta_collect_ptype(schema, collected)
  expect_type(ptype$regular, "double")
  expect_type(ptype$id, "character")
  expect_type(ptype$amount, "character")
  expect_type(ptype$local_at, "character")
})

test_that("Delta integer NA sentinels are restored without data loss", {
  ordinary_integer <- fabric_delta_restore_integer32(
    bit64::as.integer64(c("-2147483647", NA, "2147483647"))
  )
  expect_type(ordinary_integer, "integer")
  expect_identical(ordinary_integer, c(-2147483647L, NA_integer_, 2147483647L))

  boundary_integer <- fabric_delta_restore_integer32(
    bit64::as.integer64(c("-2147483648", NA, "2147483647"))
  )
  expect_type(boundary_integer, "double")
  expect_identical(boundary_integer, c(-2147483648, NA_real_, 2147483647))

  widened_integer <- expect_no_warning(
    fabric_delta_restore_integer32(
      bit64::as.integer64(c("-2147483649", "2147483648"))
    )
  )
  expect_type(widened_integer, "double")
  expect_identical(widened_integer, c(-2147483649, 2147483648))

  expect_error(
    fabric_delta_restore_integer32(
      c("9007199254740993")
    ),
    class = "fabric_delta_conversion_error"
  )

  ordinary_long <- fabric_delta_restore_integer64(
    c("-9223372036854775807", NA, "9223372036854775807")
  )
  expect_s3_class(ordinary_long, "integer64")
  expect_identical(
    as.character(ordinary_long),
    c("-9223372036854775807", NA, "9223372036854775807")
  )

  boundary_long <- fabric_delta_restore_integer64(
    c("-9223372036854775808", NA, "9223372036854775807")
  )
  expect_s3_class(boundary_long, "fabric_delta_integer64")
  expect_identical(
    as.character(boundary_long),
    c("-9223372036854775808", NA, "9223372036854775807")
  )
  expect_s3_class(boundary_long[c(3L, 1L)], "fabric_delta_integer64")
})

test_that("nested scalar restoration uses one type per logical field", {
  skip_if_not_installed("arrow")
  schema <- nanoarrow::as_nanoarrow_schema(arrow::schema(
    scores = arrow::list_of(arrow::int32()),
    longs = arrow::list_of(arrow::int64()),
    records = arrow::list_of(arrow::struct(
      score = arrow::int32(),
      long = arrow::int64()
    ))
  ))

  scores <- list(
    bit64::as.integer64(c("-2147483648", "1")),
    bit64::as.integer64(c("2", "3")),
    NULL
  )
  restored_scores <- fabric_delta_restore_collected_types(
    scores,
    schema$children$scores
  )
  expect_type(restored_scores[[1L]], "double")
  expect_type(restored_scores[[2L]], "double")
  expect_identical(restored_scores[[2L]], c(2, 3))
  expect_null(restored_scores[[3L]])

  longs <- list(
    c("-9223372036854775808", "1"),
    c("2", "3"),
    NULL
  )
  restored_longs <- fabric_delta_restore_collected_types(
    longs,
    schema$children$longs
  )
  expect_s3_class(restored_longs[[1L]], "fabric_delta_integer64")
  expect_s3_class(restored_longs[[2L]], "fabric_delta_integer64")
  expect_identical(as.character(restored_longs[[2L]]), c("2", "3"))
  expect_null(restored_longs[[3L]])

  records <- list(
    data.frame(
      score = bit64::as.integer64(c("-2147483648", "1")),
      long = c("-9223372036854775808", "1")
    ),
    data.frame(
      score = bit64::as.integer64(c("2", "3")),
      long = c("2", "3")
    )
  )
  restored_records <- fabric_delta_restore_collected_types(
    records,
    schema$children$records
  )
  expect_type(restored_records[[2L]]$score, "double")
  expect_s3_class(
    restored_records[[2L]]$long,
    "fabric_delta_integer64"
  )
})

test_that("dictionary values follow the Delta scalar restoration contract", {
  skip_if_not_installed("arrow")
  collect_dictionary <- function(dictionary) {
    value <- arrow::DictionaryArray$create(
      arrow::Array$create(c(0L, 1L, 0L), type = arrow::int8()),
      dictionary
    )
    array <- nanoarrow::as_nanoarrow_array(value)
    source_schema <- nanoarrow::as_nanoarrow_schema(value$type)
    target_schema <- fabric_delta_normalize_schema(
      source_schema,
      collect = TRUE
    )
    ptype <- fabric_delta_collect_ptype(source_schema, target_schema)
    stream <- nanoarrow::basic_array_stream(
      list(array),
      schema = target_schema
    )
    collected <- nanoarrow::convert_array_stream(stream, to = ptype)
    fabric_delta_restore_collected_types(collected, source_schema)
  }

  ordinary_integer <- collect_dictionary(arrow::Array$create(
    c(-2147483647, 2),
    type = arrow::int32()
  ))
  expect_type(ordinary_integer, "integer")
  expect_identical(ordinary_integer, c(-2147483647L, 2L, -2147483647L))

  boundary_integer <- collect_dictionary(arrow::Array$create(
    c(-2147483648, 2),
    type = arrow::int32()
  ))
  expect_type(boundary_integer, "double")
  expect_identical(boundary_integer, c(-2147483648, 2, -2147483648))

  ordinary_long <- collect_dictionary(arrow::Array$create(
    bit64::as.integer64(c("-9223372036854775807", "2")),
    type = arrow::int64()
  ))
  expect_s3_class(ordinary_long, "integer64")
  expect_identical(
    as.character(ordinary_long),
    c("-9223372036854775807", "2", "-9223372036854775807")
  )

  long_dictionary_schema <- nanoarrow::as_nanoarrow_schema(
    arrow::dictionary(arrow::int8(), arrow::int64())
  )
  boundary_long <- fabric_delta_restore_collected_types(
    c("-9223372036854775808", "2", "-9223372036854775808"),
    long_dictionary_schema
  )
  expect_s3_class(boundary_long, "fabric_delta_integer64")
  expect_identical(
    as.character(boundary_long),
    c("-9223372036854775808", "2", "-9223372036854775808")
  )
})

test_that("Warehouse projections enforce external Delta naming limits", {
  expect_no_error(
    fabric_delta_validate_columns(
      c("name", "café", "hash#value"),
      item_type = "Warehouse"
    )
  )
  forbidden <- c(
    "display name",
    "tab\tname",
    "return\rname",
    "[left",
    "right]",
    "a,b",
    "a;b",
    "{value}",
    "call()",
    "a=b"
  )
  error <- tryCatch(
    fabric_delta_validate_columns(
      forbidden,
      item_type = "Warehouse"
    ),
    error = identity
  )
  expect_s3_class(error, "fabric_delta_invalid_target")
  expect_match(conditionMessage(error), "Invalid projected column")
  expect_match(conditionMessage(error), "square brackets")
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

  authentication <- tryCatch(
    fabric_delta_abort_python(simpleError("HTTP 401: token expired")),
    error = identity
  )
  expect_s3_class(authentication, "fabric_delta_authentication_error")
  expect_s3_class(authentication, "fabric_delta_access_error")
  expect_match(conditionMessage(authentication), "storage.azure.com")

  authorization <- tryCatch(
    fabric_delta_abort_python(simpleError("HTTP 403: Forbidden")),
    error = identity
  )
  expect_s3_class(authorization, "fabric_delta_authorization_error")
  expect_s3_class(authorization, "fabric_delta_access_error")
  expect_match(conditionMessage(authorization), "Item Read")
  expect_match(
    conditionMessage(authorization),
    "never returns policy-filtered data",
    fixed = TRUE
  )
  expect_match(
    conditionMessage(authorization),
    "Users can access data stored in OneLake with apps external to Fabric",
    fixed = TRUE
  )
  expect_false(fabric_delta_is_authentication_error(authorization))

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

test_that("deletion-vector validation accepts serialized large-file scans", {
  expect_no_error(
    fabric_delta_validate_deletion_vectors(
      features = "deletionVectors",
      deletion_vector_rows = c(100L, 65536L)
    )
  )
  expect_no_error(
    fabric_delta_validate_deletion_vectors(
      features = character(),
      deletion_vector_rows = 100000L
    )
  )
  expect_no_error(
    fabric_delta_validate_deletion_vectors(
      features = "deletionVectors",
      deletion_vector_rows = numeric()
    )
  )
  expect_identical(
    fabric_delta_validate_deletion_vectors(
      features = "DeletionVectors",
      deletion_vector_rows = c(10L, 100000L)
    ),
    c(10L, 100000L)
  )

  unknown <- tryCatch(
    fabric_delta_validate_deletion_vectors(
      features = "deletionVectors",
      deletion_vector_rows = c(10L, NA_real_)
    ),
    error = identity
  )
  expect_s3_class(unknown, "fabric_delta_unsupported_feature_error")
  expect_identical(
    unknown$delta_features,
    "UnmeasuredDeletionVectorFile"
  )
  expect_identical(unknown$deletion_vector_unknown_files, 1L)
  expect_match(conditionMessage(unknown), "selection-vector length")
})

test_that("deletion-vector list lengths use Arrow offsets", {
  skip_if_not_installed("arrow")
  values <- arrow::Array$create(
    list(c(TRUE, FALSE), logical(), NULL, c(TRUE, TRUE, FALSE)),
    type = arrow::list_of(arrow::boolean())
  )
  array <- nanoarrow::as_nanoarrow_array(values)

  expect_identical(
    fabric_delta_list_array_lengths(array),
    c(2, 0, NA_real_, 3)
  )
  expect_identical(
    fabric_delta_list_array_lengths(
      nanoarrow::as_nanoarrow_array(values$Slice(1L, 2L))
    ),
    c(0, NA_real_)
  )
})

test_that("LIMIT 0 avoids deletion-vector mask materialization", {
  deletion_vector_calls <- 0L
  queries <- character()
  table <- new.env(parent = emptyenv())
  builder <- new.env(parent = emptyenv())
  builder$register <- function(...) invisible(NULL)
  builder$execute <- function(sql) {
    queries <<- c(queries, sql)
    reader <- new.env(parent = emptyenv())
    reader$sql <- sql
    reader$read_all <- function() invisible(NULL)
    reader
  }
  deltalake <- new.env(parent = emptyenv())
  deltalake$DeltaTable <- function(...) table
  deltalake$QueryBuilder <- function() builder
  builtins <- new.env(parent = emptyenv())
  builtins$int <- function(value) value

  local_mocked_bindings(
    .delta_python = list(deltalake = deltalake, builtins = builtins),
    fabric_delta_validate_runtime = function(...) invisible(NULL),
    fabric_delta_validate_snapshot_columns = function(...) invisible(NULL),
    fabric_delta_snapshot_columns = function(...) "id",
    fabric_delta_validate_deletion_vectors = function(...) {
      deletion_vector_calls <<- deletion_vector_calls + 1L
      100000
    }
  )

  expect_no_error(fabric_delta_python_reader("table", limit = 0))
  expect_identical(deletion_vector_calls, 0L)
  expect_no_error(fabric_delta_python_reader("table", limit = 1))
  expect_identical(deletion_vector_calls, 1L)
  expect_identical(
    queries,
    c(
      "SELECT * FROM \"fabric_delta_table\" LIMIT 0",
      "SET datafusion.execution.target_partitions = 1",
      paste0(
        "SELECT \"id\" FROM (SELECT \"id\", ROW_NUMBER() OVER () AS ",
        "\"__fabric_delta_limit_row_number__\" FROM ",
        "\"fabric_delta_table\") AS \"__fabric_delta_limited__\" WHERE ",
        "\"__fabric_delta_limit_row_number__\" <= 1"
      )
    )
  )
})

test_that("collection validity descriptors do not retain Arrow data arrays", {
  array <- nanoarrow::as_nanoarrow_array(data.frame(
    id = 1:2,
    label = c("one", "two")
  ))
  schema <- nanoarrow::infer_nanoarrow_schema(array)
  descriptor <- fabric_delta_array_descriptor(array, schema)
  contains_array <- function(value) {
    if (inherits(value, "nanoarrow_array")) {
      return(TRUE)
    }
    if (!is.list(value)) {
      return(FALSE)
    }
    any(vapply(value, contains_array, logical(1)))
  }

  expect_false(contains_array(descriptor))
  expect_identical(descriptor$length, 2)
  expect_identical(
    fabric_delta_array_validity(list(descriptor)),
    c(TRUE, TRUE)
  )

  null_list <- nanoarrow::as_nanoarrow_array(list(NULL, NULL))
  null_schema <- nanoarrow::infer_nanoarrow_schema(null_list)
  expect_no_error(
    null_descriptor <- fabric_delta_array_descriptor(
      null_list,
      null_schema
    )
  )
  expect_null(null_descriptor$children[[1L]]$validity)
})

test_that("Delta runtime requirements are declared without forcing initialization", {
  requirements <- reticulate::py_require()
  expect_true("deltalake==1.6.2" %in% requirements$packages)
  expect_true("nanoarrow==0.8.0" %in% requirements$packages)
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

test_that("Delta R bridge dependencies declare their supported floors", {
  description <- read.dcf(
    file.path(fabric_test_repository_root(), "DESCRIPTION"),
    fields = c("Imports", "Suggests")
  )
  imports <- gsub("[[:space:]]+", " ", description[[1L, "Imports"]])
  suggests <- gsub("[[:space:]]+", " ", description[[1L, "Suggests"]])

  expect_match(imports, "nanoarrow \\(>= 0\\.8\\.0\\)")
  expect_match(suggests, "arrow \\(>= 9\\.0\\.0\\)")
})

test_that("nullable struct validity follows data-frame subsetting semantics", {
  value <- data.frame(
    amount = c("1.00", NA, "3.00"),
    label = c("one", NA, "three"),
    row.names = c("first", "second", "third")
  )
  value <- fabric_delta_new_struct_column(value, c(TRUE, FALSE, TRUE))

  expect_identical(value$amount, c("1.00", NA, "3.00"))
  expect_identical(value[["label"]], c("one", NA, "three"))
  expect_named(as.data.frame(value), c("amount", "label"))
  expect_identical(is.na(value["amount"]), c(FALSE, TRUE, FALSE))
  expect_identical(
    suppressWarnings(is.na(value["amount", drop = FALSE])),
    c(FALSE, TRUE, FALSE)
  )
  expect_identical(
    is.na(value[, "amount", drop = FALSE]),
    c(FALSE, TRUE, FALSE)
  )
  expect_identical(
    is.na(value[c("third", "second", "second"), , drop = FALSE]),
    c(FALSE, TRUE, TRUE)
  )
  expect_identical(
    is.na(value[c(TRUE, FALSE, TRUE), , drop = FALSE]),
    c(FALSE, FALSE)
  )
  expect_identical(
    is.na(value[-1L, , drop = FALSE]),
    c(TRUE, FALSE)
  )

  wrapped <- tibble::tibble(id = c(3L, 2L, 1L), payload = value)

  reordered <- wrapped[c(3L, 1L, 2L), , drop = FALSE]
  expect_s3_class(reordered$payload, "fabric_delta_struct_column")
  expect_identical(is.na(reordered$payload), c(FALSE, FALSE, TRUE))

  sliced <- dplyr::slice(wrapped, 3L, 1L, 2L, 2L)
  expect_s3_class(sliced$payload, "fabric_delta_struct_column")
  expect_identical(is.na(sliced$payload), c(FALSE, FALSE, TRUE, TRUE))

  filtered <- dplyr::filter(wrapped, id != 2L)
  expect_s3_class(filtered$payload, "fabric_delta_struct_column")
  expect_identical(is.na(filtered$payload), c(FALSE, FALSE))

  arranged <- dplyr::arrange(wrapped, id)
  expect_s3_class(arranged$payload, "fabric_delta_struct_column")
  expect_identical(is.na(arranged$payload), c(FALSE, TRUE, FALSE))

  empty <- dplyr::slice(wrapped, integer())
  expect_s3_class(empty$payload, "fabric_delta_struct_column")
  expect_identical(is.na(empty$payload), logical())

  bound <- dplyr::bind_rows(
    dplyr::slice(wrapped, 2L, 1L),
    dplyr::slice(wrapped, 3L)
  )
  expect_s3_class(bound$payload, "fabric_delta_struct_column")
  expect_identical(is.na(bound$payload), c(TRUE, FALSE, FALSE))

  collision <- fabric_delta_new_struct_column(
    data.frame(
      "..fabric_delta_struct_validity" = 1:2,
      "..fabric_delta_struct_row_name" = 3:4,
      check.names = FALSE
    ),
    c(FALSE, TRUE)
  )
  expect_named(
    as.data.frame(collision),
    c(
      "..fabric_delta_struct_validity",
      "..fabric_delta_struct_row_name"
    )
  )
  expect_identical(is.na(collision), c(TRUE, FALSE))
})

test_that("Delta runtime compatibility is validated before querying", {
  required <- c("DeltaTable", "QueryBuilder")
  expect_invisible(
    fabric_delta_validate_runtime("1.6.2", required, "0.8.0")
  )
  expect_error(
    fabric_delta_validate_runtime("1.6.1", required, "0.8.0"),
    "exactly version 1.6.2",
    class = "fabric_delta_environment_error"
  )
  expect_error(
    fabric_delta_validate_runtime("1.6.3", required, "0.8.0"),
    "exactly version 1.6.2",
    class = "fabric_delta_environment_error"
  )
  expect_error(
    fabric_delta_validate_runtime("1.6.2", required, "0.8.1"),
    "exactly version 0.8.0",
    class = "fabric_delta_environment_error"
  )
  expect_error(
    fabric_delta_validate_runtime("1.6.2", "DeltaTable", "0.8.0"),
    "QueryBuilder",
    class = "fabric_delta_environment_error"
  )
})
