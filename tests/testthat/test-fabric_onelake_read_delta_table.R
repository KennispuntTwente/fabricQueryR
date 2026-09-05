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
    table_path = "patients",
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
  discovered_dfs <- "https://northeurope-onelake.dfs.fabric.microsoft.com"
  discovered <- fabric_delta_resolve_public_target(
    table_path = "patients",
    workspace_name = list(
      id = workspace_id,
      oneLakeEndpoints = list(dfsEndpoint = discovered_dfs)
    ),
    lakehouse_name = lakehouse,
    schema = NULL,
    dfs_base = NULL
  )
  expect_identical(discovered$target$dfs_base, discovered_dfs)
  private_target <- resolved$target
  private_host <- paste0(
    gsub("-", "", workspace_id, fixed = TRUE),
    ".z11.dfs.fabric.microsoft.com"
  )
  private_target$dfs_base <- paste0(
    "https://",
    private_host
  )
  expect_identical(
    fabric_delta_target_uri(private_target),
    paste0(
      "abfss://",
      workspace_id,
      "@onelake.dfs.fabric.microsoft.com/",
      item_id,
      "/Tables/dbo/patients"
    )
  )
  expect_null(fabric_delta_storage_endpoint(resolved$target))
  expect_identical(
    fabric_delta_storage_endpoint(private_target),
    paste0(
      "https://",
      sub(".dfs.", ".blob.", private_host, fixed = TRUE)
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
  target_error <- tryCatch(
    fabric_delta_target_uri(named),
    error = identity
  )
  expect_s3_class(target_error, "fabric_delta_error")

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

  mirrored <- warehouse
  mirrored$type <- "MirroredDatabase"
  mirrored$default_schema <- "replicated"
  resolved_mirrored <- fabric_delta_resolve_public_target(
    "orders",
    workspace,
    mirrored,
    schema = NULL,
    dfs_base = "https://onelake.dfs.fabric.microsoft.com"
  )
  expect_identical(resolved_mirrored$item_type, "MirroredDatabase")
  expect_identical(
    resolved_mirrored$table_dir,
    "Tables/replicated/orders"
  )

  named_mirrored <- fabric_delta_resolve_public_target(
    "orders",
    "Workspace",
    "Replica",
    schema = NULL,
    dfs_base = "https://onelake.dfs.fabric.microsoft.com",
    item_type = "MirroredDatabase"
  )
  expect_identical(
    named_mirrored$target$item,
    "Replica.MirroredDatabase"
  )
  expect_identical(named_mirrored$table_dir, "Tables/dbo/orders")

  expect_error(
    fabric_delta_resolve_public_target(
      "table",
      "Workspace",
      "Sales.Lakehouse",
      schema = NULL,
      dfs_base = "https://onelake.dfs.fabric.microsoft.com",
      item_type = "Warehouse"
    ),
    "conflicts with the .Lakehouse/.Warehouse/.MirroredDatabase item suffix",
    fixed = TRUE
  )

  named_warehouse <- fabric_delta_resolve_public_target(
    "sales-orders",
    "Workspace",
    "Sales",
    schema = NULL,
    dfs_base = "https://onelake.dfs.fabric.microsoft.com",
    item_type = "Warehouse"
  )
  expect_identical(named_warehouse$table_dir, "Tables/dbo/sales-orders")

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
      "wrong/table",
      workspace,
      non_schema_lakehouse,
      schema = NULL,
      dfs_base = "https://onelake.dfs.fabric.microsoft.com"
    ),
    "must be one table name",
    fixed = TRUE
  )
  expect_error(
    fabric_delta_resolve_public_target(
      "wrong\\table",
      workspace,
      non_schema_lakehouse,
      schema = NULL,
      dfs_base = "https://onelake.dfs.fabric.microsoft.com"
    ),
    "must be one table name",
    fixed = TRUE
  )

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
    "must be a Lakehouse, Warehouse, or MirroredDatabase"
  )
})

test_that("Delta schemas must be one OneLake path segment", {
  expect_error(
    fabric_delta_resolve_public_target(
      table_path = "Orders",
      workspace_name = "Analytics",
      lakehouse_name = "Curated.Lakehouse",
      schema = "sales/archive",
      dfs_base = NULL,
      item_type = "Lakehouse"
    ),
    "schema must be exactly one URI path segment",
    fixed = TRUE
  )
})

test_that("Delta public projection, limit, and version arguments validate", {
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
})

test_that("Delta queries project and limit without feature workarounds", {
  expect_identical(
    fabric_delta_query(c("normal", "quote\"inside"), 2^53),
    paste0(
      "SELECT \"normal\", \"quote\"\"inside\" ",
      "FROM \"fabric_delta_table\" LIMIT 9007199254740992"
    )
  )
  expect_identical(
    fabric_delta_query(NULL, NULL),
    "SELECT * FROM \"fabric_delta_table\""
  )
})

test_that("the public reader passes Fabric auth and query options to delta-rs", {
  captured <- NULL
  workspace_id <- "11111111-1111-1111-1111-111111111111"
  item_id <- "22222222-2222-2222-2222-222222222222"
  private_host <- paste0(
    gsub("-", "", workspace_id, fixed = TRUE),
    ".z11.dfs.fabric.microsoft.com"
  )
  workspace <- list(
    id = workspace_id,
    oneLakeEndpoints = list(dfsEndpoint = paste0("https://", private_host))
  )
  lakehouse <- list(
    id = item_id,
    type = "Lakehouse",
    workspaceId = workspace_id,
    properties = list(defaultSchema = "dbo")
  )
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
    workspace_name = workspace,
    lakehouse_name = lakehouse,
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
  expect_identical(
    captured$table_uri,
    paste0(
      "abfss://",
      workspace_id,
      "@onelake.dfs.fabric.microsoft.com/",
      item_id,
      "/Tables/dbo/table"
    )
  )
  expect_identical(
    captured$storage_endpoint,
    paste0(
      "https://",
      sub(".dfs.", ".blob.", private_host, fixed = TRUE)
    )
  )
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

test_that("Delta Arrow reads refresh when staging encounters expired auth", {
  refresh <- logical()
  provider <- function(audience, force_refresh = FALSE) {
    refresh <<- c(refresh, force_refresh)
    if (force_refresh) "fresh-token" else "expired-token"
  }
  local_mocked_bindings(
    fabric_delta_resolve_public_target = function(...) {
      list(
        table_dir = "Tables/Test",
        target = list(
          dfs_base = "https://onelake.dfs.fabric.microsoft.com",
          workspace = "workspace-id",
          item = "lakehouse-id",
          path = "Tables/Test"
        )
      )
    },
    fabric_delta_read_uri = function(bearer_token, ...) bearer_token,
    fabric_delta_spool_stream = function(stream) {
      if (identical(stream, "expired-token")) {
        stop("HTTP 401 token expired")
      }
      nanoarrow::as_nanoarrow_array_stream(data.frame(id = 1L))
    }
  )

  value <- fabric_onelake_read_delta_table(
    "Test",
    "workspace-id",
    "lakehouse-id",
    token = provider,
    result = "arrow_stream",
    verbose = FALSE
  )

  expect_s3_class(value, "nanoarrow_array_stream")
  expect_identical(refresh, c(FALSE, TRUE))
})

test_that("staged Delta streams remain readable without their remote reader", {
  source <- nanoarrow::as_nanoarrow_array_stream(data.frame(
    id = 1:3,
    value = c("a", "b", "c")
  ))
  attr(source, "fabric_delta_snapshot_version") <- 42

  stream <- fabric_delta_spool_stream(source)
  path <- attr(stream, "fabric_delta_spool_path", exact = TRUE)
  withr::defer(unlink(path, force = TRUE))

  expect_true(file.exists(path))
  expect_equal(
    nanoarrow::convert_array_stream(stream),
    data.frame(id = 1:3, value = c("a", "b", "c"))
  )
  expect_identical(
    attr(stream, "fabric_delta_snapshot_version", exact = TRUE),
    42
  )
  stream[["release"]]()
  expect_false(file.exists(path))
})

test_that("an Arrow reader releases its staged Delta stream", {
  skip_if_not_installed("arrow")
  source <- nanoarrow::as_nanoarrow_array_stream(data.frame(id = 1:3))
  stream <- fabric_delta_spool_stream(source)
  path <- attr(stream, "fabric_delta_spool_path", exact = TRUE)
  withr::defer(unlink(path, force = TRUE))

  reader <- arrow::as_record_batch_reader(stream)
  expect_equal(reader$read_table()$num_rows, 3L)
  expect_true(file.exists(path))
  reader$Close()

  expect_false(file.exists(path))
})

test_that("Arrow schemas normalize scalar types for safe collection", {
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
})

test_that("the public reader preserves native invalid-target failures", {
  local_mocked_bindings(
    fabric_delta_read_uri = function(...) {
      rlang::abort(
        "Warehouse target became invalid during reader preflight",
        class = c("fabric_delta_invalid_target", "fabric_delta_error")
      )
    }
  )

  error <- tryCatch(
    fabric_onelake_read_delta_table(
      table_path = "table",
      workspace_name = "workspace",
      lakehouse_name = "warehouse",
      item_type = "Warehouse",
      token = "token",
      verbose = FALSE
    ),
    error = identity
  )

  expect_s3_class(error, "fabric_delta_invalid_target")
  expect_s3_class(error, "fabric_delta_error")
  expect_false(inherits(error, "fabric_delta_python_error"))
  expect_match(conditionMessage(error), "reader preflight")
})

test_that("tibble collection rejects nested and extension columns", {
  nested <- nanoarrow::na_struct(list(
    id = nanoarrow::na_int32(nullable = FALSE),
    profile = nanoarrow::na_struct(list(name = nanoarrow::na_string()))
  ))
  error <- tryCatch(
    fabric_delta_validate_collect_schema(nested),
    error = identity
  )
  expect_s3_class(error, "fabric_delta_nested_collection_error")
  expect_identical(error$delta_columns, "profile")
  expect_match(conditionMessage(error), 'result = "arrow_stream"', fixed = TRUE)

  scalar <- nanoarrow::na_struct(list(
    id = nanoarrow::na_int64(),
    amount = nanoarrow::na_decimal128(20, 4)
  ))
  expect_invisible(fabric_delta_validate_collect_schema(scalar))
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

  schema <- list(to_json = function() {
    jsonlite::toJSON(
      list(
        type = "struct",
        fields = list(list(
          name = "payload",
          type = "variant",
          nullable = TRUE,
          metadata = list()
        ))
      ),
      auto_unbox = TRUE
    )
  })
  preflight <- tryCatch(
    fabric_delta_check_protocol(
      list(reader_features = "variantType"),
      schema = schema
    ),
    error = identity
  )
  translated <- tryCatch(
    fabric_delta_abort_python(preflight),
    error = identity
  )
  expect_s3_class(translated, "fabric_delta_unsupported_feature_error")
  expect_identical(conditionMessage(translated), conditionMessage(preflight))
  expect_identical(translated$delta_features, "VariantType")
})

test_that("the production Delta adapter configures and owns its query", {
  table_args <- NULL
  registered <- NULL
  executed <- NULL
  converted_version <- NULL

  table <- new.env(parent = emptyenv())
  table$protocol <- function() list(reader_features = character())
  table$schema <- function() list()

  reader <- new.env(parent = emptyenv())
  builder <- new.env(parent = emptyenv())
  builder$register <- function(name, value) {
    registered <<- list(name = name, value = value)
    invisible(NULL)
  }
  builder$execute <- function(query) {
    executed <<- query
    reader
  }

  delta_python <- new.env(parent = emptyenv())
  delta_python$builtins <- list(
    int = function(value) {
      converted_version <<- value
      paste0("python-int:", value)
    }
  )
  delta_python$deltalake <- list(
    DeltaTable = function(...) {
      table_args <<- list(...)
      table
    },
    QueryBuilder = function() builder
  )

  local_mocked_bindings(.delta_python = delta_python)
  local_mocked_bindings(
    dict = function(..., convert = TRUE) {
      expect_false(convert)
      list(...)
    },
    py_to_r = identity,
    .package = "reticulate"
  )

  actual <- fabric_delta_python_reader(
    table_uri = "abfss://workspace@onelake/table",
    bearer_token = "storage-token",
    storage_endpoint = "https://private.blob.fabric.microsoft.com",
    version = 42,
    columns = c("id", "a\"b"),
    limit = 5
  )

  expect_identical(actual, reader)
  expect_identical(table_args$table_uri, "abfss://workspace@onelake/table")
  expect_identical(
    table_args$storage_options,
    list(
      use_fabric_endpoint = "true",
      bearer_token = "storage-token",
      azure_storage_endpoint = "https://private.blob.fabric.microsoft.com"
    )
  )
  expect_identical(converted_version, "42")
  expect_identical(table_args$version, "python-int:42")
  expect_identical(registered$name, "fabric_delta_table")
  expect_identical(registered$value, table)
  expect_identical(
    executed,
    'SELECT "id", "a""b" FROM "fabric_delta_table" LIMIT 5'
  )
  expect_identical(attr(actual, "fabric_delta_table", exact = TRUE), table)
  expect_identical(
    attr(actual, "fabric_delta_query_builder", exact = TRUE),
    builder
  )
})

test_that("Delta stream adapters preserve owners and collect scalar data", {
  source_schema <- nanoarrow::na_struct(list(id = nanoarrow::na_int32()))
  target_schema <- nanoarrow::na_struct(list(id = nanoarrow::na_int32()))
  converted_to <- NULL

  table <- new.env(parent = emptyenv())
  table$version <- function() 17L
  reader <- new.env(parent = emptyenv())
  reader$schema <- source_schema
  attr(reader, "fabric_delta_table") <- table

  local_mocked_bindings(
    as_nanoarrow_array_stream = function(x, schema = NULL, ...) {
      expect_identical(x, reader)
      expect_s3_class(schema, "nanoarrow_schema")
      new.env(parent = emptyenv())
    },
    infer_nanoarrow_schema = function(stream) target_schema,
    infer_nanoarrow_ptype = function(schema) {
      expect_identical(schema, target_schema)
      data.frame(id = double())
    },
    convert_array_stream = function(stream, to) {
      converted_to <<- to
      data.frame(id = c(1, 2))
    },
    .package = "nanoarrow"
  )
  local_mocked_bindings(
    py_to_r = identity,
    .package = "reticulate"
  )

  stream <- fabric_delta_reader_stream(reader, collect = FALSE)
  expect_identical(
    attr(stream, "fabric_delta_python_owner", exact = TRUE),
    reader
  )
  expect_s3_class(
    attr(stream, "fabric_delta_source_schema", exact = TRUE),
    "nanoarrow_schema"
  )
  expect_identical(
    attr(stream, "fabric_delta_snapshot_version", exact = TRUE),
    17
  )

  value <- fabric_delta_collect_reader(reader)
  expect_s3_class(value, "tbl_df")
  expect_identical(value$id, c(1, 2))
  expect_s3_class(converted_to, "data.frame")
})

test_that("Delta protocol preflight rejects unsupported reader features", {
  protocol <- function(features) {
    list(reader_features = features)
  }

  expect_invisible(fabric_delta_check_protocol(protocol(c(
    "columnMapping",
    "timestampNtz",
    "deletionVectors"
  ))))
  for (feature in c(
    "typeWidening",
    "typeWidening-preview",
    "v2Checkpoint",
    "variantShredding",
    "variantShredding-preview"
  )) {
    error <- expect_error(
      fabric_delta_check_protocol(protocol(feature)),
      class = "fabric_delta_unsupported_feature_error"
    )
    expect_true(length(error$delta_features) == 1L, label = feature)
    expect_match(conditionMessage(error), "SQL or PySpark", label = feature)
  }

  combined <- expect_error(
    fabric_delta_check_protocol(protocol(c(
      "deletionVectors",
      "v2Checkpoint"
    ))),
    class = "fabric_delta_unsupported_feature_error"
  )
  expect_setequal(
    combined$delta_features,
    "V2Checkpoint"
  )
})

test_that("Variant schema uses delta-rs string serialization", {
  schema <- list(
    json = \() list(type = "struct", fields = list()),
    to_json = function() {
      paste0(
        '{"type":"struct","fields":[',
        '{"name":"payload","type":"variant"}]}'
      )
    }
  )

  expect_identical(fabric_delta_variant_columns(schema), "payload")
})

test_that("unshredded Variant permits only non-Variant projections", {
  schema <- list(to_json = function() {
    jsonlite::toJSON(
      list(
        type = "struct",
        fields = list(
          list(
            name = "id",
            type = "long",
            nullable = FALSE,
            metadata = list()
          ),
          list(
            name = "payload",
            type = "variant",
            nullable = TRUE,
            metadata = list()
          ),
          list(
            name = "nested",
            type = list(
              type = "array",
              elementType = list(
                type = "struct",
                fields = list(list(
                  name = "value",
                  type = "variant",
                  nullable = TRUE,
                  metadata = list()
                ))
              ),
              containsNull = TRUE
            ),
            nullable = TRUE,
            metadata = list()
          )
        )
      ),
      auto_unbox = TRUE
    )
  })
  protocol <- list(reader_features = "variantType-preview")

  expect_invisible(fabric_delta_check_protocol(
    protocol,
    schema = schema,
    columns = "id"
  ))
  error <- expect_error(
    fabric_delta_check_protocol(protocol, schema = schema),
    class = "fabric_delta_unsupported_feature_error"
  )
  expect_identical(error$delta_features, "VariantType-preview")
  expect_identical(error$delta_columns, c("payload", "nested"))
  expect_match(conditionMessage(error), "physical Variant binary", fixed = TRUE)
})

test_that("deletion-vector scans preserve source row order", {
  calls <- character()
  builder <- list(
    execute = function(sql) {
      calls <<- c(calls, sql)
      list(read_all = function() {
        calls <<- c(calls, "read_all")
        invisible(NULL)
      })
    }
  )

  expect_invisible(fabric_delta_configure_query(
    builder,
    c("columnMapping", "timestampNtz")
  ))
  expect_length(calls, 0L)

  expect_invisible(fabric_delta_configure_query(builder, "deletionVectors"))
  expect_identical(
    calls,
    c(
      "SET datafusion.execution.target_partitions = '1'",
      "read_all"
    )
  )
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

test_that("Delta configuration avoids interpreter discovery when uninitialized", {
  config_calls <- 0L
  local_mocked_bindings(
    py_require = function(...) {
      list(python_version = ">=3.10", packages = "deltalake==1.6.2")
    },
    py_available = function(initialize = FALSE) FALSE,
    py_config = function(...) {
      config_calls <<- config_calls + 1L
      stop("must not initialize Python")
    },
    .package = "reticulate"
  )

  config <- fabric_delta_config(initialize = FALSE)

  expect_false(config$initialized)
  expect_null(config$python)
  expect_null(config$python_version)
  expect_identical(config_calls, 0L)
})

test_that("Delta runtime configuration initializes and reports missing modules", {
  local_mocked_bindings(
    py_require = function(...) {
      list(
        python_version = ">=3.10",
        packages = c("deltalake==1.6.2", "nanoarrow==0.8.0")
      )
    },
    py_available = function(initialize = FALSE) TRUE,
    py_config = function(...) {
      list(
        python = "C:/python/python.exe",
        version = numeric_version("3.12.7")
      )
    },
    py_module_available = function(module) FALSE,
    .package = "reticulate"
  )

  config <- fabric_delta_config(initialize = TRUE)

  expect_true(config$initialized)
  expect_identical(config$python, "C:/python/python.exe")
  expect_identical(config$python_version, "3.12.7")
  expect_identical(
    config$available,
    c(deltalake = FALSE, nanoarrow = FALSE)
  )
  expect_null(config$versions)
})
test_that("Delta normalization preserves ordinary large buffer types", {
  skip_if_not_installed("arrow")
  for (type in list(arrow::large_utf8(), arrow::large_binary())) {
    schema <- nanoarrow::as_nanoarrow_schema(type)
    expect_identical(
      fabric_delta_normalize_schema(schema)$format,
      schema$format
    )
    expect_identical(
      fabric_delta_normalize_schema(schema, collect = TRUE)$format,
      schema$format
    )
  }
})
