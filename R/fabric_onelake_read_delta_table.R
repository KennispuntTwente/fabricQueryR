.fabric_delta_max_exact_version <- 2^53
.fabric_delta_result_types <- c("tibble", "arrow_stream")
.fabric_delta_unsupported_reader_features <- c(
  typewidening = "TypeWidening",
  typewideningpreview = "TypeWidening-preview",
  v2checkpoint = "V2Checkpoint",
  variantshredding = "VariantShredding",
  variantshreddingpreview = "VariantShredding-preview"
)
.fabric_delta_variant_reader_features <- c(
  varianttype = "VariantType",
  varianttypepreview = "VariantType-preview"
)

#' Read a Delta table from OneLake
#'
#' Loads a Lakehouse or compatible Warehouse table into R. By default the result
#' is a tibble; you can select columns, preview a limited number of rows, read an
#' earlier table version, or return an Arrow stream for larger results
#'
#' @section Basic use:
#' Supply the table name, workspace, and Lakehouse. Names, IDs, and discovery
#' records are accepted. If the Lakehouse uses schemas, pass the schema name
#' separately. The function otherwise reads the latest version and all columns
#' and rows into a tibble
#'
#' Use `columns` to keep only the fields you need, `limit` for a quick preview,
#' and `version` to read an earlier version. A row limit does not guarantee
#' which rows are selected
#'
#' @section Large and nested results:
#' For a large table, or one containing nested data, set
#' `result = "arrow_stream"` to process rows in batches instead of collecting
#' them all into R memory. The stream is disk-backed and can be read only once,
#' so enough temporary disk space must be available for the selected data.
#' Release the stream deterministically when finished: call
#' `stream[["release"]]()` when using 'nanoarrow' directly, or call
#' `reader$Close()` after `arrow::as_record_batch_reader(stream)`. Do not rely
#' on garbage collection to delete the staged file, particularly on Windows
#'
#' @section Column types:
#' Common dates, timestamps, numbers, text, and logical values are converted to
#' practical R types. Values that R cannot represent exactly, including decimal
#' and 64-bit integer values, are returned as character data when collecting a
#' tibble. Nested columns require an Arrow stream. The complete mapping is:
#'
#' | Delta/Arrow source | Arrow stream result | Tibble result |
#' |---|---|---|
#' | Decimal (any precision/scale) | UTF-8 text | character |
#' | Large UTF-8 / large binary | UTF-8 / binary with 32-bit offsets | character / blob list-column |
#' | Large-list variants | list with 32-bit offsets | rejected as nested |
#' | Signed/unsigned 64-bit integer | original integer type | exact character |
#' | Signed 32-bit integer | original integer type | double |
#' | Timestamp without timezone | original Arrow timestamp | character |
#' | Timestamp with timezone | original Arrow timestamp | UTC `POSIXct` |
#' | Date, Boolean, floating point, smaller integers, UTF-8, binary | corresponding Arrow scalar | corresponding R scalar type from 'nanoarrow' |
#' | Struct, map, list, extension/Variant | corresponding normalized Arrow type when supported | rejected; request an Arrow stream |
#'
#' Decimal text retains its scale and digits. Some large Arrow buffer types are
#' normalized for R compatibility and may fail if one value exceeds the
#' supported buffer size
#'
#' @section Permissions and supported tables:
#' Direct reads require OneLake data access; item `Read` permission by itself is
#' not enough. The caller needs `ReadAll` or a suitable OneLake security role,
#' and the tenant setting for external OneLake apps must be enabled. Callers
#' restricted by row- or column-level security must use a supported Fabric
#' engine instead. See the
#' [Fabric permission model](https://learn.microsoft.com/en-us/fabric/security/permission-model)
#' and [OneLake tenant settings](https://learn.microsoft.com/en-us/fabric/admin/service-admin-portal-onelake)
#'
#' This function uses the Python
#' [deltalake](https://pypi.org/project/deltalake/) reader through 'reticulate'
#' Some newer Delta features, including Type Widening, V2 Checkpoints, and
#' shredded Fabric Variant, are not supported by that reader. The reader can
#' query an unshredded Variant table only when `columns` explicitly excludes
#' every top-level column containing Variant values. It otherwise returns
#' Variant's physical binary storage instead of decoded logical values, so this
#' function rejects that projection. Use SQL or Spark (Livy) for Variant values
#' or when the function reports another unsupported table feature
#'
#' Compatible Warehouse tables can also be read through their published Delta
#' logs. If the reader cannot open a Warehouse table, use [fabric_sql_query()]
#'
#' @param table_path Table name. Supply its schema separately when needed
#' @param workspace_name Workspace name, ID, or an object returned by
#'   [fabric_workspaces()]
#' @param lakehouse_name Lakehouse name, ID, or discovery object. Compatible
#'   Warehouse and mirrored database items are also accepted
#' @param schema Schema containing the table, or `NULL`. Warehouses and mirrored
#'   databases default to `"dbo"` when discovery provides no default. Use `""`
#'   for a physical table directly below `Tables/` without a schema directory
#' @param item_type `"Lakehouse"`, `"Warehouse"`, `"MirroredDatabase"`, or
#'   `NULL`. Usually inferred; specify it only when using an item name without a
#'   type suffix
#' @param tenant_id Microsoft Entra tenant ID. Defaults to
#'   `FABRICQUERYR_TENANT_ID`
#' @param client_id Microsoft Entra application/client ID. Defaults to
#'   `FABRICQUERYR_CLIENT_ID`, then the Azure CLI application ID
#' @param token Optional access token or token-provider function. Most users can
#'   leave this as `NULL` and let 'fabricQueryR' sign in
#' @param auth_args Extra sign-in options passed to
#'   [AzureAuth::get_azure_token()]
#' @param version Specific table version to read, or `NULL` for the latest
#' @param verbose Whether to show authentication and read progress
#' @param dfs_base OneLake service address. Most users should keep the default;
#'   a workspace-specific address discovered from Fabric is used when available
#' @param columns Column names to return, or `NULL` for all columns
#' @param limit Maximum number of rows to return, or `NULL` for all rows
#' @param result `"tibble"` (the default) or `"arrow_stream"` for batch
#'   processing
#'
#' @return A tibble, or a disk-backed, lazy, single-use Arrow stream when
#'   `result = "arrow_stream"`. Explicitly release that stream, or close an
#'   'arrow' reader that takes ownership of it, to delete its temporary file
#' @export
#'
#' @examples
#' \dontrun{
#' # Discover a Lakehouse and one of its Delta tables
#' workspace <- fabric_workspaces()[[1L]]
#' lakehouse <- fabric_lakehouses(workspace)[[1L]]
#' tables <- fabric_lakehouse_tables(lakehouse)
#' table <- tables[1L, ]
#'
#' # Read the discovered table into a tibble
#' rows <- fabric_onelake_read_delta_table(
#'   table_path = table$name[[1L]],
#'   workspace_name = workspace,
#'   lakehouse_name = lakehouse,
#'   schema = table$schema[[1L]]
#' )
#'
#' # Stream the same table when it may not fit in R memory
#' stream <- fabric_onelake_read_delta_table(
#'   table_path = table$name[[1L]],
#'   workspace_name = workspace,
#'   lakehouse_name = lakehouse,
#'   schema = table$schema[[1L]],
#'   result = "arrow_stream"
#' )
#' reader <- arrow::as_record_batch_reader(stream)
#' rows <- reader$read_table()
#' reader$Close()
#' }
fabric_onelake_read_delta_table <- function(
  table_path,
  workspace_name,
  lakehouse_name,
  schema = NULL,
  item_type = NULL,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  token = NULL,
  auth_args = list(),
  version = NULL,
  verbose = TRUE,
  dfs_base = "https://onelake.dfs.fabric.microsoft.com",
  columns = NULL,
  limit = NULL,
  result = c("tibble", "arrow_stream")
) {
  # 1 Resolve and validate the Delta table ---------------------------------------------------------

  # Normalize discovery records, names, version, projection, and result type
  # before initializing Python or acquiring a storage token

  dfs_base_supplied <- !missing(dfs_base)
  result <- rlang::arg_match(result, .fabric_delta_result_types)
  resolved <- fabric_delta_resolve_public_target(
    table_path = table_path,
    workspace_name = workspace_name,
    lakehouse_name = lakehouse_name,
    schema = schema,
    dfs_base = if (dfs_base_supplied) dfs_base else NULL,
    item_type = item_type
  )
  fabric_require_explicit_custom_token(
    resolved$target$dfs_base,
    token,
    "dfs_base",
    allowed_hosts = .fabric_audience_hosts$storage
  )
  version <- fabric_delta_validate_whole_number(
    version,
    "version",
    allow_null = TRUE
  )
  limit <- fabric_delta_validate_whole_number(
    limit,
    "limit",
    allow_null = TRUE
  )
  fabric_delta_validate_columns(columns)

  if (!is.logical(verbose) || length(verbose) != 1L || is.na(verbose)) {
    .fabric_abort("verbose must be TRUE or FALSE")
  }

  # 2 Prepare authenticated table access -----------------------------------------------------------

  # Prepare authenticated table access once for reuse in the remaining work

  if (is.null(token)) {
    inform(verbose, "Authenticating with {.pkg AzureAuth} (MSAL v2)")
  }
  credential <- fabric_credential(
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args
  )
  table_uri <- fabric_delta_target_uri(resolved$target)
  storage_endpoint <- fabric_delta_storage_endpoint(resolved$target)
  inform(verbose, "Opening Delta table {.path {resolved$table_dir}}")

  # 3 Read with one authentication retry -----------------------------------------------------------

  # A refreshable token gets one forced refresh when the runtime reports an
  # authentication-shaped failure; other failures are never repeated blindly

  last_bearer_token <- NULL
  # Read once with an optional forced token refresh; returns value and token
  read_once <- function(force_refresh = FALSE) {
    bearer_token <- fabric_get_token(
      credential,
      .fabric_audience$storage,
      force_refresh = force_refresh
    )
    last_bearer_token <<- bearer_token
    value <- fabric_delta_read_uri(
      table_uri = table_uri,
      bearer_token = bearer_token,
      storage_endpoint = storage_endpoint,
      version = version,
      columns = columns,
      limit = limit,
      result = result
    )

    if (identical(result, "arrow_stream")) {
      value <- fabric_delta_spool_stream(value)
    }
    list(value = value, token = bearer_token)
  }

  attempt <- tryCatch(read_once(), error = identity)
  if (
    inherits(attempt, "error") &&
      isTRUE(credential$refreshable) &&
      fabric_delta_is_authentication_error(attempt)
  ) {
    inform(verbose, "Refreshing the OneLake token and retrying")
    attempt <- tryCatch(read_once(force_refresh = TRUE), error = identity)
  }

  if (inherits(attempt, "error")) {
    fabric_delta_abort_python(
      attempt,
      bearer_token = last_bearer_token
    )
  }

  # 4 Report and return the result -----------------------------------------------------------------

  # Turn the final state into clear output for the caller

  if (identical(result, "tibble")) {
    inform(
      verbose,
      "Loaded {nrow(attempt$value)} row{?s}",
      type = "success"
    )
  } else {
    inform(verbose, "Staged a disk-backed Arrow stream", type = "success")
  }
  attempt$value
}

#' Inspect the optional Python Delta runtime
#'
#' Shows whether the optional Python tools used for direct Delta reads are ready
#' By default this does not start Python. Set `initialize = TRUE` to prepare the
#' environment and report installed versions; packages may be downloaded the
#' first time
#'
#' @param initialize Whether to initialize Python
#' @return A list describing initialization state, requirements, the selected
#'   interpreter, module availability, and installed package versions when
#'   initialized
#' @examples
#' # Inspect requirements without starting Python or downloading anything
#' config <- fabric_delta_config()
#' config[c("initialized", "requirements", "available")]
#' @export
fabric_delta_config <- function(initialize = FALSE) {
  # 1 Discover the Python runtime ------------------------------------------------------------------

  # Avoid initialization by default so an inspection call never downloads or
  # starts Python unexpectedly

  if (
    !is.logical(initialize) ||
      length(initialize) != 1L ||
      is.na(initialize)
  ) {
    .fabric_abort("initialize must be TRUE or FALSE")
  }

  requirements <- reticulate::py_require()
  initialized <- reticulate::py_available(initialize = FALSE)
  discovered <- if (isTRUE(initialize)) {
    tryCatch(reticulate::py_config(), error = identity)
  } else if (initialized) {
    reticulate::py_config()
  } else {
    NULL
  }

  if (inherits(discovered, "error")) {
    fabric_delta_abort_python(discovered)
  }
  initialized <- reticulate::py_available(initialize = FALSE)

  # 2 Inspect required modules ---------------------------------------------------------------------

  # Inspect required modules before selecting the appropriate execution path

  available <- c(deltalake = NA, nanoarrow = NA)
  versions <- NULL
  if (initialized) {
    available <- c(
      deltalake = reticulate::py_module_available("deltalake"),
      nanoarrow = reticulate::py_module_available("nanoarrow")
    )

    if (all(available)) {
      versions <- list(
        deltalake = reticulate::py_to_r(
          .delta_python$deltalake$`__version__`
        ),
        nanoarrow = reticulate::py_to_r(
          .delta_python$nanoarrow$`__version__`
        )
      )
    }
  }

  # 3 Return runtime configuration -----------------------------------------------------------------

  # Return runtime configuration in the stable form expected by the caller

  list(
    initialized = initialized,
    python = if (is.null(discovered)) NULL else discovered$python,
    python_version = if (is.null(discovered)) {
      NULL
    } else {
      as.character(discovered$version)
    },
    requirements = list(
      python_version = requirements$python_version,
      packages = requirements$packages
    ),
    available = available,
    versions = versions
  )
}

#' Resolve and validate the public Fabric table arguments
#' @keywords internal
#' @noRd
# Uses the public table-location fields; returns a normalized OneLake target
fabric_delta_resolve_public_target <- function(
  table_path,
  workspace_name,
  lakehouse_name,
  schema,
  dfs_base,
  item_type = NULL
) {
  # 1 Resolve discovery records and item type ------------------------------------------------------

  # Records may provide IDs, default schema, item type, and private DFS endpoint

  workspace_target <- workspace_name
  workspace_record <- fabric_as_record(workspace_name)
  if (!is.null(workspace_record)) {
    workspace_name <- fabric_record_value(
      workspace_record,
      "id",
      "workspaceId"
    )
  }

  lakehouse_target <- lakehouse_name
  requested_item_type <- NULL

  # An explicit type takes priority when it agrees with all other clues
  if (!is.null(item_type)) {
    fabric_delta_validate_non_empty(item_type, "item_type")
    requested_item_type <- switch(
      tolower(item_type),
      lakehouse = "Lakehouse",
      warehouse = "Warehouse",
      mirroreddatabase = "MirroredDatabase",
      .fabric_abort(
        'item_type must be "Lakehouse", "Warehouse", or "MirroredDatabase"'
      )
    )
  }

  # Copied OneLake names may carry their type in a familiar suffix
  suffix_type <- if (
    is.character(lakehouse_name) &&
      length(lakehouse_name) == 1L &&
      !is.na(lakehouse_name)
  ) {
    if (grepl("\\.warehouse$", lakehouse_name, ignore.case = TRUE)) {
      "Warehouse"
    } else if (
      grepl("\\.mirroreddatabase$", lakehouse_name, ignore.case = TRUE)
    ) {
      "MirroredDatabase"
    } else if (grepl("\\.lakehouse$", lakehouse_name, ignore.case = TRUE)) {
      "Lakehouse"
    } else {
      NULL
    }
  } else {
    NULL
  }

  if (
    !is.null(requested_item_type) &&
      !is.null(suffix_type) &&
      !identical(requested_item_type, suffix_type)
  ) {
    .fabric_abort(
      paste0(
        "item_type conflicts with the ",
        ".Lakehouse/.Warehouse/.MirroredDatabase item suffix"
      )
    )
  }

  item_type <- requested_item_type %||% suffix_type %||% "Lakehouse"

  # Discovery records provide a trusted item ID and optional default schema
  lakehouse_record <- fabric_as_record(lakehouse_name)
  if (!is.null(lakehouse_record)) {
    record_type <- tolower(
      fabric_record_value(lakehouse_record, "type") %||% ""
    )

    if (!record_type %in% c("lakehouse", "warehouse", "mirroreddatabase")) {
      .fabric_abort(
        paste0(
          "lakehouse_name discovery record must be a Lakehouse, Warehouse, ",
          "or MirroredDatabase item"
        )
      )
    }

    record_item_type <- switch(
      record_type,
      warehouse = "Warehouse",
      mirroreddatabase = "MirroredDatabase",
      "Lakehouse"
    )

    if (
      !is.null(requested_item_type) &&
        !identical(requested_item_type, record_item_type)
    ) {
      .fabric_abort("item_type conflicts with the item discovery record")
    }

    item_type <- record_item_type
    lakehouse_name <- fabric_record_value(lakehouse_record, "id")
    schema <- schema %||%
      fabric_record_value(
        lakehouse_record,
        "default_schema",
        "defaultSchema"
      )
  }

  # Warehouses and mirrored databases use dbo without a discovered default
  if (
    is.null(schema) &&
      item_type %in% c("Warehouse", "MirroredDatabase")
  ) {
    schema <- "dbo"
  }

  # 2 Validate table path fields -------------------------------------------------------------------

  # Check table path fields now so later code can rely on safe input

  fabric_delta_validate_non_empty(table_path, "table_path")
  if (grepl("[/\\\\]", table_path)) {
    .fabric_abort(
      "table_path must be one table name; supply the schema with schema"
    )
  }
  fabric_delta_validate_non_empty(
    workspace_name,
    "workspace_name",
    suffix = " or record"
  )
  fabric_delta_validate_non_empty(
    lakehouse_name,
    "lakehouse_name",
    suffix = " or record"
  )

  if (identical(schema, "")) {
    schema <- NULL
  }
  if (!is.null(schema)) {
    fabric_delta_validate_non_empty(schema, "schema")
    if (grepl("[/\\\\]", schema)) {
      .fabric_abort("schema must be exactly one URI path segment")
    }
  }

  # 3 Build and return the OneLake target ----------------------------------------------------------

  # Build and return the OneLake target from the validated values required by the next step

  table_name <- table_path
  table_dir <- if (is.null(schema)) {
    paste("Tables", table_name, sep = "/")
  } else {
    paste("Tables", schema, table_name, sep = "/")
  }
  target <- onelake_resolve_target(
    workspace_target,
    lakehouse_target,
    path = table_dir,
    item_type = item_type,
    dfs_base = dfs_base
  )
  list(target = target, table_dir = table_dir, item_type = item_type)
}

#' Validate one public string argument
#' @keywords internal
#' @noRd
# Uses `value`, its friendly `name`, and error `suffix`; returns invisibly
fabric_delta_validate_non_empty <- function(value, name, suffix = "") {
  if (
    !is.character(value) ||
      length(value) != 1L ||
      is.na(value) ||
      !nzchar(value)
  ) {
    .fabric_abort(paste0(
      name,
      " must be one non-empty string",
      suffix
    ))
  }
  invisible(value)
}

#' Validate an exactly representable non-negative whole number
#' @keywords internal
#' @noRd
# Uses optional numeric `value` and bounds; returns a validated number or `NULL`
fabric_delta_validate_whole_number <- function(
  value,
  name,
  allow_null = FALSE
) {
  if (is.null(value) && allow_null) {
    return(NULL)
  }

  if (
    !is.numeric(value) ||
      length(value) != 1L ||
      is.na(value) ||
      !is.finite(value) ||
      value < 0 ||
      value != floor(value) ||
      value > .fabric_delta_max_exact_version
  ) {
    .fabric_abort(paste0(
      name,
      " must be ",
      if (allow_null) "NULL or " else "",
      "one exactly representable non-negative integer no greater than 2^53"
    ))
  }
  as.numeric(value)
}

#' Validate a logical Delta projection
#' @keywords internal
#' @noRd
# Uses optional `columns`; returns invisibly after projection validation
fabric_delta_validate_columns <- function(columns) {
  if (
    !is.null(columns) &&
      (!is.character(columns) ||
        !length(columns) ||
        anyNA(columns) ||
        !all(nzchar(columns)) ||
        anyDuplicated(columns))
  ) {
    .fabric_abort(
      "columns must be NULL or one or more unique, non-empty strings"
    )
  }
  invisible(columns)
}

#' Convert a resolved OneLake target into a delta-rs ABFSS URI
#' @keywords internal
#' @noRd
# Uses a resolved OneLake `target`; returns the ABFSS URI passed to delta-rs
fabric_delta_target_uri <- function(target) {
  host <- httr2::url_parse(target$dfs_base)$hostname
  if (is.null(host) || !nzchar(host)) {
    .fabric_abort("The resolved OneLake target has no DFS host")
  }

  # delta-rs 1.6.x cannot parse Fabric's workspace-private ABFSS authority.
  # Keep the logical URI parser-compatible and pass the private host as a
  # separate storage endpoint so requests still use the discovered route.
  if (!is.null(onelake_workspace_host_guid(host))) {
    host <- "onelake.dfs.fabric.microsoft.com"
  }

  if (
    !fabric_is_guid(target$workspace) &&
      grepl("[^A-Za-z0-9_-]", target$workspace)
  ) {
    .fabric_abort(
      c(
        "The workspace display name is not valid in an ABFSS authority.",
        "x" = paste("Workspace:", target$workspace),
        "i" = paste0(
          "Supply discovery records or paired workspace and item GUIDs when ",
          "the workspace name contains spaces or other special characters."
        )
      ),
      class = c("fabric_delta_invalid_target", "fabric_delta_error")
    )
  }
  prefix <- paste0(
    "abfss://",
    utils::URLencode(target$workspace, reserved = TRUE),
    "@",
    host,
    "/",
    utils::URLencode(target$item, reserved = TRUE)
  )

  if (!nzchar(target$path)) {
    return(prefix)
  }
  paste0(prefix, "/", onelake_encode_path(target$path))
}

#' Return a workspace-private Blob endpoint for delta-rs
#' @keywords internal
#' @noRd
# Uses a resolved OneLake `target`; returns a private endpoint or `NULL`
fabric_delta_storage_endpoint <- function(target) {
  host <- httr2::url_parse(target$dfs_base)$hostname
  if (is.null(onelake_workspace_host_guid(host))) {
    return(NULL)
  }

  blob_host <- sub(
    "\\.dfs\\.fabric\\.microsoft\\.com$",
    ".blob.fabric.microsoft.com",
    tolower(host)
  )
  paste0("https://", blob_host)
}

#' Read a Delta URI through the Python runtime
#' @keywords internal
#' @noRd
# Uses a Delta URI, token, query options, and result type; returns table data
fabric_delta_read_uri <- function(
  table_uri,
  bearer_token = NULL,
  storage_endpoint = NULL,
  version = NULL,
  columns = NULL,
  limit = NULL,
  result = "tibble"
) {
  reader <- fabric_delta_python_reader(
    table_uri = table_uri,
    bearer_token = bearer_token,
    storage_endpoint = storage_endpoint,
    version = version,
    columns = columns,
    limit = limit
  )

  if (identical(result, "arrow_stream")) {
    return(fabric_delta_reader_stream(reader, collect = FALSE))
  }
  fabric_delta_collect_reader(reader)
}

#' Open a delta-rs Arrow query
#' @keywords internal
#' @noRd
# Uses a Delta URI, token, and query settings; returns a Python Arrow reader
fabric_delta_python_reader <- function(
  table_uri,
  bearer_token = NULL,
  storage_endpoint = NULL,
  version = NULL,
  columns = NULL,
  limit = NULL
) {
  storage_options <- NULL
  if (!is.null(bearer_token) || !is.null(storage_endpoint)) {
    option_values <- list(use_fabric_endpoint = "true")
    if (!is.null(bearer_token)) {
      option_values$bearer_token <- bearer_token
    }
    if (!is.null(storage_endpoint)) {
      option_values$azure_storage_endpoint <- storage_endpoint
    }
    storage_options <- do.call(
      reticulate::dict,
      c(option_values, list(convert = FALSE))
    )
  }
  args <- list(table_uri = table_uri)
  if (!is.null(storage_options)) {
    args$storage_options <- storage_options
  }

  if (!is.null(version)) {
    args$version <- .delta_python$builtins$int(
      fabric_delta_whole_number_text(version)
    )
  }

  table <- do.call(.delta_python$deltalake$DeltaTable, args)
  features <- fabric_delta_check_protocol(
    table$protocol(),
    schema = table$schema(),
    columns = columns
  )
  builder <- .delta_python$deltalake$QueryBuilder()
  fabric_delta_configure_query(builder, features)
  builder$register("fabric_delta_table", table)
  reader <- builder$execute(fabric_delta_query(columns, limit))
  attr(reader, "fabric_delta_table") <- table
  attr(reader, "fabric_delta_query_builder") <- builder
  reader
}

#' Reject known unsupported Delta reader features before planning a query
#' @keywords internal
#' @noRd
# Uses a delta-rs `protocol`; returns supported reader features or raises
fabric_delta_check_protocol <- function(
  protocol,
  schema = NULL,
  columns = NULL
) {
  features <- reticulate::py_to_r(protocol$reader_features)
  features <- if (is.null(features)) character() else as.character(features)
  normalized <- gsub("[^a-z0-9]", "", tolower(features))
  unsupported <- unname(.fabric_delta_unsupported_reader_features[
    intersect(normalized, names(.fabric_delta_unsupported_reader_features))
  ])
  if (length(unsupported)) {
    .fabric_abort(
      c(
        paste0(
          "The selected Delta table requires unsupported reader feature",
          if (length(unsupported) == 1L) " " else "s: ",
          paste(unsupported, collapse = ", "),
          "."
        ),
        "i" = paste0(
          "The pinned deltalake runtime cannot safely read this protocol. ",
          "Use Fabric SQL or PySpark through Livy instead."
        )
      ),
      class = c(
        "fabric_delta_unsupported_feature_error",
        "fabric_delta_unsupported_error",
        "fabric_delta_error"
      ),
      delta_features = unsupported
    )
  }

  variant_features <- unname(.fabric_delta_variant_reader_features[
    intersect(normalized, names(.fabric_delta_variant_reader_features))
  ])
  if (length(variant_features) && !is.null(schema)) {
    variant_columns <- fabric_delta_variant_columns(schema)
    selected <- if (is.null(columns)) {
      variant_columns
    } else {
      intersect(columns, variant_columns)
    }
    if (length(selected)) {
      .fabric_abort(
        c(
          paste0(
            "The selected Delta projection includes unsupported Variant ",
            "column",
            if (length(selected) == 1L) " " else "s: ",
            paste(selected, collapse = ", "),
            "."
          ),
          "i" = paste0(
            "The pinned deltalake runtime returns physical Variant binary ",
            "storage instead of decoded logical values. Select only ",
            "non-Variant columns, or use Fabric SQL or PySpark through Livy."
          )
        ),
        class = c(
          "fabric_delta_unsupported_feature_error",
          "fabric_delta_unsupported_error",
          "fabric_delta_error"
        ),
        delta_features = variant_features,
        delta_columns = selected
      )
    }
  }
  invisible(features)
}

# Return top-level fields whose Delta type contains Variant. The Python schema
# supplies its canonical Delta JSON, so no physical Parquet fields are exposed.
fabric_delta_variant_columns <- function(schema) {
  schema_json <- reticulate::py_to_r(schema$to_json())
  decoded <- try(
    jsonlite::fromJSON(schema_json, simplifyVector = FALSE),
    silent = TRUE
  )
  fields <- if (inherits(decoded, "try-error")) NULL else decoded$fields
  if (!is.list(fields)) {
    .fabric_abort(
      "The Delta reader returned a malformed table schema",
      class = c("fabric_delta_protocol_error", "fabric_delta_error")
    )
  }
  vapply(
    Filter(
      function(field) fabric_delta_type_contains_variant(field$type),
      fields
    ),
    `[[`,
    character(1),
    "name"
  )
}

# Check one decoded Delta JSON type recursively for logical Variant values.
fabric_delta_type_contains_variant <- function(type) {
  if (is.character(type)) {
    return(identical(tolower(type), "variant"))
  }
  if (!is.list(type)) {
    return(FALSE)
  }
  switch(
    tolower(type$type %||% ""),
    struct = any(vapply(
      type$fields %||% list(),
      function(field) fabric_delta_type_contains_variant(field$type),
      logical(1)
    )),
    array = fabric_delta_type_contains_variant(type$elementType),
    map = fabric_delta_type_contains_variant(type$keyType) ||
      fabric_delta_type_contains_variant(type$valueType),
    FALSE
  )
}

#' Configure DataFusion for protocol-sensitive Delta scans
#' @keywords internal
#' @noRd
# Uses a DataFusion `builder` and Delta `features`; returns configured builder
fabric_delta_configure_query <- function(builder, features) {
  normalized <- gsub("[^a-z0-9]", "", tolower(features))
  if (!"deletionvectors" %in% normalized) {
    return(invisible(builder))
  }

  # DeltaScan consumes each deletion-vector mask in source row order. A
  # round-robin scan can deliver batches from one file concurrently and apply
  # the right mask positions to the wrong rows. Keep deletion-vector scans in
  # one execution partition until delta-rs associates masks with row offsets
  setting <- builder$execute(
    "SET datafusion.execution.target_partitions = '1'"
  )
  setting$read_all()
  invisible(builder)
}

#' Render one exact R whole number for Python
#' @keywords internal
#' @noRd
# Uses exact whole-number `value`; returns non-scientific text for Python
fabric_delta_whole_number_text <- function(value) {
  formatC(value, format = "f", digits = 0L)
}

#' Build a safe DataFusion projection query
#' @keywords internal
#' @noRd
# Uses optional `columns` and `limit`; returns a safe DataFusion SELECT string
fabric_delta_query <- function(columns = NULL, limit = NULL) {
  projection <- if (is.null(columns)) {
    "*"
  } else {
    paste(
      vapply(
        columns,
        fabric_delta_quote_identifier,
        character(1),
        USE.NAMES = FALSE
      ),
      collapse = ", "
    )
  }
  query <- paste0('SELECT ', projection, ' FROM "fabric_delta_table"')
  if (!is.null(limit)) {
    query <- paste(query, "LIMIT", fabric_delta_whole_number_text(limit))
  }
  query
}

#' Quote one DataFusion identifier
#' @keywords internal
#' @noRd
# Uses one identifier `value`; returns its safely quoted DataFusion spelling
fabric_delta_quote_identifier <- function(value) {
  paste0('"', gsub('"', '""', value, fixed = TRUE), '"')
}

#' Convert a Python Arrow reader into an R 'nanoarrow' stream
#' @keywords internal
#' @noRd
# Uses a Python Arrow `reader`; returns an R nanoarrow stream for result handling
fabric_delta_reader_stream <- function(reader, collect = FALSE) {
  source_schema <- nanoarrow::as_nanoarrow_schema(reader$schema)
  target_schema <- fabric_delta_normalize_schema(
    source_schema,
    collect = collect
  )
  stream <- nanoarrow::as_nanoarrow_array_stream(
    reader,
    schema = target_schema
  )
  attr(stream, "fabric_delta_python_owner") <- reader
  attr(stream, "fabric_delta_source_schema") <- source_schema
  table <- attr(reader, "fabric_delta_table", exact = TRUE)
  if (!is.null(table)) {
    attr(stream, "fabric_delta_snapshot_version") <- as.double(
      reticulate::py_to_r(table$version())
    )
  }
  stream
}

#' Stage an authenticated stream in a local Arrow IPC file
#' @keywords internal
#' @noRd
# Uses authenticated Arrow `stream`; returns a disk-backed stream safe after call
fabric_delta_spool_stream <- function(stream) {
  # 1 Write an authenticated local snapshot --------------------------------------------------------

  # Materialize while the token-backed Python reader is alive, but keep the R
  # result lazy and disk-backed for the caller

  snapshot_version <- attr(
    stream,
    "fabric_delta_snapshot_version",
    exact = TRUE
  )
  path <- tempfile("fabricQueryR-delta-", fileext = ".arrows")
  complete <- FALSE
  on.exit(
    {
      if (!complete) {
        unlink(path)
      }
    },
    add = TRUE
  )

  nanoarrow::write_nanoarrow(stream, path)

  # 2 Open a lazy local stream ---------------------------------------------------------------------

  # Open a lazy local stream only after connection settings are ready

  connection <- file(path, open = "rb")
  local_stream <- tryCatch(
    nanoarrow::read_nanoarrow(connection, lazy = TRUE),
    error = function(error) {
      close(connection)
      .fabric_abort(
        "Could not open the staged Arrow stream",
        parent = error
      )
    }
  )
  cleanup <- local({
    spool_connection <- connection
    spool_path <- path
    function() {
      .fabric_delta_release_spool(spool_connection, spool_path)
    }
  })
  local_stream <- tryCatch(
    nanoarrow::array_stream_set_finalizer(local_stream, cleanup),
    error = function(error) {
      cleanup()
      .fabric_abort(
        "Could not attach cleanup to the staged Arrow stream",
        parent = error
      )
    }
  )

  # 3 Attach cleanup metadata and return -----------------------------------------------------------

  # Attach cleanup metadata and return before returning the completed result

  attr(local_stream, "fabric_delta_spool_path") <- path
  if (!is.null(snapshot_version)) {
    attr(local_stream, "fabric_delta_snapshot_version") <- snapshot_version
  }
  complete <- TRUE
  local_stream
}

# Close a staged Arrow connection before deleting its file. Returns whether the
# file is absent after a small bounded retry for Windows handle release
.fabric_delta_release_spool <- function(connection, path) {
  open <- try(isOpen(connection), silent = TRUE)
  if (isTRUE(open)) {
    try(close(connection), silent = TRUE)
  }

  for (attempt in seq_len(3L)) {
    if (!file.exists(path) || unlink(path, force = TRUE) == 0L) {
      return(invisible(TRUE))
    }
    if (attempt < 3L) {
      Sys.sleep(0.01)
    }
  }
  invisible(!file.exists(path))
}

#' Detect timestamp-without-time-zone Arrow formats
#' @keywords internal
#' @noRd
# Uses an Arrow `format`; returns whether it is a timestamp without time zone
fabric_delta_is_timestamp_ntz_format <- function(format) {
  grepl("^ts[smnu]:$", format)
}

#' Normalize Arrow types emitted by DataFusion
#' @keywords internal
#' @noRd
# Uses an Arrow `schema`; returns a normalized schema for streaming or collect
fabric_delta_normalize_schema <- function(schema, collect = FALSE) {
  children <- lapply(
    schema$children,
    fabric_delta_normalize_schema,
    collect = collect
  )
  dictionary <- schema$dictionary
  if (!is.null(dictionary)) {
    dictionary <- fabric_delta_normalize_schema(
      dictionary,
      collect = collect
    )
  }

  format <- schema$format
  format <- switch(
    format,
    "vu" = "u",
    "vz" = "z",
    "+vl" = "+l",
    "+vL" = "+L",
    format
  )

  if (startsWith(format, "d:")) {
    format <- "u"
  }

  if (isTRUE(collect)) {
    if (identical(format, "i")) {
      format <- "g"
    } else if (format %in% c("l", "L")) {
      format <- "u"
    }
  }

  if (isTRUE(collect) && fabric_delta_is_timestamp_ntz_format(format)) {
    format <- "u"
  }

  nanoarrow::nanoarrow_schema_modify(
    schema,
    list(
      format = format,
      children = children,
      dictionary = dictionary
    )
  )
}

#' Reject columns that need a package-specific recursive R representation
#' @keywords internal
#' @noRd
# Uses an Arrow `schema`; returns invisibly or rejects unsupported nested collect
fabric_delta_validate_collect_schema <- function(schema) {
  unsupported <- vapply(
    schema$children,
    function(child) {
      if (!is.null(child$dictionary)) {
        child <- child$dictionary
      }
      startsWith(child$format, "+") ||
        identical(
          child$metadata[["ARROW:extension:name"]] %||% "",
          "arrow.parquet.variant"
        )
    },
    logical(1)
  )

  if (!any(unsupported)) {
    return(invisible(schema))
  }

  column_names <- names(schema$children)[unsupported]
  .fabric_abort(
    c(
      "Nested and extension Delta columns cannot be collected to a tibble.",
      "x" = paste(
        "Unsupported column(s):",
        paste(column_names, collapse = ", ")
      ),
      "i" = paste0(
        "Select scalar columns or use result = \"arrow_stream\" to preserve ",
        "the Arrow representation."
      )
    ),
    class = c(
      "fabric_delta_nested_collection_error",
      "fabric_delta_unsupported_error",
      "fabric_delta_error"
    ),
    delta_columns = column_names
  )
}

#' Collect common Delta scalar types through 'nanoarrow'
#' @keywords internal
#' @noRd
# Uses a Python Arrow `reader`; returns a tibble for supported scalar columns
fabric_delta_collect_reader <- function(reader) {
  stream <- fabric_delta_reader_stream(reader, collect = TRUE)
  source_schema <- attr(
    stream,
    "fabric_delta_source_schema",
    exact = TRUE
  )
  fabric_delta_validate_collect_schema(source_schema)
  target_schema <- nanoarrow::infer_nanoarrow_schema(stream)
  value <- nanoarrow::convert_array_stream(
    stream,
    to = nanoarrow::infer_nanoarrow_ptype(target_schema)
  )
  tibble::as_tibble(value)
}

#' Detect an authentication-shaped delta-rs error
#' @keywords internal
#' @noRd
# Uses one runtime `error`; returns whether refreshing credentials may help
fabric_delta_is_authentication_error <- function(error) {
  grepl(
    paste0(
      "(?:401|unauthori[sz]ed|authentication|",
      "token[^[:alnum:]]+(?:expired|invalid))"
    ),
    conditionMessage(error),
    ignore.case = TRUE,
    perl = TRUE
  )
}

#' Extract required Delta reader features from a delta-rs protocol error
#' @keywords internal
#' @noRd
# Uses runtime `message`; returns named unsupported Delta reader features
fabric_delta_unsupported_features <- function(message) {
  match <- regexec(
    "unsupported table features required:[[:space:]]*\\[([^]]+)\\]",
    message,
    ignore.case = TRUE,
    perl = TRUE
  )
  captured <- regmatches(message, match)[[1L]]
  if (length(captured) < 2L) {
    return(character())
  }
  features <- trimws(strsplit(captured[[2L]], ",", fixed = TRUE)[[1L]])
  unique(features[nzchar(features)])
}

#' Redact and translate a Python runtime error
#' @keywords internal
#' @noRd
# Uses `error` and optional token; raises a redacted, typed public condition
fabric_delta_abort_python <- function(error, bearer_token = NULL) {
  # 1 Redact and classify the runtime error --------------------------------------------------------

  # Tokens may appear in lower-level Python messages, so redact explicit and
  # token-shaped text before attaching it to a public condition

  if (inherits(error, "fabric_delta_error")) {
    rlang::cnd_signal(error)
  }

  # Remove both the exact credential and any token-shaped text
  message <- conditionMessage(error)
  if (!is.null(bearer_token) && nzchar(bearer_token)) {
    message <- gsub(bearer_token, "<redacted>", message, fixed = TRUE)
  }
  message <- gsub(
    "eyJ[A-Za-z0-9_-]+\\.[A-Za-z0-9_-]+\\.[A-Za-z0-9_-]+",
    "<redacted>",
    message,
    perl = TRUE
  )
  message <- .httr2_redact(message)

  # Classify the message so callers can handle common failure families
  environment_error <- grepl(
    "No module named ['\"](?:deltalake|nanoarrow)['\"]|ModuleNotFoundError",
    message,
    ignore.case = TRUE,
    perl = TRUE
  )
  unsupported_error <- grepl(
    "DeltaProtocolError|not supported|unsupported",
    paste(c(class(error), message), collapse = " "),
    ignore.case = TRUE
  )
  unsupported_features <- fabric_delta_unsupported_features(message)
  authorization_error <- grepl(
    "(?:403|forbidden|authorization|permission denied|access denied)",
    message,
    ignore.case = TRUE,
    perl = TRUE
  )
  authentication_error <- !authorization_error &&
    fabric_delta_is_authentication_error(simpleError(message))

  # Add the most specific classes before the shared Python error class
  classes <- "fabric_delta_python_error"
  if (environment_error) {
    classes <- c("fabric_delta_environment_error", classes)
  }

  if (unsupported_error) {
    classes <- c("fabric_delta_unsupported_error", classes)
  }

  if (length(unsupported_features)) {
    classes <- c("fabric_delta_unsupported_feature_error", classes)
  }

  if (authentication_error) {
    classes <- c(
      "fabric_delta_authentication_error",
      "fabric_delta_access_error",
      classes
    )
  }

  if (authorization_error) {
    classes <- c(
      "fabric_delta_authorization_error",
      "fabric_delta_access_error",
      classes
    )
  }

  # 2 Build actionable guidance --------------------------------------------------------------------

  # Build actionable guidance from the validated values required by the next step

  bullets <- c(
    "Unable to read the Delta table through Python delta-rs.",
    "x" = message
  )

  # Each known failure family gets guidance that the caller can act on
  if (environment_error) {
    bullets <- c(
      bullets,
      "i" = paste0(
        "Install deltalake==1.6.2 and nanoarrow==0.8.0 in the Python ",
        "selected by reticulate, or unset RETICULATE_PYTHON to use a ",
        "managed environment."
      )
    )
  }

  if (authentication_error) {
    bullets <- c(
      bullets,
      "i" = paste0(
        "Acquire a current token for https://storage.azure.com/.default; ",
        "a Fabric API or Power BI token cannot authenticate to OneLake."
      )
    )
  }

  if (authorization_error) {
    bullets <- c(
      bullets,
      "i" = paste0(
        "Grant this identity access to the Fabric item and OneLake data. ",
        "Item Read alone does not authorize OneLake data access. ",
        "fabricQueryR is not an authorized OneLake security engine. Direct ",
        "file reads are blocked when the caller's effective access has RLS ",
        "or CLS restrictions; the reader never returns policy-filtered data."
      ),
      "i" = paste0(
        "Ask a Fabric administrator to verify the OneLake tenant setting ",
        "'Users can access data stored in OneLake with apps external to ",
        "Fabric' is enabled for this identity."
      )
    )
  }

  if (length(unsupported_features)) {
    bullets <- c(
      bullets,
      "i" = paste0(
        "The selected deltalake runtime cannot read the required Delta ",
        "feature",
        if (length(unsupported_features) == 1L) "" else "s",
        ": ",
        paste(unsupported_features, collapse = ", "),
        "."
      ),
      "i" = paste0(
        "Use a Fabric PySpark notebook for this table, or select a ",
        "deltalake runtime that supports every required reader feature."
      )
    )
  }

  # 3 Raise the typed Delta error ------------------------------------------------------------------

  # Turn the final state into clear output for the caller

  .fabric_abort(
    bullets,
    class = unique(classes),
    delta_features = unsupported_features
  )
}
