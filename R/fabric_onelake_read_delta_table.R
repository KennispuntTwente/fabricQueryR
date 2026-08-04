.fabric_delta_max_exact_version <- 2^53
.fabric_delta_result_types <- c("tibble", "arrow_stream")

#' Read a Delta table from OneLake
#'
#' Read a Delta table from Microsoft Fabric OneLake using the Python
#' [deltalake](https://pypi.org/project/deltalake/) package through the
#' `reticulate` R package. Return a tibble by default or a lazy Arrow stream for
#' batch processing. Column selection, row limits, and table version reads are
#' supported.
#'
#' @details
#' Most users only need to provide the table, workspace, and Lakehouse. These
#' can be names, IDs, or records returned by fabricQueryR's discovery functions.
#' If the Lakehouse uses schemas, provide the table's `schema` separately.
#'
#' By default, the function reads all rows and columns into memory. Use `columns`
#' to select only the fields you need and `limit` for a quick preview. A limit
#' does not guarantee which rows are returned. Use `version` to read an earlier
#' version of the table.
#'
#' For a large table, or one containing nested data, set
#' `result = "arrow_stream"` to process the result in batches. The returned
#' stream is lazy and can be read only once. The first Delta read may take a
#' little longer while the optional Python reader is set up.
#'
#' Direct reads require OneLake data access; item `Read` permission by itself is
#' not enough. The caller needs `ReadAll` or a suitable OneLake data-access role,
#' and the tenant setting for external OneLake apps must be enabled. Callers
#' restricted by row- or column-level security must use a supported Fabric
#' engine instead. See the
#' [Fabric permission model](https://learn.microsoft.com/en-us/fabric/security/permission-model)
#' and [OneLake tenant settings](https://learn.microsoft.com/en-us/fabric/admin/service-admin-portal-onelake).
#'
#' Some tables use advanced Delta features that the deltalake Python package does
#' not support. The function will detect these features and abort. Unsupported
#' features include Deletion Vectors, Type Widening, V2 Checkpoints, and Fabric Variant.
#' Use the SQL or Spark (Livy) functions to read these tables.
#'
#' @param table_path Table name without a schema. Use `schema` separately when
#'   needed.
#' @param workspace_name Workspace name, ID, or a record returned by
#'   [fabric_workspaces()].
#' @param lakehouse_name Lakehouse name, ID, or discovery record. Compatible
#'   Warehouse items are also accepted.
#' @param schema Schema containing the table, or `NULL`. Warehouses default to
#'   `"dbo"`.
#' @param item_type `"Lakehouse"`, `"Warehouse"`, or `NULL`. Usually inferred;
#'   specify it only when using an item name without a type suffix.
#' @param tenant_id Microsoft Entra tenant ID. Defaults to
#'   `FABRICQUERYR_TENANT_ID`.
#' @param client_id Microsoft Entra application/client ID. Defaults to
#'   `FABRICQUERYR_CLIENT_ID`, then the Azure CLI application ID.
#' @param token Optional access token or token-provider function. Most users can
#'   leave this as `NULL` and let fabricQueryR sign in.
#' @param auth_args Extra sign-in options passed to
#'   [AzureAuth::get_azure_token()].
#' @param version Specific table version to read, or `NULL` for the latest.
#' @param verbose Whether to show authentication and read progress.
#' @param dfs_base OneLake DFS endpoint. Most users can keep the default.
#' @param columns Column names to return, or `NULL` for all columns.
#' @param limit Maximum number of rows to return, or `NULL` for all rows.
#' @param result `"tibble"` (the default) or `"arrow_stream"` for batch
#'   processing.
#'
#' @return A tibble, or a lazy, single-use Arrow stream when
#'   `result = "arrow_stream"`.
#' @export
#'
#' @examples
#' \dontrun{
#' patients <- fabric_onelake_read_delta_table(
#'   table_path = "Patients",
#'   workspace_name = "PatientsWorkspace",
#'   lakehouse_name = "Clinical.Lakehouse"
#' )
#'
#' stream <- fabric_onelake_read_delta_table(
#'   table_path = "Patients",
#'   workspace_name = "PatientsWorkspace",
#'   lakehouse_name = "Clinical.Lakehouse",
#'   columns = c("PatientId", "Status"),
#'   result = "arrow_stream"
#' )
#' reader <- arrow::as_record_batch_reader(stream)
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
  result <- rlang::arg_match(result, .fabric_delta_result_types)
  resolved <- fabric_delta_resolve_public_target(
    table_path = table_path,
    workspace_name = workspace_name,
    lakehouse_name = lakehouse_name,
    schema = schema,
    dfs_base = dfs_base,
    item_type = item_type
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
    rlang::abort("verbose must be TRUE or FALSE")
  }

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
  inform(verbose, "Opening Delta table {.path {resolved$table_dir}}")

  last_bearer_token <- NULL
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
      version = version,
      columns = columns,
      limit = limit,
      result = result
    )
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

  if (identical(result, "tibble")) {
    inform(
      verbose,
      "Loaded {nrow(attempt$value)} row{?s}",
      type = "success"
    )
  } else {
    inform(verbose, "Opened a lazy Arrow stream", type = "success")
  }
  attempt$value
}

#' Inspect the optional Python Delta runtime
#'
#' Reports the Python requirements declared by fabricQueryR without starting
#' Python by default. Set `initialize = TRUE` to initialize the selected Python
#' environment and report installed runtime versions; this may create a managed
#' environment and download packages.
#'
#' @param initialize Whether to initialize Python.
#' @return A list describing initialization state, requirements, the selected
#'   interpreter, module availability, and installed package versions when
#'   initialized.
#' @export
fabric_delta_config <- function(initialize = FALSE) {
  if (
    !is.logical(initialize) ||
      length(initialize) != 1L ||
      is.na(initialize)
  ) {
    rlang::abort("initialize must be TRUE or FALSE")
  }

  requirements <- reticulate::py_require()
  initialized <- reticulate::py_available(initialize = FALSE)
  discovered <- if (isTRUE(initialize)) {
    tryCatch(reticulate::py_config(), error = identity)
  } else {
    reticulate::py_discover_config()
  }
  if (inherits(discovered, "error")) {
    fabric_delta_abort_python(discovered)
  }
  initialized <- reticulate::py_available(initialize = FALSE)

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
fabric_delta_resolve_public_target <- function(
  table_path,
  workspace_name,
  lakehouse_name,
  schema,
  dfs_base,
  item_type = NULL
) {
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
  if (!is.null(item_type)) {
    fabric_delta_validate_non_empty(item_type, "item_type")
    requested_item_type <- switch(
      tolower(item_type),
      lakehouse = "Lakehouse",
      warehouse = "Warehouse",
      rlang::abort('item_type must be "Lakehouse" or "Warehouse"')
    )
  }
  suffix_type <- if (
    is.character(lakehouse_name) &&
      length(lakehouse_name) == 1L &&
      !is.na(lakehouse_name)
  ) {
    if (grepl("\\.warehouse$", lakehouse_name, ignore.case = TRUE)) {
      "Warehouse"
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
    rlang::abort(
      "item_type conflicts with the .Lakehouse/.Warehouse item suffix"
    )
  }
  item_type <- requested_item_type %||% suffix_type %||% "Lakehouse"
  lakehouse_record <- fabric_as_record(lakehouse_name)
  if (!is.null(lakehouse_record)) {
    record_type <- tolower(
      fabric_record_value(lakehouse_record, "type") %||% ""
    )
    if (!record_type %in% c("lakehouse", "warehouse")) {
      rlang::abort(
        "lakehouse_name discovery record must be a Lakehouse or Warehouse item"
      )
    }
    record_item_type <- if (identical(record_type, "warehouse")) {
      "Warehouse"
    } else {
      "Lakehouse"
    }
    if (
      !is.null(requested_item_type) &&
        !identical(requested_item_type, record_item_type)
    ) {
      rlang::abort("item_type conflicts with the item discovery record")
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

  if (is.null(schema) && identical(item_type, "Warehouse")) {
    schema <- "dbo"
  }

  fabric_delta_validate_non_empty(table_path, "table_path")
  if (grepl("[/\\\\]", table_path)) {
    rlang::abort(
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
  if (!is.null(schema)) {
    fabric_delta_validate_non_empty(schema, "schema")
  }

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
fabric_delta_validate_non_empty <- function(value, name, suffix = "") {
  if (
    !is.character(value) ||
      length(value) != 1L ||
      is.na(value) ||
      !nzchar(value)
  ) {
    rlang::abort(paste0(
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
    rlang::abort(paste0(
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
fabric_delta_validate_columns <- function(columns) {
  if (
    !is.null(columns) &&
      (!is.character(columns) ||
        !length(columns) ||
        anyNA(columns) ||
        !all(nzchar(columns)) ||
        anyDuplicated(columns))
  ) {
    rlang::abort(
      "columns must be NULL or one or more unique, non-empty strings"
    )
  }
  invisible(columns)
}

#' Convert a resolved OneLake target into a delta-rs ABFSS URI
#' @keywords internal
#' @noRd
fabric_delta_target_uri <- function(target) {
  host <- httr2::url_parse(target$dfs_base)$hostname
  if (is.null(host) || !nzchar(host)) {
    rlang::abort("The resolved OneLake target has no DFS host")
  }
  if (
    !fabric_is_guid(target$workspace) &&
      grepl("[^A-Za-z0-9_-]", target$workspace)
  ) {
    rlang::abort(
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

#' Read a Delta URI through the Python runtime
#' @keywords internal
#' @noRd
fabric_delta_read_uri <- function(
  table_uri,
  bearer_token = NULL,
  version = NULL,
  columns = NULL,
  limit = NULL,
  result = "tibble"
) {
  reader <- fabric_delta_python_reader(
    table_uri = table_uri,
    bearer_token = bearer_token,
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
fabric_delta_python_reader <- function(
  table_uri,
  bearer_token = NULL,
  version = NULL,
  columns = NULL,
  limit = NULL
) {
  storage_options <- NULL
  if (!is.null(bearer_token)) {
    storage_options <- reticulate::dict(
      bearer_token = bearer_token,
      use_fabric_endpoint = "true",
      convert = FALSE
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
  features <- reticulate::py_to_r(table$protocol()$reader_features)
  features <- if (is.null(features)) character() else as.character(features)
  if (any(tolower(features) == "deletionvectors")) {
    rlang::abort(
      c(
        "The selected Delta table requires deletion-vector support.",
        "i" = paste0(
          "The deltalake runtime does not support deletion-vector reads ",
          "through its stable table API. Use Fabric SQL or PySpark instead."
        )
      ),
      class = c(
        "fabric_delta_unsupported_feature_error",
        "fabric_delta_unsupported_error",
        "fabric_delta_error"
      ),
      delta_features = "DeletionVectors"
    )
  }
  builder <- .delta_python$deltalake$QueryBuilder()
  builder$register("fabric_delta_table", table)
  reader <- builder$execute(fabric_delta_query(columns, limit))
  attr(reader, "fabric_delta_table") <- table
  attr(reader, "fabric_delta_query_builder") <- builder
  reader
}

#' Render one exact R whole number for Python
#' @keywords internal
#' @noRd
fabric_delta_whole_number_text <- function(value) {
  formatC(value, format = "f", digits = 0L)
}

#' Build a safe DataFusion projection query
#' @keywords internal
#' @noRd
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
fabric_delta_quote_identifier <- function(value) {
  paste0('"', gsub('"', '""', value, fixed = TRUE), '"')
}

#' Convert a Python Arrow reader into an R nanoarrow stream
#' @keywords internal
#' @noRd
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

#' Detect timestamp-without-time-zone Arrow formats
#' @keywords internal
#' @noRd
fabric_delta_is_timestamp_ntz_format <- function(format) {
  grepl("^ts[smnu]:$", format)
}

#' Normalize Arrow types emitted by DataFusion
#' @keywords internal
#' @noRd
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
  rlang::abort(
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

#' Collect common Delta scalar types through nanoarrow
#' @keywords internal
#' @noRd
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
fabric_delta_abort_python <- function(error, bearer_token = NULL) {
  if (inherits(error, "fabric_delta_error")) {
    stop(error)
  }
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
  bullets <- c(
    "Unable to read the Delta table through Python delta-rs.",
    "x" = message
  )
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
  rlang::abort(
    bullets,
    class = unique(classes),
    delta_features = unsupported_features
  )
}
