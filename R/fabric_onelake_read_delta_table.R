.fabric_delta_max_exact_version <- 2^53
.fabric_delta_result_types <- c("tibble", "arrow_stream")
.fabric_delta_query_table <- "fabric_delta_table"

#' Read a Delta table from Microsoft Fabric OneLake
#'
#' `fabric_onelake_read_delta_table()` resolves a Fabric Lakehouse or Warehouse
#' table, authenticates to OneLake, and reads the selected Delta snapshot.
#' The Delta transaction log and Parquet data are interpreted by delta-rs
#' (through the Python '[deltalake](https://pypi.org/project/deltalake/)' package)
#' and returned as a tibble or a lazy Arrow stream.
#'
#' @details
#' `deltalake` and `nanoarrow` are declared with
#' [reticulate::py_require()] when fabricQueryR loads, but Python is not started
#' and packages are not downloaded until this function is called.
#' If `RETICULATE_PYTHON` selects a user-managed environment, install the
#' runtime dependencies in that environment with:
#'
#' ```text
#' python -m pip install "deltalake==1.6.2" "nanoarrow==0.8.0"
#' ```
#' OneLake credentials use the `https://storage.azure.com/.default` audience
#' and are passed to delta-rs as a bearer token with its Fabric endpoint
#' option enabled.
#'
#' OneLake data access is separate from item visibility. A generic Fabric item
#' `Read` grant exposes metadata but is not sufficient for a direct Delta read.
#' Workspace Admin, Member, and Contributor roles have broad OneLake access.
#' Otherwise grant item `Read` plus `ReadAll`, or, when OneLake security is
#' enabled, item `Read` plus a OneLake role whose `Read` scope contains the
#' target table. Tables protected by OneLake row- or column-level security can
#' be blocked because this external delta-rs reader does not enforce those
#' policies. A Fabric administrator must also enable the OneLake tenant setting
#' **Users can access data stored in OneLake with apps external to Fabric** for
#' the calling identity. See
#' [OneLake tenant settings](https://learn.microsoft.com/en-us/fabric/admin/service-admin-portal-onelake)
#' and the
#' [Fabric permission model](https://learn.microsoft.com/en-us/fabric/security/permission-model).
#'
#' Fabric Warehouse publishes Delta logs asynchronously. Reads therefore return
#' the latest snapshot published to OneLake, which can lag the current Warehouse
#' transaction state or remain fixed while Delta-log publishing is paused.
#'
#' `result = "arrow_stream"` is lazy and single-use. Opening either result is
#' retried once with a refreshable credential when delta-rs reports an
#' authentication failure. Failures that occur after a lazy stream has been
#' returned and consumed cannot be retried.
#'
#' `limit` is pushed down without an ordering expression. When it is smaller
#' than the snapshot, the returned rows are an implementation-defined subset
#' and can change with file layout, snapshot version, or scan scheduling. It is
#' not a stable pagination mechanism.
#'
#' `dfs_base` defaults to OneLake's global endpoint. Microsoft notes that data
#' can leave the workspace's region during global-endpoint resolution. For
#' data-residency requirements, pass the workspace capacity's regional endpoint,
#' such as `https://westeurope-onelake.dfs.fabric.microsoft.com`; use the
#' workspace FQDN required by a workspace private link where applicable. See
#' [Connecting to Microsoft OneLake](https://learn.microsoft.com/en-us/fabric/onelake/onelake-access-api).
#'
#' The tested delta-rs runtime reads ordinary Delta snapshots, schema evolution,
#' typed partitions, classic checkpoints, column mapping, deletion vectors, and
#' shallow clones. For a deletion-vector-capable snapshot, every active file
#' must have Delta statistics proving it has no more than 65,536 physical rows;
#' larger or unmeasured files are rejected because the selected runtime can
#' apply masks at incorrect record-batch offsets. This conservative,
#' metadata-only preflight avoids `deltalake`'s mask-materializing
#' `deletion_vectors()` API. Its current reader also does not support Fabric
#' tables requiring Type
#' Widening, V2 Checkpoints, or Fabric's VariantShreddingPreview; those fail with
#' `fabric_delta_unsupported_feature_error`. Arrow Variant extension columns in
#' otherwise readable tables require `result = "arrow_stream"`.
#' Feature availability is protocol- and runtime-specific; consult the
#' [Fabric Delta interoperability matrix](https://learn.microsoft.com/en-us/fabric/fundamentals/delta-lake-interoperability)
#' and use Fabric PySpark when the selected delta-rs runtime cannot read a table.
#'
#' Delta `integer` values normally use R integers and Delta `long` values
#' normally use [bit64::integer64()]. Because those R representations reserve
#' the respective minimum value as `NA`, a column containing Delta's valid
#' `-2147483648` integer is widened to an exact R double and a column containing
#' Delta's valid `-9223372036854775808` long uses the exact character-backed
#' `fabric_delta_integer64` class. This applies recursively to nested values.
#' Delta decimals are returned as exact character values, including when nested. Delta
#' `timestamp_ntz` values use the character-backed
#' `fabric_delta_timestamp_ntz` class. The Arrow stream preserves
#' timezone-free timestamps as Arrow timestamps and represents decimals as
#' strings, matching the R result's exact-decimal contract. Nullable struct
#' columns retain their parent validity through the
#' `fabric_delta_struct_column` class, so a null struct remains distinct from a
#' present struct whose children are all null. For tables the runtime can open,
#' Arrow Variant extension columns are preserved by `result = "arrow_stream"`;
#' tibble collection rejects them explicitly because exposing their physical
#' `metadata` and `value` buffers as ordinary R data would be misleading.
#'
#' @param table_path Table name. For backward compatibility, a slash-separated
#'   value is accepted and its final segment is used; select a schema with
#'   `schema`.
#' @param workspace_name Fabric workspace display name or GUID, or a record from
#'   [fabric_workspaces()]. ABFSS-safe display names can be used directly; use
#'   paired workspace/item GUIDs or discovery records when the display name
#'   contains spaces or other special characters.
#' @param lakehouse_name Lakehouse or Warehouse name, GUID, or discovery record.
#'   The argument name is retained for backward compatibility.
#' @param schema Lakehouse or Warehouse schema, or `NULL`. A discovered
#'   schema-enabled Lakehouse's default schema is used automatically. Warehouse
#'   targets default to `"dbo"`.
#' @param item_type `"Lakehouse"` or `"Warehouse"`. This is inferred from a
#'   discovery record or a `.Lakehouse`/`.Warehouse` suffix. Supply it for a
#'   suffixless item display name, especially a Warehouse name.
#' @param tenant_id Microsoft Entra tenant ID. Defaults to
#'   `FABRICQUERYR_TENANT_ID`.
#' @param client_id Microsoft Entra application/client ID. Defaults to
#'   `FABRICQUERYR_CLIENT_ID`, then the Azure CLI application ID.
#' @param token Optional `AzureAuth::AzureToken`, bearer-token string, or
#'   token-provider function.
#' @param auth_args Named list passed to [AzureAuth::get_azure_token()] when
#'   fabricQueryR acquires a token.
#' @param version Optional non-negative Delta transaction version. Values
#'   through `2^53` are represented exactly.
#' @param timestamp_partition_timezone Deprecated compatibility argument.
#'   delta-rs does not expose the previous R engine's timezone override;
#'   non-`NULL` values are rejected.
#' @param dest_dir Deprecated compatibility argument. Data is no longer staged
#'   locally. A non-`NULL` value is ignored with a warning.
#' @param verbose Logical. Show authentication and read progress.
#' @param dfs_base OneLake DFS endpoint. Use the workspace capacity's regional
#'   endpoint when endpoint-resolution data residency matters, or its required
#'   workspace-private FQDN for a workspace private link.
#' @param columns Optional character vector of logical Delta columns, in the
#'   requested order. `NULL` returns all columns.
#' @param limit Optional non-negative whole number limiting returned rows. No
#'   ordering is applied, so a partial result is an implementation-defined
#'   subset and is not suitable for stable pagination.
#' @param result `"tibble"` or `"arrow_stream"`.
#'
#' @return A tibble, or a lazy single-use `nanoarrow_array_stream` compatible
#'   with [arrow::as_record_batch_reader()].
#' @export
#'
#' @examples
#' \dontrun{
#' rows <- fabric_onelake_read_delta_table(
#'   table_path = "PatientInfo",
#'   workspace_name = "PatientsWorkspace",
#'   lakehouse_name = "Clinical.Lakehouse",
#'   schema = "dbo",
#'   columns = c("PatientId", "Status"),
#'   limit = 1000
#' )
#'
#' stream <- fabric_onelake_read_delta_table(
#'   table_path = "PatientInfo",
#'   workspace_name = "PatientsWorkspace",
#'   lakehouse_name = "Clinical.Lakehouse",
#'   schema = "dbo",
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
  timestamp_partition_timezone = NULL,
  dest_dir = NULL,
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
  fabric_delta_validate_compatibility_args(
    timestamp_partition_timezone,
    dest_dir
  )

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

  parts <- strsplit(table_path, "/", fixed = TRUE)[[1L]]
  table_name <- parts[[length(parts)]]
  if (!nzchar(table_name)) {
    rlang::abort("table_path must end in a non-empty table name")
  }
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

#' Handle arguments that belonged to the retired R engine
#' @keywords internal
#' @noRd
fabric_delta_validate_compatibility_args <- function(
  timestamp_partition_timezone,
  dest_dir
) {
  if (!is.null(timestamp_partition_timezone)) {
    rlang::abort(
      paste0(
        "timestamp_partition_timezone is not supported by the delta-rs ",
        "reader. Convert legacy offset-less timestamp partitions at the ",
        "source or omit this argument."
      ),
      class = "fabric_delta_unsupported_error"
    )
  }
  if (!is.null(dest_dir)) {
    rlang::warn(
      paste0(
        "dest_dir is deprecated and ignored because the delta-rs reader ",
        "streams from OneLake without staging files."
      ),
      class = "fabric_delta_deprecated_argument"
    )
  }
  invisible(NULL)
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
      class = "fabric_delta_invalid_target"
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

#' Open a delta-rs DataFusion query
#' @keywords internal
#' @noRd
fabric_delta_python_reader <- function(
  table_uri,
  bearer_token = NULL,
  version = NULL,
  columns = NULL,
  limit = NULL
) {
  fabric_delta_validate_runtime()
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
  fabric_delta_validate_deletion_vectors(table)
  builder <- .delta_python$deltalake$QueryBuilder()
  builder$register(.fabric_delta_query_table, table)
  reader <- builder$execute(fabric_delta_query(columns, limit))
  attr(reader, "fabric_delta_table") <- table
  attr(reader, "fabric_delta_query_builder") <- builder
  reader
}

.fabric_delta_max_deletion_vector_rows <- 65536

#' Return the snapshot's enabled Delta reader features
#' @keywords internal
#' @noRd
fabric_delta_reader_features <- function(table) {
  features <- reticulate::py_to_r(table$protocol()$reader_features)
  if (is.null(features)) character() else as.character(features)
}

#' Return physical row counts from active Delta add-action statistics
#' @keywords internal
#' @noRd
fabric_delta_active_file_rows <- function(table) {
  rows <- table$get_add_actions()$column("num_records")$to_pylist()
  count <- as.integer(reticulate::py_to_r(.delta_python$builtins$len(rows)))
  if (!count) {
    return(numeric())
  }
  vapply(
    seq_len(count) - 1L,
    function(index) {
      value <- reticulate::py_to_r(
        rows$`__getitem__`(.delta_python$builtins$int(index))
      )
      if (is.null(value)) NA_real_ else as.double(value)
    },
    numeric(1)
  )
}

#' Reject deletion-vector-capable snapshots the selected provider may misapply
#' @keywords internal
#' @noRd
fabric_delta_validate_deletion_vectors <- function(
  table = NULL,
  features = NULL,
  active_file_rows = NULL,
  max_rows = .fabric_delta_max_deletion_vector_rows
) {
  if (is.null(features)) {
    features <- fabric_delta_reader_features(table)
  }
  if (!any(tolower(features) == "deletionvectors")) {
    return(invisible(numeric()))
  }
  if (is.null(active_file_rows)) {
    active_file_rows <- fabric_delta_active_file_rows(table)
  }
  unknown <- is.na(active_file_rows)
  if (any(unknown)) {
    rlang::abort(
      c(
        paste0(
          "The selected delta-rs runtime cannot safely scan this ",
          "deletion-vector-capable snapshot."
        ),
        "x" = paste0(
          sum(unknown),
          " active file(s) have no numRecords statistic, so their physical ",
          "row counts cannot be checked without materializing deletion vectors."
        ),
        "i" = paste0(
          "Use a Fabric PySpark notebook for this table, or rewrite/compact ",
          "the table so every active file has row-count statistics."
        )
      ),
      class = c(
        "fabric_delta_unsupported_feature_error",
        "fabric_delta_unsupported_error",
        "fabric_delta_error"
      ),
      delta_features = "UnmeasuredDeletionVectorFile",
      deletion_vector_unknown_files = sum(unknown),
      deletion_vector_row_limit = max_rows
    )
  }
  unsafe <- active_file_rows[active_file_rows > max_rows]
  if (!length(unsafe)) {
    return(invisible(active_file_rows))
  }
  rlang::abort(
    c(
      paste0(
        "The selected delta-rs runtime cannot safely scan this ",
        "deletion-vector-capable snapshot."
      ),
      "x" = paste0(
        "Active files contain up to ",
        format(max(unsafe), big.mark = ",", scientific = FALSE),
        " physical rows; the safe per-file limit is ",
        format(max_rows, big.mark = ",", scientific = FALSE),
        " rows."
      ),
      "i" = paste0(
        "Use a Fabric PySpark notebook for this table, or compact the table ",
        "into files no larger than the safe deletion-vector limit."
      )
    ),
    class = c(
      "fabric_delta_unsupported_feature_error",
      "fabric_delta_unsupported_error",
      "fabric_delta_error"
    ),
    delta_features = "LargeDeletionVector",
    deletion_vector_rows = max(unsafe),
    deletion_vector_row_limit = max_rows
  )
}

#' Render one exact R whole number for Python or SQL
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
  query <- paste0(
    "SELECT ",
    projection,
    " FROM ",
    fabric_delta_quote_identifier(.fabric_delta_query_table)
  )
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

#' Convert a Python Arro3 reader into an R nanoarrow stream
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
  stream
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
      format <- "l"
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

#' Collect a Python reader without losing exact Delta scalar types
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
  ptype <- fabric_delta_collect_ptype(source_schema, target_schema)
  arrays <- nanoarrow::collect_array_stream(stream)
  collected_stream <- nanoarrow::basic_array_stream(
    arrays,
    schema = target_schema
  )
  value <- nanoarrow::convert_array_stream(collected_stream, to = ptype)
  value <- fabric_delta_restore_collected_types(value, source_schema)
  value <- tibble::as_tibble(value)
  fabric_delta_restore_struct_validity(
    value,
    source_schema,
    lapply(arrays, fabric_delta_array_descriptor),
    top_level = TRUE
  )
}

#' Validate the installed Python Delta query surface
#' @keywords internal
#' @noRd
fabric_delta_validate_runtime <- function(
  version = NULL,
  exports = NULL,
  nanoarrow_version = NULL
) {
  required_exports <- c("DeltaTable", "QueryBuilder")
  if (is.null(version)) {
    version <- reticulate::py_to_r(.delta_python$deltalake$`__version__`)
  }
  if (is.null(nanoarrow_version)) {
    nanoarrow_version <- reticulate::py_to_r(
      .delta_python$nanoarrow$`__version__`
    )
  }
  if (is.null(exports)) {
    exports <- required_exports[vapply(
      required_exports,
      function(name) reticulate::py_has_attr(.delta_python$deltalake, name),
      logical(1)
    )]
  }
  valid_version <- identical(as.character(version), "1.6.2")
  valid_nanoarrow_version <- identical(
    as.character(nanoarrow_version),
    "0.8.0"
  )
  missing_exports <- setdiff(required_exports, exports)
  if (valid_version && valid_nanoarrow_version && !length(missing_exports)) {
    return(invisible(list(
      deltalake_version = version,
      nanoarrow_version = nanoarrow_version,
      exports = exports
    )))
  }

  reasons <- character()
  if (!valid_version) {
    reasons <- c(
      reasons,
      paste0(
        "Installed deltalake version ",
        version,
        "; this package is tested with exactly version 1.6.2."
      )
    )
  }
  if (!valid_nanoarrow_version) {
    reasons <- c(
      reasons,
      paste0(
        "Installed Python nanoarrow version ",
        nanoarrow_version,
        "; this package is tested with exactly version 0.8.0."
      )
    )
  }
  if (length(missing_exports)) {
    reasons <- c(
      reasons,
      paste(
        "The deltalake module is missing:",
        paste(missing_exports, collapse = ", ")
      )
    )
  }
  rlang::abort(
    c(
      "The selected Python environment has an incompatible Delta runtime.",
      "x" = paste(reasons, collapse = " "),
      "i" = paste0(
        'Install "deltalake==1.6.2" and "nanoarrow==0.8.0" in the ',
        "Python selected by ",
        "reticulate, or unset RETICULATE_PYTHON to use a managed environment."
      )
    ),
    class = c(
      "fabric_delta_environment_error",
      "fabric_delta_error"
    )
  )
}

#' Find Arrow Parquet Variant extensions in a schema
#' @keywords internal
#' @noRd
fabric_delta_variant_paths <- function(schema, path = character()) {
  extension_name <- schema$metadata[["ARROW:extension:name"]] %||% ""
  current <- if (length(path)) paste(path, collapse = ".") else "<root>"
  found <- if (identical(extension_name, "arrow.parquet.variant")) {
    current
  } else {
    character()
  }
  for (index in seq_along(schema$children)) {
    child <- schema$children[[index]]
    child_name <- child$name %||% names(schema$children)[[index]] %||% index
    found <- c(
      found,
      fabric_delta_variant_paths(child, c(path, as.character(child_name)))
    )
  }
  unique(found)
}

#' Reject lossy Variant-to-tibble collection
#' @keywords internal
#' @noRd
fabric_delta_validate_collect_schema <- function(schema) {
  paths <- fabric_delta_variant_paths(schema)
  if (!length(paths)) {
    return(invisible(schema))
  }
  rlang::abort(
    c(
      "Arrow Variant columns cannot be collected to a tibble losslessly.",
      "x" = paste("Variant column(s):", paste(paths, collapse = ", ")),
      "i" = paste0(
        "Use result = \"arrow_stream\" to preserve the canonical ",
        "arrow.parquet.variant extension, or decode the values in a ",
        "Variant-aware engine such as Fabric PySpark."
      )
    ),
    class = c(
      "fabric_delta_variant_collection_error",
      "fabric_delta_unsupported_feature_error",
      "fabric_delta_unsupported_error",
      "fabric_delta_error"
    ),
    delta_features = "Variant",
    variant_paths = paths
  )
}

#' Build a recursive nanoarrow collection prototype
#' @keywords internal
#' @noRd
fabric_delta_collect_ptype <- function(source_schema, target_schema) {
  ptype <- nanoarrow::infer_nanoarrow_ptype(target_schema)
  fabric_delta_patch_ptype(ptype, source_schema, target_schema)
}

#' Patch nanoarrow's lossy int64 prototype recursively
#' @keywords internal
#' @noRd
fabric_delta_patch_ptype <- function(ptype, source_schema, target_schema) {
  if (identical(source_schema$format, "i")) {
    return(bit64::integer64())
  }
  if (source_schema$format %in% c("l", "L")) {
    return(character())
  }

  format <- target_schema$format
  if (identical(format, "+s")) {
    for (index in seq_along(target_schema$children)) {
      ptype[[index]] <- fabric_delta_patch_ptype(
        ptype[[index]],
        source_schema$children[[index]],
        target_schema$children[[index]]
      )
    }
    return(ptype)
  }
  if (format %in% c("+l", "+L", "+m") || startsWith(format, "+w:")) {
    attr(ptype, "ptype") <- fabric_delta_patch_ptype(
      attr(ptype, "ptype", exact = TRUE),
      source_schema$children[[1L]],
      target_schema$children[[1L]]
    )
    return(ptype)
  }
  ptype
}

#' Restore R classes that cannot be requested through an Arrow schema
#' @keywords internal
#' @noRd
fabric_delta_restore_collected_types <- function(value, schema) {
  if (identical(schema$format, "i")) {
    return(fabric_delta_restore_integer32(value))
  }
  if (schema$format %in% c("l", "L")) {
    return(fabric_delta_restore_integer64(value))
  }
  if (fabric_delta_is_timestamp_ntz_format(schema$format)) {
    return(fabric_delta_timestamp_ntz(value))
  }
  if (identical(schema$format, "+s")) {
    for (index in seq_along(schema$children)) {
      value[[index]] <- fabric_delta_restore_collected_types(
        value[[index]],
        schema$children[[index]]
      )
    }
    return(value)
  }
  if (
    schema$format %in%
      c("+l", "+L", "+m", "+vl", "+vL") ||
      startsWith(schema$format, "+w:")
  ) {
    if (!fabric_delta_schema_needs_restore(schema$children[[1L]])) {
      return(value)
    }
    return(lapply(value, function(element) {
      if (is.null(element)) {
        return(NULL)
      }
      fabric_delta_restore_collected_types(
        element,
        schema$children[[1L]]
      )
    }))
  }
  value
}

#' Restore Delta integer values without confusing their minimum with R's NA
#' @keywords internal
#' @noRd
fabric_delta_restore_integer32 <- function(value) {
  text <- as.character(value)
  numeric_value <- suppressWarnings(as.double(text))
  invalid <- !is.na(text) & (
    is.na(numeric_value) |
      !is.finite(numeric_value) |
      numeric_value != trunc(numeric_value)
  )
  if (any(invalid)) {
    rlang::abort(
      "A Delta integer column contained a value that is not an exact integer.",
      class = c("fabric_delta_conversion_error", "fabric_delta_error")
    )
  }

  needs_double <- numeric_value <= -2147483648 |
    numeric_value > 2147483647
  if (any(needs_double, na.rm = TRUE)) {
    if (any(abs(numeric_value) >= 2^53, na.rm = TRUE)) {
      rlang::abort(
        paste0(
          "A Delta integer column contained an out-of-range value that ",
          "cannot be represented exactly in an R double."
        ),
        class = c("fabric_delta_conversion_error", "fabric_delta_error")
      )
    }
    return(numeric_value)
  }
  as.integer(numeric_value)
}

#' Restore Delta long values without confusing their minimum with bit64's NA
#' @keywords internal
#' @noRd
fabric_delta_restore_integer64 <- function(value) {
  text <- as.character(value)
  if (any(text == "-9223372036854775808", na.rm = TRUE)) {
    return(structure(
      text,
      class = c("fabric_delta_integer64", "character")
    ))
  }
  bit64::as.integer64(text)
}

#' Describe a logical slice of a collected Arrow array
#' @keywords internal
#' @noRd
fabric_delta_array_descriptor <- function(array, start = 0L, length = NULL) {
  if (is.null(length)) {
    length <- array$length
  }
  list(array = array, start = as.double(start), length = as.double(length))
}

#' Read validity bits for collected Arrow array slices
#' @keywords internal
#' @noRd
fabric_delta_array_validity <- function(descriptors) {
  unlist(
    lapply(descriptors, function(descriptor) {
      length <- descriptor$length
      if (!length) {
        return(logical())
      }
      buffer <- nanoarrow::convert_buffer(
        descriptor$array$buffers[[1L]]
      )
      if (!length(buffer)) {
        return(rep(TRUE, length))
      }
      positions <- descriptor$array$offset +
        descriptor$start +
        seq_len(length)
      as.logical(buffer[positions])
    }),
    use.names = FALSE
  )
}

#' Project struct slices to one child array
#' @keywords internal
#' @noRd
fabric_delta_struct_child_descriptors <- function(descriptors, index) {
  lapply(descriptors, function(descriptor) {
    child <- descriptor$array$children[[index]]
    physical_start <- descriptor$array$offset + descriptor$start
    fabric_delta_array_descriptor(
      child,
      start = physical_start - child$offset,
      length = descriptor$length
    )
  })
}

#' Extract list offsets for one Arrow array slice
#' @keywords internal
#' @noRd
fabric_delta_list_offsets <- function(descriptor) {
  offsets <- nanoarrow::convert_buffer(
    descriptor$array$buffers[[2L]]
  )
  positions <- descriptor$array$offset +
    descriptor$start +
    seq_len(descriptor$length + 1L)
  as.double(offsets[positions])
}

#' Restore nullable struct semantics erased by data.frame conversion
#' @keywords internal
#' @noRd
fabric_delta_restore_struct_validity <- function(
  value,
  schema,
  descriptors,
  top_level = FALSE
) {
  if (identical(schema$format, "+s")) {
    for (index in seq_along(schema$children)) {
      value[[index]] <- fabric_delta_restore_struct_validity(
        value[[index]],
        schema$children[[index]],
        fabric_delta_struct_child_descriptors(descriptors, index)
      )
    }
    if (!isTRUE(top_level)) {
      class(value) <- unique(c("fabric_delta_struct_column", class(value)))
      attr(value, "fabric_delta_struct_validity") <-
        fabric_delta_array_validity(descriptors)
    }
    return(value)
  }

  is_variable_list <- schema$format %in% c("+l", "+L", "+m", "+vl", "+vL")
  is_fixed_list <- startsWith(schema$format, "+w:")
  if (!is_variable_list && !is_fixed_list) {
    return(value)
  }

  value_attributes <- attributes(value)
  attributes(value) <- NULL
  child_schema <- schema$children[[1L]]
  value_index <- 0L
  fixed_width <- if (is_fixed_list) {
    as.double(sub("^\\+w:", "", schema$format))
  } else {
    NULL
  }
  for (descriptor in descriptors) {
    parent_validity <- fabric_delta_array_validity(list(descriptor))
    child <- descriptor$array$children[[1L]]
    offsets <- if (is_variable_list) {
      fabric_delta_list_offsets(descriptor)
    } else {
      physical_start <- descriptor$array$offset + descriptor$start
      fixed_width * (physical_start + seq.int(0, descriptor$length))
    }
    for (index in seq_len(descriptor$length)) {
      value_index <- value_index + 1L
      if (!parent_validity[[index]] || is.null(value[[value_index]])) {
        next
      }
      child_start <- offsets[[index]]
      child_length <- offsets[[index + 1L]] - child_start
      child_descriptor <- fabric_delta_array_descriptor(
        child,
        start = child_start - child$offset,
        length = child_length
      )
      restored <- fabric_delta_restore_struct_validity(
        value[[value_index]],
        child_schema,
        list(child_descriptor)
      )
      value[value_index] <- list(restored)
    }
  }
  attributes(value) <- value_attributes
  value
}

#' Detect timestamp-without-time-zone Arrow formats
#' @keywords internal
#' @noRd
fabric_delta_is_timestamp_ntz_format <- function(format) {
  grepl("^ts[smnu]:$", format)
}

#' Detect timestamp_ntz recursively
#' @keywords internal
#' @noRd
fabric_delta_schema_has_timestamp_ntz <- function(schema) {
  if (fabric_delta_is_timestamp_ntz_format(schema$format)) {
    return(TRUE)
  }
  any(vapply(
    schema$children,
    fabric_delta_schema_has_timestamp_ntz,
    logical(1)
  )) ||
    (!is.null(schema$dictionary) &&
      fabric_delta_schema_has_timestamp_ntz(schema$dictionary))
}

#' Detect collected Delta types that require an R-side restoration recursively
#' @keywords internal
#' @noRd
fabric_delta_schema_needs_restore <- function(schema) {
  if (
    schema$format %in% c("i", "l", "L") ||
      fabric_delta_is_timestamp_ntz_format(schema$format)
  ) {
    return(TRUE)
  }
  any(vapply(
    schema$children,
    fabric_delta_schema_needs_restore,
    logical(1)
  )) ||
    (!is.null(schema$dictionary) &&
      fabric_delta_schema_needs_restore(schema$dictionary))
}

#' Construct exact wall-clock Delta timestamp values
#' @keywords internal
#' @noRd
fabric_delta_timestamp_ntz <- function(value) {
  text <- as.character(value)
  present <- !is.na(text)
  text[present] <- sub("T", " ", text[present], fixed = TRUE)
  fractional <- present & grepl("\\.[0-9]+$", text)
  if (any(fractional)) {
    whole <- sub("\\.([0-9]+)$", "", text[fractional])
    digits <- sub("^.*\\.([0-9]+)$", "\\1", text[fractional])
    digits <- substr(paste0(digits, "000000"), 1L, 6L)
    text[fractional] <- paste0(whole, ".", digits)
  }
  text[present & !fractional] <- paste0(
    text[present & !fractional],
    ".000000"
  )
  structure(text, class = c("fabric_delta_timestamp_ntz", "character"))
}

#' Format a Delta timestamp without time zone as wall-clock text
#' @param x A Delta `timestamp_ntz` vector.
#' @param format Optional output format.
#' @param ... Additional arguments passed to [base::format.POSIXct()].
#' @return Character wall-clock timestamps.
#' @export
#' @noRd
format.fabric_delta_timestamp_ntz <- function(x, format = NULL, ...) {
  value <- unclass(x)
  if (is.null(format)) {
    return(value)
  }
  format(
    as.POSIXct.fabric_delta_timestamp_ntz(x, tz = "UTC"),
    format = format,
    tz = "UTC",
    ...
  )
}

#' Localize a Delta timestamp without time zone
#' @param x A Delta `timestamp_ntz` vector.
#' @param tz IANA timezone in which to interpret the wall-clock values.
#' @param ... Unused.
#' @return A `POSIXct` vector localized in `tz`.
#' @export
#' @noRd
as.POSIXct.fabric_delta_timestamp_ntz <- function(x, tz = "UTC", ...) {
  as.POSIXct(
    unclass(x),
    format = "%Y-%m-%d %H:%M:%OS",
    tz = tz
  )
}

#' @export
#' @noRd
`[.fabric_delta_timestamp_ntz` <- function(x, ...) {
  structure(NextMethod("["), class = class(x))
}

#' @export
#' @noRd
`[.fabric_delta_integer64` <- function(x, ...) {
  structure(NextMethod("["), class = class(x))
}

# Methods for nullable struct columns produced by the delta-rs reader.

#' @export
#' @noRd
is.na.fabric_delta_struct_column <- function(x) {
  validity <- attr(x, "fabric_delta_struct_validity", exact = TRUE)
  if (is.null(validity)) {
    return(rep(FALSE, NROW(x)))
  }
  !validity
}

#' @export
#' @noRd
`[.fabric_delta_struct_column` <- function(x, i, j, drop = FALSE) {
  validity <- attr(x, "fabric_delta_struct_validity", exact = TRUE)
  row_index <- seq_len(nrow(x))
  names(row_index) <- row.names(x)
  one_index <- missing(j) && (
    nargs() == 2L || (!missing(drop) && nargs() == 3L)
  )
  selected_rows <- if (one_index || missing(i)) row_index else row_index[i]
  result <- NextMethod("[")
  if (is.data.frame(result)) {
    class(result) <- unique(c("fabric_delta_struct_column", class(result)))
    if (!is.null(validity)) {
      attr(result, "fabric_delta_struct_validity") <- validity[selected_rows]
    }
  }
  result
}

# Compatibility methods for Variant objects created by
# fabricQueryR <= 0.2.1.9000. These are not produced by the delta-rs reader.

#' @export
#' @noRd
format.fabric_delta_variant <- function(x, ...) {
  x$display %||% "NULL"
}

#' @export
#' @noRd
print.fabric_delta_variant <- function(x, ...) {
  cat("<fabric_delta_variant:", x$type, ">", format(x), "\n")
  invisible(x)
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
        "Item Read alone may not authorize external-engine table reads when ",
        "OneLake security, RLS, or CLS restricts the data."
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
