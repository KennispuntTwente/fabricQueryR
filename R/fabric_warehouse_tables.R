#' Read a Microsoft Fabric Warehouse table
#'
#' Provides the table-oriented read counterpart to
#' [fabric_warehouse_write_table()]. It resolves the Warehouse like the writer,
#' safely quotes the schema, table, and projected columns, and delegates query
#' execution and type conversion to [fabric_sql_query()]. Use that lower-level
#' function for filters, ordering, joins, aggregations, or other T-SQL.
#'
#' @inheritParams fabric_warehouse_write_table
#' @inheritParams fabric_sql_query
#' @param table Warehouse table name, or a record containing a `name`, `table`,
#'   or `displayName` field.
#' @param schema Warehouse schema. Defaults to `"dbo"`; a table record can
#'   supply its schema when this argument is omitted.
#' @param columns Optional unique column names to project.
#' @param limit Optional non-negative maximum number of rows to return.
#' @param api_base Fabric REST API base used when a Warehouse name or GUID must
#'   be discovered.
#'
#' @return A tibble, or a single-use `nanoarrow_array_stream` when
#'   `result = "arrow_stream"`.
#'
#' @section Large results:
#' Use `backend = "adbc"` with `result = "arrow_stream"` for a native Arrow
#' result path that can be consumed without first collecting the complete table
#' in an R data frame. The external ADBC `mssql` driver must be installed.
#'
#' `limit` uses T-SQL `TOP` and does not define row order. Use
#' [fabric_sql_query()] with an explicit `ORDER BY` when deterministic row
#' selection matters.
#'
#' @references
#' [Query a Fabric Warehouse](https://learn.microsoft.com/en-us/fabric/data-warehouse/query-warehouse)
#'
#' [Fabric Warehouse connectivity](https://learn.microsoft.com/en-us/fabric/data-warehouse/connectivity)
#' @export
#'
#' @examples
#' \dontrun{
#' warehouse <- fabric_warehouses("Analytics")[[1L]]
#' orders <- fabric_warehouse_read_table(
#'   warehouse,
#'   "orders",
#'   columns = c("id", "amount"),
#'   limit = 1000
#' )
#'
#' stream <- fabric_warehouse_read_table(
#'   warehouse,
#'   "orders",
#'   backend = "adbc",
#'   result = "arrow_stream"
#' )
#' reader <- arrow::as_record_batch_reader(stream)
#' }
fabric_warehouse_read_table <- function(
  warehouse,
  table,
  workspace = NULL,
  schema = "dbo",
  columns = NULL,
  limit = NULL,
  result = c("tibble", "arrow_stream"),
  backend = c("odbc", "adbc"),
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  token = NULL,
  auth_args = list(),
  api_base = .fabric_api_base,
  allow_custom_endpoint = FALSE,
  verbose = TRUE,
  timeout = 30L,
  max_tries = 3L,
  retry_delay = 5
) {
  schema_supplied <- !missing(schema)
  table_record <- fabric_as_record(table)
  if (is.null(table_record) && is.list(table)) {
    table_record <- table
  }
  if (!is.null(table_record)) {
    table <- fabric_record_value(
      table_record,
      "name",
      "table",
      "displayName"
    )
    if (!schema_supplied) {
      schema <- fabric_record_value(table_record, "schema") %||% schema
    }
  }

  result <- match.arg(result)
  backend <- match.arg(backend)
  .fabric_warehouse_identifier(table, "table")
  .fabric_warehouse_identifier(schema, "schema")
  fabric_delta_validate_columns(columns)
  if (!is.null(columns)) {
    .fabric_warehouse_column_names(columns)
  }
  limit <- fabric_delta_validate_whole_number(
    limit,
    "limit",
    allow_null = TRUE
  )
  fabric_sql_retry_settings(max_tries, retry_delay)
  fabric_sql_timeout(timeout)
  fabric_sql_require_backend(backend, result = result)

  projection <- if (is.null(columns)) {
    "*"
  } else {
    paste(
      vapply(
        columns,
        .fabric_warehouse_quote_identifier,
        character(1),
        USE.NAMES = FALSE
      ),
      collapse = ", "
    )
  }
  top <- if (is.null(limit)) {
    ""
  } else {
    paste0("TOP (", fabric_delta_whole_number_text(limit), ") ")
  }
  sql <- paste0(
    "SELECT ",
    top,
    projection,
    " FROM ",
    .fabric_warehouse_quote_identifier(schema),
    ".",
    .fabric_warehouse_quote_identifier(table)
  )

  api_base_supplied <- !missing(api_base)
  base <- fabric_api_base(api_base, allow_custom_endpoint)
  credential <- fabric_credential(
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args
  )
  destination <- .fabric_warehouse_resolve_item(
    warehouse,
    workspace,
    expected_type = "Warehouse",
    credential = credential,
    api_base = base,
    api_base_supplied = api_base_supplied,
    allow_custom_endpoint = allow_custom_endpoint,
    require_sql = TRUE,
    argument = "warehouse"
  )

  fabric_sql_query(
    server = destination$record,
    sql = sql,
    result = result,
    target_type = "warehouse",
    backend = backend,
    tenant_id = tenant_id,
    client_id = client_id,
    token = credential,
    auth_args = list(),
    timeout = timeout,
    read_only = TRUE,
    allow_custom_endpoint = allow_custom_endpoint,
    verbose = verbose,
    max_tries = max_tries,
    retry_delay = retry_delay,
    idempotent = TRUE
  )
}

#' Write an R or Arrow object to a Fabric Warehouse table
#'
#' Serializes a data frame, tibble, or Arrow object to bounded Parquet parts,
#' stages them in a Lakehouse, and loads them into a Fabric Warehouse table.
#' Existing tables use the Warehouse `COPY INTO` command. When creation or
#' drop-based replacement is requested, `CREATE TABLE AS SELECT` (CTAS) creates
#' and loads the table directly from the staged Parquet schema. Lazy Arrow
#' inputs are consumed as record batches and are not first collected into an R
#' data frame.
#'
#' @param warehouse A Warehouse record returned by [fabric_warehouses()] or
#'   [fabric_item()], or its name or GUID when `workspace` is supplied.
#' @param table Destination table name.
#' @param data A data frame, tibble, Arrow Table, RecordBatch, Dataset, Scanner,
#'   RecordBatchReader, Arrow dplyr query, or Arrow-compatible array stream.
#' @param staging_lakehouse A Lakehouse record returned by
#'   [fabric_lakehouses()] or [fabric_item()], or its name or GUID. Fabric does
#'   not support a Warehouse item as the OneLake source of `COPY INTO`, so a
#'   Lakehouse staging item is required.
#' @param workspace Workspace name, GUID, or discovery record containing
#'   `warehouse`. May be omitted when `warehouse` is a discovery record.
#' @param staging_workspace Workspace containing `staging_lakehouse`. Defaults
#'   to the Warehouse workspace. May be omitted when `staging_lakehouse` is a
#'   discovery record.
#' @param schema Destination schema. Defaults to `"dbo"`.
#' @param mode `"Append"` adds rows. `"Overwrite"` replaces the table contents
#'   using `overwrite_method`.
#' @param overwrite_method For `mode = "Overwrite"`, `"Truncate"` preserves the
#'   existing table definition and loads it with `COPY INTO`; `"Drop"` drops
#'   and recreates the table from the staged Parquet schema with CTAS. Drop
#'   replacement also removes table-specific metadata such as constraints and
#'   grants. Ignored for append mode.
#' @param create_if_missing Whether to create and load a missing destination
#'   with CTAS. The default preserves the previous requirement that append and
#'   truncate-overwrite targets already exist. Drop-overwrite recreates an
#'   existing table; set this argument to `TRUE` if it may be absent.
#' @param staging_root Lakehouse path below `Files/` used for temporary Parquet
#'   directories.
#' @param cleanup Whether to remove remote staging after confirmed success.
#' @param keep_staging_on_failure Whether to retain staged files after a
#'   confirmed pre-load failure. Staging is always retained when SQL execution
#'   might have reached the Warehouse.
#' @param compression Parquet compression codec passed to Arrow.
#' @param target_file_size Soft maximum size in bytes for each staged Parquet
#'   part. Fabric recommends files between 100 MB and 1 GB for Warehouse loads.
#' @param max_rows_per_file Optional exact maximum rows per staged part.
#' @param backend SQL connection backend, `"odbc"` or `"adbc"`.
#' @param verbose Whether to report SQL connection progress.
#' @param api_base Fabric REST API base used when a Warehouse or staging
#'   Lakehouse name or GUID must be discovered.
#' @inheritParams fabric_sql_connect
#' @inheritParams fabric_onelake_upload
#'
#' @return A `fabric_warehouse_write_result` list containing destination and
#'   staging identifiers, row and byte counts, part paths, and cleanup state.
#' @details
#' Existing-table writes map input fields by ordinal position to quoted
#' destination columns with the same names as `data`. With
#' `create_if_missing = TRUE`, a missing table is created and populated by a
#' single CTAS statement; Fabric infers its names and types from the staged
#' Parquet files.
#'
#' Truncate overwrite preserves the table definition. Drop overwrite recreates
#' the table and therefore intentionally discards its previous constraints,
#' indexes, permissions, and other table-level metadata. Both overwrite paths
#' run in an explicit Warehouse transaction and roll back on a confirmed SQL
#' failure.
#'
#' `COPY INTO` authenticates to OneLake as the identity executing the SQL
#' statement. That identity therefore needs the documented Warehouse bulk-load
#' permissions and Contributor access to the source and destination workspaces.
#'
#' Local staging is always removed. Remote staging is removed only after a
#' confirmed successful load unless `keep_staging_on_failure = FALSE` and the
#' failure occurred before SQL execution. Retaining files after an ambiguous
#' SQL error makes a retry or investigation possible without changing the
#' source while `COPY INTO` might still be completing.
#' @references
#' [COPY INTO in Fabric Warehouse](https://learn.microsoft.com/en-us/sql/t-sql/statements/copy-into-transact-sql?view=fabric)
#'
#' [Warehouse ingestion performance guidance](https://learn.microsoft.com/en-us/fabric/data-warehouse/guidelines-warehouse-performance)
#'
#' [Transactions in Fabric Warehouse](https://learn.microsoft.com/en-us/fabric/data-warehouse/transactions)
#'
#' [Create tables in Fabric Warehouse](https://learn.microsoft.com/en-us/fabric/data-warehouse/create-table)
#'
#' [Query Parquet files in Fabric Warehouse](https://learn.microsoft.com/en-us/fabric/data-warehouse/query-parquet-files)
#' @examples
#' \dontrun{
#' warehouse <- fabric_warehouses("Analytics")[[1L]]
#' staging <- fabric_lakehouses("Analytics")[[1L]]
#' fabric_warehouse_write_table(
#'   warehouse,
#'   "orders",
#'   data.frame(id = 1:3, amount = c(10, 20, 30)),
#'   staging_lakehouse = staging
#' )
#' }
#' @export
fabric_warehouse_write_table <- function(
  warehouse,
  table,
  data,
  staging_lakehouse,
  workspace = NULL,
  staging_workspace = NULL,
  schema = "dbo",
  mode = c("Append", "Overwrite"),
  overwrite_method = c("Truncate", "Drop"),
  create_if_missing = FALSE,
  staging_root = "Files/fabricqueryr-staging",
  cleanup = TRUE,
  keep_staging_on_failure = TRUE,
  compression = "snappy",
  target_file_size = 512 * 1024^2,
  max_rows_per_file = NULL,
  backend = c("odbc", "adbc"),
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  token = NULL,
  auth_args = list(),
  api_base = .fabric_api_base,
  dfs_base = "https://onelake.dfs.fabric.microsoft.com",
  allow_custom_endpoint = FALSE,
  verbose = TRUE
) {
  # 1 Validate and serialize without collecting lazy Arrow inputs --------------------------------

  mode <- .fabric_warehouse_choice(mode, c("Append", "Overwrite"), "mode")
  overwrite_method <- .fabric_warehouse_choice(
    overwrite_method,
    c("Truncate", "Drop"),
    "overwrite_method"
  )
  backend <- match.arg(backend)
  .fabric_warehouse_identifier(table, "table")
  .fabric_warehouse_identifier(schema, "schema")
  .fabric_operation_logical(cleanup, "cleanup")
  .fabric_operation_logical(keep_staging_on_failure, "keep_staging_on_failure")
  .fabric_operation_logical(create_if_missing, "create_if_missing")
  .fabric_operation_logical(verbose, "verbose")
  .fabric_warehouse_nonempty(compression, "compression")
  staging_root <- .fabric_warehouse_files_path(staging_root, "staging_root")
  prepared <- .fabric_warehouse_prepare_data(data)
  .fabric_warehouse_column_names(prepared$names)

  parquet_directory <- tempfile("fabricqueryr-warehouse-")
  dir.create(parquet_directory)
  on.exit(
    unlink(parquet_directory, recursive = TRUE, force = TRUE),
    add = TRUE
  )
  serialized <- .fabric_parquet_write_dataset(
    prepared,
    directory = parquet_directory,
    compression = compression,
    target_file_size = target_file_size,
    max_rows_per_file = max_rows_per_file,
    caller = "fabric_warehouse_write_table()",
    error_class = c(
      "fabric_warehouse_arrow_error",
      "fabric_warehouse_error"
    )
  )

  # 2 Resolve the Warehouse and its supported Lakehouse staging source ----------------------------

  api_base_supplied <- !missing(api_base)
  dfs_base_supplied <- !missing(dfs_base)
  base <- fabric_api_base(api_base, allow_custom_endpoint)
  credential <- fabric_credential(
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args
  )
  destination <- .fabric_warehouse_resolve_item(
    warehouse,
    workspace,
    expected_type = "Warehouse",
    credential = credential,
    api_base = base,
    api_base_supplied = api_base_supplied,
    allow_custom_endpoint = allow_custom_endpoint,
    require_sql = TRUE,
    argument = "warehouse"
  )
  stage_workspace <- staging_workspace %||%
    fabric_record_value(
      fabric_as_record(staging_lakehouse) %||% list(),
      "workspaceId"
    ) %||%
    destination$workspace_id
  stage <- .fabric_warehouse_resolve_item(
    staging_lakehouse,
    stage_workspace,
    expected_type = "Lakehouse",
    credential = credential,
    api_base = base,
    api_base_supplied = api_base_supplied,
    allow_custom_endpoint = allow_custom_endpoint,
    require_sql = FALSE,
    argument = "staging_lakehouse"
  )

  staging_path <- paste(
    staging_root,
    .fabric_warehouse_staging_id(),
    sep = "/"
  )
  storage_targets <- lapply(basename(serialized$paths), function(name) {
    onelake_resolve_target(
      stage$workspace_record %||% stage$workspace_id,
      stage$record %||% stage$item_id,
      paste(staging_path, name, sep = "/"),
      dfs_base = if (dfs_base_supplied) dfs_base else NULL
    )
  })
  storage_target <- storage_targets[[1L]]

  # 3 Upload stable Parquet parts before issuing the non-idempotent SQL ----------------------------

  tryCatch(
    for (index in seq_along(storage_targets)) {
      onelake_upload_target(
        storage_targets[[index]],
        credential,
        source = serialized$paths[[index]],
        overwrite = FALSE,
        if_match = NULL,
        chunk_size = getOption(
          "fabricqueryr.onelake.chunk_size",
          8 * 1024^2
        ),
        content_type = "application/vnd.apache.parquet",
        create_parents = TRUE
      )
    },
    error = function(error) {
      .fabric_warehouse_write_abort(
        error,
        storage_target,
        credential,
        staging_path,
        keep_staging_on_failure,
        ambiguous = FALSE
      )
    }
  )

  # 4 Connect and load, making overwrite atomic ---------------------------------------------------

  connection <- tryCatch(
    .fabric_warehouse_connect(
      destination$record,
      backend = backend,
      token = credential,
      read_only = FALSE,
      allow_custom_endpoint = allow_custom_endpoint,
      verbose = verbose
    ),
    error = function(error) {
      .fabric_warehouse_write_abort(
        error,
        storage_target,
        credential,
        staging_path,
        keep_staging_on_failure,
        ambiguous = FALSE
      )
    }
  )
  connected <- TRUE
  on.exit(
    if (connected) {
      try(.fabric_warehouse_disconnect(connection, backend), silent = TRUE)
    },
    add = TRUE
  )
  table_sql <- paste0(
    .fabric_warehouse_quote_identifier(schema),
    ".",
    .fabric_warehouse_quote_identifier(table)
  )
  copy_sql <- .fabric_warehouse_copy_sql(
    table_sql,
    prepared$names,
    storage_target
  )
  ctas_sql <- .fabric_warehouse_ctas_sql(
    table_sql,
    prepared$names,
    storage_target
  )
  transaction_open <- FALSE
  sql_started <- FALSE
  table_exists <- TRUE
  if (isTRUE(create_if_missing)) {
    table_exists <- tryCatch(
      .fabric_warehouse_table_exists(connection, schema, table),
      error = function(error) {
        .fabric_warehouse_write_abort(
          error,
          storage_target,
          credential,
          staging_path,
          keep_staging_on_failure,
          ambiguous = FALSE
        )
      }
    )
  }
  create_with_ctas <- isTRUE(create_if_missing) &&
    !table_exists ||
    identical(mode, "Overwrite") && identical(overwrite_method, "Drop")
  affected <- tryCatch(
    {
      if (identical(mode, "Overwrite") || create_with_ctas) {
        .fabric_warehouse_begin(connection)
        transaction_open <- TRUE
      }
      if (create_with_ctas) {
        sql_started <- TRUE
        if (
          identical(mode, "Overwrite") &&
            identical(overwrite_method, "Drop") &&
            table_exists
        ) {
          .fabric_warehouse_execute(
            connection,
            paste("DROP TABLE", table_sql)
          )
        }
        rows_affected <- .fabric_warehouse_execute(connection, ctas_sql)
      } else {
        if (identical(mode, "Overwrite")) {
          sql_started <- TRUE
          .fabric_warehouse_execute(
            connection,
            paste("TRUNCATE TABLE", table_sql)
          )
        }
        sql_started <- TRUE
        rows_affected <- .fabric_warehouse_execute(connection, copy_sql)
      }
      if (transaction_open) {
        .fabric_warehouse_commit(connection)
        transaction_open <- FALSE
      }
      rows_affected
    },
    error = function(error) {
      if (transaction_open) {
        try(.fabric_warehouse_rollback(connection), silent = TRUE)
        transaction_open <<- FALSE
      }
      .fabric_warehouse_write_abort(
        error,
        storage_target,
        credential,
        staging_path,
        keep_staging_on_failure,
        ambiguous = sql_started
      )
    }
  )
  tryCatch(
    .fabric_warehouse_disconnect(connection, backend),
    error = function(error) {
      rlang::warn(
        "The Warehouse load succeeded, but the SQL connection did not close cleanly",
        parent = error
      )
    }
  )
  connected <- FALSE

  # 5 Remove staging only after the Warehouse confirms success -----------------------------------

  staging_retained <- TRUE
  if (isTRUE(cleanup)) {
    removed <- .fabric_warehouse_remove_staging(storage_target, credential)
    staging_retained <- !removed
    if (!removed) {
      rlang::warn(paste0(
        "The Warehouse load succeeded, but staging cleanup failed; retained ",
        staging_path
      ))
    }
  }
  structure(
    list(
      workspace_id = destination$workspace_id,
      warehouse_id = destination$item_id,
      schema = schema,
      table = table,
      mode = mode,
      overwrite_method = overwrite_method,
      table_created = isTRUE(create_if_missing) && !table_exists,
      table_recreated = identical(mode, "Overwrite") &&
        identical(overwrite_method, "Drop") &&
        table_exists,
      rows = serialized$rows,
      bytes = serialized$total_bytes,
      file_count = serialized$file_count,
      files = vapply(storage_targets, `[[`, character(1), "path"),
      staging_workspace_id = stage$workspace_id,
      staging_lakehouse_id = stage$item_id,
      staging_path = staging_path,
      staging_retained = staging_retained,
      rows_affected = as.numeric(affected)
    ),
    class = "fabric_warehouse_write_result"
  )
}

# Resolve a Warehouse or Lakehouse record, enriching names and IDs only when
# the supplied record does not already contain the fields required downstream.
.fabric_warehouse_resolve_item <- function(
  value,
  workspace,
  expected_type,
  credential,
  api_base,
  api_base_supplied,
  allow_custom_endpoint,
  require_sql,
  argument
) {
  record <- fabric_as_record(value)
  if (!is.null(record)) {
    actual_type <- fabric_record_value(record, "type")
    if (
      is.null(actual_type) ||
        !identical(tolower(actual_type), tolower(expected_type))
    ) {
      rlang::abort(
        paste0(
          "`",
          argument,
          "` must be a ",
          expected_type,
          " item, not '",
          actual_type %||% "unknown",
          "'"
        ),
        class = c(
          "fabric_warehouse_target_error",
          "fabric_warehouse_error"
        )
      )
    }
  }
  workspace_record <- fabric_as_record(workspace)
  record_workspace_id <- fabric_record_value(record %||% list(), "workspaceId")
  supplied_workspace_id <- fabric_record_value(
    workspace_record %||% list(),
    "id",
    "workspaceId"
  )
  if (
    is.null(supplied_workspace_id) &&
      is.character(workspace) &&
      length(workspace) == 1L &&
      !is.na(workspace) &&
      fabric_is_guid(workspace)
  ) {
    supplied_workspace_id <- workspace
  }
  if (
    !is.null(record_workspace_id) &&
      !is.null(supplied_workspace_id) &&
      !identical(
        tolower(as.character(record_workspace_id)),
        tolower(as.character(supplied_workspace_id))
      )
  ) {
    rlang::abort(
      paste0("`", argument, "` belongs to a different workspace"),
      class = c(
        "fabric_warehouse_target_error",
        "fabric_warehouse_error"
      )
    )
  }
  workspace_value <- fabric_record_value(record %||% list(), "workspaceId") %||%
    fabric_record_value(workspace_record %||% list(), "id", "workspaceId") %||%
    workspace
  needs_lookup <- is.null(record) ||
    is.null(fabric_record_value(record, "id")) ||
    is.null(fabric_record_value(record, "workspaceId"))
  if (isTRUE(require_sql) && !needs_lookup) {
    needs_lookup <- inherits(
      try(fabric_sql_connection_info(record), silent = TRUE),
      "try-error"
    )
  }
  if (needs_lookup) {
    if (is.null(workspace_value)) {
      rlang::abort(
        paste0(
          "`workspace` is required to resolve `",
          argument,
          "`"
        ),
        class = c(
          "fabric_warehouse_target_error",
          "fabric_warehouse_error"
        )
      )
    }
    args <- list(
      workspace = workspace_value,
      item = value,
      type = expected_type,
      token = credential,
      allow_custom_endpoint = allow_custom_endpoint
    )
    if (isTRUE(api_base_supplied)) {
      args$api_base <- api_base
    }
    record <- do.call(fabric_item, args)
  }
  item_id <- fabric_record_value(record, "id")
  workspace_id <- fabric_record_value(record, "workspaceId")
  if (
    is.null(item_id) ||
      is.null(workspace_id) ||
      !fabric_is_guid(as.character(item_id)) ||
      !fabric_is_guid(as.character(workspace_id))
  ) {
    rlang::abort(
      paste0(
        "`",
        argument,
        "` must resolve to Fabric item and workspace GUIDs"
      ),
      class = c(
        "fabric_warehouse_target_error",
        "fabric_warehouse_error"
      )
    )
  }
  if (isTRUE(require_sql)) {
    fabric_sql_connection_info(record, target_type = "warehouse")
  }
  list(
    record = record,
    workspace_record = workspace_record,
    workspace_id = as.character(workspace_id),
    item_id = as.character(item_id)
  )
}

# Build one COPY statement with quoted identifiers and an internal OneLake URL.
.fabric_warehouse_copy_sql <- function(table_sql, columns, target) {
  column_sql <- paste(
    paste0(
      vapply(columns, .fabric_warehouse_quote_identifier, character(1)),
      " ",
      seq_along(columns)
    ),
    collapse = ", "
  )
  source <- .fabric_warehouse_source_url(target)
  paste0(
    "COPY INTO ",
    table_sql,
    " (",
    column_sql,
    ") ",
    "FROM ",
    .fabric_warehouse_string_literal(source),
    " WITH (FILE_TYPE = 'PARQUET')"
  )
}

# Build a CTAS statement that lets Fabric infer a missing table's schema from
# the exact Parquet parts created by this writer.
.fabric_warehouse_ctas_sql <- function(table_sql, columns, target) {
  projection <- paste(
    vapply(columns, .fabric_warehouse_quote_identifier, character(1)),
    collapse = ", "
  )
  paste0(
    "CREATE TABLE ",
    table_sql,
    " AS SELECT ",
    projection,
    " FROM OPENROWSET(BULK ",
    .fabric_warehouse_string_literal(.fabric_warehouse_source_url(target)),
    ", FORMAT = 'PARQUET') AS [source]"
  )
}

# Return the canonical OneLake wildcard used by both COPY and CTAS. Fabric's
# SQL engine authenticates to this public OneLake origin as the SQL identity.
.fabric_warehouse_source_url <- function(target) {
  folder <- target
  folder$path <- dirname(target$path)
  # Fabric's Warehouse contract requires the canonical OneLake origin even
  # when the client upload itself uses a workspace-private DFS endpoint.
  folder$dfs_base <- "https://onelake.dfs.fabric.microsoft.com"
  paste0(onelake_path_url(folder), "/*.parquet")
}

.fabric_warehouse_string_literal <- function(value, unicode = FALSE) {
  paste0(
    if (isTRUE(unicode)) "N" else "",
    "'",
    gsub("'", "''", value, fixed = TRUE),
    "'"
  )
}

# Quote one validated T-SQL identifier without consulting a live connection.
.fabric_warehouse_quote_identifier <- function(value) {
  paste0("[", gsub("]", "]]", value, fixed = TRUE), "]")
}

.fabric_warehouse_prepare_data <- function(data) {
  tryCatch(
    .fabric_parquet_prepare_data(
      data,
      caller = "fabric_warehouse_write_table()"
    ),
    fabric_arrow_error = function(error) {
      rlang::abort(
        conditionMessage(error),
        class = c(
          "fabric_warehouse_arrow_error",
          "fabric_warehouse_error"
        ),
        parent = error
      )
    }
  )
}

.fabric_warehouse_column_names <- function(value) {
  if (!length(value)) {
    rlang::abort("data must contain at least one column")
  }
  for (column in value) {
    .fabric_warehouse_identifier(column, "column")
  }
  normalized <- tolower(enc2utf8(value))
  if (anyDuplicated(normalized)) {
    rlang::abort("Column names must be unique ignoring case")
  }
  invisible(value)
}

.fabric_warehouse_identifier <- function(value, name) {
  if (
    !is.character(value) ||
      length(value) != 1L ||
      is.na(value) ||
      !nzchar(value) ||
      nchar(value) > 128L ||
      grepl("[[:cntrl:]]", value)
  ) {
    rlang::abort(paste0(
      "`",
      name,
      "` must be one non-empty identifier of at most 128 characters"
    ))
  }
  invisible(value)
}

.fabric_warehouse_choice <- function(value, choices, name) {
  if (length(value) > 1L) {
    value <- value[[1L]]
  }
  .fabric_warehouse_nonempty(value, name)
  index <- match(tolower(value), tolower(choices))
  if (is.na(index)) {
    rlang::abort(paste0(
      "`",
      name,
      "` must be one of ",
      paste(choices, collapse = ", ")
    ))
  }
  choices[[index]]
}

.fabric_warehouse_nonempty <- function(value, name) {
  if (
    !is.character(value) ||
      length(value) != 1L ||
      is.na(value) ||
      !nzchar(value)
  ) {
    rlang::abort(paste0("`", name, "` must be one non-empty string"))
  }
  invisible(value)
}

.fabric_warehouse_files_path <- function(value, name) {
  .fabric_warehouse_nonempty(value, name)
  normalized <- onelake_normalize_path(value)
  if (
    !startsWith(tolower(normalized), "files/") ||
      identical(tolower(normalized), "files/")
  ) {
    rlang::abort(paste0(
      "`",
      name,
      "` must begin with Files/ and name a folder"
    ))
  }
  normalized
}

.fabric_warehouse_staging_id <- function() {
  gsub("[^A-Za-z0-9_-]", "", basename(tempfile("load-")))
}

# Raise an actionable write error while preserving potentially active sources.
.fabric_warehouse_write_abort <- function(
  error,
  storage_target,
  credential,
  staging_path,
  keep_staging,
  ambiguous
) {
  retained <- TRUE
  if (!isTRUE(keep_staging) && !isTRUE(ambiguous)) {
    retained <- !.fabric_warehouse_remove_staging(storage_target, credential)
  }
  rlang::abort(
    paste0(
      "Fabric could not write the staged Parquet files to Warehouse. ",
      if (isTRUE(ambiguous)) {
        "SQL execution may have reached Fabric; "
      } else {
        ""
      },
      if (retained) {
        paste0("staging was retained at '", staging_path, "'.")
      } else {
        "The staging directory was removed."
      }
    ),
    class = c("fabric_warehouse_write_error", "fabric_warehouse_error"),
    parent = error,
    staging_path = staging_path,
    staging_retained = retained,
    ambiguous = isTRUE(ambiguous)
  )
}

.fabric_warehouse_remove_staging <- function(target, credential) {
  isTRUE(tryCatch(
    {
      directory <- target
      directory$path <- dirname(target$path)
      onelake_delete_target(
        directory,
        credential,
        recursive = TRUE,
        is_directory = TRUE
      )
      TRUE
    },
    error = function(error) FALSE
  ))
}

# Small database seams keep transaction behavior testable without Fabric.
.fabric_warehouse_connect <- function(...) fabric_sql_connect(...)

.fabric_warehouse_disconnect <- function(connection, backend) {
  if (identical(backend, "adbc")) {
    DBI::dbDisconnect(connection, force = TRUE)
  } else {
    DBI::dbDisconnect(connection)
  }
}

.fabric_warehouse_execute <- function(connection, sql) {
  DBI::dbExecute(connection, sql)
}

.fabric_warehouse_begin <- function(connection) DBI::dbBegin(connection)

.fabric_warehouse_commit <- function(connection) DBI::dbCommit(connection)

.fabric_warehouse_rollback <- function(connection) DBI::dbRollback(connection)

# Query catalog views instead of relying on backend-specific dbExistsTable()
# metadata support. The schema and table values are emitted only as escaped
# Unicode string literals.
.fabric_warehouse_table_exists <- function(connection, schema, table) {
  sql <- paste0(
    "SELECT CASE WHEN EXISTS (",
    "SELECT 1 FROM sys.tables AS [t] ",
    "INNER JOIN sys.schemas AS [s] ON [s].[schema_id] = [t].[schema_id] ",
    "WHERE [s].[name] = ",
    .fabric_warehouse_string_literal(schema, unicode = TRUE),
    " AND [t].[name] = ",
    .fabric_warehouse_string_literal(table, unicode = TRUE),
    ") THEN CAST(1 AS bit) ELSE CAST(0 AS bit) END AS [table_exists]"
  )
  value <- .fabric_warehouse_query(connection, sql)
  if (!is.data.frame(value) || nrow(value) != 1L || ncol(value) < 1L) {
    rlang::abort("Warehouse returned an invalid table-existence result")
  }
  flag <- value[[1L]][[1L]]
  if (
    length(flag) != 1L ||
      is.na(flag) ||
      !as.character(flag) %in% c("0", "1", "FALSE", "TRUE")
  ) {
    rlang::abort("Warehouse returned an invalid table-existence value")
  }
  as.character(flag) %in% c("1", "TRUE")
}

.fabric_warehouse_query <- function(connection, sql) {
  DBI::dbGetQuery(connection, sql)
}
