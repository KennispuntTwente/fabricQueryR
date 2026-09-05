#' Discover Microsoft Fabric Warehouse tables
#'
#' Lists schemas and Delta-backed tables in a Fabric Warehouse through the
#' read-only OneLake table metadata API. Set `detail = TRUE` to retrieve column
#' metadata for every table. A returned row can be passed directly to
#' [fabric_warehouse_read_table()].
#'
#' @param warehouse Warehouse GUID, exact display name, or one Warehouse object
#'   returned by [fabric_warehouses()]. A discovered object is recommended
#'   because it contains the workspace and item IDs.
#' @param workspace Workspace GUID, exact display name, or discovered workspace.
#'   Omit it when `warehouse` is an object containing `workspaceId`.
#' @param schema Optional Warehouse schema. When omitted, every schema is
#'   listed.
#' @param detail Whether to retrieve per-table column metadata.
#' @param page_size Optional maximum records requested per OneLake metadata
#'   page, from 1 to 100. All continuation tokens are followed.
#' @param tenant_id Entra tenant ID. Defaults to
#'   `FABRICQUERYR_TENANT_ID`.
#' @param client_id Entra application ID. Defaults to
#'   `FABRICQUERYR_CLIENT_ID`, then the Azure CLI application ID.
#' @param token Optional access token or audience-aware token-provider function.
#'   Warehouse lookup can require a Fabric-audience token; table metadata uses
#'   a Storage-audience token.
#' @param storage_token Optional separate Azure Storage token or token-provider
#'   function. Supply it when `token` is fixed and Warehouse lookup is needed.
#' @param auth_args Additional sign-in options passed to
#'   [AzureAuth::get_azure_token()] when no token source is supplied.
#' @param api_base Fabric REST API base URL used when a Warehouse name or GUID
#'   must be resolved. Most users should keep the default.
#' @param table_api_base OneLake Delta table API base URL. Most users should
#'   keep the default.
#'
#' @return A tibble with table `name`, `schema`, `full_name`, `type`, `format`,
#'   `location`, timestamps, list-column `columns`, `schema_metadata`, and the
#'   unmodified OneLake `raw` record. `fabric_raw` is an empty list-column
#'   because Fabric does not expose a Warehouse counterpart to the Lakehouse
#'   List Tables REST route. Unknown future OneLake metadata remains available
#'   in `raw`.
#'
#' @section Permissions:
#' The OneLake table API uses the Azure Storage token audience and requires the
#' calling identity to have permission to read tables in the Warehouse through
#' OneLake. This permission is separate from Warehouse T-SQL `ReadData`
#' permission.
#'
#' @references
#' [Explore tables with OneLake catalog APIs](https://learn.microsoft.com/en-us/rest/api/fabric/articles/onelakecatalog/overview#explore-tables-within-an-item)
#'
#' [OneLake table APIs for Delta](https://learn.microsoft.com/en-us/fabric/onelake/table-apis/delta-table-apis-overview)
#'
#' [Warehouse permissions](https://learn.microsoft.com/en-us/fabric/data-warehouse/share-warehouse-manage-permissions)
#' @export
#'
#' @examples
#' \dontrun{
#' workspace <- fabric_workspaces()[[1L]]
#' warehouse <- fabric_warehouses(workspace)[[1L]]
#'
#' tables <- fabric_warehouse_tables(warehouse)
#' orders <- fabric_warehouse_read_table(warehouse, tables[1L, ])
#' }
fabric_warehouse_tables <- function(
  warehouse,
  workspace = NULL,
  schema = NULL,
  detail = TRUE,
  page_size = NULL,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  token = NULL,
  auth_args = list(),
  api_base = .fabric_api_base,
  table_api_base = .fabric_onelake_table_base,
  storage_token = NULL
) {
  .fabric_operation_logical(detail, "detail")
  .fabric_onelake_table_page_size(page_size)
  if (!is.null(schema)) {
    .fabric_lakehouse_nonempty(schema, "schema")
  }

  api_base_supplied <- !missing(api_base)
  base <- fabric_api_base(api_base)
  table_base <- .fabric_onelake_table_api_base(
    table_api_base,
    error_class = c(
      "fabric_warehouse_endpoint_error",
      "fabric_warehouse_error"
    )
  )
  credential <- fabric_credential(
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args
  )
  storage_credential <- if (is.null(storage_token)) {
    credential
  } else {
    fabric_credential(token = storage_token)
  }
  target <- .fabric_warehouse_resolve_item(
    warehouse,
    workspace,
    expected_type = "Warehouse",
    credential = credential,
    api_base = base,
    api_base_supplied = api_base_supplied,
    require_sql = FALSE,
    argument = "warehouse"
  )

  .fabric_onelake_table_inventory(
    workspace_id = target$workspace_id,
    item_id = target$item_id,
    schema = schema,
    detail = detail,
    page_size = page_size,
    credential = storage_credential,
    table_base = table_base,
    error_class = c(
      "fabric_warehouse_protocol_error",
      "fabric_warehouse_error"
    )
  )
}

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
#' result path that avoids conversion to an R data frame. The current result path
#' through 'DBI' and 'adbi' may fetch the complete result before returning the
#' stream, so use a selective query or `limit` when the result may exceed memory.
#' The external ADBC `mssql` driver must be installed.
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
#' # Discover the Warehouse instead of copying its SQL connection details
#' workspace <- fabric_workspaces()[[1L]]
#' warehouse <- fabric_warehouses(workspace)[[1L]]
#'
#' # Use 'DBI' metadata to discover an existing table in that Warehouse
#' con <- fabric_sql_connect(warehouse)
#' tables <- DBI::dbListTables(con)
#' DBI::dbDisconnect(con)
#' table <- tables[[1L]]
#'
#' # Read a bounded selection into a tibble
#' orders <- fabric_warehouse_read_table(
#'   warehouse,
#'   table,
#'   limit = 1000
#' )
#'
#' # Keep the result Arrow-native rather than converting it to a data frame
#' stream <- fabric_warehouse_read_table(
#'   warehouse,
#'   table,
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
  verbose = TRUE,
  timeout = 30L,
  max_tries = 3L,
  retry_delay = 5,
  sql_token = NULL
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
  base <- fabric_api_base(api_base)
  credential <- fabric_credential(
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args
  )
  sql_credential <- if (is.null(sql_token)) {
    credential
  } else {
    fabric_credential(token = sql_token)
  }
  destination <- .fabric_warehouse_resolve_item(
    warehouse,
    workspace,
    expected_type = "Warehouse",
    credential = credential,
    api_base = base,
    api_base_supplied = api_base_supplied,
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
    token = sql_credential,
    auth_args = list(),
    timeout = timeout,
    read_only = TRUE,
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
#' @param warehouse A Warehouse object returned by [fabric_warehouses()] or
#'   [fabric_item()], or its name or GUID when `workspace` is supplied.
#' @param table Destination table name.
#' @param data A data frame, tibble, Arrow Table, RecordBatch, Dataset, Scanner,
#'   RecordBatchReader, Arrow 'dplyr' query, or Arrow-compatible array stream.
#' @param staging_lakehouse A Lakehouse object returned by
#'   [fabric_lakehouses()] or [fabric_item()], or its name or GUID. Fabric does
#'   not support a Warehouse item as the OneLake source of `COPY INTO`, so a
#'   Lakehouse staging item is required.
#' @param workspace Workspace name, GUID, or discovery object containing
#'   `warehouse`. May be omitted when `warehouse` is a discovery object.
#' @param staging_workspace Workspace containing `staging_lakehouse`. Defaults
#'   to the Warehouse workspace. May be omitted when `staging_lakehouse` is a
#'   discovery object.
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
#' @param storage_token Optional separate Azure Storage token or token-provider
#'   function. Supply it when `token` is fixed rather than audience-aware.
#' @param sql_token Optional separate Azure SQL token or token-provider
#'   function. Supply it when `token` is fixed rather than audience-aware.
#' @param api_base Fabric REST API base used when a Warehouse or staging
#'   Lakehouse name or GUID must be discovered.
#' @inheritParams fabric_sql_connect
#' @inheritParams fabric_onelake_upload
#'
#' @return A `fabric_warehouse_write_result` list containing destination and
#'   staging identifiers, row and byte counts, part paths, and cleanup state.
#' @details
#' Existing-table writes map input fields by ordinal position to quoted
#' destination columns whose names must exactly match the names in `data`,
#' including letter case. The writer checks the Warehouse catalog before any
#' destructive SQL is issued. With
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
#' statement. That identity needs read access to the staged Lakehouse files and
#' the Warehouse T-SQL permissions required by the selected mode, including the
#' applicable bulk-load, DML, and DDL permissions. The identity used to stage
#' and clean up files also needs OneLake write access to the staging folder.
#' Contributor access to both workspaces is a simple sufficient setup, but it
#' is not required when equivalent granular item, OneLake, and T-SQL permissions
#' are granted.
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
#'
#' [OneLake security access-control model](https://learn.microsoft.com/en-us/fabric/onelake/security/data-access-control-model)
#'
#' [Warehouse permissions](https://learn.microsoft.com/en-us/fabric/data-warehouse/share-warehouse-manage-permissions)
#' @examples
#' \dontrun{
#' # Discover both the destination Warehouse and staging Lakehouse
#' workspace <- fabric_workspaces()[[1L]]
#' warehouse <- fabric_warehouses(workspace)[[1L]]
#' staging <- fabric_lakehouses(workspace)[[1L]]
#'
#' # Upload through OneLake staging and create a new Warehouse table
#' fabric_warehouse_write_table(
#'   warehouse,
#'   "orders_from_r",
#'   data.frame(id = 1:3, amount = c(10, 20, 30)),
#'   staging_lakehouse = staging,
#'   create_if_missing = TRUE
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
  verbose = TRUE,
  storage_token = NULL,
  sql_token = NULL
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
  base <- fabric_api_base(api_base)
  credential <- fabric_credential(
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args
  )
  storage_credential <- fabric_service_credential(
    credential,
    storage_token,
    "storage_token",
    "fabric_warehouse_write_table()"
  )
  sql_credential <- fabric_service_credential(
    credential,
    sql_token,
    "sql_token",
    "fabric_warehouse_write_table()"
  )
  destination <- .fabric_warehouse_resolve_item(
    warehouse,
    workspace,
    expected_type = "Warehouse",
    credential = credential,
    api_base = base,
    api_base_supplied = api_base_supplied,
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
        storage_credential,
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
        storage_credential,
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
      token = sql_credential,
      read_only = FALSE,
      verbose = verbose
    ),
    error = function(error) {
      .fabric_warehouse_write_abort(
        error,
        storage_target,
        storage_credential,
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
          storage_credential,
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
  if (!create_with_ctas) {
    tryCatch(
      .fabric_warehouse_validate_destination_columns(
        connection,
        schema,
        table,
        prepared$names
      ),
      error = function(error) {
        .fabric_warehouse_write_abort(
          error,
          storage_target,
          storage_credential,
          staging_path,
          keep_staging_on_failure,
          ambiguous = FALSE
        )
      }
    )
  }
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
        storage_credential,
        staging_path,
        keep_staging_on_failure,
        ambiguous = sql_started
      )
    }
  )
  tryCatch(
    .fabric_warehouse_disconnect(connection, backend),
    error = function(error) {
      .fabric_warn(
        "The Warehouse load succeeded, but the SQL connection did not close cleanly",
        parent = error
      )
    }
  )
  connected <- FALSE

  # 5 Remove staging only after the Warehouse confirms success -----------------------------------

  staging_retained <- TRUE
  if (isTRUE(cleanup)) {
    removed <- .fabric_warehouse_remove_staging(
      storage_target,
      storage_credential
    )
    staging_retained <- !removed
    if (!removed) {
      .fabric_warn(
        c(
          "Staging cleanup failed after the Warehouse table load succeeded",
          "i" = "Staged files remain at {.path {staging_path}}"
        ),
        .format = TRUE
      )
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
# the supplied record does not already contain the fields required downstream
.fabric_warehouse_resolve_item <- function(
  value,
  workspace,
  expected_type,
  credential,
  api_base,
  api_base_supplied,
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
      .fabric_abort(
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
    .fabric_abort(
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
      .fabric_abort(
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
      token = credential
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
    .fabric_abort(
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

# Build one COPY statement with quoted identifiers and an internal OneLake URL
.fabric_warehouse_copy_sql <- function(table_sql, columns, target) {
  column_sql <- paste(
    vapply(columns, .fabric_warehouse_quote_identifier, character(1)),
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
# the exact Parquet parts created by this writer
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
# SQL engine authenticates to this public OneLake origin as the SQL identity
.fabric_warehouse_source_url <- function(target) {
  folder <- target
  folder$path <- dirname(target$path)
  # Fabric's Warehouse contract requires the canonical OneLake origin even
  # when the client upload itself uses a workspace-private DFS endpoint
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

# Quote one validated T-SQL identifier without consulting a live connection
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
      .fabric_abort(
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
    .fabric_abort("data must contain at least one column")
  }
  for (column in value) {
    .fabric_warehouse_identifier(column, "column")
  }
  normalized <- enc2utf8(value)
  if (anyDuplicated(normalized)) {
    .fabric_abort("Column names must be unique")
  }
  invisible(value)
}

.fabric_warehouse_identifier <- function(value, name) {
  warehouse_object <- name %in% c("schema", "table")
  if (
    !is.character(value) ||
      length(value) != 1L ||
      is.na(value) ||
      !nzchar(value) ||
      nchar(value) > 128L ||
      grepl("[[:cntrl:]]", value) ||
      (warehouse_object && (grepl("[/\\\\]", value) || endsWith(value, ".")))
  ) {
    .fabric_abort(paste0(
      "`",
      name,
      "` must be one non-empty Fabric Warehouse identifier of at most ",
      "128 characters",
      if (warehouse_object) " without / or \\ and not ending in ." else ""
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
    .fabric_abort(paste0(
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
    .fabric_abort(paste0("`", name, "` must be one non-empty string"))
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
    .fabric_abort(paste0(
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

# Raise an actionable write error while preserving potentially active sources
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
  .fabric_abort(
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

# Small database seams keep transaction behavior testable without Fabric
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
# Unicode string literals
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
    .fabric_abort("Warehouse returned an invalid table-existence result")
  }
  flag <- value[[1L]][[1L]]
  if (
    length(flag) != 1L ||
      is.na(flag) ||
      !as.character(flag) %in% c("0", "1", "FALSE", "TRUE")
  ) {
    .fabric_abort("Warehouse returned an invalid table-existence value")
  }
  as.character(flag) %in% c("1", "TRUE")
}

# Require exact destination names before COPY INTO. SQL identifier lookup can
# be case-insensitive while Parquet field mapping is positional, so accepting a
# case-folded match can silently associate values with the wrong field.
.fabric_warehouse_validate_destination_columns <- function(
  connection,
  schema,
  table,
  columns
) {
  sql <- paste0(
    "SELECT [c].[name] AS [column_name] ",
    "FROM sys.columns AS [c] ",
    "INNER JOIN sys.tables AS [t] ON [t].[object_id] = [c].[object_id] ",
    "INNER JOIN sys.schemas AS [s] ON [s].[schema_id] = [t].[schema_id] ",
    "WHERE [s].[name] = ",
    .fabric_warehouse_string_literal(schema, unicode = TRUE),
    " AND [t].[name] = ",
    .fabric_warehouse_string_literal(table, unicode = TRUE),
    " ORDER BY [c].[column_id]"
  )
  value <- .fabric_warehouse_query(connection, sql)
  if (
    !is.data.frame(value) ||
      ncol(value) < 1L ||
      !is.character(value[[1L]]) ||
      anyNA(value[[1L]])
  ) {
    .fabric_abort(
      "Warehouse returned invalid destination-column metadata",
      class = c("fabric_warehouse_column_error", "fabric_warehouse_error")
    )
  }
  destination <- value[[1L]]
  missing <- columns[!columns %in% destination]
  if (length(missing) > 0L) {
    case_matches <- destination[
      tolower(destination) %in% tolower(missing)
    ]
    detail <- if (length(case_matches) > 0L) {
      paste0(
        " Destination names are case-sensitive for this operation; catalog ",
        "matches include: ",
        paste(case_matches, collapse = ", "),
        "."
      )
    } else {
      ""
    }
    .fabric_abort(
      paste0(
        "Input column names must exactly match destination columns. Missing: ",
        paste(missing, collapse = ", "),
        ".",
        detail
      ),
      class = c("fabric_warehouse_column_error", "fabric_warehouse_error")
    )
  }
  invisible(destination)
}

.fabric_warehouse_query <- function(connection, sql) {
  DBI::dbGetQuery(connection, sql)
}
