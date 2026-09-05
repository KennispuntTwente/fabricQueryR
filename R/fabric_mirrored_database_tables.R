#' Work with Microsoft Fabric mirrored database tables
#'
#' Discover schemas and Delta tables replicated into a Fabric mirrored
#' database, retrieve one table's detailed metadata, or read a table directly
#' from OneLake. The discovery helpers use the read-only OneLake table metadata
#' API; the reader uses the mirrored Delta log.
#'
#' @param mirrored_database Mirrored Database GUID, exact display name, or one
#'   object returned by [fabric_mirrored_databases()]. A discovered object is
#'   recommended because it contains workspace, OneLake, and SQL details.
#' @param workspace Workspace GUID, exact display name, or discovered workspace.
#'   Omit it when `mirrored_database` contains `workspaceId`.
#' @param schema Optional schema filter. The singular metadata and read helpers
#'   use a discovered default schema when available, otherwise `"dbo"`.
#' @param table Table name, or a one-row record containing `name` and optionally
#'   `schema`.
#' @param detail Whether table discovery should retrieve column metadata for
#'   every table.
#' @param page_size Optional maximum records requested per OneLake metadata
#'   page, from 1 to 100. All continuation tokens are followed.
#' @param tenant_id Entra tenant ID. Defaults to
#'   `FABRICQUERYR_TENANT_ID`.
#' @param client_id Entra application ID. Defaults to
#'   `FABRICQUERYR_CLIENT_ID`, then the Azure CLI application ID.
#' @param token Optional access token or audience-aware token-provider function.
#' @param storage_token Optional separate Azure Storage token or token-provider
#'   function. Supply it when `token` is fixed and item lookup is needed.
#' @param auth_args Additional sign-in options passed to `fabric_credential()`.
#' @param api_base Fabric REST API base used when an item name or GUID must be
#'   resolved. Most users should keep the default.
#' @param table_api_base OneLake Delta table API base URL. Most users should
#'   keep the default.
#' @param version Specific Delta table version to read, or `NULL` for latest.
#' @param verbose Whether to show authentication and read progress.
#' @param dfs_base OneLake service address. Most users should keep the default;
#'   a workspace-specific address discovered from Fabric is used when available.
#' @param columns Column names to return, or `NULL` for all columns.
#' @param limit Maximum number of rows to return, or `NULL` for all rows.
#' @param result `"tibble"` or `"arrow_stream"` for batch processing.
#'
#' @return `fabric_mirrored_database_schemas()` returns the same schema tibble
#'   as [fabric_lakehouse_schemas()]. The table metadata functions return the
#'   same table tibble as [fabric_warehouse_tables()]. The reader returns a
#'   tibble or a single-use `nanoarrow_array_stream`.
#'
#' @section SQL alternative:
#' Mirrored databases also expose a read-only SQL analytics endpoint. Pass a
#' discovered mirrored database object to [fabric_sql_tables()],
#' [fabric_sql_read_table()], or [fabric_sql_query()] when SQL permissions or
#' SQL views are required.
#'
#' @references
#' [Get Mirrored Database](https://learn.microsoft.com/en-us/rest/api/fabric/mirroreddatabase/items/get-mirrored-database)
#'
#' [Mirroring in Microsoft Fabric](https://learn.microsoft.com/en-us/fabric/mirroring/overview)
#'
#' [OneLake catalog table APIs](https://learn.microsoft.com/en-us/rest/api/fabric/articles/onelakecatalog/overview#explore-tables-within-an-item)
#' @name fabric_mirrored_database_tables
NULL

#' @rdname fabric_mirrored_database_tables
#' @export
#'
#' @examples
#' \dontrun{
#' workspace <- fabric_workspaces()[[1L]]
#' database <- fabric_mirrored_databases(workspace)[[1L]]
#'
#' schemas <- fabric_mirrored_database_schemas(database)
#' tables <- fabric_mirrored_database_tables(database)
#' rows <- fabric_mirrored_database_read_table(database, tables[1L, ], limit = 1000)
#' }
fabric_mirrored_database_schemas <- function(
  mirrored_database,
  workspace = NULL,
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
  .fabric_onelake_table_page_size(page_size)
  context <- .fabric_mirrored_database_catalog_context(
    mirrored_database,
    workspace,
    tenant_id,
    client_id,
    token,
    auth_args,
    api_base,
    !missing(api_base),
    table_api_base,
    storage_token
  )
  .fabric_onelake_schema_inventory(context, page_size)
}

#' @rdname fabric_mirrored_database_tables
#' @export
fabric_mirrored_database_tables <- function(
  mirrored_database,
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
  context <- .fabric_mirrored_database_catalog_context(
    mirrored_database,
    workspace,
    tenant_id,
    client_id,
    token,
    auth_args,
    api_base,
    !missing(api_base),
    table_api_base,
    storage_token
  )
  .fabric_onelake_table_inventory(
    workspace_id = context$workspace_id,
    item_id = context$item_id,
    schema = schema,
    detail = detail,
    page_size = page_size,
    credential = context$storage_credential,
    table_base = context$table_base,
    error_class = context$error_class
  )
}

#' @rdname fabric_mirrored_database_tables
#' @export
fabric_mirrored_database_table <- function(
  mirrored_database,
  table,
  workspace = NULL,
  schema = NULL,
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
  context <- .fabric_mirrored_database_catalog_context(
    mirrored_database,
    workspace,
    tenant_id,
    client_id,
    token,
    auth_args,
    api_base,
    !missing(api_base),
    table_api_base,
    storage_token
  )
  table_target <- .fabric_onelake_table_target(
    table,
    schema,
    context$default_schema
  )
  .fabric_onelake_table_detail(context, table_target)
}

#' @rdname fabric_mirrored_database_tables
#' @export
fabric_mirrored_database_read_table <- function(
  mirrored_database,
  table,
  workspace = NULL,
  schema = NULL,
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
  result = c("tibble", "arrow_stream"),
  api_base = .fabric_api_base,
  storage_token = NULL
) {
  schema_supplied <- !is.null(schema)
  context <- .fabric_mirrored_database_catalog_context(
    mirrored_database,
    workspace,
    tenant_id,
    client_id,
    token,
    auth_args,
    api_base,
    !missing(api_base),
    table_api_base = NULL,
    storage_token = storage_token
  )
  table_target <- .fabric_onelake_table_target(
    table,
    schema,
    context$default_schema
  )
  storage_target <- .fabric_onelake_table_storage_target(table)
  if (!schema_supplied && !is.null(storage_target)) {
    table_target <- storage_target
  }
  item_target <- if (!is.null(storage_target) && is.null(table_target$schema)) {
    context$item_id
  } else {
    context$item_target$record
  }
  fabric_onelake_read_delta_table(
    table_path = table_target$table,
    workspace_name = context$workspace_id,
    lakehouse_name = item_target,
    schema = if (!schema_supplied && !is.null(storage_target)) {
      table_target$schema %||% ""
    } else {
      table_target$schema
    },
    item_type = "MirroredDatabase",
    tenant_id = tenant_id,
    client_id = client_id,
    token = context$storage_credential,
    auth_args = list(),
    version = version,
    verbose = verbose,
    dfs_base = if (missing(dfs_base)) NULL else dfs_base,
    columns = columns,
    limit = limit,
    result = match.arg(result)
  )
}

.fabric_mirrored_database_catalog_context <- function(
  mirrored_database,
  workspace,
  tenant_id,
  client_id,
  token,
  auth_args,
  api_base,
  api_base_supplied,
  table_api_base,
  storage_token
) {
  .fabric_onelake_catalog_context(
    item = mirrored_database,
    workspace = workspace,
    item_type = "MirroredDatabase",
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args,
    api_base = api_base,
    api_base_supplied = api_base_supplied,
    table_api_base = table_api_base,
    argument = "mirrored_database",
    storage_token = storage_token
  )
}
