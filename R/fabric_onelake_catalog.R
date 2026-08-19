#' Discover OneLake schemas and individual tables
#'
#' These helpers expose the read-only OneLake Delta table metadata API for
#' Lakehouses and Warehouses. The schema helpers follow every metadata page.
#' The singular table helpers retrieve one table's full column metadata without
#' listing every table in every schema.
#'
#' @param lakehouse Lakehouse GUID, exact display name, or one Lakehouse record
#'   returned by [fabric_lakehouses()].
#' @param warehouse Warehouse GUID, exact display name, or one Warehouse record
#'   returned by [fabric_warehouses()].
#' @param workspace Workspace GUID, exact display name, or discovered workspace.
#'   Omit it when the item record contains `workspaceId`.
#' @param table Table name, or a record containing a `name`, `table`, or
#'   `displayName` field. A record can also supply `schema`.
#' @param schema Schema containing `table`. Defaults to the Lakehouse default
#'   schema when available, otherwise `"dbo"`.
#' @param page_size Optional maximum schemas requested per metadata page, from
#'   1 to 100. All continuation tokens are followed.
#' @param tenant_id Entra tenant ID. Defaults to
#'   `FABRICQUERYR_TENANT_ID`.
#' @param client_id Entra application ID. Defaults to
#'   `FABRICQUERYR_CLIENT_ID`, then the Azure CLI application ID.
#' @param token Optional access token or audience-aware token-provider function.
#' @param auth_args Additional sign-in options passed to `fabric_credential()`.
#' @param api_base Fabric REST API base used when an item name or GUID must be
#'   resolved. Most users should keep the default.
#' @param table_api_base OneLake Delta table API base URL. Most users should
#'   keep the default.
#'
#' @return The schema functions return a tibble with `name`, `catalog`,
#'   `full_name`, `comment`, `owner`, `schema_id`, timestamps, and the unmodified
#'   metadata record in `raw`. The table functions return one row with the same
#'   columns as [fabric_lakehouse_tables()] or [fabric_warehouse_tables()].
#'
#' @section Permissions:
#' The OneLake table API uses the Azure Storage token audience and requires
#' permission to read the item's tables through OneLake.
#'
#' @references
#' [Explore tables with OneLake catalog APIs](https://learn.microsoft.com/en-us/rest/api/fabric/articles/onelakecatalog/overview#explore-tables-within-an-item)
#'
#' [OneLake table APIs for Delta](https://learn.microsoft.com/en-us/fabric/onelake/table-apis/delta-table-apis-overview)
#' @name fabric_onelake_catalog
NULL

#' @rdname fabric_onelake_catalog
#' @export
#'
#' @examples
#' \dontrun{
#' workspace <- fabric_workspaces()[[1L]]
#' lakehouse <- fabric_lakehouses(workspace)[[1L]]
#'
#' schemas <- fabric_lakehouse_schemas(lakehouse)
#' orders <- fabric_lakehouse_table(lakehouse, "orders", schema = "dbo")
#' }
fabric_lakehouse_schemas <- function(
  lakehouse,
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
  table_api_base = .fabric_onelake_table_base
) {
  .fabric_onelake_table_page_size(page_size)
  context <- .fabric_onelake_catalog_context(
    item = lakehouse,
    workspace = workspace,
    item_type = "Lakehouse",
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args,
    api_base = api_base,
    api_base_supplied = !missing(api_base),
    table_api_base = table_api_base,
    argument = "lakehouse"
  )
  .fabric_onelake_schema_inventory(context, page_size)
}

#' @rdname fabric_onelake_catalog
#' @export
fabric_warehouse_schemas <- function(
  warehouse,
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
  table_api_base = .fabric_onelake_table_base
) {
  .fabric_onelake_table_page_size(page_size)
  context <- .fabric_onelake_catalog_context(
    item = warehouse,
    workspace = workspace,
    item_type = "Warehouse",
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args,
    api_base = api_base,
    api_base_supplied = !missing(api_base),
    table_api_base = table_api_base,
    argument = "warehouse"
  )
  .fabric_onelake_schema_inventory(context, page_size)
}

#' @rdname fabric_onelake_catalog
#' @export
fabric_lakehouse_table <- function(
  lakehouse,
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
  table_api_base = .fabric_onelake_table_base
) {
  context <- .fabric_onelake_catalog_context(
    item = lakehouse,
    workspace = workspace,
    item_type = "Lakehouse",
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args,
    api_base = api_base,
    api_base_supplied = !missing(api_base),
    table_api_base = table_api_base,
    argument = "lakehouse"
  )
  table_target <- .fabric_onelake_table_target(
    table,
    schema,
    default_schema = context$default_schema
  )
  fabric_records <- .fabric_lakehouse_fabric_inventory(
    context$item_target,
    context$credential,
    page_size = NULL
  )
  .fabric_onelake_table_detail(context, table_target, fabric_records)
}

#' @rdname fabric_onelake_catalog
#' @export
fabric_warehouse_table <- function(
  warehouse,
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
  table_api_base = .fabric_onelake_table_base
) {
  context <- .fabric_onelake_catalog_context(
    item = warehouse,
    workspace = workspace,
    item_type = "Warehouse",
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args,
    api_base = api_base,
    api_base_supplied = !missing(api_base),
    table_api_base = table_api_base,
    argument = "warehouse"
  )
  table_target <- .fabric_onelake_table_target(
    table,
    schema,
    default_schema = "dbo"
  )
  .fabric_onelake_table_detail(context, table_target)
}

.fabric_onelake_catalog_context <- function(
  item,
  workspace,
  item_type,
  tenant_id,
  client_id,
  token,
  auth_args,
  api_base,
  api_base_supplied,
  table_api_base,
  argument
) {
  domain <- switch(
    item_type,
    Lakehouse = "lakehouse",
    Warehouse = "warehouse",
    MirroredDatabase = "mirrored_database"
  )
  error_class <- c(
    paste0("fabric_", domain, "_protocol_error"),
    paste0("fabric_", domain, "_error")
  )
  base <- fabric_api_base(api_base)
  table_base <- if (is.null(table_api_base)) {
    NULL
  } else {
    .fabric_onelake_table_api_base(
      table_api_base,
      error_class = error_class
    )
  }
  credential <- fabric_credential(
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args
  )
  item_target <- if (identical(item_type, "Lakehouse")) {
    .fabric_lakehouse_target(
      item,
      workspace,
      credential,
      base,
      use_workspace_endpoint = !api_base_supplied
    )
  } else {
    .fabric_warehouse_resolve_item(
      item,
      workspace,
      expected_type = item_type,
      credential = credential,
      api_base = base,
      api_base_supplied = api_base_supplied,
      require_sql = FALSE,
      argument = argument
    )
  }
  list(
    workspace_id = item_target$workspace_id,
    item_id = item_target$lakehouse_id %||% item_target$item_id,
    default_schema = item_target$default_schema %||%
      fabric_record_value(
        item_target$record %||% list(),
        "default_schema",
        "defaultSchema"
      ),
    table_base = table_base,
    credential = credential,
    item_target = item_target,
    error_class = error_class
  )
}

.fabric_onelake_catalog_url <- function(context) {
  paste0(
    context$table_base,
    "/",
    onelake_encode_path(context$workspace_id, context$item_id),
    "/api/2.1/unity-catalog"
  )
}

.fabric_onelake_schema_inventory <- function(context, page_size) {
  records <- .fabric_onelake_table_pages(
    paste0(.fabric_onelake_catalog_url(context), "/schemas"),
    field = "schemas",
    query = list(catalog_name = context$item_id),
    credential = context$credential,
    page_size = page_size,
    error_class = context$error_class
  )
  rows <- lapply(
    records,
    .fabric_onelake_schema_row,
    item_id = context$item_id,
    error_class = context$error_class
  )
  .fabric_onelake_schema_tibble(rows)
}

.fabric_onelake_schema_row <- function(record, item_id, error_class) {
  name <- record$name
  if (
    !is.character(name) ||
      length(name) != 1L ||
      is.na(name) ||
      !nzchar(name)
  ) {
    .fabric_abort(
      "OneLake returned schema metadata without one non-empty name",
      class = error_class
    )
  }
  catalog <- as.character(record$catalog_name %||% item_id)
  list(
    name = name,
    catalog = catalog,
    full_name = as.character(
      record$full_name %||% paste(catalog, name, sep = ".")
    ),
    comment = as.character(record$comment %||% NA_character_),
    owner = as.character(record$owner %||% NA_character_),
    schema_id = as.character(record$schema_id %||% NA_character_),
    created_at = .fabric_onelake_epoch_ms(record$created_at),
    updated_at = .fabric_onelake_epoch_ms(record$updated_at),
    raw = record
  )
}

.fabric_onelake_schema_tibble <- function(rows) {
  empty <- tibble::tibble(
    name = character(),
    catalog = character(),
    full_name = character(),
    comment = character(),
    owner = character(),
    schema_id = character(),
    created_at = as.POSIXct(character(), tz = "UTC"),
    updated_at = as.POSIXct(character(), tz = "UTC"),
    raw = list()
  )
  if (!length(rows)) {
    return(empty)
  }
  tibble::tibble(
    name = vapply(rows, `[[`, character(1), "name"),
    catalog = vapply(rows, `[[`, character(1), "catalog"),
    full_name = vapply(rows, `[[`, character(1), "full_name"),
    comment = vapply(rows, `[[`, character(1), "comment"),
    owner = vapply(rows, `[[`, character(1), "owner"),
    schema_id = vapply(rows, `[[`, character(1), "schema_id"),
    created_at = as.POSIXct(
      vapply(rows, function(row) as.numeric(row$created_at), numeric(1)),
      origin = "1970-01-01",
      tz = "UTC"
    ),
    updated_at = as.POSIXct(
      vapply(rows, function(row) as.numeric(row$updated_at), numeric(1)),
      origin = "1970-01-01",
      tz = "UTC"
    ),
    raw = lapply(rows, `[[`, "raw")
  )
}

.fabric_onelake_table_target <- function(table, schema, default_schema) {
  record <- fabric_as_record(table)
  if (!is.null(record)) {
    table <- fabric_record_value(record, "name", "table", "displayName")
    schema <- schema %||% fabric_record_value(record, "schema", "schema_name")
  }
  .fabric_lakehouse_nonempty(table, "table")
  schema <- schema %||% default_schema %||% "dbo"
  .fabric_lakehouse_nonempty(schema, "schema")
  list(table = table, schema = schema)
}

.fabric_onelake_table_detail <- function(
  context,
  table_target,
  fabric_records = list()
) {
  full_name <- paste(
    context$item_id,
    table_target$schema,
    table_target$table,
    sep = "."
  )
  request <- httr2::req_url_query(
    httr2::request(paste0(
      .fabric_onelake_catalog_url(context),
      "/tables/",
      utils::URLencode(full_name, reserved = TRUE)
    )),
    catalog_name = context$item_id,
    schema_name = table_target$schema
  )
  record <- .httr2_json(
    request,
    simplifyVector = FALSE,
    credential = context$credential,
    audience = .fabric_audience$storage
  )
  row <- .fabric_onelake_table_row(
    record,
    schema_name = table_target$schema,
    schema_record = list(name = table_target$schema),
    fabric_record = .fabric_lakehouse_match_fabric_table(
      record,
      table_target$schema,
      fabric_records
    )
  )
  .fabric_onelake_table_tibble(list(row))
}
