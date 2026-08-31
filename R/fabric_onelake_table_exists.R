.fabric_onelake_table_origin <- "https://onelake.table.fabric.microsoft.com"

#' Check whether a OneLake schema or table exists
#'
#' Uses the documented metadata-only `HEAD` operations from either the Delta
#' Unity Catalog-compatible API or the Iceberg REST Catalog API. These helpers
#' avoid downloading a complete schema or table record when only existence is
#' needed.
#'
#' @param item Fabric data item GUID, exact display name, or a discovered item
#'   object. An object containing `workspaceId` avoids workspace discovery.
#' @param schema Schema or Iceberg namespace name. For a table, `NULL` uses the
#'   item's discovered default schema and otherwise falls back to `"dbo"`.
#' @param table Table name or a record containing `name`, `table`, or
#'   `displayName`. A record can also supply its schema.
#' @param workspace Workspace GUID, exact display name, or discovered workspace.
#'   Omit it when `item` contains `workspaceId`.
#' @param item_type Optional item type used to disambiguate an item supplied by
#'   name.
#' @param protocol OneLake table metadata protocol: `"delta"` or `"iceberg"`.
#' @param table_api_base OneLake table API HTTPS origin, or a protocol-specific
#'   base ending in `/delta` or `/iceberg`. Most users should keep the default.
#' @inheritParams fabric_workspaces
#'
#' @return One logical value: `TRUE` for a successful `HEAD` response and
#'   `FALSE` for HTTP 404. Authentication, permission, throttling, and service
#'   errors are not converted to `FALSE`.
#' @details
#' The table APIs use the Azure Storage token audience and require permission
#' to read the item's tables through OneLake. If name-based item discovery is
#' necessary, use the package's normal audience-aware sign-in or token provider
#' because the Fabric Core and Storage audiences are both involved.
#'
#' Iceberg requests first call `GET /iceberg/v1/config` with the item's
#' workspace/item warehouse identity and validate the returned prefix before
#' issuing `HEAD`. Delta schema requests include `catalog_name` only when the
#' schema name contains dots. Delta table requests include `catalog_name` and
#' `schema_name`.
#' @references
#' [OneLake table APIs for Delta](https://learn.microsoft.com/en-us/fabric/onelake/table-apis/delta-table-apis-overview)
#'
#' [OneLake table APIs for Iceberg](https://learn.microsoft.com/en-us/fabric/onelake/table-apis/iceberg-table-apis-overview)
#' @examples
#' \dontrun{
#' lakehouse <- fabric_lakehouses(fabric_workspaces()[[1L]])[[1L]]
#'
#' fabric_onelake_schema_exists(lakehouse, "dbo")
#' fabric_onelake_table_exists(lakehouse, "orders", schema = "dbo")
#' fabric_onelake_table_exists(
#'   lakehouse,
#'   "orders",
#'   schema = "dbo",
#'   protocol = "iceberg"
#' )
#' }
#' @export
fabric_onelake_schema_exists <- function(
  item,
  schema,
  workspace = NULL,
  item_type = NULL,
  protocol = c("delta", "iceberg"),
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  token = NULL,
  auth_args = list(),
  api_base = .fabric_api_base,
  table_api_base = .fabric_onelake_table_origin
) {
  .fabric_onelake_table_identifier(schema, "schema")
  context <- .fabric_onelake_exists_context(
    item = item,
    workspace = workspace,
    item_type = item_type,
    protocol = protocol,
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args,
    api_base = api_base,
    api_base_supplied = !missing(api_base),
    table_api_base = table_api_base
  )
  url <- .fabric_onelake_exists_url(context, schema = schema)
  .fabric_onelake_exists_request(url, context$credential)
}

#' @rdname fabric_onelake_schema_exists
#' @export
fabric_onelake_table_exists <- function(
  item,
  table,
  workspace = NULL,
  schema = NULL,
  item_type = NULL,
  protocol = c("delta", "iceberg"),
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  token = NULL,
  auth_args = list(),
  api_base = .fabric_api_base,
  table_api_base = .fabric_onelake_table_origin
) {
  item_record <- fabric_as_record(item)
  table_target <- .fabric_onelake_table_target(
    table,
    schema,
    default_schema = fabric_record_value(
      item_record %||% list(),
      "defaultSchema",
      "default_schema"
    )
  )
  .fabric_onelake_table_identifier(table_target$schema, "schema")
  .fabric_onelake_table_identifier(table_target$table, "table")
  context <- .fabric_onelake_exists_context(
    item = item,
    workspace = workspace,
    item_type = item_type,
    protocol = protocol,
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args,
    api_base = api_base,
    api_base_supplied = !missing(api_base),
    table_api_base = table_api_base
  )
  url <- .fabric_onelake_exists_url(
    context,
    schema = table_target$schema,
    table = table_target$table
  )
  .fabric_onelake_exists_request(url, context$credential)
}

.fabric_onelake_exists_context <- function(
  item,
  workspace,
  item_type,
  protocol,
  tenant_id,
  client_id,
  token,
  auth_args,
  api_base,
  api_base_supplied,
  table_api_base
) {
  protocol <- match.arg(protocol, c("delta", "iceberg"))
  base <- fabric_api_base(api_base)
  credential <- fabric_credential(
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args
  )
  target <- .fabric_job_target(
    item,
    workspace,
    item_type,
    credential,
    base,
    use_workspace_endpoint = !api_base_supplied
  )
  protocol_base <- .fabric_onelake_protocol_base(table_api_base, protocol)
  context <- list(
    workspace_id = target$workspace_id,
    item_id = target$item_id,
    protocol = protocol,
    protocol_base = protocol_base,
    credential = credential
  )
  if (identical(protocol, "iceberg")) {
    context$iceberg_prefix <- .fabric_onelake_iceberg_prefix(context)
  }
  context
}

.fabric_onelake_protocol_base <- function(value, protocol) {
  .fabric_lakehouse_nonempty(value, "table_api_base")
  endpoint <- sub("/+$", "", trimws(value))
  parsed <- try(httr2::url_parse(endpoint), silent = TRUE)
  path <- if (inherits(parsed, "try-error")) {
    ""
  } else {
    tolower(sub("/+$", "", parsed$path %||% ""))
  }
  clean <- !inherits(parsed, "try-error") &&
    identical(tolower(parsed$scheme %||% ""), "https") &&
    nzchar(parsed$hostname %||% "") &&
    !nzchar(parsed$username %||% "") &&
    !nzchar(parsed$password %||% "") &&
    (parsed$port %||% "") %in% c("", "443") &&
    path %in% c("", paste0("/", protocol)) &&
    length(parsed$query %||% list()) == 0L &&
    !nzchar(parsed$fragment %||% "")
  if (!clean) {
    .fabric_abort(
      paste0(
        "`table_api_base` must be an HTTPS origin or end in /",
        protocol
      ),
      class = c("fabric_onelake_table_protocol_error", "fabric_onelake_error")
    )
  }
  if (identical(path, paste0("/", protocol))) {
    sub(
      paste0("/", protocol, "$"),
      paste0("/", protocol),
      endpoint,
      ignore.case = TRUE
    )
  } else {
    paste0(endpoint, "/", protocol)
  }
}

.fabric_onelake_iceberg_prefix <- function(context) {
  request <- httr2::req_url_query(
    httr2::request(paste0(context$protocol_base, "/v1/config")),
    warehouse = paste(
      context$workspace_id,
      context$item_id,
      sep = "/"
    )
  )
  config <- .httr2_json(
    request,
    simplifyVector = FALSE,
    credential = context$credential,
    audience = .fabric_audience$storage
  )
  prefix_positions <- if (
    is.list(config) &&
      is.list(config$overrides) &&
      !is.null(names(config$overrides))
  ) {
    which(names(config$overrides) == "prefix")
  } else {
    integer()
  }
  prefix <- if (length(prefix_positions) == 1L) {
    config$overrides[[prefix_positions]]
  } else {
    NULL
  }
  pieces <- if (
    is.character(prefix) &&
      length(prefix) == 1L &&
      !is.na(prefix) &&
      nzchar(prefix)
  ) {
    strsplit(prefix, "/", fixed = TRUE)[[1L]]
  } else {
    character()
  }
  valid <- length(pieces) > 0L &&
    all(nzchar(pieces)) &&
    !any(pieces %in% c(".", "..")) &&
    !grepl("[\\\\?#[:cntrl:]]", prefix)
  if (!valid) {
    .fabric_abort(
      "OneLake returned an invalid Iceberg catalog prefix",
      class = c("fabric_onelake_table_protocol_error", "fabric_onelake_error")
    )
  }
  onelake_encode_path(pieces)
}

.fabric_onelake_table_identifier <- function(value, name) {
  .fabric_lakehouse_nonempty(value, name)
  if (value %in% c(".", "..") || grepl("[/\\\\?#[:cntrl:]]", value)) {
    .fabric_abort(
      paste0("`", name, "` must be one safe table metadata path segment"),
      class = c("fabric_onelake_table_protocol_error", "fabric_onelake_error")
    )
  }
  invisible(value)
}

.fabric_onelake_exists_url <- function(context, schema, table = NULL) {
  if (identical(context$protocol, "delta")) {
    collection <- if (is.null(table)) "schemas" else "tables"
    name <- table %||% schema
    request <- httr2::request(paste0(
      context$protocol_base,
      "/",
      onelake_encode_path(context$workspace_id, context$item_id),
      "/api/2.1/unity-catalog/",
      collection,
      "/",
      onelake_encode_path(name)
    ))
    query <- list()
    if (!is.null(table) || grepl(".", schema, fixed = TRUE)) {
      query$catalog_name <- context$item_id
    }
    if (!is.null(table)) {
      query$schema_name <- schema
    }
    return(do.call(httr2::req_url_query, c(list(request), query))$url)
  }
  url <- paste0(
    context$protocol_base,
    "/v1/",
    context$iceberg_prefix,
    "/namespaces/",
    onelake_encode_path(schema)
  )
  if (!is.null(table)) {
    url <- paste0(url, "/tables/", onelake_encode_path(table))
  }
  url
}

.fabric_onelake_exists_request <- function(url, credential) {
  response <- .httr2_perform(
    httr2::request(url) |>
      httr2::req_method("HEAD"),
    credential = credential,
    audience = .fabric_audience$storage,
    idempotent = TRUE,
    accepted_status = 404L
  )
  httr2::resp_status(response) != 404L
}
