.fabric_api_base <- "https://api.fabric.microsoft.com/v1"

#' Discover Microsoft Fabric workspaces
#'
#' Returns the Fabric workspaces available to the signed-in user or application.
#' Use the result to choose a workspace for [fabric_items()] or one of the typed
#' discovery helpers.
#'
#' @param roles Optional workspace roles to include, such as `"Viewer"`,
#'   `"Contributor"`, `"Member"`, or `"Admin"`. Leave `NULL` to return every
#'   visible workspace.
#' @param prefer_workspace_endpoints Whether to request workspace-specific
#'   endpoints. Keep `FALSE` unless your organization uses workspace-level
#'   private links.
#' @param tenant_id Microsoft Entra tenant ID. Defaults to
#'   `FABRICQUERYR_TENANT_ID`.
#' @param client_id Microsoft Entra application/client ID. Defaults to
#'   `FABRICQUERYR_CLIENT_ID`, then the Azure CLI application ID.
#' @param token Optional access token or token-provider function. Leave `NULL`
#'   to let fabricQueryR use its normal sign-in flow.
#' @param auth_args Additional sign-in options passed to
#'   [AzureAuth::get_azure_token()].
#' @param api_base Fabric REST API base URL. Leave unchanged unless using a
#'   different Fabric cloud or a test service.
#' @param allow_custom_endpoint Logical. Set to `TRUE` only when `api_base` is
#'   a non-Microsoft HTTPS origin that you trust to receive a Fabric token.
#'
#' @return A list with one workspace record per visible workspace. Each record
#'   includes its ID and display name, together with other details returned by
#'   Fabric.
#' @details
#' The caller needs permission to read Fabric workspaces. Discovery uses the
#' Fabric API and requires `Workspace.Read.All` or `Workspace.ReadWrite.All`.
#' @references
#' [List workspaces REST API](https://learn.microsoft.com/en-us/rest/api/fabric/core/workspaces/list-workspaces)
#'
#' [Workspace roles](https://learn.microsoft.com/en-us/fabric/fundamentals/roles-workspaces)
#' @export
fabric_workspaces <- function(
  roles = NULL,
  prefer_workspace_endpoints = FALSE,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  token = NULL,
  auth_args = list(),
  api_base = .fabric_api_base,
  allow_custom_endpoint = FALSE
) {
  # 1 Validate inputs ------------------------------------------------------------------------------

  # Check filtering options before signing in or sending a request.
  if (
    !is.null(roles) &&
      (!is.character(roles) ||
        !length(roles) ||
        anyNA(roles) ||
        !all(nzchar(roles)))
  ) {
    rlang::abort("roles must contain one or more non-empty strings")
  }
  if (
    !is.logical(prefer_workspace_endpoints) ||
      length(prefer_workspace_endpoints) != 1L ||
      is.na(prefer_workspace_endpoints)
  ) {
    rlang::abort("prefer_workspace_endpoints must be TRUE or FALSE")
  }

  # 2 Request visible workspaces -------------------------------------------------------------------

  # Use one shared credential for every page returned by Fabric.
  base <- fabric_api_base(api_base, allow_custom_endpoint)
  credential <- fabric_credential(
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args
  )
  url <- paste0(base, "/workspaces")
  req <- httr2::request(url)
  query <- list(
    roles = if (is.null(roles)) NULL else paste(roles, collapse = ","),
    preferWorkspaceSpecificEndpoints = if (prefer_workspace_endpoints) {
      "true"
    } else {
      NULL
    }
  )
  req <- do.call(httr2::req_url_query, c(list(req), query))
  records <- .httr2_collection(
    req$url,
    credential = credential,
    audience = .fabric_audience$fabric
  )

  # 3 Return discovery records ---------------------------------------------------------------------

  # Add a small class while keeping every field returned by Fabric.
  fabric_workspace_list(records)
}

#' Discover Microsoft Fabric items
#'
#' Returns the Lakehouses, Warehouses, semantic models, notebooks, and other
#' items stored in a workspace. Set `detail = TRUE` when you want records that
#' can be passed directly to query or connection functions.
#'
#' @param workspace Workspace name, ID, or record returned by
#'   [fabric_workspaces()]. A name is convenient for interactive use; a record
#'   avoids an extra lookup.
#' @param type Optional Fabric API item type, for example `"Lakehouse"`,
#'   `"Warehouse"`, `"SemanticModel"`, or `"Notebook"`. Matching is done by
#'   Fabric, so use the API spelling. Leave `NULL` to list all item types.
#' @param detail Whether to retrieve connection details as well as names and
#'   IDs. This takes more requests and may require additional permissions. The
#'   typed discovery helpers use `TRUE` by default.
#' @param detail_errors What to do if some connection details cannot be read.
#'   `"record"` returns the available information and stores an error message
#'   with the affected item; `"abort"` stops the call.
#' @param recursive Logical. `TRUE` includes items inside workspace folders;
#'   `FALSE` lists only items at the workspace root.
#' @param personal_workspace_tenant_id Optional Microsoft Entra tenant ID used
#'   to build the XMLA endpoint for a Personal workspace.
#' @param personal_workspace_owner Optional owner UPN or Entra object ID used
#'   to build the XMLA endpoint for a Personal workspace. Microsoft Fabric's
#'   workspace API does not return either personal-workspace identifier, so
#'   supply this together with `personal_workspace_tenant_id` when a
#'   `dax_connection_string` is needed for a semantic model in My Workspace.
#' @inheritParams fabric_workspaces
#' @param api_base Fabric REST API base URL. When `workspace` is a record
#'   containing `apiEndpoint`, that workspace-specific endpoint is used unless
#'   `api_base` is supplied explicitly.
#'
#' @return A list with one item record per match. Every record includes common
#'   fields such as `id`, `displayName`, `type`, and `workspaceId`. With
#'   `detail = TRUE`, records also include the connection details needed by the
#'   matching fabricQueryR functions when Fabric makes them available.
#' @details
#' The caller needs at least access to the workspace (the Viewer role is
#' sufficient for the core list operation). Workload enrichment additionally
#' requires `Item.Read.All`/`Item.ReadWrite.All` or the corresponding
#' workload-specific read scope and access to the item.
#' Personal-workspace semantic models use Microsoft's v2 XMLA endpoint and
#' require both `personal_workspace_tenant_id` and `personal_workspace_owner`.
#'
#' @references
#' [List items REST API](https://learn.microsoft.com/en-us/rest/api/fabric/core/items/list-items)
#'
#' [Fabric item management overview](https://learn.microsoft.com/en-us/rest/api/fabric/articles/item-management/item-management-overview)
#'
#' [Personal-workspace XMLA endpoints](https://learn.microsoft.com/en-us/fabric/enterprise/powerbi/service-premium-connect-tools#connecting-to-a-personal-workspace)
#' @export
fabric_items <- function(
  workspace,
  type = NULL,
  detail = FALSE,
  detail_errors = c("record", "abort"),
  recursive = TRUE,
  personal_workspace_tenant_id = NULL,
  personal_workspace_owner = NULL,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  token = NULL,
  auth_args = list(),
  api_base = .fabric_api_base,
  allow_custom_endpoint = FALSE
) {
  # 1 Validate inputs ------------------------------------------------------------------------------

  # Reject invalid filters before resolving a workspace or making API calls.
  api_base_supplied <- !missing(api_base)
  if (
    !is.null(type) &&
      (!is.character(type) ||
        length(type) != 1L ||
        is.na(type) ||
        !nzchar(type))
  ) {
    rlang::abort("type must be one non-empty string")
  }
  if (!is.logical(detail) || length(detail) != 1L || is.na(detail)) {
    rlang::abort("detail must be TRUE or FALSE")
  }
  detail_errors <- match.arg(detail_errors)
  if (!is.logical(recursive) || length(recursive) != 1L || is.na(recursive)) {
    rlang::abort("recursive must be TRUE or FALSE")
  }
  fabric_validate_personal_workspace_identity(
    personal_workspace_tenant_id,
    personal_workspace_owner
  )

  # 2 Resolve authentication and workspace ---------------------------------------------------------

  # A discovered workspace record can provide a workspace-specific API base.
  base <- fabric_api_base(api_base, allow_custom_endpoint)
  credential <- fabric_credential(
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args
  )
  ws <- fabric_resolve_workspace(
    workspace,
    credential,
    base,
    use_workspace_endpoint = !api_base_supplied
  )
  base <- ws$api_base %||% base

  # 3 List matching items --------------------------------------------------------------------------

  # Fabric may return several pages; the shared collection helper follows all
  # continuation links before enrichment starts.
  req <- httr2::request(
    paste0(base, "/workspaces/", ws$id, "/items")
  )
  req <- httr2::req_url_query(
    req,
    type = type,
    recursive = if (recursive) "true" else "false"
  )
  records <- .httr2_collection(
    req$url,
    credential = credential,
    audience = .fabric_audience$fabric
  )

  # 4 Add connection details -----------------------------------------------------------------------

  # Enrich each record independently so one unavailable detail endpoint does
  # not discard the other items unless the caller requested strict errors.
  records <- lapply(records, function(record) {
    record <- fabric_add_workspace_context(
      record,
      ws,
      personal_workspace_tenant_id,
      personal_workspace_owner
    )
    record <- fabric_add_workspace_endpoints(record, ws)
    if (isTRUE(detail)) {
      tryCatch(
        fabric_enrich_item(
          record,
          credential,
          base,
          private_sql_errors = detail_errors
        ),
        error = function(error) {
          if (identical(detail_errors, "abort")) {
            rlang::cnd_signal(error)
          }
          record$detail_error <- conditionMessage(error)
          record$detail_error_class <- class(error)[[1L]]
          fabric_add_derived_targets(record, base)
        }
      )
    } else {
      fabric_add_derived_targets(record, base)
    }
  })
  failed <- sum(vapply(
    records,
    function(record) !is.null(record$detail_error),
    logical(1)
  ))
  if (failed > 0) {
    rlang::warn(sprintf(
      paste0(
        "Could not fully enrich %d Fabric item%s; ",
        "see detail_error"
      ),
      failed,
      if (failed == 1L) "" else "s"
    ))
  }

  # 5 Return discovery records ---------------------------------------------------------------------

  fabric_item_list(records)
}

#' Discover one Microsoft Fabric item
#'
#' Finds one item and returns the connection details needed by fabricQueryR.
#' Use this when you know the item's name or ID and do not need to list every
#' item in the workspace.
#'
#' @param item Item GUID, exact display name, or an item record returned
#'   by a discovery function. A display name must identify exactly one item of
#'   the requested `type`; use a GUID or discovered record when names are
#'   duplicated.
#' @inheritParams fabric_items
#'
#' @return One `fabric_item` record containing the item's name, ID, type,
#'   workspace, and any connection details that Fabric makes available.
#' @details
#' The caller needs access to the workspace for the core item lookup. This
#' singular helper always performs workload-specific enrichment as well, which
#' additionally requires `Item.Read.All`/`Item.ReadWrite.All` or the applicable
#' workload-specific read scope and access to the item. Use
#' [fabric_items()] with `detail = FALSE` when only core item metadata is needed.
#' @export
fabric_item <- function(
  workspace,
  item,
  type = NULL,
  personal_workspace_tenant_id = NULL,
  personal_workspace_owner = NULL,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  token = NULL,
  auth_args = list(),
  api_base = .fabric_api_base,
  allow_custom_endpoint = FALSE
) {
  # 1 Resolve authentication and workspace ---------------------------------------------------------

  # Establish the workspace first because both item lookup routes need its ID.
  api_base_supplied <- !missing(api_base)
  fabric_validate_personal_workspace_identity(
    personal_workspace_tenant_id,
    personal_workspace_owner
  )
  base <- fabric_api_base(api_base, allow_custom_endpoint)
  credential <- fabric_credential(
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args
  )
  ws <- fabric_resolve_workspace(
    workspace,
    credential,
    base,
    use_workspace_endpoint = !api_base_supplied
  )
  base <- ws$api_base %||% base

  # 2 Find the requested item ----------------------------------------------------------------------

  # Reuse a supplied discovery record, look up GUIDs directly, and use a
  # paged name search only when the caller supplied a display name.
  supplied <- fabric_as_record(item)
  if (!is.null(supplied)) {
    record <- supplied
  } else {
    if (
      !is.character(item) || length(item) != 1L || is.na(item) || !nzchar(item)
    ) {
      rlang::abort("item must be one non-empty string or a discovered item")
    }
    if (fabric_is_guid(item)) {
      record <- .httr2_json(
        httr2::request(
          paste0(base, "/workspaces/", ws$id, "/items/", item)
        ),
        simplifyVector = FALSE,
        credential = credential,
        audience = .fabric_audience$fabric
      )
    } else {
      req <- httr2::request(
        paste0(base, "/workspaces/", ws$id, "/items")
      )
      req <- httr2::req_url_query(req, type = type)
      candidates <- .httr2_collection(
        req$url,
        credential = credential,
        audience = .fabric_audience$fabric
      )
      record <- fabric_unique_name(candidates, item, "item")
    }
  }

  # 3 Validate and enrich the result ---------------------------------------------------------------

  # Add workspace context before deriving service-specific connection targets.
  fabric_validate_item_workspace(record, ws$id)
  record <- fabric_add_workspace_context(
    record,
    ws,
    personal_workspace_tenant_id,
    personal_workspace_owner
  )
  record <- fabric_add_workspace_endpoints(record, ws)
  if (!is.null(type) && !identical(tolower(record$type), tolower(type))) {
    rlang::abort(
      sprintf(
        "Item '%s' has type '%s', not '%s'",
        record$displayName %||% record$id,
        record$type,
        type
      )
    )
  }
  fabric_item_list(list(
    fabric_enrich_item(record, credential, base)
  ))[[1L]]
}

#' Typed Microsoft Fabric item discovery
#'
#' These shortcuts find one kind of Fabric item. By default, they also retrieve
#' the connection details needed by the matching query functions, so their
#' results can usually be passed straight to the next fabricQueryR call. Set
#' `detail = FALSE` when you only need names and IDs.
#'
#' @section Choosing a helper:
#' - `fabric_lakehouses()`, `fabric_warehouses()`, and
#'   `fabric_warehouse_snapshots()` find data stores that can be queried through
#'   [fabric_sql_query()]; Lakehouses can also be accessed through OneLake and
#'   Livy.
#' - `fabric_sql_databases()` finds transactional Fabric SQL databases.
#' - `fabric_semantic_models()` finds the business models queried with DAX via
#'   [fabric_pbi_dax_query()].
#' - `fabric_eventhouses()` and `fabric_kql_databases()` find real-time data
#'   stores queried with [fabric_kql_query()].
#' - `fabric_notebooks()` finds notebooks that can be run with
#'   [fabric_job_run()].
#' - `fabric_graphql_apis()` finds APIs configured in Fabric for use with
#'   [fabric_graphql_query()].
#'
#' @inheritParams fabric_items
#' @param ... Authentication and API arguments forwarded to [fabric_items()].
#'   Do not supply `type`; each helper sets that value.
#' @return A list with one `fabric_item` object per matching item. Each object
#'   contains common item metadata and applicable connection fields. See
#'   [fabric_items()] for details.
#' @name fabric_typed_items
NULL

#' @rdname fabric_typed_items
#' @export
fabric_lakehouses <- function(workspace, detail = TRUE, ...) {
  fabric_items(workspace, type = "Lakehouse", detail = detail, ...)
}

#' @rdname fabric_typed_items
#' @export
fabric_warehouses <- function(workspace, detail = TRUE, ...) {
  fabric_items(workspace, type = "Warehouse", detail = detail, ...)
}

#' @rdname fabric_typed_items
#' @export
fabric_warehouse_snapshots <- function(workspace, detail = TRUE, ...) {
  fabric_items(workspace, type = "WarehouseSnapshot", detail = detail, ...)
}

#' @rdname fabric_typed_items
#' @export
fabric_sql_databases <- function(workspace, detail = TRUE, ...) {
  fabric_items(workspace, type = "SQLDatabase", detail = detail, ...)
}

#' @rdname fabric_typed_items
#' @export
fabric_semantic_models <- function(workspace, detail = TRUE, ...) {
  fabric_items(workspace, type = "SemanticModel", detail = detail, ...)
}

#' @rdname fabric_typed_items
#' @export
fabric_eventhouses <- function(workspace, detail = TRUE, ...) {
  fabric_items(workspace, type = "Eventhouse", detail = detail, ...)
}

#' @rdname fabric_typed_items
#' @export
fabric_kql_databases <- function(workspace, detail = TRUE, ...) {
  fabric_items(workspace, type = "KQLDatabase", detail = detail, ...)
}

#' @rdname fabric_typed_items
#' @export
fabric_notebooks <- function(workspace, detail = TRUE, ...) {
  fabric_items(workspace, type = "Notebook", detail = detail, ...)
}

#' @rdname fabric_typed_items
#' @export
fabric_graphql_apis <- function(workspace, detail = TRUE, ...) {
  fabric_items(workspace, type = "GraphQLApi", detail = detail, ...)
}

# Add workspace-specific API and OneLake endpoints to `record`. Returns the
# updated item record for the discovery functions.
fabric_add_workspace_endpoints <- function(record, workspace) {
  record$workspaceApiEndpoint <- record$workspaceApiEndpoint %||%
    fabric_record_value(workspace$raw, "apiEndpoint", "api_endpoint")
  onelake <- workspace$raw$oneLakeEndpoints %||%
    workspace$raw$one_lake_endpoints
  if (is.list(onelake)) {
    record$workspaceOneLakeEndpoints <- onelake
    record$workspaceOneLakeDfsEndpoint <- onelake$dfsEndpoint %||%
      onelake$dfs_endpoint
  }
  record
}

# Check that optional `value` is one non-empty string. Returns invisibly and is
# used for personal-workspace identity fields.
fabric_discovery_optional_string <- function(value, name) {
  if (
    !is.null(value) &&
      (!is.character(value) ||
        length(value) != 1L ||
        is.na(value) ||
        !nzchar(value))
  ) {
    rlang::abort(paste0(name, " must be NULL or one non-empty string"))
  }
  invisible(value)
}

# Check the optional personal-workspace `tenant_id` and `owner` as a pair.
# Returns invisibly for the item discovery entry points.
fabric_validate_personal_workspace_identity <- function(tenant_id, owner) {
  fabric_discovery_optional_string(
    tenant_id,
    "personal_workspace_tenant_id"
  )
  fabric_discovery_optional_string(owner, "personal_workspace_owner")
  if (xor(is.null(tenant_id), is.null(owner))) {
    rlang::abort(paste0(
      "personal_workspace_tenant_id and personal_workspace_owner ",
      "must be supplied together"
    ))
  }
  invisible(TRUE)
}

# Add workspace IDs, names, and personal-workspace identity to `record`.
# Returns the updated item record before endpoint enrichment.
fabric_add_workspace_context <- function(
  record,
  workspace,
  personal_workspace_tenant_id = NULL,
  personal_workspace_owner = NULL
) {
  record$workspaceId <- record$workspaceId %||% workspace$id
  record$workspaceDisplayName <- record$workspaceDisplayName %||%
    workspace$displayName
  record$workspaceType <- record$workspaceType %||%
    fabric_record_value(workspace$raw, "type")
  record$workspaceTenantId <- record$workspaceTenantId %||%
    fabric_record_value(workspace$raw, "tenantId", "tenant_id")
  record$workspaceOwner <- record$workspaceOwner %||%
    fabric_record_value(
      workspace$raw,
      "ownerUserPrincipalName",
      "userPrincipalName",
      "ownerId",
      "userId"
    )
  personal <- tolower(record$workspaceType %||% "") %in%
    c("personal", "personalgroup")
  if (personal) {
    record$workspaceTenantId <- personal_workspace_tenant_id %||%
      record$workspaceTenantId
    record$workspaceOwner <- personal_workspace_owner %||%
      record$workspaceOwner
  }
  record
}

# Check that `item` belongs to `workspace_id`. Returns invisibly and prevents a
# supplied discovery record from being used with the wrong workspace.
fabric_validate_item_workspace <- function(item, workspace_id) {
  item_workspace <- fabric_record_value(
    item,
    "workspaceId",
    "workspace_id"
  )
  if (
    !is.null(item_workspace) &&
      !identical(
        tolower(as.character(item_workspace)),
        tolower(as.character(workspace_id))
      )
  ) {
    rlang::abort(
      "The discovered item belongs to a different workspace"
    )
  }
  invisible(TRUE)
}

# Normalize and validate `api_base`. Returns a trusted Fabric v1 base URL used
# by all discovery requests.
fabric_api_base <- function(api_base, allow_custom_endpoint = FALSE) {
  # 1 Parse the endpoint ---------------------------------------------------------------------------

  # Only a complete HTTPS URL can safely receive an access token.
  if (
    !is.logical(allow_custom_endpoint) ||
      length(allow_custom_endpoint) != 1L ||
      is.na(allow_custom_endpoint)
  ) {
    rlang::abort("allow_custom_endpoint must be TRUE or FALSE")
  }
  if (
    !is.character(api_base) ||
      length(api_base) != 1L ||
      is.na(api_base) ||
      !nzchar(api_base)
  ) {
    rlang::abort("api_base must be one non-empty string")
  }
  endpoint <- sub("/+$", "", trimws(api_base))
  parsed <- try(httr2::url_parse(endpoint), silent = TRUE)
  path <- if (inherits(parsed, "try-error")) "" else parsed$path %||% ""
  path <- sub("/+$", "", path)
  host <- if (inherits(parsed, "try-error")) {
    ""
  } else {
    tolower(parsed$hostname %||% "")
  }
  clean_origin <- !inherits(parsed, "try-error") &&
    identical(tolower(parsed$scheme %||% ""), "https") &&
    nzchar(host) &&
    !nzchar(parsed$username %||% "") &&
    !nzchar(parsed$password %||% "") &&
    (parsed$port %||% "") %in% c("", "443") &&
    path %in% c("", "/v1") &&
    length(parsed$query %||% list()) == 0L &&
    !nzchar(parsed$fragment %||% "")
  if (!clean_origin) {
    rlang::abort(
      paste0(
        "api_base must be an HTTPS origin using the default port (443), ",
        "with an optional /v1 path"
      ),
      class = "fabric_api_endpoint_error"
    )
  }
  if (
    !fabric_host_matches(host, "api.fabric.microsoft.com") &&
      !isTRUE(allow_custom_endpoint)
  ) {
    rlang::abort(
      paste0(
        "Refusing to send a Fabric token to untrusted api_base '",
        api_base,
        "'; set allow_custom_endpoint = TRUE only for an endpoint you trust"
      ),
      class = "fabric_api_endpoint_error"
    )
  }

  # 2 Normalize the API version --------------------------------------------------------------------

  if (identical(tolower(path), "/v1")) endpoint else paste0(endpoint, "/v1")
}

# Test whether `value` is one GUID string. Returns a logical value used to
# choose direct-ID lookups instead of name searches.
fabric_is_guid <- function(value) {
  grepl(
    "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    value,
    ignore.case = TRUE
  )
}

# Convert a discovered object or plain named list into a record. Returns `NULL`
# for ordinary text inputs so public functions can follow their text path.
fabric_as_record <- function(value) {
  if (inherits(value, "data.frame")) {
    if (nrow(value) != 1L) {
      rlang::abort("A discovered object must contain exactly one row")
    }
    return(lapply(value, function(column) {
      if (is.list(column)) column[[1L]] else column[[1L]]
    }))
  }
  if (is.list(value) && !is.null(value$id)) {
    return(value)
  }
  NULL
}

# Read the first present field named in `...` from `record`. Returns that value
# or `NULL`, allowing helpers to accept API and R-style field names.
fabric_record_value <- function(record, ...) {
  keys <- c(...)
  for (key in keys) {
    value <- record[[key]]
    if (is.list(value) && length(value) == 1L && is.atomic(value[[1L]])) {
      value <- value[[1L]]
    }
    if (!is.null(value) && length(value) == 1L && !is.na(value)) {
      return(value)
    }
    properties <- record$properties
    if (is.list(properties)) {
      value <- properties[[key]]
      if (!is.null(value) && length(value) == 1L && !is.na(value)) {
        return(value)
      }
    }
  }
  NULL
}

# Choose a trusted workspace API endpoint from `record`, falling back to the
# caller's base URL. Returns a normalized v1 URL for workspace requests.
fabric_workspace_api_base <- function(record, fallback) {
  endpoint <- fabric_record_value(
    record,
    "apiEndpoint",
    "api_endpoint",
    "workspaceApiEndpoint"
  )
  if (is.null(endpoint)) {
    return(fallback)
  }
  endpoint <- sub("/+$", "", trimws(endpoint))
  parsed <- try(httr2::url_parse(endpoint), silent = TRUE)
  path <- if (inherits(parsed, "try-error")) "" else parsed$path %||% ""
  path <- sub("/+$", "", path)
  hostname <- if (inherits(parsed, "try-error")) {
    ""
  } else {
    tolower(parsed$hostname %||% "")
  }
  if (
    inherits(parsed, "try-error") ||
      !identical(tolower(parsed$scheme %||% ""), "https") ||
      !fabric_host_matches(hostname, "api.fabric.microsoft.com") ||
      nzchar(parsed$username %||% "") ||
      nzchar(parsed$password %||% "") ||
      !(parsed$port %||% "") %in% c("", "443") ||
      !path %in% c("", "/v1") ||
      length(parsed$query %||% list()) > 0L ||
      nzchar(parsed$fragment %||% "")
  ) {
    rlang::abort(
      paste0(
        "The workspace apiEndpoint must be an HTTPS origin using the ",
        "default port (443), with an optional /v1 path"
      )
    )
  }
  if (identical(tolower(path), "/v1")) endpoint else paste0(endpoint, "/v1")
}

# Check whether `hostname` equals or is below `suffix`. Returns one logical
# value used by endpoint trust checks throughout the package.
fabric_host_matches <- function(hostname, suffix) {
  hostname <- tolower(hostname %||% "")
  suffix <- tolower(suffix)
  identical(hostname, suffix) || endsWith(hostname, paste0(".", suffix))
}

# Resolve a workspace record, GUID, or exact name. Returns a normalized
# workspace context used by item discovery and job operations.
fabric_resolve_workspace <- function(
  workspace,
  credential,
  api_base,
  use_workspace_endpoint = TRUE
) {
  # 1 Reuse a supplied record ----------------------------------------------------------------------

  # A discovered record may already contain a workspace-specific API endpoint.
  supplied <- fabric_as_record(workspace)
  if (!is.null(supplied)) {
    return(list(
      id = supplied$id,
      displayName = supplied$displayName %||%
        supplied$workspaceDisplayName %||%
        NA_character_,
      raw = supplied,
      api_base = if (isTRUE(use_workspace_endpoint)) {
        fabric_workspace_api_base(supplied, api_base)
      } else {
        api_base
      }
    ))
  }

  # 2 Resolve text input ---------------------------------------------------------------------------

  # GUIDs can be used directly; display names require a paged workspace list.
  if (
    !is.character(workspace) ||
      length(workspace) != 1L ||
      is.na(workspace) ||
      !nzchar(workspace)
  ) {
    rlang::abort(
      "workspace must be one non-empty string or a discovered workspace"
    )
  }
  if (fabric_is_guid(workspace)) {
    record <- .httr2_json(
      httr2::request(paste0(api_base, "/workspaces/", workspace)),
      simplifyVector = FALSE,
      credential = credential,
      audience = .fabric_audience$fabric
    )
  } else {
    records <- .httr2_collection(
      paste0(api_base, "/workspaces"),
      credential = credential,
      audience = .fabric_audience$fabric
    )
    record <- fabric_unique_name(records, workspace, "workspace")
  }

  # 3 Build workspace context ----------------------------------------------------------------------

  list(
    id = record$id,
    displayName = record$displayName,
    raw = record,
    api_base = if (isTRUE(use_workspace_endpoint)) {
      fabric_workspace_api_base(record, api_base)
    } else {
      api_base
    }
  )
}

# Find exactly one record in `records` with display name `name`. Returns that
# record or explains missing/duplicate names to discovery callers.
fabric_unique_name <- function(records, name, kind) {
  names <- vapply(
    records,
    function(record) record$displayName %||% "",
    character(1)
  )
  matches <- which(names == name)
  if (!length(matches)) {
    matches <- which(tolower(names) == tolower(name))
  }
  if (!length(matches)) {
    rlang::abort(
      sprintf("%s '%s' was not found", tools::toTitleCase(kind), name)
    )
  }
  if (length(matches) > 1L) {
    rlang::abort(
      sprintf(
        "%s name '%s' is ambiguous (%d matches). Use its GUID",
        tools::toTitleCase(kind),
        name,
        length(matches)
      )
    )
  }
  records[[matches]]
}

# Map a Fabric item `type` to its workload detail route. Returns the route name
# or `NULL` when that item type has no supported detail endpoint.
fabric_item_route <- function(type) {
  routes <- c(
    lakehouse = "lakehouses",
    warehouse = "warehouses",
    warehousesnapshot = "warehouseSnapshots",
    sqldatabase = "sqlDatabases",
    semanticmodel = "semanticModels",
    eventhouse = "eventhouses",
    kqldatabase = "kqlDatabases",
    notebook = "notebooks",
    graphqlapi = "graphQLApis"
  )
  index <- match(tolower(type), names(routes))
  if (is.na(index)) {
    return(NULL)
  }
  unname(routes[[index]])
}

# Fetch workload details for `record` and derive its query targets. Returns an
# enriched discovery record used by `fabric_item()` and `fabric_items()`.
fabric_enrich_item <- function(
  record,
  credential,
  api_base,
  private_sql_errors = c("abort", "record")
) {
  private_sql_errors <- match.arg(private_sql_errors)
  route <- fabric_item_route(record$type %||% "")
  if (!is.null(route)) {
    detail <- .httr2_json(
      httr2::request(
        paste0(
          api_base,
          "/workspaces/",
          record$workspaceId,
          "/",
          route,
          "/",
          record$id
        )
      ),
      simplifyVector = FALSE,
      credential = credential,
      audience = .fabric_audience$fabric
    )
    workspace_name <- record$workspaceDisplayName
    record <- utils::modifyList(record, detail)
    record$workspaceDisplayName <- workspace_name
    record <- tryCatch(
      fabric_enrich_private_sql_target(record, credential, api_base),
      error = function(error) {
        if (identical(private_sql_errors, "abort")) {
          rlang::cnd_signal(error)
        }
        record$detail_error <- conditionMessage(error)
        record$detail_error_class <- class(error)[[1L]]
        record$detail_error_stage <- "private_sql_connection_string"
        record
      }
    )
  }
  fabric_add_derived_targets(record, api_base)
}

# Read private-link SQL details for `record`. Returns the record with a private
# endpoint when a workspace-specific API base requires one.
fabric_enrich_private_sql_target <- function(record, credential, api_base) {
  # 1 Check whether private details are needed -----------------------------------------------------

  # Public API origins already return their normal SQL connection details.
  parsed <- try(httr2::url_parse(api_base), silent = TRUE)
  host <- if (inherits(parsed, "try-error")) {
    ""
  } else {
    tolower(parsed$hostname %||% "")
  }
  workspace_private <- fabric_host_matches(
    host,
    "api.fabric.microsoft.com"
  ) &&
    !identical(host, "api.fabric.microsoft.com")
  if (!workspace_private) {
    return(record)
  }

  type <- tolower(record$type %||% "")
  route <- NULL
  item_id <- NULL
  if (identical(type, "warehouse")) {
    route <- "warehouses"
    item_id <- record$id
  } else if (identical(type, "lakehouse")) {
    route <- "sqlEndpoints"
    item_id <- record$properties$sqlEndpointProperties$id
  }
  if (is.null(route) || is.null(item_id)) {
    return(record)
  }

  # 2 Request the private SQL endpoint -------------------------------------------------------------

  req <- httr2::request(paste0(
    api_base,
    "/workspaces/",
    record$workspaceId,
    "/",
    route,
    "/",
    item_id,
    "/connectionString"
  )) |>
    httr2::req_url_query(privateLinkType = "Workspace")
  response <- .httr2_json(
    req,
    simplifyVector = FALSE,
    credential = credential,
    audience = .fabric_audience$fabric
  )
  connection_string <- response$connectionString
  if (
    !is.character(connection_string) ||
      length(connection_string) != 1L ||
      is.na(connection_string) ||
      !nzchar(connection_string)
  ) {
    rlang::abort(
      "Fabric returned an invalid workspace-private SQL connection string"
    )
  }

  # 3 Add the private connection target ------------------------------------------------------------

  if (identical(type, "warehouse")) {
    record$properties$connectionString <- connection_string
  } else {
    record$properties$sqlEndpointProperties$connectionString <- connection_string
  }
  record$sql_private_link_type <- "Workspace"
  record
}

# Derive convenient SQL, DAX, KQL, GraphQL, and OneLake targets from `record`.
# Returns the record used directly by the package's query functions.
fabric_add_derived_targets <- function(record, api_base) {
  # 1 Add shared item context ----------------------------------------------------------------------

  # Keep the API properties intact while adding stable, R-friendly field names.
  properties <- record$properties %||% list()
  type <- tolower(record$type %||% "")
  record$properties <- properties

  # 2 Add workload-specific targets ----------------------------------------------------------------

  if (type == "lakehouse") {
    sql <- properties$sqlEndpointProperties %||% list()
    record$default_schema <- properties$defaultSchema
    record$one_lake_tables_path <- properties$oneLakeTablesPath
    record$one_lake_files_path <- properties$oneLakeFilesPath
    record$sql_server <- sql$connectionString
    record$sql_database <- record$displayName
    record$sql_endpoint_id <- sql$id
    record$sql_endpoint_status <- sql$provisioningStatus
    record$livy_url <- paste0(
      api_base,
      "/workspaces/",
      record$workspaceId,
      "/lakehouses/",
      record$id,
      "/livyapi/versions/2023-12-01/sessions"
    )
  } else if (type %in% c("warehouse", "warehousesnapshot")) {
    record$sql_server <- properties$connectionString
    record$sql_database <- record$displayName
  } else if (type == "sqldatabase") {
    record$sql_connection_string <- properties$connectionString
    record$sql_server <- properties$serverFqdn
    record$sql_database <- properties$databaseName
  } else if (type == "semanticmodel") {
    workspace_name <- record$workspaceDisplayName
    if (!is.null(workspace_name) && !is.na(workspace_name)) {
      workspace_type <- tolower(record$workspaceType %||% "")
      personal <- workspace_type %in% c("personal", "personalgroup")
      server <- if (personal) {
        tenant_id <- record$workspaceTenantId
        owner <- record$workspaceOwner
        if (is.null(tenant_id) || is.null(owner)) {
          NULL
        } else {
          paste0(
            "powerbi://api.powerbi.com/v2.0/",
            tenant_id,
            "/home/myworkspace/",
            utils::URLencode(owner, reserved = TRUE)
          )
        }
      } else {
        paste0(
          "powerbi://api.powerbi.com/v1.0/myorg/",
          utils::URLencode(workspace_name, reserved = TRUE)
        )
      }
      if (!is.null(server)) {
        record$dax_connection_string <- paste0(
          "Data Source=",
          server,
          ";Initial Catalog=",
          fabric_quote_connection_value(record$displayName),
          ";"
        )
      }
    }
  } else if (type %in% c("eventhouse", "kqldatabase")) {
    record$query_service_uri <- properties$queryServiceUri
    record$ingestion_service_uri <- properties$ingestionServiceUri
  } else if (type == "graphqlapi") {
    record$graphql_endpoint <- paste0(
      api_base,
      "/workspaces/",
      record$workspaceId,
      "/graphqlapis/",
      record$id,
      "/graphql"
    )
  }

  # 3 Return the enriched record -------------------------------------------------------------------

  record
}

# Give each workspace API record its package class. Returns a list used by
# `fabric_workspaces()` while preserving all original fields.
fabric_workspace_list <- function(records) {
  lapply(records, function(record) {
    structure(record, class = c("fabric_workspace", "list"))
  })
}

# Give each item API record its package class. Returns a list used by all item
# discovery entry points while preserving all original fields.
fabric_item_list <- function(records) {
  lapply(records, function(record) {
    structure(record, class = c("fabric_item", "list"))
  })
}
