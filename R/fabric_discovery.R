.fabric_api_base <- "https://api.fabric.microsoft.com/v1"

#' Discover Microsoft Fabric workspaces
#'
#' Lists the Fabric workspaces the signed-in user or application can access. A
#' workspace is the top-level container that holds Lakehouses, Warehouses,
#' semantic models, notebooks, and other Fabric items.
#'
#' @param roles Optional workspace roles to include, such as `"Viewer"`,
#'   `"Contributor"`, `"Member"`, or `"Admin"`. Leave `NULL` to return every
#'   visible workspace.
#' @param prefer_workspace_endpoints Logical. Set to `TRUE` to ask Fabric for a
#'   workspace-specific API endpoint, which can be needed with workspace-level
#'   private links. Most users should keep the default, `FALSE`.
#' @param tenant_id Microsoft Entra tenant ID. Defaults to
#'   `FABRICQUERYR_TENANT_ID`.
#' @param client_id Microsoft Entra application/client ID. Defaults to
#'   `FABRICQUERYR_CLIENT_ID`, then the Azure CLI application ID.
#' @param token Preferred token input: an `AzureAuth::AzureToken` object,
#'   bearer-token string, or token-provider function. With `NULL` (the
#'   default), `AzureAuth` reuses a matching cached token or starts its normal
#'   interactive login flow when a new token is required.
#' @param auth_args Named list of additional arguments passed to
#'   [AzureAuth::get_azure_token()] when no token source is supplied.
#'   Discovery uses the
#'   `https://api.fabric.microsoft.com/.default` audience and requires
#'   `Workspace.Read.All` or `Workspace.ReadWrite.All`.
#' @param api_base Fabric REST API base URL. Leave unchanged unless using a
#'   different Fabric cloud or a test service. When `workspace` is a record
#'   containing `apiEndpoint`, that workspace-specific endpoint is used unless
#'   `api_base` is supplied explicitly.
#'
#' @return A plain list with one `fabric_workspace` object per workspace. Each
#'   object is a named list containing the fields returned by Fabric, such as
#'   `id`, `displayName`, `capacityRegion`, `apiEndpoint`, and nested `tags`.
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
  api_base = .fabric_api_base
) {
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
  credential <- fabric_credential(
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args
  )
  url <- paste0(fabric_api_base(api_base), "/workspaces")
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
  fabric_workspace_list(records)
}

#' Discover Microsoft Fabric items
#'
#' Lists the items stored in one workspace. In Fabric, an *item* is a resource
#' such as a Lakehouse, Warehouse, semantic model, notebook, or Eventhouse.
#'
#' @param workspace Workspace GUID, exact display name, or a workspace record
#'   returned by [fabric_workspaces()]. A record avoids an extra lookup and, if
#'   it contains `apiEndpoint`, routes workspace calls through that endpoint. A
#'   name is often easier for interactive use.
#' @param type Optional Fabric API item type, for example `"Lakehouse"`,
#'   `"Warehouse"`, `"SemanticModel"`, or `"Notebook"`. Matching is done by
#'   Fabric, so use the API spelling. Leave `NULL` to list all item types.
#' @param detail Logical. `FALSE` makes the fewest API calls and is sufficient
#'   for names and IDs. `TRUE` also retrieves supported workload properties,
#'   such as SQL connection strings and Livy or KQL endpoints, but is slower
#'   and can require additional permissions. The typed helpers below use
#'   `TRUE`.
#' @param detail_errors How workload-detail failures are handled. `"record"`
#'   retains the item, stores the message in `detail_error`, and emits one
#'   summary warning. `"abort"` preserves strict all-or-nothing behavior.
#' @param recursive Logical. `TRUE` includes items inside workspace folders;
#'   `FALSE` lists only items at the workspace root.
#' @inheritParams fabric_workspaces
#'
#' @return A list with one `fabric_item` object per item. Each object is a
#'   named list with common fields including `id`, `displayName`, `type`,
#'   `workspaceId`, and `folderId`. With `detail = TRUE`, applicable objects
#'   also contain ready-to-use `sql_connection_string`,
#'   `one_lake_*_path`, `dax_connection_string`, `livy_url`,
#'   `query_service_uri`, or `graphql_endpoint` values. When supplied by
#'   Fabric, `workspaceApiEndpoint` preserves the workspace-specific API origin
#'   for later job calls. Fields that do not apply to an item are absent;
#'   `detail_error` records failed enrichment requests.
#'   Nested service data is retained in place, including in `properties`.
#' @details
#' The caller needs at least access to the workspace (the Viewer role is
#' sufficient for the core list operation). Workload enrichment additionally
#' requires `Item.Read.All`/`Item.ReadWrite.All` or the corresponding
#' workload-specific read scope and access to the item.
#'
#' @references
#' [List items REST API](https://learn.microsoft.com/en-us/rest/api/fabric/core/items/list-items)
#'
#' [Fabric item management overview](https://learn.microsoft.com/en-us/rest/api/fabric/articles/item-management/item-management-overview)
#' @export
fabric_items <- function(
  workspace,
  type = NULL,
  detail = FALSE,
  detail_errors = c("record", "abort"),
  recursive = TRUE,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  token = NULL,
  auth_args = list(),
  api_base = .fabric_api_base
) {
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
  credential <- fabric_credential(
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args
  )
  base <- fabric_api_base(api_base)
  ws <- fabric_resolve_workspace(
    workspace,
    credential,
    base,
    use_workspace_endpoint = !api_base_supplied
  )
  base <- ws$api_base %||% base
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
  records <- lapply(records, function(record) {
    record$workspaceId <- record$workspaceId %||% ws$id
    record$workspaceDisplayName <- ws$displayName
    record$workspaceType <- fabric_record_value(ws$raw, "type")
    record$workspaceTenantId <- fabric_record_value(
      ws$raw,
      "tenantId",
      "tenant_id"
    )
    record$workspaceOwner <- fabric_record_value(
      ws$raw,
      "ownerUserPrincipalName",
      "userPrincipalName",
      "ownerId",
      "userId"
    )
    record$workspaceApiEndpoint <- record$workspaceApiEndpoint %||%
      fabric_record_value(ws$raw, "apiEndpoint", "api_endpoint")
    if (isTRUE(detail)) {
      tryCatch(
        fabric_enrich_item(record, credential, base),
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
        "Could not retrieve workload details for %d Fabric item%s; ",
        "see detail_error"
      ),
      failed,
      if (failed == 1L) "" else "s"
    ))
  }
  fabric_item_list(records)
}

#' Discover one Microsoft Fabric item
#'
#' Finds one item and retrieves the connection details that fabricQueryR can
#' use. This is convenient when you know the item's name and do not need a
#' collection of every item in the workspace.
#'
#' @param item Item GUID, exact display name, or an item record returned
#'   by a discovery function. A display name must identify exactly one item of
#'   the requested `type`; use a GUID or discovered record when names are
#'   duplicated.
#' @inheritParams fabric_items
#'
#' @return A `fabric_item` list. It contains common fields such as `id`,
#'   `displayName`, `type`, and `workspaceId`, the nested workload
#'   `properties`, and applicable connection targets such as
#'   `sql_connection_string`, `livy_url`, or `query_service_uri`.
#' @export
fabric_item <- function(
  workspace,
  item,
  type = NULL,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  token = NULL,
  auth_args = list(),
  api_base = .fabric_api_base
) {
  api_base_supplied <- !missing(api_base)
  credential <- fabric_credential(
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args
  )
  base <- fabric_api_base(api_base)
  ws <- fabric_resolve_workspace(
    workspace,
    credential,
    base,
    use_workspace_endpoint = !api_base_supplied
  )
  base <- ws$api_base %||% base

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
  fabric_validate_item_workspace(record, ws$id)
  record$workspaceId <- record$workspaceId %||% ws$id
  record$workspaceDisplayName <- record$workspaceDisplayName %||%
    ws$displayName
  record$workspaceType <- record$workspaceType %||%
    fabric_record_value(ws$raw, "type")
  record$workspaceTenantId <- record$workspaceTenantId %||%
    fabric_record_value(ws$raw, "tenantId", "tenant_id")
  record$workspaceOwner <- record$workspaceOwner %||%
    fabric_record_value(
      ws$raw,
      "ownerUserPrincipalName",
      "userPrincipalName",
      "ownerId",
      "userId"
    )
  record$workspaceApiEndpoint <- record$workspaceApiEndpoint %||%
    fabric_record_value(ws$raw, "apiEndpoint", "api_endpoint")
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

#' Typed Microsoft Fabric item discovery
#'
#' These shortcuts list one kind of item and include the detailed connection
#' fields used by the matching query functions. They are equivalent to
#' [fabric_items()] with a fixed item type and `detail = TRUE`. Set
#' `detail = FALSE` when only names and identifiers are needed.
#'
#' @section Choosing a helper:
#' - `fabric_lakehouses()` and `fabric_warehouses()` find data stores that can
#'   be queried through [fabric_sql_query()]; Lakehouses can also be accessed
#'   through OneLake and Livy.
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

fabric_api_base <- function(api_base) {
  if (
    !is.character(api_base) ||
      length(api_base) != 1L ||
      is.na(api_base) ||
      !nzchar(api_base)
  ) {
    rlang::abort("api_base must be one non-empty string")
  }
  sub("/+$", "", api_base)
}

fabric_is_guid <- function(value) {
  grepl(
    "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    value,
    ignore.case = TRUE
  )
}

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
      nzchar(parsed$port %||% "") ||
      !path %in% c("", "/v1") ||
      length(parsed$query %||% list()) > 0L ||
      nzchar(parsed$fragment %||% "")
  ) {
    rlang::abort(
      "The workspace apiEndpoint must be an HTTPS origin with an optional /v1 path"
    )
  }
  if (identical(tolower(path), "/v1")) endpoint else paste0(endpoint, "/v1")
}

fabric_host_matches <- function(hostname, suffix) {
  hostname <- tolower(hostname %||% "")
  suffix <- tolower(suffix)
  identical(hostname, suffix) || endsWith(hostname, paste0(".", suffix))
}

fabric_resolve_workspace <- function(
  workspace,
  credential,
  api_base,
  use_workspace_endpoint = TRUE
) {
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

fabric_item_route <- function(type) {
  routes <- c(
    lakehouse = "lakehouses",
    warehouse = "warehouses",
    sqldatabase = "sqlDatabases",
    semanticmodel = "semanticModels",
    eventhouse = "eventhouses",
    kqldatabase = "kqlDatabases",
    notebook = "notebooks",
    graphqlapi = "graphQLApis"
  )
  unname(routes[[tolower(type)]])
}

fabric_enrich_item <- function(record, credential, api_base) {
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
  }
  fabric_add_derived_targets(record, api_base)
}

fabric_add_derived_targets <- function(record, api_base) {
  properties <- record$properties %||% list()
  type <- tolower(record$type %||% "")
  record$properties <- properties
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
  } else if (type == "warehouse") {
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
  record
}

fabric_workspace_list <- function(records) {
  lapply(records, function(record) {
    structure(record, class = c("fabric_workspace", "list"))
  })
}

fabric_item_list <- function(records) {
  lapply(records, function(record) {
    structure(record, class = c("fabric_item", "list"))
  })
}
