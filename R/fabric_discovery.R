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
#'   different Fabric cloud or a test service.
#'
#' @return A tibble with one row per workspace. Important columns include `id`
#'   (useful for later API calls), `displayName`, `capacityRegion`, and
#'   `apiEndpoint`. `tags` and the complete service response in `raw` are list
#'   columns.
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
  fabric_workspace_tbl(records)
}

#' Discover Microsoft Fabric items
#'
#' Lists the items stored in one workspace. In Fabric, an *item* is a resource
#' such as a Lakehouse, Warehouse, semantic model, notebook, or Eventhouse.
#'
#' @param workspace Workspace GUID, exact display name, or a workspace record
#'   returned by [fabric_workspaces()]. A record or GUID avoids an extra lookup;
#'   a name is often easier for interactive use.
#' @param type Optional Fabric API item type, for example `"Lakehouse"`,
#'   `"Warehouse"`, `"SemanticModel"`, or `"Notebook"`. Matching is done by
#'   Fabric, so use the API spelling. Leave `NULL` to list all item types.
#' @param detail Logical. `FALSE` makes the fewest API calls and is sufficient
#'   for names and IDs. `TRUE` also retrieves supported workload properties,
#'   such as SQL connection strings and Livy or KQL endpoints, but is slower
#'   and can require additional permissions. The typed helpers below use
#'   `TRUE`.
#' @param recursive Logical. `TRUE` includes items inside workspace folders;
#'   `FALSE` lists only items at the workspace root.
#' @inheritParams fabric_workspaces
#'
#' @return A tibble with one row per item and common columns including `id`,
#'   `displayName`, `type`, `workspaceId`, and `folderId`. With `detail = TRUE`,
#'   applicable rows also contain ready-to-use `sql_connection_string`,
#'   `one_lake_*_path`, `dax_connection_string`, `livy_url`,
#'   `query_service_uri`, or `graphql_endpoint` values. Fields that do not apply
#'   to an item are `NA`; `properties` and `raw` retain nested service data.
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
  ws <- fabric_resolve_workspace(workspace, credential, base)
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
    if (isTRUE(detail)) {
      fabric_enrich_item(record, credential, base)
    } else {
      fabric_add_derived_targets(record, base)
    }
  })
  fabric_item_tbl(records)
}

#' Discover one Microsoft Fabric item
#'
#' Finds one item and retrieves the connection details that fabricQueryR can
#' use. This is convenient when you know the item's name and do not need a
#' table of every item in the workspace.
#'
#' @param item Item GUID, exact display name, or a one-row item record returned
#'   by a discovery function. A display name must identify exactly one item of
#'   the requested `type`; use a GUID or discovered row when names are
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
  credential <- fabric_credential(
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args
  )
  base <- fabric_api_base(api_base)
  ws <- fabric_resolve_workspace(workspace, credential, base)

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
  structure(
    fabric_enrich_item(record, credential, base),
    class = c("fabric_item", "list")
  )
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
#' [fabric_items()] with a fixed item type and `detail = TRUE`.
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
#'   Do not supply `type` or `detail`; each helper sets those values.
#' @return A tibble with one row per matching item, common item metadata, and
#'   applicable connection fields. See [fabric_items()] for the column groups.
#' @name fabric_typed_items
NULL

#' @rdname fabric_typed_items
#' @export
fabric_lakehouses <- function(workspace, ...) {
  fabric_items(workspace, type = "Lakehouse", detail = TRUE, ...)
}

#' @rdname fabric_typed_items
#' @export
fabric_warehouses <- function(workspace, ...) {
  fabric_items(workspace, type = "Warehouse", detail = TRUE, ...)
}

#' @rdname fabric_typed_items
#' @export
fabric_sql_databases <- function(workspace, ...) {
  fabric_items(workspace, type = "SQLDatabase", detail = TRUE, ...)
}

#' @rdname fabric_typed_items
#' @export
fabric_semantic_models <- function(workspace, ...) {
  fabric_items(workspace, type = "SemanticModel", detail = TRUE, ...)
}

#' @rdname fabric_typed_items
#' @export
fabric_eventhouses <- function(workspace, ...) {
  fabric_items(workspace, type = "Eventhouse", detail = TRUE, ...)
}

#' @rdname fabric_typed_items
#' @export
fabric_kql_databases <- function(workspace, ...) {
  fabric_items(workspace, type = "KQLDatabase", detail = TRUE, ...)
}

#' @rdname fabric_typed_items
#' @export
fabric_notebooks <- function(workspace, ...) {
  fabric_items(workspace, type = "Notebook", detail = TRUE, ...)
}

#' @rdname fabric_typed_items
#' @export
fabric_graphql_apis <- function(workspace, ...) {
  fabric_items(workspace, type = "GraphQLApi", detail = TRUE, ...)
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

fabric_resolve_workspace <- function(workspace, credential, api_base) {
  supplied <- fabric_as_record(workspace)
  if (!is.null(supplied)) {
    return(list(
      id = supplied$id,
      displayName = supplied$displayName %||%
        supplied$workspaceDisplayName %||%
        NA_character_,
      raw = supplied
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
  list(id = record$id, displayName = record$displayName, raw = record)
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
      record$dax_connection_string <- paste0(
        "Data Source=powerbi://api.powerbi.com/v1.0/myorg/",
        utils::URLencode(workspace_name, reserved = TRUE),
        ";Initial Catalog=",
        record$displayName,
        ";"
      )
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

fabric_workspace_tbl <- function(records) {
  if (!length(records)) {
    return(tibble::tibble(
      id = character(),
      displayName = character(),
      description = character(),
      type = character(),
      capacityId = character(),
      domainId = character(),
      capacityRegion = character(),
      apiEndpoint = character(),
      tags = list(),
      raw = list()
    ))
  }
  tibble::tibble(
    id = vapply(records, function(x) x$id %||% NA_character_, character(1)),
    displayName = vapply(
      records,
      function(x) x$displayName %||% NA_character_,
      character(1)
    ),
    description = vapply(
      records,
      function(x) x$description %||% NA_character_,
      character(1)
    ),
    type = vapply(records, function(x) x$type %||% NA_character_, character(1)),
    capacityId = vapply(
      records,
      function(x) x$capacityId %||% NA_character_,
      character(1)
    ),
    domainId = vapply(
      records,
      function(x) x$domainId %||% NA_character_,
      character(1)
    ),
    capacityRegion = vapply(
      records,
      function(x) {
        region <- x$capacityRegion
        if (is.list(region)) {
          region <- region$displayName %||% region$name
        }
        if (is.null(region) || length(region) != 1L) {
          NA_character_
        } else {
          as.character(region)
        }
      },
      character(1)
    ),
    apiEndpoint = vapply(
      records,
      function(x) x$apiEndpoint %||% NA_character_,
      character(1)
    ),
    tags = lapply(records, function(x) x$tags %||% list()),
    raw = records
  )
}

fabric_item_tbl <- function(records) {
  scalar <- function(record, key) {
    value <- record[[key]]
    if (is.null(value) || length(value) != 1L) {
      NA_character_
    } else {
      as.character(value)
    }
  }
  columns <- c(
    "id",
    "displayName",
    "description",
    "type",
    "workspaceId",
    "workspaceDisplayName",
    "folderId",
    "sql_connection_string",
    "sql_server",
    "sql_database",
    "sql_endpoint_id",
    "sql_endpoint_status",
    "default_schema",
    "one_lake_tables_path",
    "one_lake_files_path",
    "dax_connection_string",
    "livy_url",
    "query_service_uri",
    "ingestion_service_uri",
    "graphql_endpoint"
  )
  if (!length(records)) {
    out <- stats::setNames(
      replicate(length(columns), character(), simplify = FALSE),
      columns
    )
    out$properties <- list()
    out$raw <- list()
    return(tibble::as_tibble(out))
  }
  out <- lapply(columns, function(key) {
    vapply(records, scalar, character(1), key = key)
  })
  names(out) <- columns
  out$properties <- lapply(records, function(x) x$properties %||% list())
  out$raw <- records
  tibble::as_tibble(out)
}
