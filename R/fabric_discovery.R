.fabric_api_base <- "https://api.fabric.microsoft.com/v1"

#' Discover Microsoft Fabric workspaces
#'
#' Lists every workspace visible to the authenticated principal, following all
#' Fabric continuation pages.
#'
#' @param roles Optional character vector of workspace roles used to filter the
#'   response.
#' @param prefer_workspace_endpoints Logical. Ask Fabric to include a
#'   workspace-specific API endpoint, when available.
#' @param tenant_id,client_id,access_token,token_provider Authentication
#'   arguments. Discovery uses the
#'   `https://api.fabric.microsoft.com/.default` audience and requires
#'   `Workspace.Read.All` or `Workspace.ReadWrite.All`.
#' @param api_base Fabric REST API base URL.
#'
#' @return A tibble with one row per workspace. Nested Fabric fields are kept
#'   in list columns.
#' @export
fabric_workspaces <- function(
  roles = NULL,
  prefer_workspace_endpoints = FALSE,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  access_token = NULL,
  token_provider = NULL,
  api_base = .fabric_api_base
) {
  if (!is.null(roles)) {
    stopifnot(is.character(roles), length(roles) > 0L, all(nzchar(roles)))
  }
  stopifnot(
    is.logical(prefer_workspace_endpoints),
    length(prefer_workspace_endpoints) == 1L,
    !is.na(prefer_workspace_endpoints)
  )
  credential <- fabric_credential(
    tenant_id = tenant_id,
    client_id = client_id,
    access_token = access_token,
    token_provider = token_provider
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
#' Lists items in a workspace with optional server-side item-type filtering.
#' Set `detail = TRUE` to call each supported workload API and include
#' workload-specific connection metadata.
#'
#' @param workspace Workspace GUID, exact display name, or a workspace record
#'   returned by [fabric_workspaces()].
#' @param type Optional Fabric item type, for example `"Lakehouse"` or
#'   `"Warehouse"`.
#' @param detail Logical. Retrieve workload-specific properties for supported
#'   types.
#' @param recursive Logical. Include items in nested folders.
#' @inheritParams fabric_workspaces
#'
#' @return A tibble with one row per item. `properties` and `raw` are list
#'   columns. Enriched rows also contain directly usable SQL, OneLake, DAX,
#'   Livy, and KQL fields where Fabric exposes them.
#' @details Workload enrichment requires `Item.Read.All`/`Item.ReadWrite.All`
#'   or the corresponding workload-specific read scope in addition to access
#'   to the item.
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
  access_token = NULL,
  token_provider = NULL,
  api_base = .fabric_api_base
) {
  if (!is.null(type)) {
    stopifnot(is.character(type), length(type) == 1L, nzchar(type))
  }
  stopifnot(
    is.logical(detail),
    length(detail) == 1L,
    !is.na(detail),
    is.logical(recursive),
    length(recursive) == 1L,
    !is.na(recursive)
  )
  credential <- fabric_credential(
    tenant_id = tenant_id,
    client_id = client_id,
    access_token = access_token,
    token_provider = token_provider
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
#' Resolves an item by GUID or by an exact/unique display name and retrieves
#' workload-specific properties when supported.
#'
#' @param item Item GUID, exact display name, or a one-row item record returned
#'   by a discovery function.
#' @inheritParams fabric_items
#'
#' @return A `fabric_item` list containing common metadata, workload
#'   properties, and derived connection targets.
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
  access_token = NULL,
  token_provider = NULL,
  api_base = .fabric_api_base
) {
  credential <- fabric_credential(
    tenant_id = tenant_id,
    client_id = client_id,
    access_token = access_token,
    token_provider = token_provider
  )
  base <- fabric_api_base(api_base)
  ws <- fabric_resolve_workspace(workspace, credential, base)

  supplied <- fabric_as_record(item)
  if (!is.null(supplied)) {
    record <- supplied
  } else {
    stopifnot(is.character(item), length(item) == 1L, nzchar(item))
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
  record$workspaceId <- record$workspaceId %||% ws$id
  record$workspaceDisplayName <- record$workspaceDisplayName %||%
    ws$displayName
  if (!is.null(type) && !identical(tolower(record$type), tolower(type))) {
    stop(
      sprintf(
        "Item '%s' has type '%s', not '%s'.",
        record$displayName %||% record$id,
        record$type,
        type
      ),
      call. = FALSE
    )
  }
  structure(
    fabric_enrich_item(record, credential, base),
    class = c("fabric_item", "list")
  )
}

#' Typed Microsoft Fabric item discovery
#'
#' These helpers are equivalent to [fabric_items()] with an item type and
#' `detail = TRUE`. They return workload-specific properties and derived
#' connection targets.
#'
#' @inheritParams fabric_items
#' @param ... Authentication and API arguments forwarded to [fabric_items()].
#' @return A tibble of enriched Fabric items.
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
  stopifnot(
    is.character(api_base),
    length(api_base) == 1L,
    nzchar(api_base)
  )
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
      stop("A discovered object must contain exactly one row.", call. = FALSE)
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
  stopifnot(
    is.character(workspace),
    length(workspace) == 1L,
    nzchar(workspace)
  )
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
    stop(
      sprintf("%s '%s' was not found.", tools::toTitleCase(kind), name),
      call. = FALSE
    )
  }
  if (length(matches) > 1L) {
    stop(
      sprintf(
        "%s name '%s' is ambiguous (%d matches). Use its GUID.",
        tools::toTitleCase(kind),
        name,
        length(matches)
      ),
      call. = FALSE
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
    "one_lake_tables_path",
    "one_lake_files_path",
    "dax_connection_string",
    "livy_url",
    "query_service_uri",
    "ingestion_service_uri"
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
