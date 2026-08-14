.fabric_api_base <- "https://api.fabric.microsoft.com/v1"

#' Discover Microsoft Fabric workspaces
#'
#' Returns the Fabric workspaces available to the signed-in user or application
#' Use the result to choose a workspace for [fabric_items()] or one of the typed
#' discovery helpers
#'
#' @param roles Optional workspace roles to include, such as `"Viewer"`,
#'   `"Contributor"`, `"Member"`, or `"Admin"`. Leave `NULL` to return every
#'   visible workspace
#' @param prefer_workspace_endpoints Whether to request workspace-specific
#'   endpoints. Keep `FALSE` unless your organization uses workspace-level
#'   private links
#' @param tenant_id Microsoft Entra tenant ID. Defaults to
#'   `FABRICQUERYR_TENANT_ID`
#' @param client_id Microsoft Entra application/client ID. Defaults to
#'   `FABRICQUERYR_CLIENT_ID`, then the Azure CLI application ID
#' @param token Optional access token or token-provider function. Leave `NULL`
#'   to let fabricQueryR use its normal sign-in flow
#' @param auth_args Additional sign-in options passed to
#'   [AzureAuth::get_azure_token()]
#' @param api_base Fabric REST API base URL. Leave unchanged unless using a
#'   different Fabric cloud or a test service
#' @param allow_custom_endpoint Logical. Set to `TRUE` only when `api_base` is
#'   a non-Microsoft HTTPS origin that you trust to receive a Fabric token
#'
#' @return A list with one workspace record per visible workspace. Each record
#'   includes its ID and display name, together with other details returned by
#'   Fabric
#' @details
#' The caller needs permission to read Fabric workspaces. Discovery uses the
#' Fabric API and requires `Workspace.Read.All` or `Workspace.ReadWrite.All`
#' @references
#' [List workspaces REST API](https://learn.microsoft.com/en-us/rest/api/fabric/core/workspaces/list-workspaces)
#'
#' [Workspace roles](https://learn.microsoft.com/en-us/fabric/fundamentals/roles-workspaces)
#' @examples
#' \dontrun{
#' # Sign in and list every Fabric workspace you can access.
#' workspaces <- fabric_workspaces()
#'
#' # Inspect the names before choosing a workspace record.
#' vapply(workspaces, `[[`, character(1), "displayName")
#' workspace <- workspaces[[1L]]
#'
#' # Pass the discovered record directly to the next discovery step.
#' items <- fabric_items(workspace)
#' }
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

  # Check filtering options before signing in or sending a request

  if (
    !is.null(roles) &&
      (!is.character(roles) ||
        !length(roles) ||
        anyNA(roles) ||
        !all(nzchar(roles)))
  ) {
    .fabric_abort("roles must contain one or more non-empty strings")
  }

  if (
    !is.logical(prefer_workspace_endpoints) ||
      length(prefer_workspace_endpoints) != 1L ||
      is.na(prefer_workspace_endpoints)
  ) {
    .fabric_abort("prefer_workspace_endpoints must be TRUE or FALSE")
  }

  # 2 Request visible workspaces -------------------------------------------------------------------

  # Use one shared credential for every page returned by Fabric

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

  # Add a small class while keeping every field returned by Fabric

  fabric_workspace_list(records)
}

#' Discover Microsoft Fabric items
#'
#' Returns the Lakehouses, Warehouses, semantic models, notebooks, and other
#' items stored in a workspace. Set `detail = TRUE` when you want records that
#' can be passed directly to query or connection functions
#'
#' @param workspace Workspace name, ID, or record returned by
#'   [fabric_workspaces()]. A name is convenient for interactive use; a record
#'   avoids an extra lookup
#' @param type Optional Fabric API item type, for example `"Lakehouse"`,
#'   `"Warehouse"`, `"SemanticModel"`, or `"Notebook"`. Matching is done by
#'   Fabric, so use the API spelling. Leave `NULL` to list all item types
#' @param detail Whether to retrieve connection details as well as names and
#'   IDs. This takes more requests and may require additional permissions. The
#'   typed discovery helpers use `TRUE` by default
#' @param detail_errors What to do if some connection details cannot be read
#'   `"record"` returns the available information and stores an error message
#'   with the affected item; `"abort"` stops the call
#' @param recursive Logical. `TRUE` includes items inside workspace folders;
#'   `FALSE` lists only items at the workspace root
#' @param personal_workspace_tenant_id Optional Microsoft Entra tenant ID used
#'   to build the XMLA endpoint for a Personal workspace
#' @param personal_workspace_owner Optional owner UPN or Entra object ID used
#'   to build the XMLA endpoint for a Personal workspace. Microsoft Fabric's
#'   workspace API does not return either personal-workspace identifier, so
#'   supply this together with `personal_workspace_tenant_id` when a
#'   `dax_connection_string` is needed for a semantic model in My Workspace
#' @inheritParams fabric_workspaces
#' @param api_base Fabric REST API base URL. When `workspace` is a record
#'   containing `apiEndpoint`, that workspace-specific endpoint is used unless
#'   `api_base` is supplied explicitly
#'
#' @return A list with one item record per match. Every record includes common
#'   fields such as `id`, `displayName`, `type`, and `workspaceId`. With
#'   `detail = TRUE`, records also include the connection details needed by the
#'   matching fabricQueryR functions when Fabric makes them available
#' @details
#' The caller needs at least access to the workspace (the Viewer role is
#' sufficient for the core list operation). Workload enrichment additionally
#' requires `Item.Read.All`/`Item.ReadWrite.All` or the corresponding
#' workload-specific read scope and access to the item
#' Personal-workspace semantic models use Microsoft's v2 XMLA endpoint and
#' require both `personal_workspace_tenant_id` and `personal_workspace_owner`
#'
#' @references
#' [List items REST API](https://learn.microsoft.com/en-us/rest/api/fabric/core/items/list-items)
#'
#' [Fabric item management overview](https://learn.microsoft.com/en-us/rest/api/fabric/articles/item-management/item-management-overview)
#'
#' [Personal-workspace XMLA endpoints](https://learn.microsoft.com/en-us/fabric/enterprise/powerbi/service-premium-connect-tools#connecting-to-a-personal-workspace)
#' @examples
#' \dontrun{
#' # Start by discovering a workspace instead of copying its ID.
#' workspaces <- fabric_workspaces()
#' workspace <- workspaces[[1L]]
#'
#' # List lightweight item records and inspect their names and types.
#' items <- fabric_items(workspace)
#' vapply(items, `[[`, character(1), "displayName")
#'
#' # Ask for enriched records when another function needs connection details.
#' lakehouses <- fabric_items(
#'   workspace,
#'   type = "Lakehouse",
#'   detail = TRUE
#' )
#' }
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

  # Reject invalid filters before resolving a workspace or making API calls

  api_base_supplied <- !missing(api_base)
  if (
    !is.null(type) &&
      (!is.character(type) ||
        length(type) != 1L ||
        is.na(type) ||
        !nzchar(type))
  ) {
    .fabric_abort("type must be one non-empty string")
  }

  if (!is.logical(detail) || length(detail) != 1L || is.na(detail)) {
    .fabric_abort("detail must be TRUE or FALSE")
  }
  detail_errors <- match.arg(detail_errors)
  if (!is.logical(recursive) || length(recursive) != 1L || is.na(recursive)) {
    .fabric_abort("recursive must be TRUE or FALSE")
  }
  fabric_validate_personal_workspace_identity(
    personal_workspace_tenant_id,
    personal_workspace_owner
  )

  # 2 Resolve authentication and workspace ---------------------------------------------------------

  # A discovered workspace record can provide a workspace-specific API base

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
  # continuation links before enrichment starts

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
  # not discard the other items unless the caller requested strict errors

  records <- lapply(records, function(record) {
    record <- fabric_add_workspace_context(
      record,
      ws,
      personal_workspace_tenant_id,
      personal_workspace_owner
    )
    record <- fabric_add_workspace_endpoints(record, ws)

    # Detailed enrichment can fail per item without losing the base record
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

  # Summarize partial enrichment without discarding usable records
  failed <- sum(vapply(
    records,
    function(record) !is.null(record$detail_error),
    logical(1)
  ))
  if (failed > 0) {
    .fabric_warn(
      c(
        "Could not fully enrich {failed} Fabric item{?s}",
        "i" = "See the {.field detail_error} field for each affected item"
      ),
      .format = TRUE
    )
  }

  # 5 Return discovery records ---------------------------------------------------------------------

  # Return discovery records in the stable form expected by the caller

  fabric_item_list(records)
}

#' Discover one Microsoft Fabric item
#'
#' Finds one item and returns the connection details needed by fabricQueryR
#' Use this when you know the item's name or ID and do not need to list every
#' item in the workspace
#'
#' @param item Item GUID, exact display name, or an item record returned
#'   by a discovery function. A display name must identify exactly one item of
#'   the requested `type`; use a GUID or discovered record when names are
#'   duplicated
#' @inheritParams fabric_items
#'
#' @return One `fabric_item` record containing the item's name, ID, type,
#'   workspace, and any connection details that Fabric makes available
#' @details
#' The caller needs access to the workspace for the core item lookup. This
#' singular helper always performs workload-specific enrichment as well, which
#' additionally requires `Item.Read.All`/`Item.ReadWrite.All` or the applicable
#' workload-specific read scope and access to the item. Use
#' [fabric_items()] with `detail = FALSE` when only core item metadata is needed
#' @examples
#' \dontrun{
#' # Discover a workspace and obtain a lightweight Warehouse record.
#' workspace <- fabric_workspaces()[[1L]]
#' warehouses <- fabric_items(workspace, type = "Warehouse")
#'
#' # Enrich that discovered record with connection details.
#' warehouse <- fabric_item(workspace, warehouses[[1L]])
#'
#' # The result can be passed directly to SQL helpers.
#' fabric_sql_connection_info(warehouse)
#' }
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

  # Establish the workspace first because both item lookup routes need its ID

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
  # paged name search only when the caller supplied a display name

  supplied <- fabric_as_record(item)
  if (!is.null(supplied)) {
    record <- supplied
  } else {
    if (
      !is.character(item) || length(item) != 1L || is.na(item) || !nzchar(item)
    ) {
      .fabric_abort("item must be one non-empty string or a discovered item")
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

  # Add workspace context before deriving service-specific connection targets

  fabric_validate_item_workspace(record, ws$id)
  record <- fabric_add_workspace_context(
    record,
    ws,
    personal_workspace_tenant_id,
    personal_workspace_owner
  )
  record <- fabric_add_workspace_endpoints(record, ws)
  if (!is.null(type) && !identical(tolower(record$type), tolower(type))) {
    .fabric_abort(
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
#' `detail = FALSE` when you only need names and IDs
#'
#' @section Choosing a helper:
#' - `fabric_lakehouses()`, `fabric_warehouses()`, and
#'   `fabric_warehouse_snapshots()` find data stores that can be queried through
#'   [fabric_sql_query()]; Lakehouses can also be accessed through OneLake and
#'   Livy
#' - `fabric_sql_databases()` finds transactional Fabric SQL databases
#' - `fabric_semantic_models()` finds the business models queried with DAX via
#'   [fabric_pbi_dax_query()]
#' - `fabric_eventhouses()` and `fabric_kql_databases()` find real-time data
#'   stores queried with [fabric_kql_query()]
#' - `fabric_notebooks()` finds notebooks that can be run with
#'   [fabric_job_run()]
#' - `fabric_data_pipelines()` and `fabric_spark_job_definitions()` find the
#'   other executable items supported by [fabric_job_run()]
#' - `fabric_environments()` finds reusable Spark runtime configurations
#' - `fabric_user_data_functions()` finds serverless Python function items
#' - `fabric_graphql_apis()` finds APIs configured in Fabric for use with
#'   [fabric_graphql_query()]
#'
#' @section Filtering and returned fields:
#' Each helper requests its exact Fabric item type and verifies that every
#' returned record has that type. The records otherwise keep all fields
#' returned by Fabric, including fields added by the service in the future
#'
#' Folder recursion, workspace-specific private-link routing, authentication,
#' and `detail_errors` have the same behavior as in [fabric_items()]. The four
#' new helpers do not currently make workload-specific detail requests. Their
#' core records contain the IDs and type needed by [fabric_job_run()] where
#' applicable, and fabricQueryR does not yet consume an additional target from
#' Environment or User Data Function details
#'
#' @inheritParams fabric_items
#' @param ... Authentication and API arguments forwarded to [fabric_items()]
#'   Do not supply `type`; each helper sets that value
#' @return A list with one `fabric_item` object per matching item. Each object
#'   contains common item metadata and applicable connection fields. See
#'   [fabric_items()] for details
#' @references
#' [List items REST API](https://learn.microsoft.com/en-us/rest/api/fabric/core/items/list-items)
#'
#' [List data pipelines](https://learn.microsoft.com/en-us/rest/api/fabric/datapipeline/items/list-data-pipelines)
#'
#' [List Spark job definitions](https://learn.microsoft.com/en-us/rest/api/fabric/sparkjobdefinition/items/list-spark-job-definitions)
#'
#' [List environments](https://learn.microsoft.com/en-us/rest/api/fabric/environment/items/list-environments)
#'
#' [List User Data Functions](https://learn.microsoft.com/en-us/rest/api/fabric/userdatafunction/items/list-user-data-functions)
#' @examples
#' \dontrun{
#' # Discover a workspace once, then reuse its record for typed discovery.
#' workspace <- fabric_workspaces()[[1]]
#'
#' # Discover data items that feed the package's query and storage helpers.
#' lakehouses <- fabric_lakehouses(workspace)
#' warehouses <- fabric_warehouses(workspace)
#' snapshots <- fabric_warehouse_snapshots(workspace)
#' sql_databases <- fabric_sql_databases(workspace)
#' semantic_models <- fabric_semantic_models(workspace)
#' eventhouses <- fabric_eventhouses(workspace)
#' kql_databases <- fabric_kql_databases(workspace)
#' graphql_apis <- fabric_graphql_apis(workspace)
#'
#' # A discovered record can be passed directly to a matching helper.
#' fabric_lakehouse_tables(lakehouses[[1L]])
#' fabric_sql_connection_info(warehouses[[1L]])
#' fabric_pbi_dax_query(
#'   semantic_models[[1L]],
#'   dax = Sys.getenv("FABRIC_DAX_QUERY")
#' )
#'
#' # Discover executable items that can be passed to fabric_job_run().
#' notebook <- fabric_notebooks(workspace)[[1]]
#' pipeline <- fabric_data_pipelines(workspace)[[1]]
#' spark_job <- fabric_spark_job_definitions(workspace)[[1]]
#'
#' fabric_job_wait(fabric_job_run(notebook), timeout = 900)
#' fabric_job_wait(fabric_job_run(pipeline), timeout = 900)
#' fabric_job_wait(fabric_job_run(spark_job), timeout = 900)
#'
#' # Discover supporting Spark and serverless-function items as well.
#' environments <- fabric_environments(workspace)
#' functions <- fabric_user_data_functions(workspace)
#' }
#' @name fabric_typed_items
NULL

#' @rdname fabric_typed_items
#' @export
fabric_lakehouses <- function(workspace, detail = TRUE, ...) {
  fabric_typed_item_list(workspace, "Lakehouse", detail, ...)
}

#' @rdname fabric_typed_items
#' @export
fabric_warehouses <- function(workspace, detail = TRUE, ...) {
  fabric_typed_item_list(workspace, "Warehouse", detail, ...)
}

#' @rdname fabric_typed_items
#' @export
fabric_warehouse_snapshots <- function(workspace, detail = TRUE, ...) {
  fabric_typed_item_list(workspace, "WarehouseSnapshot", detail, ...)
}

#' @rdname fabric_typed_items
#' @export
fabric_sql_databases <- function(workspace, detail = TRUE, ...) {
  fabric_typed_item_list(workspace, "SQLDatabase", detail, ...)
}

#' @rdname fabric_typed_items
#' @export
fabric_semantic_models <- function(workspace, detail = TRUE, ...) {
  fabric_typed_item_list(workspace, "SemanticModel", detail, ...)
}

#' @rdname fabric_typed_items
#' @export
fabric_eventhouses <- function(workspace, detail = TRUE, ...) {
  fabric_typed_item_list(workspace, "Eventhouse", detail, ...)
}

#' @rdname fabric_typed_items
#' @export
fabric_kql_databases <- function(workspace, detail = TRUE, ...) {
  fabric_typed_item_list(workspace, "KQLDatabase", detail, ...)
}

#' @rdname fabric_typed_items
#' @export
fabric_notebooks <- function(workspace, detail = TRUE, ...) {
  fabric_typed_item_list(workspace, "Notebook", detail, ...)
}

#' @rdname fabric_typed_items
#' @export
fabric_data_pipelines <- function(workspace, detail = TRUE, ...) {
  fabric_typed_item_list(workspace, "DataPipeline", detail, ...)
}

#' @rdname fabric_typed_items
#' @export
fabric_spark_job_definitions <- function(workspace, detail = TRUE, ...) {
  fabric_typed_item_list(workspace, "SparkJobDefinition", detail, ...)
}

#' @rdname fabric_typed_items
#' @export
fabric_environments <- function(workspace, detail = TRUE, ...) {
  fabric_typed_item_list(workspace, "Environment", detail, ...)
}

#' @rdname fabric_typed_items
#' @export
fabric_user_data_functions <- function(workspace, detail = TRUE, ...) {
  fabric_typed_item_list(workspace, "UserDataFunction", detail, ...)
}

#' @rdname fabric_typed_items
#' @export
fabric_graphql_apis <- function(workspace, detail = TRUE, ...) {
  fabric_typed_item_list(workspace, "GraphQLApi", detail, ...)
}

# Request one Fabric item type and discard any mismatched service records
# Returns the original item objects so new service fields remain available
fabric_typed_item_list <- function(workspace, type, detail, ...) {
  items <- fabric_items(workspace, type = type, detail = detail, ...)
  items[vapply(
    items,
    function(item) identical(fabric_record_value(item, "type"), type),
    logical(1)
  )]
}

# Add workspace-specific API and OneLake endpoints to `record`. Returns the
# updated item record for the discovery functions
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
# used for personal-workspace identity fields
fabric_discovery_optional_string <- function(value, name) {
  if (
    !is.null(value) &&
      (!is.character(value) ||
        length(value) != 1L ||
        is.na(value) ||
        !nzchar(value))
  ) {
    .fabric_abort(paste0(name, " must be NULL or one non-empty string"))
  }
  invisible(value)
}

# Check the optional personal-workspace `tenant_id` and `owner` as a pair
# Returns invisibly for the item discovery entry points
fabric_validate_personal_workspace_identity <- function(tenant_id, owner) {
  fabric_discovery_optional_string(
    tenant_id,
    "personal_workspace_tenant_id"
  )
  fabric_discovery_optional_string(owner, "personal_workspace_owner")
  if (xor(is.null(tenant_id), is.null(owner))) {
    .fabric_abort(paste0(
      "personal_workspace_tenant_id and personal_workspace_owner ",
      "must be supplied together"
    ))
  }
  invisible(TRUE)
}

# Add workspace IDs, names, and personal-workspace identity to `record`
# Returns the updated item record before endpoint enrichment
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
# supplied discovery record from being used with the wrong workspace
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
    .fabric_abort(
      "The discovered item belongs to a different workspace"
    )
  }
  invisible(TRUE)
}

# Normalize and validate `api_base`. Returns a trusted Fabric v1 base URL used
# by all discovery requests
fabric_api_base <- function(api_base, allow_custom_endpoint = FALSE) {
  # 1 Parse the endpoint ---------------------------------------------------------------------------

  # Only a complete HTTPS URL can safely receive an access token

  if (
    !is.logical(allow_custom_endpoint) ||
      length(allow_custom_endpoint) != 1L ||
      is.na(allow_custom_endpoint)
  ) {
    .fabric_abort("allow_custom_endpoint must be TRUE or FALSE")
  }

  if (
    !is.character(api_base) ||
      length(api_base) != 1L ||
      is.na(api_base) ||
      !nzchar(api_base)
  ) {
    .fabric_abort("api_base must be one non-empty string")
  }

  # Parse the URL once, then inspect every part of its origin
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
    .fabric_abort(
      paste0(
        "api_base must be an HTTPS origin using the default port (443), ",
        "with an optional /v1 path"
      ),
      class = "fabric_api_endpoint_error"
    )
  }

  # Custom hosts require an explicit trust decision from the caller
  if (
    !fabric_host_matches(host, "api.fabric.microsoft.com") &&
      !isTRUE(allow_custom_endpoint)
  ) {
    .fabric_abort(
      paste0(
        "Refusing to send a Fabric token to untrusted api_base '",
        api_base,
        "'; set allow_custom_endpoint = TRUE only for an endpoint you trust"
      ),
      class = "fabric_api_endpoint_error"
    )
  }

  # 2 Normalize the API version --------------------------------------------------------------------

  # Normalize the API version so later branches do not repeat the same conversion

  if (identical(tolower(path), "/v1")) endpoint else paste0(endpoint, "/v1")
}

# Test whether `value` is one GUID string. Returns a logical value used to
# choose direct-ID lookups instead of name searches
fabric_is_guid <- function(value) {
  grepl(
    "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    value,
    ignore.case = TRUE
  )
}

# Convert a discovered object or plain named list into a record. Returns `NULL`
# for ordinary text inputs so public functions can follow their text path
fabric_as_record <- function(value) {
  if (inherits(value, "data.frame")) {
    if (nrow(value) != 1L) {
      .fabric_abort("A discovered object must contain exactly one row")
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
# or `NULL`, allowing helpers to accept API and R-style field names
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
# caller's base URL. Returns a normalized v1 URL for workspace requests
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
    .fabric_abort(
      paste0(
        "The workspace apiEndpoint must be an HTTPS origin using the ",
        "default port (443), with an optional /v1 path"
      )
    )
  }

  if (identical(tolower(path), "/v1")) endpoint else paste0(endpoint, "/v1")
}

# Check whether `hostname` equals or is below `suffix`. Returns one logical
# value used by endpoint trust checks throughout the package
fabric_host_matches <- function(hostname, suffix) {
  hostname <- tolower(hostname %||% "")
  suffix <- tolower(suffix)
  identical(hostname, suffix) || endsWith(hostname, paste0(".", suffix))
}

# Resolve a workspace record, GUID, or exact name. Returns a normalized
# workspace context used by item discovery and job operations
fabric_resolve_workspace <- function(
  workspace,
  credential,
  api_base,
  use_workspace_endpoint = TRUE
) {
  # 1 Reuse a supplied record ----------------------------------------------------------------------

  # A discovered record may already contain a workspace-specific API endpoint

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

  # GUIDs can be used directly; display names require a paged workspace list

  if (
    !is.character(workspace) ||
      length(workspace) != 1L ||
      is.na(workspace) ||
      !nzchar(workspace)
  ) {
    .fabric_abort(
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

  # Build workspace context from the validated values required by the next step

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
# record or explains missing/duplicate names to discovery callers
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
    .fabric_abort(
      sprintf("%s '%s' was not found", tools::toTitleCase(kind), name)
    )
  }

  if (length(matches) > 1L) {
    .fabric_abort(
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
# or `NULL` when that item type has no supported detail endpoint
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
# enriched discovery record used by `fabric_item()` and `fabric_items()`
fabric_enrich_item <- function(
  record,
  credential,
  api_base,
  private_sql_errors = c("abort", "record")
) {
  # 1 Fetch workload details -----------------------------------------------------------------------

  # Enrich supported item types with their workload-specific API record

  private_sql_errors <- match.arg(private_sql_errors)
  route <- fabric_item_route(record$type %||% "")

  if (!is.null(route)) {
    # Workload detail endpoints add fields missing from the collection result
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

    # Preserve workspace context when service details replace record fields
    workspace_name <- record$workspaceDisplayName
    record <- utils::modifyList(record, detail)
    record$workspaceDisplayName <- workspace_name

    # Private SQL lookup may be recorded per item instead of stopping the list
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

  # 2 Add derived targets --------------------------------------------------------------------------

  # Finish with the connection fields shared by all discovery workflows

  fabric_add_derived_targets(record, api_base)
}

# Read private-link SQL details for `record`. Returns the record with a private
# endpoint when a workspace-specific API base requires one
fabric_enrich_private_sql_target <- function(record, credential, api_base) {
  # 1 Check whether private details are needed -----------------------------------------------------

  # Public API origins already return their normal SQL connection details

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

  # Request the private SQL endpoint only when the workspace route requires it

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
    .fabric_abort(
      "Fabric returned an invalid workspace-private SQL connection string"
    )
  }

  # 3 Add the private connection target ------------------------------------------------------------

  # Add the private connection target while the source context is still available

  if (identical(type, "warehouse")) {
    record$properties$connectionString <- connection_string
  } else {
    record$properties$sqlEndpointProperties$connectionString <- connection_string
  }
  record$sql_private_link_type <- "Workspace"
  record
}

# Derive convenient SQL, DAX, KQL, GraphQL, and OneLake targets from `record`
# Returns the record used directly by the package's query functions
fabric_add_derived_targets <- function(record, api_base) {
  # 1 Add shared item context ----------------------------------------------------------------------

  # Keep the API properties intact while adding stable, R-friendly field names

  properties <- record$properties %||% list()
  type <- tolower(record$type %||% "")
  record$properties <- properties

  # 2 Add workload-specific targets ----------------------------------------------------------------

  # Add workload-specific targets while the source context is still available

  if (type == "lakehouse") {
    # Lakehouses expose OneLake paths, a SQL endpoint, and a Livy endpoint
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
    # Warehouses use their display name as the SQL database
    record$sql_server <- properties$connectionString
    record$sql_database <- record$displayName
  } else if (type == "sqldatabase") {
    # SQL databases publish server and database names separately
    record$sql_connection_string <- properties$connectionString
    record$sql_server <- properties$serverFqdn
    record$sql_database <- properties$databaseName
  } else if (type == "semanticmodel") {
    # Semantic models need an XMLA-style server plus the model catalog name
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
    # KQL workloads publish query and ingestion endpoints
    record$query_service_uri <- properties$queryServiceUri
    record$ingestion_service_uri <- properties$ingestionServiceUri
  } else if (type == "graphqlapi") {
    # GraphQL items use the standard Fabric API route
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

  # Return the enriched record in the stable form expected by the caller

  record
}

# Give each workspace API record its package class. Returns a list used by
# `fabric_workspaces()` while preserving all original fields
fabric_workspace_list <- function(records) {
  lapply(records, function(record) {
    structure(record, class = c("fabric_workspace", "list"))
  })
}

# Give each item API record its package class. Returns a list used by all item
# discovery entry points while preserving all original fields
fabric_item_list <- function(records) {
  lapply(records, function(record) {
    structure(record, class = c("fabric_item", "list"))
  })
}
