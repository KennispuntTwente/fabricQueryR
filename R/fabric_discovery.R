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
#'   API and OneLake endpoints. When `TRUE`, each listed workspace is hydrated
#'   with Get Workspace because List Workspaces returns only the API endpoint.
#'   Keep `FALSE` unless your organization uses workspace-level private links
#' @param tenant_id Microsoft Entra tenant ID. Defaults to
#'   `FABRICQUERYR_TENANT_ID`
#' @param client_id Microsoft Entra application/client ID. Defaults to
#'   `FABRICQUERYR_CLIENT_ID`, then the Azure CLI application ID
#' @param token Optional access token or token-provider function. Leave `NULL`
#'   to let 'fabricQueryR' use its normal sign-in flow
#' @param auth_args Additional sign-in options passed to
#'   [AzureAuth::get_azure_token()]
#' @param api_base Fabric REST API base URL. Leave unchanged unless using a
#'   different Fabric cloud or a test service
#' @param output Discovery record representation. The default `"r6"` returns
#'   R6 objects with type-specific methods. Use `"list"` when a plain record is
#'   specifically required
#'
#' @return A list with one workspace object per visible workspace. With
#'   `output = "r6"`, each object is a [FabricWorkspace]. With
#'   `output = "list"`, each object is a `fabric_workspace` list. Both
#'   representations preserve all fields returned by Fabric
#' @details
#' The caller needs permission to read Fabric workspaces. Discovery uses the
#' Fabric API and requires `Workspace.Read.All` or `Workspace.ReadWrite.All`
#' @references
#' [List workspaces REST API](https://learn.microsoft.com/en-us/rest/api/fabric/core/workspaces/list-workspaces)
#'
#' [Get workspace REST API](https://learn.microsoft.com/en-us/rest/api/fabric/core/workspaces/get-workspace)
#'
#' [Workspace roles](https://learn.microsoft.com/en-us/fabric/fundamentals/roles-workspaces)
#' @examples
#' \dontrun{
#' # Sign in and list every Fabric workspace you can access
#' workspaces <- fabric_workspaces()
#'
#' # Inspect a field before choosing a workspace
#' vapply(workspaces, `[[`, character(1), "displayName")
#' workspace <- workspaces[[1L]]
#' workspace$displayName
#'
#' # Object methods call the corresponding exported functions
#' # workspace$items() -> fabric_items(workspace)
#' items <- workspace$items()
#' # workspace$lakehouses() -> fabric_lakehouses(workspace)
#' lakehouse <- workspace$lakehouses()[[1L]]
#' # lakehouse$tables() -> fabric_lakehouse_tables(lakehouse)
#' lakehouse$tables()
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
  output = c("r6", "list")
) {
  # 1 Validate inputs ------------------------------------------------------------------------------

  # Check filtering options before signing in or sending a request

  output <- .fabric_r6_output(output)
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

  base <- fabric_api_base(api_base)
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
  if (isTRUE(prefer_workspace_endpoints)) {
    records <- lapply(records, function(record) {
      fabric_workspace_details(
        record,
        credential,
        base,
        prefer_workspace_endpoints = TRUE
      )
    })
  }

  # 3 Return discovery records ---------------------------------------------------------------------

  # Add a small class while keeping every field returned by Fabric

  fabric_workspace_list(
    records,
    output = output,
    credential = credential,
    api_base = base
  )
}

#' Discover Microsoft Fabric items
#'
#' Returns the Lakehouses, Warehouses, semantic models, notebooks, and other
#' items stored in a workspace. Every item type returned by Fabric's core list
#' API can be represented. Where the package has a specialized R6 subclass,
#' its methods perform the matching query, connection, file, Spark, or job
#' operations; other types remain complete generic [FabricItem] records
#'
#' @param workspace Workspace name, ID, or object returned by
#'   [fabric_workspaces()]. A name is convenient for interactive use; an object
#'   avoids an extra lookup
#' @param type Optional Fabric API item type, for example `"Lakehouse"`,
#'   `"Warehouse"`, `"SemanticModel"`, or `"Notebook"`. Matching is done by
#'   Fabric, so use the API spelling. Leave `NULL` to list all item types
#' @param detail Whether to retrieve connection details as well as names and
#'   IDs. This takes more requests and may require additional permissions. For
#'   `fabric_item()`, `NULL` enriches every supported type except User Data
#'   Functions, whose detail endpoint does not support application identities.
#'   The typed Semantic Model, GraphQL, and User Data Function helpers also
#'   default to lightweight records
#' @param detail_errors What to do if some connection details cannot be read
#'   `"record"` returns the available information and stores an error message
#'   with the affected item; `"abort"` stops the call
#' @param recursive Logical. `TRUE` includes items inside workspace folders;
#'   `FALSE` lists only items at the workspace root
#' @param root_folder_id Optional Fabric folder GUID used as the root of the
#'   listing. With `recursive = FALSE`, only direct children are returned;
#'   with `TRUE`, nested folders are included
#' @param include Optional character vector of additional item properties to
#'   request. Fabric currently documents `"DefaultIdentity"`; values are sent
#'   as the API's comma-separated `include` query parameter
#' @param personal_workspace_tenant_id Optional Microsoft Entra tenant ID used
#'   to build the XMLA endpoint for a Personal workspace
#' @param personal_workspace_owner Optional owner UPN or Entra object ID used
#'   to build the XMLA endpoint for a Personal workspace. Microsoft Fabric's
#'   workspace API does not return either personal-workspace identifier, so
#'   supply this together with `personal_workspace_tenant_id` when a
#'   `dax_connection_string` is needed for a semantic model in My Workspace
#' @inheritParams fabric_workspaces
#' @param api_base Fabric REST API base URL. When `workspace` is an object
#'   containing `apiEndpoint`, that workspace-specific endpoint is used unless
#'   `api_base` is supplied explicitly
#'
#' @return A list with one item object per match. Every object includes common
#'   fields such as `id`, `displayName`, `type`, and `workspaceId`. With
#'   `output = "r6"`, results are [FabricItem] objects or type-specific
#'   subclasses. With `output = "list"`, results are `fabric_item` lists. With
#'   `detail = TRUE`, both representations include connection details when
#'   Fabric makes them available
#' @details
#' The caller needs at least access to the workspace (the Viewer role is
#' sufficient for the core list operation). Workload enrichment additionally
#' requires `Item.Read.All`/`Item.ReadWrite.All` or the corresponding
#' workload-specific read scope and access to the item
#' Personal-workspace semantic models use Microsoft's v2 XMLA endpoint and
#' require both `personal_workspace_tenant_id` and `personal_workspace_owner`
#'
#' @section Generic and typed discovery:
#' `fabric_items()` and `workspace$items()` are the broad, future-compatible
#' discovery interfaces. Their optional `type` filter is passed to Fabric, and
#' item types without package-specific methods are returned as [FabricItem]
#' objects with all service fields, `$details()`, and `$as_list()`.
#'
#' The helpers documented in [fabric_typed_items] are an intentional
#' convenience subset of Fabric's larger and evolving item catalog. A typed
#' helper means that the package knows the item-type spelling and workload Get
#' route; it does not necessarily mean that the result has its own R6 subclass.
#' See [fabric_typed_items] for the exact support matrix
#'
#' @references
#' [List items REST API](https://learn.microsoft.com/en-us/rest/api/fabric/core/items/list-items)
#'
#' [Fabric item management overview](https://learn.microsoft.com/en-us/rest/api/fabric/articles/item-management/item-management-overview)
#'
#' [Personal-workspace XMLA endpoints](https://learn.microsoft.com/en-us/fabric/enterprise/powerbi/service-premium-connect-tools#connecting-to-a-personal-workspace)
#' @examples
#' \dontrun{
#' # Start by discovering a workspace instead of copying its ID
#' workspaces <- fabric_workspaces()
#' workspace <- workspaces[[1L]]
#'
#' # `$items()` is the object interface to fabric_items()
#' items <- workspace$items()
#' vapply(items, `[[`, character(1), "displayName")
#'
#' # `$lakehouses()` calls fabric_lakehouses(); `$tables()` calls
#' # fabric_lakehouse_tables()
#' lakehouse <- workspace$lakehouses()[[1L]]
#' lakehouse$tables()
#' }
#' @export
fabric_items <- function(
  workspace,
  type = NULL,
  detail = FALSE,
  detail_errors = c("record", "abort"),
  recursive = TRUE,
  root_folder_id = NULL,
  include = NULL,
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
  output = c("r6", "list")
) {
  # 1 Validate inputs ------------------------------------------------------------------------------

  # Reject invalid filters before resolving a workspace or making API calls

  output <- .fabric_r6_output(output)
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
  if (!is.null(root_folder_id)) {
    if (
      !is.character(root_folder_id) ||
        length(root_folder_id) != 1L ||
        is.na(root_folder_id) ||
        !fabric_is_guid(root_folder_id)
    ) {
      .fabric_abort("root_folder_id must be NULL or a Fabric folder GUID")
    }
  }
  include <- fabric_normalize_item_include(include)
  fabric_validate_personal_workspace_identity(
    personal_workspace_tenant_id,
    personal_workspace_owner
  )

  # 2 Resolve authentication and workspace ---------------------------------------------------------

  # A discovered workspace record can provide a workspace-specific API base

  base <- fabric_api_base(api_base)
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
    recursive = if (recursive) "true" else "false",
    rootFolderId = root_folder_id,
    include = include
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

  fabric_item_list(
    records,
    output = output,
    credential = credential,
    api_base = base
  )
}

#' Discover one Microsoft Fabric item
#'
#' Finds one item and returns the connection details needed by 'fabricQueryR'
#' Use this when you know the item's name or ID and do not need to list every
#' item in the workspace
#'
#' @param item Item GUID, exact display name, or an item object returned
#'   by a discovery function. A display name must identify exactly one item of
#'   the requested `type`; use a GUID or discovered object when names are
#'   duplicated
#' @inheritParams fabric_items
#'
#' @return With `output = "r6"`, a [FabricItem] object or type-specific
#'   subclass. With `output = "list"`, one `fabric_item` record containing the
#'   item's name, ID, type, workspace, and available connection details
#' @details
#' The caller needs access to the workspace for the core item lookup.
#' Workload-specific enrichment additionally requires
#' `Item.Read.All`/`Item.ReadWrite.All` or the applicable workload-specific read
#' scope and access to the item. Microsoft currently limits User Data Function
#' detail retrieval to delegated user identities, so its automatic default is
#' lightweight. Set `detail = TRUE` explicitly when using a supported identity
#' @references
#' [Get item REST API](https://learn.microsoft.com/en-us/rest/api/fabric/core/items/get-item)
#' @examples
#' \dontrun{
#' # Discover a workspace and obtain a lightweight Warehouse object
#' workspace <- fabric_workspaces()[[1L]]
#' warehouses <- workspace$items(type = "Warehouse")
#'
#' # Enrich that discovered object with connection details
#' warehouse <- fabric_item(workspace, warehouses[[1L]])
#'
#' # `$sql_connection_info()` calls fabric_sql_connection_info()
#' warehouse$sql_connection_info()
#' }
#' @export
fabric_item <- function(
  workspace,
  item,
  type = NULL,
  detail = NULL,
  detail_errors = c("abort", "record"),
  include = NULL,
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
  output = c("r6", "list")
) {
  # 1 Resolve authentication and workspace ---------------------------------------------------------

  # Establish the workspace first because both item lookup routes need its ID

  output <- .fabric_r6_output(output)
  api_base_supplied <- !missing(api_base)
  if (
    !is.null(detail) &&
      (!is.logical(detail) || length(detail) != 1L || is.na(detail))
  ) {
    .fabric_abort("detail must be NULL, TRUE, or FALSE")
  }
  detail_errors <- match.arg(detail_errors)
  include <- fabric_normalize_item_include(include)
  fabric_validate_personal_workspace_identity(
    personal_workspace_tenant_id,
    personal_workspace_owner
  )
  base <- fabric_api_base(api_base)
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
  if (!is.null(supplied) && is.null(include)) {
    record <- supplied
  } else {
    item_id <- if (is.null(supplied)) {
      item
    } else {
      fabric_record_value(supplied, "id")
    }
    if (
      !is.character(item_id) ||
        length(item_id) != 1L ||
        is.na(item_id) ||
        !nzchar(item_id)
    ) {
      .fabric_abort("item must be one non-empty string or a discovered item")
    }
    if (!is.null(supplied) && !fabric_is_guid(item_id)) {
      .fabric_abort(
        "A supplied item record must contain a canonical GUID `id`"
      )
    }

    if (fabric_is_guid(item_id)) {
      req <- httr2::request(
        paste0(base, "/workspaces/", ws$id, "/items/", item_id)
      )
      req <- httr2::req_url_query(req, include = include)
      record <- .httr2_json(
        req,
        simplifyVector = FALSE,
        credential = credential,
        audience = .fabric_audience$fabric
      )
    } else {
      req <- httr2::request(
        paste0(base, "/workspaces/", ws$id, "/items")
      )
      req <- httr2::req_url_query(req, type = type, include = include)
      candidates <- .httr2_collection(
        req$url,
        credential = credential,
        audience = .fabric_audience$fabric
      )
      record <- fabric_unique_name(candidates, item_id, "item")
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
  if (is.null(detail)) {
    detail <- !identical(tolower(record$type %||% ""), "userdatafunction")
  }
  record <- if (isTRUE(detail)) {
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
  if (!is.null(record$detail_error)) {
    .fabric_warn(
      c(
        "Could not fully enrich the Fabric item",
        "i" = "See the {.field detail_error} field on the returned item"
      ),
      .format = TRUE
    )
  }
  fabric_item_list(
    list(record),
    output = output,
    credential = credential,
    api_base = base
  )[[1L]]
}

#' Typed Microsoft Fabric item discovery
#'
#' These shortcuts cover an intentional subset of Microsoft Fabric item types;
#' they are not an exhaustive list of the items that [fabric_items()] can
#' discover. Each helper requests one exact type and has a corresponding
#' [FabricWorkspace] method. Most retrieve workload connection details by
#' default. Semantic Model and GraphQL helpers default to lightweight discovery
#' because their executable targets are derived from list-level IDs and
#' workspace fields. User Data Functions default to lightweight discovery
#' because Microsoft limits detail retrieval to delegated user identities. Set
#' `detail = TRUE` when the workload and identity support it
#'
#' @section Typed support matrix:
#' `Default detail` is the value used when `detail` is omitted. `FabricItem`
#' in the final column means that the typed helper and workload Get route are
#' supported but no workload-specific R6 subclass is currently provided.
#'
#' \tabular{llll}{
#' \strong{Helper} \tab \strong{Fabric type} \tab \strong{Default detail} \tab \strong{R6 class} \cr
#' `fabric_lakehouses()` \tab `Lakehouse` \tab `TRUE` \tab `FabricLakehouse` \cr
#' `fabric_warehouses()` \tab `Warehouse` \tab `TRUE` \tab `FabricWarehouse` \cr
#' `fabric_warehouse_snapshots()` \tab `WarehouseSnapshot` \tab `TRUE` \tab `FabricWarehouseSnapshot` \cr
#' `fabric_mirrored_databases()` \tab `MirroredDatabase` \tab `TRUE` \tab `FabricMirroredDatabase` \cr
#' `fabric_sql_databases()` \tab `SQLDatabase` \tab `TRUE` \tab `FabricSqlDatabase` \cr
#' `fabric_semantic_models()` \tab `SemanticModel` \tab `FALSE` \tab `FabricSemanticModel` \cr
#' `fabric_eventhouses()` \tab `Eventhouse` \tab `TRUE` \tab `FabricEventhouse` \cr
#' `fabric_kql_databases()` \tab `KQLDatabase` \tab `TRUE` \tab `FabricKqlDatabase` \cr
#' `fabric_notebooks()` \tab `Notebook` \tab `TRUE` \tab `FabricJobItem` \cr
#' `fabric_data_pipelines()` \tab `DataPipeline` \tab `TRUE` \tab `FabricJobItem` \cr
#' `fabric_spark_job_definitions()` \tab `SparkJobDefinition` \tab `TRUE` \tab `FabricJobItem` \cr
#' `fabric_environments()` \tab `Environment` \tab `TRUE` \tab `FabricItem` \cr
#' `fabric_user_data_functions()` \tab `UserDataFunction` \tab `FALSE` \tab `FabricItem` \cr
#' `fabric_graphql_apis()` \tab `GraphQLApi` \tab `FALSE` \tab `FabricGraphQLApi` \cr
#' }
#'
#' @section Choosing a helper:
#' - `fabric_lakehouses()`, `fabric_warehouses()`,
#'   `fabric_warehouse_snapshots()`, and `fabric_mirrored_databases()` find data
#'   stores with `$sql_query()` ([fabric_sql_query()]) and other SQL methods;
#'   Lakehouses and
#'   mirrored databases can also be accessed through OneLake
#' - `fabric_sql_databases()` finds transactional Fabric SQL databases
#' - `fabric_semantic_models()` finds business models with `$dax_query()`
#'   ([fabric_pbi_dax_query()]) and refresh lifecycle methods
#' - `fabric_eventhouses()` and `fabric_kql_databases()` find real-time data
#'   stores with `$query()` ([fabric_kql_query()]) and `$read_table()`
#'   ([fabric_kql_read_table()]), plus ingestion and export methods
#' - `fabric_notebooks()` finds notebooks with job lifecycle and schedule methods
#' - `fabric_data_pipelines()` and `fabric_spark_job_definitions()` find the
#'   other executable items with the same job methods
#' - `fabric_environments()` finds reusable Spark runtime configurations
#' - `fabric_user_data_functions()` finds serverless Python function items
#' - `fabric_graphql_apis()` finds APIs configured in Fabric with `$query()`
#'   ([fabric_graphql_query()]), `$schema()` ([fabric_graphql_schema()]), and
#'   `$paginate()` ([fabric_graphql_paginate()])
#'
#' @section Filtering and returned fields:
#' Each helper requests its exact Fabric item type and verifies that every
#' returned object has that type. The objects otherwise keep all fields
#' returned by Fabric, including fields added by the service in the future
#'
#' Folder recursion, workspace-specific private-link routing, authentication,
#' and `detail_errors` have the same behavior as in [fabric_items()]. With
#' `detail = TRUE`, each helper calls its documented workload-specific Get API
#' and preserves fields such as Spark job and Environment properties. The User
#' Data Function detail endpoint supports delegated users but not service
#' principals or managed identities; those callers can use `detail = FALSE`
#'
#' @inheritParams fabric_items
#' @param ... Authentication and API arguments forwarded to [fabric_items()]
#'   Do not supply `type`; each helper sets that value
#' @return A list with one [FabricItem] object or type-specific R6 subclass per
#'   matching item. Each object contains common item metadata, applicable
#'   connection fields, and workload methods. See [fabric_items()] for details
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
#'
#' [Get data pipeline](https://learn.microsoft.com/en-us/rest/api/fabric/datapipeline/items/get-data-pipeline)
#'
#' [Get Spark job definition](https://learn.microsoft.com/en-us/rest/api/fabric/sparkjobdefinition/items/get-spark-job-definition)
#'
#' [Get environment](https://learn.microsoft.com/en-us/rest/api/fabric/environment/items/get-environment)
#'
#' [Get User Data Function](https://learn.microsoft.com/en-us/rest/api/fabric/userdatafunction/items/get-user-data-function)
#' @examples
#' \dontrun{
#' # Discover a workspace once, then reuse its object for typed discovery
#' workspace <- fabric_workspaces()[[1]]
#'
#' # Discover data items that feed the package's query and storage helpers
#' lakehouses <- fabric_lakehouses(workspace)
#' warehouses <- fabric_warehouses(workspace)
#' snapshots <- fabric_warehouse_snapshots(workspace)
#' mirrored_databases <- fabric_mirrored_databases(workspace)
#' sql_databases <- fabric_sql_databases(workspace)
#' semantic_models <- fabric_semantic_models(workspace)
#' eventhouses <- fabric_eventhouses(workspace)
#' kql_databases <- fabric_kql_databases(workspace)
#' graphql_apis <- fabric_graphql_apis(workspace)
#'
#' # Each method calls the corresponding exported function
#' # fabric_lakehouse_tables()
#' lakehouses[[1L]]$tables()
#' # fabric_sql_connection_info()
#' warehouses[[1L]]$sql_connection_info()
#' # fabric_pbi_dax_query()
#' semantic_models[[1L]]$dax_query(
#'   dax = Sys.getenv("FABRIC_DAX_QUERY")
#' )
#'
#' # Runnable methods call fabric_job_run() and fabric_job_wait()
#' notebook <- fabric_notebooks(workspace)[[1]]
#' pipeline <- fabric_data_pipelines(workspace)[[1]]
#' spark_job <- fabric_spark_job_definitions(workspace)[[1]]
#'
#' notebook$wait(notebook$run(), timeout = 900)
#' pipeline$wait(pipeline$run(), timeout = 900)
#' spark_job$wait(spark_job$run(), timeout = 900)
#'
#' # Discover supporting Spark and serverless-function items as well
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
fabric_mirrored_databases <- function(workspace, detail = TRUE, ...) {
  fabric_typed_item_list(workspace, "MirroredDatabase", detail, ...)
}

#' @rdname fabric_typed_items
#' @export
fabric_sql_databases <- function(workspace, detail = TRUE, ...) {
  fabric_typed_item_list(workspace, "SQLDatabase", detail, ...)
}

#' @rdname fabric_typed_items
#' @export
fabric_semantic_models <- function(workspace, detail = FALSE, ...) {
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

#' Discover Fabric User Data Functions
#'
#' `r lifecycle::badge("experimental")`
#'
#' Finds User Data Function items in a workspace. The default `detail = FALSE`
#' path uses Core item discovery and works with delegated users, service
#' principals, and managed identities. Set `detail = TRUE` to call the
#' workload-specific Get API, which currently supports delegated users only.
#'
#' This helper is experimental because the package's service-principal sandbox
#' can exercise only lightweight Core discovery. Fabric's User Data Function
#' create, update-definition, detailed Get, and delete APIs do not currently
#' support service principals or managed identities, so the standard live lane
#' cannot provision and fully inspect a disposable User Data Function fixture.
#'
#' @inheritParams fabric_items
#' @param detail Whether to retrieve workload-specific details. Defaults to
#'   `FALSE` so service-principal and managed-identity callers can use Core item
#'   discovery.
#' @param ... Authentication and API arguments forwarded to [fabric_items()].
#'   Do not supply `type`; this helper fixes it to `"UserDataFunction"`.
#' @return A list of [FabricItem] objects for matching User Data Function items.
#' @references
#' [List User Data Functions](https://learn.microsoft.com/en-us/rest/api/fabric/userdatafunction/items/list-user-data-functions)
#'
#' [Get User Data Function](https://learn.microsoft.com/en-us/rest/api/fabric/userdatafunction/items/get-user-data-function)
#'
#' [Create User Data Function](https://learn.microsoft.com/en-us/rest/api/fabric/userdatafunction/items/create-user-data-function)
#' @examples
#' \dontrun{
#' workspace <- fabric_workspaces()[[1L]]
#' functions <- fabric_user_data_functions(workspace)
#' }
#' @export
fabric_user_data_functions <- function(workspace, detail = FALSE, ...) {
  fabric_typed_item_list(workspace, "UserDataFunction", detail, ...)
}

#' @rdname fabric_typed_items
#' @export
fabric_graphql_apis <- function(workspace, detail = FALSE, ...) {
  fabric_typed_item_list(workspace, "GraphQLApi", detail, ...)
}

# Request one Fabric item type and discard any mismatched service records
# Returns the original item objects so new service fields remain available
fabric_typed_item_list <- function(workspace, .type, .detail, ...) {
  dots <- list(...)
  dot_names <- names(dots)
  if (
    length(dots) &&
      (is.null(dot_names) || anyNA(dot_names) || !all(nzchar(dot_names)))
  ) {
    .fabric_abort("All arguments forwarded through `...` must be named")
  }
  if (anyDuplicated(dot_names)) {
    .fabric_abort("Arguments forwarded through `...` must have unique names")
  }
  if ("type" %in% dot_names) {
    .fabric_abort(
      paste0(
        "`type` is fixed to \"",
        .type,
        "\" by this typed discovery helper"
      )
    )
  }
  items <- do.call(
    fabric_items,
    c(list(workspace = workspace, type = .type, detail = .detail), dots)
  )
  items[vapply(
    items,
    function(item) identical(fabric_record_value(item, "type"), .type),
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

# Hydrate one List Workspaces record through Get Workspace. The detail route is
# the only workspace API that returns OneLake endpoints
fabric_workspace_details <- function(
  record,
  credential,
  api_base,
  prefer_workspace_endpoints = FALSE
) {
  id <- fabric_record_value(record, "id")
  if (!is.character(id) || length(id) != 1L || !fabric_is_guid(id)) {
    .fabric_abort("A workspace record must contain a canonical GUID `id`")
  }

  request <- httr2::request(paste0(api_base, "/workspaces/", id))
  if (isTRUE(prefer_workspace_endpoints)) {
    request <- httr2::req_url_query(
      request,
      preferWorkspaceSpecificEndpoints = "true"
    )
  }
  details <- .httr2_json(
    request,
    simplifyVector = FALSE,
    credential = credential,
    audience = .fabric_audience$fabric
  )
  record[names(details)] <- details
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

fabric_normalize_item_include <- function(include) {
  if (is.null(include)) {
    return(NULL)
  }
  valid <- is.character(include) &&
    length(include) > 0L &&
    !anyNA(include) &&
    all(grepl("^[A-Za-z][A-Za-z0-9]*$", include)) &&
    !anyDuplicated(tolower(include))
  if (!valid) {
    .fabric_abort(
      "include must be NULL or a unique vector of property names"
    )
  }
  paste(include, collapse = ",")
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

# Normalize and validate `api_base`. Returns a Fabric v1 base URL used
# by all discovery requests
fabric_api_base <- function(api_base) {
  # 1 Parse the endpoint ---------------------------------------------------------------------------

  # Only a complete HTTPS URL can safely receive an access token

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

  # Supplying a custom host is the caller's explicit endpoint choice
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
  if (inherits(value, "FabricRecord")) {
    return(value$as_list())
  }

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
  if (inherits(record, "FabricRecord")) {
    record <- record$as_list()
  }
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

  # A discovered object may already contain a workspace-specific API endpoint

  supplied <- fabric_as_record(workspace)
  if (!is.null(supplied)) {
    supplied_id <- fabric_record_value(supplied, "id")
    supplied_type <- fabric_record_value(supplied, "type", "workspaceType")
    if (
      !is.character(supplied_id) ||
        length(supplied_id) != 1L ||
        is.na(supplied_id) ||
        !fabric_is_guid(supplied_id)
    ) {
      .fabric_abort(
        "A supplied workspace record must contain a canonical GUID `id`"
      )
    }
    if (
      !is.null(supplied_type) &&
        (!is.character(supplied_type) ||
          length(supplied_type) != 1L ||
          is.na(supplied_type) ||
          !tolower(supplied_type) %in%
            c("workspace", "personal", "adminworkspace"))
    ) {
      .fabric_abort(paste0(
        "A supplied workspace record has type '",
        supplied_type,
        "', not 'Workspace', 'Personal', or 'AdminWorkspace'"
      ))
    }
    return(list(
      id = supplied_id,
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
    record <- fabric_workspace_details(
      list(id = workspace),
      credential,
      api_base,
      prefer_workspace_endpoints = isTRUE(use_workspace_endpoint)
    )
  } else {
    request <- httr2::request(paste0(api_base, "/workspaces"))
    if (isTRUE(use_workspace_endpoint)) {
      request <- httr2::req_url_query(
        request,
        preferWorkspaceSpecificEndpoints = "true"
      )
    }
    records <- .httr2_collection(
      request$url,
      credential = credential,
      audience = .fabric_audience$fabric
    )
    record <- fabric_unique_name(records, workspace, "workspace")
    if (isTRUE(use_workspace_endpoint)) {
      record <- fabric_workspace_details(
        record,
        credential,
        api_base,
        prefer_workspace_endpoints = TRUE
      )
    }
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
    mirroreddatabase = "mirroredDatabases",
    sqldatabase = "sqlDatabases",
    semanticmodel = "semanticModels",
    eventhouse = "eventhouses",
    kqldatabase = "kqlDatabases",
    notebook = "notebooks",
    datapipeline = "dataPipelines",
    sparkjobdefinition = "sparkJobDefinitions",
    environment = "environments",
    userdatafunction = "userDataFunctions",
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
  } else if (identical(type, "warehousesnapshot")) {
    route <- "warehouses"
    item_id <- record$properties$parentWarehouseId
  } else if (type %in% c("lakehouse", "mirroreddatabase")) {
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

  if (type %in% c("warehouse", "warehousesnapshot")) {
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
  } else if (type == "mirroreddatabase") {
    # Mirrored databases expose OneLake tables and a read-only SQL endpoint
    sql <- properties$sqlEndpointProperties %||% list()
    record$default_schema <- properties$defaultSchema
    record$one_lake_tables_path <- properties$oneLakeTablesPath
    record$sql_server <- sql$connectionString
    record$sql_database <- record$displayName
    record$sql_endpoint_id <- sql$id
    record$sql_endpoint_status <- sql$provisioningStatus
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
fabric_workspace_list <- function(
  records,
  output = c("r6", "list"),
  credential = NULL,
  api_base = NULL
) {
  output <- .fabric_r6_output(output)
  lapply(records, function(record) {
    legacy_class <- c("fabric_workspace", "list")
    if (identical(output, "r6")) {
      record_api_base <- if (is.null(api_base)) {
        NULL
      } else {
        fabric_workspace_api_base(record, api_base)
      }
      fabric_r6_record(
        record,
        legacy_class,
        credential,
        api_base = record_api_base
      )
    } else {
      structure(record, class = legacy_class)
    }
  })
}

# Give each item API record its package class. Returns a list used by all item
# discovery entry points while preserving all original fields
fabric_item_list <- function(
  records,
  output = c("r6", "list"),
  credential = NULL,
  api_base = NULL
) {
  output <- .fabric_r6_output(output)
  lapply(records, function(record) {
    legacy_class <- c("fabric_item", "list")
    if (identical(output, "r6")) {
      fabric_r6_record(record, legacy_class, credential, api_base)
    } else {
      structure(record, class = legacy_class)
    }
  })
}
