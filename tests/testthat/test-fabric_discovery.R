test_that("fabric_workspaces follows pagination and returns workspace objects", {
  calls <- character()
  httr2::local_mocked_responses(function(req) {
    calls <<- c(calls, req$url)
    if (grepl("/workspaces/11111111-", req$url, fixed = TRUE)) {
      discovery_response(
        list(
          id = "11111111-1111-4111-8111-111111111111",
          displayName = "Analytics",
          type = "Workspace",
          apiEndpoint = "https://analytics.z13.w.api.fabric.microsoft.com",
          oneLakeEndpoints = list(
            dfsEndpoint = "https://analytics.z13.dfs.fabric.microsoft.com",
            blobEndpoint = "https://analytics.z13.blob.fabric.microsoft.com"
          )
        ),
        req$url
      )
    } else if (grepl("/workspaces/22222222-", req$url, fixed = TRUE)) {
      discovery_response(
        list(
          id = "22222222-2222-4222-8222-222222222222",
          displayName = "Research",
          type = "Workspace",
          apiEndpoint = "https://research.z14.w.api.fabric.microsoft.com",
          oneLakeEndpoints = list(
            dfsEndpoint = "https://research.z14.dfs.fabric.microsoft.com",
            blobEndpoint = "https://research.z14.blob.fabric.microsoft.com"
          )
        ),
        req$url
      )
    } else if (length(calls) == 1L) {
      discovery_response(
        list(
          value = list(list(
            id = "11111111-1111-4111-8111-111111111111",
            displayName = "Analytics",
            description = "Primary",
            type = "Workspace",
            capacityId = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            capacityRegion = list(name = "West Europe"),
            tags = list(team = "analytics")
          )),
          continuationToken = "page two"
        ),
        req$url
      )
    } else {
      discovery_response(
        list(
          value = list(list(
            id = "22222222-2222-4222-8222-222222222222",
            displayName = "Research",
            type = "Workspace"
          ))
        ),
        req$url
      )
    }
  })

  result <- fabric_workspaces(
    roles = c("Admin", "Member"),
    prefer_workspace_endpoints = TRUE,
    token = "token"
  )

  expect_identical(class(result), "list")
  expect_length(result, 2L)
  expect_equal(
    purrr::map_lgl(result, inherits, "FabricWorkspace"),
    rep(TRUE, 2L)
  )
  expect_equal(
    purrr::map_chr(result, "displayName"),
    c("Analytics", "Research")
  )
  expect_equal(result[[1L]]$description, "Primary")
  expect_equal(result[[1L]]$capacityRegion$name, "West Europe")
  expect_equal(result[[1L]]$tags$team, "analytics")
  expect_equal(
    result[[1L]]$oneLakeEndpoints$dfsEndpoint,
    "https://analytics.z13.dfs.fabric.microsoft.com"
  )
  expect_null(result[[2L]]$description)
  expect_match(calls[[1L]], "roles=Admin%2CMember")
  expect_match(calls[[1L]], "preferWorkspaceSpecificEndpoints=true")
  expect_match(calls[[2L]], "continuationToken=page%20two")
  expect_true(all(grepl(
    "preferWorkspaceSpecificEndpoints=true",
    calls[3:4],
    fixed = TRUE
  )))
})

test_that("workspace discovery can return plain records explicitly", {
  local_mocked_bindings(
    .httr2_collection = function(...) {
      list(list(
        id = "11111111-1111-4111-8111-111111111111",
        displayName = "Analytics",
        type = "Workspace",
        description = "Primary"
      ))
    }
  )

  workspace <- fabric_workspaces(token = "token", output = "list")[[1L]]

  expect_s3_class(workspace, "fabric_workspace")
  expect_identical(workspace$displayName, "Analytics")
  expect_identical(workspace$description, "Primary")
})

test_that("workspace R6 methods retain a custom Fabric API origin", {
  requests <- character()
  httr2::local_mocked_responses(function(req) {
    requests <<- c(requests, req$url)
    if (grepl("/items", req$url, fixed = TRUE)) {
      discovery_response(list(value = list()), req$url)
    } else {
      discovery_response(
        list(
          value = list(list(
            id = "11111111-1111-4111-8111-111111111111",
            displayName = "Analytics",
            type = "Workspace"
          ))
        ),
        req$url
      )
    }
  })

  workspace <- fabric_workspaces(
    token = "token",
    api_base = "https://highapi.fabric.microsoft.us"
  )[[1L]]
  items <- workspace$items(detail = FALSE)

  expect_length(items, 0L)
  expect_length(requests, 2L)
  expect_true(all(startsWith(
    requests,
    "https://highapi.fabric.microsoft.us/v1/"
  )))
})

test_that("workspace resolution retrieves preferred OneLake endpoints", {
  workspace_id <- "11111111-1111-4111-8111-111111111111"
  requests <- character()
  local_mocked_bindings(
    .httr2_json = function(req, ...) {
      requests <<- c(requests, req$url)
      list(
        id = workspace_id,
        displayName = "Private workspace",
        type = "Workspace",
        apiEndpoint = "https://private.z13.w.api.fabric.microsoft.com",
        oneLakeEndpoints = list(
          dfsEndpoint = "https://private.z13.dfs.fabric.microsoft.com",
          blobEndpoint = "https://private.z13.blob.fabric.microsoft.com"
        )
      )
    }
  )

  resolved <- fabric_resolve_workspace(
    workspace_id,
    fabric_credential(token = "token"),
    .fabric_api_base
  )

  expect_match(
    requests[[1L]],
    "preferWorkspaceSpecificEndpoints=true",
    fixed = TRUE
  )
  expect_equal(
    resolved$raw$oneLakeEndpoints$dfsEndpoint,
    "https://private.z13.dfs.fabric.microsoft.com"
  )
  expect_equal(
    resolved$api_base,
    "https://private.z13.w.api.fabric.microsoft.com/v1"
  )
})

test_that("name discovery requires an exact or unique match", {
  records <- list(
    list(id = "one", displayName = "Sales"),
    list(id = "two", displayName = "sales")
  )
  expect_equal(fabric_unique_name(records, "Sales", "workspace")$id, "one")
  expect_error(
    fabric_unique_name(records, "SALES", "workspace"),
    "ambiguous"
  )
  expect_error(
    fabric_unique_name(records, "Missing", "workspace"),
    "not found"
  )
})

test_that("supplied workspace records validate identity and type", {
  credential <- fabric_credential(token = "token")
  workspace_id <- "11111111-1111-4111-8111-111111111111"
  resolved <- fabric_resolve_workspace(
    list(id = workspace_id, type = "Workspace", displayName = "Analytics"),
    credential,
    .fabric_api_base
  )
  expect_identical(resolved$id, workspace_id)

  admin <- fabric_resolve_workspace(
    list(
      id = workspace_id,
      type = "AdminWorkspace",
      displayName = "Admin monitoring"
    ),
    credential,
    .fabric_api_base
  )
  expect_identical(admin$id, workspace_id)
  expect_identical(admin$raw$type, "AdminWorkspace")

  expect_error(
    fabric_resolve_workspace(
      list(id = "not-a-guid", type = "Workspace"),
      credential,
      .fabric_api_base
    ),
    "canonical GUID",
    fixed = TRUE
  )
  expect_error(
    fabric_resolve_workspace(
      list(id = workspace_id, type = "Notebook"),
      credential,
      .fabric_api_base
    ),
    "not 'Workspace', 'Personal', or 'AdminWorkspace'",
    fixed = TRUE
  )
})

test_that("workspace-specific API endpoints route item discovery", {
  workspace <- structure(
    list(
      id = "11111111-1111-4111-8111-111111111111",
      displayName = "Private workspace",
      apiEndpoint = "https://workspace.z13.api.fabric.microsoft.com/",
      oneLakeEndpoints = list(
        dfsEndpoint = "https://workspace.z13.dfs.fabric.microsoft.com",
        blobEndpoint = "https://workspace.z13.blob.fabric.microsoft.com"
      )
    ),
    class = c("fabric_workspace", "list")
  )
  urls <- character()
  local_mocked_bindings(
    .httr2_collection = function(url, ...) {
      urls <<- c(urls, url)
      list(list(
        id = "22222222-2222-4222-8222-222222222222",
        displayName = "Products API",
        type = "GraphQLApi"
      ))
    }
  )

  result <- fabric_items(workspace, token = "token")

  expect_match(
    urls[[1L]],
    "https://workspace.z13.api.fabric.microsoft.com/v1/workspaces/",
    fixed = TRUE
  )
  expect_match(
    result[[1L]]$graphql_endpoint,
    "https://workspace.z13.api.fabric.microsoft.com/v1/workspaces/",
    fixed = TRUE
  )
  expect_equal(
    result[[1L]]$workspaceApiEndpoint,
    "https://workspace.z13.api.fabric.microsoft.com/"
  )
  expect_equal(
    result[[1L]]$workspaceOneLakeDfsEndpoint,
    "https://workspace.z13.dfs.fabric.microsoft.com"
  )

  item_urls <- character()
  local_mocked_bindings(
    .httr2_json = function(req, ...) {
      item_urls <<- c(item_urls, req$url)
      list(
        id = "33333333-3333-4333-8333-333333333333",
        displayName = "Pipeline",
        type = "Notebook"
      )
    }
  )
  singular <- fabric_item(
    workspace,
    "33333333-3333-4333-8333-333333333333",
    token = "token"
  )
  expect_true(all(grepl(
    "https://workspace.z13.api.fabric.microsoft.com/v1/workspaces/",
    item_urls,
    fixed = TRUE
  )))
  expect_equal(
    singular$workspaceOneLakeDfsEndpoint,
    "https://workspace.z13.dfs.fabric.microsoft.com"
  )
  expect_equal(
    singular$workspaceOneLakeEndpoints$blobEndpoint,
    "https://workspace.z13.blob.fabric.microsoft.com"
  )

  fabric_items(
    workspace,
    token = "token",
    api_base = "https://explicit.test/v1"
  )
  expect_match(urls[[2L]], "https://explicit.test/v1/workspaces/", fixed = TRUE)
})

test_that("workspace-private discovery resolves dedicated SQL hostnames", {
  private_base <- "https://workspace.z13.api.fabric.microsoft.com/v1"
  calls <- character()
  local_mocked_bindings(
    .httr2_json = function(req, ...) {
      calls <<- c(calls, req$url)
      if (grepl("/connectionString", req$url, fixed = TRUE)) {
        return(list(
          connectionString = paste0(
            "workspace-item.z13.datawarehouse.fabric.microsoft.com"
          )
        ))
      }
      list(
        id = "warehouse-id",
        workspaceId = "workspace-id",
        type = "Warehouse",
        displayName = "Warehouse",
        properties = list(
          connectionString = "public.datawarehouse.fabric.microsoft.com"
        )
      )
    }
  )

  warehouse <- fabric_enrich_item(
    list(
      id = "warehouse-id",
      workspaceId = "workspace-id",
      type = "Warehouse",
      displayName = "Warehouse"
    ),
    fabric_credential(token = "token"),
    private_base
  )

  expect_identical(
    warehouse$sql_server,
    "workspace-item.z13.datawarehouse.fabric.microsoft.com"
  )
  expect_identical(warehouse$sql_private_link_type, "Workspace")
  expect_match(
    calls[[2L]],
    "/warehouses/warehouse-id/connectionString"
  )
  expect_match(calls[[2L]], "privateLinkType=Workspace")
})

test_that("workspace-private Lakehouses resolve SQL Endpoint hostnames", {
  calls <- character()
  local_mocked_bindings(
    .httr2_json = function(req, ...) {
      calls <<- c(calls, req$url)
      if (grepl("/connectionString", req$url, fixed = TRUE)) {
        return(list(
          connectionString = paste0(
            "workspace-lakehouse.z13.datawarehouse.fabric.microsoft.com"
          )
        ))
      }
      list(
        id = "lakehouse-id",
        workspaceId = "workspace-id",
        type = "Lakehouse",
        displayName = "Lakehouse",
        properties = list(
          sqlEndpointProperties = list(
            id = "sql-endpoint-id",
            connectionString = "public.datawarehouse.fabric.microsoft.com"
          )
        )
      )
    }
  )

  lakehouse <- fabric_enrich_item(
    list(
      id = "lakehouse-id",
      workspaceId = "workspace-id",
      type = "Lakehouse",
      displayName = "Lakehouse"
    ),
    fabric_credential(token = "token"),
    "https://workspace.z13.api.fabric.microsoft.com/v1"
  )

  expect_identical(
    lakehouse$sql_server,
    "workspace-lakehouse.z13.datawarehouse.fabric.microsoft.com"
  )
  expect_match(
    calls[[2L]],
    "/sqlEndpoints/sql-endpoint-id/connectionString"
  )
  expect_match(calls[[2L]], "privateLinkType=Workspace")
})

test_that("workspace-private snapshots use their parent Warehouse hostname", {
  calls <- character()
  local_mocked_bindings(
    .httr2_json = function(req, ...) {
      calls <<- c(calls, req$url)
      if (grepl("/connectionString", req$url, fixed = TRUE)) {
        return(list(
          connectionString = paste0(
            "workspace-parent.z13.datawarehouse.fabric.microsoft.com"
          )
        ))
      }
      list(
        id = "snapshot-id",
        workspaceId = "workspace-id",
        type = "WarehouseSnapshot",
        displayName = "Sales at month end",
        properties = list(
          connectionString = "public.datawarehouse.fabric.microsoft.com",
          parentWarehouseId = "parent-warehouse-id"
        )
      )
    }
  )

  snapshot <- fabric_enrich_item(
    list(
      id = "snapshot-id",
      workspaceId = "workspace-id",
      type = "WarehouseSnapshot",
      displayName = "Sales at month end"
    ),
    fabric_credential(token = "token"),
    "https://workspace.z13.api.fabric.microsoft.com/v1"
  )

  expect_identical(
    snapshot$sql_server,
    "workspace-parent.z13.datawarehouse.fabric.microsoft.com"
  )
  expect_identical(snapshot$sql_database, "Sales at month end")
  expect_identical(snapshot$sql_private_link_type, "Workspace")
  expect_match(
    calls[[2L]],
    "/warehouses/parent-warehouse-id/connectionString"
  )
  expect_match(calls[[2L]], "privateLinkType=Workspace")
})

test_that("workspace-specific API endpoints are validated", {
  expect_equal(
    fabric_workspace_api_base(
      list(apiEndpoint = "https://workspace.z13.api.fabric.microsoft.com"),
      .fabric_api_base
    ),
    "https://workspace.z13.api.fabric.microsoft.com/v1"
  )
  expect_equal(
    fabric_workspace_api_base(
      list(apiEndpoint = "https://workspace.z13.api.fabric.microsoft.com/v1/"),
      .fabric_api_base
    ),
    "https://workspace.z13.api.fabric.microsoft.com/v1"
  )
  expect_equal(
    fabric_workspace_api_base(
      list(apiEndpoint = "https://workspace.z13.api.fabric.microsoft.com:443"),
      .fabric_api_base
    ),
    "https://workspace.z13.api.fabric.microsoft.com:443/v1"
  )
  expect_error(
    fabric_workspace_api_base(
      list(apiEndpoint = "http://workspace.example.test"),
      .fabric_api_base
    ),
    "must be an HTTPS origin"
  )
  expect_error(
    fabric_workspace_api_base(
      list(apiEndpoint = "https://workspace.example.test/custom"),
      .fabric_api_base
    ),
    "optional /v1 path"
  )
  expect_error(
    fabric_workspace_api_base(
      list(apiEndpoint = "https://attacker.example/v1"),
      .fabric_api_base
    ),
    "HTTPS origin"
  )
})

test_that("fabric_items filters and enriches Lakehouse targets", {
  urls <- character()
  local_mocked_bindings(
    fabric_resolve_workspace = function(...) {
      list(
        id = "11111111-1111-4111-8111-111111111111",
        displayName = "Analytics"
      )
    },
    .httr2_collection = function(url, ...) {
      urls <<- c(urls, url)
      list(list(
        id = "22222222-2222-4222-8222-222222222222",
        displayName = "SalesLake",
        description = "Lake",
        type = "Lakehouse"
      ))
    },
    .httr2_json = function(req, ...) {
      urls <<- c(urls, req$url)
      list(
        id = "22222222-2222-4222-8222-222222222222",
        workspaceId = "11111111-1111-4111-8111-111111111111",
        displayName = "SalesLake",
        type = "Lakehouse",
        properties = list(
          defaultSchema = "dbo",
          oneLakeTablesPath = "https://onelake/Tables",
          oneLakeFilesPath = "https://onelake/Files",
          sqlEndpointProperties = list(
            id = "33333333-3333-4333-8333-333333333333",
            connectionString = "server.datawarehouse.fabric.microsoft.com",
            provisioningStatus = "Success"
          )
        )
      )
    }
  )

  result <- fabric_lakehouses(
    "Analytics",
    token = "token",
    api_base = "https://fabric.test/v1/"
  )
  lakehouse <- result[[1L]]

  expect_identical(class(result), "list")
  expect_s3_class(lakehouse, "FabricLakehouse")
  expect_equal(lakehouse$type, "Lakehouse")
  expect_equal(
    lakehouse$sql_server,
    "server.datawarehouse.fabric.microsoft.com"
  )
  expect_equal(lakehouse$sql_database, "SalesLake")
  expect_equal(lakehouse$one_lake_tables_path, "https://onelake/Tables")
  expect_equal(lakehouse$default_schema, "dbo")
  expect_equal(
    lakehouse$livy_url,
    paste0(
      "https://fabric.test/v1/workspaces/",
      "11111111-1111-4111-8111-111111111111/lakehouses/",
      "22222222-2222-4222-8222-222222222222/",
      "livyapi/versions/2023-12-01/sessions"
    )
  )
  expect_match(urls[[1L]], "type=Lakehouse")
  expect_match(urls[[2L]], "/lakehouses/")
})

test_that("item discovery records partial detail failures", {
  local_mocked_bindings(
    fabric_resolve_workspace = function(...) {
      list(id = "workspace-id", displayName = "Analytics")
    },
    .httr2_collection = function(...) {
      list(
        list(id = "available", displayName = "Available", type = "Lakehouse"),
        list(id = "forbidden", displayName = "Forbidden", type = "Lakehouse")
      )
    },
    fabric_enrich_item = function(record, ...) {
      if (identical(record$id, "forbidden")) {
        rlang::abort("detail request was forbidden")
      }
      record$properties <- list(defaultSchema = "dbo")
      fabric_add_derived_targets(record, .fabric_api_base)
    }
  )

  expect_warning(
    result <- fabric_items(
      "Analytics",
      detail = TRUE,
      token = "token"
    ),
    "Could not fully enrich 1 Fabric item",
    fixed = TRUE
  )
  expect_length(result, 2L)
  expect_equal(
    vapply(result, inherits, logical(1), "FabricLakehouse"),
    rep(TRUE, 2L)
  )
  available <- purrr::keep(result, ~ .x$id == "available")[[1L]]
  forbidden <- purrr::keep(result, ~ .x$id == "forbidden")[[1L]]
  expect_null(available$detail_error)
  expect_match(
    forbidden$detail_error,
    "forbidden",
    fixed = TRUE
  )
  expect_equal(
    forbidden$detail_error_class,
    "rlang_error"
  )

  expect_error(
    fabric_items(
      "Analytics",
      detail = TRUE,
      detail_errors = "abort",
      token = "token"
    ),
    "detail request was forbidden",
    fixed = TRUE
  )
})

test_that("private SQL failures retain successful workload details", {
  private_base <- "https://workspace.z13.api.fabric.microsoft.com/v1"
  local_mocked_bindings(
    fabric_resolve_workspace = function(...) {
      list(
        id = "workspace-id",
        displayName = "Analytics",
        api_base = private_base
      )
    },
    .httr2_collection = function(...) {
      list(list(
        id = "warehouse-id",
        displayName = "Sales",
        type = "Warehouse"
      ))
    },
    .httr2_json = function(req, ...) {
      if (grepl("/connectionString", req$url, fixed = TRUE)) {
        rlang::abort("private SQL endpoint is unavailable")
      }
      list(
        id = "warehouse-id",
        workspaceId = "workspace-id",
        displayName = "Sales",
        type = "Warehouse",
        description = "Detailed warehouse description",
        properties = list(
          connectionString = "public.datawarehouse.fabric.microsoft.com"
        )
      )
    }
  )

  expect_warning(
    result <- fabric_items(
      "Analytics",
      detail = TRUE,
      detail_errors = "record",
      token = "token"
    ),
    "Could not fully enrich 1 Fabric item",
    fixed = TRUE
  )
  warehouse <- result[[1L]]
  expect_equal(warehouse$description, "Detailed warehouse description")
  expect_equal(
    warehouse$sql_server,
    "public.datawarehouse.fabric.microsoft.com"
  )
  expect_equal(
    warehouse$detail_error_stage,
    "private_sql_connection_string"
  )
  expect_match(warehouse$detail_error, "unavailable", fixed = TRUE)

  expect_error(
    fabric_items(
      "Analytics",
      detail = TRUE,
      detail_errors = "abort",
      token = "token"
    ),
    "private SQL endpoint is unavailable",
    fixed = TRUE
  )
})

test_that("typed routes and derived targets cover supported workloads", {
  expect_identical(
    unname(vapply(
      typed_discovery_support$type,
      fabric_item_route,
      character(1)
    )),
    typed_discovery_support$route
  )
  expect_null(fabric_item_route("Report"))
  expect_null(fabric_item_route(""))

  sql_database <- fabric_add_derived_targets(
    list(
      id = "sql-id",
      type = "SQLDatabase",
      displayName = "Orders",
      properties = list(
        connectionString = "Server=sql;Initial Catalog=orders-id",
        serverFqdn = "sql.database.fabric.microsoft.com,1433",
        databaseName = "orders-id"
      )
    ),
    .fabric_api_base
  )
  expect_equal(sql_database$sql_database, "orders-id")
  expect_equal(
    sql_database$sql_server,
    "sql.database.fabric.microsoft.com,1433"
  )

  snapshot <- fabric_add_derived_targets(
    list(
      id = "snapshot-id",
      type = "WarehouseSnapshot",
      displayName = "Sales at month end",
      properties = list(
        connectionString = paste0(
          "snapshot.datawarehouse.fabric.microsoft.com"
        ),
        parentWarehouseId = "warehouse-id",
        snapshotDateTime = "2026-07-31T23:59:59Z"
      )
    ),
    .fabric_api_base
  )
  expect_equal(
    snapshot$sql_server,
    "snapshot.datawarehouse.fabric.microsoft.com"
  )
  expect_equal(snapshot$sql_database, "Sales at month end")

  mirrored <- fabric_add_derived_targets(
    list(
      id = "mirrored-id",
      type = "MirroredDatabase",
      displayName = "Operational replica",
      properties = list(
        defaultSchema = "sales",
        oneLakeTablesPath = paste0(
          "https://onelake.dfs.fabric.microsoft.com/workspace/",
          "mirrored-id/Tables"
        ),
        sqlEndpointProperties = list(
          connectionString = "mirror.datawarehouse.fabric.microsoft.com",
          id = "endpoint-id",
          provisioningStatus = "Success"
        )
      )
    ),
    .fabric_api_base
  )
  expect_equal(mirrored$default_schema, "sales")
  expect_match(mirrored$one_lake_tables_path, "/mirrored-id/Tables$")
  expect_equal(
    mirrored$sql_server,
    "mirror.datawarehouse.fabric.microsoft.com"
  )
  expect_equal(mirrored$sql_database, "Operational replica")
  expect_equal(mirrored$sql_endpoint_id, "endpoint-id")
  expect_equal(mirrored$sql_endpoint_status, "Success")

  semantic_model <- fabric_add_derived_targets(
    list(
      id = "model-id",
      workspaceId = "workspace-id",
      workspaceDisplayName = "Data & AI",
      type = "SemanticModel",
      displayName = "Sales;Archive"
    ),
    .fabric_api_base
  )
  expect_match(
    semantic_model$dax_connection_string,
    "Data%20%26%20AI",
    fixed = TRUE
  )
  expect_match(
    semantic_model$dax_connection_string,
    "Initial Catalog={Sales;Archive};",
    fixed = TRUE
  )
  expect_equal(
    pbi_parse_connstr(semantic_model$dax_connection_string)$dataset,
    "Sales;Archive"
  )

  personal_model <- fabric_add_derived_targets(
    list(
      id = "personal-model-id",
      workspaceDisplayName = "My Workspace",
      workspaceType = "Personal",
      workspaceTenantId = "11111111-1111-4111-8111-111111111111",
      workspaceOwner = "owner@example.com",
      type = "SemanticModel",
      displayName = "Personal Model"
    ),
    .fabric_api_base
  )
  expect_match(personal_model$dax_connection_string, "/v2.0/")
  expect_match(
    personal_model$dax_connection_string,
    "/home/myworkspace/owner%40example.com",
    fixed = TRUE
  )
  incomplete_personal <- personal_model
  incomplete_personal$dax_connection_string <- NULL
  incomplete_personal$workspaceOwner <- NULL
  incomplete_personal <- fabric_add_derived_targets(
    incomplete_personal,
    .fabric_api_base
  )
  expect_null(incomplete_personal$dax_connection_string)

  eventhouse <- fabric_add_derived_targets(
    list(
      id = "event-id",
      type = "Eventhouse",
      displayName = "Events",
      properties = list(
        queryServiceUri = "https://cluster.kusto.fabric.microsoft.com",
        ingestionServiceUri = "https://ingest-cluster.kusto.fabric.microsoft.com"
      )
    ),
    .fabric_api_base
  )
  expect_equal(
    eventhouse$query_service_uri,
    "https://cluster.kusto.fabric.microsoft.com"
  )
})

test_that("detail enrichment skips item types without a detail route", {
  detail_urls <- character()
  local_mocked_bindings(
    fabric_resolve_workspace = function(...) {
      list(
        id = "workspace-id",
        displayName = "Analytics",
        raw = list(type = "Workspace")
      )
    },
    .httr2_collection = function(...) {
      list(
        list(id = "report-id", displayName = "Sales", type = "Report"),
        list(
          id = "warehouse-id",
          displayName = "Warehouse",
          type = "Warehouse"
        )
      )
    },
    .httr2_json = function(req, ...) {
      detail_urls <<- c(detail_urls, req$url)
      list(
        id = "warehouse-id",
        displayName = "Warehouse",
        type = "Warehouse",
        properties = list(connectionString = "server.fabric.microsoft.com")
      )
    }
  )

  items <- fabric_items("Analytics", detail = TRUE, token = "token")

  expect_length(items, 2L)
  expect_equal(items[[1L]]$type, "Report")
  expect_s3_class(items[[1L]], "FabricItem")
  expect_false(inherits(items[[1L]], "FabricJobItem"))
  expect_equal(items[[2L]]$sql_server, "server.fabric.microsoft.com")
  expect_length(detail_urls, 1L)
  expect_match(detail_urls, "/warehouses/warehouse-id$", perl = TRUE)
})

test_that("typed workload discovery uses its documented detail routes", {
  routes <- c(
    DataPipeline = "dataPipelines",
    SparkJobDefinition = "sparkJobDefinitions",
    Environment = "environments",
    UserDataFunction = "userDataFunctions"
  )
  detail_urls <- character()
  local_mocked_bindings(
    fabric_resolve_workspace = function(...) {
      list(
        id = "workspace-id",
        displayName = "Analytics",
        raw = list(type = "Workspace")
      )
    },
    .httr2_collection = function(...) {
      lapply(seq_along(routes), function(index) {
        list(
          id = paste0("item-", index),
          displayName = names(routes)[[index]],
          type = names(routes)[[index]]
        )
      })
    },
    .httr2_json = function(req, ...) {
      detail_urls <<- c(detail_urls, req$url)
      route <- sub(".*/([^/]+)/[^/]+$", "\\1", req$url)
      type <- names(routes)[match(route, routes)]
      list(
        id = sub(".*/", "", req$url),
        displayName = type,
        type = type,
        workspaceId = "workspace-id",
        properties = list(detailRoute = route)
      )
    }
  )

  items <- fabric_items("Analytics", detail = TRUE, token = "token")

  expect_length(items, 4L)
  expect_identical(
    vapply(items, function(item) item$properties$detailRoute, character(1)),
    unname(routes)
  )
  expect_identical(
    detail_urls,
    paste0(
      .fabric_api_base,
      "/workspaces/workspace-id/",
      unname(routes),
      "/item-",
      seq_along(routes)
    )
  )
  expect_identical(
    unname(vapply(names(routes), fabric_item_route, character(1))),
    unname(routes)
  )
})

test_that("personal workspace identity builds a documented v2 DAX target", {
  local_mocked_bindings(
    fabric_resolve_workspace = function(...) {
      list(
        id = "personal-workspace-id",
        displayName = "My Workspace",
        raw = list(type = "Personal")
      )
    },
    .httr2_collection = function(...) {
      list(list(
        id = "model-id",
        displayName = "Personal Model",
        type = "SemanticModel"
      ))
    }
  )

  model <- fabric_items(
    "My Workspace",
    type = "SemanticModel",
    personal_workspace_tenant_id = "11111111-1111-4111-8111-111111111111",
    personal_workspace_owner = "owner@example.com",
    token = "token"
  )[[1L]]

  expect_match(model$dax_connection_string, "/v2.0/")
  expect_match(
    model$dax_connection_string,
    "/home/myworkspace/owner%40example.com",
    fixed = TRUE
  )
  expect_error(
    fabric_items(
      "My Workspace",
      personal_workspace_owner = "owner@example.com",
      token = "token"
    ),
    "must be supplied together",
    fixed = TRUE
  )
})

test_that("typed convenience helpers forward their workload types", {
  calls <- list()
  local_mocked_bindings(
    fabric_items = function(workspace, type, detail, ...) {
      forwarded <- list(...)
      calls[[length(calls) + 1L]] <<- list(
        workspace = workspace,
        type = type,
        detail = detail,
        forwarded = forwarded
      )
      list()
    }
  )
  for (name in typed_discovery_support$helper) {
    get(name, mode = "function")("Workspace", token = "token")
  }
  expect_identical(
    vapply(calls, `[[`, character(1), "type"),
    typed_discovery_support$type
  )
  expect_identical(
    vapply(calls, `[[`, logical(1), "detail"),
    typed_discovery_support$detail
  )
  expect_setequal(
    intersect(
      names(FabricWorkspace$public_methods),
      typed_discovery_support$workspace_method
    ),
    typed_discovery_support$workspace_method
  )
  expect_true(all(
    vapply(calls, `[[`, character(1), "workspace") == "Workspace"
  ))
  fabric_lakehouses(
    "Workspace",
    detail = FALSE,
    recursive = FALSE,
    detail_errors = "abort",
    token = "token"
  )
  expect_false(calls[[length(calls)]]$detail)
  expect_false(calls[[length(calls)]]$forwarded$recursive)
  expect_identical(calls[[length(calls)]]$forwarded$detail_errors, "abort")
  expect_identical(calls[[length(calls)]]$forwarded$token, "token")
})

test_that("Semantic Model and GraphQL defaults avoid detail requests", {
  collection_calls <- character()
  detail_calls <- 0L
  local_mocked_bindings(
    fabric_resolve_workspace = function(...) {
      list(
        id = "11111111-1111-4111-8111-111111111111",
        displayName = "Data & AI",
        raw = list(type = "Workspace")
      )
    },
    .httr2_collection = function(url, ...) {
      collection_calls <<- c(collection_calls, url)
      if (grepl("type=SemanticModel", url, fixed = TRUE)) {
        return(list(list(
          id = "22222222-2222-4222-8222-222222222222",
          displayName = "Sales",
          type = "SemanticModel"
        )))
      }
      list(list(
        id = "33333333-3333-4333-8333-333333333333",
        displayName = "Products API",
        type = "GraphQLApi"
      ))
    },
    .httr2_json = function(...) {
      detail_calls <<- detail_calls + 1L
      rlang::abort("unexpected detail request")
    }
  )

  models <- fabric_semantic_models("Workspace", token = "token")
  apis <- fabric_graphql_apis("Workspace", token = "token")

  expect_length(collection_calls, 2L)
  expect_identical(detail_calls, 0L)
  expect_match(models[[1L]]$dax_connection_string, "Data%20%26%20AI")
  expect_match(
    apis[[1L]]$graphql_endpoint,
    paste0("/graphqlapis/", apis[[1L]]$id, "/graphql$"),
    perl = TRUE
  )
})

test_that("typed helpers strictly filter records and preserve future fields", {
  local_mocked_bindings(
    fabric_items = function(workspace, type, detail, ...) {
      fabric_item_list(list(
        list(
          id = "pipeline-id",
          displayName = "Daily load",
          type = "DataPipeline",
          futureServiceField = list(enabled = TRUE, version = 2L)
        ),
        list(
          id = "notebook-id",
          displayName = "Unexpected notebook",
          type = "Notebook"
        ),
        list(
          id = "missing-type-id",
          displayName = "Incomplete item"
        )
      ))
    }
  )

  pipelines <- fabric_data_pipelines("Workspace", token = "token")

  expect_length(pipelines, 1L)
  expect_s3_class(pipelines[[1L]], "FabricJobItem")
  expect_identical(pipelines[[1L]]$type, "DataPipeline")
  expect_identical(
    pipelines[[1L]]$futureServiceField,
    list(enabled = TRUE, version = 2L)
  )
})

test_that("item listing forwards folder and include options safely", {
  requested_url <- NULL
  local_mocked_bindings(
    fabric_resolve_workspace = function(...) {
      list(id = "11111111-1111-4111-8111-111111111111")
    },
    .httr2_collection = function(url, ...) {
      requested_url <<- url
      list()
    }
  )
  folder_id <- "22222222-2222-4222-8222-222222222222"

  result <- fabric_items(
    "Workspace",
    recursive = FALSE,
    root_folder_id = folder_id,
    include = "DefaultIdentity",
    token = "token"
  )

  expect_length(result, 0L)
  parsed <- httr2::url_parse(requested_url)
  expect_identical(parsed$query$recursive, "false")
  expect_identical(parsed$query$rootFolderId, folder_id)
  expect_identical(parsed$query$include, "DefaultIdentity")
})

test_that("single-item lookup forwards include for IDs and names", {
  workspace_id <- "11111111-1111-4111-8111-111111111111"
  item_id <- "22222222-2222-4222-8222-222222222222"
  requested_urls <- character()
  local_mocked_bindings(
    fabric_resolve_workspace = function(...) {
      list(
        id = workspace_id,
        displayName = "Workspace",
        api_base = "https://example.test/v1"
      )
    },
    .httr2_json = function(req, ...) {
      requested_urls <<- c(requested_urls, req$url)
      list(
        id = item_id,
        displayName = "Sales",
        type = "Warehouse",
        defaultIdentity = list(type = "ServicePrincipal")
      )
    },
    .httr2_collection = function(url, ...) {
      requested_urls <<- c(requested_urls, url)
      list(list(
        id = item_id,
        displayName = "Sales",
        type = "Warehouse",
        defaultIdentity = list(type = "ServicePrincipal")
      ))
    },
    fabric_enrich_item = function(record, ...) record
  )

  by_id <- fabric_item(
    "Workspace",
    item_id,
    include = "DefaultIdentity",
    token = "token",
    output = "list"
  )
  by_name <- fabric_item(
    "Workspace",
    "Sales",
    include = c("DefaultIdentity", "FutureProperty"),
    token = "token",
    output = "list"
  )

  expect_identical(by_id$defaultIdentity$type, "ServicePrincipal")
  expect_identical(by_name$defaultIdentity$type, "ServicePrincipal")
  expect_identical(
    httr2::url_parse(requested_urls[[1L]])$query$include,
    "DefaultIdentity"
  )
  expect_identical(
    httr2::url_parse(requested_urls[[2L]])$query$include,
    "DefaultIdentity,FutureProperty"
  )
})

test_that("single-item include refreshes a supplied discovery record", {
  workspace_id <- "11111111-1111-4111-8111-111111111111"
  item_id <- "22222222-2222-4222-8222-222222222222"
  requested_url <- NULL
  local_mocked_bindings(
    fabric_resolve_workspace = function(...) {
      list(
        id = workspace_id,
        displayName = "Workspace",
        api_base = "https://example.test/v1"
      )
    },
    .httr2_json = function(req, ...) {
      requested_url <<- req$url
      list(
        id = item_id,
        displayName = "Sales",
        type = "Warehouse",
        defaultIdentity = list(type = "User")
      )
    },
    fabric_enrich_item = function(record, ...) record
  )
  supplied <- structure(
    list(id = item_id, displayName = "Sales", type = "Warehouse"),
    class = c("fabric_item", "list")
  )

  refreshed <- fabric_item(
    "Workspace",
    supplied,
    include = "DefaultIdentity",
    token = "token",
    output = "list"
  )

  expect_identical(refreshed$defaultIdentity$type, "User")
  expect_identical(
    httr2::url_parse(requested_url)$query$include,
    "DefaultIdentity"
  )
})

test_that("item folder options validate before authentication", {
  auth_calls <- 0L
  token <- function(...) {
    auth_calls <<- auth_calls + 1L
    stop("must not authenticate")
  }
  expect_error(
    fabric_items(
      "Workspace",
      root_folder_id = "../not-a-folder-guid",
      token = token
    ),
    "root_folder_id must be NULL or a Fabric folder GUID",
    fixed = TRUE
  )
  for (value in list(
    character(),
    c("DefaultIdentity", "defaultidentity"),
    "x,y"
  )) {
    expect_error(
      fabric_items("Workspace", include = value, token = token),
      "include must be NULL or a unique vector of property names",
      fixed = TRUE
    )
    expect_error(
      fabric_item("Workspace", "Item", include = value, token = token),
      "include must be NULL or a unique vector of property names",
      fixed = TRUE
    )
  }
  expect_equal(auth_calls, 0L)
})

test_that("typed helpers reject unsafe forwarded arguments before auth", {
  token <- function(...) stop("must not authenticate")
  expect_error(
    fabric_lakehouses(
      "Workspace",
      type = "Warehouse",
      token = token
    ),
    '`type` is fixed to "Lakehouse"',
    fixed = TRUE
  )
  expect_error(
    fabric_lakehouses(
      "Workspace",
      TRUE,
      "unexpected positional value",
      token = token
    ),
    "forwarded through `...` must be named",
    fixed = TRUE
  )
  duplicated <- structure(
    list("token-a", "token-b"),
    names = c("token", "token")
  )
  expect_error(
    do.call(fabric_lakehouses, c(list("Workspace"), duplicated)),
    "must have unique names",
    fixed = TRUE
  )
})

test_that("empty discovery results retain their public types", {
  workspaces <- fabric_workspace_list(list())
  items <- fabric_item_list(list())
  expect_identical(class(workspaces), "list")
  expect_length(workspaces, 0L)
  expect_identical(class(items), "list")
  expect_length(items, 0L)
})

test_that("item collections preserve nested service records", {
  record <- list(
    id = "item-id",
    displayName = "Nested",
    type = "Notebook",
    properties = list(
      definition = list(format = "ipynb"),
      enabled = TRUE
    ),
    tags = list(team = "analytics")
  )

  items <- fabric_item_list(list(record))

  expect_identical(class(items), "list")
  expect_length(items, 1L)
  expect_s3_class(items[[1L]], "FabricJobItem")
  expect_identical(items[[1L]]$properties, record$properties)
  expect_identical(items[[1L]]$tags, record$tags)
})

test_that("fabric_item resolves names and rejects type mismatches", {
  local_mocked_bindings(
    fabric_resolve_workspace = function(...) {
      list(id = "workspace-id", displayName = "Workspace")
    },
    .httr2_collection = function(...) {
      list(list(id = "item-id", displayName = "Sales", type = "Warehouse"))
    },
    fabric_enrich_item = function(record, ...) record
  )

  result <- fabric_item(
    "Workspace",
    "Sales",
    type = "Warehouse",
    token = "token"
  )
  expect_s3_class(result, "FabricWarehouse")
  expect_equal(result$workspaceId, "workspace-id")

  expect_error(
    fabric_item(
      "Workspace",
      "Sales",
      type = "Lakehouse",
      token = "token"
    ),
    "not 'Lakehouse'"
  )
})

test_that("fabric_item keeps UDF discovery lightweight unless requested", {
  workspace_id <- "11111111-1111-4111-8111-111111111111"
  item_id <- "22222222-2222-4222-8222-222222222222"
  urls <- character()
  local_mocked_bindings(
    fabric_resolve_workspace = function(...) {
      list(
        id = workspace_id,
        displayName = "Workspace",
        api_base = "https://example.test/v1"
      )
    },
    .httr2_json = function(req, ...) {
      urls <<- c(urls, req$url)
      if (grepl("/userDataFunctions/", req$url, fixed = TRUE)) {
        return(list(
          id = item_id,
          displayName = "Detailed function",
          type = "UserDataFunction",
          description = "Workload detail"
        ))
      }
      list(
        id = item_id,
        displayName = "Function",
        type = "UserDataFunction"
      )
    }
  )

  lightweight <- fabric_item(
    "Workspace",
    item_id,
    token = "token",
    output = "list"
  )
  expect_identical(lightweight$displayName, "Function")
  expect_length(urls, 1L)
  expect_match(urls[[1L]], paste0("/items/", item_id), fixed = TRUE)

  detailed <- fabric_item(
    "Workspace",
    item_id,
    detail = TRUE,
    token = "token",
    output = "list"
  )
  expect_identical(detailed$description, "Workload detail")
  expect_length(urls, 3L)
  expect_match(
    urls[[3L]],
    paste0("/userDataFunctions/", item_id),
    fixed = TRUE
  )
})

test_that("fabric_item can retain a failed detail lookup", {
  workspace_id <- "11111111-1111-4111-8111-111111111111"
  item_id <- "22222222-2222-4222-8222-222222222222"
  local_mocked_bindings(
    fabric_resolve_workspace = function(...) {
      list(
        id = workspace_id,
        displayName = "Workspace",
        api_base = "https://example.test/v1"
      )
    },
    .httr2_json = function(req, ...) {
      if (grepl("/userDataFunctions/", req$url, fixed = TRUE)) {
        rlang::abort("User Data Function detail requires a delegated identity")
      }
      list(
        id = item_id,
        displayName = "Function",
        type = "UserDataFunction"
      )
    }
  )

  expect_warning(
    retained <- fabric_item(
      "Workspace",
      item_id,
      detail = TRUE,
      detail_errors = "record",
      token = "token",
      output = "list"
    ),
    "Could not fully enrich the Fabric item",
    fixed = TRUE
  )
  expect_match(retained$detail_error, "requires a delegated identity")
  expect_identical(retained$detail_error_class, "rlang_error")
})

test_that("fabric_item rejects an item record from another workspace", {
  local_mocked_bindings(
    fabric_resolve_workspace = function(...) {
      list(
        id = "11111111-1111-1111-1111-111111111111",
        displayName = "Workspace"
      )
    },
    fabric_enrich_item = function(record, ...) record
  )
  item <- list(
    id = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
    workspaceId = "22222222-2222-2222-2222-222222222222",
    displayName = "Elsewhere",
    type = "Notebook"
  )

  expect_error(
    fabric_item("Workspace", item, token = "token"),
    "belongs to a different workspace",
    fixed = TRUE
  )
})

test_that("discovered semantic models bypass name lookup for DAX", {
  captured <- NULL
  local_mocked_bindings(
    pbi_execute_dax = function(...) {
      captured <<- list(...)
      tibble::tibble(value = 1)
    }
  )
  model <- structure(
    list(
      id = "22222222-2222-4222-8222-222222222222",
      workspaceId = "11111111-1111-4111-8111-111111111111",
      displayName = "Model",
      type = "SemanticModel"
    ),
    class = c("fabric_item", "list")
  )
  result <- fabric_pbi_dax_query(
    connstr = model,
    dax = 'EVALUATE ROW("value", 1)',
    token = "token"
  )
  expect_equal(
    captured$group_id,
    "11111111-1111-4111-8111-111111111111"
  )
  expect_equal(
    captured$dataset_id,
    "22222222-2222-4222-8222-222222222222"
  )
  expect_equal(result$value, 1)
})

test_that("GraphQL discovery derives an executable endpoint", {
  record <- fabric_add_derived_targets(
    list(
      id = "5b218778-e7a5-4d73-8187-f10824047715",
      displayName = "Products API",
      type = "GraphQLApi",
      workspaceId = "cfafbeb1-8037-4d0c-896e-a46fb27ff229"
    ),
    "https://api.fabric.microsoft.com/v1"
  )

  expect_equal(
    record$graphql_endpoint,
    paste0(
      "https://api.fabric.microsoft.com/v1/workspaces/",
      "cfafbeb1-8037-4d0c-896e-a46fb27ff229/graphqlapis/",
      "5b218778-e7a5-4d73-8187-f10824047715/graphql"
    )
  )
  items <- fabric_item_list(list(record))
  expect_equal(items[[1L]]$graphql_endpoint, record$graphql_endpoint)
})

test_that("Fabric REST API bases accept explicit custom HTTPS origins", {
  expect_equal(
    fabric_api_base("https://api.fabric.microsoft.com"),
    "https://api.fabric.microsoft.com/v1"
  )
  expect_equal(
    fabric_api_base(
      "https://workspace.z13.api.fabric.microsoft.com/v1/"
    ),
    "https://workspace.z13.api.fabric.microsoft.com/v1"
  )
  expect_equal(
    fabric_api_base("https://api.fabric.microsoft.com:443"),
    "https://api.fabric.microsoft.com:443/v1"
  )
  expect_equal(
    fabric_api_base("https://fabric.test/v1"),
    "https://fabric.test/v1"
  )

  invalid <- c(
    "http://api.fabric.microsoft.com/v1",
    "https://user@api.fabric.microsoft.com/v1",
    "https://api.fabric.microsoft.com:8443/v1",
    "https://api.fabric.microsoft.com/v1/workspaces",
    "https://api.fabric.microsoft.com/v1?token=secret",
    "https://api.fabric.microsoft.com/v1#fragment"
  )
  for (endpoint in invalid) {
    expect_error(
      fabric_api_base(endpoint),
      class = "fabric_api_endpoint_error",
      info = endpoint
    )
  }
})
