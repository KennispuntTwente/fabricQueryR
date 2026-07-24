discovery_response <- function(body, url = "https://api.fabric.microsoft.com/v1") {
  httr2::response(
    status_code = 200L,
    url = url,
    headers = list("content-type" = "application/json"),
    body = charToRaw(jsonlite::toJSON(body, auto_unbox = TRUE))
  )
}

test_that("fabric_workspaces follows pagination and returns stable columns", {
  calls <- character()
  httr2::local_mocked_responses(function(req) {
    calls <<- c(calls, req$url)
    if (length(calls) == 1L) {
      discovery_response(
        list(
          value = list(list(
            id = "11111111-1111-4111-8111-111111111111",
            displayName = "Analytics",
            description = "Primary",
            type = "Workspace",
            capacityId = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
          )),
          continuationToken = "page two"
        ),
        req$url
      )
    } else {
      discovery_response(
        list(value = list(list(
          id = "22222222-2222-4222-8222-222222222222",
          displayName = "Research",
          type = "Workspace"
        ))),
        req$url
      )
    }
  })

  result <- fabric_workspaces(
    roles = c("Admin", "Member"),
    prefer_workspace_endpoints = TRUE,
    access_token = "token"
  )

  expect_s3_class(result, "tbl_df")
  expect_equal(result$displayName, c("Analytics", "Research"))
  expect_equal(result$description, c("Primary", NA_character_))
  expect_match(calls[[1L]], "roles=Admin%2CMember")
  expect_match(calls[[1L]], "preferWorkspaceSpecificEndpoints=true")
  expect_match(calls[[2L]], "continuationToken=page%20two")
  expect_true(all(c("tags", "raw") %in% names(result)))
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
    access_token = "token",
    api_base = "https://fabric.test/v1/"
  )

  expect_equal(result$type, "Lakehouse")
  expect_equal(
    result$sql_server,
    "server.datawarehouse.fabric.microsoft.com"
  )
  expect_equal(result$sql_database, "SalesLake")
  expect_equal(result$one_lake_tables_path, "https://onelake/Tables")
  expect_equal(
    result$livy_url,
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

test_that("typed routes and derived targets cover supported workloads", {
  expect_equal(
    unname(vapply(
      c(
        "Lakehouse", "Warehouse", "SQLDatabase", "SemanticModel",
        "Eventhouse", "KQLDatabase", "Notebook", "GraphQLApi"
      ),
      fabric_item_route,
      character(1)
    )),
    c(
      "lakehouses", "warehouses", "sqlDatabases", "semanticModels",
      "eventhouses", "kqlDatabases", "notebooks", "graphQLApis"
    )
  )

  sql_database <- fabric_add_derived_targets(list(
    id = "sql-id",
    type = "SQLDatabase",
    displayName = "Orders",
    properties = list(
      connectionString = "Server=sql;Initial Catalog=orders-id",
      serverFqdn = "sql.database.fabric.microsoft.com,1433",
      databaseName = "orders-id"
    )
  ), .fabric_api_base)
  expect_equal(sql_database$sql_database, "orders-id")
  expect_equal(
    sql_database$sql_server,
    "sql.database.fabric.microsoft.com,1433"
  )

  semantic_model <- fabric_add_derived_targets(list(
    id = "model-id",
    workspaceId = "workspace-id",
    workspaceDisplayName = "Data & AI",
    type = "SemanticModel",
    displayName = "Sales Model"
  ), .fabric_api_base)
  expect_match(
    semantic_model$dax_connection_string,
    "Data%20%26%20AI",
    fixed = TRUE
  )

  eventhouse <- fabric_add_derived_targets(list(
    id = "event-id",
    type = "Eventhouse",
    displayName = "Events",
    properties = list(
      queryServiceUri = "https://cluster.kusto.fabric.microsoft.com",
      ingestionServiceUri = "https://ingest-cluster.kusto.fabric.microsoft.com"
    )
  ), .fabric_api_base)
  expect_equal(
    eventhouse$query_service_uri,
    "https://cluster.kusto.fabric.microsoft.com"
  )
})

test_that("typed convenience helpers forward their workload types", {
  calls <- list()
  local_mocked_bindings(
    fabric_items = function(workspace, type, detail, ...) {
      calls[[length(calls) + 1L]] <<- list(
        workspace = workspace,
        type = type,
        detail = detail
      )
      tibble::tibble()
    }
  )
  helpers <- list(
    fabric_lakehouses = "Lakehouse",
    fabric_warehouses = "Warehouse",
    fabric_sql_databases = "SQLDatabase",
    fabric_semantic_models = "SemanticModel",
    fabric_eventhouses = "Eventhouse",
    fabric_kql_databases = "KQLDatabase",
    fabric_notebooks = "Notebook",
    fabric_graphql_apis = "GraphQLApi"
  )
  for (name in names(helpers)) {
    get(name, mode = "function")("Workspace", access_token = "token")
  }
  expect_equal(
    vapply(calls, `[[`, character(1), "type"),
    unname(unlist(helpers))
  )
  expect_true(all(vapply(calls, `[[`, logical(1), "detail")))
  expect_true(all(vapply(calls, `[[`, character(1), "workspace") == "Workspace"))
})

test_that("empty discovery results retain their public schema", {
  workspaces <- fabric_workspace_tbl(list())
  items <- fabric_item_tbl(list())
  expect_s3_class(workspaces, "tbl_df")
  expect_s3_class(items, "tbl_df")
  expect_equal(nrow(workspaces), 0L)
  expect_equal(nrow(items), 0L)
  expect_true(all(c("id", "displayName", "tags", "raw") %in% names(workspaces)))
  expect_true(all(c("id", "properties", "raw") %in% names(items)))
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
    access_token = "token"
  )
  expect_s3_class(result, "fabric_item")
  expect_equal(result$workspaceId, "workspace-id")

  expect_error(
    fabric_item(
      "Workspace",
      "Sales",
      type = "Lakehouse",
      access_token = "token"
    ),
    "not 'Lakehouse'"
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
      id = "dataset-id",
      workspaceId = "workspace-id",
      displayName = "Model",
      type = "SemanticModel"
    ),
    class = c("fabric_item", "list")
  )
  result <- fabric_pbi_dax_query(
    connstr = model,
    dax = 'EVALUATE ROW("value", 1)',
    access_token = "token"
  )
  expect_equal(captured$group_id, "workspace-id")
  expect_equal(captured$dataset_id, "dataset-id")
  expect_equal(result$value, 1)
})
