discovery_response <- function(
  body,
  url = "https://api.fabric.microsoft.com/v1"
) {
  httr2::response(
    status_code = 200L,
    url = url,
    headers = list("content-type" = "application/json"),
    body = charToRaw(jsonlite::toJSON(body, auto_unbox = TRUE))
  )
}

test_that("fabric_workspaces follows pagination and returns workspace lists", {
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
  expect_true(all(purrr::map_lgl(result, inherits, "fabric_workspace")))
  expect_equal(
    purrr::map_chr(result, "displayName"),
    c("Analytics", "Research")
  )
  expect_equal(result[[1L]]$description, "Primary")
  expect_equal(result[[1L]]$capacityRegion$name, "West Europe")
  expect_equal(result[[1L]]$tags$team, "analytics")
  expect_null(result[[2L]]$description)
  expect_match(calls[[1L]], "roles=Admin%2CMember")
  expect_match(calls[[1L]], "preferWorkspaceSpecificEndpoints=true")
  expect_match(calls[[2L]], "continuationToken=page%20two")
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

test_that("workspace-specific API endpoints route item discovery", {
  workspace <- structure(
    list(
      id = "11111111-1111-4111-8111-111111111111",
      displayName = "Private workspace",
      apiEndpoint = "https://workspace.z13.api.fabric.microsoft.com/"
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
  fabric_item(
    workspace,
    "33333333-3333-4333-8333-333333333333",
    token = "token"
  )
  expect_true(all(grepl(
    "https://workspace.z13.api.fabric.microsoft.com/v1/workspaces/",
    item_urls,
    fixed = TRUE
  )))

  fabric_items(
    workspace,
    token = "token",
    api_base = "https://explicit.test/v1"
  )
  expect_match(urls[[2L]], "https://explicit.test/v1/workspaces/", fixed = TRUE)
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
  expect_s3_class(lakehouse, "fabric_item")
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
    "Could not retrieve workload details for 1 Fabric item",
    fixed = TRUE
  )
  expect_length(result, 2L)
  expect_true(all(vapply(result, inherits, logical(1), "fabric_item")))
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

test_that("typed routes and derived targets cover supported workloads", {
  expect_equal(
    unname(vapply(
      c(
        "Lakehouse",
        "Warehouse",
        "SQLDatabase",
        "SemanticModel",
        "Eventhouse",
        "KQLDatabase",
        "Notebook",
        "GraphQLApi"
      ),
      fabric_item_route,
      character(1)
    )),
    c(
      "lakehouses",
      "warehouses",
      "sqlDatabases",
      "semanticModels",
      "eventhouses",
      "kqlDatabases",
      "notebooks",
      "graphQLApis"
    )
  )

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

  semantic_model <- fabric_add_derived_targets(
    list(
      id = "model-id",
      workspaceId = "workspace-id",
      workspaceDisplayName = "Data & AI",
      type = "SemanticModel",
      displayName = "Sales Model"
    ),
    .fabric_api_base
  )
  expect_match(
    semantic_model$dax_connection_string,
    "Data%20%26%20AI",
    fixed = TRUE
  )

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

test_that("typed convenience helpers forward their workload types", {
  calls <- list()
  local_mocked_bindings(
    fabric_items = function(workspace, type, detail, ...) {
      calls[[length(calls) + 1L]] <<- list(
        workspace = workspace,
        type = type,
        detail = detail
      )
      list()
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
    get(name, mode = "function")("Workspace", token = "token")
  }
  expect_equal(
    vapply(calls, `[[`, character(1), "type"),
    unname(unlist(helpers))
  )
  expect_true(all(vapply(calls, `[[`, logical(1), "detail")))
  expect_true(all(
    vapply(calls, `[[`, character(1), "workspace") == "Workspace"
  ))
  fabric_lakehouses("Workspace", detail = FALSE, token = "token")
  expect_false(calls[[length(calls)]]$detail)
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
  expect_s3_class(items[[1L]], "fabric_item")
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
  expect_s3_class(result, "fabric_item")
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
