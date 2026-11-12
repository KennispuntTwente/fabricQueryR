test_that("personal semantic models use My Workspace routes through R6 and lists", {
  urls <- character()
  httr2::local_mocked_responses(function(req) {
    urls <<- c(urls, req$url)
    if (endsWith(req$url, "/executeQueries")) {
      return(httr2::response(
        200L, url = req$url,
        headers = list("content-type" = "application/json"),
        body = charToRaw(jsonlite::toJSON(list(results = list(list(
          tables = list(list(rows = list(list("[value]" = 1L))))
        ))), auto_unbox = TRUE))
      ))
    }
    httr2::response(
      202L, url = req$url,
      headers = list(RequestId = pbi_refresh_id), body = raw()
    )
  })
  for (workspace_type in c("Personal", "PersonalGroup")) {
    record <- list(
      id = pbi_test_dataset_id, workspaceId = pbi_test_workspace_id,
      type = "SemanticModel", displayName = "Personal model",
      workspaceType = workspace_type
    )
    model <- fabric_r6_record(
      record, c("fabric_item", "list"), fabric_credential(token = "test-token")
    )
    for (explicit in c(FALSE, TRUE)) {
      options <- if (explicit) list(my_workspace = TRUE) else list()
      query <- do.call(model$dax_query, c(
        list(dax = 'EVALUATE ROW("value", 1)'), options
      ))
      expect_identical(query[["[value]"]], 1L)
      expect_true(startsWith(
        tail(urls, 1L),
        paste0("https://api.powerbi.com/v1.0/myorg/datasets/", record$id, "/")
      ))
      refresh <- do.call(
        model$refresh, c(list(principal_type = "delegated"), options)
      )
      expect_true(refresh$my_workspace)
      expect_null(refresh$workspace_id)
      expect_true(all(startsWith(
        tail(urls, 2L),
        paste0("https://api.powerbi.com/v1.0/myorg/datasets/", record$id, "/")
      )))
    }
    fabric_pbi_dax_query(record, 'EVALUATE ROW("value", 1)', token = "test-token")
    refresh <- fabric_pbi_refresh(
      record, token = "test-token", principal_type = "delegated"
    )
    expect_true(refresh$my_workspace)
    expect_true(all(!grepl("/groups/", urls, fixed = TRUE)))
    expect_error(
      model$dax_query("EVALUATE ROW()", my_workspace = NA),
      "my_workspace must be"
    )
    expect_error(model$refresh(my_workspace = "yes"), "my_workspace must be")
    expect_error(model$refresh(workspace_id = record$workspaceId), "not both")
  }
})

test_that("personal refresh handles retain their route across lifecycle calls", {
  urls <- character()
  httr2::local_mocked_responses(function(req) {
    urls <<- c(urls, req$url)
    if (identical(req$method, "GET")) {
      detail <- list(
        requestId = pbi_refresh_id, status = "Completed",
        extendedStatus = "Completed", refreshType = "ViaEnhancedApi"
      )
      body <- if (endsWith(req$url, "/refreshes")) {
        list(value = list(detail))
      } else {
        detail
      }
      return(httr2::response(
        200L, url = req$url,
        headers = list("content-type" = "application/json"),
        body = charToRaw(jsonlite::toJSON(body, auto_unbox = TRUE))
      ))
    }
    httr2::response(
      202L, url = req$url,
      headers = list(RequestId = pbi_refresh_id), body = raw()
    )
  })
  model <- fabric_r6_record(
    list(
      id = pbi_test_dataset_id, workspaceId = pbi_test_workspace_id,
      type = "SemanticModel", workspaceType = "Personal"
    ),
    c("fabric_item", "list"), fabric_credential(token = "test-token")
  )
  refresh <- model$refresh(mode = "enhanced", type = "Full")
  expect_length(model$refresh_history(), 1L)
  detail <- model$refresh_status(refresh)
  expect_identical(model$refresh_wait(detail)$state, "Completed")
  saved <- unserialize(serialize(refresh, NULL))
  expect_identical(model$refresh_status(saved)$state, "Completed")
  expect_identical(model$refresh_status(pbi_refresh_id)$state, "Completed")
  expect_true(model$refresh_cancel(refresh))
  expect_true(all(startsWith(
    urls,
    paste0("https://api.powerbi.com/v1.0/myorg/datasets/", pbi_test_dataset_id, "/")
  )))
})

test_that("catalog objects retain workspace API and OneLake routing metadata", {
  api <- "https://workspace.z13.api.fabric.microsoft.com"
  dfs <- "https://workspace.z13.dfs.fabric.microsoft.com"
  urls <- character()
  workspace_record <- list(
    id = catalog_test_workspace_id, displayName = "Private", type = "Workspace",
    apiEndpoint = api, oneLakeEndpoints = list(dfsEndpoint = dfs)
  )
  httr2::local_mocked_responses(function(req) {
    urls <<- c(urls, req$url)
    if (endsWith(req$url, "/catalog/search")) {
      lake <- catalog_test_entry()
      lake$hierarchy$workspace <- workspace_record
      return(catalog_test_response(
        list(value = list(workspace_record, lake)), req$url
      ))
    }
    body <- if (grepl(".dfs.fabric.microsoft.com", req$url, fixed = TRUE)) {
      list(paths = list())
    } else {
      list(value = list())
    }
    discovery_response(body, req$url)
  })
  result <- fabric_catalog_search(token = function(...) "test-token")
  workspace <- result[[1L]]
  lakehouse <- result[[2L]]
  workspace$items(detail = FALSE)
  expect_true(startsWith(tail(urls, 1L), paste0(api, "/v1/workspaces/")))
  lakehouse$onelake_list()
  expect_true(startsWith(tail(urls, 1L), paste0(dfs, "/")))
  expect_identical(lakehouse$workspaceApiEndpoint, api)
  expect_identical(lakehouse$workspaceOneLakeDfsEndpoint, dfs)
  expect_true(startsWith(lakehouse$livy_url, paste0(api, "/v1/")))
  workspace$items(detail = FALSE, api_base = "https://explicit.test/v1")
  expect_true(startsWith(tail(urls, 1L), "https://explicit.test/v1/"))
  records <- fabric_catalog_search(token = "test-token", output = "list")
  expect_identical(onelake_resolve_target(NULL, records[[2L]])$dfs_base, dfs)
})

test_that("retrying item enrichment replaces stale detail errors", {
  record <- r6_test_record()$as_list()
  record$detail_error <- "old private SQL failure"
  record$detail_error_class <- "old_error"
  record$detail_error_stage <- "private_sql_connection_string"
  workspace <- list(id = record$workspaceId, displayName = "Analytics")
  fail <- FALSE
  local_mocked_bindings(
    .httr2_json = function(req, ...) {
      if (fail) rlang::abort("current workload failure", class = "current_error")
      list(
        id = record$id, type = "Lakehouse",
        properties = list(defaultSchema = "dbo")
      )
    }
  )
  expect_no_warning(fresh <- fabric_item(workspace, record, token = "test-token"))
  expect_identical(fresh$default_schema, "dbo")
  expect_null(fresh$detail_error)
  expect_null(fresh$detail_error_class)
  expect_null(fresh$detail_error_stage)
  fail <- TRUE
  expect_warning(
    fresh <- fabric_item(
      workspace, record, token = "test-token", detail_errors = "record"
    ),
    "Could not fully enrich"
  )
  expect_identical(fresh$detail_error, "current workload failure")
  expect_identical(fresh$detail_error_class, "current_error")
  expect_null(fresh$detail_error_stage)
})

test_that("SQL discovery preserves authoritative connection-string catalogs", {
  server <- "server.datawarehouse.fabric.microsoft.com"
  connection_string <- paste0("Server=", server, ";Initial Catalog=ActualCatalog;")
  for (type in c("Lakehouse", "Warehouse", "WarehouseSnapshot", "MirroredDatabase")) {
    for (connection in c(connection_string, server)) {
      local_mocked_bindings(
        .httr2_collection = function(...) {
          list(list(
            id = catalog_test_item_id, type = type, displayName = "Friendly name"
          ))
        },
        .httr2_json = function(req, ...) {
          properties <- if (type %in% c("Warehouse", "WarehouseSnapshot")) {
            list(connectionString = connection)
          } else {
            list(sqlEndpointProperties = list(connectionString = connection))
          }
          list(id = catalog_test_item_id, type = type, properties = properties)
        }
      )
      item <- fabric_items(
        list(id = catalog_test_workspace_id, displayName = "Analytics"),
        type = type, detail = TRUE, token = "test-token"
      )[[1L]]
      expected <- if (identical(connection, server)) {
        "Friendly name"
      } else {
        "ActualCatalog"
      }
      expect_identical(item$sql_connection_info()$database, expected)
      expect_identical(fabric_sql_connection_info(item$as_list())$database, expected)
      expect_identical(
        item$sql_connection_info(database = "Override")$database, "Override"
      )
    }
  }
})
