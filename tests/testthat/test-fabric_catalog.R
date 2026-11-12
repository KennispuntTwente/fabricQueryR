test_that("catalog search paginates POST bodies and returns R6 items", {
  requests <- list()
  httr2::local_mocked_responses(function(req) {
    requests[[length(requests) + 1L]] <<- req
    body <- if (length(requests) == 1L) {
      list(value = list(catalog_test_entry()), continuationToken = "page-2")
    } else {
      list(value = list(catalog_test_entry("Archive Lakehouse")))
    }
    catalog_test_response(body, req$url)
  })

  result <- fabric_catalog_search(
    search = "sales",
    types = c("Lakehouse", "Warehouse"),
    page_size = 25,
    token = "test-token"
  )

  expect_length(result, 2L)
  expect_s3_class(result[[1L]], "FabricLakehouse")
  expect_s3_class(result[[1L]]$as_list(), "fabric_catalog_entry")
  expect_equal(result[[1L]]$workspaceId, catalog_test_workspace_id)
  expect_equal(result[[1L]]$workspaceDisplayName, "Sales Analytics")
  expect_equal(result[[2L]]$displayName, "Archive Lakehouse")
  expect_length(requests, 2L)
  expect_equal(requests[[1L]]$method, "POST")
  expect_equal(requests[[1L]]$url, paste0(.fabric_api_base, "/catalog/search"))
  expect_equal(requests[[1L]]$body$data$search, "sales")
  expect_equal(requests[[1L]]$body$data$pageSize, 25L)
  expect_equal(
    requests[[1L]]$body$data$filter,
    "Type eq 'Lakehouse' or Type eq 'Warehouse'"
  )
  expect_null(requests[[1L]]$body$data$continuationToken)
  expect_named(requests[[2L]]$body$data, "continuationToken")
  expect_equal(requests[[2L]]$body$data$continuationToken, "page-2")
})

test_that("catalog Lakehouses can start Livy without workload detail requests", {
  for (api_base in c(
    .fabric_api_base,
    "https://workspace.z13.api.fabric.microsoft.com/v1"
  )) {
    for (output in c("r6", "list")) {
      urls <- character()
      httr2::local_mocked_responses(function(req) {
        urls <<- c(urls, req$url)
        if (endsWith(req$url, "/catalog/search")) {
          return(catalog_test_response(
            list(value = list(catalog_test_entry())),
            req$url
          ))
        }
        httr2::response(
          201L,
          headers = list("content-type" = "application/json"),
          body = charToRaw('{"id":1,"state":"idle"}'),
          url = req$url
        )
      })
      lakehouse <- fabric_catalog_search(
        types = "Lakehouse",
        token = "test-token",
        api_base = api_base,
        output = output
      )[[1L]]
      endpoint <- paste0(
        api_base,
        "/workspaces/", catalog_test_workspace_id,
        "/lakehouses/", lakehouse$id,
        "/livyapi/versions/2023-12-01/sessions"
      )

      session <- if (identical(output, "r6")) {
        lakehouse$livy_session(verbose = FALSE)
      } else {
        fabric_livy_session(lakehouse, token = "test-token", verbose = FALSE)
      }
      expect_identical(lakehouse$livy_url, endpoint)
      expect_identical(session$url, paste0(endpoint, "/1"))
      expect_identical(urls, c(paste0(api_base, "/catalog/search"), endpoint))
    }
  }
})

test_that("unfiltered catalog browsing handles workspace and item records", {
  workspace <- list(
    id = catalog_test_workspace_id,
    displayName = "Sales Analytics",
    type = "Workspace"
  )
  httr2::local_mocked_responses(function(req) {
    catalog_test_response(
      list(value = list(workspace, catalog_test_entry())),
      req$url
    )
  })
  result <- fabric_catalog_search(token = "test-token")
  expect_length(result, 2L)
  expect_s3_class(result[[1L]], "FabricWorkspace")
  expect_identical(result[[1L]]$id, catalog_test_workspace_id)
  expect_s3_class(result[[2L]], "FabricLakehouse")
  records <- fabric_catalog_search(token = "test-token", output = "list")
  expect_s3_class(records[[1L]], "fabric_workspace")
  expect_s3_class(records[[1L]], "fabric_catalog_entry")
  expect_identical(records[[2L]]$workspaceId, records[[1L]]$id)
})

test_that("catalog search can return plain item records explicitly", {
  httr2::local_mocked_responses(function(req) {
    catalog_test_response(
      list(value = list(catalog_test_entry())),
      req$url
    )
  })

  result <- fabric_catalog_search(token = "test-token", output = "list")

  expect_length(result, 1L)
  expect_s3_class(result[[1L]], "fabric_catalog_entry")
  expect_s3_class(result[[1L]], "fabric_item")
  expect_equal(result[[1L]]$workspaceId, catalog_test_workspace_id)
})

test_that("catalog GraphQL records retain their discovery API endpoint", {
  api_base <- "https://workspace.z13.api.fabric.microsoft.com/v1"
  urls <- character()
  httr2::local_mocked_responses(function(req) {
    urls <<- c(urls, req$url)
    if (endsWith(req$url, "/catalog/search")) {
      entry <- catalog_test_entry()
      entry$type <- "GraphQLApi"
      return(catalog_test_response(list(value = list(entry)), req$url))
    }
    graphql_test_response(list(data = list(value = 3L)), url = req$url)
  })
  for (output in c("r6", "list")) {
    api <- fabric_catalog_search(
      types = "GraphQLApi",
      api_base = api_base,
      output = output,
      token = "test-token"
    )[[1L]]
    fabric_graphql_query(api, "{ value }", token = "test-token")
    expect_identical(
      tail(urls, 1L),
      paste0(
        api_base, "/workspaces/", catalog_test_workspace_id,
        "/graphqlapis/", catalog_test_item_id, "/graphql"
      )
    )
  }
})

test_that("catalog search validates filters before making requests", {
  calls <- 0L
  httr2::local_mocked_responses(function(req) {
    calls <<- calls + 1L
    catalog_test_response(list(value = list()), req$url)
  })

  invalid_calls <- list(
    function() fabric_catalog_search(search = "", token = "test-token"),
    function() fabric_catalog_search(types = character(), token = "test-token"),
    function() {
      fabric_catalog_search(
        types = c("Lakehouse", "lakehouse"),
        token = "test-token"
      )
    },
    function() fabric_catalog_search(types = "Bad Type", token = "test-token"),
    function() {
      fabric_catalog_search(
        types = "Lakehouse",
        filter = "Type ne 'Report'",
        token = "test-token"
      )
    },
    function() fabric_catalog_search(page_size = 0, token = "test-token"),
    function() fabric_catalog_search(page_size = 1001, token = "test-token"),
    function() fabric_catalog_search(page_size = 1.5, token = "test-token")
  )
  for (call in invalid_calls) {
    expect_s3_class(rlang::catch_cnd(call()), "rlang_error")
  }
  expect_equal(calls, 0L)
})

test_that("catalog search rejects repeated tokens and malformed records", {
  calls <- 0L
  httr2::local_mocked_responses(function(req) {
    calls <<- calls + 1L
    catalog_test_response(
      list(value = list(), continuationToken = "repeated"),
      req$url
    )
  })

  error <- rlang::catch_cnd(fabric_catalog_search(token = "test-token"))
  expect_s3_class(error, "fabric_catalog_protocol_error")
  expect_match(conditionMessage(error), "repeated")
  expect_equal(error$page_number, 2L)
  expect_equal(calls, 2L)

  bad <- catalog_test_entry()
  bad$hierarchy$workspace$id <- "not-a-guid"
  httr2::local_mocked_responses(function(req) {
    catalog_test_response(list(value = list(bad)), req$url)
  })
  error <- rlang::catch_cnd(fabric_catalog_search(token = "test-token"))
  expect_s3_class(error, "fabric_catalog_protocol_error")
  expect_match(conditionMessage(error), "valid item and workspace identity")
})
