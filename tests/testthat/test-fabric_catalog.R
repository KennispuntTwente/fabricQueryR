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
