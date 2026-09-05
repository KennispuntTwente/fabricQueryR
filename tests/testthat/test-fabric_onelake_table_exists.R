test_that("Delta existence checks search metadata inventories", {
  requests <- list()
  httr2::local_mocked_responses(function(req) {
    requests[[length(requests) + 1L]] <<- req
    body <- if (grepl("/schemas", req$url, fixed = TRUE)) {
      list(schemas = list(list(name = "sales.v2")))
    } else {
      list(tables = list())
    }
    exists_test_response(req, body = body)
  })

  schema_exists <- fabric_onelake_schema_exists(
    exists_test_item(),
    "sales.v2",
    token = "storage-token"
  )
  table_exists <- fabric_onelake_table_exists(
    exists_test_item(),
    "orders.v2",
    schema = "sales.v2",
    token = "storage-token"
  )

  expect_true(schema_exists)
  expect_false(table_exists)
  expect_length(requests, 2L)
  expect_true(all(vapply(
    requests,
    function(request) identical(request$method %||% "GET", "GET"),
    logical(1)
  )))
  expect_match(requests[[1L]]$url, "/schemas[?]")
  expect_match(
    utils::URLdecode(requests[[1L]]$url),
    paste0("catalog_name=", exists_test_item_id),
    fixed = TRUE
  )
  expect_match(requests[[2L]]$url, "/tables[?]")
  expect_match(
    utils::URLdecode(requests[[2L]]$url),
    "schema_name=sales.v2",
    fixed = TRUE
  )
})

test_that("Delta schema existence follows pagination", {
  requests <- list()
  httr2::local_mocked_responses(function(req) {
    requests[[length(requests) + 1L]] <<- req
    body <- if (length(requests) == 1L) {
      list(
        schemas = list(list(name = "curated")),
        next_page_token = "next-page"
      )
    } else {
      list(schemas = list(list(name = "dbo")))
    }
    exists_test_response(req, body = body)
  })

  result <- fabric_onelake_schema_exists(
    exists_test_item(),
    "dbo",
    token = "storage-token"
  )

  expect_true(result)
  expect_length(requests, 2L)
  expect_null(httr2::url_parse(requests[[1L]]$url)$query$page_token)
  expect_equal(
    httr2::url_parse(requests[[2L]]$url)$query$page_token,
    "next-page"
  )
})

test_that("table existence uses a discovered default schema", {
  request <- NULL
  httr2::local_mocked_responses(function(req) {
    request <<- req
    exists_test_response(
      req,
      body = list(tables = list(list(name = "orders")))
    )
  })

  result <- fabric_onelake_table_exists(
    exists_test_item(),
    list(name = "orders"),
    token = "storage-token"
  )

  expect_true(result)
  expect_match(
    utils::URLdecode(request$url),
    "schema_name=curated",
    fixed = TRUE
  )
})

test_that("Iceberg existence resolves and validates the service prefix", {
  requests <- list()
  audiences <- character()
  provider <- function(audience, force_refresh = FALSE) {
    audiences <<- c(audiences, audience)
    "audience-token"
  }
  httr2::local_mocked_responses(function(req) {
    requests[[length(requests) + 1L]] <<- req
    if (length(requests) == 1L) {
      return(exists_test_response(
        req,
        body = list(
          defaults = list(),
          overrides = list(prefix = "tenant/catalog-prefix")
        )
      ))
    }
    exists_test_response(req)
  })

  result <- fabric_onelake_table_exists(
    exists_test_item(),
    "orders 2026",
    schema = "sales data",
    protocol = "iceberg",
    token = provider
  )

  expect_true(result)
  expect_length(requests, 2L)
  expect_equal(requests[[1L]]$method %||% "GET", "GET")
  expect_match(requests[[1L]]$url, "/iceberg/v1/config[?]")
  expect_match(
    utils::URLdecode(requests[[1L]]$url),
    paste0(
      "warehouse=",
      exists_test_workspace_id,
      "/",
      exists_test_item_id
    ),
    fixed = TRUE
  )
  expect_equal(requests[[2L]]$method %||% "GET", "GET")
  expect_match(
    requests[[2L]]$url,
    "/iceberg/v1/tenant/catalog-prefix/namespaces/sales%20data/tables/orders%202026$"
  )
  expect_equal(audiences, rep(.fabric_audience$storage, 2L))
})

test_that("Iceberg existence converts only HTTP 404 to false", {
  requests <- list()
  httr2::local_mocked_responses(function(req) {
    requests[[length(requests) + 1L]] <<- req
    if (length(requests) == 1L) {
      return(exists_test_response(
        req,
        body = list(overrides = list(prefix = "tenant/catalog-prefix"))
      ))
    }
    exists_test_response(req, status = 404L)
  })

  result <- fabric_onelake_schema_exists(
    exists_test_item(),
    "missing",
    protocol = "iceberg",
    token = "storage-token"
  )

  expect_identical(result, FALSE)
  expect_length(requests, 2L)
  expect_equal(requests[[2L]]$method %||% "GET", "GET")
})

test_that("existence checks reject unsafe protocol routing", {
  calls <- 0L
  httr2::local_mocked_responses(function(req) {
    calls <<- calls + 1L
    exists_test_response(
      req,
      body = list(overrides = list(prefix = "../other-item"))
    )
  })

  error <- rlang::catch_cnd(fabric_onelake_schema_exists(
    exists_test_item(),
    "dbo",
    protocol = "iceberg",
    token = "storage-token"
  ))
  expect_s3_class(error, "fabric_onelake_table_protocol_error")
  expect_match(conditionMessage(error), "invalid Iceberg catalog prefix")
  expect_equal(calls, 1L)

  error <- rlang::catch_cnd(fabric_onelake_schema_exists(
    exists_test_item(),
    "dbo",
    protocol = "delta",
    table_api_base = "https://catalog.test/iceberg",
    token = "storage-token"
  ))
  expect_s3_class(error, "fabric_onelake_table_protocol_error")
  expect_match(conditionMessage(error), "end in /delta")
  expect_equal(calls, 1L)

  for (schema in c("../other", "dbo?redirect=true", "dbo\\other")) {
    error <- rlang::catch_cnd(fabric_onelake_schema_exists(
      exists_test_item(),
      schema,
      token = "storage-token"
    ))
    expect_s3_class(error, "fabric_onelake_table_protocol_error")
    expect_match(conditionMessage(error), "safe table metadata path segment")
  }
  expect_equal(calls, 1L)
})
test_that("Delta table existence distinguishes missing schemas from service errors", {
  for (code in c("PathNotFound", "ItemNotFound", "Forbidden")) {
    httr2::local_mocked_responses(function(req) {
      if (grepl("/schemas", req$url, fixed = TRUE)) {
        return(exists_test_response(req, body = list(schemas = list())))
      }
      exists_test_response(
        req,
        status = if (code == "Forbidden") 403L else 404L,
        body = list(errorCode = code, message = "missing")
      )
    })
    result <- tryCatch(
      fabric_onelake_table_exists(
        exists_test_item(),
        "missing",
        schema = "missing",
        token = "storage-token"
      ),
      error = identity
    )
    if (code == "PathNotFound") {
      expect_identical(result, FALSE)
    } else {
      expect_s3_class(result, "fabric_http_error")
    }
  }
})
