exists_test_workspace_id <- "11111111-1111-4111-8111-111111111111"
exists_test_item_id <- "22222222-2222-4222-8222-222222222222"

exists_test_item <- function() {
  structure(
    list(
      id = exists_test_item_id,
      workspaceId = exists_test_workspace_id,
      displayName = "Lakehouse",
      type = "Lakehouse",
      defaultSchema = "curated"
    ),
    class = c("fabric_item", "list")
  )
}

exists_test_response <- function(
  req,
  status = 200L,
  body = NULL
) {
  headers <- list()
  raw_body <- raw()
  if (!is.null(body)) {
    headers[["content-type"]] <- "application/json"
    raw_body <- charToRaw(jsonlite::toJSON(
      body,
      auto_unbox = TRUE,
      null = "null"
    ))
  }
  httr2::response(
    status_code = status,
    url = req$url,
    headers = headers,
    body = raw_body
  )
}

test_that("Delta existence checks use HEAD and distinguish only 404", {
  requests <- list()
  httr2::local_mocked_responses(function(req) {
    requests[[length(requests) + 1L]] <<- req
    exists_test_response(
      req,
      status = if (length(requests) == 1L) 200L else 404L
    )
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
    function(request) identical(request$method, "HEAD"),
    logical(1)
  )))
  expect_match(requests[[1L]]$url, "/schemas/sales.v2[?]")
  expect_match(
    utils::URLdecode(requests[[1L]]$url),
    paste0("catalog_name=", exists_test_item_id),
    fixed = TRUE
  )
  expect_match(requests[[2L]]$url, "/tables/orders.v2[?]")
  expect_match(
    utils::URLdecode(requests[[2L]]$url),
    "schema_name=sales.v2",
    fixed = TRUE
  )
})

test_that("table existence uses a discovered default schema", {
  request <- NULL
  httr2::local_mocked_responses(function(req) {
    request <<- req
    exists_test_response(req)
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
  expect_equal(requests[[2L]]$method, "HEAD")
  expect_match(
    requests[[2L]]$url,
    "/iceberg/v1/tenant/catalog-prefix/namespaces/sales%20data/tables/orders%202026$"
  )
  expect_equal(audiences, rep(.fabric_audience$storage, 2L))
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
