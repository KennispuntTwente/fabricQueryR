graphql_test_response <- function(
  body,
  status = 200L,
  url = "https://api.fabric.microsoft.com/graphql"
) {
  if (!is.raw(body)) {
    body <- charToRaw(jsonlite::toJSON(
      body,
      auto_unbox = TRUE,
      null = "null"
    ))
  }
  httr2::response(
    status_code = status,
    url = url,
    headers = list("content-type" = "application/json"),
    body = body
  )
}

graphql_fake_azure_token <- function() {
  class <- R6::R6Class(
    "GraphQLFakeAzureToken",
    inherit = AzureAuth::AzureToken,
    public = list(
      initialize = function() {
        self$credentials <- list(access_token = "azure-token")
      },
      validate = function() TRUE,
      refresh = function() invisible(self)
    )
  )
  class$new()
}

test_that("GraphQL endpoints resolve from URLs, IDs, and discovery records", {
  workspace_id <- "cfafbeb1-8037-4d0c-896e-a46fb27ff229"
  api_id <- "5b218778-e7a5-4d73-8187-f10824047715"
  expected <- paste0(
    "https://api.fabric.microsoft.com/v1/workspaces/",
    workspace_id,
    "/graphqlapis/",
    api_id,
    "/graphql"
  )

  expect_equal(
    graphql_resolve_endpoint(api_id, workspace_id = workspace_id),
    expected
  )
  expect_equal(
    graphql_resolve_endpoint(list(
      id = api_id,
      type = "GraphQLApi",
      workspaceId = workspace_id
    )),
    expected
  )
  expect_equal(
    graphql_resolve_endpoint(
      list(
        id = api_id,
        type = "GraphQLApi",
        graphql_endpoint = "https://custom.test/graphql/"
      ),
      allow_custom_endpoint = TRUE
    ),
    "https://custom.test/graphql"
  )
  expect_error(
    graphql_resolve_endpoint(api_id),
    "workspace_id must be a GUID",
    fixed = TRUE
  )
  expect_error(
    graphql_resolve_endpoint(list(
      id = "lakehouse",
      type = "Lakehouse",
      workspaceId = workspace_id
    )),
    "GraphQLApi",
    fixed = TRUE
  )
  expect_error(
    graphql_resolve_endpoint("http://unsafe.test/graphql"),
    "valid HTTPS",
    fixed = TRUE
  )
  expect_error(
    graphql_resolve_endpoint("https://attacker.example/graphql"),
    "Microsoft Fabric endpoint",
    fixed = TRUE
  )
  expect_equal(
    graphql_resolve_endpoint(
      "https://trusted.example/graphql",
      allow_custom_endpoint = TRUE
    ),
    "https://trusted.example/graphql"
  )
  expect_error(
    graphql_resolve_endpoint(
      "https://api.fabric.microsoft.com/graphql?redirect=attacker"
    ),
    "valid HTTPS",
    fixed = TRUE
  )
})

test_that("fabric_graphql_query sends variables and operation names unchanged", {
  captured <- NULL
  httr2::local_mocked_responses(function(req) {
    captured <<- req
    graphql_test_response(
      list(
        data = list(
          products = list(
            items = list(
              list(id = 2L, name = "beta", amount = NULL)
            )
          )
        ),
        extensions = list(requestId = "request-id")
      ),
      url = req$url
    )
  })
  audiences <- character()
  document <- paste(
    "query Products($category: String!) {",
    "products(filter: {category: {eq: $category}}) {",
    "items { id name amount }",
    "}",
    "}"
  )

  result <- fabric_graphql_query(
    "https://api.fabric.microsoft.com/graphql",
    query = document,
    variables = list(category = "B", nullable = NULL),
    operation_name = "Products",
    timeout = 17,
    token = function(audience, force_refresh = FALSE) {
      audiences <<- c(audiences, audience)
      "graphql-token"
    }
  )

  expect_s3_class(result, "fabric_graphql_result")
  expect_equal(result$data$products$items[[1L]]$id, 2L)
  expect_null(result$data$products$items[[1L]]$amount)
  expect_length(result$errors, 0L)
  expect_equal(result$extensions$requestId, "request-id")
  expect_equal(
    audiences,
    "https://analysis.windows.net/powerbi/api/GraphQLApi.Execute.All"
  )
  expect_equal(captured$options$timeout_ms, 17000)
  expect_equal(
    captured$headers$accept,
    "application/graphql-response+json"
  )
  expect_equal(captured$body$data$query, document)
  expect_equal(captured$body$data$variables$category, "B")
  expect_null(captured$body$data$variables$nullable)
  expect_equal(captured$body$data$operationName, "Products")
})

test_that("GraphQL selects the audience from the AzureAuth flow", {
  calls <- list()
  local_mocked_bindings(
    get_azure_token = function(...) {
      calls[[length(calls) + 1L]] <<- list(...)
      graphql_fake_azure_token()
    },
    .package = "AzureAuth"
  )
  httr2::local_mocked_responses(function(req) {
    graphql_test_response(list(data = list(typename = "Query")), url = req$url)
  })

  fabric_graphql_query(
    "https://api.fabric.microsoft.com/graphql",
    query = "{ typename: __typename }",
    tenant_id = "tenant",
    client_id = "client",
    auth_args = list(password = "secret", auth_type = "client_credentials")
  )
  fabric_graphql_query(
    "https://api.fabric.microsoft.com/graphql",
    query = "{ typename: __typename }",
    tenant_id = "tenant",
    client_id = "client",
    auth_args = list(auth_type = "device_code", use_cache = FALSE)
  )

  expect_identical(calls[[1L]]$resource, .fabric_audience$fabric)
  expect_equal(
    calls[[2L]]$resource,
    c(.fabric_audience$graphql, "offline_access")
  )
})

test_that("GraphQL permits an explicit custom-provider audience", {
  audience <- NULL
  httr2::local_mocked_responses(function(req) {
    graphql_test_response(list(data = list(typename = "Query")), url = req$url)
  })

  fabric_graphql_query(
    "https://api.fabric.microsoft.com/graphql",
    query = "{ typename: __typename }",
    token = function(value) {
      audience <<- value
      "token"
    },
    audience = .fabric_audience$fabric
  )

  expect_identical(audience, .fabric_audience$fabric)
})

test_that("GraphQL partial data and errors are independently preserved", {
  payload <- list(
    data = list(
      products = list(items = list(list(id = 1L))),
      restricted = NULL
    ),
    errors = list(list(
      message = "Access denied",
      path = list("restricted"),
      extensions = list(code = "AUTH_NOT_AUTHORIZED")
    ))
  )

  result <- graphql_parse_response(payload)
  expect_equal(result$data$products$items[[1L]]$id, 1L)
  expect_null(result$data$restricted)
  expect_length(result$errors, 1L)
  expect_equal(result$errors[[1L]]$message, "Access denied")

  expect_warning(
    warned <- graphql_parse_response(payload, error_policy = "warn"),
    "Access denied.*restricted.*AUTH_NOT_AUTHORIZED"
  )
  expect_equal(warned$data, result$data)

  error <- expect_error(
    graphql_parse_response(payload, error_policy = "error"),
    class = "fabric_graphql_error"
  )
  expect_s3_class(error$result, "fabric_graphql_result")
  expect_equal(error$result$data, result$data)
  expect_equal(error$errors, result$errors)
})

test_that("GraphQL errors without data and malformed responses are handled", {
  errors_only <- graphql_parse_response(list(
    errors = list(list(message = "Validation failed"))
  ))
  expect_null(errors_only$data)
  expect_equal(errors_only$errors[[1L]]$message, "Validation failed")

  expect_error(
    graphql_parse_response(list(extensions = list())),
    "neither data nor errors",
    fixed = TRUE
  )
  expect_error(
    graphql_parse_response(list(data = list(), errors = "bad")),
    "errors must be a list",
    fixed = TRUE
  )
})

test_that("GraphQL responses preserve integers beyond double precision", {
  local_mocked_bindings(
    .httr2_perform = function(req, ...) {
      graphql_test_response(
        charToRaw('{"data":{"identifier":9007199254740993}}'),
        url = req$url
      )
    }
  )

  result <- fabric_graphql_query(
    "https://api.fabric.microsoft.com/graphql",
    query = "{ identifier }",
    token = "token"
  )

  expect_identical(result$data$identifier, "9007199254740993")
})

test_that("empty GraphQL variables are omitted instead of encoded as an array", {
  captured <- NULL
  httr2::local_mocked_responses(function(req) {
    captured <<- req
    graphql_test_response(list(data = list(typename = "Query")), url = req$url)
  })

  fabric_graphql_query(
    "https://api.fabric.microsoft.com/graphql",
    query = "{ typename: __typename }",
    token = "token"
  )

  expect_false("variables" %in% names(captured$body$data))
})

test_that("GraphQL cursor helper follows arbitrary connection paths", {
  cursor <- fabric_graphql_cursor(c("viewer", "products"))
  page <- structure(
    list(
      data = list(
        viewer = list(
          products = list(
            items = list(list(id = 1L)),
            hasNextPage = TRUE,
            endCursor = "opaque-cursor"
          )
        )
      ),
      errors = list()
    ),
    class = c("fabric_graphql_result", "list")
  )
  expect_equal(cursor(page), "opaque-cursor")

  page$data$viewer$products$hasNextPage <- FALSE
  expect_null(cursor(page))

  page$data$viewer$products$hasNextPage <- NULL
  expect_error(cursor(page), "hasNextPage.*TRUE or FALSE")
  expect_error(
    fabric_graphql_cursor("missing")(page),
    "path 'missing' was not found",
    fixed = TRUE
  )
})

test_that("fabric_graphql_paginate passes opaque cursors and combines errors", {
  requests <- list()
  responses <- list(
    list(
      data = list(
        products = list(
          items = list(list(id = 1L), list(id = 2L)),
          hasNextPage = TRUE,
          endCursor = "page-one"
        )
      )
    ),
    list(
      data = list(
        products = list(
          items = list(list(id = 3L)),
          hasNextPage = FALSE,
          endCursor = NULL
        )
      ),
      errors = list(list(message = "partial warning", path = list("products")))
    )
  )
  httr2::local_mocked_responses(function(req) {
    requests[[length(requests) + 1L]] <<- req
    graphql_test_response(
      responses[[length(requests)]],
      url = req$url
    )
  })

  pages <- fabric_graphql_paginate(
    "https://api.fabric.microsoft.com/graphql",
    query = paste(
      "query Page($first: Int!, $after: String) {",
      "products(first: $first, after: $after) {",
      "items { id } hasNextPage endCursor",
      "}",
      "}"
    ),
    variables = list(first = 2L, after = NULL),
    operation_name = "Page",
    next_cursor = fabric_graphql_cursor("products"),
    token = "token"
  )

  expect_s3_class(pages, "fabric_graphql_pages")
  expect_true(pages$complete)
  expect_length(pages$pages, 2L)
  expect_equal(
    vapply(
      pages$pages,
      function(page) page$data$products$items[[1L]]$id,
      integer(1)
    ),
    c(1L, 3L)
  )
  expect_length(pages$errors, 1L)
  expect_null(requests[[1L]]$body$data$variables$after)
  expect_equal(requests[[2L]]$body$data$variables$after, "page-one")
  expect_equal(pages$variables$after, "page-one")
})

test_that("GraphQL pagination reuses one AzureAuth credential", {
  token_requests <- 0L
  page <- 0L
  local_mocked_bindings(
    get_azure_token = function(...) {
      token_requests <<- token_requests + 1L
      graphql_fake_azure_token()
    },
    .package = "AzureAuth"
  )
  httr2::local_mocked_responses(function(req) {
    page <<- page + 1L
    graphql_test_response(
      list(
        data = list(
          products = list(
            hasNextPage = page == 1L,
            endCursor = if (page == 1L) "page-one" else NULL
          )
        )
      ),
      url = req$url
    )
  })

  pages <- fabric_graphql_paginate(
    "https://api.fabric.microsoft.com/graphql",
    query = "{ products { hasNextPage endCursor } }",
    next_cursor = fabric_graphql_cursor("products"),
    tenant_id = "tenant",
    client_id = "client",
    auth_args = list(auth_type = "device_code", use_cache = FALSE)
  )

  expect_length(pages$pages, 2L)
  expect_identical(token_requests, 1L)
})

test_that("GraphQL pagination prevents loops and enforces max_pages", {
  httr2::local_mocked_responses(function(req) {
    graphql_test_response(
      list(
        data = list(
          products = list(
            hasNextPage = TRUE,
            endCursor = "same"
          )
        )
      ),
      url = req$url
    )
  })
  expect_error(
    fabric_graphql_paginate(
      "https://api.fabric.microsoft.com/graphql",
      query = "{ products { hasNextPage endCursor } }",
      next_cursor = fabric_graphql_cursor("products"),
      max_pages = 3L,
      token = "token"
    ),
    "already used",
    fixed = TRUE
  )

  counter <- 0L
  httr2::local_mocked_responses(function(req) {
    counter <<- counter + 1L
    graphql_test_response(
      list(
        data = list(
          products = list(
            hasNextPage = TRUE,
            endCursor = paste0("cursor-", counter)
          )
        )
      ),
      url = req$url
    )
  })
  error <- expect_error(
    fabric_graphql_paginate(
      "https://api.fabric.microsoft.com/graphql",
      query = "{ products { hasNextPage endCursor } }",
      next_cursor = fabric_graphql_cursor("products"),
      max_pages = 2L,
      token = "token"
    ),
    class = "fabric_graphql_pagination_error"
  )
  expect_length(error$pages$pages, 2L)
  expect_false(error$pages$complete)

  expect_error(
    expect_no_warning(fabric_graphql_paginate(
      "https://api.fabric.microsoft.com/graphql",
      query = "{ products { id } }",
      next_cursor = function(result) NULL,
      max_pages = .Machine$integer.max + 1,
      token = "token"
    )),
    "max_pages must be one positive integer",
    fixed = TRUE
  )
})

test_that("GraphQL pagination forwards trusted custom endpoint opt-in", {
  httr2::local_mocked_responses(function(req) {
    graphql_test_response(
      list(data = list(products = list(hasNextPage = FALSE))),
      url = req$url
    )
  })

  pages <- fabric_graphql_paginate(
    "https://trusted.example/graphql",
    query = "{ products { hasNextPage } }",
    next_cursor = fabric_graphql_cursor("products"),
    token = "token",
    allow_custom_endpoint = TRUE
  )

  expect_true(pages$complete)
  expect_length(pages$pages, 1L)
})

test_that("fabric_graphql_query surfaces authentication and validates inputs", {
  httr2::local_mocked_responses(function(req) {
    graphql_test_response(
      list(
        errorCode = "Unauthorized",
        message = "Token is invalid"
      ),
      status = 401L,
      url = req$url
    )
  })
  expect_error(
    fabric_graphql_query(
      "https://api.fabric.microsoft.com/graphql",
      query = "{ __typename }",
      token = "invalid-token"
    ),
    "HTTP 401.*Token is invalid"
  )

  expect_error(
    fabric_graphql_query(
      "https://api.fabric.microsoft.com/graphql",
      query = "",
      token = "token"
    ),
    "query must be one non-empty",
    fixed = TRUE
  )
  expect_error(
    fabric_graphql_query(
      "https://api.fabric.microsoft.com/graphql",
      query = "{ __typename }",
      variables = list(1L),
      token = "token"
    ),
    "variables must have unique",
    fixed = TRUE
  )
  expect_error(
    fabric_graphql_query(
      "https://api.fabric.microsoft.com/graphql",
      query = "{ __typename }",
      timeout = 0,
      token = "token"
    ),
    "timeout",
    fixed = TRUE
  )
})
