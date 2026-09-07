graphql_request_body <- function(request) {
  jsonlite::fromJSON(
    rawToChar(request$body$data),
    simplifyVector = FALSE
  )
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
        workspaceId = workspace_id,
        graphql_endpoint = "https://custom.test/graphql/"
      ),
      workspace_id = toupper(workspace_id)
    ),
    "https://custom.test/graphql"
  )
  expect_error(
    graphql_resolve_endpoint(
      list(
        id = api_id,
        type = "GraphQLApi",
        workspaceId = workspace_id,
        graphql_endpoint = "https://custom.test/graphql/"
      ),
      workspace_id = "00000000-0000-0000-0000-000000000000"
    ),
    "conflicts with the api discovery record workspaceId",
    fixed = TRUE
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
  expect_equal(
    graphql_resolve_endpoint("https://trusted.example/graphql"),
    "https://trusted.example/graphql"
  )
  expect_equal(
    graphql_resolve_endpoint(
      "https://api.fabric.microsoft.com:443/v1/graphql"
    ),
    "https://api.fabric.microsoft.com:443/v1/graphql"
  )
  expect_error(
    graphql_resolve_endpoint(
      "https://api.fabric.microsoft.com:444/v1/graphql"
    ),
    "valid HTTPS",
    fixed = TRUE
  )
  expect_error(
    graphql_resolve_endpoint(
      "https://api.fabric.microsoft.com/graphql?redirect=attacker"
    ),
    "valid HTTPS",
    fixed = TRUE
  )
})

test_that("GraphQL HTTP defaults include server-timeout overhead", {
  expect_identical(formals(fabric_graphql_query)$timeout, 110)
  expect_identical(formals(fabric_graphql_schema)$timeout, 110)
  expect_identical(formals(fabric_graphql_paginate)$timeout, 110)
})

test_that("discovered GraphQL methods preserve the credential authentication mode", {
  for (application in c(TRUE, FALSE)) {
    audiences <- character()
    credential <- fabric_credential(token = function(audience) {
      audiences <<- c(audiences, audience)
      "synthetic-token"
    })
    credential$client_credentials <- application
    api <- r6_test_record("GraphQLApi", credential)
    httr2::local_mocked_responses(function(req) {
      graphql_test_response(
        list(
          data = list(
            `__schema` = list(queryType = list(name = "Query")),
            items = list()
          )
        ),
        url = req$url
      )
    })
    api$query("{ items { id } }")
    api$schema()
    api$paginate("{ items { id } }", next_cursor = function(...) NULL)
    expect_identical(
      audiences,
      rep(
        if (application) .fabric_audience$fabric else .fabric_audience$graphql,
        3
      )
    )
  }
})

test_that("fabric_graphql_schema runs standard introspection", {
  captured <- NULL
  httr2::local_mocked_responses(function(req) {
    captured <<- req
    graphql_test_response(
      list(
        data = list(
          `__schema` = list(
            queryType = list(name = "Query"),
            mutationType = list(name = "Mutation"),
            subscriptionType = NULL,
            types = list(
              list(
                kind = "OBJECT",
                name = "Product",
                fields = list(list(name = "id"))
              )
            ),
            directives = list()
          )
        )
      ),
      url = req$url
    )
  })

  schema <- fabric_graphql_schema(
    "https://api.fabric.microsoft.com/graphql",
    token = "token"
  )

  expect_s3_class(schema, "fabric_graphql_schema")
  expect_equal(schema$queryType$name, "Query")
  expect_equal(schema$types[[1L]]$name, "Product")
  expect_length(attr(schema, "errors"), 0L)
  body <- graphql_request_body(captured)
  expect_equal(body$operationName, "IntrospectionQuery")
  expect_match(body$query, "__schema", fixed = TRUE)
  expect_match(
    body$query,
    "fields(includeDeprecated: true)",
    fixed = TRUE
  )
  expect_match(
    body$query,
    "fragment TypeRef on __Type",
    fixed = TRUE
  )
})

test_that("fabric_graphql_schema explains disabled introspection", {
  httr2::local_mocked_responses(function(req) {
    graphql_test_response(
      list(
        errors = list(list(
          message = "GraphQL introspection is not allowed",
          extensions = list(code = "INTROSPECTION_DISABLED")
        ))
      ),
      url = req$url
    )
  })

  error <- expect_error(
    fabric_graphql_schema(
      "https://api.fabric.microsoft.com/graphql",
      token = "secret-token"
    ),
    class = "fabric_graphql_introspection_error"
  )
  expect_match(error$message, "disables introspection by default", fixed = TRUE)
  expect_match(error$message, "workspace admin", fixed = TRUE)
  expect_match(error$message, "API Settings > Introspection", fixed = TRUE)
  expect_match(error$message, "Export schema", fixed = TRUE)
  expect_s3_class(error$result, "fabric_graphql_result")
  expect_equal(error$errors[[1L]]$extensions$code, "INTROSPECTION_DISABLED")
  expect_false(grepl("secret-token", error$message, fixed = TRUE))
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
  body <- graphql_request_body(captured)
  expect_equal(body$query, document)
  expect_equal(body$variables$category, "B")
  expect_null(body$variables$nullable)
  expect_equal(body$operationName, "Products")
})

test_that("GraphQL request JSON preserves recursive negative zero", {
  captured <- NULL
  httr2::local_mocked_responses(function(req) {
    captured <<- rawToChar(req$body$data)
    graphql_test_response(list(data = list(echo = 0)), url = req$url)
  })

  fabric_graphql_query(
    "https://api.fabric.microsoft.com/graphql",
    query = "query Echo($input: EchoInput!) { echo(input: $input) }",
    variables = list(input = list(value = -0, values = c(0, -0), text = "-0")),
    token = "token"
  )

  expect_match(captured, '"value":-0.0', fixed = TRUE)
  expect_match(captured, '"values":[0,-0.0]', fixed = TRUE)
  expect_match(captured, '"text":"-0"', fixed = TRUE)
})

test_that("GraphQL variables map missing values to null and reject non-finite numbers", {
  requests <- 0L
  captured <- NULL
  httr2::local_mocked_responses(function(req) {
    requests <<- requests + 1L
    captured <<- rawToChar(req$body$data)
    graphql_test_response(list(data = list(echo = NULL)), url = req$url)
  })
  query <- "query Echo($input: EchoInput) { echo(input: $input) }"

  fabric_graphql_query(
    "https://api.fabric.microsoft.com/graphql",
    query = query,
    variables = list(input = list(missing = NA_real_, values = c(1, NA_real_))),
    token = "token"
  )
  expect_match(captured, '"missing":null', fixed = TRUE)
  expect_match(captured, '"values":[1,null]', fixed = TRUE)
  expect_identical(requests, 1L)

  for (value in list(NaN, Inf, -Inf, list(nested = c(1, Inf)))) {
    expect_error(
      fabric_graphql_query(
        "https://api.fabric.microsoft.com/graphql",
        query = query,
        variables = list(input = value),
        token = "token"
      ),
      class = "fabric_graphql_variables_error"
    )
  }
  expect_identical(requests, 1L)

  expect_error(
    fabric_graphql_paginate(
      "https://api.fabric.microsoft.com/graphql",
      query = query,
      variables = list(input = list(value = Inf)),
      next_cursor = function(result) NULL,
      token = "token"
    ),
    class = "fabric_graphql_variables_error"
  )
  expect_identical(requests, 1L)
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

test_that("GraphQL responses distinguish adjacent uint64 values", {
  body <- charToRaw(paste0(
    '{"data":{"items":[',
    '{"value":18446744073709551615},',
    '{"value":18446744073709551614},',
    '{"value":null}]}}'
  ))
  httr2::local_mocked_responses(function(req) {
    graphql_test_response(body, url = req$url)
  })

  exact <- fabric_graphql_query(
    "https://api.fabric.microsoft.com/graphql",
    query = "{ items { value } }",
    token = "token"
  )
  double <- fabric_graphql_query(
    "https://api.fabric.microsoft.com/graphql",
    query = "{ items { value } }",
    token = "token",
    numeric_policy = "double"
  )

  expected <- c("18446744073709551615", "18446744073709551614")
  expect_identical(
    vapply(exact$data$items[1:2], `[[`, character(1), "value"),
    expected
  )
  expect_identical(
    vapply(double$data$items[1:2], `[[`, character(1), "value"),
    expected
  )
  expect_null(exact$data$items[[3L]]$value)
})

test_that("GraphQL pagination and collection retain adjacent uint64 values", {
  bodies <- lapply(
    c(
      paste0(
        '{"data":{"items":{"rows":[',
        '{"value":18446744073709551615}],',
        '"hasNextPage":true,"endCursor":"next"}}}'
      ),
      paste0(
        '{"data":{"items":{"rows":[',
        '{"value":18446744073709551614},{"value":null}],',
        '"hasNextPage":false,"endCursor":null}}}'
      )
    ),
    charToRaw
  )
  page <- 0L
  httr2::local_mocked_responses(function(req) {
    page <<- page + 1L
    graphql_test_response(bodies[[page]], url = req$url)
  })

  pages <- fabric_graphql_paginate(
    "https://api.fabric.microsoft.com/graphql",
    query = "query Values($after: String) { items { rows { value } } }",
    next_cursor = fabric_graphql_cursor("items"),
    token = "token"
  )
  rows <- fabric_graphql_collect(pages, c("items", "rows"))

  expect_identical(
    rows$value,
    c("18446744073709551615", "18446744073709551614", NA_character_)
  )
  expect_identical(attr(rows, "page_count"), 2L)
})

test_that("GraphQL exact numeric policy preserves decimal source tokens", {
  body <- charToRaw(paste0(
    '{"data":{"ordinary":1.25,"trailing":10.50,',
    '"large":12345678901234567890.125,',
    '"scientific":9.007199254740993e15,',
    '"overflow":1e400,"underflow":1e-400,',
    '"negativeZero":-0.0,"integer":2}}'
  ))
  httr2::local_mocked_responses(function(req) {
    graphql_test_response(body, url = req$url)
  })

  exact <- fabric_graphql_query(
    "https://api.fabric.microsoft.com/graphql",
    query = "{ values }",
    token = "token"
  )
  double <- fabric_graphql_query(
    "https://api.fabric.microsoft.com/graphql",
    query = "{ values }",
    token = "token",
    numeric_policy = "double"
  )

  expect_identical(
    exact$data,
    list(
      ordinary = "1.25",
      trailing = "10.50",
      large = "12345678901234567890.125",
      scientific = "9.007199254740993e15",
      overflow = "1e400",
      underflow = "1e-400",
      negativeZero = "-0.0",
      integer = 2L
    )
  )
  expect_identical(exact$response$data, exact$data)
  expect_identical(double$data$trailing, 10.5)
  expect_identical(double$data$overflow, Inf)
  expect_identical(double$data$underflow, 0)
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

  expect_false("variables" %in% names(graphql_request_body(captured)))
})

test_that("GraphQL singleton list variables can preserve their array shape", {
  captured <- NULL
  httr2::local_mocked_responses(function(req) {
    captured <<- req
    graphql_test_response(list(data = list(products = list())), url = req$url)
  })

  fabric_graphql_query(
    "https://api.fabric.microsoft.com/graphql",
    query = paste0(
      "query Products($ids: [ID!]!) { ",
      "products(filter: {id: {in: $ids}}) { id } }"
    ),
    variables = list(ids = I("x")),
    token = "token"
  )

  encoded <- rawToChar(captured$body$data)
  expect_match(encoded, '"ids":["x"]', fixed = TRUE)
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
  expect_null(graphql_request_body(requests[[1L]])$variables$after)
  expect_equal(
    graphql_request_body(requests[[2L]])$variables$after,
    "page-one"
  )
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
  request_count <- 0L
  httr2::local_mocked_responses(function(req) {
    request_count <<- request_count + 1L
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
  expect_identical(request_count, 2L)

  request_count <- 0L
  initial_cursor_error <- rlang::catch_cnd(fabric_graphql_paginate(
    "https://api.fabric.microsoft.com/graphql",
    query = "{ products { hasNextPage endCursor } }",
    variables = list(after = "same"),
    next_cursor = fabric_graphql_cursor("products"),
    max_pages = 3L,
    token = "token"
  ))
  expect_match(
    conditionMessage(initial_cursor_error),
    "already used",
    fixed = TRUE
  )
  expect_identical(request_count, 1L)

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

test_that("GraphQL pagination accepts an explicitly supplied custom endpoint", {
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
    token = "token"
  )

  expect_true(pages$complete)
  expect_length(pages$pages, 1L)
})

test_that("custom GraphQL hosts require an explicit credential", {
  error <- rlang::catch_cnd(fabric_graphql_query(
    "https://gateway.example/graphql",
    query = "{ products { id } }"
  ))

  expect_s3_class(error, "fabric_custom_endpoint_requires_token")
  expect_identical(error$endpoint_host, "gateway.example")
  expect_identical(error$argument, "api")
})

test_that("GraphQL collection preserves large doubles promoted to character", {
  body <- charToRaw(paste0(
    '{"data":{"items":[',
    '{"value":4.425243029857038e213},',
    '{"value":-1.280544414187844e237},',
    '{"value":9007199254740993},',
    '{"value":null}]}}'
  ))
  httr2::local_mocked_responses(function(req) {
    graphql_test_response(body, url = req$url)
  })
  pages <- fabric_graphql_paginate(
    "https://api.fabric.microsoft.com/graphql",
    "{ items { value } }",
    next_cursor = function(result) NULL,
    token = "token"
  )

  rows <- fabric_graphql_collect(pages, "items")

  expect_identical(
    numeric_test_decode(rows$value[1:2]),
    as.numeric(c(
      "0x1.a4a3bd7d8804dp+709",
      "-0x1.92be36a62eeeep+787"
    ))
  )
  expect_identical(rows$value[3:4], c("9007199254740993", NA_character_))
})

test_that("GraphQL collection preserves fractions and large integers across pages", {
  bodies <- lapply(
    c(
      paste0(
        '{"data":{"items":{"rows":[',
        '{"value":1.25},{"value":3.1415926535897931},',
        '{"value":null},{}],"hasNextPage":true,"endCursor":"next"}}}'
      ),
      paste0(
        '{"data":{"items":{"rows":[',
        '{"value":9007199254740993},{"value":4.9406564584124654e-324},',
        '{"value":-0.0},{"value":1.7976931348623157e308},',
        '{"value":-9223372036854775807},{"value":2},',
        '{"value":-9223372036854775808}],',
        '"hasNextPage":false,"endCursor":null}}}'
      )
    ),
    charToRaw
  )
  page <- 0L
  httr2::local_mocked_responses(function(req) {
    page <<- page + 1L
    graphql_test_response(bodies[[page]], url = req$url)
  })
  pages <- fabric_graphql_paginate(
    "https://api.fabric.microsoft.com/graphql",
    "query Values($after: String) { items(after: $after) { rows { value } hasNextPage endCursor } }",
    next_cursor = fabric_graphql_cursor("items"),
    token = "token"
  )

  result <- fabric_graphql_collect(pages, c("items", "rows"))

  expect_type(result$value, "character")
  expect_identical(
    writeBin(
      numeric_test_decode(result$value[c(1L, 2L, 6L, 7L, 8L, 10L, 11L)]),
      raw(),
      size = 8L
    ),
    writeBin(
      c(
        1.25,
        pi,
        .Machine$double.xmin * .Machine$double.eps,
        as.numeric("-0.0"),
        .Machine$double.xmax,
        2,
        -2^63
      ),
      raw(),
      size = 8L
    )
  )
  expect_identical(result$value[5L], "9007199254740993")
  expect_identical(result$value[9L], "-9223372036854775807")
  expect_identical(result$value[3:4], rep(NA_character_, 2L))
  expect_identical(attr(result, "complete"), TRUE)
  expect_identical(attr(result, "page_count"), 2L)
})

test_that("GraphQL fractional text promotion is independent of row order", {
  values <- list("9007199254740993", pi, 1.25, NULL, "-9223372036854775808")

  forward <- graphql_rows_column(values, "value")
  backward <- graphql_rows_column(rev(values), "value")

  expect_identical(forward, rev(backward))
  expect_identical(as.numeric(forward[2:3]), c(pi, 1.25))
  expect_identical(
    forward[c(1L, 4L, 5L)],
    c(
      "9007199254740993",
      NA_character_,
      "-9223372036854775808"
    )
  )
})

test_that("GraphQL numeric columns retain numeric types without integer strings", {
  result <- graphql_rows_column(list(pi, 1.25, 2L, NULL), "value")

  expect_identical(result, c(pi, 1.25, 2, NA_real_))
})

test_that("fabric_graphql_collect binds evolving nested rows exactly", {
  first <- graphql_parse_response(list(
    data = list(
      viewer = list(
        products = list(
          items = list(
            list(
              id = 1L,
              name = "alpha",
              profile = list(source = "warehouse", rank = 1L),
              tags = list("a", "b")
            ),
            list(
              id = "9007199254740993",
              name = NULL,
              profile = NULL,
              tags = list()
            )
          )
        )
      )
    )
  ))
  empty <- graphql_parse_response(list(
    data = list(viewer = list(products = list(items = list())))
  ))
  evolved <- graphql_parse_response(list(
    data = list(
      viewer = list(
        products = list(
          items = list(list(
            id = 3L,
            name = "gamma",
            category = "new",
            profile = list(source = "lakehouse"),
            tags = list("c")
          ))
        )
      )
    ),
    errors = list(list(
      message = "A restricted sibling field was omitted",
      path = list("viewer", "restricted")
    ))
  ))
  pages <- graphql_pages_result(
    list(first, empty, evolved),
    variables = list(after = "last"),
    complete = TRUE
  )

  rows <- fabric_graphql_collect(
    pages,
    c("viewer", "products", "items")
  )

  expect_s3_class(rows, "fabric_graphql_rows")
  expect_s3_class(rows, "tbl_df")
  expect_named(rows, c("id", "name", "profile", "tags", "category"))
  expect_identical(rows$id, c("1", "9007199254740993", "3"))
  expect_identical(rows$name, c("alpha", NA_character_, "gamma"))
  expect_true(is.list(rows$profile))
  expect_equal(rows$profile[[1L]]$source, "warehouse")
  expect_null(rows$profile[[2L]])
  expect_equal(rows$profile[[3L]]$source, "lakehouse")
  expect_true(is.list(rows$tags))
  expect_equal(rows$tags[[1L]], list("a", "b"))
  expect_identical(rows$category, c(NA_character_, NA_character_, "new"))
  expect_true(attr(rows, "complete"))
  expect_identical(attr(rows, "page_count"), 3L)
  expect_identical(
    attr(rows, "path"),
    c("viewer", "products", "items")
  )
  expect_length(attr(rows, "errors"), 1L)
  expect_match(
    paste(capture.output(print(rows)), collapse = "\n"),
    "pagination complete; 3 pages; 1 GraphQL error",
    fixed = TRUE
  )
})

test_that("fabric_graphql_collect handles empty and partial-error pages", {
  empty <- graphql_parse_response(list(
    data = list(products = list(items = list()))
  ))
  unavailable <- graphql_parse_response(list(
    data = list(products = NULL),
    errors = list(list(
      message = "Products could not be resolved",
      path = list("products")
    ))
  ))
  pages <- graphql_pages_result(
    list(empty, unavailable),
    variables = list(),
    complete = TRUE
  )

  rows <- fabric_graphql_collect(pages, c("products", "items"))

  expect_s3_class(rows, "fabric_graphql_rows")
  expect_equal(dim(rows), c(0L, 0L))
  expect_true(attr(rows, "complete"))
  expect_length(attr(rows, "errors"), 1L)
})

test_that("fabric_graphql_collect refuses incomplete pagination", {
  page <- graphql_parse_response(list(
    data = list(products = list(items = list(list(id = 1L))))
  ))
  pages <- graphql_pages_result(
    list(page),
    variables = list(after = "next"),
    complete = FALSE
  )

  error <- expect_error(
    fabric_graphql_collect(pages, c("products", "items")),
    class = "fabric_graphql_collection_error"
  )

  expect_match(error$message, "max_pages", fixed = TRUE)
  expect_match(error$message, "100,000-item", fixed = TRUE)
  expect_s3_class(error$partial_data, "fabric_graphql_rows")
  expect_false(attr(error$partial_data, "complete"))
  expect_identical(error$partial_data$id, 1L)
})

test_that("fabric_graphql_collect validates row paths and scalar evolution", {
  result <- graphql_parse_response(list(
    data = list(products = list(items = list(list(id = 1L))))
  ))
  expect_error(
    fabric_graphql_collect(result, c("products", "items")),
    "fabric_graphql_pages"
  )

  pages <- graphql_pages_result(list(result), list(), complete = TRUE)
  expect_error(
    fabric_graphql_collect(pages, c("products", "missing")),
    "path 'products.missing' was not found"
  )

  changed <- graphql_pages_result(
    list(
      graphql_parse_response(list(
        data = list(products = list(items = list(list(value = "text"))))
      )),
      graphql_parse_response(list(
        data = list(products = list(items = list(list(value = 2L))))
      ))
    ),
    list(),
    complete = TRUE
  )
  expect_error(
    fabric_graphql_collect(changed, c("products", "items")),
    "incompatible scalar types"
  )
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
