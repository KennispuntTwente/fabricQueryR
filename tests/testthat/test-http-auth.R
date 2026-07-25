json_response <- function(
  status = 200L,
  body = list(ok = TRUE),
  headers = list(),
  url = "https://example.com"
) {
  headers[["content-type"]] <- "application/json"
  httr2::response(
    status_code = status,
    url = url,
    headers = headers,
    body = charToRaw(jsonlite::toJSON(body, auto_unbox = TRUE))
  )
}

fake_azure_token <- function(access_token = "azure-token") {
  class <- R6::R6Class(
    "FabricQueryRFakeAzureToken",
    inherit = AzureAuth::AzureToken,
    public = list(
      refreshes = 0L,
      initialize = function() {
        self$credentials <- list(access_token = access_token)
      },
      validate = function() TRUE,
      refresh = function() {
        self$refreshes <- self$refreshes + 1L
        self$credentials$access_token <- paste0(
          access_token,
          "-refreshed"
        )
        invisible(self)
      }
    )
  )
  class$new()
}

test_that("AzureAuth token objects are accepted and refreshed", {
  token <- fake_azure_token()
  expect_true(AzureAuth::is_azure_token(token))
  credential <- fabric_credential(token = token)

  expect_equal(
    fabric_get_token(credential, .fabric_audience$fabric),
    "azure-token"
  )
  expect_equal(
    fabric_get_token(
      credential,
      .fabric_audience$fabric,
      force_refresh = TRUE
    ),
    "azure-token-refreshed"
  )
  expect_equal(token$refreshes, 1L)
})

test_that("public functions accept AzureAuth token objects", {
  token <- fake_azure_token()
  httr2::local_mocked_responses(function(req) {
    json_response(body = list(value = list()), url = req$url)
  })

  result <- fabric_workspaces(
    token = token,
    api_base = "https://fabric.test/v1"
  )

  expect_s3_class(result, "tbl_df")
  expect_equal(nrow(result), 0L)
})

test_that("token NULL delegates acquisition and caching to AzureAuth", {
  calls <- list()
  local_mocked_bindings(
    get_azure_token = function(...) {
      calls[[length(calls) + 1L]] <<- list(...)
      fake_azure_token()
    },
    .package = "AzureAuth"
  )
  credential <- fabric_credential(
    tenant_id = "tenant",
    client_id = "client",
    auth_args = list(auth_type = "device_code", use_cache = TRUE)
  )

  expect_equal(
    fabric_get_token(credential, .fabric_audience$fabric),
    "azure-token"
  )
  expect_equal(
    fabric_get_token(credential, .fabric_audience$fabric),
    "azure-token"
  )
  expect_length(calls, 1L)
  expect_equal(
    calls[[1L]]$resource,
    c(.fabric_audience$fabric, "offline_access")
  )
  expect_equal(calls[[1L]]$tenant, "tenant")
  expect_equal(calls[[1L]]$app, "client")
  expect_equal(calls[[1L]]$version, 2)
  expect_equal(calls[[1L]]$auth_type, "device_code")
  expect_true(calls[[1L]]$use_cache)
})

test_that("client credentials omit offline_access", {
  calls <- list()
  local_mocked_bindings(
    get_azure_token = function(...) {
      calls[[1L]] <<- list(...)
      fake_azure_token()
    },
    .package = "AzureAuth"
  )
  credential <- fabric_credential(
    tenant_id = "tenant",
    client_id = "client",
    auth_args = list(password = "secret")
  )
  fabric_get_token(credential, .fabric_audience$storage)

  expect_identical(calls[[1L]]$resource, .fabric_audience$storage)
  expect_equal(calls[[1L]]$password, "secret")
})

test_that("authentication inputs are validated consistently", {
  expect_error(
    fabric_credential(token = list(value = "not-a-token")),
    "token must be"
  )
  expect_error(
    fabric_credential(
      token = "one",
      auth_args = list(use_cache = FALSE)
    ),
    "auth_args can only be used"
  )
  expect_error(
    fabric_credential(
      tenant_id = "tenant",
      client_id = "client",
      auth_args = list(resource = "other")
    ),
    "cannot override resource"
  )
  expect_error(
    fabric_credential(
      tenant_id = "tenant",
      client_id = "client",
      auth_args = list(not_an_azureauth_argument = TRUE)
    ),
    "Unknown AzureAuth argument"
  )
})

test_that("all exported authenticated functions share auth arguments", {
  exports <- getNamespaceExports("fabricQueryR")
  authenticated <- Filter(
    function(name) {
      value <- getExportedValue("fabricQueryR", name)
      if (!is.function(value)) {
        return(FALSE)
      }
      args <- names(formals(value))
      any(c("tenant_id", "client_id") %in% args)
    },
    exports
  )
  expected <- c(
    "tenant_id",
    "client_id",
    "token",
    "auth_args"
  )
  for (name in authenticated) {
    args <- names(formals(getExportedValue("fabricQueryR", name)))
    expect_true(
      all(expected %in% args),
      info = name
    )
    expect_false(
      any(c("access_token", "token_provider") %in% args),
      info = name
    )
  }
})

test_that("credential callbacks receive audiences and refresh after 401", {
  provider_calls <- list()
  credential <- fabric_credential(
    token = function(audience, force_refresh = FALSE) {
      provider_calls[[length(provider_calls) + 1L]] <<- list(
        audience = audience,
        force_refresh = force_refresh
      )
      if (force_refresh) "fresh-token" else "stale-token"
    }
  )
  requests <- 0L
  httr2::local_mocked_responses(function(req) {
    requests <<- requests + 1L
    if (requests == 1L) json_response(401L) else json_response()
  })

  result <- .httr2_json(
    httr2::request("https://example.test/items"),
    credential = credential,
    audience = .fabric_audience$fabric,
    .sleep = function(...) rlang::abort("unexpected sleep")
  )

  expect_true(result$ok)
  expect_equal(requests, 2L)
  expect_equal(
    vapply(provider_calls, `[[`, character(1), "audience"),
    rep(.fabric_audience$fabric, 2)
  )
  expect_equal(
    vapply(provider_calls, `[[`, logical(1), "force_refresh"),
    c(FALSE, TRUE)
  )
})

test_that("HTTP retries honor Retry-After and bounded backoff", {
  calls <- 0L
  delays <- numeric()
  httr2::local_mocked_responses(function(req) {
    calls <<- calls + 1L
    switch(
      as.character(calls),
      "1" = json_response(429L, headers = list("retry-after" = "2")),
      "2" = json_response(503L),
      json_response()
    )
  })

  response <- .httr2_perform(
    httr2::request("https://example.test/items"),
    max_tries = 3L,
    .sleep = function(delay) {
      delays <<- c(delays, delay)
    },
    .runif = function(...) 1
  )

  expect_equal(httr2::resp_status(response), 200L)
  expect_equal(calls, 3L)
  expect_equal(delays, c(2, 1))
})

test_that("shared HTTP requests have bounded overridable timeouts", {
  captured <- list()
  httr2::local_mocked_responses(function(req) {
    captured[[length(captured) + 1L]] <<- req
    json_response(url = req$url)
  })

  .httr2_perform(
    httr2::request("https://example.test/default"),
    request_timeout = 45
  )
  .httr2_perform(
    httr2::request("https://example.test/explicit") |>
      httr2::req_timeout(12),
    request_timeout = 45
  )

  expect_equal(captured[[1L]]$options$timeout_ms, 45000)
  expect_equal(captured[[2L]]$options$timeout_ms, 12000)
  expect_error(
    .httr2_perform(
      httr2::request("https://example.test/invalid"),
      request_timeout = 0
    ),
    "request_timeout",
    fixed = TRUE
  )
})

test_that("POST requests retry only with an explicit idempotency decision", {
  calls <- 0L
  httr2::local_mocked_responses(function(req) {
    calls <<- calls + 1L
    json_response(503L)
  })
  request <- httr2::request("https://example.test/items") |>
    httr2::req_method("POST")

  expect_error(
    .httr2_perform(
      request,
      max_tries = 3L,
      .sleep = function(...) NULL
    ),
    "HTTP 503"
  )
  expect_equal(calls, 1L)

  calls <- 0L
  httr2::local_mocked_responses(function(req) {
    calls <<- calls + 1L
    if (calls == 1L) json_response(503L) else json_response()
  })
  response <- .httr2_perform(
    request,
    idempotent = TRUE,
    max_tries = 2L,
    .sleep = function(...) NULL
  )
  expect_equal(httr2::resp_status(response), 200L)
  expect_equal(calls, 2L)
})

test_that("HTTP errors include diagnostics and redact secrets", {
  httr2::local_mocked_responses(function(req) {
    json_response(
      400L,
      body = list(
        access_token = "secret-access-token",
        nested = list(authorization = "Bearer secret-bearer"),
        message = "safe detail"
      ),
      headers = list(
        "x-ms-request-id" = "request-123",
        "x-ms-activity-id" = "activity-456"
      ),
      url = req$url
    )
  })

  error <- expect_error(
    .httr2_perform(
      httr2::request(
        "https://example.test/private?access_token=url-secret"
      )
    ),
    "Endpoint: https://example.test/private?access_token=<redacted>",
    fixed = TRUE
  )
  expect_match(conditionMessage(error), "Request ID: request-123")
  expect_match(conditionMessage(error), "Activity ID: activity-456")
  expect_match(conditionMessage(error), "<redacted>")
  expect_false(grepl(
    "secret-access-token",
    conditionMessage(error),
    fixed = TRUE
  ))
  expect_false(grepl("secret-bearer", conditionMessage(error), fixed = TRUE))
  expect_false(grepl("url-secret", conditionMessage(error), fixed = TRUE))
})

test_that("shared pagination follows continuation URIs and tokens", {
  credential <- fabric_credential(token = "token")
  urls <- character()
  pages <- list(
    list(
      value = list(list(id = "one")),
      continuationUri = "https://example.test/items?page=2"
    ),
    list(
      value = list(list(id = "two")),
      continuationToken = "next-token"
    ),
    list(value = list(list(id = "three")))
  )
  local_mocked_bindings(
    .httr2_json = function(req, ...) {
      urls <<- c(urls, req$url)
      pages[[length(urls)]]
    }
  )

  values <- .httr2_collection(
    "https://example.test/items",
    credential,
    .fabric_audience$fabric
  )
  expect_equal(
    vapply(values, `[[`, character(1), "id"),
    c("one", "two", "three")
  )
  expect_equal(urls[[2]], "https://example.test/items?page=2")
  expect_match(urls[[3]], "continuationToken=next-token")
})

test_that("long-running operation polling handles terminal states", {
  credential <- fabric_credential(token = "token")
  responses <- list(
    list(status = "Running"),
    list(status = "Succeeded", result = list(id = "item"))
  )
  calls <- 0L
  local_mocked_bindings(
    .httr2_json = function(...) {
      calls <<- calls + 1L
      responses[[calls]]
    }
  )
  result <- .httr2_poll_lro(
    "https://example.test/operations/1",
    credential,
    poll_interval = 0,
    .sleep = function(...) NULL
  )
  expect_equal(result$result$id, "item")

  local_mocked_bindings(
    .httr2_json = function(...) {
      list(status = "Failed", error = list(message = "capacity unavailable"))
    }
  )
  expect_error(
    .httr2_poll_lro(
      "https://example.test/operations/2",
      credential,
      .sleep = function(...) NULL
    ),
    "capacity unavailable",
    fixed = TRUE
  )
  expect_error(
    .httr2_poll_lro(
      "https://example.test/operations/3",
      credential,
      cancel = function() TRUE
    ),
    "cancelled",
    fixed = TRUE
  )

  ticks <- as.POSIXct("2026-01-01", tz = "UTC") + c(0, 0, 2)
  tick <- 0L
  local_mocked_bindings(
    .httr2_json = function(...) list(status = "Running")
  )
  expect_error(
    .httr2_poll_lro(
      "https://example.test/operations/4",
      credential,
      timeout = 1,
      poll_interval = 0,
      .sleep = function(...) NULL,
      .now = function() {
        tick <<- tick + 1L
        ticks[[tick]]
      }
    ),
    "Timed out",
    fixed = TRUE
  )
})

test_that("token accepts strings and validates provider results", {
  expect_error(
    fabric_credential(token = ""),
    "token must be one non-empty string"
  )
  credential <- fabric_credential(token = function() {
    list(token = "ok")
  })
  expect_equal(
    fabric_get_token(credential, .fabric_audience$sql),
    "ok"
  )
  expect_error(
    fabric_get_token(
      fabric_credential(token = function() ""),
      .fabric_audience$sql
    ),
    "must return one non-empty bearer token",
    fixed = TRUE
  )
})
