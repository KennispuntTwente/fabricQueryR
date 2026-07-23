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

test_that("credential callbacks receive audiences and refresh after 401", {
  provider_calls <- list()
  credential <- fabric_credential(
    token_provider = function(audience, force_refresh = FALSE) {
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
    .sleep = function(...) stop("unexpected sleep")
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
  credential <- fabric_credential(access_token = "token")
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
  credential <- fabric_credential(access_token = "token")
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

test_that("credential validation rejects conflicting or invalid providers", {
  expect_error(
    fabric_credential(
      access_token = "token",
      token_provider = function() "other"
    ),
    "only one",
    fixed = TRUE
  )
  credential <- fabric_credential(token_provider = function() {
    list(token = "ok")
  })
  expect_equal(
    fabric_get_token(credential, .fabric_audience$sql),
    "ok"
  )
  expect_error(
    fabric_get_token(
      fabric_credential(token_provider = function() ""),
      .fabric_audience$sql
    ),
    "must return one non-empty bearer token",
    fixed = TRUE
  )
})
