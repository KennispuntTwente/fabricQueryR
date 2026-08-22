test_that("immediate 200 and 201 responses use one result contract", {
  calls <- 0L
  httr2::local_mocked_responses(function(req) {
    calls <<- calls + 1L
    operation_test_response(
      status = c(200L, 201L)[[calls]],
      body = list(id = paste0("item-", calls), type = "Notebook"),
      headers = list(
        `x-ms-request-id` = paste0("request-", calls),
        `x-ms-activity-id` = paste0("activity-", calls)
      ),
      url = req$url
    )
  })

  first <- .fabric_operation_perform(
    operation_test_request(),
    fabric_credential(token = "test-token")
  )
  second <- .fabric_operation_perform(
    operation_test_request(),
    fabric_credential(token = "test-token")
  )

  expect_s3_class(first, "fabric_operation_result")
  expect_s3_class(second, "fabric_operation_result")
  expect_identical(names(first), names(second))
  expect_equal(first$value$id, "item-1")
  expect_equal(second$value$id, "item-2")
  expect_false(first$empty)
  expect_equal(first$request_id, "request-1")
  expect_equal(second$activity_id, "activity-2")
  expect_true(first$operation$immediate)
  expect_equal(calls, 2L)

  first$operation$credential <- NULL
  first$operation$.credential_key <- NULL
  expect_equal(
    fabric_operation_result(first$operation)$value$id,
    "item-1"
  )
})

test_that("202 operations honor polling hints and retrieve a separate result", {
  clock <- operation_test_clock()
  calls <- list()
  response_number <- 0L
  responses <- list(
    operation_test_response(
      status = 202L,
      headers = list(
        Location = paste0("/v1/operations/", operation_test_id),
        `x-ms-operation-id` = operation_test_id,
        `Retry-After` = "3"
      )
    ),
    operation_test_response(
      body = list(
        status = "Running",
        createdTimeUtc = "2026-08-13T12:00:00.000Z",
        lastUpdatedTimeUtc = "2026-08-13T12:00:03.000Z",
        percentComplete = 25L
      ),
      headers = list(
        Location = paste0("/operations/", operation_test_id),
        `x-ms-operation-id` = operation_test_id,
        `Retry-After` = "4",
        `x-ms-request-id` = "status-request",
        `x-ms-activity-id` = "status-activity"
      )
    ),
    operation_test_response(
      body = list(
        status = "Succeeded",
        createdTimeUtc = "2026-08-13T12:00:00.000Z",
        lastUpdatedTimeUtc = "2026-08-13T12:00:07.000Z",
        percentComplete = 100L
      ),
      headers = list(
        Location = paste0("/operations/", operation_test_id, "/result"),
        `x-ms-operation-id` = operation_test_id
      )
    ),
    operation_test_response(
      body = list(id = "created-item", type = "Lakehouse"),
      headers = list(`request-id` = "result-request")
    )
  )
  httr2::local_mocked_responses(function(req) {
    response_number <<- response_number + 1L
    calls[[response_number]] <<- list(
      method = req$method %||% "GET",
      url = req$url
    )
    response <- responses[[response_number]]
    response$url <- req$url
    response
  })

  result <- .fabric_operation_perform(
    operation_test_request(),
    fabric_credential(token = "test-token"),
    timeout = 30,
    .sleep = clock$sleep,
    .now = clock$now
  )

  expect_s3_class(result, "fabric_operation_result")
  expect_equal(result$value$id, "created-item")
  expect_equal(result$operation_id, operation_test_id)
  expect_equal(result$request_id, "result-request")
  expect_false(result$operation$immediate)
  expect_equal(clock$delays(), c(3, 4))
  expect_equal(
    vapply(calls, `[[`, character(1), "method"),
    c("POST", "GET", "GET", "GET")
  )
  expect_equal(sum(vapply(calls, `[[`, character(1), "method") == "POST"), 1L)
  expect_match(calls[[2L]]$url, paste0("/operations/", operation_test_id, "$"))
  expect_match(
    calls[[4L]]$url,
    paste0("/operations/", operation_test_id, "/result$")
  )
  expect_identical(
    names(result),
    c(
      "value",
      "content_type",
      "empty",
      "status_code",
      "request_id",
      "activity_id",
      "operation_id",
      "operation"
    )
  )
})

test_that("202 state responses remain active until a state body is available", {
  clock <- operation_test_clock()
  calls <- 0L
  httr2::local_mocked_responses(function(req) {
    calls <<- calls + 1L
    if (calls == 1L) {
      return(operation_test_response(
        status = 202L,
        headers = list(
          Location = paste0("/v1/operations/", operation_test_id),
          `x-ms-operation-id` = operation_test_id,
          `Retry-After` = "0"
        ),
        url = req$url
      ))
    }
    if (calls == 2L) {
      return(operation_test_response(
        status = 202L,
        headers = list(`Retry-After` = "1"),
        url = req$url
      ))
    }
    if (calls == 3L) {
      return(operation_test_response(
        body = list(status = "Succeeded", percentComplete = 100L),
        headers = list(
          Location = paste0("/v1/operations/", operation_test_id, "/result")
        ),
        url = req$url
      ))
    }
    operation_test_response(body = list(id = "completed"), url = req$url)
  })

  result <- .fabric_operation_perform(
    operation_test_request(),
    fabric_credential(token = "test-token"),
    timeout = 10,
    .sleep = clock$sleep,
    .now = clock$now
  )

  expect_equal(result$value$id, "completed")
  expect_equal(clock$delays(), 1)
  expect_equal(calls, 4L)
})

test_that("status preserves progress, timestamps, identifiers, and future states", {
  location <- paste0(
    "https://api.fabric.microsoft.com/v1/operations/",
    operation_test_id
  )
  requested <- NULL
  httr2::local_mocked_responses(function(req) {
    requested <<- req$url
    operation_test_response(
      body = list(
        status = "PausedForCapacity",
        createdTimeUtc = "2026-08-13T12:00:00.125Z",
        lastUpdatedTimeUtc = "2026-08-13T12:01:02.500Z",
        percentComplete = 42L,
        futureField = list(value = "kept", optional = NULL)
      ),
      headers = list(
        `Retry-After` = "7",
        `x-ms-request-id` = "request-id",
        `x-ms-activity-id` = "activity-id"
      ),
      url = req$url
    )
  })

  state <- fabric_operation_status(
    location,
    token = "test-token",
    respect_retry_after = FALSE
  )

  expect_s3_class(state, "fabric_operation_state")
  expect_equal(state$status, "PausedForCapacity")
  expect_equal(state$percent_complete, 42L)
  expect_s3_class(state$created_time, "POSIXct")
  expect_equal(format(state$created_time, "%OS3", tz = "UTC"), "00.125")
  expect_equal(state$request_id, "request-id")
  expect_equal(state$activity_id, "activity-id")
  expect_equal(state$retry_after, 7)
  expect_equal(state$raw$futureField$value, "kept")
  expect_true("optional" %in% names(state$raw$futureField))
  expect_null(state$raw$futureField$optional)
  expect_equal(requested, location)
})

test_that("wait reports structured failures and unfamiliar states", {
  failed_body <- list(
    status = "Failed",
    percentComplete = 65L,
    error = list(
      errorCode = "ProvisioningFailed",
      message = "Capacity rejected the item",
      requestId = "body-request",
      moreDetails = list(list(errorCode = "Detail", message = "detail"))
    )
  )
  httr2::local_mocked_responses(function(req) {
    operation_test_response(
      body = failed_body,
      headers = list(`x-ms-activity-id` = "failure-activity"),
      url = req$url
    )
  })

  failure <- expect_error(
    fabric_operation_wait(
      operation_test_id,
      token = "test-token",
      timeout = 10
    ),
    class = "fabric_operation_failed"
  )
  expect_equal(failure$operation_error$errorCode, "ProvisioningFailed")
  expect_equal(failure$request_id, "body-request")
  expect_equal(failure$activity_id, "failure-activity")
  expect_equal(failure$operation_status$percent_complete, 65L)

  httr2::local_mocked_responses(function(req) {
    operation_test_response(
      body = list(status = "PausedForCapacity"),
      url = req$url
    )
  })
  unknown <- expect_error(
    fabric_operation_wait(
      operation_test_id,
      token = "test-token",
      timeout = 10
    ),
    class = "fabric_operation_unknown_status"
  )
  expect_equal(unknown$operation_status$status, "PausedForCapacity")
})

test_that("running operations time out without replaying initiation", {
  clock <- operation_test_clock()
  calls <- 0L
  httr2::local_mocked_responses(function(req) {
    calls <<- calls + 1L
    if (calls == 1L) {
      return(operation_test_response(
        status = 202L,
        headers = list(
          Location = paste0("/v1/operations/", operation_test_id),
          `x-ms-operation-id` = operation_test_id
        ),
        url = req$url
      ))
    }
    operation_test_response(
      body = list(status = "Running", percentComplete = 10L),
      headers = list(`Retry-After` = "5"),
      url = req$url
    )
  })

  operation <- .fabric_operation_submit(
    operation_test_request(),
    fabric_credential(token = "test-token"),
    .now = clock$now
  )
  timed_out <- expect_error(
    fabric_operation_wait(
      operation,
      timeout = 3,
      .sleep = clock$sleep,
      .now = clock$now
    ),
    class = "fabric_operation_timeout"
  )

  expect_equal(timed_out$operation_status$status, "Running")
  expect_equal(calls, 2L)
  expect_length(clock$delays(), 0L)
})

test_that("non-idempotent initiation is attempted exactly once", {
  calls <- 0L
  httr2::local_mocked_responses(function(req) {
    calls <<- calls + 1L
    operation_test_response(
      status = 503L,
      body = list(errorCode = "Busy", isRetriable = TRUE),
      headers = list(`Retry-After` = "0"),
      url = req$url
    )
  })

  expect_error(
    .fabric_operation_submit(
      operation_test_request(),
      fabric_credential(token = "test-token"),
      idempotent = FALSE
    ),
    class = "fabric_http_error"
  )
  expect_equal(calls, 1L)
})

test_that("non-idempotent initiation refreshes once after a 401", {
  calls <- 0L
  refreshes <- logical()
  credential <- fabric_credential(token = function(
    audience,
    force_refresh = FALSE
  ) {
    refreshes <<- c(refreshes, force_refresh)
    if (force_refresh) "fresh-token" else "stale-token"
  })
  httr2::local_mocked_responses(function(req) {
    calls <<- calls + 1L
    if (calls == 1L) {
      return(operation_test_response(status = 401L, url = req$url))
    }
    operation_test_response(
      status = 202L,
      headers = list(
        Location = paste0(
          "https://api.fabric.microsoft.com/v1/operations/",
          operation_test_id
        ),
        `Retry-After` = "2"
      ),
      url = req$url
    )
  })

  operation <- .fabric_operation_submit(
    operation_test_request(),
    credential,
    idempotent = FALSE
  )

  expect_s3_class(operation, "fabric_operation")
  expect_equal(calls, 2L)
  expect_identical(refreshes, c(FALSE, TRUE))
})

test_that("operation headers can independently provide Location or ID", {
  calls <- 0L
  httr2::local_mocked_responses(function(req) {
    calls <<- calls + 1L
    headers <- if (calls == 1L) {
      list(Location = paste0("/v1/operations/", operation_test_id))
    } else {
      list(`x-ms-operation-id` = operation_test_id)
    }
    operation_test_response(status = 202L, headers = headers, url = req$url)
  })

  from_location <- .fabric_operation_submit(
    operation_test_request(),
    fabric_credential(token = "test-token")
  )
  from_id <- .fabric_operation_submit(
    operation_test_request(),
    fabric_credential(token = "test-token")
  )

  expect_equal(from_location$id, operation_test_id)
  expect_equal(from_id$id, operation_test_id)
  expect_equal(from_location$status_url, from_id$status_url)
  expected_result <- paste0(
    "https://api.fabric.microsoft.com/v1/operations/",
    operation_test_id,
    "/result"
  )
  expect_equal(from_location$result_url, expected_result)
  expect_equal(from_id$result_url, expected_result)
  expect_true(from_location$result_expected)
  expect_true(from_id$result_expected)
})

test_that("a successful bare core operation ID retrieves its result", {
  calls <- character()
  httr2::local_mocked_responses(function(req) {
    calls <<- c(calls, req$url)
    if (grepl("/result$", req$url)) {
      return(operation_test_response(
        body = list(id = "created-item", type = "Notebook"),
        url = req$url
      ))
    }
    operation_test_response(
      body = list(status = "Succeeded", percentComplete = 100L),
      headers = list(`x-ms-operation-id` = operation_test_id),
      url = req$url
    )
  })

  result <- fabric_operation_result(
    operation_test_id,
    wait = FALSE,
    token = "test-token"
  )

  expect_s3_class(result, "fabric_operation_result")
  expect_equal(result$value$id, "created-item")
  expect_true(result$operation$result_expected)
  expect_length(calls, 2L)
  expect_match(calls[[1L]], paste0("/operations/", operation_test_id, "$"))
  expect_match(
    calls[[2L]],
    paste0("/operations/", operation_test_id, "/result$")
  )
})

test_that("regional Fabric operation locations select the Power BI audience", {
  regional <- paste0(
    "https://wabi-switzerland-north-primary-redirect.analysis.windows.net/",
    "v1/operations/",
    operation_test_id
  )
  audiences <- character()
  provider <- function(audience, force_refresh = FALSE) {
    audiences <<- c(audiences, audience)
    "test-token"
  }
  calls <- 0L
  httr2::local_mocked_responses(function(req) {
    calls <<- calls + 1L
    if (calls == 1L) {
      return(operation_test_response(
        status = 202L,
        headers = list(
          Location = regional,
          `x-ms-operation-id` = operation_test_id,
          `Retry-After` = "0"
        ),
        url = req$url
      ))
    }
    if (calls == 2L) {
      return(operation_test_response(
        body = list(status = "Succeeded", percentComplete = 100L),
        headers = list(
          Location = paste0(regional, "/result"),
          `x-ms-operation-id` = operation_test_id
        ),
        url = req$url
      ))
    }
    operation_test_response(
      body = list(id = "regional-result"),
      url = req$url
    )
  })

  result <- .fabric_operation_perform(
    operation_test_request(),
    fabric_credential(token = provider),
    timeout = 10
  )

  expect_equal(result$value$id, "regional-result")
  expect_equal(
    audiences,
    c(
      .fabric_audience$fabric,
      .fabric_audience$power_bi,
      .fabric_audience$power_bi
    )
  )
  expect_equal(result$operation$status_url, regional)
})

test_that("malformed operation headers and states raise protocol errors", {
  other_id <- "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
  header_cases <- list(
    list(),
    list(
      Location = paste0("/v1/operations/", operation_test_id),
      `x-ms-operation-id` = other_id
    ),
    list(
      Location = paste0(
        "https://attacker.example/v1/operations/",
        operation_test_id
      ),
      `x-ms-operation-id` = operation_test_id
    )
  )

  for (headers in header_cases) {
    httr2::local_mocked_responses(function(req) {
      operation_test_response(status = 202L, headers = headers, url = req$url)
    })
    expect_error(
      .fabric_operation_submit(
        operation_test_request(),
        fabric_credential(token = "test-token")
      ),
      class = "fabric_operation_protocol_error"
    )
  }

  malformed_bodies <- list(
    list(percentComplete = 25L),
    list(status = "Running", percentComplete = 101L),
    list(status = "Running", createdTimeUtc = "not-a-time")
  )
  for (body in malformed_bodies) {
    httr2::local_mocked_responses(function(req) {
      operation_test_response(body = body, url = req$url)
    })
    expect_error(
      fabric_operation_status(
        operation_test_id,
        token = "test-token",
        respect_retry_after = FALSE
      ),
      class = "fabric_operation_protocol_error"
    )
  }
})

test_that("empty and binary result bodies remain distinguishable", {
  result_for <- function(body, content_type) {
    calls <- 0L
    httr2::local_mocked_responses(function(req) {
      calls <<- calls + 1L
      if (calls == 1L) {
        return(operation_test_response(
          body = list(status = "Succeeded", percentComplete = 100L),
          url = req$url
        ))
      }
      operation_test_response(
        body = body,
        content_type = content_type,
        url = req$url
      )
    })
    fabric_operation_result(
      paste0(
        "https://api.fabric.microsoft.com/v1/operations/",
        operation_test_id,
        "/result"
      ),
      token = "test-token",
      timeout = 10
    )
  }

  empty <- result_for(NULL, "application/octet-stream")
  binary <- result_for(as.raw(c(0L, 1L, 255L)), "application/octet-stream")

  expect_true(empty$empty)
  expect_null(empty$value)
  expect_false(binary$empty)
  expect_identical(binary$value, as.raw(c(0L, 1L, 255L)))
  expect_identical(names(empty), names(binary))
})

test_that("result retrieval can refuse to wait", {
  httr2::local_mocked_responses(function(req) {
    operation_test_response(
      body = list(status = "Running", percentComplete = 5L),
      url = req$url
    )
  })

  condition <- expect_error(
    fabric_operation_result(
      operation_test_id,
      wait = FALSE,
      token = "test-token",
      timeout = 10
    ),
    class = "fabric_operation_not_ready"
  )
  expect_equal(condition$operation_status$status, "Running")
})
