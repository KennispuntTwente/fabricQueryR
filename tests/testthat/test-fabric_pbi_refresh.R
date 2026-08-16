pbi_refresh_workspace_id <- "11111111-1111-4111-8111-111111111111"
pbi_refresh_dataset_id <- "22222222-2222-4222-8222-222222222222"
pbi_refresh_id <- "33333333-3333-4333-8333-333333333333"

pbi_refresh_test_model <- function() {
  structure(
    list(
      id = pbi_refresh_dataset_id,
      workspaceId = pbi_refresh_workspace_id,
      type = "SemanticModel",
      displayName = "Refresh fixture"
    ),
    class = c("fabric_item", "list")
  )
}

pbi_refresh_test_handle <- function(
  mode = "enhanced",
  retry_after = NULL,
  my_workspace = FALSE
) {
  .pbi_refresh_handle(
    refresh_id = pbi_refresh_id,
    target = list(
      workspace_id = if (my_workspace) NULL else pbi_refresh_workspace_id,
      dataset_id = pbi_refresh_dataset_id,
      my_workspace = my_workspace
    ),
    credential = fabric_credential(token = "test-token"),
    api_base = "https://powerbi.test/v1.0/myorg",
    allow_custom_endpoint = TRUE,
    retry_after = retry_after,
    mode = mode
  )
}

test_that("standard refresh submission returns a reusable handle", {
  call <- NULL
  local_mocked_bindings(
    .pbi_refresh_request = function(
      method,
      url,
      credential,
      payload = NULL,
      idempotent = NULL
    ) {
      call <<- list(
        method = method,
        url = url,
        credential = credential,
        payload = payload,
        idempotent = idempotent
      )
      list(
        status_code = 202L,
        location = paste0(url, "/", pbi_refresh_id),
        request_id = pbi_refresh_id,
        retry_after = 7,
        body = list()
      )
    }
  )

  refresh <- fabric_pbi_refresh(
    pbi_refresh_test_model(),
    notify_option = "mailonfailure",
    token = "test-token",
    api_base = "https://powerbi.test/v1.0/myorg",
    allow_custom_endpoint = TRUE
  )

  expect_s3_class(refresh, "fabric_pbi_refresh")
  expect_identical(refresh$id, pbi_refresh_id)
  expect_identical(refresh$mode, "standard")
  expect_identical(refresh$workspace_id, pbi_refresh_workspace_id)
  expect_identical(refresh$dataset_id, pbi_refresh_dataset_id)
  expect_identical(refresh$retry_after, 7)
  expect_identical(call$method, "POST")
  expect_false(call$idempotent)
  expect_match(
    call$url,
    paste0(
      "/groups/",
      pbi_refresh_workspace_id,
      "/datasets/",
      pbi_refresh_dataset_id,
      "/refreshes"
    ),
    fixed = TRUE
  )
  expect_equal(call$payload, list(notifyOption = "MailOnFailure"))
  expect_s3_class(call$credential, "fabric_credential")
})

test_that("enhanced refresh builds documented processing controls", {
  payload <- NULL
  url <- NULL
  local_mocked_bindings(
    .pbi_refresh_request = function(method, request_url, credential, ...) {
      args <- list(...)
      payload <<- args$payload
      url <<- request_url
      list(
        status_code = 202L,
        location = NULL,
        request_id = pbi_refresh_id,
        retry_after = NULL,
        body = list()
      )
    }
  )

  refresh <- fabric_pbi_refresh(
    workspace_id = pbi_refresh_workspace_id,
    dataset_id = pbi_refresh_dataset_id,
    mode = "enhanced",
    type = "full",
    commit_mode = "transactional",
    objects = list(
      list(table = "Sales", partition = "2026"),
      list(table = "Customers")
    ),
    apply_refresh_policy = FALSE,
    effective_date = as.Date("2026-08-13"),
    max_parallelism = 4L,
    retry_count = 2L,
    timeout = "02:00:00",
    token = "test-token",
    api_base = "https://powerbi.test/v1.0/myorg",
    allow_custom_endpoint = TRUE
  )

  expect_identical(refresh$mode, "enhanced")
  expect_equal(
    payload,
    list(
      type = "Full",
      commitMode = "Transactional",
      objects = list(
        list(table = "Sales", partition = "2026"),
        list(table = "Customers")
      ),
      applyRefreshPolicy = FALSE,
      effectiveDate = "2026-08-13T00:00:00Z",
      maxParallelism = 4L,
      retryCount = 2L,
      timeout = "02:00:00"
    )
  )
  expect_match(url, paste0("/groups/", pbi_refresh_workspace_id), fixed = TRUE)
})

test_that("automatic mode infers enhanced and My Workspace routes", {
  call <- NULL
  local_mocked_bindings(
    .pbi_refresh_request = function(method, url, credential, payload, ...) {
      call <<- list(url = url, payload = payload)
      list(
        status_code = 202L,
        location = paste0(url, "/", pbi_refresh_id),
        request_id = NULL,
        retry_after = NULL,
        body = list()
      )
    }
  )

  refresh <- fabric_pbi_refresh(
    dataset_id = pbi_refresh_dataset_id,
    my_workspace = TRUE,
    objects = c("Facts", "Calendar"),
    token = "test-token",
    api_base = "https://powerbi.test/v1.0/myorg",
    allow_custom_endpoint = TRUE
  )

  expect_identical(refresh$mode, "enhanced")
  expect_true(refresh$my_workspace)
  expect_match(
    call$url,
    paste0("/myorg/datasets/", pbi_refresh_dataset_id, "/refreshes"),
    fixed = TRUE
  )
  expect_equal(
    call$payload$objects,
    list(list(table = "Facts"), list(table = "Calendar"))
  )
})

test_that("refresh payload validation enforces Power BI contracts", {
  expect_error(
    .pbi_refresh_payload(
      "standard",
      NULL,
      "Full",
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL
    ),
    "Enhanced refresh options"
  )
  expect_error(
    .pbi_refresh_payload(
      "enhanced",
      "MailOnFailure",
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL
    ),
    "notify_option"
  )
  expect_error(
    .pbi_refresh_payload(
      "enhanced",
      NULL,
      NULL,
      "PartialBatch",
      NULL,
      TRUE,
      NULL,
      NULL,
      NULL,
      NULL
    ),
    "Transactional"
  )
  expect_error(
    .pbi_refresh_payload(
      "enhanced",
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      2L,
      "09:00:00"
    ),
    "24 hours"
  )
  expect_error(.pbi_refresh_timeout("24:00:00"), "HH:MM:SS")
  expect_error(.pbi_refresh_timeout("1:2:03"), "HH:MM:SS")
  expect_error(.pbi_refresh_objects(list(list(partition = "p"))), "table")
  expect_error(
    .pbi_refresh_objects(list(list(table = "t", future = "x"))),
    "optional partition"
  )
  expect_error(
    .pbi_refresh_effective_date("2026-08-13"),
    "ISO 8601"
  )
  expect_error(
    .pbi_refresh_effective_date("2026-02-30T00:00:00Z"),
    "valid ISO 8601"
  )

  defaults <- .pbi_refresh_payload(
    "enhanced",
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL
  )
  expect_equal(defaults$payload, list(type = "Automatic"))

  partial_batch <- .pbi_refresh_payload(
    "enhanced",
    NULL,
    NULL,
    "PartialBatch",
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL
  )
  expect_identical(partial_batch$payload$applyRefreshPolicy, FALSE)
})

test_that("refresh response IDs are validated and reconciled", {
  expect_identical(
    .pbi_refresh_response_id(list(
      location = paste0("https://example.test/refreshes/", pbi_refresh_id),
      request_id = NULL
    )),
    pbi_refresh_id
  )
  expect_error(
    .pbi_refresh_response_id(list(location = NULL, request_id = NULL)),
    class = "fabric_pbi_refresh_protocol_error"
  )
  expect_error(
    .pbi_refresh_response_id(list(
      location = paste0("https://example.test/refreshes/", pbi_refresh_id),
      request_id = "44444444-4444-4444-8444-444444444444"
    )),
    "conflicting refresh IDs"
  )
})

test_that("request encoding preserves object arrays and empty standard bodies", {
  requests <- list()
  local_mocked_bindings(
    .httr2_perform = function(req, ...) {
      requests[[length(requests) + 1L]] <<- list(req = req, args = list(...))
      httr2::new_response(
        method = req$method,
        url = req$url,
        status_code = 202L,
        headers = list(`x-ms-request-id` = pbi_refresh_id),
        body = charToRaw("")
      )
    }
  )

  .pbi_refresh_request(
    "POST",
    "https://powerbi.test/v1.0/myorg/refreshes",
    fabric_credential(token = "test-token"),
    payload = list(objects = list(list(table = "Facts"))),
    idempotent = FALSE
  )
  .pbi_refresh_request(
    "POST",
    "https://powerbi.test/v1.0/myorg/refreshes",
    fabric_credential(token = "test-token"),
    payload = list(),
    idempotent = FALSE
  )

  first <- requests[[1L]]
  encoded <- jsonlite::toJSON(
    first$req$body$data,
    auto_unbox = first$req$body$params$auto_unbox,
    null = first$req$body$params$null
  )
  expect_match(encoded, '"objects":\\[\\{"table":"Facts"\\}\\]')
  expect_false(first$args$idempotent)
  expect_identical(first$args$audience, .fabric_audience$power_bi)
  expect_identical(rawToChar(requests[[2L]]$req$body$data), "{}")
})

test_that("history normalizes attempts, errors, times, and detail links", {
  call <- NULL
  local_mocked_bindings(
    .pbi_refresh_request = function(method, url, credential, ...) {
      call <<- list(method = method, url = url)
      list(
        status_code = 200L,
        location = NULL,
        request_id = NULL,
        retry_after = NULL,
        body = list(
          value = list(list(
            refreshType = "ViaEnhancedApi",
            startTime = "2026-08-13T08:00:00Z",
            endTime = "2026-08-13T08:02:00Z",
            status = "Failed",
            requestId = pbi_refresh_id,
            serviceExceptionJson = paste0(
              '{"errorCode":"ModelRefreshFailed",',
              '"errorDescription":"source unavailable"}'
            ),
            refreshAttempts = list(
              list(
                attemptId = 1L,
                startTime = "2026-08-13T08:00:00Z",
                endTime = "2026-08-13T08:01:00Z",
                type = "Data",
                serviceExceptionJson = '{"errorCode":"Transient"}'
              ),
              list(
                attemptId = 2L,
                startTime = "2026-08-13T08:01:00Z",
                endTime = "2026-08-13T08:02:00Z",
                type = "Data"
              )
            ),
            futureField = list(retained = TRUE)
          ))
        )
      )
    }
  )

  history <- fabric_pbi_refresh_history(
    pbi_refresh_test_model(),
    top = 5L,
    token = "test-token",
    api_base = "https://powerbi.test/v1.0/myorg",
    allow_custom_endpoint = TRUE
  )

  expect_s3_class(history, "fabric_pbi_refresh_history")
  expect_length(history, 1L)
  detail <- history[[1L]]
  expect_s3_class(detail, "fabric_pbi_refresh_detail")
  expect_identical(detail$state, "Failed")
  expect_s3_class(detail$start_time, "POSIXct")
  expect_identical(format(detail$start_time, tz = "UTC"), "2026-08-13 08:00:00")
  expect_length(detail$attempts, 2L)
  expect_identical(detail$attempts[[1L]]$status, "Failed")
  expect_identical(detail$attempts[[1L]]$service_error$errorCode, "Transient")
  expect_identical(detail$attempts[[2L]]$status, "Completed")
  expect_identical(detail$service_error$errorCode, "ModelRefreshFailed")
  expect_true(detail$raw$futureField$retained)
  expect_match(
    detail$details_url,
    paste0(
      "/groups/",
      pbi_refresh_workspace_id,
      "/datasets/",
      pbi_refresh_dataset_id,
      "/refreshdetails/",
      pbi_refresh_id
    ),
    fixed = TRUE
  )
  expect_identical(detail$refresh$mode, "enhanced")
  expect_identical(call$method, "GET")
  expect_match(call$url, "%24top=5")
})

test_that("detail states preserve queue, warning, cancellation, and timeout", {
  handle <- pbi_refresh_test_handle()
  queued <- .pbi_refresh_detail(
    list(status = "Unknown", extendedStatus = "NotStarted"),
    handle,
    202L
  )
  warning <- .pbi_refresh_detail(
    list(
      status = "Completed",
      extendedStatus = "Completed",
      messages = list(list(type = "Warning", message = "Measure skipped"))
    ),
    handle,
    200L
  )
  cancelled <- .pbi_refresh_detail(
    list(status = "Unknown", extendedStatus = "Cancelled"),
    handle,
    200L
  )
  timed_out <- .pbi_refresh_detail(
    list(status = "Failed", extendedStatus = "TimedOut"),
    handle,
    200L
  )

  expect_identical(queued$state, "Queued")
  expect_false(queued$terminal)
  expect_identical(warning$state, "CompletedWithWarnings")
  expect_true(warning$has_warnings)
  expect_identical(cancelled$state, "Cancelled")
  expect_identical(timed_out$state, "TimedOut")
  expect_true(timed_out$terminal)
})

test_that("status accepts handles and raw request IDs", {
  calls <- character()
  local_mocked_bindings(
    .pbi_refresh_request = function(method, url, credential, ...) {
      calls <<- c(calls, url)
      list(
        status_code = 202L,
        location = NULL,
        request_id = NULL,
        retry_after = 4,
        body = list(
          status = "Unknown",
          extendedStatus = "InProgress",
          numberOfAttempts = 1L
        )
      )
    }
  )
  handle <- pbi_refresh_test_handle()

  from_handle <- fabric_pbi_refresh_status(
    handle,
    .sleep = function(seconds) NULL
  )
  from_id <- fabric_pbi_refresh_status(
    refresh_id = pbi_refresh_id,
    workspace_id = pbi_refresh_workspace_id,
    dataset_id = pbi_refresh_dataset_id,
    token = "override-token",
    api_base = "https://powerbi.test/v1.0/myorg",
    allow_custom_endpoint = TRUE,
    .sleep = function(seconds) NULL
  )

  expect_identical(from_handle$state, "InProgress")
  expect_identical(from_handle$retry_after, 4)
  expect_identical(from_id$state, "InProgress")
  expect_length(calls, 2L)
  expect_true(all(grepl(pbi_refresh_id, calls, fixed = TRUE)))
  expect_error(
    fabric_pbi_refresh_status(
      handle,
      dataset_id = pbi_refresh_dataset_id,
      .sleep = function(seconds) NULL
    ),
    "cannot be combined"
  )
})

test_that("wait observes active attempts and returns completion", {
  responses <- list(
    list(
      status_code = 202L,
      retry_after = 0,
      body = list(
        status = "Unknown",
        extendedStatus = "NotStarted",
        numberOfAttempts = 0L
      )
    ),
    list(
      status_code = 202L,
      retry_after = 0,
      body = list(
        status = "Unknown",
        extendedStatus = "InProgress",
        numberOfAttempts = 2L,
        refreshAttempts = list(
          list(
            attemptId = 1L,
            type = "Data",
            endTime = "2026-08-13T08:00:01Z",
            serviceExceptionJson = '{"errorCode":"Transient"}'
          ),
          list(
            attemptId = 2L,
            type = "Data",
            startTime = "2026-08-13T08:00:02Z"
          )
        )
      )
    ),
    list(
      status_code = 200L,
      retry_after = NULL,
      body = list(
        status = "Completed",
        extendedStatus = "Completed",
        numberOfAttempts = 2L,
        endTime = "2026-08-13T08:00:03Z"
      )
    )
  )
  index <- 0L
  now <- as.POSIXct("2026-08-13 08:00:00", tz = "UTC")
  local_mocked_bindings(
    .pbi_refresh_request = function(...) {
      index <<- index + 1L
      c(
        responses[[index]],
        list(location = NULL, request_id = NULL)
      )
    }
  )

  result <- fabric_pbi_refresh_wait(
    pbi_refresh_test_handle(),
    poll_interval = 0,
    timeout = 10,
    .sleep = function(seconds) {
      now <<- now + seconds
    },
    .now = function() now
  )

  expect_identical(result$state, "Completed")
  expect_identical(result$number_of_attempts, 2L)
  expect_identical(index, 3L)
})

test_that("wait raises distinct service terminal conditions", {
  states <- list(
    Failed = c("Failed", "Failed", "fabric_pbi_refresh_failed"),
    TimedOut = c("Failed", "TimedOut", "fabric_pbi_refresh_service_timeout"),
    Cancelled = c("Unknown", "Cancelled", "fabric_pbi_refresh_cancelled"),
    Disabled = c("Disabled", "Disabled", "fabric_pbi_refresh_disabled")
  )

  for (values in states) {
    now <- as.POSIXct("2026-08-13 08:00:00", tz = "UTC")
    local_mocked_bindings(
      .pbi_refresh_request = function(...) {
        list(
          status_code = 200L,
          location = NULL,
          request_id = NULL,
          retry_after = NULL,
          body = list(status = values[[1L]], extendedStatus = values[[2L]])
        )
      }
    )
    expect_error(
      fabric_pbi_refresh_wait(
        pbi_refresh_test_handle(),
        poll_interval = 0,
        timeout = 1,
        .sleep = function(seconds) {
          now <<- now + seconds
        },
        .now = function() now
      ),
      class = values[[3L]]
    )
  }
})

test_that("client wait timeout is distinct and can cancel", {
  cancel_calls <- 0L
  now <- as.POSIXct("2026-08-13 08:00:00", tz = "UTC")
  local_mocked_bindings(
    .pbi_refresh_cancel_context = function(context) {
      cancel_calls <<- cancel_calls + 1L
      invisible(TRUE)
    }
  )

  error <- expect_error(
    fabric_pbi_refresh_wait(
      pbi_refresh_test_handle(),
      timeout = 1,
      cancel_on_timeout = TRUE,
      .sleep = function(seconds) {
        now <<- now + seconds
      },
      .now = function() now
    ),
    class = "fabric_pbi_refresh_wait_timeout"
  )
  expect_identical(error$cancel_accepted, TRUE)
  expect_identical(cancel_calls, 1L)
})

test_that("wait rejects unknown future states without polling forever", {
  now <- as.POSIXct("2026-08-13 08:00:00", tz = "UTC")
  local_mocked_bindings(
    .pbi_refresh_request = function(...) {
      list(
        status_code = 200L,
        location = NULL,
        request_id = NULL,
        retry_after = NULL,
        body = list(status = "FutureState")
      )
    }
  )

  condition <- expect_error(
    fabric_pbi_refresh_wait(
      pbi_refresh_test_handle(),
      poll_interval = 0,
      timeout = 1,
      .sleep = function(seconds) {
        now <<- now + seconds
      },
      .now = function() now
    ),
    class = "fabric_pbi_refresh_unknown_status"
  )
  expect_identical(condition$refresh_status$state, "FutureState")
})

test_that("cancel uses the request-specific DELETE route", {
  call <- NULL
  local_mocked_bindings(
    .pbi_refresh_request = function(method, url, credential, ...) {
      call <<- list(method = method, url = url, args = list(...))
      list(
        status_code = 200L,
        location = NULL,
        request_id = NULL,
        retry_after = NULL,
        body = list()
      )
    }
  )

  result <- withVisible(fabric_pbi_refresh_cancel(pbi_refresh_test_handle()))

  expect_false(result$visible)
  expect_identical(result$value, TRUE)
  expect_identical(call$method, "DELETE")
  expect_true(call$args$idempotent)
  expect_match(call$url, paste0("/refreshes/", pbi_refresh_id), fixed = TRUE)
})

test_that("connection strings resolve through the existing DAX target lookup", {
  call <- NULL
  local_mocked_bindings(
    pbi_resolve_ids_from_connstr = function(connstr, credential, api_base) {
      expect_match(connstr, "powerbi://", fixed = TRUE)
      list(
        group_id = pbi_refresh_workspace_id,
        dataset_id = pbi_refresh_dataset_id
      )
    },
    .pbi_refresh_request = function(method, url, credential, payload, ...) {
      call <<- url
      list(
        status_code = 202L,
        location = NULL,
        request_id = pbi_refresh_id,
        retry_after = NULL,
        body = list()
      )
    }
  )

  fabric_pbi_refresh(
    paste0(
      "Data Source=powerbi://api.powerbi.com/v1.0/myorg/Workspace;",
      "Initial Catalog=Model;"
    ),
    token = "test-token"
  )

  expect_match(
    call,
    paste0("/groups/", pbi_refresh_workspace_id),
    fixed = TRUE
  )
})

test_that("print methods expose identity and state without credentials", {
  handle <- pbi_refresh_test_handle()
  detail <- .pbi_refresh_detail(
    list(status = "Completed", numberOfAttempts = 1L),
    handle,
    200L
  )

  expect_output(print(handle), pbi_refresh_id, fixed = TRUE)
  expect_output(print(handle), "enhanced", fixed = TRUE)
  expect_output(print(detail), "Completed", fixed = TRUE)
  expect_output(print(detail), "attempts: 1", fixed = TRUE)
  expect_false(any(grepl(
    "test-token",
    capture.output(print(handle)),
    fixed = TRUE
  )))
})

test_that("refresh handles do not serialize bearer credentials", {
  secret <- "refresh-handle-secret-that-must-not-be-serialized"
  handle <- pbi_refresh_test_handle()
  reference <- .pbi_refresh_credential_reference(
    fabric_credential(token = secret)
  )
  handle$credential <- reference$reference
  handle$.credential_key <- reference$key

  expect_identical(
    fabric_get_token(.pbi_refresh_credential(handle), "audience"),
    secret
  )
  serialized <- serialize(handle, NULL, ascii = TRUE)
  expect_false(grepl(secret, rawToChar(serialized), fixed = TRUE))

  restored <- unserialize(serialized)
  expect_error(
    .pbi_refresh_credential(restored),
    "no longer has an in-process credential",
    class = "fabric_pbi_refresh_credential_error"
  )
})
