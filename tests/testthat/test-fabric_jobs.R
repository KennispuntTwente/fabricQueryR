job_test_item <- function(type = "Notebook") {
  list(
    id = "11111111-1111-1111-1111-111111111111",
    workspaceId = "22222222-2222-2222-2222-222222222222",
    type = type,
    displayName = "Job fixture"
  )
}

job_test_handle <- function(
  status_result = NULL,
  item_type = "Notebook",
  retry_after = NULL
) {
  structure(
    list(
      id = "33333333-3333-3333-3333-333333333333",
      workspace_id = "22222222-2222-2222-2222-222222222222",
      item_id = "11111111-1111-1111-1111-111111111111",
      item_type = item_type,
      job_type = if (item_type == "Notebook") "RunNotebook" else "Pipeline",
      location = "/jobs/instances/33333333-3333-3333-3333-333333333333",
      retry_after = retry_after,
      submitted_at = as.POSIXct("2026-01-01", tz = "UTC"),
      api_base = "https://api.fabric.test/v1",
      route = if (item_type == "Notebook") "notebook" else "core",
      credential = fabric_credential(token = "test-token"),
      status_result = status_result
    ),
    class = "fabric_job"
  )
}

test_that("job submission rejects a contradictory explicit workspace", {
  expect_error(
    fabric_job_run(
      job_test_item(),
      workspace = "99999999-9999-9999-9999-999999999999",
      token = "test-token",
      api_base = "https://api.fabric.test/v1"
    ),
    "belongs to a different workspace",
    fixed = TRUE
  )
})

test_that("job submission uses workspace-specific API endpoints", {
  call <- NULL
  local_mocked_bindings(
    .fabric_job_request = function(method, url, ...) {
      call <<- list(method = method, url = url)
      list(
        status_code = 202L,
        location = paste0(
          "/jobs/instances/",
          "33333333-3333-3333-3333-333333333333"
        ),
        retry_after = NULL
      )
    }
  )
  workspace <- structure(
    list(
      id = "22222222-2222-2222-2222-222222222222",
      displayName = "Private workspace",
      apiEndpoint = "https://workspace.z13.api.fabric.microsoft.com"
    ),
    class = c("fabric_workspace", "list")
  )

  job <- fabric_job_run(
    job_test_item(),
    workspace = workspace,
    token = "test-token"
  )

  expect_match(
    call$url,
    "https://workspace.z13.api.fabric.microsoft.com/v1/workspaces/",
    fixed = TRUE
  )
  expect_equal(
    job$api_base,
    "https://workspace.z13.api.fabric.microsoft.com/v1"
  )

  fabric_job_run(
    job_test_item(),
    workspace = workspace,
    token = "test-token",
    api_base = "https://explicit.test/v1"
  )
  expect_match(call$url, "https://explicit.test/v1/workspaces/", fixed = TRUE)
})

test_that("notebook run builds typed release payload and job handle", {
  call <- NULL
  local_mocked_bindings(
    .fabric_job_request = function(
      method,
      url,
      credential,
      payload = NULL,
      idempotent = NULL,
      ...
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
        location = paste0(
          "https://api.fabric.test/v1/workspaces/w/items/i/",
          "jobs/instances/33333333-3333-3333-3333-333333333333"
        ),
        retry_after = 7
      )
    }
  )

  job <- fabric_job_run(
    job_test_item(),
    parameters = list(
      count = 2L,
      ratio = 1.5,
      enabled = TRUE,
      label = "unit",
      when = as.Date("2026-07-24"),
      correlation = "44444444-4444-4444-4444-444444444444"
    ),
    parameter_types = c(correlation = "Guid"),
    default_lakehouse = list(
      id = "55555555-5555-5555-5555-555555555555",
      workspaceId = "66666666-6666-6666-6666-666666666666"
    ),
    session_tag = "fabricqueryr_tests",
    token = "test-token",
    api_base = "https://api.fabric.test/v1/"
  )

  expect_s3_class(job, "fabric_job")
  expect_equal(job$id, "33333333-3333-3333-3333-333333333333")
  expect_equal(job$retry_after, 7)
  expect_equal(job$route, "notebook")
  expect_equal(call$method, "POST")
  expect_false(call$idempotent)
  expect_match(
    call$url,
    "/notebooks/11111111-1111-1111-1111-111111111111/",
    fixed = TRUE
  )
  expect_match(call$url, "?beta=false", fixed = TRUE)
  expect_equal(call$payload$executionData$compute, "Spark")
  expect_equal(
    call$payload$executionData$computeConfiguration$defaultLakehouse,
    list(
      referenceType = "ById",
      itemId = "55555555-5555-5555-5555-555555555555",
      workspaceId = "66666666-6666-6666-6666-666666666666"
    )
  )
  expect_equal(
    call$payload$executionData$computeConfiguration$highConcurrencyModeOptions,
    list(enabled = TRUE, sessionTag = "fabricqueryr_tests")
  )
  expect_equal(
    unname(vapply(
      call$payload$parameters,
      `[[`,
      character(1),
      "type"
    )),
    c("Integer", "Number", "Boolean", "Text", "DateTime", "Guid")
  )
  expect_null(names(call$payload$parameters))
  expect_equal(call$payload$parameters[[5L]]$value, "2026-07-24T00:00:00Z")
})

test_that("notebook run preserves configured compute without overrides", {
  payload <- "not called"
  local_mocked_bindings(
    .fabric_job_request = function(
      method,
      url,
      credential,
      payload = NULL,
      ...
    ) {
      payload <<- payload
      list(
        status_code = 202L,
        location = paste0(
          "/jobs/instances/",
          "33333333-3333-3333-3333-333333333333"
        ),
        retry_after = NULL
      )
    }
  )

  fabric_job_run(
    job_test_item(),
    token = "test-token",
    api_base = "https://api.fabric.test/v1"
  )

  expect_null(payload)
})

test_that("pipeline run uses current core path without a JSON payload", {
  call <- NULL
  local_mocked_bindings(
    .fabric_job_request = function(
      method,
      url,
      credential,
      payload = NULL,
      idempotent = NULL,
      ...
    ) {
      call <<- list(
        method = method,
        url = url,
        payload = payload,
        idempotent = idempotent
      )
      list(
        status_code = 202L,
        location = paste0(
          "/jobs/instances/",
          "33333333-3333-3333-3333-333333333333"
        ),
        retry_after = NULL
      )
    }
  )

  job <- fabric_job_run(
    job_test_item("DataPipeline"),
    token = "test-token",
    api_base = "https://api.fabric.test/v1"
  )

  expect_equal(job$job_type, "Pipeline")
  expect_equal(job$route, "core")
  expect_match(
    call$url,
    "/items/11111111-1111-1111-1111-111111111111/jobs/Pipeline/instances",
    fixed = TRUE
  )
  expect_null(call$payload)
  expect_false(call$idempotent)
})

test_that("job POST requests carry an explicit zero-length body", {
  request <- NULL
  local_mocked_bindings(
    .httr2_perform = function(req, ...) {
      request <<- req
      httr2::response(status_code = 202L)
    }
  )

  .fabric_job_request(
    "POST",
    "https://api.fabric.test/v1/jobs",
    fabric_credential(token = "test-token"),
    payload = NULL,
    parse_json = FALSE
  )

  expect_identical(request$body$type, "raw")
  expect_length(request$body$data, 0L)
})

test_that("job POST requests preserve one-element schema arrays", {
  request <- NULL
  local_mocked_bindings(
    .httr2_perform = function(req, ...) {
      request <<- req
      httr2::response(status_code = 202L)
    }
  )

  .fabric_job_request(
    "POST",
    "https://api.fabric.test/v1/jobs",
    fabric_credential(token = "test-token"),
    payload = list(
      executionData = list(
        additionalLibraryUris = "abfss://account/library.zip",
        computeConfiguration = list(jars = "abfss://account/library.jar")
      ),
      parameters = list(list(name = "mode", type = "Text", value = "test"))
    ),
    parse_json = FALSE
  )

  body <- jsonlite::toJSON(
    request$body$data,
    auto_unbox = request$body$params$auto_unbox,
    null = request$body$params$null
  )
  parsed <- jsonlite::fromJSON(body, simplifyVector = FALSE)
  expect_length(parsed$executionData$additionalLibraryUris, 1L)
  expect_length(parsed$executionData$computeConfiguration$jars, 1L)
  expect_length(parsed$parameters, 1L)
  expect_match(
    body,
    '"additionalLibraryUris":\\["abfss://account/library.zip"\\]'
  )
  expect_match(body, '"jars":\\["abfss://account/library.jar"\\]')
})

test_that("Spark job definition execution data uses its typed route", {
  call <- NULL
  local_mocked_bindings(
    .fabric_job_request = function(
      method,
      url,
      credential,
      payload = NULL,
      ...
    ) {
      call <<- list(method = method, url = url, payload = payload)
      list(
        status_code = 202L,
        location = paste0(
          "/jobs/instances/",
          "33333333-3333-3333-3333-333333333333"
        ),
        retry_after = 1
      )
    }
  )
  reference <- list(
    referenceType = "ById",
    itemId = "55555555-5555-5555-5555-555555555555",
    workspaceId = "22222222-2222-2222-2222-222222222222"
  )

  job <- fabric_job_run(
    job_test_item("SparkJobDefinition"),
    execution_data = list(
      executableFile = "abfss://container@account/path/job.py",
      commandLineArguments = "--mode test",
      additionalLibraryUris = c(
        "abfss://container@account/path/library.zip"
      ),
      defaultLakehouseId = reference
    ),
    token = "test-token",
    api_base = "https://api.fabric.test/v1"
  )

  expect_equal(job$route, "spark_job_definition")
  expect_match(call$url, "/sparkJobDefinitions/", fixed = TRUE)
  expect_match(call$url, "/jobs/sparkjob/instances", fixed = TRUE)
  expect_equal(call$payload$executionData$defaultLakehouseId, reference)
})

test_that("job payload fields follow the selected route contract", {
  call <- NULL
  local_mocked_bindings(
    .fabric_job_request = function(
      method,
      url,
      credential,
      payload = NULL,
      ...
    ) {
      call <<- list(method = method, url = url, payload = payload)
      list(
        status_code = 202L,
        location = paste0(
          "/jobs/instances/",
          "33333333-3333-3333-3333-333333333333"
        ),
        retry_after = NULL
      )
    }
  )

  fabric_job_run(
    job_test_item("DataPipeline"),
    execution_data = list(executeOption = "ApplyChangesIfNeeded"),
    token = "test-token",
    api_base = "https://api.fabric.test/v1"
  )
  expect_equal(
    call$payload$executionData,
    list(executeOption = "ApplyChangesIfNeeded")
  )
  expect_match(call$url, "/jobs/Pipeline/instances", fixed = TRUE)

  expect_error(
    fabric_job_run(
      job_test_item("SparkJobDefinition"),
      parameters = list(mode = "test"),
      token = "test-token",
      api_base = "https://api.fabric.test/v1"
    ),
    "do not support `parameters`",
    fixed = TRUE
  )
})

test_that("status normalizes documented metadata and notebook exit value", {
  called_url <- NULL
  local_mocked_bindings(
    .fabric_job_request = function(
      method,
      url,
      credential,
      idempotent = NULL,
      ...
    ) {
      called_url <<- url
      expect_equal(method, "GET")
      expect_true(idempotent)
      list(
        status_code = 200L,
        retry_after = 3,
        body = list(
          id = "33333333-3333-3333-3333-333333333333",
          itemId = "11111111-1111-1111-1111-111111111111",
          jobType = "RunNotebook",
          invokeType = "Manual",
          status = "Completed",
          rootActivityId = "77777777-7777-7777-7777-777777777777",
          startTimeUtc = "2026-07-24T10:00:00Z",
          endTimeUtc = "2026-07-24T10:01:02.500Z",
          properties = list(exitValue = "fabricqueryr-job-success:unit")
        )
      )
    }
  )

  result <- fabric_job_status(job_test_handle())

  expect_s3_class(result, "fabric_job_instance")
  expect_equal(result$status, "Completed")
  expect_equal(result$invoke_type, "Manual")
  expect_equal(
    result$root_activity_id,
    "77777777-7777-7777-7777-777777777777"
  )
  expect_equal(result$exit_value, "fabricqueryr-job-success:unit")
  expect_s3_class(result$start_time, "POSIXct")
  expect_s3_class(result$end_time, "POSIXct")
  expect_equal(result$retry_after, 3)
  expect_true(result$visible)
  expect_match(called_url, "/notebooks/", fixed = TRUE)
  expect_match(called_url, "?beta=true", fixed = TRUE)
})

test_that("status treats a completed response with failure details as failed", {
  local_mocked_bindings(
    .fabric_job_request = function(...) {
      list(
        status_code = 200L,
        retry_after = NULL,
        body = list(
          id = "33333333-3333-3333-3333-333333333333",
          status = "Completed",
          rootActivityId = "77777777-7777-7777-7777-777777777777",
          failureReason = list(
            message = "FABRICQUERYR_INTENTIONAL_JOB_FAILURE"
          )
        )
      )
    }
  )

  result <- fabric_job_status(job_test_handle())

  expect_equal(result$status, "Failed")
  expect_match(
    .fabric_job_failure_text(result$failure_reason),
    "FABRICQUERYR_INTENTIONAL_JOB_FAILURE",
    fixed = TRUE
  )
})

test_that("notebook completion without an exit reconciles scheduler failure", {
  urls <- character()
  local_mocked_bindings(
    .fabric_job_request = function(
      method,
      url,
      ...,
      accepted_status = integer()
    ) {
      urls <<- c(urls, url)
      expect_equal(method, "GET")
      if (grepl("/notebooks/", url, fixed = TRUE)) {
        expect_equal(accepted_status, c(400L, 404L, 410L))
        return(list(
          status_code = 200L,
          retry_after = NULL,
          body = list(
            id = "33333333-3333-3333-3333-333333333333",
            status = "Completed"
          )
        ))
      }
      expect_equal(accepted_status, 404L)
      list(
        status_code = 200L,
        retry_after = NULL,
        body = list(
          id = "33333333-3333-3333-3333-333333333333",
          status = "Failed",
          rootActivityId = "77777777-7777-7777-7777-777777777777",
          failureReason = list(
            message = "FABRICQUERYR_INTENTIONAL_JOB_FAILURE"
          )
        )
      )
    }
  )

  condition <- rlang::catch_cnd(
    fabric_job_wait(
      job_test_handle(),
      poll_interval = 0,
      timeout = 1
    ),
    classes = "error"
  )

  expect_s3_class(condition, "fabric_job_failed")
  expect_equal(condition$job_status$status, "Failed")
  expect_match(
    .fabric_job_failure_text(condition$job_status$failure_reason),
    "FABRICQUERYR_INTENTIONAL_JOB_FAILURE",
    fixed = TRUE
  )
  expect_length(urls, 2L)
  expect_match(urls[[1L]], "/notebooks/", fixed = TRUE)
  expect_match(urls[[2L]], "/items/", fixed = TRUE)
})

test_that("core timestamps without an explicit UTC suffix are parsed as UTC", {
  parsed <- .fabric_job_time("2023-04-22T06:35:00.7812154")

  expect_s3_class(parsed, "POSIXct")
  expect_false(is.na(parsed))
  expect_equal(format(parsed, tz = "UTC"), "2023-04-22 06:35:00")
})

test_that("notebook status falls back to the core scheduler", {
  urls <- character()
  local_mocked_bindings(
    .fabric_job_request = function(
      method,
      url,
      ...,
      accepted_status = integer()
    ) {
      urls <<- c(urls, url)
      expect_equal(method, "GET")
      if (length(urls) == 1L) {
        expect_equal(accepted_status, c(400L, 404L, 410L))
        return(list(status_code = 410L, retry_after = 5, body = list()))
      }
      expect_length(accepted_status, 0L)
      list(
        status_code = 200L,
        retry_after = 3,
        body = list(id = "job", status = "InProgress")
      )
    }
  )

  result <- fabric_job_status(job_test_handle())

  expect_equal(result$status, "InProgress")
  expect_true(result$visible)
  expect_equal(result$retry_after, 3)
  expect_match(urls[[1L]], "/notebooks/", fixed = TRUE)
  expect_match(urls[[2L]], "/items/", fixed = TRUE)
})

test_that("status represents delays in both notebook job stores", {
  calls <- 0L
  local_mocked_bindings(
    .fabric_job_request = function(..., accepted_status = integer()) {
      calls <<- calls + 1L
      if (calls == 1L) {
        expect_equal(accepted_status, c(400L, 404L, 410L))
      } else {
        expect_equal(accepted_status, 404L)
      }
      list(status_code = 404L, retry_after = 5, body = list())
    }
  )

  result <- .fabric_job_get_status(
    .fabric_job_context(job_test_handle()),
    allow_not_found = TRUE
  )

  expect_equal(result$status, "NotStarted")
  expect_false(result$visible)
  expect_equal(result$retry_after, 5)
  expect_equal(calls, 2L)
})

test_that("wait honors Retry-After and returns a completed result", {
  responses <- list(
    list(
      status_code = 200L,
      retry_after = 4,
      body = list(id = "job", status = "InProgress")
    ),
    list(
      status_code = 200L,
      retry_after = NULL,
      body = list(
        id = "job",
        status = "Completed",
        properties = list(exitValue = "done")
      )
    )
  )
  index <- 0L
  elapsed <- 0
  sleeps <- numeric()
  local_mocked_bindings(
    .fabric_job_request = function(...) {
      index <<- index + 1L
      responses[[index]]
    }
  )

  result <- fabric_job_wait(
    job_test_handle(retry_after = 6),
    timeout = 30,
    .sleep = function(seconds) {
      sleeps <<- c(sleeps, seconds)
      elapsed <<- elapsed + seconds
    },
    .now = function() {
      as.POSIXct("2026-01-01", tz = "UTC") + elapsed
    }
  )

  expect_equal(result$status, "Completed")
  expect_equal(sleeps, c(6, 4))
  expect_equal(index, 2L)
})

test_that("wait tolerates visibility delays until its timeout", {
  polls <- 0L
  elapsed <- 0
  local_mocked_bindings(
    .fabric_job_request = function(
      method,
      url,
      ...,
      accepted_status = integer()
    ) {
      expect_equal(method, "GET")
      if (grepl("/notebooks/", url, fixed = TRUE)) {
        expect_equal(accepted_status, c(400L, 404L, 410L))
        return(list(
          status_code = 404L,
          retry_after = NULL,
          body = list()
        ))
      }
      expect_equal(accepted_status, 404L)
      polls <<- polls + 1L
      if (polls <= 15L) {
        return(list(
          status_code = 404L,
          retry_after = NULL,
          body = list()
        ))
      }
      list(
        status_code = 200L,
        retry_after = NULL,
        body = list(
          id = "job",
          status = "Completed",
          properties = list(exitValue = "done")
        )
      )
    }
  )

  result <- fabric_job_wait(
    job_test_handle(retry_after = NULL),
    poll_interval = 1,
    timeout = 30,
    .sleep = function(seconds) {
      elapsed <<- elapsed + seconds
    },
    .now = function() {
      as.POSIXct("2026-01-01", tz = "UTC") + elapsed
    }
  )

  expect_equal(result$status, "Completed")
  expect_equal(polls, 16L)
})

test_that("wait fails fast for statuses added by the service", {
  local_mocked_bindings(
    .fabric_job_request = function(...) {
      list(
        status_code = 200L,
        retry_after = NULL,
        body = list(id = "job", status = "Paused")
      )
    }
  )

  condition <- rlang::catch_cnd(
    fabric_job_wait(
      job_test_handle(),
      poll_interval = 0,
      timeout = 30
    ),
    classes = "error"
  )

  expect_s3_class(condition, "fabric_job_unknown_status")
  expect_s3_class(condition, "fabric_job_error")
  expect_equal(condition$job_status$status, "Paused")
  expect_match(condition$message, "Paused", fixed = TRUE)
})

test_that("failed, cancelled, and deduped jobs have distinct conditions", {
  cases <- c(
    Failed = "fabric_job_failed",
    Cancelled = "fabric_job_cancelled",
    Deduped = "fabric_job_deduped"
  )
  for (status in names(cases)) {
    local_mocked_bindings(
      .fabric_job_request = local({
        terminal <- status
        function(...) {
          list(
            status_code = 200L,
            retry_after = NULL,
            body = list(
              id = "job",
              status = terminal,
              rootActivityId = "activity",
              failureReason = list(message = paste(terminal, "reason"))
            )
          )
        }
      })
    )
    condition <- rlang::catch_cnd(
      fabric_job_wait(
        job_test_handle(),
        poll_interval = 0,
        timeout = 1
      ),
      classes = "error"
    )
    expect_s3_class(condition, unname(cases[[status]]))
    expect_s3_class(condition, "fabric_job_error")
    expect_match(condition$message, "activity", fixed = TRUE)
    expect_match(condition$message, paste(status, "reason"), fixed = TRUE)
    expect_equal(condition$job_status$status, status)
  }
})

test_that("terminal errors can be returned for inspection", {
  local_mocked_bindings(
    .fabric_job_request = function(...) {
      list(
        status_code = 200L,
        retry_after = NULL,
        body = list(id = "job", status = "Failed")
      )
    }
  )

  result <- fabric_job_wait(
    job_test_handle(),
    poll_interval = 0,
    timeout = 1,
    error_on_failure = FALSE
  )
  expect_equal(result$status, "Failed")
})

test_that("timeout can request cancellation and retains last status", {
  elapsed <- 0
  calls <- character()
  local_mocked_bindings(
    .fabric_job_request = function(method, url, ...) {
      calls <<- c(calls, paste(method, url))
      if (method == "POST") {
        return(list(status_code = 202L))
      }
      list(
        status_code = 200L,
        retry_after = NULL,
        body = list(id = "job", status = "InProgress")
      )
    }
  )

  condition <- rlang::catch_cnd(
    fabric_job_wait(
      job_test_handle(),
      poll_interval = 0.5,
      timeout = 1,
      cancel_on_timeout = TRUE,
      .sleep = function(seconds) {
        elapsed <<- elapsed + seconds
      },
      .now = function() {
        as.POSIXct("2026-01-01", tz = "UTC") + elapsed
      }
    ),
    classes = "error"
  )

  expect_s3_class(condition, "fabric_job_timeout")
  expect_true(any(grepl("/cancel$", calls)))
  expect_match(condition$message, "last status: InProgress", fixed = TRUE)
})

test_that("cancel uses the common scheduler route and is idempotent", {
  call <- NULL
  local_mocked_bindings(
    .fabric_job_request = function(
      method,
      url,
      credential,
      payload = NULL,
      idempotent = NULL,
      ...
    ) {
      call <<- list(
        method = method,
        url = url,
        payload = payload,
        idempotent = idempotent
      )
      list(status_code = 202L)
    }
  )

  expect_true(fabric_job_cancel(job_test_handle()))
  expect_equal(call$method, "POST")
  expect_match(
    call$url,
    paste0(
      "/items/11111111-1111-1111-1111-111111111111/",
      "jobs/instances/33333333-3333-3333-3333-333333333333/cancel"
    ),
    fixed = TRUE
  )
  expect_true(call$idempotent)
  expect_null(call$payload)
})

test_that("job payload validation rejects ambiguous and unsafe input", {
  expect_error(
    .fabric_job_parameters(list(Name = "one", name = "two")),
    "unique ignoring case"
  )
  expect_error(
    .fabric_job_parameters(list(many = c("one", "two"))),
    "one non-missing scalar"
  )
  expect_error(
    .fabric_job_parameters(
      list(id = "not-a-guid"),
      c(id = "Unsupported")
    ),
    "Unsupported Fabric parameter type"
  )
  expect_error(
    .fabric_job_parameters(
      list(id = "not-a-guid"),
      c(id = "Guid")
    ),
    "canonical GUID"
  )
  expect_error(
    .fabric_job_parameters(
      list(count = 1.5),
      c(count = "Integer")
    ),
    "whole 32-bit"
  )
  expect_error(
    .fabric_job_route("Other", "../unsafe"),
    "letters, numbers"
  )
  expect_equal(
    .fabric_job_execution_data(
      target = list(workspace_id = "workspace"),
      route = list(route = "core"),
      execution_data = list(arbitrary = TRUE),
      default_lakehouse = NULL,
      default_lakehouse_workspace = NULL,
      compute = NULL,
      session_tag = NULL
    ),
    list(arbitrary = TRUE)
  )
  expect_error(
    .fabric_job_validate_spark_definition(
      list(unbounded = "payload")
    ),
    "Unsupported SparkJobDefinition"
  )
  expect_error(
    .fabric_job_validate_notebook_compute(
      list(unbounded = "payload"),
      "Spark"
    ),
    "Unsupported Spark"
  )
  for (invalid_tag in list(
    "",
    NA_character_,
    c("one", "two"),
    42,
    "invalid-tag",
    "invalid tag"
  )) {
    expect_error(
      .fabric_job_validate_session_tag(invalid_tag, "session_tag"),
      "non-empty string|letters, numbers, and underscores"
    )
  }
})

test_that("print methods do not expose credentials", {
  job <- job_test_handle()
  job_text <- capture.output(print(job))
  instance <- .fabric_job_instance(
    list(id = job$id, status = "Completed"),
    .fabric_job_context(job),
    retry_after = NULL,
    visible = TRUE
  )
  instance_text <- capture.output(print(instance))

  expect_match(paste(job_text, collapse = "\n"), "fabric_job")
  expect_false(any(grepl("test-token", job_text, fixed = TRUE)))
  expect_match(paste(instance_text, collapse = "\n"), "Completed")
  expect_false(any(grepl("test-token", instance_text, fixed = TRUE)))
})
