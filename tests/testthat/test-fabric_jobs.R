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

test_that("job item-name lookup uses the workspace-specific endpoint", {
  lookup_url <- NULL
  run_url <- NULL
  workspace <- structure(
    list(
      id = "22222222-2222-2222-2222-222222222222",
      displayName = "Private workspace",
      apiEndpoint = "https://workspace.z13.api.fabric.microsoft.com"
    ),
    class = c("fabric_workspace", "list")
  )
  local_mocked_bindings(
    .httr2_collection = function(url, ...) {
      lookup_url <<- url
      list(list(
        id = "11111111-1111-1111-1111-111111111111",
        displayName = "Named pipeline",
        type = "DataPipeline"
      ))
    },
    .fabric_job_request = function(method, url, ...) {
      run_url <<- url
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
    "Named pipeline",
    workspace = workspace,
    token = "test-token"
  )

  expect_match(
    lookup_url,
    "https://workspace.z13.api.fabric.microsoft.com/v1/workspaces/",
    fixed = TRUE
  )
  expect_match(
    run_url,
    paste0(
      "https://workspace.z13.api.fabric.microsoft.com/v1/workspaces/",
      "22222222-2222-2222-2222-222222222222/dataPipelines/",
      "11111111-1111-1111-1111-111111111111/jobs/execute/instances"
    ),
    fixed = TRUE
  )
})

test_that("notebook run uses the released workload-specific route", {
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
    paste0(
      "/notebooks/11111111-1111-1111-1111-111111111111/",
      "jobs/execute/instances?beta=false"
    ),
    fixed = TRUE
  )
  expect_false(grepl("/items/", call$url, fixed = TRUE))
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

test_that("data pipeline run uses current typed path without a JSON payload", {
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

  expect_equal(job$job_type, "Execute")
  expect_equal(job$route, "data_pipeline")
  expect_match(
    call$url,
    paste0(
      "/dataPipelines/11111111-1111-1111-1111-111111111111/",
      "jobs/execute/instances"
    ),
    fixed = TRUE
  )
  expect_null(call$payload)
  expect_false(call$idempotent)
})

test_that("data pipeline run retains explicit legacy core contract", {
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

  job <- fabric_job_run(
    job_test_item("DataPipeline"),
    job_type = "Pipeline",
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
})

test_that("job types reject URI dot segments", {
  for (job_type in c(".", "..")) {
    expect_error(
      .fabric_job_route("CustomItem", job_type),
      "safe path segment",
      fixed = TRUE
    )
  }
  expect_equal(
    .fabric_job_route("CustomItem", "Refresh.v2")$job_type,
    "Refresh.v2"
  )
})

test_that("job submission rejects missing or malformed Location headers", {
  response <- list(status_code = 202L, location = NULL, retry_after = NULL)
  local_mocked_bindings(
    .fabric_job_request = function(...) response
  )

  missing <- expect_error(
    fabric_job_run(
      job_test_item(),
      token = "test-token",
      api_base = "https://api.fabric.test/v1"
    ),
    class = "fabric_job_protocol_error"
  )
  expect_match(conditionMessage(missing), "Location header", fixed = TRUE)

  response$location <- "/jobs/instances/not-a-guid"
  malformed <- expect_error(
    fabric_job_run(
      job_test_item(),
      token = "test-token",
      api_base = "https://api.fabric.test/v1"
    ),
    class = "fabric_job_protocol_error"
  )
  expect_match(
    conditionMessage(malformed),
    "valid job instance ID",
    fixed = TRUE
  )
})

test_that("parameterized jobs recover collection Location instance IDs", {
  history_url <- NULL
  local_mocked_bindings(
    .fabric_job_request = function(...) {
      list(
        status_code = 202L,
        location = paste0(
          "https://api.fabric.test/v1/workspaces/workspace/items/item/",
          "jobs/instances?jobType=RunNotebook"
        ),
        retry_after = 5,
        body = list()
      )
    },
    .httr2_collection = function(url, credential, audience, ...) {
      history_url <<- url
      expect_s3_class(credential, "fabric_credential")
      expect_identical(audience, .fabric_audience$fabric)
      list(
        list(
          id = "44444444-4444-4444-4444-444444444444",
          itemId = "11111111-1111-1111-1111-111111111111",
          jobType = "RunNotebook",
          invokeType = "Manual",
          startTimeUtc = "2020-01-01T00:00:00Z"
        ),
        list(
          id = "33333333-3333-3333-3333-333333333333",
          itemId = "11111111-1111-1111-1111-111111111111",
          jobType = "RunNotebook",
          invokeType = "Manual",
          startTimeUtc = format(
            Sys.time(),
            "%Y-%m-%dT%H:%M:%SZ",
            tz = "UTC"
          )
        )
      )
    }
  )

  job <- fabric_job_run(
    job_test_item(),
    parameters = list(label = "unit"),
    token = "test-token",
    api_base = "https://api.fabric.test/v1"
  )

  expect_identical(job$id, "33333333-3333-3333-3333-333333333333")
  expect_match(
    history_url,
    paste0(
      "/workspaces/22222222-2222-2222-2222-222222222222/",
      "items/11111111-1111-1111-1111-111111111111/jobs/instances"
    ),
    fixed = TRUE
  )
})

test_that("parameterized job recovery refuses ambiguous history", {
  local_mocked_bindings(
    .fabric_job_request = function(...) {
      list(
        status_code = 202L,
        location = "/jobs/instances?jobType=RunNotebook",
        retry_after = NULL,
        body = list()
      )
    },
    .httr2_collection = function(...) {
      started <- format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
      list(
        list(
          id = "33333333-3333-3333-3333-333333333333",
          jobType = "RunNotebook",
          invokeType = "Manual",
          startTimeUtc = started
        ),
        list(
          id = "44444444-4444-4444-4444-444444444444",
          jobType = "RunNotebook",
          invokeType = "Manual",
          startTimeUtc = started
        )
      )
    }
  )

  expect_error(
    fabric_job_run(
      job_test_item(),
      parameters = list(label = "unit"),
      token = "test-token",
      api_base = "https://api.fabric.test/v1"
    ),
    "cannot be identified safely",
    class = "fabric_job_protocol_error"
  )
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

test_that("job submissions accept successful responses without bodies", {
  local_mocked_bindings(
    .httr2_perform = function(req, ...) {
      httr2::response(
        status_code = 202L,
        headers = list(
          Location = paste0(
            "https://api.fabric.test/v1/jobs/instances/",
            "33333333-3333-3333-3333-333333333333"
          ),
          `Retry-After` = "60"
        )
      )
    }
  )

  job <- fabric_job_run(
    job_test_item(),
    token = "test-token",
    api_base = "https://api.fabric.test/v1"
  )

  expect_s3_class(job, "fabric_job")
  expect_identical(job$id, "33333333-3333-3333-3333-333333333333")
  expect_identical(job$retry_after, 60)
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
        computeConfiguration = list(jars = "abfss://account/library.jar"),
        customExtension = list(values = I("only"))
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
  expect_length(parsed$executionData$customExtension$values, 1L)
  expect_length(parsed$parameters, 1L)
  expect_match(
    body,
    '"additionalLibraryUris":\\["abfss://account/library.zip"\\]'
  )
  expect_match(body, '"jars":\\["abfss://account/library.jar"\\]')
  expect_match(body, '"values":\\["only"\\]')
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

test_that("typed DataPipeline jobs reject undocumented request bodies", {
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

  expect_error(
    fabric_job_run(
      job_test_item("DataPipeline"),
      execution_data = list(executeOption = "ApplyChangesIfNeeded"),
      token = "test-token",
      api_base = "https://api.fabric.test/v1"
    ),
    "do not support `parameters` or `execution_data`",
    fixed = TRUE
  )
  expect_error(
    fabric_job_run(
      job_test_item("DataPipeline"),
      parameters = list(mode = "test"),
      token = "test-token",
      api_base = "https://api.fabric.test/v1"
    ),
    "do not support `parameters` or `execution_data`",
    fixed = TRUE
  )
  expect_null(call)

  fabric_job_run(
    job_test_item("DataPipeline"),
    job_type = "Pipeline",
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

  result <- fabric_job_status(job_test_handle(), notebook_details = TRUE)

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

test_that("job handles honor explicit tenant and client authentication", {
  captured_credential <- NULL
  local_mocked_bindings(
    .fabric_job_request = function(
      method,
      url,
      credential,
      ...
    ) {
      captured_credential <<- credential
      list(
        status_code = 200L,
        retry_after = NULL,
        body = list(
          id = "33333333-3333-3333-3333-333333333333",
          status = "Completed"
        )
      )
    }
  )
  job <- job_test_handle(item_type = "DataPipeline")

  fabric_job_status(job)
  expect_identical(captured_credential, .fabric_job_credential(job))

  fabric_job_status(
    job,
    tenant_id = "other-tenant",
    client_id = "other-client"
  )
  expect_identical(captured_credential$type, "AzureAuth")
  provider_environment <- environment(captured_credential$provider)
  expect_identical(
    get("tenant_id", envir = provider_environment),
    "other-tenant"
  )
  expect_identical(
    get("client_id", envir = provider_environment),
    "other-client"
  )
})

test_that("job handles do not serialize their stored bearer credential", {
  secret <- "sentinel-job-bearer-secret"
  reference <- .fabric_job_credential_reference(
    fabric_credential(token = secret)
  )
  job <- job_test_handle(item_type = "DataPipeline")
  job$credential <- reference$reference
  job$.credential_key <- reference$key

  expect_identical(
    fabric_get_token(.fabric_job_credential(job), "audience"),
    secret
  )
  serialized <- serialize(job, NULL, ascii = TRUE)
  expect_false(grepl(secret, rawToChar(serialized), fixed = TRUE))

  restored <- unserialize(serialized)
  expect_error(
    .fabric_job_credential(restored),
    "no longer has an in-process credential",
    class = "fabric_job_credential_error"
  )
})

test_that("status reconstructs context from a raw job instance ID", {
  called_url <- NULL
  local_mocked_bindings(
    .fabric_job_request = function(method, url, ...) {
      called_url <<- url
      expect_equal(method, "GET")
      list(
        status_code = 200L,
        retry_after = NULL,
        body = list(
          id = "33333333-3333-3333-3333-333333333333",
          status = "Completed"
        )
      )
    }
  )

  result <- fabric_job_status(
    job_instance_id = "33333333-3333-3333-3333-333333333333",
    workspace = "22222222-2222-2222-2222-222222222222",
    item = "11111111-1111-1111-1111-111111111111",
    item_type = "DataPipeline",
    job_type = "Pipeline",
    token = "test-token",
    api_base = "https://api.fabric.test/v1"
  )

  expect_s3_class(result, "fabric_job_instance")
  expect_equal(result$status, "Completed")
  expect_s3_class(result$job, "fabric_job")
  expect_match(
    called_url,
    paste0(
      "/workspaces/22222222-2222-2222-2222-222222222222/",
      "items/11111111-1111-1111-1111-111111111111/",
      "jobs/instances/33333333-3333-3333-3333-333333333333"
    ),
    fixed = TRUE
  )
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
      timeout = 1,
      notebook_details = TRUE
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

test_that("beta notebook status falls back to the Core scheduler", {
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

  result <- fabric_job_status(
    job_test_handle(),
    notebook_details = TRUE
  )

  expect_equal(result$status, "InProgress")
  expect_true(result$visible)
  expect_equal(result$retry_after, 3)
  expect_match(urls[[1L]], "/notebooks/", fixed = TRUE)
  expect_match(urls[[2L]], "/items/", fixed = TRUE)
})

test_that("notebook status defaults to the stable Core scheduler", {
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
      expect_length(accepted_status, 0L)
      list(
        status_code = 200L,
        retry_after = NULL,
        body = list(
          id = "33333333-3333-3333-3333-333333333333",
          status = "Completed"
        )
      )
    }
  )

  result <- fabric_job_status(
    job_test_handle(),
    respect_retry_after = FALSE
  )

  expect_equal(result$status, "Completed")
  expect_null(result$exit_value)
  expect_length(urls, 1L)
  expect_match(urls[[1L]], "/items/", fixed = TRUE)
  expect_false(grepl("beta=", urls[[1L]], fixed = TRUE))
})

test_that("notebook detail selection validates before status requests", {
  calls <- 0L
  local_mocked_bindings(
    .fabric_job_request = function(...) {
      calls <<- calls + 1L
      stop("must not request")
    }
  )

  expect_error(
    fabric_job_status(
      job_test_handle(),
      notebook_details = NA,
      respect_retry_after = FALSE
    ),
    "`notebook_details` must be TRUE or FALSE",
    fixed = TRUE
  )
  expect_error(
    fabric_job_wait(job_test_handle(), notebook_details = "beta"),
    "`notebook_details` must be TRUE or FALSE",
    fixed = TRUE
  )
  expect_equal(calls, 0L)
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
    allow_not_found = TRUE,
    notebook_details = TRUE
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

test_that("wait only sleeps the unelapsed submission Retry-After", {
  requested <- 0L
  elapsed <- 0
  submitted <- as.POSIXct("2026-01-01 00:00:00", tz = "UTC")
  job <- job_test_handle(retry_after = 60)
  job$submitted_at <- submitted
  job$next_poll_at <- submitted + 60
  local_mocked_bindings(
    .fabric_job_request = function(...) {
      requested <<- requested + 1L
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
    job,
    timeout = 30,
    .sleep = function(seconds) {
      elapsed <<- elapsed + seconds
    },
    .now = function() submitted + 50 + elapsed
  )

  expect_equal(result$status, "Completed")
  expect_equal(elapsed, 10)
  expect_equal(requested, 1L)
})

test_that("manual status honors the submission Retry-After delay", {
  requested <- FALSE
  slept <- numeric()
  submitted <- as.POSIXct("2026-01-01 00:00:00", tz = "UTC")
  job <- job_test_handle(retry_after = 7)
  job$submitted_at <- submitted
  job$next_poll_at <- submitted + 7
  local_mocked_bindings(
    .fabric_job_get_status = function(...) {
      requested <<- TRUE
      list(status = "InProgress")
    }
  )

  result <- fabric_job_status(
    job,
    .now = function() submitted + 2,
    .sleep = function(seconds) {
      slept <<- c(slept, seconds)
      invisible(NULL)
    }
  )
  expect_true(requested)
  expect_equal(result$status, "InProgress")
  expect_equal(slept, 5)

  slept <- numeric()
  fabric_job_status(
    job,
    respect_retry_after = FALSE,
    .now = function() submitted,
    .sleep = function(seconds) {
      slept <<- c(slept, seconds)
      invisible(NULL)
    }
  )
  expect_length(slept, 0L)
})

test_that("manual status preserves the latest response Retry-After", {
  responses <- list(
    list(
      status_code = 200L,
      retry_after = 20,
      body = list(id = "job", status = "InProgress")
    ),
    list(
      status_code = 200L,
      retry_after = NULL,
      body = list(id = "job", status = "InProgress")
    )
  )
  requested <- 0L
  slept <- numeric()
  now <- as.POSIXct("2026-01-01 00:00:00", tz = "UTC")
  local_mocked_bindings(
    .fabric_job_request = function(...) {
      requested <<- requested + 1L
      response <- responses[[1L]]
      responses <<- responses[-1L]
      response
    }
  )
  job <- job_test_handle(item_type = "DataPipeline")

  first <- fabric_job_status(
    job,
    respect_retry_after = FALSE,
    .now = function() now
  )
  second <- fabric_job_status(
    first,
    .now = function() now + 5,
    .sleep = function(seconds) {
      slept <<- c(slept, seconds)
    }
  )

  expect_equal(first$next_poll_at, now + 20)
  expect_equal(second$status, "InProgress")
  expect_equal(slept, 15)
  expect_equal(requested, 2L)
})

test_that("wait retains a positive polling floor", {
  responses <- list(
    list(
      status_code = 200L,
      retry_after = NULL,
      body = list(id = "job", status = "InProgress")
    ),
    list(
      status_code = 200L,
      retry_after = NULL,
      body = list(id = "job", status = "Completed")
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
    job_test_handle(item_type = "DataPipeline", retry_after = NULL),
    poll_interval = 0,
    timeout = 1,
    .sleep = function(seconds) {
      sleeps <<- c(sleeps, seconds)
      elapsed <<- elapsed + seconds
    },
    .now = function() {
      as.POSIXct("2026-01-01", tz = "UTC") + elapsed
    }
  )

  expect_equal(result$status, "Completed")
  expect_equal(sleeps, c(.fabric_job_poll_floor, .fabric_job_poll_floor))
  expect_true(all(sleeps > 0))
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
      expect_false(grepl("/notebooks/", url, fixed = TRUE))
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
  expect_true(condition$cancel_accepted)
  expect_null(condition$cancel_error)
})

test_that("wait conditions retain remote cancellation failures", {
  local_mocked_bindings(
    fabric_job_cancel = function(...) {
      rlang::abort(
        "Fabric rejected cancellation",
        class = "fabric_job_cancel_error"
      )
    }
  )

  timed_out <- rlang::catch_cnd(
    fabric_job_wait(
      job_test_handle(),
      timeout = 0,
      cancel_on_timeout = TRUE
    ),
    classes = "error"
  )
  expect_s3_class(timed_out, "fabric_job_timeout")
  expect_false(timed_out$cancel_accepted)
  expect_s3_class(timed_out$cancel_error, "fabric_job_cancel_error")

  cancelled <- rlang::catch_cnd(
    fabric_job_wait(
      job_test_handle(),
      timeout = 1,
      cancel = function() TRUE
    ),
    classes = "error"
  )
  expect_s3_class(cancelled, "fabric_job_cancelled_by_caller")
  expect_false(cancelled$cancel_accepted)
  expect_match(
    conditionMessage(cancelled$cancel_error),
    "Fabric rejected cancellation",
    fixed = TRUE
  )
})

test_that("cancel uses the scheduler route without automatic POST retries", {
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
  expect_false(call$idempotent)
  expect_null(call$payload)
})

test_that("cancel reconciles a lost response against terminal status", {
  calls <- 0L
  local_mocked_bindings(
    .fabric_job_request = function(method, ...) {
      calls <<- calls + 1L
      if (identical(method, "POST")) {
        stop("connection closed after request")
      }
      list(
        status_code = 200L,
        retry_after = NULL,
        body = list(id = "job", status = "Cancelled")
      )
    }
  )

  expect_true(fabric_job_cancel(job_test_handle(item_type = "DataPipeline")))
  expect_equal(calls, 2L)
})

test_that("status does not turn ItemNotFound into NotStarted", {
  local_mocked_bindings(
    .fabric_job_request = function(...) {
      list(
        status_code = 404L,
        retry_after = NULL,
        body = list(
          errorCode = "ItemNotFound",
          message = "The requested item was not found"
        )
      )
    }
  )

  condition <- rlang::catch_cnd(
    .fabric_job_get_status(
      .fabric_job_context(job_test_handle(item_type = "DataPipeline")),
      allow_not_found = TRUE
    ),
    classes = "error"
  )

  expect_s3_class(condition, "fabric_job_status_error")
  expect_s3_class(condition, "fabric_job_error")
  expect_equal(condition$error_code, "ItemNotFound")
  expect_match(condition$message, "HTTP 404", fixed = TRUE)
})

test_that("cancel reconciles JobAlreadyCompleted but preserves other errors", {
  response_code <- "JobAlreadyCompleted"
  calls <- 0L
  local_mocked_bindings(
    .fabric_job_request = function(method, ...) {
      calls <<- calls + 1L
      if (identical(method, "POST")) {
        return(list(
          status_code = 400L,
          body = list(errorCode = response_code)
        ))
      }
      list(
        status_code = 200L,
        retry_after = NULL,
        body = list(id = "job", status = "Completed")
      )
    }
  )

  job <- job_test_handle(item_type = "DataPipeline")
  expect_true(fabric_job_cancel(job))
  expect_equal(calls, 2L)

  response_code <- "ItemNotFound"
  condition <- rlang::catch_cnd(fabric_job_cancel(job), classes = "error")
  expect_s3_class(condition, "fabric_job_cancel_error")
  expect_equal(condition$error_code, "ItemNotFound")
  expect_match(condition$message, "HTTP 400", fixed = TRUE)
  expect_equal(calls, 3L)
})

test_that("cancel rethrows ambiguous failures while the job is active", {
  local_mocked_bindings(
    .fabric_job_request = function(method, ...) {
      if (identical(method, "POST")) {
        stop("connection closed after request")
      }
      list(
        status_code = 200L,
        retry_after = NULL,
        body = list(id = "job", status = "InProgress")
      )
    }
  )

  expect_error(
    fabric_job_cancel(job_test_handle(item_type = "DataPipeline")),
    "connection closed after request",
    fixed = TRUE
  )
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
  integer_extrema <- .fabric_job_parameters(
    list(minimum = -2147483648, maximum = 2147483647),
    c(minimum = "Integer", maximum = "Integer")
  )
  expect_identical(integer_extrema[[1L]]$value, -2147483648)
  expect_identical(integer_extrema[[2L]]$value, 2147483647L)
  expect_match(
    jsonlite::toJSON(integer_extrema, auto_unbox = TRUE),
    '"value":-2147483648',
    fixed = TRUE
  )
  for (type in c("Integer", "Number", "Text", "Boolean", "Guid")) {
    expect_error(
      .fabric_job_parameters(
        list(run_date = as.Date("2026-08-07")),
        c(run_date = type)
      ),
      "use type DateTime or Automatic",
      fixed = TRUE,
      info = type
    )
  }
  automatic_date <- .fabric_job_parameters(
    list(run_date = as.Date("2026-08-07")),
    c(run_date = "Automatic")
  )[[1L]]
  expect_equal(automatic_date$type, "Automatic")
  expect_equal(automatic_date$value, "2026-08-07T00:00:00Z")
  posix_parameter <- .fabric_job_parameters(list(
    list(
      name = "started_at",
      value = as.POSIXct("2026-08-07 10:15:30", tz = "Europe/Amsterdam"),
      type = "DateTime"
    )
  ))[[1L]]
  expect_equal(posix_parameter$value, "2026-08-07T08:15:30Z")
  length.legacy_POSIXlt <- function(x) 9L
  legacy_posixlt <- as.POSIXlt(
    "2026-08-07 10:15:30",
    tz = "Europe/Amsterdam"
  )
  class(legacy_posixlt) <- c("legacy_POSIXlt", class(legacy_posixlt))
  expect_equal(length(legacy_posixlt), 9L)
  legacy_parameter <- .fabric_job_parameters(list(
    started_at = legacy_posixlt
  ))[[1L]]
  expect_equal(legacy_parameter$type, "DateTime")
  expect_equal(legacy_parameter$value, "2026-08-07T08:15:30Z")
  expect_error(
    .fabric_job_parameters(list(
      started_at = as.POSIXlt(
        c("2026-08-07 10:15:30", "2026-08-08 10:15:30"),
        tz = "Europe/Amsterdam"
      )
    )),
    "one non-missing scalar",
    fixed = TRUE
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
    .fabric_job_validate_spark_definition(
      list(executableFile = "https://example.test/job.py")
    ),
    "executableFile must be an abfss:// URI",
    fixed = TRUE
  )
  expect_error(
    .fabric_job_validate_spark_definition(
      list(additionalLibraryUris = "https://example.test/library.jar")
    ),
    "additionalLibraryUris must be an abfss:// URI",
    fixed = TRUE
  )
  expect_silent(.fabric_job_validate_spark_definition(list()))
  expect_error(
    .fabric_job_validate_item_reference(
      list(
        referenceType = "ById",
        itemId = "not-a-guid",
        workspaceId = "22222222-2222-2222-2222-222222222222"
      ),
      "defaultLakehouse"
    ),
    "itemId.*canonical GUID format"
  )
  expect_error(
    .fabric_job_validate_item_reference(
      list(
        referenceType = "ById",
        itemId = "11111111-1111-1111-1111-111111111111",
        workspaceId = "not-a-guid"
      ),
      "attachedEnvironment"
    ),
    "workspaceId.*canonical GUID format"
  )
  expect_error(
    .fabric_job_validate_notebook_compute(
      list(unbounded = "payload"),
      "Spark"
    ),
    "Unsupported Spark"
  )
  for (compute in list(NA_character_, c("Spark", "Jupyter"))) {
    expect_error(
      .fabric_job_execution_data(
        target = list(workspace_id = "workspace"),
        route = list(route = "notebook"),
        execution_data = list(compute = compute),
        default_lakehouse = NULL,
        default_lakehouse_workspace = NULL,
        compute = NULL,
        session_tag = NULL
      ),
      "must be one non-empty string",
      class = "fabric_job_validation_error"
    )
  }
  expect_error(
    .fabric_job_validate_notebook_compute(
      list(highConcurrencyModeOptions = list(sessionTag = "shared")),
      "Spark"
    ),
    "enabled` is required",
    fixed = TRUE
  )
  expect_silent(
    .fabric_job_validate_notebook_compute(
      list(
        highConcurrencyModeOptions = list(
          enabled = TRUE,
          sessionTag = "team A / run-1.0"
        )
      ),
      "Spark"
    )
  )
  expect_silent(.fabric_job_validate_notebook_compute(
    list(
      name = "validated-session",
      driverMemory = "28g",
      driverCores = 4L,
      executorMemory = "56g",
      executorCores = 8L,
      numExecutors = 2L,
      jars = "abfss://container@account/path/library.jar",
      sparkProperties = list(list(key = "spark.key", value = "value")),
      instancePool = list(name = "pool", type = "Workspace"),
      mountPoints = list(list(
        source = "abfss://container@account/path",
        mountPointPath = "/mnt/data"
      ))
    ),
    "Spark"
  ))
  expect_error(
    .fabric_job_validate_notebook_compute(
      list(driverCores = "banana"),
      "Spark"
    ),
    "Spark driverCores must be one of"
  )
  expect_error(
    .fabric_job_validate_notebook_compute(list(driverMemory = "30g"), "Spark"),
    "Spark driverMemory must be one of"
  )
  expect_error(
    .fabric_job_validate_notebook_compute(list(numExecutors = 0), "Spark"),
    "positive whole number"
  )
  expect_error(
    .fabric_job_validate_notebook_compute(
      list(jars = "https://example.test/library.jar"),
      "Spark"
    ),
    "abfss:// URI",
    fixed = TRUE
  )
  expect_error(
    .fabric_job_validate_notebook_compute(
      list(mountPoints = list(list(source = "abfss://account/path"))),
      "Spark"
    ),
    "source and mountPointPath"
  )
  expect_error(
    .fabric_job_validate_notebook_compute(
      list(sparkProperties = list(list(key = "spark.key"))),
      "Spark"
    ),
    "key and value"
  )
  expect_error(
    .fabric_job_validate_notebook_compute(
      list(instancePool = list(type = "Workspace")),
      "Spark"
    ),
    "needs an id or name"
  )
  expect_error(
    .fabric_job_validate_notebook_compute(list(numCores = -1), "Jupyter"),
    "Jupyter numCores must be one of"
  )
  for (invalid_tag in list(
    "",
    NA_character_,
    c("one", "two"),
    42
  )) {
    expect_error(
      .fabric_job_nonempty(invalid_tag, "session_tag"),
      "non-empty string"
    )
  }
  expect_silent(
    .fabric_job_nonempty("team A / run-1.0", "session_tag")
  )
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
