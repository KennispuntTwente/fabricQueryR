test_that("job history follows Fabric pagination and returns refreshable records", {
  calls <- character()
  pages <- list(
    list(
      value = list(list(
        id = "33333333-3333-3333-3333-333333333333",
        itemId = "11111111-1111-1111-1111-111111111111",
        jobType = "RunNotebook",
        invokeType = "Scheduled",
        status = "Completed",
        startTimeUtc = "2026-08-13T10:00:00Z",
        endTimeUtc = "2026-08-13T10:01:00Z"
      )),
      continuationToken = "next page"
    ),
    list(
      value = list(list(
        id = "55555555-5555-5555-5555-555555555555",
        itemId = "11111111-1111-1111-1111-111111111111",
        jobType = "FutureNotebookJob",
        invokeType = "FutureInvocation",
        status = "FutureStatus"
      ))
    )
  )
  local_mocked_bindings(
    .httr2_json = function(req, ...) {
      calls <<- c(calls, req$url)
      pages[[length(calls)]]
    }
  )

  history <- fabric_job_instances(
    scheduler_test_item(),
    token = "test-token",
    api_base = "https://api.fabric.microsoft.com/v1"
  )

  expect_s3_class(history, "fabric_job_instance_list")
  expect_length(history, 2L)
  expect_s3_class(history[[1L]], "fabric_job_instance")
  expect_equal(history[[1L]]$invoke_type, "Scheduled")
  expect_equal(history[[2L]]$invoke_type, "FutureInvocation")
  expect_equal(history[[2L]]$status, "FutureStatus")
  expect_match(calls[[1L]], "/jobs/instances$", perl = TRUE)
  expect_match(calls[[2L]], "continuationToken=next%20page", fixed = TRUE)

  local_mocked_bindings(
    .fabric_job_request = function(...) {
      list(
        status_code = 200L,
        retry_after = NULL,
        body = list(
          id = history[[1L]]$id,
          status = "Completed",
          jobType = "RunNotebook"
        )
      )
    }
  )
  refreshed <- fabric_job_status(history[[1L]], respect_retry_after = FALSE)
  expect_s3_class(refreshed, "fabric_job_instance")
  expect_equal(refreshed$id, history[[1L]]$id)

  waited <- fabric_job_wait(
    history[[1L]],
    poll_interval = 0,
    timeout = 1,
    .sleep = function(seconds) invisible(seconds),
    .now = function() as.POSIXct("2026-08-24", tz = "UTC")
  )
  expect_s3_class(waited, "fabric_job_instance")
  expect_equal(waited$id, history[[1L]]$id)
  expect_equal(waited$status, "Completed")
})

test_that("schedule constructors cover every documented configuration", {
  default_cron <- fabric_job_schedule_config(
    start_time = "2026-10-01T00:00:00Z",
    end_time = "2027-10-01T00:00:00Z",
    interval = 30L
  )
  cron <- scheduler_test_configuration("Cron")
  daily <- scheduler_test_configuration("Daily")
  weekly <- scheduler_test_configuration("Weekly")
  monthly <- scheduler_test_configuration("Monthly")
  ordinal <- fabric_job_schedule_config(
    "Monthly",
    start_time = "2026-10-01T00:00:00Z",
    end_time = "2027-10-01T00:00:00Z",
    times = c("08:00", "17:30"),
    recurrence = 1L,
    week_index = "second",
    weekday = "Friday"
  )

  expect_equal(default_cron$type, "Cron")
  expect_s3_class(cron, "fabric_job_schedule_config")
  expect_identical(cron$interval, 15L)
  expect_equal(daily$times, "09:30")
  expect_equal(weekly$weekdays, c("Monday", "Thursday"))
  expect_equal(monthly$occurrence$occurrenceType, "DayOfMonth")
  expect_identical(monthly$occurrence$dayOfMonth, 15L)
  expect_equal(ordinal$occurrence$occurrenceType, "OrdinalWeekday")
  expect_equal(ordinal$occurrence$weekIndex, "Second")
  expect_equal(ordinal$occurrence$weekday, "Friday")
})

test_that("schedule boundaries are UTC and independent of the process time zone", {
  old_timezone <- Sys.getenv("TZ", unset = NA_character_)
  on.exit({
    if (is.na(old_timezone)) {
      Sys.unsetenv("TZ")
    } else {
      Sys.setenv(TZ = old_timezone)
    }
  })
  Sys.setenv(TZ = "Pacific/Auckland")

  configuration <- fabric_job_schedule_config(
    "Daily",
    start_time = "2026-03-29T01:30:00+01:00",
    end_time = "2026-10-25T02:30:00+02:00",
    time_zone = "W. Europe Standard Time",
    times = "02:30"
  )

  expect_equal(configuration$startDateTime, "2026-03-29T00:30:00Z")
  expect_equal(configuration$endDateTime, "2026-10-25T00:30:00Z")
  expect_equal(configuration$localTimeZoneId, "W. Europe Standard Time")
})

test_that("known schedule inputs are validated before requests", {
  expect_error(
    fabric_job_schedule_config(
      "Daily",
      start_time = "2026-01-01T00:00:00",
      end_time = "2027-01-01T00:00:00Z",
      times = "09:00"
    ),
    "explicit offset"
  )
  expect_error(
    fabric_job_schedule_config(
      "Daily",
      start_time = "2027-01-01T00:00:00Z",
      end_time = "2026-01-01T00:00:00Z",
      times = "09:00"
    ),
    "later than"
  )
  expect_error(
    fabric_job_schedule_config(
      "Weekly",
      start_time = "2026-01-01T00:00:00Z",
      end_time = "2027-01-01T00:00:00Z",
      times = "24:00",
      weekdays = "Monday"
    ),
    "HH:MM"
  )
  expect_error(
    fabric_job_schedule_config(
      "Weekly",
      start_time = "2026-01-01T00:00:00Z",
      end_time = "2027-01-01T00:00:00Z",
      times = "09:00",
      weekdays = c("Monday", "monday")
    ),
    "unique values"
  )
  expect_error(
    fabric_job_schedule_config(
      "Monthly",
      start_time = "2026-01-01T00:00:00Z",
      end_time = "2027-01-01T00:00:00Z",
      times = "09:00",
      recurrence = 13L,
      day_of_month = 1L
    ),
    "1 through 12"
  )
})

test_that("schedule constructors reject arguments from other known shapes", {
  common <- list(
    start_time = "2026-01-01T00:00:00Z",
    end_time = "2027-01-01T00:00:00Z"
  )
  incompatible <- list(
    Cron = list(interval = 15L, times = "09:00"),
    Daily = list(times = "09:00", interval = 15L),
    Weekly = list(
      times = "09:00",
      weekdays = "Monday",
      recurrence = 1L
    ),
    Monthly = list(
      times = "09:00",
      recurrence = 1L,
      day_of_month = 1L,
      weekdays = "Monday"
    )
  )

  for (type in names(incompatible)) {
    expect_error(
      do.call(
        fabric_job_schedule_config,
        c(list(type = type), common, incompatible[[type]])
      ),
      paste0(type, " schedules do not use"),
      class = "fabric_job_validation_error",
      info = type
    )
  }
})

test_that("known raw schedule shapes reject cross-type fields", {
  daily <- unclass(scheduler_test_configuration("Daily"))
  daily$interval <- 15L
  expect_error(
    .fabric_job_schedule_configuration(daily),
    "Daily schedule configuration contains incompatible fields: `interval`",
    class = "fabric_job_validation_error"
  )

  monthly <- unclass(scheduler_test_configuration("Monthly"))
  monthly$occurrence$weekIndex <- "First"
  monthly$occurrence$weekday <- "Monday"
  expect_error(
    .fabric_job_schedule_configuration(monthly),
    "DayOfMonth occurrence contains incompatible fields",
    class = "fabric_job_validation_error"
  )

  ordinal <- unclass(fabric_job_schedule_config(
    "Monthly",
    start_time = "2026-01-01T00:00:00Z",
    end_time = "2027-01-01T00:00:00Z",
    times = "09:00",
    recurrence = 1L,
    week_index = "First",
    weekday = "Monday"
  ))
  ordinal$occurrence$dayOfMonth <- 1L
  expect_error(
    .fabric_job_schedule_configuration(ordinal),
    "OrdinalWeekday occurrence contains incompatible fields: `dayOfMonth`",
    class = "fabric_job_validation_error"
  )
})

test_that("known schedule shapes preserve unrecognized future fields", {
  daily <- unclass(scheduler_test_configuration("Daily"))
  daily$futureServiceField <- list(version = 2L)

  normalized <- .fabric_job_schedule_configuration(daily)

  expect_identical(normalized$futureServiceField, list(version = 2L))
})

test_that("schedule listing normalizes common fields and preserves future data", {
  future <- list(
    id = "33333333-3333-3333-3333-333333333333",
    enabled = FALSE,
    autoDisabled = TRUE,
    configuration = list(
      type = "EventDriven",
      eventSource = list(kind = "FutureSource")
    ),
    futureProperty = list(kept = TRUE)
  )
  local_mocked_bindings(
    .httr2_collection = function(url, ...) {
      expect_match(url, "/jobs/RunNotebook/schedules$", perl = TRUE)
      list(future)
    }
  )

  schedules <- fabric_job_schedules(
    scheduler_test_item(),
    token = "test-token"
  )

  expect_s3_class(schedules, "fabric_job_schedule_list")
  expect_s3_class(schedules[[1L]], "fabric_job_schedule")
  expect_equal(schedules[[1L]]$type, "EventDriven")
  expect_true(schedules[[1L]]$auto_disabled)
  expect_equal(schedules[[1L]]$state, "AutoDisabled")
  expect_true(schedules[[1L]]$raw$futureProperty$kept)
})

test_that("schedule responses require a usable configuration object", {
  context <- list(
    workspace_id = "22222222-2222-2222-2222-222222222222",
    item_id = "11111111-1111-1111-1111-111111111111",
    item_type = "Notebook",
    job_type = "RunNotebook"
  )
  malformed <- list(
    list(
      id = "33333333-3333-3333-3333-333333333333",
      enabled = TRUE
    ),
    list(
      id = "33333333-3333-3333-3333-333333333333",
      enabled = TRUE,
      configuration = NULL
    ),
    list(
      id = "33333333-3333-3333-3333-333333333333",
      enabled = TRUE,
      configuration = list()
    ),
    list(
      id = "33333333-3333-3333-3333-333333333333",
      enabled = TRUE,
      configuration = list(type = c("Daily", "Weekly"))
    )
  )

  for (body in malformed) {
    expect_error(
      .fabric_job_schedule_response(
        list(status_code = 200L, body = body),
        context,
        expected_status = 200L
      ),
      class = "fabric_job_schedule_protocol_error"
    )
  }
})

test_that("schedule creation sends the documented payload and preserves arrays", {
  call <- NULL
  configuration <- scheduler_test_configuration("Weekly")
  local_mocked_bindings(
    .fabric_job_request = function(
      method,
      url,
      credential,
      payload,
      idempotent,
      ...
    ) {
      call <<- list(
        method = method,
        url = url,
        payload = payload,
        idempotent = idempotent
      )
      list(
        status_code = 201L,
        body = scheduler_test_response(
          configuration = payload$configuration,
          enabled = payload$enabled,
          execution_data = payload$executionData
        )
      )
    }
  )

  schedule <- fabric_job_schedule_create(
    scheduler_test_item(),
    configuration,
    execution_data = list(parameters = list(marker = "scheduled")),
    token = "test-token"
  )

  expect_s3_class(schedule, "fabric_job_schedule")
  expect_equal(call$method, "POST")
  expect_match(call$url, "/jobs/RunNotebook/schedules$", perl = TRUE)
  expect_false(call$idempotent)
  expect_equal(call$payload$configuration$type, "Weekly")
  expect_equal(schedule$execution_data$parameters$marker, "scheduled")

  json <- jsonlite::toJSON(
    .fabric_job_preserve_json_arrays(call$payload),
    auto_unbox = TRUE,
    null = "null"
  )
  expect_match(json, '"times":["09:30"]', fixed = TRUE)
  expect_match(
    json,
    '"weekdays":["Monday","Thursday"]',
    fixed = TRUE
  )
})

test_that("unknown future schedule types use the documented escape hatch", {
  configuration <- list(
    type = "EventDriven",
    source = list(kind = "OneLakeEvent"),
    futureArray = c("one", "two")
  )
  payload <- NULL
  local_mocked_bindings(
    .fabric_job_request = function(..., payload) {
      payload <<- payload
      list(
        status_code = 201L,
        body = list(
          id = "33333333-3333-3333-3333-333333333333",
          enabled = TRUE,
          configuration = payload$configuration
        )
      )
    }
  )

  schedule <- fabric_job_schedule_create(
    scheduler_test_item(),
    configuration,
    token = "test-token"
  )

  expect_equal(payload$configuration, configuration)
  expect_equal(schedule$type, "EventDriven")
  expect_equal(schedule$configuration$source$kind, "OneLakeEvent")
})

test_that("partial schedule updates preserve service-required fields", {
  calls <- list()
  service_configuration <- scheduler_test_configuration("Daily")
  service_configuration$startDateTime <- "2026-10-01T00:00:00"
  service_configuration$endDateTime <- "2027-10-01T00:00:00"
  service_configuration$times <- list("09:30")
  current <- scheduler_test_response(
    configuration = service_configuration,
    enabled = TRUE,
    execution_data = list(parameters = list(marker = "keep"))
  )
  local_mocked_bindings(
    .fabric_job_request = function(
      method,
      url,
      credential,
      payload = NULL,
      idempotent,
      ...
    ) {
      calls[[length(calls) + 1L]] <<- list(
        method = method,
        url = url,
        payload = payload,
        idempotent = idempotent
      )
      if (identical(method, "GET")) {
        return(list(status_code = 200L, body = current))
      }
      list(
        status_code = 200L,
        body = c(
          list(id = current$id),
          payload,
          list(createdDateTime = current$createdDateTime)
        )
      )
    }
  )

  schedule <- fabric_job_schedule_update(
    scheduler_test_item(),
    current$id,
    enabled = FALSE,
    token = "test-token"
  )

  expect_equal(vapply(calls, `[[`, character(1), "method"), c("GET", "PATCH"))
  expect_true(calls[[2L]]$idempotent)
  expect_false(calls[[2L]]$payload$enabled)
  expect_equal(calls[[2L]]$payload$configuration$type, "Daily")
  expect_equal(
    calls[[2L]]$payload$configuration$startDateTime,
    "2026-10-01T00:00:00"
  )
  expect_equal(calls[[2L]]$payload$configuration$times, list("09:30"))
  expect_equal(
    calls[[2L]]$payload$executionData$parameters$marker,
    "keep"
  )
  expect_false(schedule$enabled)
  expect_equal(schedule$state, "Disabled")
})

test_that("schedule updates preserve execution data for omitted and null input", {
  payloads <- list()
  current <- scheduler_test_response(
    execution_data = list(parameters = list(marker = "preserved"))
  )
  local_mocked_bindings(
    .fabric_job_request = function(
      method,
      url,
      credential,
      payload = NULL,
      ...
    ) {
      if (identical(method, "GET")) {
        return(list(status_code = 200L, body = current))
      }
      payloads[[length(payloads) + 1L]] <<- payload
      list(status_code = 200L, body = c(list(id = current$id), payload))
    }
  )

  fabric_job_schedule_update(
    scheduler_test_item(),
    current$id,
    enabled = FALSE,
    token = "test-token"
  )
  fabric_job_schedule_update(
    scheduler_test_item(),
    current$id,
    configuration = scheduler_test_configuration(),
    enabled = FALSE,
    execution_data = NULL,
    token = "test-token"
  )

  expect_true("executionData" %in% names(payloads[[1L]]))
  expect_equal(
    payloads[[1L]]$executionData$parameters$marker,
    "preserved"
  )
  expect_true("executionData" %in% names(payloads[[2L]]))
  expect_equal(
    payloads[[2L]]$executionData$parameters$marker,
    "preserved"
  )
})

test_that("schedule deletion requires confirmation and uses the exact route", {
  schedule_id <- "33333333-3333-3333-3333-333333333333"
  expect_error(
    fabric_job_schedule_delete(
      scheduler_test_item(),
      schedule_id,
      token = "test-token"
    ),
    "confirm = TRUE",
    class = "fabric_job_schedule_confirmation_error"
  )

  call <- NULL
  local_mocked_bindings(
    .fabric_job_request = function(method, url, credential, ...) {
      call <<- list(method = method, url = url, arguments = list(...))
      list(status_code = 200L, body = NULL)
    }
  )
  expect_true(fabric_job_schedule_delete(
    scheduler_test_item(),
    schedule_id,
    confirm = TRUE,
    token = "test-token"
  ))
  expect_equal(call$method, "DELETE")
  expect_match(
    call$url,
    paste0("/jobs/RunNotebook/schedules/", schedule_id, "$"),
    perl = TRUE
  )
  expect_false(call$arguments$idempotent)
  expect_false(call$arguments$parse_json)
  expect_identical(call$arguments$accepted_status, 404L)
})

test_that("schedule deletion accepts an already absent schedule", {
  local_mocked_bindings(
    .fabric_job_request = function(...) {
      list(status_code = 404L, body = NULL)
    }
  )

  expect_true(fabric_job_schedule_delete(
    scheduler_test_item(),
    "33333333-3333-3333-3333-333333333333",
    confirm = TRUE,
    token = "test-token"
  ))
})

test_that("workloads infer their documented schedule job type", {
  urls <- character()
  local_mocked_bindings(
    .httr2_collection = function(url, ...) {
      urls <<- c(urls, url)
      list()
    }
  )

  fabric_job_schedules(
    scheduler_test_item("Notebook"),
    token = "test-token"
  )
  fabric_job_schedules(
    scheduler_test_item("SparkJobDefinition"),
    token = "test-token"
  )
  fabric_job_schedules(
    scheduler_test_item("DataPipeline"),
    token = "test-token"
  )
  fabric_job_schedules(
    scheduler_test_item("pipeline"),
    token = "test-token"
  )
  fabric_job_schedules(
    scheduler_test_item("Dataflow"),
    token = "test-token"
  )
  fabric_job_schedules(
    scheduler_test_item("DataBuildToolJob"),
    token = "test-token"
  )
  fabric_job_schedules(
    scheduler_test_item("SparkJobDefinition"),
    job_type = "ScheduledSparkJob",
    token = "test-token"
  )

  expect_match(urls[[1L]], "/jobs/RunNotebook/schedules$", perl = TRUE)
  expect_match(urls[[2L]], "/jobs/SparkJob/schedules$", perl = TRUE)
  expect_match(urls[[3L]], "/jobs/Execute/schedules$", perl = TRUE)
  expect_match(urls[[4L]], "/jobs/Execute/schedules$", perl = TRUE)
  expect_match(urls[[5L]], "/jobs/Execute/schedules$", perl = TRUE)
  expect_match(urls[[6L]], "/jobs/Execute/schedules$", perl = TRUE)
  expect_match(urls[[7L]], "/jobs/ScheduledSparkJob/schedules$", perl = TRUE)
})

test_that("semantic model schedules require the Power BI dataset API", {
  expect_snapshot(
    error = TRUE,
    fabric_job_schedules(
      scheduler_test_item("SemanticModel"),
      token = "test-token"
    )
  )
  expect_identical(
    .fabric_job_schedule_type("SemanticModel", "FutureRefresh"),
    "FutureRefresh"
  )
})

test_that("Lakehouse schedules require an explicit workload job type", {
  requests <- 0L
  local_mocked_bindings(
    .fabric_job_request = function(...) {
      requests <<- requests + 1L
      stop("A request should not be sent")
    }
  )

  expect_error(
    fabric_job_schedule_create(
      scheduler_test_item("Lakehouse"),
      scheduler_test_configuration(),
      token = "test-token"
    ),
    "explicit.*job_type",
    class = "fabric_job_schedule_job_type_required"
  )
  expect_identical(requests, 0L)
})

test_that("Lakehouse schedules preserve an explicit materialized-view route", {
  call <- NULL
  execution_data <- list(
    mlvExecutionDefinitionId = "44444444-4444-4444-4444-444444444444"
  )
  local_mocked_bindings(
    .fabric_job_request = function(method, url, credential, payload, ...) {
      call <<- list(method = method, url = url, payload = payload)
      list(
        status_code = 201L,
        body = scheduler_test_response(
          configuration = payload$configuration,
          enabled = payload$enabled,
          execution_data = payload$executionData
        )
      )
    }
  )

  schedule <- fabric_job_schedule_create(
    scheduler_test_item("Lakehouse"),
    scheduler_test_configuration(),
    job_type = "RefreshMaterializedLakeViews",
    execution_data = execution_data,
    token = "test-token"
  )

  expect_identical(call$method, "POST")
  expect_match(
    call$url,
    "/jobs/RefreshMaterializedLakeViews/schedules$",
    perl = TRUE
  )
  expect_identical(call$payload$executionData, execution_data)
  expect_identical(schedule$job_type, "RefreshMaterializedLakeViews")
})

test_that("DataPipeline schedule creation uses and records Execute", {
  call <- NULL
  local_mocked_bindings(
    .fabric_job_request = function(method, url, credential, payload, ...) {
      call <<- list(method = method, url = url)
      list(
        status_code = 201L,
        body = scheduler_test_response(
          configuration = payload$configuration,
          enabled = payload$enabled
        )
      )
    }
  )

  schedule <- fabric_job_schedule_create(
    scheduler_test_item("DataPipeline"),
    scheduler_test_configuration(),
    token = "test-token"
  )

  expect_equal(call$method, "POST")
  expect_match(call$url, "/jobs/Execute/schedules$", perl = TRUE)
  expect_equal(schedule$job_type, "Execute")
})

test_that("Dataflow schedule creation defaults to Execute", {
  call <- NULL
  local_mocked_bindings(
    .fabric_job_request = function(method, url, credential, payload, ...) {
      call <<- list(method = method, url = url)
      list(
        status_code = 201L,
        body = scheduler_test_response(
          configuration = payload$configuration,
          enabled = payload$enabled
        )
      )
    }
  )

  schedule <- fabric_job_schedule_create(
    scheduler_test_item("Dataflow"),
    scheduler_test_configuration(),
    token = "test-token"
  )

  expect_equal(call$method, "POST")
  expect_match(call$url, "/jobs/Execute/schedules$", perl = TRUE)
  expect_equal(schedule$job_type, "Execute")
})

test_that("Dataflow ApplyChanges schedules remain explicit", {
  call <- NULL
  local_mocked_bindings(
    .fabric_job_request = function(method, url, credential, payload, ...) {
      call <<- list(method = method, url = url)
      list(
        status_code = 201L,
        body = scheduler_test_response(
          configuration = payload$configuration,
          enabled = payload$enabled
        )
      )
    }
  )

  schedule <- fabric_job_schedule_create(
    scheduler_test_item("Dataflow"),
    scheduler_test_configuration(),
    job_type = "ApplyChanges",
    token = "test-token"
  )

  expect_equal(call$method, "POST")
  expect_match(call$url, "/jobs/ApplyChanges/schedules$", perl = TRUE)
  expect_equal(schedule$job_type, "ApplyChanges")
})

test_that("schedule records retain custom job types for later operations", {
  calls <- character()
  schedule <- structure(
    list(
      id = "33333333-3333-3333-3333-333333333333",
      job_type = "ScheduledSparkJob"
    ),
    class = "fabric_job_schedule"
  )
  local_mocked_bindings(
    .fabric_job_request = function(method, url, credential, ...) {
      calls <<- c(calls, url)
      list(status_code = 200L, body = NULL)
    }
  )

  fabric_job_schedule_delete(
    scheduler_test_item("SparkJobDefinition"),
    schedule,
    confirm = TRUE,
    token = "test-token"
  )

  expect_match(calls[[1L]], "/jobs/ScheduledSparkJob/schedules/", fixed = TRUE)
})

test_that("schedule response and print methods do not expose credentials", {
  context <- list(
    workspace_id = "22222222-2222-2222-2222-222222222222",
    item_id = "11111111-1111-1111-1111-111111111111",
    item_type = "Notebook",
    job_type = "RunNotebook"
  )
  schedule <- .fabric_job_schedule_record(scheduler_test_response(), context)
  text <- capture.output(print(schedule))

  expect_match(paste(text, collapse = "\n"), "fabric_job_schedule")
  expect_false(any(grepl("test-token", text, fixed = TRUE)))
  expect_s3_class(schedule$created_time, "POSIXct")
})
