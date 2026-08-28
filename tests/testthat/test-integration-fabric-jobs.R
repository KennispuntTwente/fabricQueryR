# Fabric integration coverage: asynchronous jobs for Fabric items
# These tests run sandbox notebooks, pipelines, and Spark job definitions, then
# check successful runs as well as failure, timeout, and cancellation behavior

test_that("Fabric item jobs complete, fail, time out, and cancel", {
  manifest <- fabric_test_manifest()
  token <- fabric_test_token("FABRIC_TEST_API_TOKEN")
  notebook <- fabric_test_manifest_item(manifest, "JobFixtures")
  lakehouse <- fabric_test_manifest_item(manifest, "TestLakehouse")
  item <- list(
    id = notebook$id,
    workspaceId = manifest$workspace_id,
    type = notebook$type,
    displayName = notebook$display_name
  )
  # Do not inherit a high-concurrency session from an earlier test invocation
  session_tag <- paste0(
    "fabricqueryr_job_integration_",
    substr(notebook$id, 1L, 8L),
    "_",
    Sys.getpid(),
    "_",
    format(Sys.time(), "%Y%m%d%H%M%S", tz = "UTC")
  )
  completed_job <- fabric_job_run(
    item,
    parameters = list(mode = "success", marker = "integration"),
    default_lakehouse = lakehouse$id,
    session_tag = session_tag,
    token = token
  )
  expect_s3_class(completed_job, "fabric_job")
  completed <- fabric_job_wait(
    completed_job,
    timeout = 900,
    cancel_on_timeout = TRUE,
    notebook_details = TRUE
  )
  expect_s3_class(completed, "fabric_job_instance")
  expect_equal(completed$status, "Completed")
  expect_equal(
    completed$exit_value,
    "fabricqueryr-job-success:integration"
  )
  expect_true(nzchar(completed$root_activity_id))
  expect_s3_class(completed$start_time, "POSIXct")
  expect_s3_class(completed$end_time, "POSIXct")
  stable_status <- fabric_job_status(
    completed_job,
    respect_retry_after = FALSE
  )
  expect_s3_class(stable_status, "fabric_job_instance")
  expect_equal(stable_status$id, completed$id)
  expect_equal(stable_status$status, "Completed")
  detailed_status <- fabric_job_status(
    completed_job,
    notebook_details = TRUE,
    respect_retry_after = FALSE
  )
  expect_equal(detailed_status$id, completed$id)
  expect_equal(detailed_status$status, "Completed")
  expect_equal(
    detailed_status$exit_value,
    "fabricqueryr-job-success:integration"
  )

  # The failure run deliberately avoids high-concurrency mode. Fabric keeps a
  # shared session alive when one of its statements fails, so a high-concurrency
  # run reports Completed with no exit value; only a run that owns its Spark
  # session is cancelled and surfaced to the job scheduler as Failed
  failed_job <- fabric_job_run(
    item,
    parameters = list(mode = "failure"),
    default_lakehouse = lakehouse$id,
    token = token
  )
  failed <- tryCatch(
    fabric_job_wait(
      failed_job,
      timeout = 900,
      cancel_on_timeout = TRUE
    ),
    error = identity
  )
  failure_info <- if (inherits(failed, "fabric_job_instance")) {
    paste0(
      "Fabric returned ",
      failed$status,
      "; exit value: ",
      failed$exit_value %||% "<none>",
      "; failure reason: ",
      .fabric_job_failure_text(failed$failure_reason)
    )
  } else {
    NULL
  }
  expect_true(
    inherits(failed, "fabric_job_failed"),
    info = failure_info
  )
  if (inherits(failed, "fabric_job_failed")) {
    expect_equal(failed$job_status$status, "Failed")
    expect_true(nzchar(failed$job_status$root_activity_id))
    expect_null(failed$job_status$exit_value)
    # Fabric reports the Spark session cancellation, not the notebook's own
    # exception text; that text stays in the run snapshot
    expect_true(nzchar(
      .fabric_job_failure_text(failed$job_status$failure_reason)
    ))
  }

  slow_job <- fabric_job_run(
    item,
    parameters = list(mode = "slow", delay_seconds = 600L),
    default_lakehouse = lakehouse$id,
    session_tag = session_tag,
    token = token
  )
  on.exit(try(fabric_job_cancel(slow_job), silent = TRUE), add = TRUE)
  timed_out <- rlang::catch_cnd(
    fabric_job_wait(
      slow_job,
      timeout = 2,
      cancel_on_timeout = TRUE
    ),
    classes = "error"
  )
  expect_s3_class(timed_out, "fabric_job_timeout")

  slow_job$retry_after <- 2
  cancelled <- fabric_job_wait(
    slow_job,
    poll_interval = 2,
    timeout = 600,
    error_on_failure = FALSE
  )
  expect_equal(cancelled$status, "Cancelled")
  expect_true(nzchar(cancelled$root_activity_id))
})

test_that("Fabric pipeline and Spark job definition jobs complete", {
  manifest <- fabric_test_manifest()
  token <- fabric_test_token("FABRIC_TEST_API_TOKEN")
  storage_token <- fabric_test_token("FABRIC_TEST_STORAGE_TOKEN")
  fixtures <- c("TestPipeline", "TestSparkJob")

  for (name in fixtures) {
    fixture <- fabric_test_manifest_item(manifest, name)
    item <- list(
      id = fixture$id,
      workspaceId = manifest$workspace_id,
      type = fixture$type,
      displayName = fixture$display_name
    )
    job <- fabric_job_run(item, token = token)
    result <- fabric_job_wait(job, timeout = 1200)

    expect_s3_class(result, "fabric_job_instance")
    expect_equal(result$status, "Completed", info = name)
    expect_equal(result$item_id, fixture$id, info = name)

    if (identical(name, "TestSparkJob")) {
      lakehouse <- fabric_test_manifest_item(manifest, "TestLakehouse")
      marker <- fabric_onelake_read_delta_table(
        table_path = lakehouse$tables$spark_job_result,
        workspace_name = manifest$workspace_id,
        lakehouse_name = lakehouse$id,
        schema = lakehouse$schema,
        token = storage_token,
        verbose = FALSE
      )
      expect_equal(marker$mode, "success")
      expect_equal(as.numeric(marker$row_count), 3)
    }
  }
})

test_that("Fabric job history and daily and weekly schedules complete a lifecycle", {
  manifest <- fabric_test_manifest()
  token <- fabric_test_token("FABRIC_TEST_API_TOKEN")
  fixture <- fabric_test_manifest_item(manifest, "TestPipeline")
  item <- list(
    id = fixture$id,
    workspaceId = manifest$workspace_id,
    type = fixture$type,
    displayName = fixture$display_name
  )

  history <- fabric_job_instances(item, token = token)
  expect_s3_class(history, "fabric_job_instance_list")
  expect_true(length(history) >= 1L)
  expect_true(all(vapply(
    history,
    inherits,
    logical(1),
    what = "fabric_job_instance"
  )))
  terminal_history <- Filter(
    function(instance) {
      instance$status %in% c("Completed", "Failed", "Cancelled", "Deduped")
    },
    history
  )
  expect_true(length(terminal_history) >= 1L)
  historical <- terminal_history[[1L]]
  refreshed <- fabric_job_status(historical, respect_retry_after = FALSE)
  expect_equal(refreshed$id, historical$id)
  waited <- fabric_job_wait(
    historical,
    poll_interval = 2,
    timeout = 120,
    error_on_failure = FALSE
  )
  expect_s3_class(waited, "fabric_job_instance")
  expect_equal(waited$id, historical$id)

  start <- as.POSIXct(Sys.Date() + 2, tz = "UTC")
  end <- start + (14 * 24 * 60 * 60)
  created_ids <- character()
  on.exit(
    {
      for (id in created_ids) {
        try(
          fabric_job_schedule_delete(
            item,
            id,
            confirm = TRUE,
            token = token
          ),
          silent = TRUE
        )
      }
    },
    add = TRUE
  )

  daily_configuration <- fabric_job_schedule_config(
    "Daily",
    start_time = start,
    end_time = end,
    time_zone = "UTC",
    times = "03:17"
  )
  daily <- fabric_job_schedule_create(
    item,
    daily_configuration,
    enabled = TRUE,
    token = token
  )
  created_ids <- c(created_ids, daily$id)
  expect_true(daily$enabled)
  expect_equal(daily$job_type, "Execute")

  disabled <- fabric_job_schedule_update(
    item,
    daily,
    enabled = FALSE,
    token = token
  )
  expect_false(disabled$enabled)
  expect_equal(disabled$state, "Disabled")

  weekly_configuration <- fabric_job_schedule_config(
    "Weekly",
    start_time = start,
    end_time = end,
    time_zone = "UTC",
    times = "04:23",
    weekdays = c("Tuesday", "Saturday")
  )
  weekly <- fabric_job_schedule_create(
    item,
    weekly_configuration,
    enabled = FALSE,
    token = token
  )
  created_ids <- c(created_ids, weekly$id)
  expect_false(weekly$enabled)

  schedules <- fabric_job_schedules(item, token = token)
  listed_ids <- vapply(schedules, `[[`, character(1), "id")
  expect_true(all(c(daily$id, weekly$id) %in% listed_ids))

  expect_true(fabric_job_schedule_delete(
    item,
    daily,
    confirm = TRUE,
    token = token
  ))
  created_ids <- setdiff(created_ids, daily$id)
  expect_true(fabric_job_schedule_delete(
    item,
    weekly,
    confirm = TRUE,
    token = token
  ))
  created_ids <- setdiff(created_ids, weekly$id)
})

test_that("Cron and monthly Fabric schedules complete live lifecycles", {
  manifest <- fabric_test_manifest()
  token <- fabric_test_token("FABRIC_TEST_API_TOKEN")
  fixture <- fabric_test_manifest_item(manifest, "TestPipeline")
  item <- list(
    id = fixture$id,
    workspaceId = manifest$workspace_id,
    type = fixture$type,
    displayName = fixture$display_name
  )
  start <- as.POSIXct(Sys.Date() + 2, tz = "UTC")
  end <- start + (90 * 24 * 60 * 60)
  configurations <- list(
    Cron = fabric_job_schedule_config(
      "Cron",
      start_time = start,
      end_time = end,
      time_zone = "UTC",
      interval = 37L
    ),
    Monthly = fabric_job_schedule_config(
      "Monthly",
      start_time = start,
      end_time = end,
      time_zone = "UTC",
      times = "06:37",
      recurrence = 1L,
      day_of_month = 15L
    )
  )
  created_ids <- character()
  on.exit(
    {
      for (id in created_ids) {
        try(
          fabric_job_schedule_delete(
            item,
            id,
            confirm = TRUE,
            token = token
          ),
          silent = TRUE
        )
      }
    },
    add = TRUE
  )

  created <- lapply(configurations, function(configuration) {
    schedule <- fabric_job_schedule_create(
      item,
      configuration,
      enabled = FALSE,
      token = token
    )
    created_ids <<- c(created_ids, schedule$id)
    schedule
  })

  expect_false(created$Cron$enabled)
  expect_equal(created$Cron$type, "Cron")
  expect_equal(created$Cron$configuration$interval, 37L)
  expect_false(created$Monthly$enabled)
  expect_equal(created$Monthly$type, "Monthly")
  expect_equal(created$Monthly$configuration$recurrence, 1L)
  expect_equal(
    created$Monthly$configuration$occurrence$occurrenceType,
    "DayOfMonth"
  )
  expect_equal(created$Monthly$configuration$occurrence$dayOfMonth, 15L)

  schedules <- fabric_job_schedules(item, token = token)
  listed_ids <- vapply(schedules, `[[`, character(1), "id")
  expect_true(all(created_ids %in% listed_ids))

  for (schedule in created) {
    expect_true(fabric_job_schedule_delete(
      item,
      schedule,
      confirm = TRUE,
      token = token
    ))
    created_ids <- setdiff(created_ids, schedule$id)
  }
})

test_that("notebook and Spark schedule defaults complete live lifecycles", {
  manifest <- fabric_test_manifest()
  token <- fabric_test_token("FABRIC_TEST_API_TOKEN")
  cases <- list(
    JobFixtures = "RunNotebook",
    TestSparkJob = "SparkJob"
  )
  start <- as.POSIXct(Sys.Date() + 2, tz = "UTC")
  configuration <- fabric_job_schedule_config(
    "Daily",
    start_time = start,
    end_time = start + (14 * 24 * 60 * 60),
    time_zone = "UTC",
    times = "05:41"
  )
  created <- list()
  on.exit(
    {
      for (entry in created) {
        try(
          fabric_job_schedule_delete(
            entry$item,
            entry$id,
            confirm = TRUE,
            token = token
          ),
          silent = TRUE
        )
      }
    },
    add = TRUE
  )

  for (name in names(cases)) {
    fixture <- fabric_test_manifest_item(manifest, name)
    item <- list(
      id = fixture$id,
      workspaceId = manifest$workspace_id,
      type = fixture$type,
      displayName = fixture$display_name
    )
    schedule <- fabric_job_schedule_create(
      item,
      configuration,
      enabled = FALSE,
      token = token
    )
    created[[length(created) + 1L]] <- list(item = item, id = schedule$id)

    expect_identical(schedule$job_type, cases[[name]], info = name)
    listed <- fabric_job_schedules(item, token = token)
    expect_true(
      schedule$id %in% vapply(listed, `[[`, character(1), "id"),
      info = name
    )
  }

  for (entry in created) {
    expect_true(fabric_job_schedule_delete(
      entry$item,
      entry$id,
      confirm = TRUE,
      token = token
    ))
  }
  created <- list()
})
