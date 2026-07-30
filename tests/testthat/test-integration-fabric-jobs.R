# Fabric integration coverage: asynchronous jobs for Fabric items.
# These tests run sandbox notebooks, pipelines, and Spark job definitions, then
# check successful runs as well as failure, timeout, and cancellation behavior.

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
  # Do not inherit a high-concurrency session from an earlier test invocation.
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
    cancel_on_timeout = TRUE
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
  status <- fabric_job_status(completed_job)
  expect_s3_class(status, "fabric_job_instance")
  expect_equal(status$id, completed$id)
  expect_equal(status$status, "Completed")

  failed_job <- fabric_job_run(
    item,
    parameters = list(mode = "failure"),
    default_lakehouse = lakehouse$id,
    session_tag = paste0(session_tag, "_failure"),
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
      "Fabric returned ", failed$status,
      "; exit value: ", failed$exit_value %||% "<none>",
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
    expect_true(nzchar(
      .fabric_job_failure_text(failed$job_status$failure_reason)
    ))
    expect_match(
      .fabric_job_failure_text(failed$job_status$failure_reason),
      "FABRICQUERYR_INTENTIONAL_JOB_FAILURE",
      fixed = TRUE
    )
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
