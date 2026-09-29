# Fabric integration coverage: jobs schedules nonutc
test_that("a non-UTC Daily schedule fires at the intended instant", {
  if (!fabric_test_feature_required("nonutc-schedules")) {
    skip(
      "Non-UTC firing evidence belongs to the required nonutc-schedules feature lane"
    )
  }
  manifest <- fabric_test_manifest()
  token <- fabric_test_token_provider()
  fixture <- fabric_test_manifest_item(manifest, "JobFixtures")
  item <- list(
    id = fixture$id,
    workspaceId = manifest$workspace_id,
    type = fixture$type
  )
  marker <- paste0("scheduled-", kusto_ingestion_source_id())
  seen <- vapply(
    fabric_job_instances(item, token = token),
    `[[`,
    character(1),
    "id"
  )
  start <- as.POSIXct(
    ceiling(as.numeric(Sys.time()) / 60) * 60 + 240,
    origin = "1970-01-01",
    tz = "UTC"
  )
  schedule <- fabric_job_schedule_create(
    item,
    fabric_job_schedule_config(
      "Daily",
      start_time = start - 120,
      end_time = start + 600,
      time_zone = "W. Europe Standard Time",
      times = format(start, "%H:%M", tz = "Europe/Amsterdam")
    ),
    enabled = TRUE,
    # RunNotebook schedules use the workload's String type, not REST's Text.
    execution_data = list(
      executionData = list(compute = "Spark"),
      parameters = list(
        list(name = "mode", value = "success", type = "String"),
        list(name = "marker", value = marker, type = "String")
      )
    ),
    token = token
  )
  on.exit(
    fabric_job_schedule_delete(item, schedule, confirm = TRUE, token = token),
    add = TRUE
  )

  deadline <- Sys.time() + 1200
  matched <- NULL
  repeat {
    history <- fabric_job_instances(item, token = token)
    for (instance in history) {
      if (
        instance$id %in%
          seen ||
          !identical(instance$invoke_type, "Scheduled") ||
          !identical(instance$status, "Completed")
      ) {
        next
      }
      completed <- fabric_job_status(
        instance,
        notebook_details = TRUE,
        respect_retry_after = FALSE
      )
      if (
        identical(
          completed$exit_value,
          paste0("fabricqueryr-job-success:", marker)
        )
      ) {
        matched <- completed
        break
      }
      if (!is.null(completed$exit_value) && nzchar(completed$exit_value)) {
        seen <- c(seen, instance$id)
      }
    }
    if (!is.null(matched)) {
      break
    }
    if (Sys.time() >= deadline) {
      rlang::abort(
        "Enabled schedule did not return its unique notebook marker in time"
      )
    }
    Sys.sleep(5)
  }
  expect_identical(matched$status, "Completed")
  expect_identical(matched$invoke_type, "Scheduled")
  expect_identical(
    matched$exit_value,
    paste0("fabricqueryr-job-success:", marker)
  )
  expect_gte(as.numeric(matched$start_time), as.numeric(start))
  expect_lt(as.numeric(matched$start_time), as.numeric(start + 600))
})

test_that("Spark job schedules use their workload route through CRUD", {
  manifest <- fabric_test_manifest()
  token <- fabric_test_token_provider()
  fixture <- fabric_test_manifest_item(manifest, "TestSparkJob")
  item <- list(
    id = fixture$id,
    workspaceId = manifest$workspace_id,
    type = fixture$type
  )
  start <- trunc(Sys.time(), "secs") + 86400
  schedule <- fabric_job_schedule_create(
    item,
    fabric_job_schedule_config(
      "Daily",
      start_time = start,
      end_time = start + 86400,
      times = "12:00",
      time_zone = "UTC"
    ),
    enabled = FALSE,
    token = token
  )
  withr::defer(try(
    fabric_job_schedule_delete(item, schedule, confirm = TRUE, token = token),
    silent = TRUE
  ))
  expect_false(schedule$enabled)
  schedules <- fabric_job_schedules(item, token = token)
  expect_true(schedule$id %in% vapply(schedules, `[[`, character(1), "id"))
  changed <- fabric_job_schedule_update(
    item,
    schedule,
    enabled = FALSE,
    configuration = fabric_job_schedule_config(
      "Weekly",
      start_time = start,
      end_time = start + 86400 * 7,
      times = "13:00",
      weekdays = "Friday"
    ),
    token = token
  )
  expect_identical(changed$type, "Weekly")
  expect_true(fabric_job_schedule_delete(
    item,
    schedule,
    confirm = TRUE,
    token = token
  ))
})
