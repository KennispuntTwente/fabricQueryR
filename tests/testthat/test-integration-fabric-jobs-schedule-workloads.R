# Fabric integration coverage: jobs workload schedule routes
for (route in c(
  "dataflow-execute",
  "dataflow-publish",
  "lakehouse-mlv",
  "dbt"
)) {
  local({
    name <- route
    test_that(paste("configured schedule CRUD uses the", name, "route"), {
      config <- jsonlite::fromJSON(
        fabric_test_feature_environment(
          "workload-schedules",
          "FABRIC_TEST_WORKLOAD_SCHEDULES_JSON"
        ),
        simplifyVector = FALSE
      )
      fixture <- config[[name]]
      if (is.null(fixture)) {
        fabric_test_feature_unavailable(
          "workload-schedules",
          paste("Missing workload schedule fixture:", name)
        )
      }
      types <- list(
        "dataflow-execute" = c("Dataflow", "Execute"),
        "dataflow-publish" = c("Dataflow", "ApplyChanges"),
        "lakehouse-mlv" = c("Lakehouse", "RefreshMaterializedLakeViews"),
        "dbt" = c("DataBuildToolJob", "Execute")
      )
      route_type <- types[[name]]
      token <- fabric_test_token_provider()
      item <- list(
        id = fixture$id,
        workspaceId = fixture$workspace,
        type = route_type[[1L]]
      )
      job_type <- if (name %in% c("dataflow-publish", "lakehouse-mlv")) {
        route_type[[2L]]
      } else {
        NULL
      }
      start <- trunc(Sys.time(), "secs") + 86400
      daily <- fabric_job_schedule_config(
        "Daily",
        start,
        start + 86400 * 7,
        times = "12:00"
      )
      schedule <- fabric_job_schedule_create(
        item,
        daily,
        job_type = job_type,
        enabled = FALSE,
        execution_data = fixture$execution_data,
        token = token
      )
      withr::defer(try(
        fabric_job_schedule_delete(
          item,
          schedule,
          job_type = job_type,
          confirm = TRUE,
          token = token
        ),
        silent = TRUE
      ))
      expect_identical(schedule$job_type, route_type[[2L]])
      expect_false(schedule$enabled)
      listed <- fabric_job_schedules(item, job_type = job_type, token = token)
      expect_true(schedule$id %in% vapply(listed, `[[`, character(1), "id"))
      daily$times <- "13:00"
      changed <- fabric_job_schedule_update(
        item,
        schedule,
        job_type = job_type,
        configuration = daily,
        enabled = FALSE,
        token = token
      )
      expect_identical(unlist(changed$configuration$times), "13:00")
      expect_true(fabric_job_schedule_delete(
        item,
        schedule,
        job_type = job_type,
        confirm = TRUE,
        token = token
      ))
    })
  })
}
