# Fabric integration coverage: R6 job credential precedence
test_that("R6 lifecycle calls use the submitted job credential", {
  manifest <- fabric_test_manifest()
  token <- fabric_test_token_provider()
  fixture <- fabric_test_manifest_item(manifest, "JobFixtures")
  item <- fabric_r6_record(
    list(
      id = fixture$id,
      workspaceId = manifest$workspace_id,
      type = "Notebook"
    ),
    c("fabric_item", "list"),
    fabric_credential(token = function(...) {
      stop("Lifecycle call unexpectedly used the discovery credential")
    })
  )
  marker <- kusto_ingestion_source_id()
  job <- item$run(
    parameters = list(mode = "success", marker = marker),
    token = token
  )
  withr::defer(try(fabric_job_cancel(job), silent = TRUE))
  done <- item$wait(
    job,
    timeout = 900,
    cancel_on_timeout = TRUE,
    notebook_details = TRUE
  )
  expect_identical(done$status, "Completed")
  expect_identical(done$exit_value, paste0("fabricqueryr-job-success:", marker))
  expect_identical(
    item$status(done, respect_retry_after = FALSE)$status,
    "Completed"
  )
  slow <- item$run(
    parameters = list(mode = "slow", delay_seconds = 600L),
    token = token
  )
  withr::defer(try(fabric_job_cancel(slow), silent = TRUE))
  expect_identical(item$cancel(slow), TRUE)
  cancelled <- item$wait(slow, timeout = 600, error_on_failure = FALSE)
  expect_identical(cancelled$status, "Cancelled")
})
