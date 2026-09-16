# Fabric integration coverage: kql graphql recovery
test_that("live KQL ingestion reports independent successful and failed sources", {
  manifest <- fabric_test_manifest()
  database <- fabric_test_manifest_item(manifest, "TestKQLDatabase")
  lake <- fabric_test_manifest_item(manifest, "TestLakehouse")
  token <- fabric_test_token_provider()
  table <- paste0(
    "fabricqueryr_mixed_",
    gsub("-", "", kusto_ingestion_source_id())
  )
  target <- kusto_resolve_target(database)
  credential <- fabric_credential(token = token)
  management <- function(command) {
    kusto_export_management(
      target,
      command,
      credential,
      deadline = Sys.time() + 60,
      idempotent = TRUE,
      operation = "RecoveryFixture"
    )
  }
  management(paste(".create table", table, "(id:long)"))
  withr::defer(try(
    management(paste(".drop table", table, "ifexists")),
    silent = TRUE
  ))
  folder <- paste0("Files/", table)
  fabric_onelake_upload(
    manifest$workspace_id,
    lake$id,
    paste0(folder, "/valid.csv"),
    source = charToRaw("73\n"),
    token = token
  )
  withr::defer(try(
    fabric_onelake_delete(
      manifest$workspace_id,
      lake$id,
      folder,
      recursive = TRUE,
      confirm = TRUE,
      token = token
    ),
    silent = TRUE
  ))
  base <- paste0(
    "https://onelake.dfs.fabric.microsoft.com/",
    manifest$workspace_id,
    "/",
    lake$id,
    "/",
    folder,
    "/"
  )
  ids <- c(kusto_ingestion_source_id(), kusto_ingestion_source_id())
  ingestion <- fabric_kql_ingest(
    database,
    table,
    sources = paste0(
      base,
      c("valid.csv", "missing.csv"),
      ";token=",
      token(.fabric_audience$storage)
    ),
    source_ids = ids,
    raw_sizes = c(3, 3),
    format = "csv",
    skip_batching = TRUE,
    token = token
  )
  status <- fabric_kql_ingestion_status(
    ingestion,
    wait = TRUE,
    timeout = 600,
    poll_interval = 2,
    error_on_failure = FALSE
  )
  expect_identical(status$state, "PartiallySucceeded")
  expect_true(status$complete)
  expect_equal(status$succeeded, 1)
  expect_equal(status$failed, 1)
  expect_identical(
    status$details$status[match(ids, status$details$source_id)],
    c("Succeeded", "Failed")
  )
  rows <- fabric_kql_query(
    database,
    paste(table, "| project id"),
    token = token
  )
  expect_identical(as.numeric(rows$id), 73)
})

test_that("live KQL writes retain staging after an injected cleanup failure and recover", {
  manifest <- fabric_test_manifest()
  database <- fabric_test_manifest_item(manifest, "TestKQLDatabase")
  lake <- fabric_test_manifest_item(manifest, "TestLakehouse")
  token <- fabric_test_token_provider()
  table <- paste0(
    "fabricqueryr_cleanup_",
    gsub("-", "", kusto_ingestion_source_id())
  )
  target <- kusto_resolve_target(database)
  credential <- fabric_credential(token = token)
  withr::defer(try(
    kusto_export_management(
      target,
      paste(".drop table", table, "ifexists"),
      credential,
      deadline = Sys.time() + 60,
      idempotent = TRUE,
      operation = "RecoveryCleanup"
    ),
    silent = TRUE
  ))
  original <- kusto_remove_staging
  cleanup_args <- NULL
  withr::defer(
    if (!is.null(cleanup_args)) {
      try(do.call(original, cleanup_args), silent = TRUE)
    }
  )
  # All upload, ingestion, status and recovery requests are real. Inject only
  # the first cleanup failure, which cannot be induced reliably by the service.
  local_mocked_bindings(kusto_remove_staging = function(...) {
    cleanup_args <<- list(...)
    FALSE
  })
  result <- NULL
  expect_warning(
    result <- fabric_kql_write_table(
      database,
      table,
      data.frame(id = 73L),
      create_if_missing = TRUE,
      skip_batching = TRUE,
      timeout = 600,
      token = token,
      staging_folder = paste0(
        "https://onelake.dfs.fabric.microsoft.com/",
        manifest$workspace_id,
        "/",
        lake$id,
        "/Files/fabricqueryr_recovery"
      )
    ),
    "Staging cleanup failed",
    fixed = TRUE
  )
  expect_identical(result$status$state, "Succeeded")
  expect_true(result$staging_retained)
  expect_false(is.null(cleanup_args))
  rows <- fabric_kql_query(
    database,
    paste(table, "| project id"),
    token = token
  )
  expect_identical(as.numeric(rows$id), 73)
  expect_true(do.call(original, cleanup_args))
  cleanup_args <- NULL
})

test_that("a service-canceled KQL ingestion retains per-source cancellation details", {
  id <- fabric_test_feature_environment(
    "kql-cancellation",
    "FABRIC_TEST_KQL_CANCELED_INGESTION_ID"
  )
  table <- fabric_test_feature_environment(
    "kql-cancellation",
    "FABRIC_TEST_KQL_CANCELED_TABLE"
  )
  manifest <- fabric_test_manifest()
  database <- fabric_test_manifest_item(manifest, "TestKQLDatabase")
  status <- fabric_kql_ingestion_status(
    id,
    cluster = database,
    table = table,
    token = fabric_test_token_provider(),
    error_on_failure = FALSE
  )
  expect_contains(c("Canceled", "PartiallyCanceled"), status$state)
  expect_true(status$complete)
  expect_gt(status$canceled, 0)
  expect_true(any(status$details$status == "Canceled"))
})
