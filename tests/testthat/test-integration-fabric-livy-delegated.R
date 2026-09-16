# Fabric integration coverage: livy delegated
test_that("delegated Livy discovery finds this test's executing session", {
  manifest <- fabric_test_manifest()
  auth <- fabric_test_azure_auth_config()
  if (!fabric_test_is_delegated_auth(auth)) {
    fabric_test_feature_unavailable(
      "delegated-livy",
      "Livy activity discovery requires a delegated user identity"
    )
  }
  lake <- fabric_test_manifest_item(manifest, "TestLakehouse")
  call <- function(fun, ...) do.call(fun, c(list(...), auth))
  session <- call(fabric_livy_session, lake$livy_url, verbose = FALSE)
  withr::defer(try(session$close(), silent = TRUE))
  session$wait(timeout = 900)
  probe <- session$run(
    "print(spark.range(1).count())",
    kind = "pyspark",
    timeout = 300
  )
  expect_identical(probe$output$status, "ok")
  sessions <- fabric_test_eventually(
    function() call(fabric_livy_sessions, lake$livy_url),
    ready = function(value) session$id %in% value$id
  )
  expect_contains(sessions$id, session$id)
})

test_that("delegated Livy discovery finds this test's batch", {
  manifest <- fabric_test_manifest()
  auth <- fabric_test_azure_auth_config()
  if (!fabric_test_is_delegated_auth(auth)) {
    fabric_test_feature_unavailable(
      "delegated-livy",
      "Livy activity discovery requires a delegated user identity"
    )
  }
  lake <- fabric_test_manifest_item(manifest, "TestLakehouse")
  call <- function(fun, ...) do.call(fun, c(list(...), auth))
  path <- paste0(
    "Files/fabricqueryr_discovery_",
    .fabric_lakehouse_staging_id(),
    ".py"
  )
  token <- fabric_test_token_provider()
  fabric_onelake_upload(
    manifest$workspace_id,
    lake$id,
    path,
    source = charToRaw("import time\ntime.sleep(300)\n"),
    token = token
  )
  withr::defer(try(
    fabric_onelake_delete(
      manifest$workspace_id,
      lake$id,
      path,
      confirm = TRUE,
      token = token
    ),
    silent = TRUE
  ))
  uri <- paste0(
    "abfss://",
    manifest$workspace_id,
    "@onelake.dfs.fabric.microsoft.com/",
    lake$id,
    "/",
    path
  )
  batch <- call(
    fabric_livy_batch_submit,
    lake$livy_url,
    file = uri,
    target_lakehouse_id = lake$id,
    verbose = FALSE
  )
  withr::defer(try(batch$cancel(), silent = TRUE))
  batches <- fabric_test_eventually(
    function() call(fabric_livy_batches, lake$livy_url),
    ready = function(value) batch$id %in% value$id
  )
  expect_contains(batches$id, batch$id)
})
