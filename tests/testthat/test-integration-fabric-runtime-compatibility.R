# Fabric integration coverage: cross-runtime compatibility smoke tests
# These complement the full OneLake tests on the
# Runtime 2.0 lane and the full Livy tests on the Runtime 1.3 lane

test_that("delta-rs reads a Fabric table on Runtime 1.3", {
  fabric_test_runtime_lane("core")
  manifest <- fabric_test_manifest()
  lakehouse <- fabric_test_manifest_item(manifest, "TestLakehouse")

  result <- fabric_onelake_read_delta_table(
    table_path = lakehouse$tables$basic,
    workspace_name = manifest$workspace_id,
    lakehouse_name = lakehouse$id,
    schema = lakehouse$schema,
    token = fabric_test_token_provider(),
    verbose = FALSE
  )

  expect_s3_class(result, "tbl_df")
  expect_equal(sort(result$id), 1:3)
})

test_that("Livy executes PySpark on Runtime 2.0", {
  fabric_test_runtime_lane("preview")
  manifest <- fabric_test_manifest()
  lakehouse <- fabric_test_manifest_item(manifest, "TestLakehouse")
  auth <- fabric_test_azure_auth_config()

  result <- fabric_livy_query(
    livy_url = lakehouse$livy_url,
    code = 'print("FABRICQUERYR_RUNTIME_2_LIVY_OK")',
    kind = "pyspark",
    tenant_id = auth$tenant_id,
    client_id = auth$client_id,
    auth_args = auth$auth_args,
    verbose = FALSE
  )

  expect_equal(result$state, "available")
  expect_equal(result$output$status, "ok")
  expect_match(
    paste(result$output$parsed, collapse = "\n"),
    "FABRICQUERYR_RUNTIME_2_LIVY_OK",
    fixed = TRUE
  )
})

test_that("Livy sessions exercise supported languages on Runtime 2.0", {
  fabric_test_runtime_lane("preview")
  manifest <- fabric_test_manifest()
  lakehouse <- fabric_test_manifest_item(manifest, "TestLakehouse")
  auth <- fabric_test_azure_auth_config()
  session <- fabric_livy_session(
    lakehouse$livy_url,
    tenant_id = auth$tenant_id,
    client_id = auth$client_id,
    auth_args = auth$auth_args,
    name = "fabricqueryr-runtime-2-session",
    verbose = FALSE
  )
  on.exit(try(session$close(), silent = TRUE), add = TRUE)
  session$wait(timeout = 900, poll_interval = 5)

  assignment <- session$run(
    "fabricqueryr_runtime_2_value = 40",
    kind = "pyspark",
    timeout = 300,
    poll_interval = 2
  )
  expect_equal(assignment$output$status, "ok")

  reused <- session$run(
    paste0(
      "print('FABRICQUERYR_RUNTIME_2_SHARED=' + ",
      "str(fabricqueryr_runtime_2_value + 2))"
    ),
    kind = "pyspark",
    timeout = 300,
    poll_interval = 2
  )
  expect_match(
    paste(reused$output$parsed, collapse = "\n"),
    "FABRICQUERYR_RUNTIME_2_SHARED=42",
    fixed = TRUE
  )

  scala <- session$run(
    "println(\"FABRICQUERYR_RUNTIME_2_SCALA_OK\")",
    kind = "spark",
    timeout = 300,
    poll_interval = 2
  )
  expect_equal(scala$output$status, "ok")
  expect_match(
    paste(scala$output$parsed, collapse = "\n"),
    "FABRICQUERYR_RUNTIME_2_SCALA_OK",
    fixed = TRUE
  )

  sql <- session$run(
    "SELECT 42 AS fabricqueryr_runtime_2_sql_value",
    kind = "sql",
    timeout = 300,
    poll_interval = 2
  )
  expect_equal(sql$output$status, "ok")
  expect_s3_class(sql$output$parsed, "tbl_df")
  expect_identical(
    as.character(sql$output$parsed[[1L]][[1L]]),
    "42"
  )

  failed <- session$submit(
    "raise RuntimeError('FABRICQUERYR_RUNTIME_2_FAILURE')",
    kind = "pyspark"
  )
  error <- rlang::catch_cnd(
    failed$wait(timeout = 300, poll_interval = 2)
  )
  expect_s3_class(error, "fabric_livy_statement_error")
  expect_match(
    paste(
      conditionMessage(error),
      error$output$evalue %||% "",
      error$traceback,
      collapse = "\n"
    ),
    "FABRICQUERYR_RUNTIME_2_FAILURE",
    fixed = TRUE
  )
})

test_that("high-concurrency Livy executes on Runtime 2.0", {
  fabric_test_runtime_lane("preview")
  manifest <- fabric_test_manifest()
  lakehouse <- fabric_test_manifest_item(manifest, "TestLakehouse")
  auth <- fabric_test_azure_auth_config()
  session <- fabric_livy_session(
    lakehouse$livy_url,
    high_concurrency = TRUE,
    session_tag = paste0("fabricqueryr-runtime-2-", manifest$workspace_id),
    artifact_name = lakehouse$display_name,
    tenant_id = auth$tenant_id,
    client_id = auth$client_id,
    auth_args = auth$auth_args,
    verbose = FALSE
  )
  on.exit(try(session$close(), silent = TRUE), add = TRUE)
  session$wait(timeout = 900, poll_interval = 5)

  result <- session$run(
    "print('FABRICQUERYR_RUNTIME_2_HC_OK')",
    kind = "pyspark",
    timeout = 300,
    poll_interval = 2
  )
  expect_equal(result$output$status, "ok")
  expect_match(
    paste(result$output$parsed, collapse = "\n"),
    "FABRICQUERYR_RUNTIME_2_HC_OK",
    fixed = TRUE
  )
  expect_true(nzchar(session$session_id))
  expect_true(nzchar(session$repl_id))
})

test_that("Livy batches complete on Runtime 2.0", {
  fabric_test_runtime_lane("preview")
  manifest <- fabric_test_manifest()
  lakehouse <- fabric_test_manifest_item(manifest, "TestLakehouse")
  auth <- fabric_test_azure_auth_config()
  batch <- fabric_livy_batch_submit(
    lakehouse$livy_url,
    file = lakehouse$livy_batch_file,
    name = "fabricqueryr-runtime-2-batch",
    args = "success",
    target_lakehouse_id = lakehouse$id,
    tenant_id = auth$tenant_id,
    client_id = auth$client_id,
    auth_args = auth$auth_args,
    verbose = FALSE
  )
  on.exit(try(batch$cancel(), silent = TRUE), add = TRUE)
  batch$wait(timeout = 1200, poll_interval = 5)

  result <- batch$result(refresh = FALSE)
  expect_s3_class(result, "fabric_livy_batch_result")
  expect_identical(result$id, batch$id)
  expect_equal(tolower(result$state), "success")
})
