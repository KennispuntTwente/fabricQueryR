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

test_that("Livy executes PySpark and reads Lakehouse data on Runtime 2.0", {
  fabric_test_runtime_lane("runtime2")
  manifest <- fabric_test_manifest()
  lakehouse <- fabric_test_manifest_item(manifest, "TestLakehouse")
  table_name <- fabric_test_spark_table(manifest, lakehouse)
  auth <- fabric_test_azure_auth_config()

  result <- fabric_livy_query(
    livy_url = lakehouse$livy_url,
    code = sprintf(
      paste0(
        "import sys\n",
        'row_count = spark.sql("SELECT COUNT(*) FROM %s").first()[0]\n',
        'print("FABRICQUERYR_RUNTIME_2_ROW_COUNT=" + str(row_count))\n',
        'print("FABRICQUERYR_RUNTIME_2_SPARK=" + spark.version)\n',
        'print("FABRICQUERYR_RUNTIME_2_SHUFFLE=" + ',
        'spark.conf.get("spark.sql.shuffle.partitions"))\n',
        'print("FABRICQUERYR_RUNTIME_2_PYTHON=" + ',
        "'.'.join(map(str, sys.version_info[:3])))"
      ),
      table_name
    ),
    kind = "pyspark",
    conf = list("spark.sql.shuffle.partitions" = "2"),
    tenant_id = auth$tenant_id,
    client_id = auth$client_id,
    auth_args = auth$auth_args,
    verbose = FALSE
  )

  expect_equal(result$state, "available")
  expect_equal(result$output$status, "ok")
  expect_match(
    paste(result$output$parsed, collapse = "\n"),
    "FABRICQUERYR_RUNTIME_2_ROW_COUNT=3",
    fixed = TRUE
  )
  output <- paste(result$output$parsed, collapse = "\n")
  expect_match(output, "FABRICQUERYR_RUNTIME_2_SPARK=4[.]1[.]")
  expect_match(output, "FABRICQUERYR_RUNTIME_2_SHUFFLE=2", fixed = TRUE)
  expect_match(output, "FABRICQUERYR_RUNTIME_2_PYTHON=3[.]13[.]")
  expect_identical(manifest$runtime$fabric_runtime, "2.0")
  expect_match(manifest$runtime$spark_version, "^4[.]1[.]")
  expect_match(manifest$runtime$delta_version, "^4[.]2[.]")
})

test_that("Livy sessions exercise supported languages on Runtime 2.0", {
  fabric_test_runtime_lane("runtime2")
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

  discovered <- fabric_test_eventually(function() {
    value <- fabric_livy_sessions(
      lakehouse$livy_url,
      tenant_id = auth$tenant_id,
      client_id = auth$client_id,
      auth_args = auth$auth_args
    )
    if (!session$id %in% value$id) {
      return(NULL)
    }
    value
  })
  expect_contains(discovered$id, session$id)
  recovered <- fabric_livy_session_attach(
    lakehouse$livy_url,
    session$id,
    tenant_id = auth$tenant_id,
    client_id = auth$client_id,
    auth_args = auth$auth_args,
    verbose = FALSE
  )
  on.exit(try(recovered$close(), silent = TRUE), add = TRUE)
  expect_identical(recovered$id, session$id)

  reused <- recovered$run(
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

  sparkr <- recovered$run(
    paste0(
      "cat(paste0('FABRICQUERYR_RUNTIME_2_R=', ",
      "R.version$major, '.', R.version$minor, '\\n'))"
    ),
    kind = "sparkr",
    timeout = 300,
    poll_interval = 2
  )
  expect_equal(sparkr$output$status, "ok")
  expect_match(
    paste(sparkr$output$parsed, collapse = "\n"),
    "FABRICQUERYR_RUNTIME_2_R=4[.]5[.]"
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

  statements <- recovered$statements()
  statement_ids <- vapply(
    statements$statements,
    `[[`,
    integer(1),
    "id"
  )
  expect_identical(
    statements$total_statements,
    length(statements$statements)
  )
  expect_contains(
    statement_ids,
    c(assignment$id, reused$id, sparkr$id, scala$id, sql$id)
  )

  failed <- session$submit(
    "raise RuntimeError('FABRICQUERYR_RUNTIME_2_FAILURE')",
    kind = "pyspark"
  )
  error <- tryCatch(
    failed$wait(timeout = 300, poll_interval = 2),
    error = identity
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
  fabric_test_runtime_lane("runtime2")
  manifest <- fabric_test_manifest()
  lakehouse <- fabric_test_manifest_item(manifest, "TestLakehouse")
  auth <- fabric_test_azure_auth_config()
  tag <- paste0("fabricqueryr-runtime-2-", manifest$workspace_id)
  session_a <- fabric_livy_session(
    lakehouse$livy_url,
    high_concurrency = TRUE,
    session_tag = tag,
    artifact_name = lakehouse$display_name,
    tenant_id = auth$tenant_id,
    client_id = auth$client_id,
    auth_args = auth$auth_args,
    verbose = FALSE
  )
  on.exit(try(session_a$close(), silent = TRUE), add = TRUE)
  session_a$wait(timeout = 900, poll_interval = 5)
  session_b <- fabric_livy_session(
    lakehouse$livy_url,
    high_concurrency = TRUE,
    session_tag = tag,
    artifact_name = lakehouse$display_name,
    tenant_id = auth$tenant_id,
    client_id = auth$client_id,
    auth_args = auth$auth_args,
    verbose = FALSE
  )
  on.exit(try(session_b$close(), silent = TRUE), add = TRUE)
  session_b$wait(timeout = 900, poll_interval = 5)

  assignment <- session_a$run(
    "fabricqueryr_runtime_2_hc_secret = 'session-a-only'",
    kind = "pyspark",
    timeout = 300,
    poll_interval = 2
  )
  expect_equal(assignment$output$status, "ok")
  recovered_a <- fabric_livy_session_attach(
    lakehouse$livy_url,
    session_a$id,
    high_concurrency = TRUE,
    tenant_id = auth$tenant_id,
    client_id = auth$client_id,
    auth_args = auth$auth_args,
    verbose = FALSE
  )
  on.exit(try(recovered_a$close(), silent = TRUE), add = TRUE)
  expect_identical(recovered_a$id, session_a$id)
  expect_identical(recovered_a$session_id, session_a$session_id)
  expect_identical(recovered_a$repl_id, session_a$repl_id)
  recovered <- recovered_a$run(
    "print('FABRICQUERYR_RUNTIME_2_HC_RECOVERED=' + fabricqueryr_runtime_2_hc_secret)",
    kind = "pyspark",
    timeout = 300,
    poll_interval = 2
  )
  expect_match(
    paste(recovered$output$parsed, collapse = "\n"),
    "FABRICQUERYR_RUNTIME_2_HC_RECOVERED=session-a-only",
    fixed = TRUE
  )
  isolated <- session_b$run(
    paste0(
      "print('FABRICQUERYR_RUNTIME_2_HC_VISIBLE=' + ",
      "str('fabricqueryr_runtime_2_hc_secret' in globals()))"
    ),
    kind = "pyspark",
    timeout = 300,
    poll_interval = 2
  )
  expect_equal(isolated$output$status, "ok")
  expect_match(
    paste(isolated$output$parsed, collapse = "\n"),
    "FABRICQUERYR_RUNTIME_2_HC_VISIBLE=False",
    fixed = TRUE
  )
  expect_false(identical(session_a$id, session_b$id))
  expect_false(identical(session_a$repl_id, session_b$repl_id))
  identifiers <- c(
    session_a$session_id,
    session_b$session_id,
    session_a$repl_id,
    session_b$repl_id
  )
  expect_type(identifiers, "character")
  expect_length(identifiers, 4L)
  expect_gt(min(nchar(identifiers)), 0L)
})

test_that("Livy batches complete on Runtime 2.0", {
  fabric_test_runtime_lane("runtime2")
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
  discovered <- fabric_test_eventually(function() {
    value <- fabric_livy_batches(
      lakehouse$livy_url,
      tenant_id = auth$tenant_id,
      client_id = auth$client_id,
      auth_args = auth$auth_args
    )
    if (!batch$id %in% value$id) {
      return(NULL)
    }
    value
  })
  expect_contains(discovered$id, batch$id)
  recovered <- fabric_livy_batch_attach(
    lakehouse$livy_url,
    batch$id,
    tenant_id = auth$tenant_id,
    client_id = auth$client_id,
    auth_args = auth$auth_args,
    verbose = FALSE
  )
  on.exit(try(recovered$cancel(), silent = TRUE), add = TRUE)
  expect_identical(recovered$id, batch$id)
  recovered$wait(timeout = 1200, poll_interval = 5)

  result <- recovered$result(refresh = FALSE)
  expect_s3_class(result, "fabric_livy_batch_result")
  expect_identical(result$id, batch$id)
  expect_equal(tolower(result$state), "success")
})
