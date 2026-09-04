# Fabric integration coverage: running Spark code through Fabric Livy
# These tests use the sandbox lakehouse to exercise one-off queries, reusable
# sessions, concurrent sessions, and batch success, failure, and cancellation

test_that("fabric_livy_query executes Spark and returns its output", {
  manifest <- fabric_test_manifest()
  lakehouse <- manifest$items$TestLakehouse
  environment <- fabric_test_manifest_item(manifest, "TestEnvironment")
  table_name <- fabric_test_spark_table(manifest, lakehouse)
  auth <- fabric_test_azure_auth_config()

  result <- expect_no_warning(fabric_livy_query(
    livy_url = lakehouse$livy_url,
    code = sprintf(
      paste0(
        'row_count = spark.sql("SELECT COUNT(*) FROM %s").first()[0]\n',
        'print("FABRICQUERYR_ROW_COUNT=" + str(row_count))\n',
        'print("FABRICQUERYR_SHUFFLE_PARTITIONS=" + ',
        'spark.conf.get("spark.sql.shuffle.partitions"))'
      ),
      table_name
    ),
    kind = "pyspark",
    tenant_id = auth$tenant_id,
    client_id = auth$client_id,
    auth_args = auth$auth_args,
    environment_id = environment$id,
    conf = list("spark.sql.shuffle.partitions" = "2"),
    verbose = FALSE
  ))

  expect_equal(result$state, "available")
  expect_equal(result$output$status, "ok")
  expect_match(
    paste(result$output$parsed, collapse = "\n"),
    "FABRICQUERYR_ROW_COUNT=3",
    fixed = TRUE
  )
  expect_match(
    paste(result$output$parsed, collapse = "\n"),
    "FABRICQUERYR_SHUFFLE_PARTITIONS=2",
    fixed = TRUE
  )
  expect_true(is.finite(result$duration_sec))
  expect_gte(result$duration_sec, 0)
  expect_length(result$id, 1L)
  expect_true(is.numeric(result$id))
  expect_gt(result$id, -1L)
  expect_s3_class(result$started_local, "POSIXct")
  expect_s3_class(result$completed_local, "POSIXct")
  expect_gte(result$completed_local, result$started_local)
  expect_match(
    result$url,
    sprintf("/statements/%s$", result$id)
  )
  expect_true("text/plain" %in% names(result$output$data))
  expect_true(is.numeric(result$output$execution_count))
})

test_that("FabricLivySession shares state and preserves statement failures", {
  manifest <- fabric_test_manifest()
  lakehouse <- fabric_test_manifest_item(manifest, "TestLakehouse")
  auth <- fabric_test_azure_auth_config()
  session <- fabric_livy_session(
    lakehouse$livy_url,
    tenant_id = auth$tenant_id,
    client_id = auth$client_id,
    auth_args = auth$auth_args,
    name = "fabricqueryr-integration-session",
    tags = list(test = "multiple-statements"),
    conf = list("spark.sql.shuffle.partitions" = "2"),
    verbose = FALSE
  )
  on.exit(try(session$close(), silent = TRUE), add = TRUE)
  session$wait(timeout = 900, poll_interval = 5)

  assignment <- session$run(
    "fabricqueryr_shared_value = 40",
    kind = "pyspark",
    timeout = 300,
    poll_interval = 2
  )
  expect_equal(assignment$output$status, "ok")

  if (fabric_test_is_delegated_auth(auth)) {
    discovered <- fabric_test_eventually(
      function() {
        fabric_livy_sessions(
          lakehouse$livy_url,
          tenant_id = auth$tenant_id,
          client_id = auth$client_id,
          auth_args = auth$auth_args
        )
      },
      ready = function(value) session$id %in% value$id
    )
    expect_contains(discovered$id, session$id)
  }
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
    "print('FABRICQUERYR_SHARED_VALUE=' + str(fabricqueryr_shared_value + 2))",
    kind = "pyspark",
    timeout = 300,
    poll_interval = 2
  )
  expect_match(
    paste(reused$output$parsed, collapse = "\n"),
    "FABRICQUERYR_SHARED_VALUE=42",
    fixed = TRUE
  )

  sql_null <- session$run(
    "SELECT 'value' AS present, CAST(NULL AS STRING) AS missing",
    kind = "sql",
    timeout = 300,
    poll_interval = 2
  )
  expect_contains(names(sql_null$output$data), "application/json")
  expect_identical(sql_null$output$parsed$present, "value")
  expect_identical(sql_null$output$parsed$missing, NA_character_)
  expect_null(
    sql_null$output$data[["application/json"]]$data[[1L]][[2L]]
  )

  scala <- session$run(
    "println(\"FABRICQUERYR_SCALA_OK\")",
    kind = "spark",
    timeout = 300,
    poll_interval = 2
  )
  expect_equal(scala$output$status, "ok")
  expect_match(
    paste(scala$output$parsed, collapse = "\n"),
    "FABRICQUERYR_SCALA_OK",
    fixed = TRUE
  )

  sparkr <- session$run(
    "cat(\"FABRICQUERYR_SPARKR_OK\\n\")",
    kind = "sparkr",
    timeout = 300,
    poll_interval = 2
  )
  expect_equal(sparkr$output$status, "ok")
  expect_match(
    paste(sparkr$output$parsed, collapse = "\n"),
    "FABRICQUERYR_SPARKR_OK",
    fixed = TRUE
  )

  sql <- session$run(
    "SELECT 42 AS fabricqueryr_sql_value",
    kind = "sql",
    timeout = 300,
    poll_interval = 2
  )
  expect_equal(sql$output$status, "ok")
  expect_gt(length(sql$output$data), 0L)
  expect_s3_class(sql$output$parsed, "tbl_df")
  expect_equal(nrow(sql$output$parsed), 1L)
  expect_equal(as.character(sql$output$parsed[[1L]][[1L]]), "42")
  expect_match(
    jsonlite::toJSON(sql$output$data, auto_unbox = TRUE),
    "fabricqueryr_sql_value",
    fixed = TRUE
  )
  expect_match(
    jsonlite::toJSON(sql$output$data, auto_unbox = TRUE),
    "42",
    fixed = TRUE
  )

  bigint <- session$run(
    "SELECT CAST(9007199254740993 AS BIGINT) AS fabricqueryr_bigint",
    kind = "sql",
    timeout = 300,
    poll_interval = 2
  )
  expect_identical(
    bigint$output$parsed$fabricqueryr_bigint,
    "9007199254740993"
  )

  integer_boundaries <- session$run(
    paste0(
      "SELECT stack(3, CAST(-2147483648 AS INT), ",
      "CAST(2147483647 AS INT), CAST(NULL AS INT)) ",
      "AS fabricqueryr_int"
    ),
    kind = "sql",
    timeout = 300,
    poll_interval = 2
  )
  expect_identical(
    integer_boundaries$output$parsed$fabricqueryr_int,
    c(-2147483648, 2147483647, NA_real_)
  )

  special_values <- session$run(
    paste0(
      "SELECT CAST('NaN' AS DOUBLE) AS nan_value, ",
      "TIMESTAMP_NTZ '2026-08-10 12:30:01.125' AS local_at"
    ),
    kind = "sql",
    timeout = 300,
    poll_interval = 2
  )
  # Fabric serializes non-finite SQL doubles as JSON null. Preserve the typed
  # missing value instead of guessing whether the source was NaN or SQL NULL
  wire_values <- special_values$output$data[["application/json"]]$data
  expect_null(wire_values[[1L]][[1L]])
  expect_identical(special_values$output$parsed$nan_value, NA_real_)
  expect_identical(
    special_values$output$parsed$local_at,
    "2026-08-10 12:30:01.125"
  )

  statements <- session$statements()
  expect_identical(
    statements$total_statements,
    length(statements$statements)
  )
  statement_ids <- vapply(
    statements$statements,
    `[[`,
    integer(1),
    "id"
  )
  expect_true(all(c(assignment$id, reused$id, sql$id) %in% statement_ids))

  failed <- session$submit(
    "raise RuntimeError('FABRICQUERYR_INTENTIONAL_STATEMENT_FAILURE')",
    kind = "pyspark"
  )
  error <- expect_error(
    failed$wait(timeout = 300, poll_interval = 2),
    class = "fabric_livy_statement_error"
  )
  expect_match(
    paste(
      conditionMessage(error),
      error$output$evalue %||% "",
      error$traceback,
      collapse = "\n"
    ),
    "FABRICQUERYR_INTENTIONAL_STATEMENT_FAILURE",
    fixed = TRUE
  )
  raw_failure <- failed$result(
    refresh = FALSE,
    error_on_failure = FALSE
  )
  expect_equal(raw_failure$output$status, "error")
  expect_gt(length(raw_failure$output$traceback), 0L)

  expect_true(session$close())
  expect_true(session$closed)
  expect_false(session$close())
})

test_that("high-concurrency Livy sessions isolate their REPLs", {
  manifest <- fabric_test_manifest()
  lakehouse <- fabric_test_manifest_item(manifest, "TestLakehouse")
  auth <- fabric_test_azure_auth_config()
  tag <- paste0("fabricqueryr-", manifest$workspace_id)
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

  expect_false(identical(session_a$id, session_b$id))
  expect_false(identical(session_a$repl_id, session_b$repl_id))
  expect_true(nzchar(session_a$session_id))
  expect_true(nzchar(session_b$session_id))
  expect_true(nzchar(session_a$repl_id))
  expect_true(nzchar(session_b$repl_id))

  assigned <- session_a$run(
    "fabricqueryr_hc_secret = 'session-a-only'",
    kind = "pyspark",
    timeout = 300,
    poll_interval = 2
  )
  expect_equal(assigned$output$status, "ok")
  isolated <- session_b$run(
    paste0(
      "print('FABRICQUERYR_HC_VARIABLE_VISIBLE=' + ",
      "str('fabricqueryr_hc_secret' in globals()))"
    ),
    kind = "pyspark",
    timeout = 300,
    poll_interval = 2
  )
  expect_equal(isolated$output$status, "ok")
  expect_match(
    paste(isolated$output$parsed, collapse = "\n"),
    "FABRICQUERYR_HC_VARIABLE_VISIBLE=False",
    fixed = TRUE
  )
  expect_true(session_a$close())
  expect_true(session_b$close())
})

test_that("Livy batches cover success, failure, and cancellation", {
  manifest <- fabric_test_manifest()
  lakehouse <- fabric_test_manifest_item(manifest, "TestLakehouse")
  auth <- fabric_test_azure_auth_config()
  storage_token <- fabric_test_token("FABRIC_TEST_STORAGE_TOKEN")
  marker <- function() {
    fabric_onelake_read_delta_table(
      table_path = lakehouse$tables$livy_batch_result,
      workspace_name = manifest$workspace_id,
      lakehouse_name = lakehouse$id,
      schema = lakehouse$schema,
      token = storage_token,
      verbose = FALSE
    )
  }
  wait_for_marker <- function(expected_mode, timeout = 120) {
    deadline <- Sys.time() + timeout
    repeat {
      value <- try(marker(), silent = TRUE)
      if (
        !inherits(value, "try-error") &&
          nrow(value) == 1L &&
          identical(value$mode[[1L]], expected_mode)
      ) {
        return(value)
      }
      if (Sys.time() >= deadline) {
        rlang::abort(paste(
          "Livy batch marker did not reach mode",
          shQuote(expected_mode),
          "in time"
        ))
      }
      Sys.sleep(2)
    }
  }

  success <- fabric_livy_batch_submit(
    lakehouse$livy_url,
    file = lakehouse$livy_batch_file,
    name = "fabricqueryr-batch-success",
    args = "success",
    target_lakehouse_id = lakehouse$id,
    tenant_id = auth$tenant_id,
    client_id = auth$client_id,
    auth_args = auth$auth_args,
    verbose = FALSE
  )
  on.exit(try(success$cancel(), silent = TRUE), add = TRUE)
  recovered_success <- fabric_livy_batch_attach(
    lakehouse$livy_url,
    success$id,
    tenant_id = auth$tenant_id,
    client_id = auth$client_id,
    auth_args = auth$auth_args,
    verbose = FALSE
  )
  on.exit(try(recovered_success$cancel(), silent = TRUE), add = TRUE)
  expect_identical(recovered_success$id, success$id)
  recovered_success$wait(timeout = 1200, poll_interval = 5)
  success_result <- recovered_success$result(refresh = FALSE)
  expect_s3_class(success_result, "fabric_livy_batch_result")
  expect_identical(success_result$id, success$id)
  expect_equal(tolower(success_result$state), "success")
  success_marker <- wait_for_marker("success")
  expect_equal(success_marker$mode, "success")
  expect_equal(as.numeric(success_marker$row_count), 3)

  failure <- fabric_livy_batch_submit(
    lakehouse$livy_url,
    file = lakehouse$livy_batch_file,
    name = "fabricqueryr-batch-failure",
    args = "failure",
    target_lakehouse_id = lakehouse$id,
    tenant_id = auth$tenant_id,
    client_id = auth$client_id,
    auth_args = auth$auth_args,
    verbose = FALSE
  )
  failure_error <- expect_error(
    failure$wait(timeout = 1200, poll_interval = 5),
    class = "fabric_livy_batch_error"
  )
  expect_equal(tolower(failure_error$batch$state), "dead")
  failure_marker <- wait_for_marker("failure")
  expect_equal(failure_marker$mode, "failure")
  expect_equal(as.numeric(failure_marker$row_count), -1)

  slow <- fabric_livy_batch_submit(
    lakehouse$livy_url,
    file = lakehouse$livy_batch_file,
    name = "fabricqueryr-batch-cancel",
    args = "slow",
    target_lakehouse_id = lakehouse$id,
    tenant_id = auth$tenant_id,
    client_id = auth$client_id,
    auth_args = auth$auth_args,
    verbose = FALSE
  )
  on.exit(try(slow$cancel(), silent = TRUE), add = TRUE)
  deadline <- Sys.time() + 900
  repeat {
    slow_status <- slow$status()
    if (tolower(slow_status$state %||% "") == "running") {
      break
    }
    if (Sys.time() >= deadline) {
      rlang::abort("Slow Livy batch did not start in time")
    }
    Sys.sleep(5)
  }
  slow_marker <- wait_for_marker("slow")
  expect_equal(slow_marker$mode, "slow")
  expect_equal(as.numeric(slow_marker$row_count), -1)
  if (fabric_test_is_delegated_auth(auth)) {
    discovered_batches <- fabric_test_eventually(
      function() {
        fabric_livy_batches(
          lakehouse$livy_url,
          tenant_id = auth$tenant_id,
          client_id = auth$client_id,
          auth_args = auth$auth_args
        )
      },
      ready = function(value) slow$id %in% value$id
    )
    expect_contains(discovered_batches$id, slow$id)
  }
  timeout_error <- expect_error(
    slow$wait(
      timeout = 0,
      poll_interval = 0,
      cancel_on_timeout = TRUE
    ),
    class = "fabric_livy_timeout_error"
  )
  expect_s3_class(timeout_error$batch, "fabric_livy_batch_metadata")
  expect_identical(timeout_error$batch$id, slow$id)
  expect_identical(timeout_error$batch$url, slow$url)
  expect_identical(timeout_error$batch$state, slow$state)
  expect_true(timeout_error$batch$cancel_requested)
  expect_identical(timeout_error$handle, slow)
  expect_true(timeout_error$cancel_accepted)
  expect_null(timeout_error$cancel_error)
  expect_true(slow$cancel_requested)
  slow$wait(
    timeout = 600,
    poll_interval = 5,
    error_on_failure = FALSE
  )
  cancelled <- slow$result(
    refresh = FALSE,
    error_on_failure = FALSE
  )
  terminal <- tolower(c(
    cancelled$state,
    cancelled$result %||% "",
    cancelled$raw$fabricBatchStateInfo$state %||% ""
  ))
  expect_true(any(
    terminal %in%
      c(
        "killed",
        "dead",
        "cancelled",
        "canceled"
      )
  ))
})
