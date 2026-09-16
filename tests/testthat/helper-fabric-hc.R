fabric_test_hc_lifecycle <- function(require_packed = FALSE) {
  manifest <- fabric_test_manifest()
  lakehouse <- fabric_test_manifest_item(manifest, "TestLakehouse")
  auth <- fabric_test_azure_auth_config()
  tag <- paste0("fabricqueryr-", basename(tempfile()), "-", Sys.getpid())
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
  expect_true(nzchar(session_a$session_id))
  expect_true(nzchar(session_b$session_id))
  expect_true(nzchar(session_a$repl_id))
  expect_true(nzchar(session_b$repl_id))
  packed <- identical(session_a$session_id, session_b$session_id)
  if (packed) {
    expect_false(identical(session_a$repl_id, session_b$repl_id))
  }
  if (require_packed && !packed) {
    stop(
      "Packed HC evidence required, but Fabric allocated separate backing sessions"
    )
  }
  first <- session_a$submit(
    "import time; time.sleep(30); print('first')",
    kind = "pyspark"
  )
  second <- session_b$submit(
    "import time; time.sleep(30); print('second')",
    kind = "pyspark"
  )
  states <- fabric_test_eventually(
    function() c(first$status()$state, second$status()$state),
    ready = function(value) length(value) == 2L && all(value == "running"),
    attempts = 20L,
    delay = 1
  )
  expect_identical(states, c("running", "running"))
  first$wait(timeout = 120, poll_interval = 2)
  second$wait(timeout = 120, poll_interval = 2)
  expect_identical(first$result()$output$status, "ok")
  expect_identical(second$result()$output$status, "ok")

  assigned <- session_a$run(
    "fabricqueryr_hc_secret = 'session-a-only'",
    kind = "pyspark",
    timeout = 300,
    poll_interval = 2
  )
  expect_equal(assigned$output$status, "ok")
  isolated <- session_b$run(
    paste0(
      "fabricqueryr_hc_survivor = 'session-b-only'\n",
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
  recovered <- fabric_livy_session_attach(
    lakehouse$livy_url,
    toupper(session_a$id),
    high_concurrency = TRUE,
    tenant_id = auth$tenant_id,
    client_id = auth$client_id,
    auth_args = auth$auth_args,
    verbose = FALSE
  )
  expect_identical(recovered$id, session_a$id)
  expect_identical(recovered$repl_id, session_a$repl_id)
  # A driver-side sleep cannot be reliably interrupted by Livy. Cancel an
  # executor task instead, then verify cancellation actually completed.
  slow <- recovered$submit(
    paste0(
      "import time\n",
      "spark.sparkContext.parallelize([1], 1).foreach(",
      "lambda _: time.sleep(120))"
    ),
    kind = "pyspark"
  )
  fabric_test_eventually(
    function() slow$status()$state,
    ready = function(state) identical(state, "running"),
    attempts = 30L,
    delay = 1
  )
  if (packed) {
    fabric_test_eventually(
      function() {
        active <- session_b$run(
          paste0(
            "print('FABRICQUERYR_HC_JOB_ACTIVE=' + ",
            "str(len(spark.sparkContext.statusTracker().getActiveJobsIds()) > 0))"
          ),
          kind = "pyspark",
          timeout = 60,
          poll_interval = 1
        )
        paste(active$output$parsed, collapse = "\n")
      },
      ready = function(value) {
        grepl("FABRICQUERYR_HC_JOB_ACTIVE=True", value, fixed = TRUE)
      },
      attempts = 30L,
      delay = 1
    )
  }
  slow$cancel()
  slow$wait(timeout = 300, poll_interval = 2, error_on_failure = FALSE)
  expect_identical(slow$state, "cancelled")
  other <- session_b$run(
    "print('continued-after-cancel=' + fabricqueryr_hc_survivor)",
    kind = "pyspark",
    timeout = 300
  )
  expect_match(
    paste(other$output$parsed, collapse = ""),
    "continued-after-cancel=session-b-only",
    fixed = TRUE
  )
  expect_true(session_a$close())
  continued <- session_b$run(
    "print('still-running=' + fabricqueryr_hc_survivor)",
    kind = "pyspark",
    timeout = 300,
    poll_interval = 2
  )
  expect_match(
    paste(continued$output$parsed, collapse = "\n"),
    "still-running=session-b-only",
    fixed = TRUE
  )
  expect_true(session_b$close())
}
