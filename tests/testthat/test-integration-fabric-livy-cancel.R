# Fabric integration coverage: ordinary Livy statement cancellation
test_that("ordinary Livy sessions remain usable after statement cancellation", {
  manifest <- fabric_test_manifest()
  lakehouse <- fabric_test_manifest_item(manifest, "TestLakehouse")
  session <- do.call(
    fabric_livy_session,
    c(
      list(livy_url = lakehouse$livy_url, verbose = FALSE),
      fabric_test_azure_auth_config()
    )
  )
  withr::defer(try(session$close(), silent = TRUE))
  session$wait(timeout = 900, poll_interval = 5)
  session$run("fabricqueryr_survivor = 'ordinary-session'", kind = "pyspark")
  # An executor task is interruptible; driver-side time.sleep is not reliable.
  slow <- session$submit(
    paste0(
      "import time\n",
      "spark.sparkContext.parallelize([1], 1).foreach(lambda _: time.sleep(120))"
    ),
    kind = "pyspark"
  )
  fabric_test_eventually(
    function() slow$status()$state,
    ready = function(state) identical(state, "running"),
    attempts = 30L,
    delay = 1
  )
  slow$cancel()
  slow$wait(timeout = 300, poll_interval = 2, error_on_failure = FALSE)
  expect_identical(slow$state, "cancelled")
  result <- session$run(
    "print(fabricqueryr_survivor + ':' + str(spark.range(3).count()))",
    kind = "pyspark",
    timeout = 300
  )
  expect_identical(result$output$status, "ok")
  expect_match(
    paste(result$output$parsed, collapse = ""),
    "ordinary-session:3",
    fixed = TRUE
  )
})
