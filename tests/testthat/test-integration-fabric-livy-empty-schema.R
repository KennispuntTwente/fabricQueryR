# Fabric integration coverage: Livy results without columns
test_that("Livy SQL retains rows after all Spark columns are dropped", {
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
  session$run(
    "spark.range(3).drop('id').createOrReplaceTempView('fabricqueryr_empty_schema')",
    kind = "pyspark",
    timeout = 300
  )
  rows <- session$run(
    "SELECT * FROM fabricqueryr_empty_schema",
    kind = "sql",
    timeout = 300
  )
  expect_identical(rows$output$status, "ok")
  expect_identical(dim(rows$output$parsed), c(3L, 0L))
  empty <- session$run(
    "SELECT * FROM fabricqueryr_empty_schema LIMIT 0",
    kind = "sql",
    timeout = 300
  )
  expect_identical(dim(empty$output$parsed), c(0L, 0L))
})
