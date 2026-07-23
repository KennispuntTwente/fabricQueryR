test_that("OneLake reads the seeded Delta fixture", {
  skip_if_not_installed("AzureStor")
  skip_if_not_installed("arrow")
  skip_if_not_installed("readr")
  skip_if_not_installed("fs")
  manifest <- fabric_test_manifest()
  lakehouse <- manifest$items$TestLakehouse

  result <- fabric_onelake_read_delta_table(
    table_path = lakehouse$tables$basic,
    workspace_name = manifest$workspace_id,
    lakehouse_name = lakehouse$id,
    schema = lakehouse$schema,
    access_token = fabric_test_token("FABRIC_TEST_STORAGE_TOKEN"),
    verbose = FALSE
  )

  expect_equal(nrow(result), 3L)
  expect_equal(result$id, c(1L, 2L, 3L))
  expect_equal(result$name, c("alpha", "beta", "gamma"))
})

test_that("SQL queries the seeded Lakehouse table", {
  skip_if_not_installed("DBI")
  skip_if_not_installed("odbc")
  manifest <- fabric_test_manifest()
  lakehouse <- manifest$items$TestLakehouse

  result <- fabric_sql_query(
    server = lakehouse$sql_endpoint,
    database = "TestLakehouse",
    sql = "SELECT COUNT(*) AS row_count FROM dbo.fabricqueryr_basic",
    access_token = fabric_test_token("FABRIC_TEST_SQL_TOKEN"),
    verbose = FALSE
  )

  expect_equal(result$row_count, 3)
})

test_that("Livy executes code against the seeded Lakehouse", {
  manifest <- fabric_test_manifest()
  lakehouse <- manifest$items$TestLakehouse
  table_name <- paste(
    sprintf(
      "`%s`",
      c(
        manifest$workspace_name,
        lakehouse$display_name,
        lakehouse$schema,
        lakehouse$tables$basic
      )
    ),
    collapse = "."
  )

  result <- fabric_livy_query(
    livy_url = lakehouse$livy_url,
    code = sprintf(
      'spark.sql("SELECT COUNT(*) FROM %s").collect()',
      table_name
    ),
    kind = "pyspark",
    access_token = fabric_test_token("FABRIC_TEST_API_TOKEN"),
    verbose = FALSE
  )

  expect_equal(result$state, "available")
  expect_equal(result$output$status, "ok")
})
