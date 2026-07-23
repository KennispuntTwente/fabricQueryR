test_that("fabric_onelake_read_delta_table reads schema-enabled Delta data", {
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
  result <- result[order(result$id), ]

  expect_s3_class(result, "tbl_df")
  expect_named(
    result,
    c("id", "name", "category", "amount", "loaded_at"),
    ignore.order = TRUE
  )
  expect_equal(nrow(result), 3L)
  expect_equal(result$id, c(1L, 2L, 3L))
  expect_equal(result$name, c("alpha", "beta", "gamma"))
  expect_equal(result$category, c("A", "B", "A"))
  expect_equal(result$amount, c(10.5, 20, NA))
  expect_true(inherits(result$loaded_at, "POSIXct"))
})

test_that("fabric_onelake_read_delta_table preserves Hive partitions", {
  skip_if_not_installed("AzureStor")
  skip_if_not_installed("arrow")
  skip_if_not_installed("readr")
  skip_if_not_installed("fs")
  manifest <- fabric_test_manifest()
  lakehouse <- manifest$items$TestLakehouse

  result <- fabric_onelake_read_delta_table(
    table_path = lakehouse$tables$partitioned,
    workspace_name = manifest$workspace_id,
    lakehouse_name = lakehouse$id,
    schema = lakehouse$schema,
    access_token = fabric_test_token("FABRIC_TEST_STORAGE_TOKEN"),
    verbose = FALSE
  )
  id_counts <- table(result$id)

  expect_s3_class(result, "tbl_df")
  expect_true("category" %in% names(result))
  expect_equal(nrow(result), 13L)
  expect_equal(unname(id_counts[c("1", "2", "3")]), c(11L, 1L, 1L))
  expect_equal(sort(unique(result$category)), c("A", "B"))
})

test_that("fabric_sql_connect opens a usable connection and disconnects", {
  skip_if_not_installed("DBI")
  skip_if_not_installed("odbc")
  manifest <- fabric_test_manifest()
  lakehouse <- manifest$items$TestLakehouse

  con <- fabric_sql_connect(
    server = paste0("Server=tcp:", lakehouse$sql_endpoint),
    database = lakehouse$display_name,
    access_token = fabric_test_token("FABRIC_TEST_SQL_TOKEN"),
    verbose = FALSE
  )
  on.exit(
    if (DBI::dbIsValid(con)) {
      DBI::dbDisconnect(con)
    },
    add = TRUE
  )

  expect_true(DBI::dbIsValid(con))
  result <- DBI::dbGetQuery(
    con,
    paste(
      "SELECT id, name, category, amount",
      "FROM dbo.fabricqueryr_basic",
      "ORDER BY id"
    )
  )
  expect_equal(result$id, c(1L, 2L, 3L))
  expect_equal(result$name, c("alpha", "beta", "gamma"))
  expect_equal(result$amount, c(10.5, 20, NA))

  DBI::dbDisconnect(con)
  expect_false(DBI::dbIsValid(con))
})

test_that("fabric_sql_query returns a tibble with aggregate results", {
  skip_if_not_installed("DBI")
  skip_if_not_installed("odbc")
  manifest <- fabric_test_manifest()
  lakehouse <- manifest$items$TestLakehouse

  result <- fabric_sql_query(
    server = lakehouse$sql_endpoint,
    database = lakehouse$display_name,
    sql = paste(
      "SELECT COUNT(*) AS row_count,",
      "SUM(amount) AS amount_sum",
      "FROM dbo.fabricqueryr_basic"
    ),
    access_token = fabric_test_token("FABRIC_TEST_SQL_TOKEN"),
    verbose = FALSE
  )

  expect_s3_class(result, "tbl_df")
  expect_equal(nrow(result), 1L)
  expect_equal(as.numeric(result$row_count), 3)
  expect_equal(as.numeric(result$amount_sum), 30.5)
})

test_that("fabric_livy_query executes Spark and returns its output", {
  manifest <- fabric_test_manifest()
  lakehouse <- manifest$items$TestLakehouse
  table_name <- fabric_test_spark_table(manifest, lakehouse)

  result <- fabric_livy_query(
    livy_url = lakehouse$livy_url,
    code = sprintf(
      paste0(
        'print("FABRICQUERYR_ROW_COUNT=" + ',
        'str(spark.sql("SELECT COUNT(*) FROM %s").first()[0]))'
      ),
      table_name
    ),
    kind = "pyspark",
    access_token = fabric_test_token("FABRIC_TEST_API_TOKEN"),
    verbose = FALSE
  )

  expect_equal(result$state, "available")
  expect_equal(result$output$status, "ok")
  expect_match(
    paste(result$output$parsed, collapse = "\n"),
    "FABRICQUERYR_ROW_COUNT=3",
    fixed = TRUE
  )
  expect_true(is.finite(result$duration_sec))
  expect_gte(result$duration_sec, 0)
})

test_that("fabric_pbi_dax_query resolves and queries a semantic model", {
  manifest <- fabric_test_manifest()
  semantic_model <- manifest$items$TestSemanticModel

  result <- fabric_pbi_dax_query(
    connstr = semantic_model$connection_string,
    dax = paste0(
      'EVALUATE ROW("row_count", COUNTROWS(\'Facts\'), ',
      '"amount_sum", SUM(\'Facts\'[amount]))'
    ),
    access_token = fabric_test_token("FABRIC_TEST_PBI_TOKEN")
  )

  expect_s3_class(result, "tbl_df")
  expect_equal(nrow(result), 1L)
  expect_equal(as.numeric(result[["[row_count]"]]), 3)
  expect_equal(as.numeric(result[["[amount_sum]"]]), 30.5)
})
