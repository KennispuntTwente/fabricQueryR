test_that("fabric_onelake_read_delta_table reads schema-enabled Delta data", {
  skip_if_not_installed("AzureStor")
  skip_if_not_installed("duckdb")
  skip_if_not_installed("fs")
  manifest <- fabric_test_manifest()
  lakehouse <- manifest$items$TestLakehouse

  result <- fabric_onelake_read_delta_table(
    table_path = lakehouse$tables$basic,
    workspace_name = manifest$workspace_name,
    lakehouse_name = lakehouse$display_name,
    schema = lakehouse$schema,
    tenant_id = "",
    client_id = "",
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
  expect_equal(
    as.numeric(result$loaded_at),
    rep(as.numeric(as.POSIXct("2026-01-01", tz = "UTC")), 3)
  )
})

test_that("fabric_onelake_read_delta_table resolves Delta removals and partitions", {
  skip_if_not_installed("AzureStor")
  skip_if_not_installed("duckdb")
  skip_if_not_installed("fs")
  manifest <- fabric_test_manifest()
  lakehouse <- manifest$items$TestLakehouse
  dest_dir <- tempfile("fabricqueryr-integration-")
  on.exit(
    if (fs::dir_exists(dest_dir)) {
      fs::dir_delete(dest_dir)
    },
    add = TRUE
  )

  result <- fabric_onelake_read_delta_table(
    table_path = lakehouse$tables$partitioned,
    workspace_name = manifest$workspace_id,
    lakehouse_name = lakehouse$id,
    schema = lakehouse$schema,
    tenant_id = "",
    client_id = "",
    access_token = fabric_test_token("FABRIC_TEST_STORAGE_TOKEN"),
    dest_dir = dest_dir,
    verbose = FALSE
  )
  id_counts <- table(result$id)
  replaced <- result[result$id == 2L, ]

  expect_s3_class(result, "tbl_df")
  expect_true("category" %in% names(result))
  expect_equal(nrow(result), 13L)
  expect_equal(as.integer(id_counts[c("1", "2", "3")]), c(11L, 1L, 1L))
  expect_equal(sort(unique(result$category)), c("A", "B"))
  expect_equal(replaced$name, "beta-updated")
  expect_equal(replaced$amount, 21)
  expect_true(fs::dir_exists(fs::path(dest_dir, "category=A")))
  expect_true(fs::dir_exists(fs::path(dest_dir, "category=B")))
  expect_gt(
    length(fs::dir_ls(dest_dir, recurse = TRUE, regexp = "\\.parquet$")),
    0L
  )
  expect_true(
    any(fs::file_exists(
      fs::dir_ls(
        fs::path(dest_dir, "_delta_log"),
        regexp = "checkpoint.*\\.parquet$"
      )
    ))
  )

  historical <- fabric_delta_read_staged(dest_dir, version = 10)
  historical_beta <- historical[historical$id == 2L, ]
  expect_equal(historical_beta$name, "beta")
  expect_equal(historical_beta$amount, 20)
})

test_that("fabric_sql_connect opens a usable connection and disconnects", {
  skip_if_not_installed("DBI")
  skip_if_not_installed("odbc")
  manifest <- fabric_test_manifest()
  lakehouse <- manifest$items$TestLakehouse

  con <- fabric_sql_connect(
    server = paste0("Server=tcp:", lakehouse$sql_endpoint),
    database = lakehouse$display_name,
    tenant_id = "",
    client_id = "",
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
      "SELECT id, name, category, amount, loaded_at",
      "FROM dbo.fabricqueryr_basic",
      "ORDER BY id"
    )
  )
  expect_equal(result$id, c(1L, 2L, 3L))
  expect_equal(result$name, c("alpha", "beta", "gamma"))
  expect_equal(result$category, c("A", "B", "A"))
  expect_equal(result$amount, c(10.5, 20, NA))
  expect_s3_class(result$loaded_at, "POSIXct")
  expect_equal(
    as.numeric(result$loaded_at),
    rep(as.numeric(as.POSIXct("2026-01-01", tz = "UTC")), 3)
  )
  expect_true(
    DBI::dbExistsTable(
      con,
      DBI::Id(schema = "dbo", table = "fabricqueryr_basic")
    )
  )

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
    tenant_id = "",
    client_id = "",
    access_token = fabric_test_token("FABRIC_TEST_SQL_TOKEN"),
    verbose = FALSE
  )

  expect_s3_class(result, "tbl_df")
  expect_equal(nrow(result), 1L)
  expect_equal(as.numeric(result$row_count), 3)
  expect_equal(as.numeric(result$amount_sum), 30.5)

  empty <- fabric_sql_query(
    server = lakehouse$sql_endpoint,
    database = lakehouse$display_name,
    sql = paste(
      "SELECT id, name",
      "FROM dbo.fabricqueryr_basic",
      "WHERE 1 = 0"
    ),
    tenant_id = "",
    client_id = "",
    access_token = fabric_test_token("FABRIC_TEST_SQL_TOKEN"),
    verbose = FALSE
  )
  expect_s3_class(empty, "tbl_df")
  expect_equal(nrow(empty), 0L)
  expect_named(empty, c("id", "name"))
})

test_that("fabric_livy_query executes Spark and returns its output", {
  manifest <- fabric_test_manifest()
  lakehouse <- manifest$items$TestLakehouse
  table_name <- fabric_test_spark_table(manifest, lakehouse)

  result <- fabric_livy_query(
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
    tenant_id = "",
    client_id = "",
    access_token = fabric_test_token("FABRIC_TEST_API_TOKEN"),
    conf = list("spark.sql.shuffle.partitions" = "2"),
    verbose = FALSE
  )

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

test_that("fabric_pbi_dax_query resolves and queries a semantic model", {
  manifest <- fabric_test_manifest()
  semantic_model <- manifest$items$TestSemanticModel

  result <- fabric_pbi_dax_query(
    connstr = semantic_model$connection_string,
    dax = paste0(
      'EVALUATE ROW("row_count", COUNTROWS(\'Facts\'), ',
      '"amount_sum", SUM(\'Facts\'[amount]))'
    ),
    tenant_id = "",
    client_id = "",
    access_token = fabric_test_token("FABRIC_TEST_PBI_TOKEN")
  )

  expect_s3_class(result, "tbl_df")
  expect_equal(nrow(result), 1L)
  expect_equal(as.numeric(result[["[row_count]"]]), 3)
  expect_equal(as.numeric(result[["[amount_sum]"]]), 30.5)

  rows <- fabric_pbi_dax_query(
    connstr = semantic_model$connection_string,
    dax = paste(
      "EVALUATE",
      "SELECTCOLUMNS(",
      "  'Facts',",
      '  "id", \'Facts\'[id],',
      '  "name", \'Facts\'[name],',
      '  "category", \'Facts\'[category],',
      '  "amount", \'Facts\'[amount]',
      ")",
      "ORDER BY [id]",
      sep = "\n"
    ),
    access_token = fabric_test_token("FABRIC_TEST_PBI_TOKEN")
  )
  expect_s3_class(rows, "tbl_df")
  expect_equal(nrow(rows), 3L)
  expect_named(rows, c("[id]", "[name]", "[category]", "[amount]"))
  expect_equal(as.numeric(rows[["[id]"]]), c(1, 2, 3))
  expect_equal(rows[["[name]"]], c("alpha", "beta", "gamma"))
  expect_equal(rows[["[category]"]], c("A", "B", "A"))
  expect_equal(as.numeric(rows[["[amount]"]]), c(10.5, 20, NA))

  empty <- fabric_pbi_dax_query(
    connstr = semantic_model$connection_string,
    dax = paste0(
      "EVALUATE FILTER(",
      "SELECTCOLUMNS('Facts', \"id\", 'Facts'[id]), ",
      "[id] > 100)"
    ),
    access_token = fabric_test_token("FABRIC_TEST_PBI_TOKEN")
  )
  expect_s3_class(empty, "tbl_df")
  expect_equal(nrow(empty), 0L)
})
