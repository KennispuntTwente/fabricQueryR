test_that("Fabric discovery resolves sandbox workspaces and item targets", {
  manifest <- fabric_test_manifest()
  token <- fabric_test_token("FABRIC_TEST_API_TOKEN")

  workspaces <- fabric_workspaces(access_token = token)
  workspace <- workspaces[workspaces$id == manifest$workspace_id, ]
  expect_equal(nrow(workspace), 1L)
  expect_equal(workspace$displayName, manifest$workspace_name)

  items <- fabric_items(
    workspace,
    access_token = token
  )
  expect_true(manifest$items$TestLakehouse$id %in% items$id)
  expect_true(manifest$items$SeedFixtures$id %in% items$id)
  expect_true(manifest$items$TestWarehouse$id %in% items$id)
  expect_true(manifest$items$TestSQLDatabase$id %in% items$id)
  expect_true(manifest$items$TestEventhouse$id %in% items$id)
  expect_true(manifest$items$TestKQLDatabase$id %in% items$id)

  lakehouses <- fabric_lakehouses(workspace, access_token = token)
  lakehouse <- lakehouses[
    lakehouses$id == manifest$items$TestLakehouse$id,
  ]
  expect_equal(nrow(lakehouse), 1L)
  expect_equal(
    lakehouse$sql_server,
    manifest$items$TestLakehouse$sql_endpoint
  )
  expect_equal(
    lakehouse$one_lake_tables_path,
    manifest$items$TestLakehouse$one_lake_tables_path
  )
  expect_equal(lakehouse$livy_url, manifest$items$TestLakehouse$livy_url)

  warehouses <- fabric_warehouses(workspace, access_token = token)
  warehouse <- warehouses[
    warehouses$id == manifest$items$TestWarehouse$id,
  ]
  expect_equal(nrow(warehouse), 1L)
  expect_equal(
    warehouse$sql_server,
    manifest$items$TestWarehouse$connection_string
  )
  expect_equal(
    warehouse$sql_database,
    manifest$items$TestWarehouse$database_name
  )

  sql_databases <- fabric_sql_databases(workspace, access_token = token)
  sql_database <- sql_databases[
    sql_databases$id == manifest$items$TestSQLDatabase$id,
  ]
  expect_equal(nrow(sql_database), 1L)
  expect_equal(
    sql_database$sql_connection_string,
    manifest$items$TestSQLDatabase$connection_string
  )
  expect_equal(
    sql_database$sql_server,
    manifest$items$TestSQLDatabase$server_fqdn
  )
  expect_equal(
    sql_database$sql_database,
    manifest$items$TestSQLDatabase$database_name
  )

  model <- fabric_item(
    workspace,
    manifest$items$TestSemanticModel$id,
    type = "SemanticModel",
    access_token = token
  )
  expect_equal(model$id, manifest$items$TestSemanticModel$id)
  expect_equal(model$workspaceId, manifest$workspace_id)
  expect_match(model$dax_connection_string, "powerbi://", fixed = TRUE)

  eventhouses <- fabric_eventhouses(workspace, access_token = token)
  eventhouse <- eventhouses[
    eventhouses$id == manifest$items$TestEventhouse$id,
  ]
  expect_equal(nrow(eventhouse), 1L)
  expect_equal(
    eventhouse$query_service_uri,
    manifest$items$TestEventhouse$query_service_uri
  )

  kql_databases <- fabric_kql_databases(workspace, access_token = token)
  kql_database <- kql_databases[
    kql_databases$id == manifest$items$TestKQLDatabase$id,
  ]
  expect_equal(nrow(kql_database), 1L)
  expect_equal(
    kql_database$query_service_uri,
    manifest$items$TestKQLDatabase$query_service_uri
  )
})

test_that("fabric_kql_query returns typed seeded Eventhouse data", {
  manifest <- fabric_test_manifest()
  database <- fabric_test_manifest_item(manifest, "TestKQLDatabase")
  result <- fabric_kql_query(
    database$query_service_uri,
    query = paste(
      database$tables$events,
      "| order by id asc"
    ),
    database = database$database_name,
    token_provider = function(audience, force_refresh = FALSE) {
      expect_equal(audience, "https://api.kusto.windows.net/.default")
      fabric_test_token("FABRIC_TEST_KUSTO_TOKEN")
    }
  )

  expect_s3_class(result, "tbl_df")
  expect_named(
    result,
    c(
      "id", "name", "category", "amount", "observed_at",
      "active", "correlation_id", "metadata"
    )
  )
  expect_equal(result$id, c(1L, 2L, 3L))
  expect_equal(result$name, c("alpha", "beta", "gamma"))
  expect_equal(result$category, c("A", "B", "A"))
  expect_equal(result$amount, c(10.5, 20, NA))
  expect_s3_class(result$observed_at, "POSIXct")
  expect_equal(
    as.Date(result$observed_at),
    as.Date(c("2026-01-01", "2026-01-02", "2026-01-03"))
  )
  expect_equal(result$active, c(TRUE, FALSE, TRUE))
  expect_equal(
    result$correlation_id,
    c(
      "11111111-1111-1111-1111-111111111111",
      "22222222-2222-2222-2222-222222222222",
      "33333333-3333-3333-3333-333333333333"
    )
  )
  expect_equal(result$metadata[[1L]]$source, "sandbox")
  expect_equal(result$metadata[[1L]]$rank, 1L)
})

test_that("fabric_kql_query discovers targets and binds safe parameters", {
  manifest <- fabric_test_manifest()
  provisioned <- fabric_test_manifest_item(manifest, "TestKQLDatabase")
  kusto_token <- fabric_test_token("FABRIC_TEST_KUSTO_TOKEN")
  target <- fabric_item(
    manifest$workspace_id,
    provisioned$id,
    type = "KQLDatabase",
    access_token = fabric_test_token("FABRIC_TEST_API_TOKEN")
  )

  selected <- fabric_kql_query(
    target,
    query = paste(
      "declare query_parameters(selected_category:string);",
      provisioned$tables$events,
      "| where category == selected_category",
      "| order by id asc"
    ),
    parameters = list(selected_category = "A"),
    access_token = kusto_token
  )
  expect_equal(selected$id, c(1L, 3L))

  hostile <- fabric_kql_query(
    target,
    query = paste(
      "declare query_parameters(selected_name:string);",
      provisioned$tables$events,
      "| where name == selected_name"
    ),
    parameters = list(
      selected_name = "alpha'; drop table fabricqueryr_events; --"
    ),
    access_token = kusto_token
  )
  expect_s3_class(hostile, "tbl_df")
  expect_equal(nrow(hostile), 0L)

  still_present <- fabric_kql_query(
    target,
    query = paste(provisioned$tables$events, "| count"),
    access_token = kusto_token
  )
  expect_equal(as.numeric(still_present$Count), 3)
})

test_that("fabric_kql_query returns multiple live primary tables", {
  manifest <- fabric_test_manifest()
  database <- fabric_test_manifest_item(manifest, "TestKQLDatabase")
  table <- database$tables$events
  result <- fabric_kql_query(
    database$query_service_uri,
    query = paste0(
      table,
      " | summarize row_count=count(); ",
      table,
      " | summarize amount_sum=sum(amount)"
    ),
    database = database$database_name,
    access_token = fabric_test_token("FABRIC_TEST_KUSTO_TOKEN")
  )

  expect_s3_class(result, "fabric_kql_tables")
  expect_length(result, 2L)
  expect_equal(as.numeric(result[[1L]]$row_count), 3)
  expect_equal(result[[2L]]$amount_sum, 30.5)
})

test_that("fabric_kql_query surfaces live Kusto service errors", {
  manifest <- fabric_test_manifest()
  database <- fabric_test_manifest_item(manifest, "TestKQLDatabase")
  expect_error(
    fabric_kql_query(
      database$query_service_uri,
      query = "fabricqueryr_table_that_does_not_exist | take 1",
      database = database$database_name,
      access_token = fabric_test_token("FABRIC_TEST_KUSTO_TOKEN")
    ),
    "(?i)(failed|HTTP 4)"
  )
})

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
    token_provider = function(audience, force_refresh = FALSE) {
      expect_equal(audience, "https://storage.azure.com/.default")
      fabric_test_token("FABRIC_TEST_STORAGE_TOKEN")
    },
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
  target <- fabric_item(
    manifest$workspace_id,
    lakehouse$id,
    type = "Lakehouse",
    access_token = fabric_test_token("FABRIC_TEST_API_TOKEN")
  )

  con <- fabric_sql_connect(
    server = target,
    tenant_id = "",
    client_id = "",
    token_provider = function(audience, force_refresh = FALSE) {
      expect_equal(audience, "https://database.windows.net/.default")
      fabric_test_token("FABRIC_TEST_SQL_TOKEN")
    },
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

  metacharacters <- "Robert'); DROP TABLE dbo.fabricqueryr_basic;--"
  bound <- fabric_sql_query(
    server = paste0(
      "Server=tcp:",
      lakehouse$sql_endpoint,
      ";Initial Catalog=",
      lakehouse$display_name,
      ";MultipleActiveResultSets=False"
    ),
    sql = paste(
      "SELECT CAST(? AS nvarchar(200)) AS text_value,",
      "CAST(? AS date) AS date_value,",
      "CAST(? AS nvarchar(20)) AS null_value"
    ),
    params = list(
      metacharacters,
      as.Date("2026-07-24"),
      NA_character_
    ),
    access_token = fabric_test_token("FABRIC_TEST_SQL_TOKEN"),
    verbose = FALSE
  )
  expect_equal(bound$text_value, metacharacters)
  expect_equal(as.Date(bound$date_value), as.Date("2026-07-24"))
  expect_true(is.na(bound$null_value))
  still_present <- fabric_sql_query(
    server = lakehouse$sql_endpoint,
    database = lakehouse$display_name,
    sql = "SELECT COUNT(*) AS row_count FROM dbo.fabricqueryr_basic",
    access_token = fabric_test_token("FABRIC_TEST_SQL_TOKEN"),
    verbose = FALSE
  )
  expect_equal(as.numeric(still_present$row_count), 3)
})

fabric_test_sql_item <- function(name) {
  skip_if_not_installed("DBI")
  skip_if_not_installed("odbc")
  manifest <- fabric_test_manifest()
  api_token <- fabric_test_token("FABRIC_TEST_API_TOKEN")
  sql_token <- fabric_test_token("FABRIC_TEST_SQL_TOKEN")

  provisioned <- fabric_test_manifest_item(manifest, name)
  target <- fabric_item(
    manifest$workspace_id,
    provisioned$id,
    type = provisioned$type,
    access_token = api_token
  )
  result <- fabric_sql_query(
    target,
    "SELECT CAST(? AS int) AS bound_value",
    params = list(42L),
    access_token = sql_token,
    verbose = FALSE
  )
  expect_equal(result$bound_value, 42L, info = name)

  info <- fabric_sql_connection_info(target)
  expect_equal(info$database, provisioned$database_name, info = name)
  expect_equal(
    info$target_type,
    if (identical(name, "TestWarehouse")) "warehouse" else "sql_database",
    info = name
  )

  from_manifest <- fabric_sql_query(
    provisioned$connection_string,
    "SELECT CAST(? AS nvarchar(100)) AS bound_value",
    params = list("safe ' value; --"),
    database = if (identical(name, "TestWarehouse")) {
      provisioned$database_name
    } else {
      NULL
    },
    access_token = sql_token,
    verbose = FALSE
  )
  expect_equal(from_manifest$bound_value, "safe ' value; --", info = name)
}

test_that("provisioned Warehouse target is discoverable and connectable", {
  fabric_test_sql_item("TestWarehouse")
})

test_that("provisioned SQL Database target is discoverable and connectable", {
  fabric_test_sql_item("TestSQLDatabase")
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
    token_provider = function(audience, force_refresh = FALSE) {
      expect_equal(audience, "https://api.fabric.microsoft.com/.default")
      fabric_test_token("FABRIC_TEST_API_TOKEN")
    },
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
    token_provider = function(audience, force_refresh = FALSE) {
      expect_equal(
        audience,
        "https://analysis.windows.net/powerbi/api/.default"
      )
      fabric_test_token("FABRIC_TEST_PBI_TOKEN")
    }
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

  discovered_model <- fabric_item(
    manifest$workspace_id,
    semantic_model$id,
    type = "SemanticModel",
    access_token = fabric_test_token("FABRIC_TEST_API_TOKEN")
  )
  by_id <- fabric_pbi_dax_query(
    connstr = discovered_model,
    dax = 'EVALUATE ROW("row_count", COUNTROWS(\'Facts\'))',
    access_token = fabric_test_token("FABRIC_TEST_PBI_TOKEN")
  )
  expect_s3_class(by_id, "tbl_df")
  expect_equal(as.numeric(by_id[["[row_count]"]]), 3)
})
