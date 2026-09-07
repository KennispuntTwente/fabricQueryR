# Fabric integration coverage: Fabric SQL endpoints and query clients
# These tests connect to seeded Lakehouse, Warehouse, and SQL Database targets
# and verify discovery, DBI/ODBC queries, typed results, and Arrow streaming

test_that("SQL integration rejects parser-confused ADBC endpoints before auth", {
  acquired <- FALSE
  local_mocked_bindings(
    fabric_sql_require_backend = function(...) invisible(TRUE),
    fabric_sql_load_adbc_driver = function(...) list()
  )

  error <- tryCatch(
    fabric_sql_connect(
      "attacker.example/path.datawarehouse.fabric.microsoft.com",
      target_type = "warehouse",
      backend = "adbc",
      token = function(...) {
        acquired <<- TRUE
        "must-not-be-returned"
      },
      verbose = FALSE
    ),
    error = identity
  )

  expect_s3_class(error, "fabric_sql_endpoint_error")
  expect_identical(acquired, FALSE)
})

test_that("fabric_sql_query acquires a live SQL token through AzureAuth", {
  backend <- fabric_test_sql_backends()[[1L]]
  manifest <- fabric_test_manifest()
  lakehouse <- manifest$items$TestLakehouse
  auth <- fabric_test_azure_auth_config()

  result <- fabric_sql_query(
    numeric_policy = "driver",
    server = lakehouse$sql_endpoint,
    database = lakehouse$display_name,
    sql = "SELECT CAST(1 AS int) AS authenticated",
    backend = backend,
    tenant_id = auth$tenant_id,
    client_id = auth$client_id,
    token = NULL,
    auth_args = auth$auth_args,
    verbose = FALSE
  )

  expect_identical(as.numeric(result$authenticated), 1)
})

test_that("fabric_sql_connect opens a usable connection and disconnects", {
  backends <- fabric_test_sql_backends()
  manifest <- fabric_test_manifest()
  lakehouse <- manifest$items$TestLakehouse
  token <- fabric_test_token_provider()
  target <- fabric_item(
    manifest$workspace_id,
    lakehouse$id,
    type = "Lakehouse",
    token = fabric_test_token("FABRIC_TEST_API_TOKEN")
  )

  for (backend in backends) {
    con <- fabric_sql_connect(
      server = target,
      backend = backend,
      tenant_id = "",
      client_id = "",
      token = token,
      verbose = FALSE
    )
    expect_true(DBI::dbIsValid(con), info = backend)
    result <- DBI::dbGetQuery(
      con,
      paste(
        "SELECT id, name, category, amount, loaded_at",
        "FROM dbo.fabricqueryr_basic",
        "ORDER BY id"
      )
    )
    expect_equal(result$id, c(1L, 2L, 3L), info = backend)
    expect_equal(result$name, c("alpha", "beta", "gamma"), info = backend)
    expect_equal(result$category, c("A", "B", "A"), info = backend)
    expect_equal(result$amount, c(10.5, 20, NA), info = backend)
    expect_s3_class(result$loaded_at, "POSIXct")
    expect_equal(
      as.numeric(result$loaded_at),
      rep(as.numeric(as.POSIXct("2026-01-01", tz = "UTC")), 3),
      info = backend
    )
    expect_true(
      DBI::dbExistsTable(
        con,
        DBI::Id(schema = "dbo", table = "fabricqueryr_basic")
      ),
      info = backend
    )

    # adbi's dbExistsTable() leaves its temporary metadata stream to GC.
    # Finalize that stream before checking for outstanding connection handles.
    if (identical(backend, "adbc")) {
      invisible(gc())
    }
    disconnected <- NULL
    expect_silent(disconnected <- DBI::dbDisconnect(con))
    expect_true(isTRUE(disconnected), info = backend)
    # ADBC Driver Foundry 1.x can keep reporting released handles as valid.
    # Require a quiet successful disconnect instead of masking child leaks.
    if (!identical(backend, "adbc")) {
      expect_false(DBI::dbIsValid(con), info = backend)
    }
  }
})

test_that("SQL backends bind factor labels consistently", {
  manifest <- fabric_test_manifest()
  fixture <- fabric_test_manifest_item(manifest, "TestLakehouse")
  token <- fabric_test_token_provider()
  target <- fabric_item(
    manifest$workspace_id,
    fixture$id,
    type = "Lakehouse",
    token = token
  )
  for (backend in fabric_test_sql_backends()) {
    value <- fabric_sql_query(
      numeric_policy = "driver",
      target,
      "SELECT CAST(? AS varchar(20)) AS value",
      params = list(factor("alpha", levels = c("other", "alpha"))),
      backend = backend,
      token = token,
      verbose = FALSE
    )
    expect_identical(value$value, "alpha", info = backend)
  }
})

test_that("ADBC default connections accept ordinary large BIGINT values", {
  fabric_test_require_package("adbi")
  manifest <- fabric_test_manifest()
  fixture <- fabric_test_manifest_item(manifest, "TestLakehouse")
  target <- fabric_item(
    manifest$workspace_id,
    fixture$id,
    type = "Lakehouse",
    token = fabric_test_token_provider()
  )
  con <- fabric_sql_connect(
    target,
    backend = "adbc",
    token = fabric_test_token_provider(),
    verbose = FALSE
  )
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  value <- DBI::dbGetQuery(con, "SELECT CAST('2147483648' AS bigint) AS v")
  expect_identical(as.character(value$v), "2147483648")
})

test_that("fabric_sql_query returns tibbles and consumable Arrow streams", {
  backends <- fabric_test_sql_backends()
  manifest <- fabric_test_manifest()
  lakehouse <- manifest$items$TestLakehouse

  for (backend in backends) {
    result <- fabric_sql_query(
      numeric_policy = "driver",
      server = lakehouse$sql_endpoint,
      database = lakehouse$display_name,
      sql = paste(
        "SELECT COUNT(*) AS row_count,",
        "SUM(amount) AS amount_sum",
        "FROM dbo.fabricqueryr_basic"
      ),
      backend = backend,
      tenant_id = "",
      client_id = "",
      token = fabric_test_token("FABRIC_TEST_SQL_TOKEN"),
      verbose = FALSE
    )

    expect_s3_class(result, "tbl_df")
    expect_equal(nrow(result), 1L, info = backend)
    expect_equal(as.numeric(result$row_count), 3, info = backend)
    expect_equal(as.numeric(result$amount_sum), 30.5, info = backend)

    nested_comment <- fabric_sql_query(
      numeric_policy = "driver",
      server = lakehouse$sql_endpoint,
      database = lakehouse$display_name,
      sql = paste0(
        "SELECT /* outer /* SELECT * INTO dbo.fabricqueryr_hidden */ ",
        "outer */ CAST(42 AS int) AS value"
      ),
      backend = backend,
      token = fabric_test_token("FABRIC_TEST_SQL_TOKEN"),
      verbose = FALSE
    )
    expect_equal(as.numeric(nested_comment$value), 42, info = backend)

    empty <- fabric_sql_query(
      numeric_policy = "driver",
      server = lakehouse$sql_endpoint,
      database = lakehouse$display_name,
      sql = paste(
        "SELECT id, name",
        "FROM dbo.fabricqueryr_basic",
        "WHERE 1 = 0"
      ),
      backend = backend,
      token = fabric_test_token("FABRIC_TEST_SQL_TOKEN"),
      verbose = FALSE
    )
    expect_s3_class(empty, "tbl_df")
    expect_equal(nrow(empty), 0L, info = backend)
    expect_named(empty, c("id", "name"), info = backend)

    metacharacters <- "Robert'); DROP TABLE dbo.fabricqueryr_basic;--"
    bound <- fabric_sql_query(
      numeric_policy = "driver",
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
      backend = backend,
      token = fabric_test_token("FABRIC_TEST_SQL_TOKEN"),
      verbose = FALSE
    )
    expect_equal(bound$text_value, metacharacters, info = backend)
    expect_equal(
      as.Date(bound$date_value),
      as.Date("2026-07-24"),
      info = backend
    )
    expect_true(is.na(bound$null_value), info = backend)

    still_present <- fabric_sql_query(
      numeric_policy = "driver",
      server = lakehouse$sql_endpoint,
      database = lakehouse$display_name,
      sql = "SELECT COUNT(*) AS row_count FROM dbo.fabricqueryr_basic",
      backend = backend,
      token = fabric_test_token("FABRIC_TEST_SQL_TOKEN"),
      verbose = FALSE
    )
    expect_equal(as.numeric(still_present$row_count), 3, info = backend)

    stream <- fabric_sql_query(
      numeric_policy = "driver",
      server = lakehouse$sql_endpoint,
      database = lakehouse$display_name,
      sql = paste(
        "SELECT id, name",
        "FROM dbo.fabricqueryr_basic",
        "ORDER BY id"
      ),
      backend = backend,
      result = "arrow_stream",
      token = fabric_test_token("FABRIC_TEST_SQL_TOKEN"),
      verbose = FALSE
    )
    expect_s3_class(stream, "nanoarrow_array_stream")
    streamed <- as.data.frame(nanoarrow::collect_array_stream(stream))
    expect_equal(streamed$id, c(1L, 2L, 3L), info = backend)
    expect_equal(streamed$name, c("alpha", "beta", "gamma"), info = backend)

    bound_stream <- fabric_sql_query(
      numeric_policy = "driver",
      server = lakehouse$sql_endpoint,
      database = lakehouse$display_name,
      sql = "SELECT CAST(? AS int) AS bound_value",
      params = list(42L),
      backend = backend,
      result = "arrow_stream",
      token = fabric_test_token("FABRIC_TEST_SQL_TOKEN"),
      verbose = FALSE
    )
    expect_s3_class(bound_stream, "nanoarrow_array_stream")
    bound_result <- as.data.frame(
      nanoarrow::collect_array_stream(bound_stream)
    )
    expect_equal(bound_result$bound_value, 42L, info = backend)

    arrow_stream <- fabric_sql_query(
      numeric_policy = "driver",
      server = lakehouse$sql_endpoint,
      database = lakehouse$display_name,
      sql = paste(
        "SELECT id, name",
        "FROM dbo.fabricqueryr_basic",
        "ORDER BY id"
      ),
      backend = backend,
      result = "arrow_stream",
      token = fabric_test_token("FABRIC_TEST_SQL_TOKEN"),
      verbose = FALSE
    )
    reader <- arrow::as_record_batch_reader(arrow_stream)
    arrow_result <- as.data.frame(reader$read_table())
    expect_s3_class(reader, "RecordBatchReader")
    expect_equal(arrow_result$id, c(1L, 2L, 3L), info = backend)
    expect_equal(
      arrow_result$name,
      c("alpha", "beta", "gamma"),
      info = backend
    )
  }
})

test_that("generic SQL helpers discover and read every seeded SQL surface", {
  manifest <- fabric_test_manifest()
  token <- fabric_test_token_provider()
  cases <- list(
    Lakehouse = list(
      item = fabric_test_manifest_item(manifest, "TestLakehouse"),
      type = "Lakehouse",
      table = "fabricqueryr_basic"
    ),
    Warehouse = list(
      item = fabric_test_manifest_item(manifest, "TestWarehouse"),
      type = "Warehouse",
      table = fabric_test_manifest_item(manifest, "TestWarehouse")$tables$types,
      view = fabric_test_manifest_item(manifest, "TestWarehouse")$views$types
    ),
    WarehouseSnapshot = list(
      item = fabric_test_manifest_item(manifest, "TestWarehouseSnapshot"),
      type = "WarehouseSnapshot",
      table = fabric_test_manifest_item(manifest, "TestWarehouse")$tables$types
    )
  )
  sql_database <- manifest$items$TestSQLDatabase
  if (!is.null(sql_database)) {
    cases$SQLDatabase <- list(
      item = sql_database,
      type = "SQLDatabase",
      table = sql_database$tables$types,
      view = sql_database$views$types
    )
  }

  for (name in names(cases)) {
    case <- cases[[name]]
    target <- fabric_item(
      manifest$workspace_id,
      case$item$id,
      type = case$type,
      token = fabric_test_token("FABRIC_TEST_API_TOKEN")
    )
    table <- fabric_test_eventually(function() {
      tables <- fabric_sql_tables(
        target,
        schema = "dbo",
        detail = TRUE,
        backend = "odbc",
        token = token,
        verbose = FALSE
      )
      row <- tables[tables$name == case$table, , drop = FALSE]
      if (nrow(row) != 1L || !length(row$columns[[1L]])) {
        return(NULL)
      }
      row
    })
    expect_equal(table$name, case$table, info = name)
    expect_equal(table$schema, "dbo", info = name)
    expect_true(length(table$columns[[1L]]) > 0L, info = name)

    rows <- fabric_sql_read_table(
      numeric_policy = "driver",
      target,
      table,
      columns = c("id", "name", "amount"),
      backend = "odbc",
      token = token,
      verbose = FALSE
    )
    expect_s3_class(rows, "tbl_df")
    rows <- rows[rows$id %in% 1:3, , drop = FALSE]
    rows <- rows[order(rows$id), ]
    expect_equal(rows$id, 1:3, info = name)
    expect_equal(rows$name, c("alpha", "beta", "gamma"), info = name)
    expect_equal(as.numeric(rows$amount), c(10.5, 20, NA), info = name)

    if (!is.null(case$view)) {
      view <- fabric_test_eventually(function() {
        views <- fabric_sql_views(
          target,
          schema = "dbo",
          detail = TRUE,
          backend = "odbc",
          token = token,
          verbose = FALSE
        )
        row <- views[views$name == case$view, , drop = FALSE]
        if (nrow(row) != 1L || !length(row$columns[[1L]])) {
          return(NULL)
        }
        row
      })
      expect_equal(view$name, case$view, info = name)
      expect_equal(view$schema, "dbo", info = name)
      expect_equal(view$type, "VIEW", info = name)
      expect_match(view$definition, case$table, fixed = TRUE, info = name)
      expect_equal(
        vapply(view$columns[[1L]], `[[`, character(1), "name"),
        c("id", "name", "amount"),
        info = name
      )

      view_rows <- fabric_sql_read_table(
        numeric_policy = "driver",
        target,
        view,
        columns = c("id", "name", "amount"),
        backend = "odbc",
        token = token,
        verbose = FALSE
      )
      view_rows <- view_rows[view_rows$id %in% 1:3, , drop = FALSE]
      view_rows <- view_rows[order(view_rows$id), ]
      expect_equal(view_rows$id, 1:3, info = name)
      expect_equal(view_rows$name, c("alpha", "beta", "gamma"), info = name)
      expect_equal(as.numeric(view_rows$amount), c(10.5, 20, NA), info = name)
    }
  }
})

test_that("fabric_warehouse_tables discovers seeded Warehouse metadata", {
  fabric_test_require_package("DBI")
  fabric_test_require_package("odbc")
  manifest <- fabric_test_manifest()
  provisioned <- fabric_test_manifest_item(manifest, "TestWarehouse")
  target <- fabric_item(
    manifest$workspace_id,
    provisioned$id,
    type = "Warehouse",
    token = fabric_test_token("FABRIC_TEST_API_TOKEN")
  )
  token <- fabric_test_token_provider()
  expected_table <- provisioned$tables$types

  schemas <- fabric_warehouse_schemas(target, page_size = 1L, token = token)
  expect_true("dbo" %in% schemas$name)

  discovered <- fabric_test_eventually(function() {
    tables <- fabric_warehouse_tables(
      target,
      schema = "dbo",
      detail = TRUE,
      page_size = 1L,
      token = token
    )
    row <- tables[tables$name == expected_table, , drop = FALSE]
    if (nrow(row) != 1L || !length(row$columns[[1L]])) {
      return(NULL)
    }
    row
  })

  expect_s3_class(discovered, "tbl_df")
  expect_equal(discovered$name, expected_table)
  expect_equal(discovered$schema, "dbo")
  expect_equal(toupper(discovered$format), "DELTA")
  expect_match(discovered$location, "/Tables/dbo/", fixed = TRUE)
  expect_equal(discovered$schema_metadata[[1L]]$name, "dbo")
  expect_equal(
    vapply(discovered$columns[[1L]], `[[`, character(1), "name"),
    c(
      "id",
      "name",
      "category",
      "amount",
      "active",
      "event_date",
      "loaded_at",
      "nullable_value"
    )
  )
  expect_length(discovered$fabric_raw[[1L]], 0L)

  single <- fabric_warehouse_table(
    target,
    expected_table,
    schema = "dbo",
    token = token
  )
  expect_equal(single$name, expected_table)
  expect_equal(single$schema, "dbo")
  expect_equal(
    vapply(single$columns[[1L]], `[[`, character(1), "name"),
    vapply(discovered$columns[[1L]], `[[`, character(1), "name")
  )

  rows <- fabric_warehouse_read_table(
    numeric_policy = "driver",
    target,
    discovered,
    columns = c("id", "name"),
    limit = 1L,
    backend = "odbc",
    token = token,
    verbose = FALSE
  )
  expect_s3_class(rows, "tbl_df")
  expect_named(rows, c("id", "name"))
  expect_equal(nrow(rows), 1L)
})

test_that("fabric_warehouse_read_table returns projected rows and streams", {
  manifest <- fabric_test_manifest()
  provisioned <- fabric_test_manifest_item(manifest, "TestWarehouse")
  target <- fabric_item(
    manifest$workspace_id,
    provisioned$id,
    type = "Warehouse",
    token = fabric_test_token("FABRIC_TEST_API_TOKEN")
  )
  token <- fabric_test_token_provider()

  for (backend in fabric_test_sql_backends()) {
    rows <- fabric_warehouse_read_table(
      numeric_policy = "driver",
      target,
      provisioned$tables$types,
      columns = c("id", "name", "amount"),
      limit = 3,
      backend = backend,
      token = token,
      verbose = FALSE
    )
    rows <- rows[order(rows$id), ]
    expect_s3_class(rows, "tbl_df")
    expect_named(rows, c("id", "name", "amount"), info = backend)
    expect_equal(rows$id, 1:3, info = backend)
    expect_equal(rows$name, c("alpha", "beta", "gamma"), info = backend)
    expect_equal(as.numeric(rows$amount), c(10.5, 20, NA), info = backend)

    stream <- fabric_warehouse_read_table(
      numeric_policy = "driver",
      target,
      provisioned$tables$types,
      columns = c("id", "name"),
      limit = 2,
      result = "arrow_stream",
      backend = backend,
      token = token,
      verbose = FALSE
    )
    expect_s3_class(stream, "nanoarrow_array_stream")
    streamed <- as.data.frame(nanoarrow::collect_array_stream(stream))
    expect_named(streamed, c("id", "name"), info = backend)
    expect_equal(nrow(streamed), 2L, info = backend)
    expect_true(all(streamed$id %in% 1:3), info = backend)
  }
})


test_that("provisioned Warehouse target is discoverable and connectable", {
  results <- lapply(
    fabric_test_sql_backends(),
    function(backend) fabric_test_sql_item("TestWarehouse", backend)
  )
  expect_identical(results[[1L]], results[[2L]])
})

test_that("provisioned Warehouse snapshot is discoverable and connectable", {
  manifest <- fabric_test_manifest()
  provisioned <- fabric_test_manifest_item(
    manifest,
    "TestWarehouseSnapshot"
  )
  target <- fabric_item(
    manifest$workspace_id,
    provisioned$id,
    type = provisioned$type,
    token = fabric_test_token("FABRIC_TEST_API_TOKEN")
  )
  info <- fabric_sql_connection_info(target)
  expect_equal(info$database, provisioned$database_name)
  expect_equal(info$target_type, "warehouse")
  expect_equal(info$source, "discovery")

  for (backend in fabric_test_sql_backends()) {
    result <- fabric_sql_query(
      numeric_policy = "driver",
      target,
      "SELECT DB_NAME() AS database_name",
      backend = backend,
      token = fabric_test_token("FABRIC_TEST_SQL_TOKEN"),
      verbose = FALSE
    )
    expect_equal(
      result$database_name,
      provisioned$database_name,
      info = backend
    )
  }
})

test_that("provisioned SQL Database target is discoverable and connectable", {
  manifest <- fabric_test_manifest()
  skip_if(
    is.null(manifest$items$TestSQLDatabase),
    "Fabric capacity quota omitted the SQL Database fixture"
  )
  results <- lapply(
    fabric_test_sql_backends(),
    function(backend) fabric_test_sql_item("TestSQLDatabase", backend)
  )
  expect_identical(results[[1L]], results[[2L]])
})

test_that("Warehouse writer loads R and lazy Arrow data through OneLake", {
  fabric_test_require_package("DBI")
  fabric_test_require_package("odbc")
  fabric_test_require_package("arrow")
  manifest <- fabric_test_manifest()
  warehouse_fixture <- fabric_test_manifest_item(manifest, "TestWarehouse")
  lakehouse_fixture <- fabric_test_manifest_item(manifest, "TestLakehouse")
  token <- fabric_test_token_provider()
  warehouse <- fabric_item(
    manifest$workspace_id,
    warehouse_fixture$id,
    type = "Warehouse",
    token = token
  )
  staging_lakehouse <- fabric_item(
    manifest$workspace_id,
    lakehouse_fixture$id,
    type = "Lakehouse",
    token = token
  )
  table <- "fabricqueryr_r_write"
  table_sql <- paste0("[dbo].[", table, "]")
  con <- fabric_sql_connect(
    warehouse,
    backend = "odbc",
    token = token,
    verbose = FALSE
  )
  connected <- TRUE
  on.exit(
    {
      if (connected) {
        try(
          DBI::dbExecute(con, paste("DROP TABLE IF EXISTS", table_sql)),
          silent = TRUE
        )
        try(DBI::dbDisconnect(con), silent = TRUE)
      }
    },
    add = TRUE
  )
  DBI::dbExecute(con, paste("DROP TABLE IF EXISTS", table_sql))
  DBI::dbExecute(
    con,
    paste0(
      "CREATE TABLE ",
      table_sql,
      " ([id] int NOT NULL, [label] varchar(50) NULL, [amount] float NULL)"
    )
  )

  mismatched <- tryCatch(
    fabric_warehouse_write_table(
      warehouse,
      table,
      data.frame(
        Id = 99L,
        label = "must-not-load",
        amount = 99
      ),
      staging_lakehouse = staging_lakehouse,
      keep_staging_on_failure = FALSE,
      backend = "odbc",
      token = token,
      verbose = FALSE
    ),
    error = identity
  )
  expect_s3_class(mismatched, "fabric_warehouse_write_error")
  expect_s3_class(mismatched$parent, "fabric_warehouse_column_error")
  expect_equal(
    DBI::dbGetQuery(con, paste("SELECT COUNT(*) AS n FROM", table_sql))$n,
    0
  )

  appended <- fabric_warehouse_write_table(
    warehouse,
    table,
    data.frame(
      id = 1:3,
      label = c("alpha", "beta", "gamma"),
      amount = c(10.5, NA, 30)
    ),
    staging_lakehouse = staging_lakehouse,
    mode = "Append",
    max_rows_per_file = 2,
    backend = "odbc",
    token = token,
    verbose = FALSE
  )
  expect_equal(appended$rows, 3)
  expect_equal(appended$file_count, 2L)
  expect_false(appended$staging_retained)
  rows <- DBI::dbGetQuery(
    con,
    paste("SELECT id, label, amount FROM", table_sql, "ORDER BY id")
  )
  expect_equal(rows$id, 1:3)
  expect_equal(rows$label, c("alpha", "beta", "gamma"))
  expect_equal(rows$amount, c(10.5, NA, 30))

  dataset_directory <- tempfile("fabricqueryr-warehouse-live-")
  dir.create(dataset_directory)
  on.exit(
    unlink(dataset_directory, recursive = TRUE, force = TRUE),
    add = TRUE
  )
  arrow::write_parquet(
    data.frame(
      id = 4:5,
      label = c("delta", "epsilon"),
      amount = c(40, 50)
    ),
    file.path(dataset_directory, "part-1.parquet")
  )
  overwritten <- fabric_warehouse_write_table(
    warehouse,
    table,
    arrow::open_dataset(dataset_directory),
    staging_lakehouse = staging_lakehouse,
    mode = "Overwrite",
    backend = "odbc",
    token = token,
    verbose = FALSE
  )
  expect_equal(overwritten$rows, 2)
  expect_false(overwritten$staging_retained)
  replaced <- DBI::dbGetQuery(
    con,
    paste("SELECT id, label, amount FROM", table_sql, "ORDER BY id")
  )
  expect_equal(replaced$id, 4:5)
  expect_equal(replaced$label, c("delta", "epsilon"))
  expect_equal(replaced$amount, c(40, 50))

  DBI::dbExecute(con, paste("DROP TABLE IF EXISTS", table_sql))
  created <- fabric_warehouse_write_table(
    warehouse,
    table,
    data.frame(
      id = 6:7,
      label = c("created-a", "created-b"),
      amount = c(60, 70)
    ),
    staging_lakehouse = staging_lakehouse,
    mode = "Append",
    create_if_missing = TRUE,
    backend = "odbc",
    token = token,
    verbose = FALSE
  )
  expect_true(created$table_created)
  created_rows <- DBI::dbGetQuery(
    con,
    paste("SELECT id, label, amount FROM", table_sql, "ORDER BY id")
  )
  expect_equal(created_rows$id, 6:7)
  expect_equal(created_rows$label, c("created-a", "created-b"))
  expect_equal(created_rows$amount, c(60, 70))

  recreated <- fabric_warehouse_write_table(
    warehouse,
    table,
    data.frame(
      id = 8:9,
      label = c("drop-a", "drop-b"),
      amount = c(80, 90)
    ),
    staging_lakehouse = staging_lakehouse,
    mode = "Overwrite",
    overwrite_method = "Drop",
    backend = "odbc",
    token = token,
    verbose = FALSE
  )
  expect_true(recreated$table_recreated)
  recreated_rows <- DBI::dbGetQuery(
    con,
    paste("SELECT id, label, amount FROM", table_sql, "ORDER BY id")
  )
  expect_equal(recreated_rows$id, 8:9)
  expect_equal(recreated_rows$label, c("drop-a", "drop-b"))
  expect_equal(recreated_rows$amount, c(80, 90))

  DBI::dbExecute(con, paste("DROP TABLE IF EXISTS", table_sql))
  DBI::dbDisconnect(con)
  connected <- FALSE
})

test_that("Warehouse writer loads through the live ADBC backend", {
  for (package in c("DBI", "odbc", "adbi", "adbcdrivermanager", "arrow")) {
    fabric_test_require_package(package)
  }
  manifest <- fabric_test_manifest()
  warehouse_fixture <- fabric_test_manifest_item(manifest, "TestWarehouse")
  lakehouse_fixture <- fabric_test_manifest_item(manifest, "TestLakehouse")
  token <- fabric_test_token_provider()
  warehouse <- fabric_item(
    manifest$workspace_id,
    warehouse_fixture$id,
    type = "Warehouse",
    token = token
  )
  staging_lakehouse <- fabric_item(
    manifest$workspace_id,
    lakehouse_fixture$id,
    type = "Lakehouse",
    token = token
  )
  table <- "fabricqueryr_adbc_write"
  table_sql <- paste0("[dbo].[", table, "]")
  cleanup_connection <- fabric_sql_connect(
    warehouse,
    backend = "odbc",
    token = token,
    verbose = FALSE
  )
  connected <- TRUE
  on.exit(
    {
      if (connected) {
        try(
          DBI::dbExecute(
            cleanup_connection,
            paste("DROP TABLE IF EXISTS", table_sql)
          ),
          silent = TRUE
        )
        try(DBI::dbDisconnect(cleanup_connection), silent = TRUE)
      }
    },
    add = TRUE
  )
  DBI::dbExecute(
    cleanup_connection,
    paste("DROP TABLE IF EXISTS", table_sql)
  )

  written <- fabric_warehouse_write_table(
    warehouse,
    table,
    data.frame(
      id = 1:3,
      label = c("adbc-a", "adbc-b", "adbc-c"),
      amount = c(10.5, NA, 30)
    ),
    staging_lakehouse = staging_lakehouse,
    mode = "Append",
    create_if_missing = TRUE,
    backend = "adbc",
    token = token,
    verbose = FALSE
  )

  expect_true(written$table_created)
  expect_equal(written$rows, 3)
  expect_false(written$staging_retained)
  rows <- DBI::dbGetQuery(
    cleanup_connection,
    paste("SELECT id, label, amount FROM", table_sql, "ORDER BY id")
  )
  expect_equal(rows$id, 1:3)
  expect_equal(rows$label, c("adbc-a", "adbc-b", "adbc-c"))
  expect_equal(rows$amount, c(10.5, NA, 30))

  DBI::dbExecute(
    cleanup_connection,
    paste("DROP TABLE IF EXISTS", table_sql)
  )
  DBI::dbDisconnect(cleanup_connection)
  connected <- FALSE
})
test_that("Warehouse writes and projections preserve case-distinct columns", {
  manifest <- fabric_test_manifest()
  token <- fabric_test_token_provider()
  warehouse <- fabric_item(
    manifest$workspace_id,
    fabric_test_manifest_item(manifest, "TestWarehouse")$id,
    type = "Warehouse",
    token = token
  )
  lake <- fabric_item(
    manifest$workspace_id,
    fabric_test_manifest_item(manifest, "TestLakehouse")$id,
    type = "Lakehouse",
    token = token
  )
  table <- paste0("fabricqueryr_case_", Sys.getpid())
  con <- fabric_sql_connect(warehouse, token = token, verbose = FALSE)
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  on.exit(
    try(
      DBI::dbExecute(con, paste0("DROP TABLE IF EXISTS [dbo].[", table, "]")),
      silent = TRUE
    ),
    add = TRUE,
    after = FALSE
  )
  for (backend in fabric_test_sql_backends()) {
    for (mode in c("append", "overwrite")) {
      fabric_warehouse_write_table(
        warehouse,
        table,
        data.frame(id = 1L, ID = 2L, check.names = FALSE),
        create_if_missing = TRUE,
        mode = mode,
        backend = backend,
        staging_lakehouse = lake,
        token = token,
        verbose = FALSE
      )
      rows <- fabric_warehouse_read_table(
        numeric_policy = "driver",
        warehouse,
        table,
        columns = c("id", "ID"),
        backend = backend,
        token = token,
        verbose = FALSE
      )
      expect_identical(names(rows), c("id", "ID"))
      expect_equal(unique(rows$id), 1L)
      expect_equal(unique(rows$ID), 2L)
    }
  }
})
test_that("Warehouse CTAS and COPY preserve timezone-free timestamps as datetime2", {
  manifest <- fabric_test_manifest()
  fabric_test_require_package("arrow")
  token <- fabric_test_token_provider()
  warehouse <- fabric_item(
    manifest$workspace_id,
    fabric_test_manifest_item(manifest, "TestWarehouse")$id,
    type = "Warehouse",
    token = token
  )
  lake <- fabric_item(
    manifest$workspace_id,
    fabric_test_manifest_item(manifest, "TestLakehouse")$id,
    type = "Lakehouse",
    token = token
  )
  con <- fabric_sql_connect(warehouse, token = token, verbose = FALSE)
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  table <- paste0("fabricqueryr_timestamp_", Sys.getpid())
  sql <- paste0("[dbo].[", table, "]")
  on.exit(
    DBI::dbExecute(con, paste("DROP TABLE IF EXISTS", sql)),
    add = TRUE,
    after = FALSE
  )
  ticks <- arrow::Array$create(c("1788613200123456", NA))$cast(arrow::int64())
  data <- arrow::Table$create(value = ticks$cast(arrow::timestamp("us")))
  for (backend in fabric_test_sql_backends()) {
    written <- fabric_warehouse_write_table(
      warehouse,
      table,
      data,
      staging_lakehouse = lake,
      create_if_missing = TRUE,
      backend = backend,
      token = token,
      verbose = FALSE
    )
    expect_equal(written$rows, 2)
    rows <- DBI::dbGetQuery(
      con,
      paste0("SELECT CONVERT(varchar(40), value, 126) AS value FROM ", sql)
    )
    expect_true(all(grepl("123456$", rows$value[!is.na(rows$value)])))
  }
  types <- DBI::dbGetQuery(
    con,
    paste0(
      "SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '",
      table,
      "'"
    )
  )
  expect_identical(types[[1L]], "datetime2")
})
test_that("SQL exact reads preserve numeric boundaries or reject unsafe ODBC conversion", {
  manifest <- fabric_test_manifest()
  token <- fabric_test_token_provider()
  warehouse <- fabric_item(
    manifest$workspace_id,
    fabric_test_manifest_item(manifest, "TestWarehouse")$id,
    type = "Warehouse",
    token = token
  )
  sql <- paste(
    "SELECT CAST('12345678901234567890.1234' AS decimal(24,4)) AS amount,",
    "CAST('-2147483648' AS int) AS i32,",
    "CAST('-9223372036854775808' AS bigint) AS i64,",
    "CAST('2147483648' AS bigint) AS large"
  )
  for (backend in fabric_test_sql_backends()) {
    for (shape in c("tibble", "arrow_stream")) {
      result <- tryCatch(
        fabric_sql_query(
          warehouse,
          sql,
          backend = backend,
          result = shape,
          token = token,
          verbose = FALSE
        ),
        error = identity
      )
      if (backend == "odbc") {
        expect_s3_class(result, "fabric_sql_execution_error")
        expect_s3_class(result$parent, "fabric_sql_precision_error")
        next
      }
      if (shape == "arrow_stream") {
        expect_identical(
          attr(result, "fabric_sql_stream_source"),
          "adbc_native"
        )
        stream <- result
        result <- .fabric_arrow_exact_tibble(stream)
        nanoarrow::nanoarrow_pointer_release(stream)
      }
      expect_identical(result$amount, "12345678901234567890.1234")
      expect_identical(result$i32, -2147483648)
      expect_identical(result$i64, "-9223372036854775808")
      expect_identical(as.character(result$large), "2147483648")
    }
    cast <- fabric_sql_query(
      warehouse,
      "SELECT CAST(CAST('-9223372036854775808' AS bigint) AS varchar(20)) AS v",
      backend = backend,
      token = token,
      verbose = FALSE
    )
    expect_identical(cast$v, "-9223372036854775808")
  }
})

test_that("SQL integer64 parameters preserve values, nulls and bigint operations", {
  manifest <- fabric_test_manifest()
  token <- fabric_test_token_provider()
  warehouse <- fabric_item(
    manifest$workspace_id,
    fabric_test_manifest_item(manifest, "TestWarehouse")$id,
    type = "Warehouse",
    token = token
  )
  expected <- c(
    "-9223372036854775807",
    "-9007199254740993",
    "-2147483648",
    "-1",
    "0",
    "1",
    "2147483648",
    "9007199254740993",
    "9223372036854775807",
    NA_character_
  )
  values <- bit64::as.integer64(expected)
  for (backend in fabric_test_sql_backends()) {
    for (shape in c("tibble", "arrow_stream")) {
      result <- fabric_sql_query(
        warehouse,
        "SELECT CAST(? AS varchar(20)) AS value",
        params = list(value = values),
        backend = backend,
        result = shape,
        token = token,
        verbose = FALSE
      )
      if (shape == "arrow_stream") {
        stream <- result
        result <- .fabric_arrow_exact_tibble(stream)
        nanoarrow::nanoarrow_pointer_release(stream)
      }
      expect_identical(result$value, expected)
    }
    result <- fabric_sql_query(
      warehouse,
      paste0(
        "SELECT CAST(? + CAST(1 AS bigint) AS varchar(20)) AS incremented, ",
        "CAST(? / CAST(2 AS bigint) AS varchar(20)) AS divided, ",
        "CAST(CASE WHEN ? = CAST('9007199254740993' AS bigint) ",
        "THEN 'equal' ELSE 'different' END AS varchar(10)) AS compared"
      ),
      params = list(
        bit64::as.integer64("9223372036854775806"),
        bit64::as.integer64("3"),
        bit64::as.integer64("9007199254740993")
      ),
      backend = backend,
      token = token,
      verbose = FALSE
    )
    expect_identical(result$incremented, "9223372036854775807")
    expect_identical(result$divided, "1")
    expect_identical(result$compared, "equal")
    if (backend == "odbc") {
      type <- local({
        con <- fabric_sql_connect(
          warehouse,
          backend = backend,
          token = token,
          verbose = FALSE
        )
        on.exit(DBI::dbDisconnect(con), add = TRUE)
        binding <- .fabric_sql_odbc_query_params(
          "SELECT ? AS value",
          list(bit64::as.integer64(3))
        )
        query_result <- .fabric_sql_db_send_query(con, binding$sql, "tibble")
        on.exit(
          .fabric_sql_db_clear_result(query_result),
          add = TRUE,
          after = FALSE
        )
        .fabric_sql_db_bind(query_result, binding$params)
        DBI::dbColumnInfo(query_result)$type
      })
      expect_identical(as.character(type), "-5")
    }
  }
})
