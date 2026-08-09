# Fabric integration coverage: Fabric SQL endpoints and query clients.
# These tests connect to seeded Lakehouse, Warehouse, and SQL Database targets
# and verify discovery, DBI/ODBC queries, typed results, and Arrow streaming.

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

    disconnected <- if (identical(backend, "adbc")) {
      DBI::dbDisconnect(con, force = TRUE)
    } else {
      DBI::dbDisconnect(con)
    }
    expect_true(isTRUE(disconnected), info = backend)
    # ADBC Driver Foundry 1.x can keep reporting released handles as valid.
    # The driver's successful disconnect return is its reliable lifecycle signal.
    if (!identical(backend, "adbc")) {
      expect_false(DBI::dbIsValid(con), info = backend)
    }
  }
})

test_that("fabric_sql_query returns tibbles and consumable Arrow streams", {
  backends <- fabric_test_sql_backends()
  manifest <- fabric_test_manifest()
  lakehouse <- manifest$items$TestLakehouse

  for (backend in backends) {
    result <- fabric_sql_query(
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

    empty <- fabric_sql_query(
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
      server = lakehouse$sql_endpoint,
      database = lakehouse$display_name,
      sql = "SELECT COUNT(*) AS row_count FROM dbo.fabricqueryr_basic",
      backend = backend,
      token = fabric_test_token("FABRIC_TEST_SQL_TOKEN"),
      verbose = FALSE
    )
    expect_equal(as.numeric(still_present$row_count), 3, info = backend)

    stream <- fabric_sql_query(
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

    arrow_stream <- fabric_sql_query(
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

fabric_test_sql_item <- function(name, backend) {
  manifest <- fabric_test_manifest()
  api_token <- fabric_test_token("FABRIC_TEST_API_TOKEN")
  sql_token <- fabric_test_token("FABRIC_TEST_SQL_TOKEN")

  provisioned <- fabric_test_manifest_item(manifest, name)
  table_name <- provisioned$tables$types
  if (is.null(table_name)) {
    rlang::abort(sprintf(
      "Fabric integration manifest has no typed SQL table for '%s'",
      name
    ))
  }
  context <- paste(name, backend)
  target <- fabric_item(
    manifest$workspace_id,
    provisioned$id,
    type = provisioned$type,
    token = api_token
  )
  info <- fabric_sql_connection_info(target)
  expect_equal(info$database, provisioned$database_name, info = context)
  expect_equal(info$source, "discovery", info = context)
  expect_equal(
    info$target_type,
    if (identical(name, "TestWarehouse")) "warehouse" else "sql_database",
    info = context
  )

  con <- fabric_sql_connect(
    target,
    backend = backend,
    token = sql_token,
    read_only = TRUE,
    verbose = FALSE
  )
  connected <- TRUE
  on.exit(
    if (connected) {
      if (identical(backend, "adbc")) {
        DBI::dbDisconnect(con, force = TRUE)
      } else {
        DBI::dbDisconnect(con)
      }
    },
    add = TRUE
  )
  expect_true(DBI::dbIsValid(con), info = context)
  expect_true(
    DBI::dbExistsTable(
      con,
      DBI::Id(schema = "dbo", table = table_name)
    ),
    info = context
  )
  catalog <- DBI::dbGetQuery(con, "SELECT DB_NAME() AS database_name")
  expect_equal(
    catalog$database_name,
    provisioned$database_name,
    info = context
  )
  rows <- DBI::dbGetQuery(
    con,
    sprintf(
      paste(
        "SELECT id, name, amount, active, event_date,",
        "loaded_at, nullable_value",
        "FROM dbo.%s",
        "WHERE id > 0",
        "ORDER BY id"
      ),
      table_name
    )
  )
  expect_equal(rows$id, c(1L, 2L, 3L), info = context)
  expect_equal(
    rows$name,
    c("alpha", "beta", "gamma"),
    info = context
  )
  expect_equal(as.numeric(rows$amount), c(10.5, 20, NA), info = context)
  expect_equal(as.logical(rows$active), c(TRUE, FALSE, NA), info = context)
  expect_s3_class(rows$event_date, "Date")
  expect_equal(
    rows$event_date,
    as.Date(c("2026-01-01", "2026-01-02", NA)),
    info = context
  )
  expect_s3_class(rows$loaded_at, "POSIXct")
  expect_equal(
    as.numeric(rows$loaded_at),
    rep(as.numeric(as.POSIXct("2026-01-01", tz = "UTC")), 3),
    info = context
  )
  expect_equal(
    rows$nullable_value,
    c(NA, "present", NA),
    info = context
  )
  disconnected <- if (identical(backend, "adbc")) {
    DBI::dbDisconnect(con, force = TRUE)
  } else {
    DBI::dbDisconnect(con)
  }
  connected <- FALSE
  expect_true(isTRUE(disconnected), info = context)
  if (!identical(backend, "adbc")) {
    expect_false(DBI::dbIsValid(con), info = context)
  }

  bound_rows <- fabric_sql_query(
    target,
    sprintf(
      paste(
        "SELECT id, name",
        "FROM dbo.%s",
        "WHERE id >= ?",
        "ORDER BY id"
      ),
      table_name
    ),
    params = list(2L),
    backend = backend,
    token = sql_token,
    verbose = FALSE
  )
  expect_s3_class(bound_rows, "tbl_df")
  expect_equal(bound_rows$id, c(2L, 3L), info = context)
  expect_equal(bound_rows$name, c("beta", "gamma"), info = context)

  from_manifest <- fabric_sql_query(
    provisioned$connection_string,
    "SELECT CAST(? AS nvarchar(100)) AS bound_value",
    params = list("safe ' value; --"),
    backend = backend,
    database = if (identical(name, "TestWarehouse")) {
      provisioned$database_name
    } else {
      NULL
    },
    token = sql_token,
    verbose = FALSE
  )
  expect_equal(
    from_manifest$bound_value,
    "safe ' value; --",
    info = context
  )
  bare_server <- if (identical(name, "TestSQLDatabase")) {
    provisioned$server_fqdn
  } else {
    fabric_sql_connection_info(provisioned$connection_string)$server
  }
  from_server_and_database <- fabric_sql_query(
    bare_server,
    "SELECT DB_NAME() AS database_name",
    database = provisioned$database_name,
    backend = backend,
    token = sql_token,
    verbose = FALSE
  )
  expect_equal(
    from_server_and_database$database_name,
    provisioned$database_name,
    info = context
  )

  stream <- fabric_sql_query(
    target,
    sprintf(
      paste(
        "SELECT id, name, amount",
        "FROM dbo.%s",
        "WHERE id > 0",
        "ORDER BY id"
      ),
      table_name
    ),
    backend = backend,
    result = "arrow_stream",
    token = sql_token,
    verbose = FALSE
  )
  expect_s3_class(
    stream,
    "nanoarrow_array_stream"
  )
  streamed <- as.data.frame(nanoarrow::collect_array_stream(stream))
  expect_equal(streamed$id, c(1L, 2L, 3L), info = context)
  expect_equal(
    streamed$name,
    c("alpha", "beta", "gamma"),
    info = context
  )
  expect_equal(as.numeric(streamed$amount), c(10.5, 20, NA), info = context)

  list(
    id = as.integer(rows$id),
    name = as.character(rows$name),
    amount = as.numeric(rows$amount),
    active = as.logical(rows$active),
    event_date = as.character(rows$event_date),
    nullable_value = as.character(rows$nullable_value)
  )
}

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
  results <- lapply(
    fabric_test_sql_backends(),
    function(backend) fabric_test_sql_item("TestSQLDatabase", backend)
  )
  expect_identical(results[[1L]], results[[2L]])
})
