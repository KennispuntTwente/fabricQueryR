test_that("SQL connection info parses portal strings and bare endpoints", {
  full <- fabric_sql_connection_info(
    paste0(
      "Data Source=tcp:abc.database.fabric.microsoft.com,1444;",
      "Initial Catalog=Orders-123;",
      "MultipleActiveResultSets=False;Connect Timeout=30;",
      "Encrypt=True;TrustServerCertificate=False"
    )
  )
  expect_equal(full$server, "abc.database.fabric.microsoft.com")
  expect_equal(full$database, "Orders-123")
  expect_equal(full$port, 1444L)
  expect_equal(full$target_type, "sql_database")

  bare <- fabric_sql_connection_info(
    "server.datawarehouse.fabric.microsoft.com",
    database = "Sales"
  )
  prefixed <- fabric_sql_connection_info(
    "Server=tcp:server.datawarehouse.fabric.microsoft.com",
    database = "Sales"
  )
  expect_equal(bare[c("server", "database", "port")], prefixed[c(
    "server", "database", "port"
  )])
  expect_equal(bare$target_type, "sql_analytics_endpoint")
})

test_that("SQL connection info consumes discovered item rows", {
  item <- tibble::tibble(
    id = "item-id",
    displayName = "Lake",
    type = "Lakehouse",
    workspaceId = "workspace-id",
    sql_server = "lake.datawarehouse.fabric.microsoft.com",
    sql_database = "Lake",
    properties = list(list())
  )
  info <- fabric_sql_connection_info(item)
  expect_equal(info$server, "lake.datawarehouse.fabric.microsoft.com")
  expect_equal(info$database, "Lake")
  expect_equal(info$target_type, "lakehouse")
  expect_equal(info$source, "discovery")

  expect_error(
    fabric_sql_connection_info(
      structure(
        list(id = "model", type = "SemanticModel"),
        class = c("fabric_item", "list")
      )
    ),
    class = "fabric_sql_target_error"
  )
})

test_that("SQL targets require a catalog and validate malformed inputs", {
  expect_error(
    fabric_sql_connection_info(
      "server.datawarehouse.fabric.microsoft.com"
    ),
    class = "fabric_sql_database_error"
  )
  expect_error(
    fabric_sql_connection_info("Server=;Database=Sales"),
    "server is empty"
  )
  expect_error(
    fabric_sql_connection_info("one;two", database = "Sales"),
    "unique Server"
  )
  expect_error(
    fabric_sql_connection_info("server", database = "Sales", port = 70000),
    "between 1 and 65535"
  )
})

test_that("SQL connections enforce Fabric ODBC options", {
  captured <- NULL
  connection <- structure(list(), class = "test_connection")
  local_mocked_bindings(
    .fabric_sql_db_connect = function(...) {
      captured <<- list(...)
      connection
    }
  )

  result <- fabric_sql_connect(
    server = "server.datawarehouse.fabric.microsoft.com",
    database = "Warehouse",
    access_token = "sql-token",
    read_only = TRUE,
    timeout = 17L,
    verbose = FALSE
  )

  expect_identical(result, connection)
  expect_equal(captured$server, "server.datawarehouse.fabric.microsoft.com")
  expect_equal(captured$database, "Warehouse")
  expect_equal(captured$Port, 1433L)
  expect_equal(captured$MARS_Connection, "no")
  expect_equal(captured$ApplicationIntent, "ReadOnly")
  expect_equal(captured$timeout, 17L)
  expect_equal(captured$attributes$azure_token, "sql-token")
})

test_that("fabric_sql_query passes bound parameters unchanged", {
  connection <- structure(list(), class = "test_connection")
  captured <- NULL
  disconnected <- FALSE
  values <- list(
    "Robert'); DROP TABLE Students;--",
    as.Date("2026-07-24"),
    NA_character_,
    NULL
  )
  local_mocked_bindings(
    fabric_sql_connect = function(...) connection,
    .fabric_sql_db_get_query = function(con, sql, params = NULL) {
      captured <<- list(con = con, sql = sql, params = params)
      data.frame(ok = TRUE)
    },
    .fabric_sql_db_disconnect = function(con) {
      disconnected <<- TRUE
      invisible(TRUE)
    }
  )

  result <- fabric_sql_query(
    "unused",
    "SELECT ?, ?, ?, ?",
    params = values,
    access_token = "token",
    verbose = FALSE
  )

  expect_s3_class(result, "tbl_df")
  expect_identical(captured$params, values)
  expect_identical(captured$sql, "SELECT ?, ?, ?, ?")
  expect_true(disconnected)
})

test_that("SQL failures have actionable condition classes", {
  local_mocked_bindings(
    .fabric_sql_db_connect = function(...) {
      stop("Login failed for user; error 18456")
    }
  )
  expect_error(
    fabric_sql_connect(
      "server",
      database = "db",
      access_token = "token",
      verbose = FALSE
    ),
    class = "fabric_sql_authentication_error"
  )

  local_mocked_bindings(
    fabric_sql_connect = function(...) structure(list(), class = "connection"),
    .fabric_sql_db_get_query = function(...) stop("syntax error"),
    .fabric_sql_db_disconnect = function(...) invisible(TRUE)
  )
  expect_error(
    fabric_sql_query(
      "server",
      "SELECT bad",
      database = "db",
      access_token = "token",
      verbose = FALSE
    ),
    class = "fabric_sql_execution_error"
  )
  expect_error(
    fabric_sql_query(
      "server",
      "SELECT ?",
      params = "not-a-list",
      database = "db",
      access_token = "token",
      verbose = FALSE
    ),
    "params must be NULL or a list"
  )
})
