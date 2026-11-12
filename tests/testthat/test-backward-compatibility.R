test_that("legacy access_token is accepted only through dots", {
  compatible <- c(
    "fabric_livy_query",
    "fabric_sql_connect",
    "fabric_sql_query"
  )
  for (name in compatible) {
    args <- names(formals(get(name, mode = "function")))
    expect_false("access_token" %in% args, info = name)
    expect_true("..." %in% args, info = name)
  }

  resolved <- fabric_resolve_token_alias(
    token = NULL,
    dots = list(access_token = "legacy-token", extra = 1L),
    caller = "test()"
  )
  expect_identical(resolved$token, "legacy-token")
  expect_identical(resolved$dots, list(extra = 1L))
  expect_error(
    fabric_resolve_token_alias(
      token = "new-token",
      dots = list(access_token = "legacy-token"),
      caller = "test()"
    ),
    "received both token"
  )
})

test_that("fabric_livy_query consumes named access_token from dots", {
  captured <- NULL
  closed <- FALSE
  fake_session <- new.env(parent = emptyenv())
  fake_session$wait <- function(...) invisible(fake_session)
  fake_session$run <- function(...) list(ok = TRUE)
  fake_session$close <- function(deadline = NULL) {
    closed <<- TRUE
    invisible(TRUE)
  }
  local_mocked_bindings(
    fabric_livy_session = function(...) {
      captured <<- list(...)
      fake_session
    }
  )

  result <- fabric_livy_query(
    "https://example.test/livy/sessions",
    "1 + 1",
    access_token = "legacy-token",
    verbose = FALSE
  )

  expect_identical(captured$token, "legacy-token")
  expect_identical(result, list(ok = TRUE))
  expect_true(closed)

  record <- r6_test_record()$as_list()
  record$livy_url <- "https://example.test/livy/sessions"
  lakehouse <- fabric_r6_record(
    record,
    class(record),
    fabric_credential(token = "discovery-token")
  )
  result <- lakehouse$livy_query(
    "1 + 1",
    access_token = "legacy-token",
    verbose = FALSE
  )
  expect_identical(captured$token, "legacy-token")
  expect_identical(result, list(ok = TRUE))
  lakehouse$livy_query("1 + 1", access_token = NULL, verbose = FALSE)
  expect_identical(
    fabric_get_token(captured$token, .fabric_audience$fabric),
    "discovery-token"
  )
  expect_error(
    fabric_livy_query(
      "https://example.test/livy/sessions",
      "1 + 1",
      unexpected = TRUE,
      verbose = FALSE
    ),
    "unused arguments"
  )
})

test_that("SQL helpers consume named access_token from dots", {
  connection <- structure(list(), class = "test_connection")
  connect_args <- NULL
  query_connect_args <- NULL
  disconnected <- FALSE
  local_mocked_bindings(
    .fabric_sql_db_connect = function(...) {
      connect_args <<- list(...)
      connection
    }
  )

  result <- fabric_sql_connect(
    server = "server.datawarehouse.fabric.microsoft.com",
    database = "Warehouse",
    access_token = "legacy-token",
    odbc_driver = "Legacy ODBC Driver",
    port = 1444L,
    timeout = 9L,
    verbose = FALSE
  )

  expect_identical(result, connection)
  expect_identical(connect_args$database, "Warehouse")
  expect_identical(
    connect_args$server,
    "tcp:server.datawarehouse.fabric.microsoft.com,1444"
  )
  expect_null(connect_args$Port)
  expect_identical(connect_args$attributes$azure_token, "legacy-token")
  expect_false("access_token" %in% names(connect_args))

  local_mocked_bindings(
    fabric_sql_connect = function(...) {
      query_connect_args <<- list(...)
      connection
    },
    .fabric_sql_db_get_query = function(...) data.frame(value = 1L),
    .fabric_sql_db_disconnect = function(...) {
      disconnected <<- TRUE
      invisible(TRUE)
    }
  )

  query <- fabric_sql_query(
    server = "server.datawarehouse.fabric.microsoft.com",
    sql = "SELECT 1 AS value",
    database = "Warehouse",
    access_token = "legacy-token",
    odbc_driver = "Legacy ODBC Driver",
    port = 1444L,
    timeout = 9L,
    verbose = FALSE
  )

  expect_identical(query_connect_args$database, "Warehouse")
  expect_identical(query_connect_args$port, 1444L)
  expect_identical(
    query_connect_args$token(.fabric_audience$sql),
    "legacy-token"
  )
  expect_false("access_token" %in% names(query_connect_args))
  expect_equal(query$value, 1L)
  expect_true(disconnected)
})

test_that("R6 SQL methods honor access_token over discovery credentials", {
  record <- r6_test_record("Warehouse")$as_list()
  record$sql_server <- "server.datawarehouse.fabric.microsoft.com"
  record$sql_database <- "Warehouse"
  warehouse <- fabric_r6_record(
    record,
    class(record),
    fabric_credential(token = "discovery-token")
  )
  used <- NULL
  connection <- structure(list(), class = "test_connection")
  local_mocked_bindings(
    .fabric_sql_db_connect = function(...) {
      used <<- list(...)$attributes$azure_token
      connection
    },
    .fabric_sql_db_get_query = function(...) data.frame(value = 1L),
    .fabric_sql_db_disconnect = function(...) invisible(TRUE)
  )
  warehouse$sql_connect(access_token = "legacy-token", verbose = FALSE)
  expect_identical(used, "legacy-token")
  result <- warehouse$sql_query(
    "SELECT 1 AS value",
    access_token = "legacy-token",
    verbose = FALSE
  )
  expect_identical(used, "legacy-token")
  expect_identical(result$value, 1L)
  expect_error(
    warehouse$sql_connect(token = "new-token", access_token = "legacy-token"),
    "received both token"
  )
})
