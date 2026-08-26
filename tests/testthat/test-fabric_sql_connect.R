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
  expect_equal(
    bare[c("server", "database", "port")],
    prefixed[c(
      "server",
      "database",
      "port"
    )]
  )
  expect_equal(bare$target_type, "sql_analytics_endpoint")

  azure_sql <- fabric_sql_connection_info(
    "fabric-db.database.windows.net",
    database = "Orders"
  )
  expect_equal(azure_sql$target_type, "sql_database")
})

test_that("SQL connection strings preserve quoted semicolons", {
  braced <- fabric_sql_connection_info(paste0(
    "Server={tcp:abc.database.fabric.microsoft.com,1433};",
    "Initial Catalog={Orders;Archive};"
  ))
  expect_equal(braced$server, "abc.database.fabric.microsoft.com")
  expect_equal(braced$database, "Orders;Archive")

  quoted <- fabric_sql_connection_info(paste0(
    "Server=abc.database.fabric.microsoft.com;",
    "Initial Catalog=\"Orders; \"\"North\"\"\";"
  ))
  expect_equal(quoted$database, 'Orders; "North"')
  expect_error(
    fabric_sql_connection_info(paste0(
      "Server=abc.database.fabric.microsoft.com;",
      "Initial Catalog={Orders;Archive;"
    )),
    "unterminated quoted or braced value",
    fixed = TRUE
  )
})

test_that("connection-string delimiters only open at value boundaries", {
  parsed <- fabric_parse_sql_connection_string(paste0(
    "Server=abc.database.fabric.microsoft.com;",
    "Database=Sales;",
    "Application Name=O'Brien{preview;",
    "Password=abc\"def;"
  ))

  expect_equal(parsed$database, "Sales")
  expect_equal(parsed$fields$applicationname, "O'Brien{preview")
  expect_equal(parsed$fields$password, 'abc"def')

  spaced <- fabric_parse_sql_connection_string(paste0(
    "Server =   {abc.database.fabric.microsoft.com};",
    "Database =   'Sales; Archive';"
  ))
  expect_equal(spaced$server, "abc.database.fabric.microsoft.com")
  expect_equal(spaced$database, "Sales; Archive")
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

  snapshot <- structure(
    list(
      id = "snapshot-id",
      displayName = "Sales at month end",
      type = "WarehouseSnapshot",
      workspaceId = "workspace-id",
      properties = list(
        connectionString = paste0(
          "snapshot.datawarehouse.fabric.microsoft.com"
        )
      )
    ),
    class = c("fabric_item", "list")
  )
  snapshot_info <- fabric_sql_connection_info(snapshot)
  expect_equal(
    snapshot_info$server,
    "snapshot.datawarehouse.fabric.microsoft.com"
  )
  expect_equal(snapshot_info$database, "Sales at month end")
  expect_equal(snapshot_info$target_type, "warehouse")
  expect_equal(snapshot_info$source, "discovery")

  mirrored <- structure(
    list(
      id = "mirrored-id",
      displayName = "Operational replica",
      type = "MirroredDatabase",
      workspaceId = "workspace-id",
      properties = list(
        sqlEndpointProperties = list(
          connectionString = "mirror.datawarehouse.fabric.microsoft.com"
        )
      ),
      sql_server = "mirror.datawarehouse.fabric.microsoft.com"
    ),
    class = c("fabric_item", "list")
  )
  mirrored_info <- fabric_sql_connection_info(mirrored)
  expect_equal(
    mirrored_info$server,
    "mirror.datawarehouse.fabric.microsoft.com"
  )
  expect_equal(mirrored_info$database, "Operational replica")
  expect_equal(mirrored_info$target_type, "sql_analytics_endpoint")
  expect_equal(mirrored_info$source, "discovery")

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

test_that("SQL targets allow Fabric master and validate malformed inputs", {
  master <- fabric_sql_connection_info(
    "server.datawarehouse.fabric.microsoft.com"
  )
  expect_null(master$database)
  expect_equal(master$target_type, "sql_analytics_endpoint")

  captured <- NULL
  connection <- structure(list(), class = "test_connection")
  local_mocked_bindings(
    .fabric_sql_db_connect = function(...) {
      captured <<- list(...)
      connection
    }
  )
  expect_identical(
    fabric_sql_connect(
      "server.datawarehouse.fabric.microsoft.com",
      token = "token",
      verbose = FALSE
    ),
    connection
  )
  expect_false("database" %in% names(captured))
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
    token = "sql-token",
    read_only = TRUE,
    port = 1544L,
    timeout = 17L,
    verbose = FALSE
  )

  expect_identical(result, connection)
  expect_equal(captured$backend, "odbc")
  expect_equal(
    captured$server,
    "tcp:server.datawarehouse.fabric.microsoft.com,1544"
  )
  expect_equal(captured$database, "Warehouse")
  expect_false("Port" %in% names(captured))
  expect_equal(captured$MARS_Connection, "no")
  expect_equal(captured$ApplicationIntent, "ReadOnly")
  expect_equal(captured$timeout, 17L)
  expect_equal(captured$attributes$azure_token, "sql-token")
})

test_that("ODBC passthrough attributes merge without token override", {
  captured <- NULL
  connection <- structure(list(), class = "test_connection")
  local_mocked_bindings(
    .fabric_sql_db_connect = function(...) {
      captured <<- list(...)
      connection
    }
  )

  result <- fabric_sql_connect(
    "server.datawarehouse.fabric.microsoft.com",
    token = "sql-token",
    attributes = list(trace = "yes", custom = "value"),
    verbose = FALSE
  )
  expect_identical(result, connection)
  expect_equal(
    captured$attributes,
    list(trace = "yes", custom = "value", azure_token = "sql-token")
  )

  for (attributes in list(
    list(azure_token = "caller-token"),
    list(AZURE_TOKEN = "caller-token")
  )) {
    expect_error(
      fabric_sql_connect(
        "server.datawarehouse.fabric.microsoft.com",
        token = "sql-token",
        attributes = attributes,
        verbose = FALSE
      ),
      "cannot override",
      fixed = TRUE
    )
  }
  expect_error(
    fabric_sql_connect(
      "server.datawarehouse.fabric.microsoft.com",
      token = "sql-token",
      attributes = "not-a-list",
      verbose = FALSE
    ),
    "must be a named list",
    fixed = TRUE
  )
})

test_that("ODBC access tokens reject conflicting authentication options", {
  conflicting <- list(
    list(UID = "user@example.com"),
    list(pwd = "secret"),
    list(Authentication = "ActiveDirectoryPassword"),
    list(TRUSTED_CONNECTION = "yes")
  )

  for (option in conflicting) {
    error <- expect_error(
      do.call(
        fabric_sql_connect,
        c(
          list(
            server = "server.datawarehouse.fabric.microsoft.com",
            token = "sql-token",
            verbose = FALSE
          ),
          option
        )
      ),
      class = "fabric_sql_authentication_error"
    )
    expect_identical(error$conflicting_options, names(option))
    expect_identical(error$location, "...")
  }

  for (attributes in conflicting) {
    error <- expect_error(
      fabric_sql_connect(
        "server.datawarehouse.fabric.microsoft.com",
        token = "sql-token",
        attributes = attributes,
        verbose = FALSE
      ),
      class = "fabric_sql_authentication_error"
    )
    expect_identical(error$conflicting_options, names(attributes))
    expect_identical(error$location, "attributes")
  }
})

test_that("SQL accepts an explicitly supplied custom endpoint", {
  local_mocked_bindings(
    fabric_sql_require_backend = function(...) invisible(TRUE),
    fabric_sql_load_adbc_driver = function(...) list(),
    .fabric_sql_db_connect = function(...) {
      structure(list(), class = "test_connection")
    }
  )

  expect_s3_class(
    fabric_sql_connect(
      "sql.example.test",
      token = "sql-token",
      verbose = FALSE
    ),
    "test_connection"
  )
  expect_silent(fabric_sql_validate_endpoint("tenant.database.windows.net"))
  legacy_warehouse_hosts <- c(
    "tenant.datawarehouse.pbidedicated.microsoft.com",
    "tenant.pbidedicated.microsoft.com",
    "tenant.pbidedicated.windows.net"
  )
  for (host in legacy_warehouse_hosts) {
    expect_silent(fabric_sql_validate_endpoint(host))
    expect_equal(
      fabric_sql_connection_info(host)$target_type,
      "sql_analytics_endpoint"
    )
  }
  generic_analytics_host <- "warehouse-id.contoso.fabric.microsoft.com"
  expect_silent(fabric_sql_validate_endpoint(generic_analytics_host))
  expect_identical(
    fabric_sql_connection_info(generic_analytics_host)$target_type,
    "sql_analytics_endpoint"
  )
  expect_identical(
    fabric_sql_connection_info(
      "sql-id.database.fabric.microsoft.com"
    )$target_type,
    "sql_database"
  )
  expect_equal(
    fabric_sql_validate_endpoint("custom.example.test"),
    "custom.example.test"
  )
  parser_confusion_hosts <- c(
    "evil.example?x=.datawarehouse.fabric.microsoft.com",
    "evil.example/path.datawarehouse.fabric.microsoft.com",
    "evil.example#x=.database.windows.net",
    "user@tenant.database.windows.net",
    "tenant.database.windows.net:1433",
    "tenant.database.windows.net\r\nattacker.example"
  )
  for (host in parser_confusion_hosts) {
    acquired <- FALSE
    expect_error(
      fabric_sql_validate_endpoint(host),
      class = "fabric_sql_endpoint_error"
    )
    error <- tryCatch(
      fabric_sql_connect(
        host,
        target_type = "warehouse",
        backend = "adbc",
        token = function(...) {
          acquired <<- TRUE
          "sql-token"
        },
        verbose = FALSE
      ),
      error = identity
    )
    expect_true(
      inherits(error, "fabric_sql_endpoint_error") ||
        inherits(error, "fabric_sql_target_error")
    )
    expect_identical(acquired, FALSE)
  }
  expect_identical(
    fabric_sql_validate_endpoint("TENANT.DATABASE.WINDOWS.NET."),
    "tenant.database.windows.net"
  )
})

test_that("SQL entry points reject automatic credentials for custom hosts", {
  local_mocked_bindings(
    fabric_sql_require_backend = function(...) invisible(TRUE)
  )
  connect_error <- rlang::catch_cnd(
    fabric_sql_connect(
      "sql.example.test",
      tenant_id = "tenant",
      client_id = "client",
      verbose = FALSE
    ),
    classes = "error"
  )
  query_error <- rlang::catch_cnd(
    fabric_sql_query(
      "sql.example.test",
      "SELECT 1",
      tenant_id = "tenant",
      client_id = "client",
      verbose = FALSE
    ),
    classes = "error"
  )

  for (error in list(connect_error, query_error)) {
    expect_s3_class(error, "fabric_custom_endpoint_requires_token")
    expect_identical(error$endpoint_host, "sql.example.test")
    expect_identical(error$argument, "server")
  }
})

test_that("SQL timeouts are not constrained by the TCP port range", {
  expect_silent(fabric_sql_timeout(86400))
  expect_silent(fabric_sql_timeout(0))
  for (value in list(
    -1,
    1.5,
    Inf,
    NA_real_,
    "30",
    c(1, 2),
    .Machine$integer.max + 1
  )) {
    expect_error(
      fabric_sql_timeout(value),
      "non-negative whole number",
      fixed = TRUE,
      class = "fabric_sql_target_error"
    )
  }
})

test_that("SQL connections configure the ADBC MSSQL driver with a safe URI", {
  captured <- NULL
  connection <- structure(list(), class = "test_connection")
  token <- "token+/with=?&reserved"
  local_mocked_bindings(
    fabric_sql_load_adbc_driver = function(...) "mssql",
    .fabric_sql_db_connect = function(...) {
      captured <<- list(...)
      connection
    }
  )

  result <- fabric_sql_connect(
    server = "server.datawarehouse.fabric.microsoft.com",
    database = "Warehouse with space",
    backend = "adbc",
    token = token,
    read_only = TRUE,
    timeout = 17L,
    verbose = FALSE
  )

  parsed <- httr2::url_parse(captured$uri)
  expect_identical(result, connection)
  expect_equal(captured$backend, "adbc")
  expect_equal(captured$adbc_driver, "mssql")
  expect_equal(parsed$scheme, "sqlserver")
  expect_equal(parsed$hostname, "server.datawarehouse.fabric.microsoft.com")
  expect_equal(parsed$port, "1433")
  expect_equal(parsed$query$database, "Warehouse with space")
  expect_equal(
    parsed$query$fedauth,
    "ActiveDirectoryServicePrincipalAccessToken"
  )
  expect_equal(parsed$query$password, token)
  expect_equal(parsed$query$encrypt, "true")
  expect_equal(parsed$query$TrustServerCertificate, "false")
  expect_equal(parsed$query$`connection timeout`, "17")
  expect_equal(parsed$query$ApplicationIntent, "ReadOnly")
  expect_false(grepl(token, captured$uri, fixed = TRUE))
  expect_false("attributes" %in% names(captured))
})

test_that("missing ADBC drivers fail before authentication with install guidance", {
  acquired <- FALSE
  missing_driver <- "fabricqueryr_missing_mssql_driver"

  error <- tryCatch(
    fabric_sql_connect(
      "server.datawarehouse.fabric.microsoft.com",
      backend = "adbc",
      adbc_driver = missing_driver,
      token = function(...) {
        acquired <<- TRUE
        "token"
      },
      verbose = FALSE
    ),
    error = identity
  )

  expect_s3_class(error, "fabric_sql_driver_error")
  expect_s3_class(error, "fabric_sql_connection_error")
  expect_false(acquired)
  expect_match(
    conditionMessage(error),
    paste0("dbc install ", missing_driver),
    fixed = TRUE
  )
  expect_match(conditionMessage(error), "adbcdrivermanager", fixed = TRUE)
})

test_that("SQL connections retry transient Fabric failures with fresh tokens", {
  attempts <- 0L
  refreshes <- logical()
  delays <- numeric()
  connection <- structure(list(), class = "test_connection")
  local_mocked_bindings(
    .fabric_sql_db_connect = function(...) {
      attempts <<- attempts + 1L
      if (attempts < 3L) {
        rlang::abort("Error 6008: Workspace is temporarily unavailable")
      }
      connection
    },
    .fabric_sql_sleep = function(delay) {
      delays <<- c(delays, delay)
    },
    .fabric_sql_runif = function(...) 1
  )

  result <- fabric_sql_connect(
    "server.datawarehouse.fabric.microsoft.com",
    token = function(audience, force_refresh = FALSE) {
      refreshes <<- c(refreshes, force_refresh)
      "token"
    },
    max_tries = 3L,
    retry_delay = 5,
    verbose = FALSE
  )

  expect_identical(result, connection)
  expect_equal(attempts, 3L)
  expect_identical(refreshes, c(FALSE, TRUE, TRUE))
  expect_equal(delays, c(5, 10))
})

test_that("ADBC connection retries rebuild the URI with a fresh token", {
  attempts <- 0L
  tokens <- character()
  connection <- structure(list(), class = "test_connection")
  local_mocked_bindings(
    fabric_sql_load_adbc_driver = function(...) "mssql",
    .fabric_sql_db_connect = function(...) {
      args <- list(...)
      attempts <<- attempts + 1L
      tokens <<- c(tokens, httr2::url_parse(args$uri)$query$password)
      if (attempts == 1L) {
        rlang::abort("Error 6008: temporarily unavailable")
      }
      connection
    },
    .fabric_sql_sleep = function(...) invisible(NULL),
    .fabric_sql_runif = function(...) 1
  )

  result <- fabric_sql_connect(
    "server.datawarehouse.fabric.microsoft.com",
    backend = "adbc",
    token = function(audience, force_refresh = FALSE) {
      if (force_refresh) "fresh-token" else "initial-token"
    },
    max_tries = 2L,
    retry_delay = 0,
    verbose = FALSE
  )

  expect_identical(result, connection)
  expect_identical(tokens, c("initial-token", "fresh-token"))
})

test_that("fabric_sql_query passes bound parameters unchanged", {
  connection <- structure(list(), class = "test_connection")
  captured <- NULL
  disconnected <- FALSE
  disconnect_force <- NULL
  values <- list(
    "Robert'); DROP TABLE Students;--",
    as.Date("2026-07-24"),
    NA_character_,
    NULL
  )
  local_mocked_bindings(
    fabric_sql_connect = function(...) connection,
    .fabric_sql_db_get_query = function(
      con,
      sql,
      params = NULL,
      result = "tibble"
    ) {
      captured <<- list(
        con = con,
        sql = sql,
        params = params,
        result = result
      )
      data.frame(ok = TRUE)
    },
    .fabric_sql_db_disconnect = function(con, force = FALSE) {
      disconnected <<- TRUE
      disconnect_force <<- force
      invisible(TRUE)
    }
  )

  result <- fabric_sql_query(
    "unused",
    "SELECT ?, ?, ?, ?",
    params = values,
    token = "token",
    verbose = FALSE
  )

  expect_s3_class(result, "tbl_df")
  expect_identical(captured$params, values)
  expect_identical(captured$sql, "SELECT ?, ?, ?, ?")
  expect_identical(captured$result, "tibble")
  expect_true(disconnected)
  expect_true(disconnect_force)
})

test_that("fabric_sql_query accepts exactly one read-only SELECT", {
  statements <- c(
    "INSERT INTO dbo.t VALUES (1)",
    "UPDATE dbo.t SET value = 2",
    "DELETE FROM dbo.t",
    "CREATE TABLE dbo.t (value int)",
    "WITH doomed AS (SELECT * FROM dbo.t) DELETE FROM doomed",
    "SELECT 1; DELETE FROM dbo.t",
    "SELECT 1\nDELETE FROM dbo.t",
    "SELECT 1\nDROP TABLE dbo.t",
    "SELECT 1\nUPDATE dbo.t SET value = 2",
    "SELECT 1\nINSERT INTO dbo.t VALUES (1)",
    "SELECT 1\nEXEC dbo.proc",
    "SELECT 1\nCREATE TABLE dbo.t(value int)",
    "WITH source AS (SELECT 1 AS value) SELECT value FROM source\nDROP TABLE dbo.t",
    "CREATE TABLE dbo.t(value int); SELECT 1",
    "EXEC dbo.proc; SELECT 1",
    "SELECT * INTO dbo.copy FROM dbo.source",
    "SELECT 1;;"
  )
  for (backend in c("odbc", "adbc")) {
    for (statement in statements) {
      expect_error(
        fabric_sql_query(
          "server.datawarehouse.fabric.microsoft.com",
          statement,
          backend = backend,
          token = "token"
        ),
        "only result-producing SELECT statements",
        class = "fabric_sql_statement_error"
      )
    }
  }
  expect_silent(fabric_sql_validate_query_statement(
    "WITH source AS (SELECT 1 AS value) SELECT value FROM source"
  ))
  expect_silent(fabric_sql_validate_query_statement(
    paste0(
      "SELECT ';' AS terminator, 'INTO' AS keyword ",
      "/* ; DELETE INTO */ -- ; EXEC\n;"
    )
  ))
  expect_silent(fabric_sql_validate_query_statement(
    paste0(
      "SELECT /* outer comment /* nested ; SELECT * INTO dbo.hidden */ ",
      "still inside outer */ 1 AS value"
    )
  ))
  expect_error(
    fabric_sql_validate_query_statement(
      paste0(
        "SELECT /* outer /* nested INTO dbo.hidden */ outer */ * ",
        "INTO dbo.copy FROM dbo.source"
      )
    ),
    class = "fabric_sql_statement_error"
  )
  expect_error(
    fabric_sql_validate_query_statement("SELECT 1 /* outer /* nested */"),
    "unterminated block comment",
    fixed = TRUE
  )
})

test_that("ADBC parameter translation ignores SQL literals and comments", {
  sql <- paste0(
    "SELECT ?, '?', \"?\", [?], [a]]?], ",
    "'it''s ?' -- ?\n",
    "FROM t /* ? /* nested ? */ still ? */ WHERE x = ?"
  )
  translated <- fabric_sql_adbc_parameter_sql(sql, list(1L, 2L))

  expect_equal(
    translated,
    paste0(
      "SELECT @p1, '?', \"?\", [?], [a]]?], ",
      "'it''s ?' -- ?\n",
      "FROM t /* ? /* nested ? */ still ? */ WHERE x = @p2"
    )
  )
  expect_error(
    fabric_sql_adbc_parameter_sql("SELECT ?, ?", list(1L)),
    "2 SQL placeholders for 1 value",
    fixed = TRUE,
    class = "fabric_sql_execution_error"
  )
})

test_that("fabric_sql_query uses ADBC parameters and returns Arrow streams", {
  connection <- structure(list(), class = "test_connection")
  fake_stream <- nanoarrow::basic_array_stream(list(data.frame(id = 1L)))
  query_result <- structure(list(), class = "test_result")
  connect_args <- NULL
  query_args <- NULL
  cleared <- FALSE
  disconnected <- FALSE
  disconnect_force <- NULL
  local_mocked_bindings(
    fabric_sql_connect = function(...) {
      connect_args <<- list(...)
      connection
    },
    .fabric_sql_db_get_query = function(
      con,
      sql,
      params = NULL,
      result = "tibble"
    ) {
      query_args <<- list(
        con = con,
        sql = sql,
        params = params,
        result = result
      )
      .fabric_sql_own_arrow_stream(fake_stream, query_result, con)
    },
    .fabric_sql_db_clear_result = function(result) {
      expect_identical(result, query_result)
      cleared <<- TRUE
      invisible(TRUE)
    },
    .fabric_sql_db_disconnect = function(con, force = FALSE) {
      disconnected <<- TRUE
      disconnect_force <<- force
      invisible(TRUE)
    }
  )

  result <- fabric_sql_query(
    "unused",
    "SELECT ? AS value, '?' AS literal",
    params = list(42L),
    backend = "adbc",
    result = "arrow_stream",
    token = "token",
    verbose = FALSE
  )

  expect_s3_class(result, "nanoarrow_array_stream")
  expect_equal(connect_args$backend, "adbc")
  expect_equal(connect_args$adbc_driver, "mssql")
  expect_equal(query_args$sql, "SELECT @p1 AS value, '?' AS literal")
  expect_identical(query_args$params, list("@p1" = 42L))
  expect_equal(query_args$result, "arrow_stream")
  expect_false(cleared)
  expect_false(disconnected)

  nanoarrow::nanoarrow_pointer_release(result)
  gc()
  expect_true(cleared)
  expect_true(disconnected)
  expect_true(disconnect_force)
})

test_that("ADBC bind frames preserve SQL Server placeholder names", {
  params <- list(
    "@p1" = 42L,
    "@p2" = as.Date("2026-07-24"),
    "@p3" = NA_character_
  )

  frame <- .fabric_sql_adbc_bind_frame(params)

  expect_s3_class(frame, "data.frame")
  expect_identical(names(frame), names(params))
  expect_identical(frame[["@p1"]], I(42L))
  expect_identical(frame[["@p2"]], I(as.Date("2026-07-24")))
  expect_identical(frame[["@p3"]], I(NA_character_))
})

test_that("ADBC Arrow binding preserves SQL Server placeholder names", {
  result <- structure(list(), class = "AdbiResultArrow")
  params <- list("@p1" = 42L)
  bound <- NULL
  local_mocked_bindings(
    dbBind = function(res, values) {
      expect_identical(res, result)
      bound <<- values
      invisible(res)
    },
    .package = "DBI"
  )

  .fabric_sql_db_bind(result, params)

  expect_s3_class(bound, "data.frame")
  expect_identical(names(bound), "@p1")
  expect_identical(bound[["@p1"]], I(42L))
})

test_that("SQL connection adapters construct ODBC and ADBC connections", {
  skip_if_not_installed("odbc")
  skip_if_not_installed("adbi")
  skip_if_not_installed("adbcdrivermanager")

  odbc_driver <- structure(list(name = "odbc"), class = "test_odbc_driver")
  managed_driver <- structure(list(name = "adbc"), class = "adbc_driver")
  adbi_driver <- structure(list(driver = managed_driver), class = "test_adbi")
  connection_calls <- list()
  manager_calls <- character()

  local_mocked_bindings(
    odbc = function() odbc_driver,
    .package = "odbc"
  )
  local_mocked_bindings(
    adbc_driver = function(driver) {
      manager_calls <<- c(manager_calls, driver)
      managed_driver
    },
    .package = "adbcdrivermanager"
  )
  local_mocked_bindings(
    adbi = function(driver) {
      expect_identical(driver, managed_driver)
      adbi_driver
    },
    .package = "adbi"
  )
  local_mocked_bindings(
    dbConnect = function(driver, ...) {
      connection_calls[[length(connection_calls) + 1L]] <<- list(
        driver = driver,
        args = list(...)
      )
      structure(
        list(index = length(connection_calls)),
        class = if (identical(driver, adbi_driver)) {
          "AdbiConnection"
        } else {
          "OdbcConnection"
        }
      )
    },
    .package = "DBI"
  )

  odbc_connection <- .fabric_sql_db_connect(
    "odbc",
    server = "sql.fabric.microsoft.com"
  )
  adbc_connection <- .fabric_sql_db_connect(
    "adbc",
    adbc_driver = "adbc_mssql",
    uri = "sql.fabric.microsoft.com"
  )
  direct_connection <- .fabric_sql_db_connect(
    "adbc",
    adbc_driver = managed_driver,
    uri = "sql.fabric.microsoft.com"
  )

  expect_s3_class(odbc_connection, "OdbcConnection")
  expect_s3_class(adbc_connection, "AdbiConnection")
  expect_s3_class(direct_connection, "AdbiConnection")
  expect_identical(connection_calls[[1L]]$driver, odbc_driver)
  expect_identical(
    connection_calls[[1L]]$args$server,
    "sql.fabric.microsoft.com"
  )
  expect_identical(manager_calls, "adbc_mssql")
  expect_identical(connection_calls[[2L]]$driver, adbi_driver)
  expect_identical(connection_calls[[3L]]$driver, adbi_driver)
})

test_that("SQL query adapters preserve result shape and disconnect semantics", {
  connection <- structure(list(), class = "OdbcConnection")
  adbc_connection <- structure(list(), class = "AdbiConnection")
  tabular_result <- structure(list(kind = "tabular"), class = "DBIResult")
  arrow_result <- structure(list(kind = "arrow"), class = "DBIResultArrow")
  arrow_stream <- structure(list(kind = "stream"), class = "test_arrow_stream")
  calls <- list()

  record <- function(name, ...) {
    calls[[length(calls) + 1L]] <<- c(list(name = name), list(...))
  }
  local_mocked_bindings(
    dbSendQuery = function(conn, statement, immediate = NULL, ...) {
      record("send", conn = conn, statement = statement, immediate = immediate)
      tabular_result
    },
    dbSendQueryArrow = function(conn, statement, immediate = NULL, ...) {
      record(
        "send_arrow",
        conn = conn,
        statement = statement,
        immediate = immediate
      )
      arrow_result
    },
    dbFetch = function(res, ...) {
      record("fetch", result = res)
      data.frame(value = 1L)
    },
    dbFetchArrow = function(res, ...) {
      record("fetch_arrow", result = res)
      arrow_stream
    },
    dbDisconnect = function(conn, ...) {
      record("disconnect", conn = conn, args = list(...))
      invisible(TRUE)
    },
    .package = "DBI"
  )

  expect_identical(
    .fabric_sql_db_send_query(connection, "SELECT 1", "tibble"),
    tabular_result
  )
  expect_identical(
    .fabric_sql_db_send_query(
      connection,
      "SELECT 2",
      "arrow_stream",
      immediate = TRUE
    ),
    arrow_result
  )
  expect_identical(
    .fabric_sql_db_fetch(tabular_result, "tibble"),
    data.frame(value = 1L)
  )
  expect_identical(
    .fabric_sql_db_fetch(arrow_result, "arrow_stream"),
    arrow_stream
  )
  expect_true(.fabric_sql_db_disconnect(connection, force = TRUE))
  expect_true(.fabric_sql_db_disconnect(adbc_connection, force = TRUE))

  expect_identical(
    vapply(calls, `[[`, character(1), "name"),
    c(
      "send",
      "send_arrow",
      "fetch",
      "fetch_arrow",
      "disconnect",
      "disconnect"
    )
  )
  expect_false(calls[[1L]]$immediate)
  expect_true(calls[[2L]]$immediate)
  expect_length(calls[[5L]]$args, 0L)
  expect_identical(calls[[6L]]$args$force, TRUE)
})

test_that("ADBC bind failures clear the result before disconnecting", {
  connection <- structure(list(), class = "AdbiConnection")
  query_result <- structure(list(), class = "test_result")
  cleared <- FALSE
  local_mocked_bindings(
    .fabric_sql_db_send_query = function(...) query_result,
    .fabric_sql_db_bind = function(...) rlang::abort("bind failed"),
    .fabric_sql_db_fetch = function(...) {
      rlang::abort("fetch should not be called")
    },
    .fabric_sql_db_clear_result = function(result) {
      expect_identical(result, query_result)
      cleared <<- TRUE
      invisible(TRUE)
    }
  )

  expect_error(
    .fabric_sql_db_get_query(
      connection,
      "SELECT @p1",
      params = list("@p1" = 1L)
    ),
    "bind failed",
    fixed = TRUE
  )
  expect_true(cleared)
})

test_that("ADBC bound Arrow queries own their result until release", {
  connection <- structure(list(), class = "AdbiConnection")
  query_result <- structure(list(), class = "test_result")
  stream <- nanoarrow::basic_array_stream(list(data.frame(value = 42L)))
  events <- character()
  params <- list("@p1" = 42L)
  local_mocked_bindings(
    .fabric_sql_db_send_query = function(
      con,
      sql,
      result,
      immediate = FALSE
    ) {
      expect_identical(con, connection)
      expect_identical(sql, "SELECT @p1 AS value")
      expect_identical(result, "arrow_stream")
      expect_false(immediate)
      events <<- c(events, "send")
      query_result
    },
    .fabric_sql_db_bind = function(result, values) {
      expect_identical(result, query_result)
      expect_identical(values, params)
      events <<- c(events, "bind")
      invisible(result)
    },
    .fabric_sql_db_fetch = function(result, shape) {
      expect_identical(result, query_result)
      expect_identical(shape, "arrow_stream")
      events <<- c(events, "fetch")
      stream
    },
    .fabric_sql_db_clear_result = function(result) {
      expect_identical(result, query_result)
      events <<- c(events, "clear")
      invisible(TRUE)
    },
    .fabric_sql_db_disconnect = function(con, force = FALSE) {
      expect_identical(con, connection)
      expect_true(force)
      events <<- c(events, "disconnect")
      invisible(TRUE)
    }
  )

  result <- .fabric_sql_db_get_query(
    connection,
    "SELECT @p1 AS value",
    params = params,
    result = "arrow_stream"
  )

  expect_s3_class(result, "nanoarrow_array_stream")
  expect_identical(events, c("send", "bind", "fetch"))

  nanoarrow::nanoarrow_pointer_release(result)
  gc()
  expect_identical(events, c("send", "bind", "fetch", "clear", "disconnect"))
  nanoarrow::nanoarrow_pointer_release(result)
  gc()
  expect_identical(events, c("send", "bind", "fetch", "clear", "disconnect"))
})

test_that("ODBC bound Arrow queries own their result until release", {
  connection <- structure(list(), class = "OdbcConnection")
  query_result <- structure(list(), class = "test_result")
  stream <- nanoarrow::basic_array_stream(list(data.frame(value = 42L)))
  events <- character()
  params <- list(42L)
  local_mocked_bindings(
    .fabric_sql_db_send_query = function(
      con,
      sql,
      result,
      immediate = FALSE
    ) {
      expect_identical(con, connection)
      expect_identical(sql, "SELECT CAST(? AS int) AS value")
      expect_identical(result, "arrow_stream")
      expect_false(immediate)
      events <<- c(events, "send")
      query_result
    },
    .fabric_sql_db_bind = function(result, values) {
      expect_identical(result, query_result)
      expect_identical(values, params)
      events <<- c(events, "bind")
      invisible(result)
    },
    .fabric_sql_db_fetch = function(result, shape) {
      expect_identical(result, query_result)
      expect_identical(shape, "arrow_stream")
      events <<- c(events, "fetch")
      stream
    },
    .fabric_sql_db_clear_result = function(result) {
      expect_identical(result, query_result)
      events <<- c(events, "clear")
      invisible(TRUE)
    },
    .fabric_sql_db_disconnect = function(con, force = FALSE) {
      expect_identical(con, connection)
      expect_true(force)
      events <<- c(events, "disconnect")
      invisible(TRUE)
    }
  )

  result <- .fabric_sql_db_get_query(
    connection,
    "SELECT CAST(? AS int) AS value",
    params = params,
    result = "arrow_stream"
  )

  expect_s3_class(result, "nanoarrow_array_stream")
  expect_identical(events, c("send", "bind", "fetch"))

  nanoarrow::nanoarrow_pointer_release(result)
  gc()
  expect_identical(events, c("send", "bind", "fetch", "clear", "disconnect"))
})

test_that("unbound Arrow queries use an owned DBI result", {
  connection <- structure(list(), class = "OdbcConnection")
  query_result <- structure(list(), class = "test_result")
  stream <- nanoarrow::basic_array_stream(list(data.frame(value = 1L)))
  events <- character()
  local_mocked_bindings(
    .fabric_sql_db_send_query = function(
      con,
      sql,
      result,
      immediate = FALSE
    ) {
      expect_identical(con, connection)
      expect_identical(sql, "SELECT 1 AS value")
      expect_identical(result, "arrow_stream")
      expect_true(immediate)
      events <<- c(events, "send")
      query_result
    },
    .fabric_sql_db_bind = function(...) {
      rlang::abort("an unbound query must not bind")
    },
    .fabric_sql_db_fetch = function(result, shape) {
      expect_identical(result, query_result)
      expect_identical(shape, "arrow_stream")
      events <<- c(events, "fetch")
      stream
    },
    .fabric_sql_db_clear_result = function(result) {
      expect_identical(result, query_result)
      events <<- c(events, "clear")
      invisible(TRUE)
    },
    .fabric_sql_db_disconnect = function(con, force = FALSE) {
      expect_identical(con, connection)
      expect_true(force)
      events <<- c(events, "disconnect")
      invisible(TRUE)
    }
  )

  result <- .fabric_sql_db_get_query(
    connection,
    "SELECT 1 AS value",
    result = "arrow_stream"
  )

  expect_s3_class(result, "nanoarrow_array_stream")
  expect_identical(events, c("send", "fetch"))

  nanoarrow::nanoarrow_pointer_release(result)
  gc()
  expect_identical(events, c("send", "fetch", "clear", "disconnect"))
})

test_that("Arrow streams convert directly to arrow RecordBatchReader objects", {
  skip_if_not_installed("arrow")
  connection <- structure(list(), class = "test_connection")
  query_result <- structure(list(), class = "test_result")
  stream <- nanoarrow::basic_array_stream(
    list(data.frame(id = 1:2, value = c("a", "b")))
  )
  cleared <- 0L
  disconnected <- 0L
  local_mocked_bindings(
    fabric_sql_connect = function(...) connection,
    .fabric_sql_db_get_query = function(...) {
      .fabric_sql_own_arrow_stream(stream, query_result, connection)
    },
    .fabric_sql_db_clear_result = function(result) {
      expect_identical(result, query_result)
      cleared <<- cleared + 1L
      invisible(TRUE)
    },
    .fabric_sql_db_disconnect = function(con, force = FALSE) {
      expect_identical(con, connection)
      expect_true(force)
      disconnected <<- disconnected + 1L
      invisible(TRUE)
    }
  )

  result <- fabric_sql_query(
    "unused",
    "SELECT id, value FROM dbo.items",
    result = "arrow_stream",
    token = "token",
    verbose = FALSE
  )
  reader <- arrow::as_record_batch_reader(result)
  rm(result)
  gc()
  expect_identical(cleared, 0L)
  expect_identical(disconnected, 0L)

  table <- reader$read_table()
  rm(reader)
  gc()

  expect_equal(
    as.data.frame(table),
    data.frame(id = 1:2, value = c("a", "b"))
  )
  expect_identical(cleared, 1L)
  expect_identical(disconnected, 1L)
})

test_that("failed Arrow queries force connection cleanup", {
  connection <- structure(list(), class = "test_connection")
  disconnect_force <- NULL
  local_mocked_bindings(
    fabric_sql_connect = function(...) connection,
    .fabric_sql_db_get_query = function(...) {
      rlang::abort("stream failed")
    },
    .fabric_sql_db_disconnect = function(con, force = FALSE) {
      disconnect_force <<- force
      invisible(TRUE)
    }
  )

  expect_error(
    fabric_sql_query(
      "unused",
      "SELECT 1",
      result = "arrow_stream",
      token = "token",
      verbose = FALSE
    ),
    class = "fabric_sql_execution_error"
  )
  expect_true(disconnect_force)
})

test_that("SQL query retries require idempotency and use fresh connections", {
  connections <- 0L
  queries <- 0L
  disconnected <- integer()
  delays <- numeric()
  refreshes <- logical()
  local_mocked_bindings(
    fabric_sql_connect = function(token, ...) {
      token(.fabric_audience$sql, force_refresh = FALSE)
      connections <<- connections + 1L
      structure(list(id = connections), class = "test_connection")
    },
    .fabric_sql_db_get_query = function(
      con,
      sql,
      params = NULL,
      result = "tibble"
    ) {
      queries <<- queries + 1L
      if (queries == 1L) {
        rlang::abort("Error 24804: operation interrupted by a system update")
      }
      data.frame(connection_id = con$id)
    },
    .fabric_sql_db_disconnect = function(con, ...) {
      disconnected <<- c(disconnected, con$id)
      invisible(TRUE)
    },
    .fabric_sql_sleep = function(delay) {
      delays <<- c(delays, delay)
    },
    .fabric_sql_runif = function(...) 1
  )

  result <- fabric_sql_query(
    "server",
    "SELECT 1",
    token = function(audience, force_refresh = FALSE) {
      refreshes <<- c(refreshes, force_refresh)
      "token"
    },
    idempotent = TRUE,
    max_tries = 3L,
    retry_delay = 5,
    verbose = FALSE
  )

  expect_equal(result$connection_id, 2L)
  expect_equal(connections, 2L)
  expect_identical(disconnected, c(1L, 2L))
  expect_equal(delays, 5)
  expect_identical(refreshes, c(FALSE, TRUE))

  connections <- 0L
  queries <- 0L
  disconnected <- integer()
  delays <- numeric()
  refreshes <- logical()
  expect_error(
    fabric_sql_query(
      "server",
      "UPDATE dbo.items SET value = 1",
      token = function(audience, force_refresh = FALSE) {
        refreshes <<- c(refreshes, force_refresh)
        "token"
      },
      max_tries = 3L,
      retry_delay = 5,
      verbose = FALSE
    ),
    class = "fabric_sql_statement_error"
  )
  expect_equal(connections, 0L)
  expect_equal(queries, 0L)
  expect_length(disconnected, 0L)
  expect_length(delays, 0L)
  expect_length(refreshes, 0L)
})

test_that("SQL retry controls reject invalid values", {
  expect_error(
    fabric_sql_connect(
      "server.datawarehouse.fabric.microsoft.com",
      token = "token",
      max_tries = 0,
      verbose = FALSE
    ),
    "max_tries",
    fixed = TRUE
  )
  expect_error(
    fabric_sql_connect(
      "server.datawarehouse.fabric.microsoft.com",
      token = "token",
      max_tries = .Machine$integer.max + 1,
      verbose = FALSE
    ),
    "max_tries",
    fixed = TRUE
  )
  expect_error(
    fabric_sql_query(
      "server",
      "SELECT 1",
      token = "token",
      retry_delay = -1,
      verbose = FALSE
    ),
    "retry_delay",
    fixed = TRUE
  )
  expect_error(
    fabric_sql_query(
      "server",
      "SELECT 1",
      token = "token",
      idempotent = NA,
      verbose = FALSE
    ),
    "idempotent",
    fixed = TRUE
  )
  expect_error(
    fabric_sql_connect(
      "server",
      backend = "invalid",
      token = "token",
      verbose = FALSE
    ),
    "should be one of",
    fixed = TRUE
  )
  expect_error(
    fabric_sql_query(
      "server",
      "SELECT 1",
      result = "invalid",
      token = "token",
      verbose = FALSE
    ),
    "should be one of",
    fixed = TRUE
  )
})

test_that("SQL failures have actionable condition classes", {
  local_mocked_bindings(
    .fabric_sql_db_connect = function(...) {
      rlang::abort("Login failed for user; error 18456")
    }
  )
  expect_error(
    fabric_sql_connect(
      "server.datawarehouse.fabric.microsoft.com",
      database = "db",
      token = "token",
      verbose = FALSE
    ),
    class = "fabric_sql_authentication_error"
  )

  secret <- "sensitive+/token"
  local_mocked_bindings(
    fabric_sql_load_adbc_driver = function(...) "mssql",
    .fabric_sql_db_connect = function(...) {
      args <- list(...)
      rlang::abort(paste("driver rejected", args$uri))
    }
  )
  error <- tryCatch(
    fabric_sql_connect(
      "server.datawarehouse.fabric.microsoft.com",
      database = "db",
      backend = "adbc",
      token = secret,
      verbose = FALSE
    ),
    error = identity
  )
  expect_s3_class(error, "fabric_sql_connection_error")
  expect_false(grepl(secret, conditionMessage(error), fixed = TRUE))
  expect_false(grepl(
    "password=sensitive",
    conditionMessage(error),
    fixed = TRUE
  ))
  expect_match(conditionMessage(error), "password=<redacted>", fixed = TRUE)

  parent_secret <- "odbc-parent-sentinel-secret"
  parent_error <- tryCatch(
    fabric_sql_connection_error(
      simpleError(paste("Bearer", parent_secret)),
      secrets = parent_secret
    ),
    error = identity
  )
  expect_false(grepl(
    parent_secret,
    conditionMessage(parent_error$parent),
    fixed = TRUE
  ))

  local_mocked_bindings(
    fabric_sql_connect = function(...) structure(list(), class = "connection"),
    .fabric_sql_db_get_query = function(...) rlang::abort("syntax error"),
    .fabric_sql_db_disconnect = function(...) invisible(TRUE)
  )
  expect_error(
    fabric_sql_query(
      "server",
      "SELECT bad",
      database = "db",
      token = "token",
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
      token = "token",
      verbose = FALSE
    ),
    "params must be NULL or a list"
  )
})

test_that("SQL helper defaults preserve ODBC and tibble behavior", {
  expect_identical(
    eval(formals(fabric_sql_connect)$backend),
    c("odbc", "adbc")
  )
  expect_identical(
    eval(formals(fabric_sql_query)$result),
    c("tibble", "arrow_stream")
  )
})

test_that("the Fabric integration manifest requires every SQL fixture", {
  manifest <- list(
    items = list(
      TestWarehouse = list(
        id = "warehouse-id",
        tables = list(types = "fabricqueryr_sql_types")
      )
    )
  )

  expect_identical(
    fabric_test_manifest_item(manifest, "TestWarehouse")$id,
    "warehouse-id"
  )
  expect_identical(
    fabric_test_manifest_item(
      manifest,
      "TestWarehouse"
    )$tables$types,
    "fabricqueryr_sql_types"
  )
  expect_error(
    fabric_test_manifest_item(manifest, "TestSQLDatabase"),
    "does not provision required item 'TestSQLDatabase'"
  )
})
