#' Get connection details for a Fabric SQL item
#'
#' Shows the server, database, port, and item type that fabricQueryR will use for
#' a Fabric SQL connection. Most users can pass a discovered item directly to
#' [fabric_sql_connect()] and do not need to call this helper
#'
#' @param server A Fabric SQL server name, a complete connection string copied
#'   from the Fabric portal, or one Lakehouse, Warehouse, Warehouse snapshot,
#'   or SQL Database record returned by a discovery function. A discovered
#'   record is usually simplest because it also supplies the database name
#' @param database Optional catalog/database. An explicit value overrides a
#'   database found in `server`. For a bare endpoint, supply the item database
#'   shown with its connection string in Fabric. If omitted, Warehouse and SQL
#'   analytics endpoints open Fabric's `master` context, which is useful for
#'   discovery but does not select the item's tables
#' @param target_type Kind of Fabric SQL item. Keep `"auto"` unless a custom
#'   hostname prevents fabricQueryR from identifying it
#' @param port Optional TCP port. An explicit value overrides a port in
#'   `server`; otherwise the standard SQL port, 1433, is used
#'
#' @return A `fabric_sql_connection_info` list with `server`, `database`,
#'   `port`, `target_type`, and `source` (whether the input was text or a
#'   discovery record). No connection is opened
#' @export
fabric_sql_connection_info <- function(
  server,
  database = NULL,
  target_type = c(
    "auto",
    "lakehouse",
    "warehouse",
    "sql_database",
    "sql_analytics_endpoint"
  ),
  port = NULL
) {
  # 1 Validate caller options ----------------------------------------------------------------------

  # Check caller options now so later code can rely on safe input

  target_type <- match.arg(target_type)
  if (!is.null(database)) {
    fabric_sql_scalar(database, "database")
  }

  if (!is.null(port)) {
    fabric_sql_port(port)
  }

  # 2 Resolve discovery or text input --------------------------------------------------------------

  # Discovery records can supply a server, database, and known Fabric item type

  record <- fabric_as_record(server)
  discovered_type <- NULL
  connection_string <- NULL

  if (!is.null(record)) {
    # Only SQL-capable discovery records may become connection targets
    discovered_type <- tolower(fabric_record_value(record, "type") %||% "")
    if (
      !discovered_type %in%
        c(
          "lakehouse",
          "warehouse",
          "warehousesnapshot",
          "sqldatabase"
        )
    ) {
      rlang::abort(
        paste0(
          "SQL connections require a discovered Lakehouse, Warehouse, ",
          "WarehouseSnapshot, or SQLDatabase item"
        ),
        class = "fabric_sql_target_error"
      )
    }

    # Prefer a complete connection string, then fall back to separate fields
    connection_string <- fabric_record_value(
      record,
      "sql_connection_string",
      "connectionString"
    )
    server_value <- connection_string %||%
      fabric_record_value(
        record,
        "sql_server",
        "serverFqdn"
      )
    database <- database %||%
      fabric_record_value(
        record,
        "sql_database",
        "databaseName"
      )

    # Fabric SQL workloads commonly use their item name as the database
    if (
      is.null(database) &&
        discovered_type %in% c("lakehouse", "warehouse", "warehousesnapshot")
    ) {
      database <- fabric_record_value(record, "displayName")
    }
  } else {
    server_value <- server
  }

  # Normalize copied connection strings and direct server names identically
  fabric_sql_scalar(server_value, "server")
  parsed <- fabric_parse_sql_connection_string(server_value)
  database <- database %||% parsed$database
  if (!is.null(database) && !nzchar(trimws(database))) {
    rlang::abort(
      "database must be one non-empty character value when supplied",
      class = c("fabric_sql_database_error", "fabric_sql_target_error")
    )
  }

  # 3 Infer connection details ---------------------------------------------------------------------

  # Infer connection details from the available context before applying defaults

  resolved_type <- target_type
  if (identical(target_type, "auto")) {
    resolved_type <- switch(
      discovered_type %||% "",
      lakehouse = "lakehouse",
      warehouse = "warehouse",
      warehousesnapshot = "warehouse",
      sqldatabase = "sql_database",
      fabric_infer_sql_target(parsed$server)
    )
  }
  resolved_port <- port %||% parsed$port %||% 1433L
  fabric_sql_port(resolved_port)

  # 4 Return normalized connection information -----------------------------------------------------

  # Return normalized connection information in the stable form expected by the caller

  structure(
    list(
      server = parsed$server,
      database = if (is.null(database)) NULL else trimws(database),
      port = as.integer(resolved_port),
      target_type = resolved_type,
      source = if (is.null(record)) "character" else "discovery"
    ),
    class = c("fabric_sql_connection_info", "list")
  )
}

#' Connect to a Microsoft Fabric SQL target
#'
#' Opens a DBI connection to a Fabric Warehouse, Warehouse snapshot, Lakehouse,
#' or SQL Database. Use the connection with familiar DBI functions such as
#' [DBI::dbListTables()] and [DBI::dbGetQuery()]
#'
#' @details
#' The easiest input is an item returned by [fabric_warehouses()],
#' [fabric_lakehouses()], or [fabric_sql_databases()]. You can also paste a SQL
#' connection string from Fabric. A Lakehouse's SQL endpoint is read-only; use
#' Spark or another OneLake writer to change Lakehouse data
#'
#' @section Choosing a backend:
#' `backend = "odbc"` is the default and works well for ordinary DBI use. It
#' requires Microsoft ODBC Driver 18 or newer. Use `backend = "adbc"` when you
#' want a native Arrow result path, typically for larger analytical results
#' ADBC requires the external `mssql` driver; install it separately with
#' `dbc install mssql`
#'
#' @section Connection and permissions:
#' Discovery records and complete portal connection strings normally include
#' the database. A bare server can omit `database` to open Fabric's `master`
#' context. Transient connection failures are retried automatically. The user
#' or application must have access through a workspace role or the item's
#' **Manage permissions** settings; SQL permissions may further restrict data
#'
#' @inheritParams fabric_sql_connection_info
#' @param backend Connection driver. Use `"odbc"` for ordinary DBI work or
#'   `"adbc"` for a native Arrow path after installing its `mssql` driver
#' @param tenant_id Microsoft Entra tenant ID. Defaults to
#'   `FABRICQUERYR_TENANT_ID`
#' @param client_id Microsoft Entra application/client ID. Defaults to
#'   `FABRICQUERYR_CLIENT_ID`, then the Azure CLI application ID
#' @param token Optional access token or token-provider function. Leave `NULL`
#'   to let fabricQueryR use its normal sign-in flow
#' @param auth_args Additional sign-in options passed to
#'   [AzureAuth::get_azure_token()]
#' @param odbc_driver ODBC driver name. ODBC Driver 18 for SQL Server is the
#'   default
#' @param adbc_driver ADBC driver name or shared-library path. The separately
#'   installed ADBC Driver Foundry `mssql` driver is the default
#' @param encrypt Whether the driver encrypts the connection. Keep the secure
#'   default, `"yes"`, for Fabric
#' @param trust_server_certificate Whether to accept a server certificate
#'   without validating its trust chain. Keep the secure default, `"no"`,
#'   unless diagnosing a controlled test environment
#' @param timeout Non-negative whole-number login/connect timeout in seconds;
#'   `0` lets the driver use an unlimited or driver-specific timeout
#' @param read_only Whether to ask the driver for a read-only connection. This
#'   is a connection hint, not a replacement for Fabric or SQL permissions
#' @param allow_custom_endpoint Logical. Fabric SQL and Microsoft SQL Database
#'   hostnames are trusted by default. Set to `TRUE` only when deliberately
#'   sending the SQL access token to another hostname, such as a controlled
#'   proxy or test server
#' @param max_tries Maximum attempts after temporary Fabric SQL failures
#' @param retry_delay Initial delay in seconds before retrying. Later retries
#'   wait progressively longer, up to 60 seconds
#' @param verbose Logical. Show authentication, retry, and connection progress
#' @param ... Additional arguments forwarded to [DBI::dbConnect()]. The former
#'   named `access_token` argument is consumed here as a deprecated alias for
#'   `token` and is not forwarded. For ODBC, a caller-supplied `attributes`
#'   named list is merged with the package-managed `azure_token`; that protected
#'   attribute cannot be overridden
#'
#' @return A live `DBIConnection`. Close it with [DBI::dbDisconnect()] when
#'   finished. For an ADBC connection with child results still registered,
#'   use `DBI::dbDisconnect(con, force = TRUE)` to release them immediately
#' @references
#' [Connect to a Fabric Warehouse or SQL analytics endpoint](https://learn.microsoft.com/en-us/fabric/data-warehouse/how-to-connect)
#'
#' [Microsoft Entra authentication in Fabric Data Warehouse](https://learn.microsoft.com/en-us/fabric/data-warehouse/entra-id-authentication)
#'
#' [Lakehouse SQL analytics endpoint](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-sql-analytics-endpoint)
#'
#' [Download Microsoft ODBC Driver 18 for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server)
#' @export
#'
#' @examples
#' \dontrun{
#' warehouse <- fabric_warehouses("Analytics")[[1]]
#' con <- fabric_sql_connect(warehouse)
#' DBI::dbGetQuery(con, "SELECT TOP 10 * FROM dbo.Customers")
#' DBI::dbDisconnect(con)
#'
#' # A connection string copied from Fabric also works
#' con <- fabric_sql_connect(paste0(
#'   "Server=tcp:example.datawarehouse.fabric.microsoft.com,1433;",
#'   "Initial Catalog=SalesWarehouse;"
#' ))
#' DBI::dbDisconnect(con)
#'
#' # After installing the external driver with `dbc install mssql`:
#' con <- fabric_sql_connect(warehouse, backend = "adbc")
#' DBI::dbDisconnect(con)
#' }
fabric_sql_connect <- function(
  server,
  database = NULL,
  target_type = c(
    "auto",
    "lakehouse",
    "warehouse",
    "sql_database",
    "sql_analytics_endpoint"
  ),
  backend = c("odbc", "adbc"),
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  token = NULL,
  auth_args = list(),
  odbc_driver = getOption(
    "fabricqueryr.sql.driver",
    "ODBC Driver 18 for SQL Server"
  ),
  adbc_driver = getOption("fabricqueryr.sql.adbc_driver", "mssql"),
  port = NULL,
  encrypt = "yes",
  trust_server_certificate = "no",
  timeout = 30L,
  read_only = FALSE,
  allow_custom_endpoint = FALSE,
  verbose = TRUE,
  max_tries = 3L,
  retry_delay = 5,
  ...
) {
  # 1 Validate connection options ------------------------------------------------------------------

  # Resolve compatibility arguments and backend requirements before signing in

  resolved <- fabric_resolve_token_alias(
    token = token,
    dots = list(...),
    caller = "fabric_sql_connect()"
  )
  token <- resolved$token
  target_type <- match.arg(target_type)
  backend <- match.arg(backend)
  if (!is.logical(read_only) || length(read_only) != 1L || is.na(read_only)) {
    rlang::abort("read_only must be TRUE or FALSE")
  }
  fabric_sql_timeout(timeout)
  fabric_sql_retry_settings(max_tries, retry_delay)
  fabric_sql_require_backend(backend)
  adbc_driver_object <- NULL
  if (identical(backend, "adbc")) {
    fabric_sql_scalar(adbc_driver, "adbc_driver")
    if ("uri" %in% names(resolved$dots)) {
      rlang::abort(
        "fabric_sql_connect() constructs the ADBC uri; uri cannot be supplied in ...",
        class = "fabric_sql_target_error"
      )
    }
    adbc_driver_object <- fabric_sql_load_adbc_driver(adbc_driver)
  }

  # 2 Resolve target and authentication ------------------------------------------------------------

  # Resolve target and authentication once so later steps use one consistent value

  info <- fabric_sql_connection_info(
    server = server,
    database = database,
    target_type = target_type,
    port = port
  )
  fabric_sql_validate_endpoint(info$server, allow_custom_endpoint)
  if (is.null(token)) {
    inform(
      verbose,
      "Authenticating with {.pkg AzureAuth} (MSAL v2) for SQL"
    )
  }
  credential <- fabric_credential(
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args
  )
  backend_label <- toupper(backend)
  message <- if (is.null(info$database)) {
    "Opening {backend_label} connection to {info$server} / Fabric master context"
  } else {
    "Opening {backend_label} connection to {info$server} / DB '{info$database}'"
  }
  inform(verbose, message)

  # 3 Build backend arguments ----------------------------------------------------------------------

  # ODBC and ADBC receive the same resolved target through their native option
  # shapes; access tokens are added fresh inside the retry loop

  odbc_options <- if (identical(backend, "odbc")) {
    fabric_sql_odbc_options(resolved$dots)
  } else {
    list(dots = resolved$dots, attributes = list())
  }
  odbc_args <- c(
    list(
      backend = backend,
      driver = odbc_driver,
      server = paste0("tcp:", info$server, ",", info$port),
      Encrypt = encrypt,
      TrustServerCertificate = trust_server_certificate,
      MARS_Connection = "no",
      timeout = as.integer(timeout)
    ),
    if (!is.null(info$database)) list(database = info$database) else list(),
    if (isTRUE(read_only)) list(ApplicationIntent = "ReadOnly") else list(),
    odbc_options$dots
  )

  # 4 Open the connection --------------------------------------------------------------------------

  # Acquire a fresh token on retries and stop immediately for non-transient
  # driver failures

  for (attempt in seq_len(as.integer(max_tries))) {
    # Authentication failures are reported separately from driver failures
    token_value <- tryCatch(
      fabric_get_token(
        credential,
        .fabric_audience$sql,
        force_refresh = attempt > 1L
      ),
      error = function(error) {
        rlang::abort(
          "Fabric SQL authentication failed while acquiring an access token",
          class = "fabric_sql_authentication_error",
          parent = error
        )
      }
    )

    # Build the argument shape expected by the selected database backend
    connect_args <- if (identical(backend, "odbc")) {
      c(
        odbc_args,
        list(
          attributes = c(
            odbc_options$attributes,
            list(azure_token = token_value)
          )
        )
      )
    } else {
      c(
        list(
          backend = backend,
          adbc_driver = adbc_driver_object,
          uri = fabric_sql_adbc_uri(
            info = info,
            token = token_value,
            encrypt = encrypt,
            trust_server_certificate = trust_server_certificate,
            timeout = timeout,
            read_only = read_only
          )
        ),
        resolved$dots
      )
    }

    connection <- tryCatch(
      do.call(.fabric_sql_db_connect, connect_args),
      error = function(error) error
    )

    if (!inherits(connection, "error")) {
      inform(verbose, "Connected", type = "success")
      return(connection)
    }

    # Stop immediately for permanent failures or after the final attempt
    if (
      attempt == as.integer(max_tries) ||
        !fabric_sql_transient_error(connection)
    ) {
      fabric_sql_connection_error(
        connection,
        secrets = if (identical(backend, "adbc")) token_value else NULL
      )
    }

    delay <- fabric_sql_retry_delay(attempt, retry_delay)
    inform(
      verbose,
      "Transient SQL connection failure; retrying in {delay} seconds"
    )
    .fabric_sql_sleep(delay)
  }
  rlang::abort("Fabric SQL connection retry loop ended unexpectedly")
}

#' Run a parameterized query against Microsoft Fabric SQL
#'
#' Runs one SQL query and returns its rows, opening and closing the connection
#' automatically. Use [fabric_sql_connect()] instead when several operations
#' should share a connection. Supply changing values through `params` rather
#' than pasting them into the SQL text
#'
#' @inheritParams fabric_sql_connect
#' @param sql One result-producing T-SQL `SELECT` statement, optionally beginning
#'   with a common-table-expression `WITH` clause. For DDL or DML, open a
#'   connection with [fabric_sql_connect()] and call [DBI::dbExecute()]. A
#'   Lakehouse SQL analytics endpoint is read-only and does not support
#'   `INSERT`, `UPDATE`, or `DELETE`
#' @param params Optional list of values for `?` placeholders in `sql`. Values
#'   are sent separately from the SQL text, which is safer and easier to quote
#'   correctly than building a query with `paste()`
#' @param result Return a `"tibble"` for ordinary R analysis, or a single-use
#'   `"arrow_stream"` when a large result should be processed without first
#'   collecting it into an R data frame. The native Arrow path uses the ADBC
#'   backend
#' @param idempotent Logical. Set to `TRUE` only if running the entire statement
#'   a second time has no unwanted effect (usually a plain `SELECT`). This
#'   permits a retry when it is unclear whether Fabric executed the first
#'   attempt
#'
#' @return With `result = "tibble"`, a tibble containing the returned rows and
#'   driver-converted column types. With `result = "arrow_stream"`, a single-use
#'   `nanoarrow_array_stream` that can be consumed by Arrow-compatible tools
#' @export
#'
#' @examples
#' \dontrun{
#' warehouse <- fabric_warehouses("Analytics")[[1]]
#' result <- fabric_sql_query(
#'   warehouse,
#'   sql = "SELECT * FROM dbo.Customers WHERE region = ?",
#'   params = list("West")
#' )
#'
#' stream <- fabric_sql_query(
#'   warehouse,
#'   "SELECT * FROM dbo.Customers",
#'   backend = "adbc",
#'   result = "arrow_stream"
#' )
#' reader <- arrow::as_record_batch_reader(stream)
#' table <- reader$read_table()
#' }
fabric_sql_query <- function(
  server,
  sql,
  params = NULL,
  result = c("tibble", "arrow_stream"),
  database = NULL,
  target_type = c(
    "auto",
    "lakehouse",
    "warehouse",
    "sql_database",
    "sql_analytics_endpoint"
  ),
  backend = c("odbc", "adbc"),
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  token = NULL,
  auth_args = list(),
  odbc_driver = getOption(
    "fabricqueryr.sql.driver",
    "ODBC Driver 18 for SQL Server"
  ),
  adbc_driver = getOption("fabricqueryr.sql.adbc_driver", "mssql"),
  port = NULL,
  encrypt = "yes",
  trust_server_certificate = "no",
  timeout = 30L,
  read_only = FALSE,
  allow_custom_endpoint = FALSE,
  verbose = TRUE,
  max_tries = 3L,
  retry_delay = 5,
  idempotent = FALSE,
  ...
) {
  # 1 Validate query options -----------------------------------------------------------------------

  # This convenience function accepts only one result-producing read statement;
  # callers can use DBI directly for broader SQL workflows

  resolved <- fabric_resolve_token_alias(
    token = token,
    dots = list(...),
    caller = "fabric_sql_query()"
  )
  token <- resolved$token
  result <- match.arg(result)
  backend <- match.arg(backend)
  fabric_sql_scalar(sql, "sql")
  fabric_sql_validate_query_statement(sql)
  if (!is.null(params) && !is.list(params)) {
    rlang::abort(
      "params must be NULL or a list",
      class = "fabric_sql_execution_error"
    )
  }

  if (
    !is.logical(idempotent) ||
      length(idempotent) != 1L ||
      is.na(idempotent)
  ) {
    rlang::abort("idempotent must be TRUE or FALSE")
  }
  fabric_sql_retry_settings(max_tries, retry_delay)
  fabric_sql_require_backend(backend, result = result)

  # 2 Prepare authentication and parameters --------------------------------------------------------

  # Prepare authentication and parameters once for reuse in the remaining work

  credential <- fabric_credential(
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args
  )
  adbc_params <- identical(backend, "adbc") && !is.null(params)
  query_sql <- if (adbc_params) {
    fabric_sql_adbc_parameter_sql(sql, params)
  } else {
    sql
  }
  query_params <- if (adbc_params) {
    stats::setNames(params, paste0("@p", seq_along(params)))
  } else {
    params
  }

  # 3 Build one-shot connection arguments ----------------------------------------------------------

  # Build one-shot connection arguments from the validated values required by the next step

  connect_args <- c(
    list(
      server = server,
      database = database,
      target_type = match.arg(target_type),
      backend = backend,
      tenant_id = tenant_id,
      client_id = client_id,
      token = credential,
      auth_args = list(),
      odbc_driver = odbc_driver,
      adbc_driver = adbc_driver,
      port = port,
      encrypt = encrypt,
      trust_server_certificate = trust_server_certificate,
      timeout = timeout,
      read_only = read_only,
      allow_custom_endpoint = allow_custom_endpoint,
      verbose = verbose,
      max_tries = 1L,
      retry_delay = retry_delay
    ),
    resolved$dots
  )

  # 4 Connect, execute, and close ------------------------------------------------------------------

  # Each retry gets a new connection. Query failures are retried only when the
  # caller explicitly confirms the statement is safe to repeat

  for (attempt in seq_len(as.integer(max_tries))) {
    # Refresh authentication after a failed attempt before opening a connection
    force_refresh <- attempt > 1L
    connect_args$token <- local({
      refresh_on_first_use <- force_refresh
      function(audience, force_refresh = FALSE) {
        fabric_get_token(
          credential,
          audience,
          force_refresh = isTRUE(force_refresh) || refresh_on_first_use
        )
      }
    })

    # Keep connection cleanup paired with the query attempt
    con <- NULL
    preserve_stream <- FALSE
    outcome <- tryCatch(
      {
        con <- do.call(fabric_sql_connect, connect_args)
        value <- .fabric_sql_db_get_query(
          con,
          query_sql,
          params = query_params,
          result = result
        )
        preserve_stream <- identical(result, "arrow_stream")
        list(
          value = value,
          error = NULL
        )
      },
      error = function(error) list(value = NULL, error = error),
      finally = {
        if (!is.null(con)) {
          try(
            .fabric_sql_db_disconnect(
              con,
              force = !preserve_stream
            ),
            silent = TRUE
          )
        }
      }
    )

    # Successful streams keep their connection; tabular results can close it
    if (is.null(outcome$error)) {
      if (identical(result, "arrow_stream")) {
        return(outcome$value)
      }

      return(tibble::as_tibble(outcome$value))
    }

    # Retry only transient failures that are safe to repeat
    connection_failure <- inherits(
      outcome$error,
      "fabric_sql_connection_error"
    )
    retryable <- fabric_sql_transient_error(outcome$error) &&
      (connection_failure || isTRUE(idempotent))
    if (attempt == as.integer(max_tries) || !retryable) {
      if (connection_failure) {
        rlang::cnd_signal(outcome$error)
      }
      rlang::abort(
        "Fabric SQL query execution failed",
        class = "fabric_sql_execution_error",
        parent = outcome$error
      )
    }

    delay <- fabric_sql_retry_delay(attempt, retry_delay)
    inform(
      verbose,
      "Transient SQL query failure; retrying on a new connection in {delay} seconds"
    )
    .fabric_sql_sleep(delay)
  }
  rlang::abort("Fabric SQL query retry loop ended unexpectedly")
}

# Validate `sql` as one top-level SELECT or WITH...SELECT statement. Returns
# invisibly before the one-shot query helper opens a connection
fabric_sql_validate_query_statement <- function(sql) {
  tokens <- fabric_sql_top_level_tokens(sql)
  terminators <- which(tokens == ";")
  if (length(terminators)) {
    valid_terminator <- length(terminators) == 1L &&
      identical(terminators, length(tokens))
    if (!valid_terminator) {
      fabric_sql_statement_error()
    }
    tokens <- tokens[-length(tokens)]
  }
  first <- if (length(tokens)) tokens[[1L]] else ""
  select <- match("SELECT", tokens, nomatch = 0L)
  valid_start <- identical(first, "SELECT") ||
    (identical(first, "WITH") && select > 0L)
  select_tokens <- if (select > 0L) {
    tokens[seq.int(select, length(tokens))]
  } else {
    character()
  }
  # Semicolons are optional for most T-SQL statements, so checking only for a
  # second terminator does not establish that this is one statement. Reject
  # top-level tokens that can begin a second statement or turn the batch into a
  # state-changing/control-flow operation. Quoted identifiers, comments, string
  # literals, and tokens inside parentheses have already been excluded by the
  # tokenizer.
  non_query_tokens <- c(
    "ALTER", "BACKUP", "BEGIN", "BREAK", "BULK", "CHECKPOINT", "CLOSE",
    "COMMIT", "CREATE", "DBCC", "DEALLOCATE", "DECLARE", "DELETE", "DENY",
    "DISABLE", "DROP", "ENABLE", "EXEC", "EXECUTE", "GRANT", "INSERT",
    "KILL", "MERGE", "OPEN", "PRINT", "RAISERROR", "RECONFIGURE",
    "RESTORE", "RETURN", "REVERT", "ROLLBACK", "SAVE", "SET", "SHUTDOWN",
    "THROW", "TRANSACTION", "TRUNCATE", "UPDATE", "USE", "WAITFOR", "WHILE"
  )
  write_capable <- any(c("INTO", non_query_tokens) %in% select_tokens)
  valid <- valid_start && !write_capable
  if (!valid) {
    fabric_sql_statement_error()
  }
  invisible(sql)
}

# Raise the shared one-shot SQL statement error. This function does not return
# and keeps invalid-statement guidance consistent across parser branches
fabric_sql_statement_error <- function() {
  rlang::abort(
    paste0(
      "fabric_sql_query() accepts only result-producing SELECT statements, ",
      "with exactly one statement per call. ",
      "Use DBI::dbExecute() with fabric_sql_connect() for DDL or DML"
    ),
    class = "fabric_sql_statement_error"
  )
}

# Tokenize top-level SQL keywords and semicolons while ignoring quoted text and
# comments. Returns tokens used to enforce the one-shot read-only shape
fabric_sql_top_level_tokens <- function(sql) {
  # 1 Prepare tokenizer state ----------------------------------------------------------------------

  # Scan character-by-character so quoted strings and comments cannot masquerade
  # as executable top-level keywords

  chars <- strsplit(sql, "", fixed = TRUE)[[1L]]
  tokens <- character()
  token <- character()
  depth <- 0L
  quote <- NULL
  index <- 1L
  # Store the current top-level token, if any, then reset its character buffer
  flush <- function() {
    if (length(token) && depth == 0L) {
      tokens <<- c(tokens, toupper(paste0(token, collapse = "")))
    }
    token <<- character()
  }

  # 2 Read top-level tokens ------------------------------------------------------------------------

  # Read top-level tokens once so later checks use a consistent view

  while (index <= length(chars)) {
    char <- chars[[index]]
    next_char <- if (index < length(chars)) chars[[index + 1L]] else ""

    # Quoted text is skipped because keywords inside it are not SQL structure
    if (!is.null(quote)) {
      if (identical(char, quote)) {
        if (identical(next_char, quote)) {
          index <- index + 2L
          next
        }
        quote <- NULL
      }
      index <- index + 1L
      next
    }

    # Line comments end at the next newline
    if (identical(char, "-") && identical(next_char, "-")) {
      flush()
      newline <- which(
        chars[seq.int(index + 2L, length(chars))] %in% c("\r", "\n")
      )
      index <- if (length(newline)) {
        index + 1L + newline[[1L]]
      } else {
        length(chars) + 1L
      }
      next
    }

    # Block comments are skipped as one unit and must have a closing marker
    if (identical(char, "/") && identical(next_char, "*")) {
      flush()
      remainder <- paste0(
        chars[seq.int(index + 2L, length(chars))],
        collapse = ""
      )
      close <- regexpr("*/", remainder, fixed = TRUE)[[1L]]
      if (close < 0L) {
        rlang::abort("sql contains an unterminated block comment")
      }
      index <- index + close + 3L
      next
    }

    # Opening quote characters switch the tokenizer into quoted-text mode
    if (char %in% c("'", '"', "[")) {
      flush()
      quote <- if (identical(char, "[")) "]" else char
      index <- index + 1L
      next
    }

    # Parentheses track nesting so only top-level semicolons split statements
    if (identical(char, "(")) {
      flush()
      depth <- depth + 1L
    } else if (identical(char, ")")) {
      flush()
      depth <- max(0L, depth - 1L)
    } else if (grepl("[A-Za-z_]", char)) {
      token <- c(token, char)
    } else {
      flush()
      if (identical(char, ";") && depth == 0L) {
        tokens <- c(tokens, ";")
      }
    }
    index <- index + 1L
  }

  # 3 Return the final token sequence --------------------------------------------------------------

  # Return the final token sequence in the stable form expected by the caller

  flush()
  tokens
}

# Check that packages required by `backend` and `result` are installed. Returns
# invisibly before connection or query work begins
fabric_sql_require_backend <- function(
  backend = c("odbc", "adbc"),
  result = NULL
) {
  backend <- match.arg(backend)
  packages <- if (identical(backend, "odbc")) {
    c("DBI", "odbc")
  } else {
    c("DBI", "adbi", "adbcdrivermanager")
  }

  if (identical(result, "arrow_stream")) {
    packages <- c(packages, "nanoarrow")
  }
  rlang::check_installed(
    unique(packages),
    reason = sprintf("to use the Fabric SQL %s backend", backend)
  )
  invisible(TRUE)
}

# Load the configured `adbc_driver` name or path. Returns a driver object passed
# to the isolated DBI connection seam
fabric_sql_load_adbc_driver <- function(adbc_driver) {
  tryCatch(
    adbcdrivermanager::adbc_driver(adbc_driver),
    error = function(error) {
      install_guidance <- if (grepl("^[A-Za-z0-9._-]+$", adbc_driver)) {
        sprintf("Install it with `dbc install %s`, then retry.", adbc_driver)
      } else {
        paste0(
          "Verify the shared-library path, or install a registered driver ",
          "with `dbc install <driver>`."
        )
      }
      rlang::abort(
        paste(
          sprintf("Could not load ADBC driver '%s'.", adbc_driver),
          install_guidance,
          paste0(
            "The adbcdrivermanager R package loads installed driver ",
            "manifests but does not install external driver binaries."
          )
        ),
        class = c(
          "fabric_sql_driver_error",
          "fabric_sql_connection_error"
        ),
        parent = error
      )
    }
  )
}

# Build an ADBC SQL Server URI from resolved `info` and connection settings
# Returns URL text containing the short-lived token for immediate driver use
fabric_sql_adbc_uri <- function(
  info,
  token,
  encrypt,
  trust_server_certificate,
  timeout,
  read_only
) {
  parsed <- httr2::url_parse(
    sprintf("sqlserver://%s:%d", info$server, info$port)
  )
  parsed$query <- c(
    if (!is.null(info$database)) list(database = info$database) else list(),
    list(
      fedauth = "ActiveDirectoryServicePrincipalAccessToken",
      password = token,
      encrypt = fabric_sql_adbc_encrypt(encrypt),
      TrustServerCertificate = fabric_sql_adbc_boolean(
        trust_server_certificate,
        "trust_server_certificate"
      ),
      `connection timeout` = as.character(as.integer(timeout)),
      `app name` = "fabricQueryR"
    ),
    if (isTRUE(read_only)) list(ApplicationIntent = "ReadOnly") else list()
  )
  httr2::url_build(parsed)
}

# Normalize a logical or yes/no `value` for an ADBC URI. Returns `true` or
# `false` text and names invalid input with `argument`
fabric_sql_adbc_boolean <- function(value, argument) {
  fabric_sql_scalar(value, argument)
  normalized <- tolower(trimws(value))
  if (normalized %in% c("true", "yes", "1", "t")) {
    return("true")
  }

  if (normalized %in% c("false", "no", "0", "f")) {
    return("false")
  }
  rlang::abort(
    sprintf(
      "%s must be a true/false or yes/no value for the ADBC backend",
      argument
    ),
    class = "fabric_sql_target_error"
  )
}

# Normalize the SQL encryption `value` for ADBC. Returns a valid driver spelling
# while preserving Fabric's secure default
fabric_sql_adbc_encrypt <- function(value) {
  fabric_sql_scalar(value, "encrypt")
  normalized <- tolower(trimws(value))
  if (normalized %in% c("strict", "mandatory", "disable", "optional")) {
    return(normalized)
  }
  fabric_sql_adbc_boolean(value, "encrypt")
}

# Replace top-level positional placeholders in `sql` with named ADBC parameters
# Returns rewritten SQL while leaving quoted text and comments untouched
fabric_sql_adbc_parameter_sql <- function(sql, params) {
  # 1 Prepare placeholder parsing ------------------------------------------------------------------

  # Only question marks in executable SQL should become named parameters

  chars <- strsplit(sql, "", fixed = TRUE)[[1L]]
  output <- character()
  state <- "normal"
  block_depth <- 0L
  marker <- 0L
  position <- 1L
  total <- length(chars)

  # Append supplied characters to rewritten SQL; updates the local output buffer
  append_chars <- function(...) {
    output <<- c(output, ...)
  }
  # Return the character after the current position, or empty text at the end
  next_char <- function() {
    if (position < total) chars[[position + 1L]] else ""
  }

  # 2 Rewrite executable placeholders --------------------------------------------------------------

  # Rewrite executable placeholders only after its structure has been validated

  while (position <= total) {
    current <- chars[[position]]
    following <- next_char()

    if (identical(state, "normal")) {
      if (identical(current, "'")) {
        state <- "single_quote"
      } else if (identical(current, "\"")) {
        state <- "double_quote"
      } else if (identical(current, "[")) {
        state <- "bracket"
      } else if (identical(current, "-") && identical(following, "-")) {
        append_chars(current, following)
        position <- position + 2L
        state <- "line_comment"
        next
      } else if (identical(current, "/") && identical(following, "*")) {
        append_chars(current, following)
        position <- position + 2L
        state <- "block_comment"
        block_depth <- 1L
        next
      } else if (identical(current, "?")) {
        marker <- marker + 1L
        append_chars(paste0("@p", marker))
        position <- position + 1L
        next
      }
      append_chars(current)
      position <- position + 1L
      next
    }

    if (identical(state, "single_quote")) {
      append_chars(current)
      if (identical(current, "'")) {
        if (identical(following, "'")) {
          append_chars(following)
          position <- position + 2L
          next
        }
        state <- "normal"
      }
      position <- position + 1L
      next
    }

    if (identical(state, "double_quote")) {
      append_chars(current)
      if (identical(current, "\"")) {
        if (identical(following, "\"")) {
          append_chars(following)
          position <- position + 2L
          next
        }
        state <- "normal"
      }
      position <- position + 1L
      next
    }

    if (identical(state, "bracket")) {
      append_chars(current)
      if (identical(current, "]")) {
        if (identical(following, "]")) {
          append_chars(following)
          position <- position + 2L
          next
        }
        state <- "normal"
      }
      position <- position + 1L
      next
    }

    if (identical(state, "line_comment")) {
      append_chars(current)
      if (current %in% c("\r", "\n")) {
        state <- "normal"
      }
      position <- position + 1L
      next
    }

    append_chars(current)
    if (identical(current, "/") && identical(following, "*")) {
      append_chars(following)
      position <- position + 2L
      block_depth <- block_depth + 1L
      next
    }

    if (identical(current, "*") && identical(following, "/")) {
      append_chars(following)
      position <- position + 2L
      block_depth <- block_depth - 1L
      if (block_depth == 0L) {
        state <- "normal"
      }
      next
    }
    position <- position + 1L
  }

  # 3 Validate and return rewritten SQL ------------------------------------------------------------

  # Check and return rewritten SQL now so later code can rely on safe input

  if (marker != length(params)) {
    rlang::abort(
      sprintf(
        "ADBC parameter binding found %d SQL placeholder%s for %d value%s",
        marker,
        if (marker == 1L) "" else "s",
        length(params),
        if (length(params) == 1L) "" else "s"
      ),
      class = "fabric_sql_execution_error"
    )
  }
  paste0(output, collapse = "")
}

# Validate connection/query retry settings. Returns invisibly before either
# retry loop starts
fabric_sql_retry_settings <- function(max_tries, retry_delay) {
  if (
    length(max_tries) != 1L ||
      is.na(max_tries) ||
      !is.numeric(max_tries) ||
      !is.finite(max_tries) ||
      max_tries < 1 ||
      max_tries != floor(max_tries) ||
      max_tries > .Machine$integer.max
  ) {
    rlang::abort("max_tries must be one positive integer")
  }

  if (
    length(retry_delay) != 1L ||
      is.na(retry_delay) ||
      !is.numeric(retry_delay) ||
      !is.finite(retry_delay) ||
      retry_delay < 0
  ) {
    rlang::abort("retry_delay must be one non-negative number")
  }
  invisible(TRUE)
}

# Validate SQL `server` and its trust boundary. Returns invisibly before the SQL
# access token can be passed to a driver
fabric_sql_validate_endpoint <- function(server, allow_custom_endpoint) {
  if (
    !is.logical(allow_custom_endpoint) ||
      length(allow_custom_endpoint) != 1L ||
      is.na(allow_custom_endpoint)
  ) {
    rlang::abort("allow_custom_endpoint must be TRUE or FALSE")
  }
  host <- tolower(sub("\\.$", "", trimws(server)))
  trusted_suffixes <- c(
    ".datawarehouse.fabric.microsoft.com",
    ".datawarehouse.pbidedicated.microsoft.com",
    ".pbidedicated.microsoft.com",
    ".pbidedicated.windows.net",
    ".database.fabric.microsoft.com",
    ".database.windows.net"
  )
  trusted <- any(vapply(
    trusted_suffixes,
    function(suffix) {
      endsWith(host, suffix) && nchar(host) > nchar(suffix)
    },
    logical(1)
  ))
  if (!trusted && !isTRUE(allow_custom_endpoint)) {
    rlang::abort(
      paste0(
        "Refusing to send a SQL access token to untrusted server '",
        server,
        "'; set allow_custom_endpoint = TRUE only for an endpoint you trust"
      ),
      class = c("fabric_sql_endpoint_error", "fabric_sql_target_error")
    )
  }
  invisible(server)
}

# Separate caller ODBC connection arguments and attributes from `dots`. Returns
# both lists while protecting the package-managed Azure token attribute
fabric_sql_odbc_options <- function(dots) {
  positions <- which(names(dots) == "attributes")
  if (length(positions) > 1L) {
    rlang::abort("attributes may be supplied only once in ...")
  }
  attributes <- if (length(positions)) dots[[positions]] else list()
  if (length(positions)) {
    dots <- dots[-positions]
  }

  if (!is.list(attributes)) {
    rlang::abort("attributes in ... must be a named list")
  }
  attribute_names <- names(attributes)
  if (
    length(attributes) &&
      (is.null(attribute_names) || !all(nzchar(attribute_names)))
  ) {
    rlang::abort("attributes in ... must be a named list")
  }
  normalized <- tolower(attribute_names %||% character())
  if (anyDuplicated(normalized)) {
    rlang::abort("attributes in ... must have unique names ignoring case")
  }

  if ("azure_token" %in% normalized) {
    rlang::abort(
      "attributes in ... cannot override the package-managed azure_token"
    )
  }
  list(dots = dots, attributes = attributes)
}

# Calculate jittered backoff for `attempt` from `retry_delay`. Returns seconds
# used by both SQL retry loops
fabric_sql_retry_delay <- function(attempt, retry_delay) {
  base <- min(60, retry_delay * 2^(attempt - 1L))
  base * .fabric_sql_runif(1L, 0.8, 1.2)
}

# Detect whether `error` looks temporary from class, SQL state, or message
# Returns one logical value used to decide whether reconnecting is safe
fabric_sql_transient_error <- function(error) {
  messages <- character()
  current <- error
  while (inherits(current, "condition")) {
    messages <- c(messages, conditionMessage(current))
    current <- current$parent %||% NULL
  }
  grepl(
    paste0(
      "(?i)(",
      "\\b(?:24804|6005|6008|40197|40501|40613|49918|49919|49920|",
      "10053|10054|10060|11001)\\b|",
      "temporar(?:y|ily)|timed?\\s*out|timeout|",
      "transport-level|communication link failure|",
      "connection.{0,30}(?:reset|closed|broken|forcibly)|",
      "network-related|service unavailable|server is not currently available|",
      "shutdown is in progress|system update",
      ")"
    ),
    paste(messages, collapse = "\n"),
    perl = TRUE
  )
}

# Parse a SQL `server` or full connection string. Returns normalized server,
# database, port, and original fields for connection resolution
fabric_parse_sql_connection_string <- function(server) {
  # 1 Split connection-string fields ---------------------------------------------------------------

  # Split the copied string first so each key/value pair can be checked separately

  value <- trimws(server)
  tokens <- trimws(fabric_split_connection_string(value))
  tokens <- tokens[nzchar(tokens)]
  pairs <- tokens[grepl("=", tokens, fixed = TRUE)]
  fields <- list()
  if (length(pairs)) {
    for (pair in pairs) {
      position <- regexpr("=", pair, fixed = TRUE)[[1L]]
      key <- tolower(trimws(substr(pair, 1L, position - 1L)))
      key <- gsub("[ _]", "", key)
      fields[[key]] <- fabric_unquote_connection_value(
        substr(pair, position + 1L, nchar(pair))
      )
    }
  }

  # 2 Resolve server and port ----------------------------------------------------------------------

  # Resolve server and port once so later steps use one consistent value

  host <- fields$server %||%
    fields$datasource %||%
    fields$address %||%
    fields$addr %||%
    fields$networkaddress
  if (is.null(host)) {
    bare <- tokens[!grepl("=", tokens, fixed = TRUE)]
    if (length(bare) != 1L) {
      rlang::abort(
        "Could not find a unique Server/Data Source in the SQL target",
        class = "fabric_sql_target_error"
      )
    }
    host <- fabric_unquote_connection_value(bare[[1L]])
  }
  host <- sub("(?i)^tcp:\\s*", "", trimws(host), perl = TRUE)
  port <- NULL
  match <- regexec("^(.+?)[,](\\d+)$", host)
  parts <- regmatches(host, match)[[1L]]
  if (length(parts)) {
    host <- trimws(parts[[2L]])
    port <- as.integer(parts[[3L]])
  }

  if (!nzchar(host)) {
    rlang::abort(
      "Fabric SQL server is empty",
      class = "fabric_sql_target_error"
    )
  }

  # 3 Return parsed connection details -------------------------------------------------------------

  # Return parsed connection details in the stable form expected by the caller

  database <- fields$initialcatalog %||%
    fields$database %||%
    fields$catalog
  list(server = host, database = database, port = port, fields = fields)
}

# Infer a Fabric SQL target kind from `server`. Returns a target-type string used
# when the caller selects `target_type = "auto"`
fabric_infer_sql_target <- function(server) {
  if (
    grepl(
      paste0(
        "(?:\\.datawarehouse\\.fabric\\.microsoft\\.com|",
        "\\.datawarehouse\\.pbidedicated\\.microsoft\\.com|",
        "\\.pbidedicated\\.microsoft\\.com|",
        "\\.pbidedicated\\.windows\\.net)$"
      ),
      server,
      ignore.case = TRUE
    )
  ) {
    "sql_analytics_endpoint"
  } else if (
    grepl(
      "(?:\\.database\\.fabric\\.microsoft\\.com|\\.database\\.windows\\.net)$",
      server,
      ignore.case = TRUE
    )
  ) {
    "sql_database"
  } else {
    "auto"
  }
}

# Check `value` as one non-empty string identified by `argument`. Returns
# invisibly for shared SQL option validation
fabric_sql_scalar <- function(value, argument) {
  if (
    !is.character(value) ||
      length(value) != 1L ||
      is.na(value) ||
      !nzchar(trimws(value))
  ) {
    rlang::abort(
      sprintf("%s must be one non-empty character value", argument),
      class = "fabric_sql_target_error"
    )
  }
  invisible(value)
}

# Validate a numeric SQL `value` as a port within the supplied bounds. Returns
# invisibly for public connection resolution
fabric_sql_port <- function(
  value,
  argument = "port",
  allow_zero = FALSE
) {
  minimum <- if (allow_zero) 0 else 1
  if (
    length(value) != 1L ||
      is.na(value) ||
      !is.numeric(value) ||
      value < minimum ||
      value > 65535 ||
      value != floor(value)
  ) {
    rlang::abort(
      sprintf(
        "%s must be one integer between %d and 65535",
        argument,
        minimum
      ),
      class = "fabric_sql_target_error"
    )
  }
  invisible(value)
}

# Validate SQL timeout `value` as a non-negative whole number. Returns invisibly
# before a driver connection is attempted
fabric_sql_timeout <- function(value) {
  if (
    length(value) != 1L ||
      is.na(value) ||
      !is.numeric(value) ||
      !is.finite(value) ||
      value < 0 ||
      value != floor(value) ||
      value > .Machine$integer.max
  ) {
    rlang::abort(
      "timeout must be one non-negative whole number",
      class = "fabric_sql_target_error"
    )
  }
  invisible(value)
}

# Redact `error` and raise a typed SQL connection condition. This function does
# not return and optionally removes short-lived `secrets` from driver messages
fabric_sql_connection_error <- function(error, secrets = NULL) {
  message <- fabric_sql_redact_secrets(conditionMessage(error), secrets)
  class <- if (
    grepl(
      "token|authentication|login failed|18456",
      message,
      ignore.case = TRUE
    )
  ) {
    "fabric_sql_authentication_error"
  } else if (
    grepl(
      "cannot open database|catalog|database .* (?:not|doesn't) exist",
      message,
      ignore.case = TRUE
    )
  ) {
    "fabric_sql_database_error"
  } else {
    "fabric_sql_endpoint_error"
  }
  rlang::abort(
    paste0("Fabric SQL connection failed: ", message),
    class = c(class, "fabric_sql_connection_error"),
    # Driver conditions can retain call arguments and backend-specific state.
    # Keep only the already-redacted message in the public condition chain.
    parent = simpleError(message)
  )
}

# Remove explicit `secrets` and shared credential patterns from `message`
# Returns safe text suitable for a public SQL error
fabric_sql_redact_secrets <- function(message, secrets = NULL) {
  secrets <- unique(secrets[!is.na(secrets) & nzchar(secrets)])
  for (secret in secrets) {
    message <- gsub(secret, "<redacted>", message, fixed = TRUE)
    encoded <- utils::URLencode(secret, reserved = TRUE)
    message <- gsub(encoded, "<redacted>", message, fixed = TRUE)
  }
  message <- gsub(
    "(?i)(password=)[^&;[:space:]]+",
    "\\1<redacted>",
    message,
    perl = TRUE
  )
  .httr2_redact(message)
}

# Open ODBC or ADBC from normalized arguments. Returns a DBI connection and is a
# test seam that keeps driver setup out of public retry logic
.fabric_sql_db_connect <- function(
  backend = c("odbc", "adbc"),
  adbc_driver = NULL,
  ...
) {
  backend <- match.arg(backend)
  if (identical(backend, "odbc")) {
    return(DBI::dbConnect(odbc::odbc(), ...))
  }
  driver <- if (inherits(adbc_driver, "adbc_driver")) {
    adbc_driver
  } else {
    adbcdrivermanager::adbc_driver(adbc_driver)
  }
  DBI::dbConnect(adbi::adbi(driver), ...)
}

# Execute one query through `con` and return the requested result shape. This
# test seam handles DBI binding details for ODBC and ADBC
.fabric_sql_db_get_query <- function(
  con,
  sql,
  params = NULL,
  result = c("tibble", "arrow_stream")
) {
  result <- match.arg(result)
  if (!is.null(params) && inherits(con, "AdbiConnection")) {
    query_result <- .fabric_sql_db_send_query(con, sql, result)
    on.exit(.fabric_sql_db_clear_result(query_result), add = TRUE)
    .fabric_sql_db_bind(query_result, params)
    return(.fabric_sql_db_fetch(query_result, result))
  }

  if (identical(result, "arrow_stream")) {
    return(DBI::dbGetQueryArrow(con, sql, params = params))
  }
  DBI::dbGetQuery(con, sql, params = params)
}

# Send `sql` through `con` for the selected `result`. Returns a DBI result object
# and remains isolated for driver-compatibility tests
.fabric_sql_db_send_query <- function(con, sql, result) {
  if (identical(result, "arrow_stream")) {
    return(DBI::dbSendQueryArrow(con, sql, immediate = FALSE))
  }
  DBI::dbSendQuery(con, sql, immediate = FALSE)
}

# Bind named `params` to a DBI `result`. Returns the DBI binding result and keeps
# ADBC parameter behavior behind a test seam
.fabric_sql_db_bind <- function(result, params) {
  if (
    inherits(result, "AdbiResult") &&
      is.list(params) &&
      !inherits(params, "data.frame")
  ) {
    params <- .fabric_sql_adbc_bind_frame(params)
  }
  DBI::dbBind(result, params)
}

# Convert named scalar `params` into a one-row data frame. Returns the binding
# shape required by the ADBC DBI backend
.fabric_sql_adbc_bind_frame <- function(params) {
  # adbi converts lists with syntactic name repair, changing @p1 to X.p1
  # Supplying a data frame with exact names keeps it aligned with the driver
  parameter_names <- names(params)
  frame <- as.data.frame(
    lapply(params, I),
    fix.empty.names = FALSE,
    check.names = FALSE
  )
  names(frame) <- parameter_names
  frame
}

# Fetch `result` as a data frame or Arrow stream according to `output`. Returns
# the driver result used by `fabric_sql_query()`
.fabric_sql_db_fetch <- function(result, output) {
  if (identical(output, "arrow_stream")) {
    return(DBI::dbFetchArrow(result))
  }
  DBI::dbFetch(result)
}

# Clear a DBI `result`. Returns the DBI cleanup value and remains a test seam for
# guaranteed one-shot query cleanup
.fabric_sql_db_clear_result <- function(result) {
  DBI::dbClearResult(result)
}

# Disconnect DBI `con`, optionally forcing ADBC child cleanup. Returns the DBI
# cleanup value and remains a test seam for stream ownership behavior
.fabric_sql_db_disconnect <- function(con, force = FALSE) {
  if (inherits(con, "AdbiConnection")) {
    return(DBI::dbDisconnect(con, force = force))
  }
  DBI::dbDisconnect(con)
}

# Sleep for retry `delay`. Returns invisibly through base R and remains a test
# seam so SQL retry tests do not actually wait
.fabric_sql_sleep <- function(delay) {
  Sys.sleep(delay)
}

# Draw retry jitter between `min` and `max`. Returns numeric values and remains a
# test seam for deterministic retry-delay tests
.fabric_sql_runif <- function(n, min, max) {
  stats::runif(n, min, max)
}
