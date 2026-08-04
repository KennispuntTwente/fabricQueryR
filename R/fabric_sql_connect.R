#' Parse a Microsoft Fabric SQL target
#'
#' Converts the different ways Fabric identifies a SQL endpoint into one
#' consistent set of connection values. Most users can pass a discovered item
#' directly to [fabric_sql_connect()] and do not need to call this function.
#'
#' @param server A Fabric SQL server name, a complete connection string copied
#'   from the Fabric portal, or one Lakehouse, Warehouse, or SQL Database record
#'   returned by a discovery function. A discovered record is usually simplest
#'   because it also supplies the database name.
#' @param database Optional catalog/database. An explicit value overrides a
#'   database found in `server`. For a bare endpoint, supply the item database
#'   shown with its connection string in Fabric. If omitted, Warehouse and SQL
#'   analytics endpoints open Fabric's `master` context, which is useful for
#'   discovery but does not select the item's tables.
#' @param target_type Label for the endpoint kind. Keep `"auto"` unless the
#'   hostname is custom or ambiguous. The explicit choices distinguish a
#'   Lakehouse SQL analytics endpoint, Warehouse, transactional SQL Database,
#'   or another read-only SQL analytics endpoint; they do not convert one kind
#'   of endpoint into another.
#' @param port Optional TCP port. An explicit value overrides a port in
#'   `server`; otherwise the standard SQL port, 1433, is used.
#'
#' @return A `fabric_sql_connection_info` list with `server`, `database`,
#'   `port`, `target_type`, and `source` (whether the input was text or a
#'   discovery record). No connection is opened.
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
  target_type <- match.arg(target_type)
  if (!is.null(database)) {
    fabric_sql_scalar(database, "database")
  }
  if (!is.null(port)) {
    fabric_sql_port(port)
  }

  record <- fabric_as_record(server)
  discovered_type <- NULL
  connection_string <- NULL
  if (!is.null(record)) {
    discovered_type <- tolower(fabric_record_value(record, "type") %||% "")
    if (!discovered_type %in% c("lakehouse", "warehouse", "sqldatabase")) {
      rlang::abort(
        paste0(
          "SQL connections require a discovered Lakehouse, Warehouse, or ",
          "SQLDatabase item"
        ),
        class = "fabric_sql_target_error"
      )
    }
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
    if (is.null(database) && discovered_type %in% c("lakehouse", "warehouse")) {
      database <- fabric_record_value(record, "displayName")
    }
  } else {
    server_value <- server
  }

  fabric_sql_scalar(server_value, "server")
  parsed <- fabric_parse_sql_connection_string(server_value)
  database <- database %||% parsed$database
  if (!is.null(database) && !nzchar(trimws(database))) {
    rlang::abort(
      "database must be one non-empty character value when supplied",
      class = c("fabric_sql_database_error", "fabric_sql_target_error")
    )
  }

  resolved_type <- target_type
  if (identical(target_type, "auto")) {
    resolved_type <- switch(
      discovered_type %||% "",
      lakehouse = "lakehouse",
      warehouse = "warehouse",
      sqldatabase = "sql_database",
      fabric_infer_sql_target(parsed$server)
    )
  }
  resolved_port <- port %||% parsed$port %||% 1433L
  fabric_sql_port(resolved_port)

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
#' Opens a standard R DBI connection to a Fabric Warehouse, Lakehouse SQL
#' analytics endpoint, or SQL Database using Microsoft Entra authentication.
#' Use the returned connection with familiar DBI functions such as
#' [DBI::dbListTables()] and [DBI::dbGetQuery()].
#'
#' @details
#' In Fabric, copy a SQL connection string from the Warehouse, SQL analytics
#' endpoint, or SQL Database settings. Alternatively, use a row returned by
#' [fabric_warehouses()], [fabric_lakehouses()], or
#' [fabric_sql_databases()]. A Lakehouse's SQL analytics endpoint is read-only;
#' use Spark or another OneLake writer when data must be changed.
#'
#' `"odbc"` is the most familiar choice for DBI workflows and requires
#' Microsoft ODBC Driver 18 or newer. `"adbc"` is useful when a query result
#' should remain in Arrow form, especially for larger analytical results. It
#' uses [adbi::adbi()] with the `mssql` driver loaded by
#' [adbcdrivermanager::adbc_driver()]. Install that external driver separately
#' with `dbc install mssql`. The package checks that the driver can be loaded
#' before authenticating or opening a network connection. `adbcdrivermanager`
#' discovers and loads installed drivers; it does not install driver binaries.
#' Multiple Active Result Sets (MARS) is disabled because Fabric Warehouse does
#' not support it.
#'
#' Complete portal connection strings and enriched discovery records provide a
#' catalog automatically. Bare endpoints may omit `database` to use Fabric's
#' `master` context; the package never guesses a catalog name.
#'
#' Transient Fabric connection failures are retried on fresh connections with
#' refreshed tokens and bounded exponential backoff.
#'
#' The SQL audience is `https://database.windows.net/.default`. In Fabric, give
#' the user or application access through a workspace role or the item's
#' **Manage permissions** dialog. SQL `GRANT`/`DENY` rules can further restrict
#' what the identity may query.
#'
#' @inheritParams fabric_sql_connection_info
#' @param backend SQL client backend. Use `"odbc"` for broad DBI compatibility
#'   and the easiest setup; use `"adbc"` for its native Arrow result path after
#'   separately installing the ADBC `mssql` driver.
#' @param tenant_id Microsoft Entra tenant ID. Defaults to
#'   `FABRICQUERYR_TENANT_ID`.
#' @param client_id Microsoft Entra application/client ID. Defaults to
#'   `FABRICQUERYR_CLIENT_ID`, then the Azure CLI application ID.
#' @param token Optional `AzureAuth::AzureToken`, bearer-token string, or
#'   token-provider function. With `NULL`, `AzureAuth` reuses a matching cached
#'   token or starts its normal interactive login flow.
#' @param auth_args Named list of additional arguments passed to
#'   [AzureAuth::get_azure_token()].
#' @param odbc_driver ODBC driver name. ODBC Driver 18 for SQL Server is the
#'   default.
#' @param adbc_driver ADBC driver name or shared-library path. The separately
#'   installed ADBC Driver Foundry `mssql` driver is the default.
#' @param encrypt Whether the driver encrypts the connection. Keep the secure
#'   default, `"yes"`, for Fabric.
#' @param trust_server_certificate Whether to accept a server certificate
#'   without validating its trust chain. Keep the secure default, `"no"`,
#'   unless diagnosing a controlled test environment.
#' @param timeout Non-negative whole-number login/connect timeout in seconds;
#'   `0` lets the driver use an unlimited or driver-specific timeout.
#' @param read_only Logical. `TRUE` sends `ApplicationIntent=ReadOnly` as a
#'   connection hint; it is not a substitute for Fabric/SQL permissions.
#' @param max_tries Positive maximum number of attempts for transient Fabric SQL
#'   failures. Connections are always safe to retry. In
#'   `fabric_sql_query()`, execution failures are retried only when
#'   `idempotent = TRUE`.
#' @param retry_delay Non-negative initial retry delay in seconds. Subsequent
#'   delays use exponential backoff with jitter, capped at 60 seconds.
#' @param verbose Logical. Show authentication, retry, and connection progress.
#' @param ... Additional arguments forwarded to [DBI::dbConnect()]. The former
#'   named `access_token` argument is consumed here as a deprecated alias for
#'   `token` and is not forwarded.
#'
#' @return A live `DBIConnection`. Close it with [DBI::dbDisconnect()] when
#'   finished. For an ADBC connection with child results still registered,
#'   use `DBI::dbDisconnect(con, force = TRUE)` to release them immediately.
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
#' con <- fabric_sql_connect(
#'   server = paste0(
#'     "Server=tcp:example.datawarehouse.fabric.microsoft.com,1433;",
#'     "Initial Catalog=SalesWarehouse;"
#'   )
#' )
#' DBI::dbGetQuery(con, "SELECT TOP 10 * FROM dbo.Customers")
#' DBI::dbDisconnect(con)
#'
#' warehouse <- fabric_warehouses("Analytics")[[1]]
#' con <- fabric_sql_connect(warehouse)
#'
#' # After installing the external driver with `dbc install mssql`:
#' con <- fabric_sql_connect(warehouse, backend = "adbc")
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
  verbose = TRUE,
  max_tries = 3L,
  retry_delay = 5,
  ...
) {
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

  info <- fabric_sql_connection_info(
    server = server,
    database = database,
    target_type = target_type,
    port = port
  )
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
  odbc_args <- c(
    list(
      backend = backend,
      driver = odbc_driver,
      server = info$server,
      Port = info$port,
      Encrypt = encrypt,
      TrustServerCertificate = trust_server_certificate,
      MARS_Connection = "no",
      timeout = as.integer(timeout)
    ),
    if (!is.null(info$database)) list(database = info$database) else list(),
    if (isTRUE(read_only)) list(ApplicationIntent = "ReadOnly") else list(),
    resolved$dots
  )
  for (attempt in seq_len(as.integer(max_tries))) {
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
    connect_args <- if (identical(backend, "odbc")) {
      c(odbc_args, list(attributes = list(azure_token = token_value)))
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
#' Opens a connection with [fabric_sql_connect()], executes `sql`, and closes
#' it automatically. This is the convenient choice for a single query; use
#' [fabric_sql_connect()] when several queries should share one connection.
#' Values in `params` are bound by DBI rather than pasted into the SQL string.
#'
#' @inheritParams fabric_sql_connect
#' @param sql One T-SQL statement. A Lakehouse SQL analytics endpoint supports
#'   read queries but not `INSERT`, `UPDATE`, or `DELETE`.
#' @param params Optional list of values for DBI parameter placeholders (`?`).
#'   Strings, dates, missing values, and values containing SQL metacharacters
#'   are passed unchanged to the driver. With `backend = "adbc"`, placeholders
#'   outside SQL strings, identifiers, and comments are safely translated to
#'   the SQL Server driver's native `@p1`, `@p2`, ... syntax.
#' @param result Result representation. `"tibble"` collects the query result.
#'   `"arrow_stream"` returns a `nanoarrow_array_stream` from
#'   [DBI::dbGetQueryArrow()]. ADBC provides the native Arrow path; DBI may
#'   materialize results when adapting an ODBC connection. The stream implements
#'   the Arrow C Stream interface and can be converted directly with
#'   `arrow::as_record_batch_reader()` when the optional `arrow` package is
#'   installed. A stream is single-use. Prefer `"tibble"` for ordinary analysis
#'   and `"arrow_stream"` when avoiding collection into an R data frame matters.
#' @param idempotent Logical. Set to `TRUE` only if running the entire statement
#'   a second time has no unwanted effect (usually a plain `SELECT`). This
#'   permits a retry when it is unclear whether Fabric executed the first
#'   attempt.
#'
#' @return With `result = "tibble"`, a tibble containing the returned rows and
#'   driver-converted column types. With `result = "arrow_stream"`, a single-use
#'   `nanoarrow_array_stream` that can be consumed by Arrow-compatible tools.
#' @export
#'
#' @examples
#' \dontrun{
#' result <- fabric_sql_query(
#'   server = paste0(
#'     "Server=example.datawarehouse.fabric.microsoft.com;",
#'     "Database=SalesWarehouse;"
#'   ),
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
  verbose = TRUE,
  max_tries = 3L,
  retry_delay = 5,
  idempotent = FALSE,
  ...
) {
  resolved <- fabric_resolve_token_alias(
    token = token,
    dots = list(...),
    caller = "fabric_sql_query()"
  )
  token <- resolved$token
  result <- match.arg(result)
  backend <- match.arg(backend)
  fabric_sql_scalar(sql, "sql")
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
      verbose = verbose,
      max_tries = 1L,
      retry_delay = retry_delay
    ),
    resolved$dots
  )
  for (attempt in seq_len(as.integer(max_tries))) {
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
    if (is.null(outcome$error)) {
      if (identical(result, "arrow_stream")) {
        return(outcome$value)
      }
      return(tibble::as_tibble(outcome$value))
    }
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

fabric_sql_adbc_encrypt <- function(value) {
  fabric_sql_scalar(value, "encrypt")
  normalized <- tolower(trimws(value))
  if (normalized %in% c("strict", "mandatory", "disable", "optional")) {
    return(normalized)
  }
  fabric_sql_adbc_boolean(value, "encrypt")
}

fabric_sql_adbc_parameter_sql <- function(sql, params) {
  chars <- strsplit(sql, "", fixed = TRUE)[[1L]]
  output <- character()
  state <- "normal"
  block_depth <- 0L
  marker <- 0L
  position <- 1L
  total <- length(chars)

  append_chars <- function(...) {
    output <<- c(output, ...)
  }
  next_char <- function() {
    if (position < total) chars[[position + 1L]] else ""
  }

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

fabric_sql_retry_settings <- function(max_tries, retry_delay) {
  if (
    length(max_tries) != 1L ||
      is.na(max_tries) ||
      !is.numeric(max_tries) ||
      !is.finite(max_tries) ||
      max_tries < 1 ||
      max_tries != floor(max_tries)
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

fabric_sql_retry_delay <- function(attempt, retry_delay) {
  base <- min(60, retry_delay * 2^(attempt - 1L))
  base * .fabric_sql_runif(1L, 0.8, 1.2)
}

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

fabric_parse_sql_connection_string <- function(server) {
  value <- trimws(server)
  tokens <- trimws(strsplit(value, ";", fixed = TRUE)[[1L]])
  tokens <- tokens[nzchar(tokens)]
  pairs <- tokens[grepl("=", tokens, fixed = TRUE)]
  fields <- list()
  if (length(pairs)) {
    for (pair in pairs) {
      position <- regexpr("=", pair, fixed = TRUE)[[1L]]
      key <- tolower(trimws(substr(pair, 1L, position - 1L)))
      key <- gsub("[ _]", "", key)
      fields[[key]] <- trimws(substr(pair, position + 1L, nchar(pair)))
    }
  }
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
    host <- bare[[1L]]
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
  database <- fields$initialcatalog %||%
    fields$database %||%
    fields$catalog
  list(server = host, database = database, port = port, fields = fields)
}

fabric_infer_sql_target <- function(server) {
  if (
    grepl(
      "\\.datawarehouse\\.fabric\\.microsoft\\.com$",
      server,
      ignore.case = TRUE
    )
  ) {
    "sql_analytics_endpoint"
  } else if (
    grepl(
      "\\.(?:database\\.fabric|database\\.windows)\\.microsoft\\.com$",
      server,
      ignore.case = TRUE
    )
  ) {
    "sql_database"
  } else {
    "auto"
  }
}

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

fabric_sql_timeout <- function(value) {
  if (
    length(value) != 1L ||
      is.na(value) ||
      !is.numeric(value) ||
      !is.finite(value) ||
      value < 0 ||
      value != floor(value)
  ) {
    rlang::abort(
      "timeout must be one non-negative whole number",
      class = "fabric_sql_target_error"
    )
  }
  invisible(value)
}

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
    parent = if (is.null(secrets)) error else simpleError(message)
  )
}

fabric_sql_redact_secrets <- function(message, secrets = NULL) {
  secrets <- unique(secrets[!is.na(secrets) & nzchar(secrets)])
  for (secret in secrets) {
    message <- gsub(secret, "<redacted>", message, fixed = TRUE)
    encoded <- utils::URLencode(secret, reserved = TRUE)
    message <- gsub(encoded, "<redacted>", message, fixed = TRUE)
  }
  gsub(
    "(?i)(password=)[^&;[:space:]]+",
    "\\1<redacted>",
    message,
    perl = TRUE
  )
}

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

.fabric_sql_db_send_query <- function(con, sql, result) {
  if (identical(result, "arrow_stream")) {
    return(DBI::dbSendQueryArrow(con, sql, immediate = FALSE))
  }
  DBI::dbSendQuery(con, sql, immediate = FALSE)
}

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

.fabric_sql_adbc_bind_frame <- function(params) {
  # adbi converts lists with syntactic name repair, changing @p1 to X.p1.
  # Supplying a data frame with exact names keeps it aligned with the driver.
  parameter_names <- names(params)
  frame <- as.data.frame(
    lapply(params, I),
    fix.empty.names = FALSE,
    check.names = FALSE
  )
  names(frame) <- parameter_names
  frame
}

.fabric_sql_db_fetch <- function(result, output) {
  if (identical(output, "arrow_stream")) {
    return(DBI::dbFetchArrow(result))
  }
  DBI::dbFetch(result)
}

.fabric_sql_db_clear_result <- function(result) {
  DBI::dbClearResult(result)
}

.fabric_sql_db_disconnect <- function(con, force = FALSE) {
  if (inherits(con, "AdbiConnection")) {
    return(DBI::dbDisconnect(con, force = force))
  }
  DBI::dbDisconnect(con)
}

.fabric_sql_sleep <- function(delay) {
  Sys.sleep(delay)
}

.fabric_sql_runif <- function(n, min, max) {
  stats::runif(n, min, max)
}
