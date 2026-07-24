#' Parse a Microsoft Fabric SQL target
#'
#' Normalizes a bare Fabric SQL endpoint, a complete portal connection string,
#' or one enriched discovery record into connection information used by
#' [fabric_sql_connect()].
#'
#' @param server A character endpoint/connection string, or one Lakehouse,
#'   Warehouse, or SQL Database record returned by a discovery function.
#' @param database Optional catalog/database. An explicit value overrides a
#'   catalog found in `server`.
#' @param target_type Target kind. `"auto"` infers it from discovery metadata or
#'   the endpoint hostname.
#' @param port Optional TCP port. An explicit value overrides a port in
#'   `server`; otherwise port 1433 is used.
#'
#' @return A `fabric_sql_connection_info` list with `server`, `database`,
#'   `port`, and `target_type`.
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
          "SQLDatabase item."
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
  if (is.null(database) || !nzchar(trimws(database))) {
    rlang::abort(
      paste0(
        "A Fabric SQL catalog is required. Supply database, use a complete ",
        "connection string containing Initial Catalog/Database, or pass an ",
        "enriched discovery record."
      ),
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
      database = trimws(database),
      port = as.integer(resolved_port),
      target_type = resolved_type,
      source = if (is.null(record)) "character" else "discovery"
    ),
    class = c("fabric_sql_connection_info", "list")
  )
}

#' Connect to a Microsoft Fabric SQL target
#'
#' Opens a DBI/ODBC connection to a Fabric Warehouse, Lakehouse SQL analytics
#' endpoint, or SQL Database using a Microsoft Entra access token.
#'
#' @details
#' Fabric Warehouse and SQL analytics endpoints require ODBC Driver 18 or
#' newer. Multiple Active Result Sets (MARS) is disabled because Fabric
#' Warehouse does not support it. A catalog is always required so a bare server
#' must be paired with `database`; complete portal connection strings and
#' enriched discovery records provide it automatically.
#'
#' The SQL audience is `https://database.windows.net/.default`. The identity
#' must have permission to connect to and query the target item.
#'
#' @inheritParams fabric_sql_connection_info
#' @param tenant_id Character. Entra tenant ID.
#' @param client_id Character. Application/client ID.
#' @param access_token Optional pre-acquired SQL bearer token.
#' @param token_provider Optional refreshable SQL token callback.
#' @param odbc_driver ODBC driver name. ODBC Driver 18 for SQL Server is the
#'   default.
#' @param encrypt,trust_server_certificate ODBC encryption flags.
#' @param timeout Login/connect timeout in seconds.
#' @param read_only Logical. Set ODBC `ApplicationIntent=ReadOnly`.
#' @param verbose Logical. Emit connection progress.
#' @param ... Additional arguments forwarded to [DBI::dbConnect()].
#'
#' @return A live `DBIConnection`.
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
#' warehouse <- fabric_warehouses("Analytics")[1, ]
#' con <- fabric_sql_connect(warehouse)
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
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  access_token = NULL,
  token_provider = NULL,
  odbc_driver = getOption(
    "fabricqueryr.sql.driver",
    "ODBC Driver 18 for SQL Server"
  ),
  port = NULL,
  encrypt = "yes",
  trust_server_certificate = "no",
  timeout = 30L,
  read_only = FALSE,
  verbose = TRUE,
  ...
) {
  target_type <- match.arg(target_type)
  stopifnot(
    is.logical(read_only),
    length(read_only) == 1L,
    !is.na(read_only)
  )
  fabric_sql_port(timeout, "timeout", allow_zero = TRUE)
  rlang::check_installed(
    c("DBI", "odbc"),
    reason = "to open a Fabric SQL connection"
  )

  info <- fabric_sql_connection_info(
    server = server,
    database = database,
    target_type = target_type,
    port = port
  )
  if (is.null(access_token) && is.null(token_provider)) {
    inform(
      verbose,
      "Authenticating with {.pkg AzureAuth} (MSAL v2) for SQL ..."
    )
  }
  credential <- fabric_credential(
    tenant_id = tenant_id,
    client_id = client_id,
    access_token = access_token,
    token_provider = token_provider
  )
  token <- tryCatch(
    fabric_get_token(credential, .fabric_audience$sql),
    error = function(error) {
      rlang::abort(
        "Fabric SQL authentication failed while acquiring an access token.",
        class = "fabric_sql_authentication_error",
        parent = error
      )
    }
  )

  inform(
    verbose,
    "Opening ODBC connection to {info$server} / DB '{info$database}' ..."
  )
  args <- c(
    list(
      driver = odbc_driver,
      server = info$server,
      database = info$database,
      Port = info$port,
      Encrypt = encrypt,
      TrustServerCertificate = trust_server_certificate,
      MARS_Connection = "no",
      timeout = as.integer(timeout),
      attributes = list(azure_token = token)
    ),
    if (isTRUE(read_only)) list(ApplicationIntent = "ReadOnly") else list(),
    list(...)
  )
  con <- tryCatch(
    do.call(.fabric_sql_db_connect, args),
    error = fabric_sql_connection_error
  )
  inform(verbose, "Connected.", type = "success")
  con
}

#' Run a parameterized query against Microsoft Fabric SQL
#'
#' Opens a connection with [fabric_sql_connect()], executes `sql`, and closes
#' the connection. Values in `params` are bound by DBI; they are never
#' interpolated into the SQL string.
#'
#' @inheritParams fabric_sql_connect
#' @param sql One SQL statement.
#' @param params Optional list of values for DBI parameter placeholders (`?`).
#'   Strings, dates, missing values, and values containing SQL metacharacters
#'   are passed unchanged to the driver.
#'
#' @return A tibble containing the result.
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
#' }
fabric_sql_query <- function(
  server,
  sql,
  params = NULL,
  database = NULL,
  target_type = c(
    "auto",
    "lakehouse",
    "warehouse",
    "sql_database",
    "sql_analytics_endpoint"
  ),
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  access_token = NULL,
  token_provider = NULL,
  odbc_driver = getOption(
    "fabricqueryr.sql.driver",
    "ODBC Driver 18 for SQL Server"
  ),
  port = NULL,
  encrypt = "yes",
  trust_server_certificate = "no",
  timeout = 30L,
  read_only = FALSE,
  verbose = TRUE,
  ...
) {
  fabric_sql_scalar(sql, "sql")
  if (!is.null(params) && !is.list(params)) {
    rlang::abort(
      "params must be NULL or a list.",
      class = "fabric_sql_execution_error"
    )
  }
  con <- fabric_sql_connect(
    server = server,
    database = database,
    target_type = match.arg(target_type),
    tenant_id = tenant_id,
    client_id = client_id,
    access_token = access_token,
    token_provider = token_provider,
    odbc_driver = odbc_driver,
    port = port,
    encrypt = encrypt,
    trust_server_certificate = trust_server_certificate,
    timeout = timeout,
    read_only = read_only,
    verbose = verbose,
    ...
  )
  on.exit(try(.fabric_sql_db_disconnect(con), silent = TRUE), add = TRUE)
  result <- tryCatch(
    .fabric_sql_db_get_query(con, sql, params = params),
    error = function(error) {
      rlang::abort(
        "Fabric SQL query execution failed.",
        class = "fabric_sql_execution_error",
        parent = error
      )
    }
  )
  tibble::as_tibble(result)
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
        "Could not find a unique Server/Data Source in the SQL target.",
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
      "Fabric SQL server is empty.",
      class = "fabric_sql_target_error"
    )
  }
  database <- fields$initialcatalog %||%
    fields$database %||%
    fields$catalog
  list(server = host, database = database, port = port, fields = fields)
}

# Kept for compatibility with callers that used the former internal helper.
fabric_normalize_server <- function(server) {
  fabric_parse_sql_connection_string(server)$server
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
      sprintf("%s must be one non-empty character value.", argument),
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
        "%s must be one integer between %d and 65535.",
        argument,
        minimum
      ),
      class = "fabric_sql_target_error"
    )
  }
  invisible(value)
}

fabric_sql_connection_error <- function(error) {
  message <- conditionMessage(error)
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
    parent = error
  )
}

.fabric_sql_db_connect <- function(...) {
  DBI::dbConnect(odbc::odbc(), ...)
}

.fabric_sql_db_get_query <- function(con, sql, params = NULL) {
  DBI::dbGetQuery(con, sql, params = params)
}

.fabric_sql_db_disconnect <- function(con) {
  DBI::dbDisconnect(con)
}
