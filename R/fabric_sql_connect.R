#' @title
#' Connect to a Microsoft Fabric SQL endpoint
#'
#' @description
#' Opens a DBI/ODBC connection to a Microsoft Fabric **Data Warehouse** or
#' **Lakehouse SQL endpoint**, authenticating with Azure AD (MSAL v2)
#' and passing an access token to the ODBC driver.
#'
#' @details
#' - `server` is the Microsoft Fabric SQL connection string, e.g.
#'   `"xxxx.datawarehouse.fabric.microsoft.com"`.
#'   You can find this by going to your **Lakehouse** or **Data Warehouse** item,
#'   then **Settings** -> **SQL analytics endpoint** -> **SQL connection string**.
#'   You may also pass a DSN-less `Server=...` string; it will be normalized.
#' - By default we request a token for
#'   `https://database.windows.net/.default`.
#' - \pkg{AzureAuth} is used to acquire the token. Be wary of
#'  caching behavior; you may want to call [AzureAuth::clean_token_directory()]
#'  to clear cached tokens if you run into issues
#'
#' @param server Character. Microsoft Fabric SQL connection string or `Server=...` string
#' (see details).
#' @param database Character. Database name. Defaults to `"Lakehouse"`.
#' @param tenant_id Character. Entra ID (AAD) tenant GUID. Defaults to
#'   `Sys.getenv("FABRICQUERYR_TENANT_ID")`.
#' @param client_id Character. App registration (client) ID. Defaults to
#'   `Sys.getenv("FABRICQUERYR_CLIENT_ID")`, falling back to the Azure CLI app id
#'   `"04b07795-8ddb-461a-bbee-02f9e1bf7b46"` if unset.
#' @param access_token Optional character. If supplied, use this bearer token
#'   instead of acquiring a new one via `{AzureAuth}`.
#' @param odbc_driver Character. ODBC driver name. Defaults to
#'   `getOption("fabricqueryr.sql.driver", "ODBC Driver 18 for SQL Server")`.
#' @param port Integer. TCP port (default 1433).
#' @param encrypt,trust_server_certificate Character flags passed to ODBC.
#'   Defaults `"yes"` and `"no"`, respectively.
#' @param timeout Integer. Login/connect timeout in seconds. Default 30.
#' @param verbose Logical. Emit progress via `{cli}`. Default `TRUE`.
#' @param ... Additional arguments forwarded to [DBI::dbConnect()].
#'
#' @return A live `DBIConnection` object.
#' @export
#'
#' @examples
#' # Example is not executed since it requires configured credentials for Fabric
#' \dontrun{
#' con <- fabric_sql_connect(
#'   server    = "2gxz...qiy.datawarehouse.fabric.microsoft.com",
#'   database  = "Lakehouse",
#'   tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
#'   client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID")
#' )
#'
#' # List databases
#' DBI::dbGetQuery(con, "SELECT name FROM sys.databases")
#'
#' # List tables
#' DBI::dbGetQuery(con, "
#'  SELECT TABLE_SCHEMA, TABLE_NAME
#'  FROM INFORMATION_SCHEMA.TABLES
#'  WHERE TABLE_TYPE = 'BASE TABLE'
#' ")
#'
#' # Get a table
#' df <- DBI::dbReadTable(con, "Customers")
#' dplyr::glimpse(df)
#'
#' DBI::dbDisconnect(con)
#' }
fabric_sql_connect <- function(
  server,
  database = "Lakehouse",
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  access_token = NULL,
  odbc_driver = getOption(
    "fabricqueryr.sql.driver",
    "ODBC Driver 18 for SQL Server"
  ),
  port = 1433L,
  encrypt = "yes",
  trust_server_certificate = "no",
  timeout = 30L,
  verbose = TRUE,
  ...
) {
  # ---- validation ----
  stopifnot(
    is.character(server),
    length(server) == 1L,
    nzchar(server),
    is.character(database),
    length(database) == 1L,
    nzchar(database)
  )

  # ---- deps ----
  rlang::check_installed(
    c("DBI", "odbc"),
    reason = "to open a Fabric SQL connection"
  )

  # ---- access token ----
  if (is.null(access_token)) {
    if (!nzchar(tenant_id))
      stop(
        "tenant_id is required (or set FABRICQUERYR_TENANT_ID env var).",
        call. = FALSE
      )
    if (!nzchar(client_id))
      stop(
        "client_id is required (or set FABRICQUERYR_CLIENT_ID env var).",
        call. = FALSE
      )

    inform(verbose, "Authenticating with {.pkg AzureAuth} (MSAL v2) for SQL ...")
    access_token <- fabric_get_sqldb_token(
      tenant_id = tenant_id,
      client_id = client_id
    )
  }

  # ---- normalize server ----
  host <- fabric_normalize_server(server)

  # ---- connect via ODBC ----
  inform(verbose, "Opening ODBC connection to {host} / DB '{database}' ...")
  con <- DBI::dbConnect(
    odbc::odbc(),
    driver = odbc_driver,
    server = host,
    database = database,
    Port = as.integer(port),
    Encrypt = encrypt,
    TrustServerCertificate = trust_server_certificate,
    timeout = as.integer(timeout),
    # Pass Azure AD access token through to the driver
    attributes = list(azure_token = access_token),
    ...
  )
  inform(verbose, "Connected.", type = "success")
  con
}

#' Run a SQL query against a Microsoft Fabric SQL endpoint (opening & closing connection)
#'
#' Convenience wrapper that opens a connection with
#' [fabric_sql_connect()], executes `sql`, and returns a tibble. The
#' connection is closed on exit.
#'
#' @inheritParams fabric_sql_connect
#' @param sql Character scalar. The SQL to run.
#'
#' @return A tibble with the query results (0 rows if none).
#' @export
#'
#' @examples
#' # Example is not executed since it requires configured credentials for Fabric
#' \dontrun{
#' df <- fabric_sql_query(
#'   server    = "2gxz...qiy.datawarehouse.fabric.microsoft.com",
#'   database  = "Lakehouse",
#'   sql       = "SELECT TOP 100 * FROM sys.objects",
#'   tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
#'   client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID")
#' )
#' dplyr::glimpse(df)
#' }
fabric_sql_query <- function(
  server,
  sql,
  database = "Lakehouse",
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  access_token = NULL,
  odbc_driver = getOption(
    "fabricqueryr.sql.driver",
    "ODBC Driver 18 for SQL Server"
  ),
  port = 1433L,
  encrypt = "yes",
  trust_server_certificate = "no",
  timeout = 30L,
  verbose = TRUE,
  ...
) {
  stopifnot(is.character(sql), length(sql) == 1L, nzchar(sql))
  con <- fabric_sql_connect(
    server = server,
    database = database,
    tenant_id = tenant_id,
    client_id = client_id,
    access_token = access_token,
    odbc_driver = odbc_driver,
    port = port,
    encrypt = encrypt,
    trust_server_certificate = trust_server_certificate,
    timeout = timeout,
    verbose = verbose,
    ...
  )
  on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)
  res <- DBI::dbGetQuery(con, sql)
  tibble::as_tibble(res)
}

#' Acquire an Azure AD token for SQL (database.windows.net)
#' @param tenant_id Tenant GUID.
#' @param client_id App (client) ID.
#' @return Bearer access token string.
#' @keywords internal
#' @noRd
fabric_get_sqldb_token <- function(tenant_id, client_id) {
  tok <- AzureAuth::get_azure_token(
    resource = c("https://database.windows.net/.default", "offline_access"),
    tenant = tenant_id,
    app = client_id,
    version = 2
  )
  tok$credentials$access_token
}

#' Normalize a Fabric SQL server value
#' @param server Input like "Server=host" or "tcp:host" or bare "host".
#' @return Hostname string.
#' @keywords internal
#' @noRd
fabric_normalize_server <- function(server) {
  s <- trimws(server)
  s <- sub("(?i)^server\\s*=\\s*", "", s, perl = TRUE)
  s <- sub("(?i)^tcp:\\s*", "", s, perl = TRUE)
  s
}
