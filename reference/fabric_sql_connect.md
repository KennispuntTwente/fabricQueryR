# Connect to a Microsoft Fabric SQL endpoint

Opens a DBI/ODBC connection to a Microsoft Fabric **Data Warehouse** or
**Lakehouse SQL endpoint**, authenticating with Azure AD (MSAL v2) and
passing an access token to the ODBC driver.

## Usage

``` r
fabric_sql_connect(
  server,
  database = "Lakehouse",
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  access_token = NULL,
  odbc_driver = getOption("fabricqueryr.sql.driver", "ODBC Driver 18 for SQL Server"),
  port = 1433L,
  encrypt = "yes",
  trust_server_certificate = "no",
  timeout = 30L,
  verbose = TRUE,
  ...
)
```

## Arguments

- server:

  Character. Microsoft Fabric SQL connection string or `Server=...`
  string (see details).

- database:

  Character. Database name. Defaults to `"Lakehouse"`.

- tenant_id:

  Character. Entra ID (AAD) tenant GUID. Defaults to
  `Sys.getenv("FABRICQUERYR_TENANT_ID")`.

- client_id:

  Character. App registration (client) ID. Defaults to
  `Sys.getenv("FABRICQUERYR_CLIENT_ID")`, falling back to the Azure CLI
  app id `"04b07795-8ddb-461a-bbee-02f9e1bf7b46"` if unset.

- access_token:

  Optional character. If supplied, use this bearer token instead of
  acquiring a new one via `{AzureAuth}`.

- odbc_driver:

  Character. ODBC driver name. Defaults to
  `getOption("fabricqueryr.sql.driver", "ODBC Driver 18 for SQL Server")`.

- port:

  Integer. TCP port (default 1433).

- encrypt, trust_server_certificate:

  Character flags passed to ODBC. Defaults `"yes"` and `"no"`,
  respectively.

- timeout:

  Integer. Login/connect timeout in seconds. Default 30.

- verbose:

  Logical. Emit progress via `{cli}`. Default `TRUE`.

- ...:

  Additional arguments forwarded to
  [`DBI::dbConnect()`](https://dbi.r-dbi.org/reference/dbConnect.html).

## Value

A live `DBIConnection` object.

## Details

- `server` is the Microsoft Fabric SQL connection string, e.g.
  `"xxxx.datawarehouse.fabric.microsoft.com"`. You can find this by
  going to your **Lakehouse** or **Data Warehouse** item, then
  **Settings** -\> **SQL analytics endpoint** -\> **SQL connection
  string**. You may also pass a DSN-less `Server=...` string; it will be
  normalized.

- By default we request a token for
  `https://database.windows.net/.default`.

- AzureAuth is used to acquire the token. Be wary of caching behavior;
  you may want to call
  [`AzureAuth::clean_token_directory()`](https://rdrr.io/pkg/AzureAuth/man/get_azure_token.html)
  to clear cached tokens if you run into issues

## Examples

``` r
# Example is not executed since it requires configured credentials for Fabric
if (FALSE) { # \dontrun{
con <- fabric_sql_connect(
  server    = "2gxz...qiy.datawarehouse.fabric.microsoft.com",
  database  = "Lakehouse",
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID")
)

# List databases
DBI::dbGetQuery(con, "SELECT name FROM sys.databases")

# List tables
DBI::dbGetQuery(con, "
 SELECT TABLE_SCHEMA, TABLE_NAME
 FROM INFORMATION_SCHEMA.TABLES
 WHERE TABLE_TYPE = 'BASE TABLE'
")

# Get a table
df <- DBI::dbReadTable(con, "Customers")
dplyr::glimpse(df)

DBI::dbDisconnect(con)
} # }
```
