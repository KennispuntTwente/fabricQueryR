# Run a SQL query against a Microsoft Fabric SQL endpoint (opening & closing connection)

Convenience wrapper that opens a connection with
[`fabric_sql_connect()`](https://lukakoning.github.io/fabricQueryR/reference/fabric_sql_connect.md),
executes `sql`, and returns a tibble. The connection is closed on exit.

## Usage

``` r
fabric_sql_query(
  server,
  sql,
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

- sql:

  Character scalar. The SQL to run.

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

A tibble with the query results (0 rows if none).

## Examples

``` r
# Example is not executed since it requires configured credentials for Fabric
if (FALSE) { # \dontrun{
df <- fabric_sql_query(
  server    = "2gxz...qiy.datawarehouse.fabric.microsoft.com",
  database  = "Lakehouse",
  sql       = "SELECT TOP 100 * FROM sys.objects",
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID")
)
dplyr::glimpse(df)
} # }
```
