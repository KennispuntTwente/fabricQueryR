# Connect to a Microsoft Fabric SQL target

Opens a standard R DBI connection to a Fabric Warehouse, Lakehouse SQL
analytics endpoint, or SQL Database using Microsoft Entra
authentication. Use the returned connection with familiar DBI functions
such as
[`DBI::dbListTables()`](https://dbi.r-dbi.org/reference/dbListTables.html)
and
[`DBI::dbGetQuery()`](https://dbi.r-dbi.org/reference/dbGetQuery.html).

## Usage

``` r
fabric_sql_connect(
  server,
  database = NULL,
  target_type = c("auto", "lakehouse", "warehouse", "sql_database",
    "sql_analytics_endpoint"),
  backend = c("odbc", "adbc"),
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  token = NULL,
  auth_args = list(),
  odbc_driver = getOption("fabricqueryr.sql.driver", "ODBC Driver 18 for SQL Server"),
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
)
```

## Arguments

- server:

  A Fabric SQL server name, a complete connection string copied from the
  Fabric portal, or one Lakehouse, Warehouse, or SQL Database record
  returned by a discovery function. A discovered record is usually
  simplest because it also supplies the database name.

- database:

  Optional catalog/database. An explicit value overrides a database
  found in `server`. For a bare endpoint, supply the item database shown
  with its connection string in Fabric. If omitted, Warehouse and SQL
  analytics endpoints open Fabric's `master` context, which is useful
  for discovery but does not select the item's tables.

- target_type:

  Label for the endpoint kind. Keep `"auto"` unless the hostname is
  custom or ambiguous. The explicit choices distinguish a Lakehouse SQL
  analytics endpoint, Warehouse, transactional SQL Database, or another
  read-only SQL analytics endpoint; they do not convert one kind of
  endpoint into another.

- backend:

  SQL client backend. Use `"odbc"` for broad DBI compatibility and the
  easiest setup; use `"adbc"` for its native Arrow result path after
  separately installing the ADBC `mssql` driver.

- tenant_id:

  Microsoft Entra tenant ID. Defaults to `FABRICQUERYR_TENANT_ID`.

- client_id:

  Microsoft Entra application/client ID. Defaults to
  `FABRICQUERYR_CLIENT_ID`, then the Azure CLI application ID.

- token:

  Optional
  [`AzureAuth::AzureToken`](https://rdrr.io/pkg/AzureAuth/man/AzureToken.html),
  bearer-token string, or token-provider function. With `NULL`,
  `AzureAuth` reuses a matching cached token or starts its normal
  interactive login flow.

- auth_args:

  Named list of additional arguments passed to
  [`AzureAuth::get_azure_token()`](https://rdrr.io/pkg/AzureAuth/man/get_azure_token.html).

- odbc_driver:

  ODBC driver name. ODBC Driver 18 for SQL Server is the default.

- adbc_driver:

  ADBC driver name or shared-library path. The separately installed ADBC
  Driver Foundry `mssql` driver is the default.

- port:

  Optional TCP port. An explicit value overrides a port in `server`;
  otherwise the standard SQL port, 1433, is used.

- encrypt:

  Whether the driver encrypts the connection. Keep the secure default,
  `"yes"`, for Fabric.

- trust_server_certificate:

  Whether to accept a server certificate without validating its trust
  chain. Keep the secure default, `"no"`, unless diagnosing a controlled
  test environment.

- timeout:

  Non-negative whole-number login/connect timeout in seconds; `0` lets
  the driver use an unlimited or driver-specific timeout.

- read_only:

  Logical. `TRUE` sends `ApplicationIntent=ReadOnly` as a connection
  hint; it is not a substitute for Fabric/SQL permissions.

- allow_custom_endpoint:

  Logical. Fabric SQL and Microsoft SQL Database hostnames are trusted
  by default. Set to `TRUE` only when deliberately sending the SQL
  access token to another hostname, such as a controlled proxy or test
  server.

- verbose:

  Logical. Show authentication, retry, and connection progress.

- max_tries:

  Positive maximum number of attempts for transient Fabric SQL failures.
  Connections are always safe to retry. In
  [`fabric_sql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_query.md),
  execution failures are retried only when `idempotent = TRUE`.

- retry_delay:

  Non-negative initial retry delay in seconds. Subsequent delays use
  exponential backoff with jitter, capped at 60 seconds.

- ...:

  Additional arguments forwarded to
  [`DBI::dbConnect()`](https://dbi.r-dbi.org/reference/dbConnect.html).
  The former named `access_token` argument is consumed here as a
  deprecated alias for `token` and is not forwarded.

## Value

A live `DBIConnection`. Close it with
[`DBI::dbDisconnect()`](https://dbi.r-dbi.org/reference/dbDisconnect.html)
when finished. For an ADBC connection with child results still
registered, use `DBI::dbDisconnect(con, force = TRUE)` to release them
immediately.

## Details

In Fabric, copy a SQL connection string from the Warehouse, SQL
analytics endpoint, or SQL Database settings. Alternatively, use a row
returned by
[`fabric_warehouses()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md),
[`fabric_lakehouses()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md),
or
[`fabric_sql_databases()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md).
A Lakehouse's SQL analytics endpoint is read-only; use Spark or another
OneLake writer when data must be changed.

`"odbc"` is the most familiar choice for DBI workflows and requires
Microsoft ODBC Driver 18 or newer. `"adbc"` is useful when a query
result should remain in Arrow form, especially for larger analytical
results. It uses
[`adbi::adbi()`](https://adbi.r-dbi.org/reference/dbConnect.html) with
the `mssql` driver loaded by
[`adbcdrivermanager::adbc_driver()`](https://arrow.apache.org/adbc/current/r/adbcdrivermanager/reference/adbc_driver_void.html).
Install that external driver separately with `dbc install mssql`. The
package checks that the driver can be loaded before authenticating or
opening a network connection. `adbcdrivermanager` discovers and loads
installed drivers; it does not install driver binaries. Multiple Active
Result Sets (MARS) is disabled because Fabric Warehouse does not support
it.

Complete portal connection strings and enriched discovery records
provide a catalog automatically. Bare endpoints may omit `database` to
use Fabric's `master` context; the package never guesses a catalog name.

Transient Fabric connection failures are retried on fresh connections
with refreshed tokens and bounded exponential backoff.

The SQL audience is `https://database.windows.net/.default`. In Fabric,
give the user or application access through a workspace role or the
item's **Manage permissions** dialog. SQL `GRANT`/`DENY` rules can
further restrict what the identity may query.

## References

[Connect to a Fabric Warehouse or SQL analytics
endpoint](https://learn.microsoft.com/en-us/fabric/data-warehouse/how-to-connect)

[Microsoft Entra authentication in Fabric Data
Warehouse](https://learn.microsoft.com/en-us/fabric/data-warehouse/entra-id-authentication)

[Lakehouse SQL analytics
endpoint](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-sql-analytics-endpoint)

[Download Microsoft ODBC Driver 18 for SQL
Server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server)

## Examples

``` r
if (FALSE) { # \dontrun{
con <- fabric_sql_connect(
  server = paste0(
    "Server=tcp:example.datawarehouse.fabric.microsoft.com,1433;",
    "Initial Catalog=SalesWarehouse;"
  )
)
DBI::dbGetQuery(con, "SELECT TOP 10 * FROM dbo.Customers")
DBI::dbDisconnect(con)

warehouse <- fabric_warehouses("Analytics")[[1]]
con <- fabric_sql_connect(warehouse)

# After installing the external driver with `dbc install mssql`:
con <- fabric_sql_connect(warehouse, backend = "adbc")
} # }
```
