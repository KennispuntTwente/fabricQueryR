# Connect to a Microsoft Fabric SQL target

Opens a DBI connection to a Fabric Warehouse, Lakehouse SQL analytics
endpoint, or SQL Database using a Microsoft Entra access token. ODBC is
the default backend; ADBC is available as an opt-in backend.

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
  verbose = TRUE,
  max_tries = 3L,
  retry_delay = 5,
  ...
)
```

## Arguments

- server:

  A character endpoint/connection string, or one Lakehouse, Warehouse,
  or SQL Database record returned by a discovery function.

- database:

  Optional catalog/database. An explicit value overrides a catalog found
  in `server`. `fabric_sql_connect()` and
  [`fabric_sql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_query.md)
  infer complete connection strings and discovery records when this
  argument is omitted. A bare Warehouse or SQL analytics endpoint
  without a catalog connects to Fabric's `master` context.

- target_type:

  Target kind. `"auto"` infers it from discovery metadata or the
  endpoint hostname.

- backend:

  SQL client backend. `"odbc"` remains the default.

- tenant_id:

  Character. Entra tenant ID.

- client_id:

  Character. Application/client ID.

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
  otherwise port 1433 is used.

- encrypt, trust_server_certificate:

  SQL client encryption flags.

- timeout:

  Login/connect timeout in seconds.

- read_only:

  Logical. Set `ApplicationIntent=ReadOnly`.

- verbose:

  Logical. Emit connection progress.

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

A live `DBIConnection`.

## Details

The ODBC backend requires ODBC Driver 18 or newer. Multiple Active
Result Sets (MARS) is disabled because Fabric Warehouse does not support
it. The ADBC backend uses
[`adbi::adbi()`](https://adbi.r-dbi.org/reference/dbConnect.html) with
the `mssql` driver loaded by
[`adbcdrivermanager::adbc_driver()`](https://arrow.apache.org/adbc/current/r/adbcdrivermanager/reference/adbc_driver_void.html).
Install that external driver separately with `dbc install mssql`. The
package checks that the driver can be loaded before authenticating or
opening a network connection. `adbcdrivermanager` discovers and loads
installed drivers; it does not install driver binaries.

Complete portal connection strings and enriched discovery records
provide a catalog automatically. Bare endpoints may omit `database` to
use Fabric's `master` context; the package never guesses a catalog name.

Transient Fabric connection failures are retried on fresh connections
with refreshed tokens and bounded exponential backoff.

The SQL audience is `https://database.windows.net/.default`. The
identity must have permission to connect to and query the target item.

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

warehouse <- fabric_warehouses("Analytics")[1, ]
con <- fabric_sql_connect(warehouse)

# After installing the external driver with `dbc install mssql`:
con <- fabric_sql_connect(warehouse, backend = "adbc")
} # }
```
