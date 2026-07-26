# Run a parameterized query against Microsoft Fabric SQL

Opens a connection with
[`fabric_sql_connect()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_connect.md),
executes `sql`, and closes it automatically. This is the convenient
choice for a single query; use
[`fabric_sql_connect()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_connect.md)
when several queries should share one connection. Values in `params` are
bound by DBI rather than pasted into the SQL string.

## Usage

``` r
fabric_sql_query(
  server,
  sql,
  params = NULL,
  result = c("tibble", "arrow_stream"),
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
  idempotent = FALSE,
  ...
)
```

## Arguments

- server:

  A Fabric SQL server name, a complete connection string copied from the
  Fabric portal, or one Lakehouse, Warehouse, or SQL Database row
  returned by a discovery function. A discovered row is usually simplest
  because it also supplies the database name.

- sql:

  One T-SQL statement. A Lakehouse SQL analytics endpoint supports read
  queries but not `INSERT`, `UPDATE`, or `DELETE`.

- params:

  Optional list of values for DBI parameter placeholders (`?`). Strings,
  dates, missing values, and values containing SQL metacharacters are
  passed unchanged to the driver. With `backend = "adbc"`, placeholders
  outside SQL strings, identifiers, and comments are safely translated
  to the SQL Server driver's native `@p1`, `@p2`, ... syntax.

- result:

  Result representation. `"tibble"` collects the query result.
  `"arrow_stream"` returns a `nanoarrow_array_stream` from
  [`DBI::dbGetQueryArrow()`](https://dbi.r-dbi.org/reference/dbGetQueryArrow.html).
  ADBC provides the native Arrow path; DBI may materialize results when
  adapting an ODBC connection. The stream implements the Arrow C Stream
  interface and can be converted directly with
  [`arrow::as_record_batch_reader()`](https://arrow.apache.org/docs/r/reference/as_record_batch_reader.html)
  when the optional `arrow` package is installed. A stream is
  single-use. Prefer `"tibble"` for ordinary analysis and
  `"arrow_stream"` when avoiding collection into an R data frame
  matters.

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

  Non-negative login/connect timeout in seconds; `0` lets the driver use
  an unlimited or driver-specific timeout.

- read_only:

  Logical. `TRUE` sends `ApplicationIntent=ReadOnly` as a connection
  hint; it is not a substitute for Fabric/SQL permissions.

- verbose:

  Logical. Show authentication, retry, and connection progress.

- max_tries:

  Positive maximum number of attempts for transient Fabric SQL failures.
  Connections are always safe to retry. In `fabric_sql_query()`,
  execution failures are retried only when `idempotent = TRUE`.

- retry_delay:

  Non-negative initial retry delay in seconds. Subsequent delays use
  exponential backoff with jitter, capped at 60 seconds.

- idempotent:

  Logical. Set to `TRUE` only if running the entire statement a second
  time has no unwanted effect (usually a plain `SELECT`). This permits a
  retry when it is unclear whether Fabric executed the first attempt.

- ...:

  Additional arguments forwarded to
  [`DBI::dbConnect()`](https://dbi.r-dbi.org/reference/dbConnect.html).
  The former named `access_token` argument is consumed here as a
  deprecated alias for `token` and is not forwarded.

## Value

With `result = "tibble"`, a tibble containing the returned rows and
driver-converted column types. With `result = "arrow_stream"`, a
single-use `nanoarrow_array_stream` that can be consumed by
Arrow-compatible tools.

## Examples

``` r
if (FALSE) { # \dontrun{
result <- fabric_sql_query(
  server = paste0(
    "Server=example.datawarehouse.fabric.microsoft.com;",
    "Database=SalesWarehouse;"
  ),
  sql = "SELECT * FROM dbo.Customers WHERE region = ?",
  params = list("West")
)

stream <- fabric_sql_query(
  warehouse,
  "SELECT * FROM dbo.Customers",
  backend = "adbc",
  result = "arrow_stream"
)
reader <- arrow::as_record_batch_reader(stream)
table <- reader$read_table()
} # }
```
