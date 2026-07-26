# Run a parameterized query against Microsoft Fabric SQL

Opens a connection with
[`fabric_sql_connect()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_connect.md),
executes `sql`, and closes the connection. Values in `params` are bound
by DBI; they are never interpolated into the SQL string. Transient
connection failures are safe to retry. Set `idempotent = TRUE` only when
the complete SQL statement may be rerun after an ambiguous transient
execution failure.

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

  A character endpoint/connection string, or one Lakehouse, Warehouse,
  or SQL Database record returned by a discovery function.

- sql:

  One SQL statement.

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
  single-use.

- database:

  Optional catalog/database. An explicit value overrides a catalog found
  in `server`.
  [`fabric_sql_connect()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_connect.md)
  and `fabric_sql_query()` infer complete connection strings and
  discovery records when this argument is omitted. A bare Warehouse or
  SQL analytics endpoint without a catalog connects to Fabric's `master`
  context.

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
  Connections are always safe to retry. In `fabric_sql_query()`,
  execution failures are retried only when `idempotent = TRUE`.

- retry_delay:

  Non-negative initial retry delay in seconds. Subsequent delays use
  exponential backoff with jitter, capped at 60 seconds.

- idempotent:

  Logical. Whether the SQL statement may safely rerun on a fresh
  connection after a transient execution failure.

- ...:

  Additional arguments forwarded to
  [`DBI::dbConnect()`](https://dbi.r-dbi.org/reference/dbConnect.html).
  The former named `access_token` argument is consumed here as a
  deprecated alias for `token` and is not forwarded.

## Value

A tibble or `nanoarrow_array_stream`, according to `result`.

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
