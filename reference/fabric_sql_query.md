# Run a parameterized query against Microsoft Fabric SQL

Runs one SQL query and returns its rows, opening and closing the
connection automatically. Use
[`fabric_sql_connect()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_connect.md)
instead when several operations should share a connection. Supply
changing values through `params` rather than pasting them into the SQL
text

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
  numeric_policy = c("exact", "driver"),
  ...
)
```

## Arguments

- server:

  A Fabric SQL server name, a complete connection string copied from the
  Fabric portal, or one Lakehouse, Warehouse, Warehouse snapshot, or SQL
  Database object returned by a discovery function. A discovered object
  is usually simplest because it also supplies the database name

- sql:

  One result-producing T-SQL `SELECT` statement, optionally beginning
  with a common-table-expression `WITH` clause. For DDL or DML, open a
  connection with
  [`fabric_sql_connect()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_connect.md)
  and call
  [`DBI::dbExecute()`](https://dbi.r-dbi.org/reference/dbExecute.html).
  A Lakehouse SQL analytics endpoint is read-only and does not support
  `INSERT`, `UPDATE`, or `DELETE`

- params:

  Optional list of values for `?` placeholders in `sql`. Values are sent
  separately from the SQL text, which is safer and easier to quote
  correctly than building a query with
  [`paste()`](https://rdrr.io/r/base/paste.html). Factors are bound as
  their character labels on both backends. For ODBC,
  [`bit64::integer64`](https://bit64.r-lib.org/reference/bit64-package.html)
  parameters are sent as exact decimal text and their placeholders are
  cast to `bigint` in SQL, preserving numeric operations and missing
  values. ADBC binds them natively. This normalization applies to this
  query helper; direct DBI calls on
  [`fabric_sql_connect()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_connect.md)
  use the driver's parameter conversion.

- result:

  Return a `"tibble"` for ordinary R analysis, or a single-use
  `"arrow_stream"`. ADBC streams retain native Arrow types. ODBC streams
  are converted from R data frames and cannot recover values lost by the
  driver. The 'adbi' driver may fetch the complete result before
  returning the stream, so this option does not guarantee bounded-memory
  retrieval. An Arrow stream owns its DBI result and connection until
  the stream is released; consume it promptly or release it explicitly
  with
  [`nanoarrow::nanoarrow_pointer_release()`](https://arrow.apache.org/nanoarrow/latest/r/reference/nanoarrow_pointer_is_valid.html)

- database:

  Optional catalog/database. An explicit value overrides a database
  found in `server`. For a bare endpoint, supply the item database shown
  with its connection string in Fabric. If omitted, Warehouse and SQL
  analytics endpoints open Fabric's `master` context, which is useful
  for discovery but does not select the item's tables

- target_type:

  Kind of Fabric SQL item. Keep `"auto"` unless a custom hostname
  prevents 'fabricQueryR' from identifying it

- backend:

  Connection driver. Use `"odbc"` for ordinary 'DBI' work or `"adbc"`
  for a native Arrow path after installing its `mssql` driver

- tenant_id:

  Microsoft Entra tenant ID. Defaults to `FABRICQUERYR_TENANT_ID`

- client_id:

  Microsoft Entra application/client ID. Defaults to
  `FABRICQUERYR_CLIENT_ID`, then the Azure CLI application ID

- token:

  Optional access token or token-provider function. Leave `NULL` to let
  'fabricQueryR' use its normal sign-in flow

- auth_args:

  Additional sign-in options passed to
  [`AzureAuth::get_azure_token()`](https://rdrr.io/pkg/AzureAuth/man/get_azure_token.html)

- odbc_driver:

  ODBC driver name. ODBC Driver 18 for SQL Server is the default

- adbc_driver:

  ADBC driver name or shared-library path. The separately installed ADBC
  Driver Foundry `mssql` driver version 1.5.0 or newer is the default
  requirement

- port:

  Optional TCP port. An explicit value overrides a port in `server`;
  otherwise the standard SQL port, 1433, is used

- encrypt:

  Whether the driver encrypts the connection. Keep the secure default,
  `"yes"`, for Fabric

- trust_server_certificate:

  Whether to accept a server certificate without validating its trust
  chain. Keep the secure default, `"no"`, unless diagnosing a controlled
  test environment

- timeout:

  Non-negative whole-number login/connect timeout in seconds; `0` lets
  the driver use an unlimited or driver-specific timeout

- read_only:

  Whether to ask the driver for a read-only connection. This is a
  connection hint, not a replacement for Fabric or SQL permissions

- verbose:

  Logical. Show authentication, retry, and connection progress

- max_tries:

  Maximum attempts after temporary Fabric SQL failures

- retry_delay:

  Initial delay in seconds before retrying. Later retries wait
  progressively longer, up to 60 seconds

- idempotent:

  Logical. Set to `TRUE` only if running the entire statement a second
  time has no unwanted effect (usually a plain `SELECT`). This permits a
  retry when it is unclear whether Fabric executed the first attempt

- numeric_policy:

  `"exact"` (default) preserves ADBC decimals as character and BIGINT as
  [`bit64::integer64`](https://bit64.r-lib.org/reference/bit64-package.html),
  using character for columns containing the minimum BIGINT. INT columns
  containing `-2147483648` use exact doubles. Nested lists retain
  character decimals and 64-bit integers, and double 32-bit integers.
  ODBC rejects DECIMAL, NUMERIC, INT and BIGINT columns before fetching:
  its conversion can round or truncate values or turn valid integer
  boundaries into missing values. Cast these columns to `varchar` in SQL
  or use ADBC. `"driver"` explicitly accepts the backend's conversions,
  including possible rounding and missing values, for either output
  format. This policy applies to this query helper; direct DBI calls on
  [`fabric_sql_connect()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_connect.md)
  use the selected driver's conversion settings.

- ...:

  Additional arguments forwarded to
  [`DBI::dbConnect()`](https://dbi.r-dbi.org/reference/dbConnect.html).
  The former named `access_token` argument is consumed here as a
  deprecated alias for `token` and is not forwarded. For ODBC, a
  caller-supplied `attributes` named list is merged with the
  package-managed `azure_token`; that protected attribute cannot be
  overridden. ODBC authentication, target, driver, and TLS options
  cannot be supplied through `...` because the package validates and
  constructs those settings before attaching the access token. This also
  excludes raw `.connection_string`, `DSN`, and `FileDSN` arguments ADBC
  defaults to `bigint = "integer64"`, so ordinary BIGINT values do not
  have to fit an R 32-bit integer. Supply another `bigint` policy
  explicitly through `...` if needed. Direct DBI reads with `integer64`
  cannot represent the minimum signed BIGINT because 'bit64' reserves
  that value for `NA`. Direct ODBC binding can misinterpret `integer64`
  parameters as doubles. Use `fabric_sql_query()` for its exact
  parameter handling, use ADBC, or supply character parameters with
  explicit SQL `bigint` casts.

## Value

With `result = "tibble"`, a tibble containing the returned rows and
driver-converted column types. With `result = "arrow_stream"`, a
single-use `nanoarrow_array_stream` that can be consumed by
Arrow-compatible tools

## Examples

``` r
if (FALSE) { # \dontrun{
# Discover the Warehouse that will receive the query
workspace <- fabric_workspaces()[[1L]]
warehouse <- fabric_warehouses(workspace)[[1L]]

# Discover and quote a table name through a short 'DBI' connection
con <- fabric_sql_connect(warehouse)
table <- DBI::dbListTables(con)[[1L]]
table <- DBI::dbQuoteIdentifier(con, table)
DBI::dbDisconnect(con)
sql <- paste("SELECT TOP 100 * FROM", table)

# Run the resulting read-only query and collect a tibble
result <- fabric_sql_query(warehouse, sql)

# Return Arrow-native batches instead of converting to a data frame
stream <- fabric_sql_query(
  warehouse,
  sql,
  backend = "adbc",
  result = "arrow_stream"
)
reader <- arrow::as_record_batch_reader(stream)
table <- reader$read_table()
} # }
```
