# Run a parameterized query against Microsoft Fabric SQL

Opens a connection with
[`fabric_sql_connect()`](https://lukakoning.github.io/fabricQueryR/reference/fabric_sql_connect.md),
executes `sql`, and closes the connection. Values in `params` are bound
by DBI; they are never interpolated into the SQL string.

## Usage

``` r
fabric_sql_query(
  server,
  sql,
  params = NULL,
  database = NULL,
  target_type = c("auto", "lakehouse", "warehouse", "sql_database",
    "sql_analytics_endpoint"),
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  token = NULL,
  auth_args = list(),
  odbc_driver = getOption("fabricqueryr.sql.driver", "ODBC Driver 18 for SQL Server"),
  port = NULL,
  encrypt = "yes",
  trust_server_certificate = "no",
  timeout = 30L,
  read_only = FALSE,
  verbose = TRUE,
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
  passed unchanged to the driver.

- database:

  Optional catalog/database. An explicit value overrides a catalog found
  in `server`.
  [`fabric_sql_connect()`](https://lukakoning.github.io/fabricQueryR/reference/fabric_sql_connect.md)
  and `fabric_sql_query()` infer complete connection strings and
  discovery records when this argument is omitted. Bare endpoints
  require an explicit catalog.

- target_type:

  Target kind. `"auto"` infers it from discovery metadata or the
  endpoint hostname.

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

- port:

  Optional TCP port. An explicit value overrides a port in `server`;
  otherwise port 1433 is used.

- encrypt, trust_server_certificate:

  ODBC encryption flags.

- timeout:

  Login/connect timeout in seconds.

- read_only:

  Logical. Set ODBC `ApplicationIntent=ReadOnly`.

- verbose:

  Logical. Emit connection progress.

- ...:

  Additional arguments forwarded to
  [`DBI::dbConnect()`](https://dbi.r-dbi.org/reference/dbConnect.html).
  The former named `access_token` argument is consumed here as a
  deprecated alias for `token` and is not forwarded.

## Value

A tibble containing the result.

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
} # }
```
