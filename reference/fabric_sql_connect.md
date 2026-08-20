# Connect to a Microsoft Fabric SQL target

Opens a DBI connection to a Fabric Warehouse, Warehouse snapshot,
Lakehouse, mirrored database, or SQL Database. Use the connection with
familiar DBI functions such as
[`DBI::dbListTables()`](https://dbi.r-dbi.org/reference/dbListTables.html)
and
[`DBI::dbGetQuery()`](https://dbi.r-dbi.org/reference/dbGetQuery.html)

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

  A Fabric SQL server name, a complete connection string copied from the
  Fabric portal, or one Lakehouse, Warehouse, Warehouse snapshot, or SQL
  Database record returned by a discovery function. A discovered record
  is usually simplest because it also supplies the database name

- database:

  Optional catalog/database. An explicit value overrides a database
  found in `server`. For a bare endpoint, supply the item database shown
  with its connection string in Fabric. If omitted, Warehouse and SQL
  analytics endpoints open Fabric's `master` context, which is useful
  for discovery but does not select the item's tables

- target_type:

  Kind of Fabric SQL item. Keep `"auto"` unless a custom hostname
  prevents fabricQueryR from identifying it

- backend:

  Connection driver. Use `"odbc"` for ordinary DBI work or `"adbc"` for
  a native Arrow path after installing its `mssql` driver

- tenant_id:

  Microsoft Entra tenant ID. Defaults to `FABRICQUERYR_TENANT_ID`

- client_id:

  Microsoft Entra application/client ID. Defaults to
  `FABRICQUERYR_CLIENT_ID`, then the Azure CLI application ID

- token:

  Optional access token or token-provider function. Leave `NULL` to let
  fabricQueryR use its normal sign-in flow

- auth_args:

  Additional sign-in options passed to
  [`AzureAuth::get_azure_token()`](https://rdrr.io/pkg/AzureAuth/man/get_azure_token.html)

- odbc_driver:

  ODBC driver name. ODBC Driver 18 for SQL Server is the default

- adbc_driver:

  ADBC driver name or shared-library path. The separately installed ADBC
  Driver Foundry `mssql` driver is the default

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

- ...:

  Additional arguments forwarded to
  [`DBI::dbConnect()`](https://dbi.r-dbi.org/reference/dbConnect.html).
  The former named `access_token` argument is consumed here as a
  deprecated alias for `token` and is not forwarded. For ODBC, a
  caller-supplied `attributes` named list is merged with the
  package-managed `azure_token`; that protected attribute cannot be
  overridden

## Value

A live `DBIConnection`. Close it with
[`DBI::dbDisconnect()`](https://dbi.r-dbi.org/reference/dbDisconnect.html)
when finished. For an ADBC connection with child results still
registered, use `DBI::dbDisconnect(con, force = TRUE)` to release them
immediately

## Details

The easiest input is an item returned by
[`fabric_warehouses()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md),
[`fabric_lakehouses()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md),
[`fabric_mirrored_databases()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md),
or
[`fabric_sql_databases()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md).
You can also paste a SQL connection string from Fabric. Lakehouse and
mirrored database SQL endpoints are read-only; use the source system,
Spark, or another appropriate writer to change their data

## Choosing a backend

`backend = "odbc"` is the default and works well for ordinary DBI use.
It requires Microsoft ODBC Driver 18 or newer. Use `backend = "adbc"`
when you want a native Arrow result path, typically for larger
analytical results ADBC requires the external `mssql` driver; install it
separately with `dbc install mssql`

## Connection and permissions

Discovery records and complete portal connection strings normally
include the database. A bare server can omit `database` to open Fabric's
`master` context. Transient connection failures are retried
automatically. The user or application must have access through a
workspace role or the item's **Manage permissions** settings; SQL
permissions may further restrict data

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
# Discover a Warehouse so no server name or database ID is copied by hand
workspace <- fabric_workspaces()[[1L]]
warehouse <- fabric_warehouses(workspace)[[1L]]

# Open a DBI connection, use it, and always disconnect when finished
con <- fabric_sql_connect(warehouse)
table <- DBI::dbListTables(con)[[1L]]
table <- DBI::dbQuoteIdentifier(con, table)
DBI::dbGetQuery(con, paste("SELECT TOP 10 * FROM", table))
DBI::dbDisconnect(con)

# The ADBC backend can return Arrow-native results when installed
adbc_con <- fabric_sql_connect(warehouse, backend = "adbc")
DBI::dbDisconnect(adbc_con)
} # }
```
