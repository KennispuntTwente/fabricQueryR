# Connect to a Microsoft Fabric SQL target

Opens a DBI/ODBC connection to a Fabric Warehouse, Lakehouse SQL
analytics endpoint, or SQL Database using a Microsoft Entra access
token.

## Usage

``` r
fabric_sql_connect(
  server,
  database = NULL,
  target_type = c("auto", "lakehouse", "warehouse", "sql_database",
    "sql_analytics_endpoint"),
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  access_token = NULL,
  token_provider = NULL,
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

- database:

  Optional catalog/database. An explicit value overrides a catalog found
  in `server`.

- target_type:

  Target kind. `"auto"` infers it from discovery metadata or the
  endpoint hostname.

- tenant_id:

  Character. Entra tenant ID.

- client_id:

  Character. Application/client ID.

- access_token:

  Optional pre-acquired SQL bearer token.

- token_provider:

  Optional refreshable SQL token callback.

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

## Value

A live `DBIConnection`.

## Details

Fabric Warehouse and SQL analytics endpoints require ODBC Driver 18 or
newer. Multiple Active Result Sets (MARS) is disabled because Fabric
Warehouse does not support it. A catalog is always required so a bare
server must be paired with `database`; complete portal connection
strings and enriched discovery records provide it automatically.

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
} # }
```
