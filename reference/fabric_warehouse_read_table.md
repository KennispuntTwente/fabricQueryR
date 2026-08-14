# Read a Microsoft Fabric Warehouse table

Provides the table-oriented read counterpart to
[`fabric_warehouse_write_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_warehouse_write_table.md).
It resolves the Warehouse like the writer, safely quotes the schema,
table, and projected columns, and delegates query execution and type
conversion to
[`fabric_sql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_query.md).
Use that lower-level function for filters, ordering, joins,
aggregations, or other T-SQL.

## Usage

``` r
fabric_warehouse_read_table(
  warehouse,
  table,
  workspace = NULL,
  schema = "dbo",
  columns = NULL,
  limit = NULL,
  result = c("tibble", "arrow_stream"),
  backend = c("odbc", "adbc"),
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  token = NULL,
  auth_args = list(),
  api_base = .fabric_api_base,
  allow_custom_endpoint = FALSE,
  verbose = TRUE,
  timeout = 30L,
  max_tries = 3L,
  retry_delay = 5
)
```

## Arguments

- warehouse:

  A Warehouse record returned by
  [`fabric_warehouses()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md)
  or
  [`fabric_item()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_item.md),
  or its name or GUID when `workspace` is supplied.

- table:

  Warehouse table name, or a record containing a `name`, `table`, or
  `displayName` field.

- workspace:

  Workspace name, GUID, or discovery record containing `warehouse`. May
  be omitted when `warehouse` is a discovery record.

- schema:

  Warehouse schema. Defaults to `"dbo"`; a table record can supply its
  schema when this argument is omitted.

- columns:

  Optional unique column names to project.

- limit:

  Optional non-negative maximum number of rows to return.

- result:

  Return a `"tibble"` for ordinary R analysis, or a single-use
  `"arrow_stream"` when a large result should be processed without first
  collecting it into an R data frame. The native Arrow path uses the
  ADBC backend

- backend:

  SQL connection backend, `"odbc"` or `"adbc"`.

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

- api_base:

  Fabric REST API base used when a Warehouse name or GUID must be
  discovered.

- allow_custom_endpoint:

  Logical. Fabric SQL and Microsoft SQL Database hostnames are trusted
  by default. Set to `TRUE` only when deliberately sending the SQL
  access token to another hostname, such as a controlled proxy or test
  server

- verbose:

  Whether to report SQL connection progress.

- timeout:

  Non-negative whole-number login/connect timeout in seconds; `0` lets
  the driver use an unlimited or driver-specific timeout

- max_tries:

  Maximum attempts after temporary Fabric SQL failures

- retry_delay:

  Initial delay in seconds before retrying. Later retries wait
  progressively longer, up to 60 seconds

## Value

A tibble, or a single-use `nanoarrow_array_stream` when
`result = "arrow_stream"`.

## Large results

Use `backend = "adbc"` with `result = "arrow_stream"` for a native Arrow
result path that can be consumed without first collecting the complete
table in an R data frame. The external ADBC `mssql` driver must be
installed.

`limit` uses T-SQL `TOP` and does not define row order. Use
[`fabric_sql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_query.md)
with an explicit `ORDER BY` when deterministic row selection matters.

## References

[Query a Fabric
Warehouse](https://learn.microsoft.com/en-us/fabric/data-warehouse/query-warehouse)

[Fabric Warehouse
connectivity](https://learn.microsoft.com/en-us/fabric/data-warehouse/connectivity)

## Examples

``` r
if (FALSE) { # \dontrun{
# Discover the Warehouse instead of copying its SQL connection details
workspace <- fabric_workspaces()[[1L]]
warehouse <- fabric_warehouses(workspace)[[1L]]

# Use DBI metadata to discover an existing table in that Warehouse
con <- fabric_sql_connect(warehouse)
tables <- DBI::dbListTables(con)
DBI::dbDisconnect(con)
table <- tables[[1L]]

# Read a bounded selection into a tibble
orders <- fabric_warehouse_read_table(
  warehouse,
  table,
  limit = 1000
)

# Keep a larger read out of R memory with an Arrow stream
stream <- fabric_warehouse_read_table(
  warehouse,
  table,
  backend = "adbc",
  result = "arrow_stream"
)
reader <- arrow::as_record_batch_reader(stream)
} # }
```
