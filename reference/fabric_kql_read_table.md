# Read a Microsoft Fabric KQL table

Provides the table-oriented read counterpart to
[`fabric_kql_write_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_kql_write_table.md).
It safely resolves the table through Kusto's
[`table()`](https://rdrr.io/r/base/table.html) function, optionally
projects columns and limits rows, then delegates typed result handling
to
[`fabric_kql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_kql_query.md).
Use that lower-level function for filters, ordering, joins,
aggregations, or other KQL expressions.

## Usage

``` r
fabric_kql_read_table(
  cluster,
  table,
  database = NULL,
  columns = NULL,
  limit = NULL,
  request_properties = list(),
  timeout = 60,
  retain_raw_frames = FALSE,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  token = NULL,
  auth_args = list()
)
```

## Arguments

- cluster:

  Query URI, or one Eventhouse or KQLDatabase record returned by
  [`fabric_eventhouses()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md),
  [`fabric_kql_databases()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md),
  or
  [`fabric_item()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_item.md).
  A KQLDatabase record also supplies `database`. Despite the argument
  name, use Fabric's **Query URI** here

- table:

  KQL table name, or a record containing a `name`, `table`, or
  `displayName` field.

- database:

  KQL database display name. Supply it with a copied Query URI or an
  Eventhouse record; omit it when `cluster` is a KQLDatabase record

- columns:

  Optional unique column names to project.

- limit:

  Optional non-negative whole-number maximum number of rows to return,
  no greater than Kusto's signed 32-bit `take` limit.

- request_properties:

  Named list of Kusto client request options, such as
  `servertimeout = "2m"` or `notruncation = TRUE`. Most users can leave
  this empty; these are server-side Kusto controls, not query
  parameters. Fabric does not support `queryconsistency` or
  `query_weakconsistency_session_id`

- timeout:

  Positive client-side HTTP timeout in seconds. This is separate from
  the Kusto `servertimeout` request property

- retain_raw_frames:

  Logical. Attach the complete decoded Kusto frame response as
  `kusto_raw_frames`. Keep `FALSE` for normal queries to avoid retaining
  a second copy of large result data

- tenant_id:

  Microsoft Entra tenant ID. Defaults to `FABRICQUERYR_TENANT_ID`

- client_id:

  Microsoft Entra application/client ID. Defaults to
  `FABRICQUERYR_CLIENT_ID`, with the Azure CLI application ID as
  fallback

- token:

  Optional access token or token-provider function. Leave `NULL` to let
  fabricQueryR use its normal sign-in flow

- auth_args:

  Additional sign-in options passed to
  [`AzureAuth::get_azure_token()`](https://rdrr.io/pkg/AzureAuth/man/get_azure_token.html)

## Value

A typed tibble containing the selected table rows. Kusto metadata is
retained in the same attributes as
[`fabric_kql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_kql_query.md).

## Large results

The Kusto query HTTP response is collected and decoded in R. Use
`columns` and `limit` to bound an interactive read. For a result too
large for client memory, use
[`fabric_kql_export()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_kql_export.md)
to export it server-side to OneLake or another supported storage
destination.

## References

[Kusto `table()`
function](https://learn.microsoft.com/en-us/kusto/query/table-function?view=microsoft-fabric)

[Kusto `take`
operator](https://learn.microsoft.com/en-us/kusto/query/take-operator?view=microsoft-fabric)

[Kusto entity
names](https://learn.microsoft.com/en-us/kusto/query/schema-entities/entity-names?view=microsoft-fabric)

[Kusto query HTTP
request](https://learn.microsoft.com/en-us/kusto/api/rest/request?view=microsoft-fabric)

## Examples

``` r
if (FALSE) { # \dontrun{
# Discover a KQL database instead of copying its Query URI and name
workspace <- fabric_workspaces()[[1L]]
database <- fabric_kql_databases(workspace)[[1L]]

# Choose an existing table shown under Tables in the Fabric KQL explorer
table <- Sys.getenv("FABRIC_KQL_TABLE")

# Read a bounded portion of that table into a tibble
events <- fabric_kql_read_table(
  database,
  table,
  limit = 1000
)
} # }
```
