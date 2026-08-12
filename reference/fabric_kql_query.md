# Run a KQL query in Microsoft Fabric

Runs a read-only query against a KQL database and returns the result as
a tibble. KQL databases are commonly used for event, log, telemetry, and
time-series data in a Fabric Eventhouse

## Usage

``` r
fabric_kql_query(
  cluster,
  query,
  database = NULL,
  parameters = list(),
  request_properties = list(),
  timeout = 60,
  retain_raw_frames = FALSE,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  token = NULL,
  auth_args = list(),
  allow_custom_endpoint = FALSE
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

- query:

  One non-empty, read-only KQL query, for example
  `"Events | where Severity == 'Error' | take 100"`

- database:

  KQL database display name. Supply it with a copied Query URI or an
  Eventhouse record; omit it when `cluster` is a KQLDatabase record

- parameters:

  Named list of values declared with `declare query_parameters(...)` in
  `query`

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

- allow_custom_endpoint:

  Logical. Permit a non-Microsoft Kusto HTTPS origin. Keep `FALSE`
  unless the endpoint is trusted; credentials are sent to the supplied
  origin

## Value

A typed tibble for one primary result, a `fabric_kql_tables` list for
multiple primary results (one named element per table), or an empty
tibble when there is no primary result. See Details for the KQL-to-R
type mapping

## Basic use

The easiest input is an item from
[`fabric_kql_databases()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md),
which already contains the database name and its **Query URI**. If you
copy a URI from Fabric, choose **Query URI**, not **Ingestion URI**.
This function reads existing data; it does not load data or run
management commands

Put changing values in `parameters` and declare them in KQL with
`declare query_parameters(...)`. The values are sent separately from the
query text, which is safer and easier to quote correctly than using
[`paste()`](https://rdrr.io/r/base/paste.html). Scalar R values become
KQL scalar values; vectors and lists become `dynamic` arrays or objects

## Advanced request options

`request_properties` controls server behavior such as timeouts and
result truncation. Most users can leave it empty Microsoft Fabric does
not support the `queryconsistency` or `query_weakconsistency_session_id`
request properties. Do not include either name in `request_properties`,
even though Azure Data Explorer supports them

## Result types

KQL `bool`, `datetime`, `int`, `long`, `real`, and `timespan` columns
normally become logical, UTC `POSIXct`, integer,
[`bit64::integer64`](https://bit64.r-lib.org/reference/bit64-package.html),
double, and `difftime` vectors. Base R and `bit64` reserve the minimum
signed `int` and `long` values for missing data; a column containing
either boundary is returned as character with a warning so the value
remains exact. `dynamic` columns are list-columns, and GUIDs, strings,
and `decimal` values are character vectors. Keeping decimal values in
their original lexical form avoids the silent precision loss that
conversion to an R double can cause

A query with several result tables returns a named `fabric_kql_tables`
list; a query with no result table returns an empty tibble. Service
metadata is retained in `kusto_*` attributes for troubleshooting

## Permissions

The caller needs database access through a Fabric workspace role,
Eventhouse sharing, or KQL database sharing. Authentication uses the
Kusto query service

## References

[Access a KQL database and copy its Query
URI](https://learn.microsoft.com/en-us/fabric/real-time-intelligence/access-database-copy-uri)

[Kusto query HTTP request and
parameters](https://learn.microsoft.com/en-us/kusto/api/rest/request?view=microsoft-fabric)

[Kusto request
properties](https://learn.microsoft.com/en-us/kusto/api/rest/request-properties?view=microsoft-fabric)

[Kusto role-based access
control](https://learn.microsoft.com/en-us/kusto/access-control/role-based-access-control?view=microsoft-fabric)

## Examples

``` r
if (FALSE) { # \dontrun{
database <- fabric_kql_databases("Telemetry workspace")[[1]]

events <- fabric_kql_query(
  database,
  query = paste(
    "declare query_parameters(selected_type:string);",
    "Events | where EventType == selected_type | take 100"
  ),
  parameters = list(selected_type = "Warning")
)
} # }
```
