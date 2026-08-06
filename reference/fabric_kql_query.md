# Query a Microsoft Fabric Eventhouse with KQL

Runs a read-only Kusto Query Language (KQL) query against a KQL database
and converts the result to R objects. In Fabric, an Eventhouse is a
container for one or more KQL databases designed for fast analysis of
event, log, telemetry, and time-series data.

## Usage

``` r
fabric_kql_query(
  cluster,
  query,
  database = NULL,
  parameters = list(),
  request_properties = list(),
  timeout = 60,
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

  Query-service URI, or one Eventhouse or KQLDatabase record returned by
  [`fabric_eventhouses()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md),
  [`fabric_kql_databases()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md),
  or
  [`fabric_item()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_item.md).
  A KQLDatabase record also supplies `database`. Despite the argument
  name, use Fabric's **Query URI** here.

- query:

  One non-empty, read-only KQL query, for example
  `"Events | where Severity == 'Error' | take 100"`.

- database:

  KQL database display name. Supply it with a copied Query URI or an
  Eventhouse record; omit it when `cluster` is a KQLDatabase record.

- parameters:

  Named list of values for parameters declared by
  `declare query_parameters(...)` in `query`. Scalar R values become
  Kusto scalar values; vectors and lists become `dynamic` values.
  Binding is safer and easier to quote correctly than building KQL with
  [`paste()`](https://rdrr.io/r/base/paste.html).

- request_properties:

  Named list of Kusto client request options, such as
  `servertimeout = "2m"` or `notruncation = TRUE`. Most users can leave
  this empty; these are server-side Kusto controls, not query
  parameters.

- timeout:

  Positive client-side HTTP timeout in seconds. This is separate from
  the Kusto `servertimeout` request property.

- tenant_id:

  Microsoft Entra tenant ID. Defaults to `FABRICQUERYR_TENANT_ID`.

- client_id:

  Microsoft Entra application/client ID. Defaults to
  `FABRICQUERYR_CLIENT_ID`, with the Azure CLI application ID as
  fallback.

- token:

  Optional
  [`AzureAuth::AzureToken`](https://rdrr.io/pkg/AzureAuth/man/AzureToken.html),
  bearer-token string, or token-provider function. With `NULL`,
  `AzureAuth` reuses a matching cached token or starts its normal
  interactive login flow.

- auth_args:

  Named list of additional arguments passed to
  [`AzureAuth::get_azure_token()`](https://rdrr.io/pkg/AzureAuth/man/get_azure_token.html)
  when no token source is supplied.

- allow_custom_endpoint:

  Logical. Permit a non-Microsoft Kusto HTTPS origin. Keep `FALSE`
  unless the endpoint is trusted; credentials are sent to the supplied
  origin.

## Value

A typed tibble for one primary result, a `fabric_kql_tables` list for
multiple primary results (one named element per table), or an empty
tibble when there is no primary result. See Details for the KQL-to-R
type mapping.

## Details

The easiest input is an item from
[`fabric_kql_databases()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md),
because it supplies both the database name and its **Query URI**. If
copying a URI from Fabric, use **Query URI**, not **Ingestion URI**;
this function queries existing data and does not load new data. The
caller needs database access through a Fabric workspace role, Eventhouse
sharing, or KQL database sharing.

This function uses the Kusto v2 REST query endpoint and requests a token
for `https://api.kusto.windows.net/.default`.

Query parameters are sent through Kusto client request properties, never
interpolated into `query`. Declare them in KQL with
`declare query_parameters(...)`. Scalar R values are encoded as Kusto
parameter values; vectors and lists are encoded as `dynamic(...)`
literals. Zero-length vectors and unnamed lists encode `dynamic([])`. A
zero-length list with non-`NULL` names encodes `dynamic({})`; `NULL`
remains invalid so it cannot be confused with an empty collection or a
typed Kusto null.

KQL `bool`, `datetime`, `int`, `long`, `real`, and `timespan` columns
normally become logical, UTC `POSIXct`, integer,
[`bit64::integer64`](https://bit64.r-lib.org/reference/bit64-package.html),
double, and `difftime` vectors. Base R and `bit64` reserve the minimum
signed `int` and `long` values for missing data; a column containing
either boundary is returned as character with a warning so the value
remains exact. `dynamic` columns are list-columns, GUIDs and strings are
character vectors, and decimals are doubles. Decimal values outside R's
double precision should be converted to strings in KQL when exact digits
are needed.

A query with one primary result table returns a tibble. A query with
multiple primary result tables returns a named list of tibbles with
class `fabric_kql_tables`. Auxiliary protocol tables are validated but
not returned. A query with no primary table returns an empty tibble.
Management commands and ingestion endpoints are intentionally not
supported.

## References

[Access a KQL database and copy its Query
URI](https://learn.microsoft.com/en-us/fabric/real-time-intelligence/access-database-copy-uri)

[Kusto query HTTP request and
parameters](https://learn.microsoft.com/en-us/kusto/api/rest/request?view=microsoft-fabric)

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
