# Query a Microsoft Fabric Eventhouse with KQL

Executes a read-only Kusto Query Language (KQL) query against a Fabric
Eventhouse query service and converts primary result tables to typed
tibbles.

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
  access_token = NULL,
  token_provider = NULL
)
```

## Arguments

- cluster:

  Character query-service/cluster URI, or one Eventhouse or KQLDatabase
  record returned by
  [`fabric_eventhouses()`](https://lukakoning.github.io/fabricQueryR/reference/fabric_typed_items.md),
  [`fabric_kql_databases()`](https://lukakoning.github.io/fabricQueryR/reference/fabric_typed_items.md),
  or
  [`fabric_item()`](https://lukakoning.github.io/fabricQueryR/reference/fabric_item.md).
  A KQLDatabase record also supplies `database`.

- query:

  A non-empty KQL query.

- database:

  KQL database display name. Required for a direct URI or an Eventhouse
  record; inferred from a KQLDatabase record.

- parameters:

  Named list of values for parameters declared by
  `declare query_parameters(...)` in `query`.

- request_properties:

  Named list of Kusto client request options, such as
  `servertimeout = "2m"` or `notruncation = TRUE`.

- timeout:

  Positive request timeout in seconds.

- tenant_id:

  Microsoft Entra tenant ID. Defaults to `FABRICQUERYR_TENANT_ID`.

- client_id:

  Microsoft Entra application/client ID. Defaults to
  `FABRICQUERYR_CLIENT_ID`, with the Azure CLI application ID as
  fallback.

- access_token:

  Optional Kusto bearer token. Supply only one of `access_token` and
  `token_provider`.

- token_provider:

  Optional callback returning a Kusto bearer token. It may accept
  `audience` and `force_refresh` arguments.

## Value

A typed tibble for one primary result, a `fabric_kql_tables` list for
multiple primary results, or an empty tibble when there is no primary
result.

## Details

This function uses the Kusto v2 REST query endpoint and requests a token
for `https://api.kusto.windows.net/.default`. The caller needs access to
the KQL database, normally through a Fabric workspace role or KQL
database sharing.

Query parameters are sent through Kusto client request properties, never
interpolated into `query`. Declare them in KQL with
`declare query_parameters(...)`. Scalar R values are encoded as Kusto
parameter values; vectors and lists are encoded as `dynamic(...)`
literals.

KQL `bool`, `datetime`, `int`, `long`, `real`, and `timespan` columns
become logical, UTC `POSIXct`, integer,
[`bit64::integer64`](https://bit64.r-lib.org/reference/bit64-package.html),
double, and `difftime` vectors. `dynamic` columns are list-columns,
GUIDs and strings are character vectors, and decimals are doubles.
Decimal values outside R's double precision should be converted to
strings in KQL when exact digits are needed.

A query with one primary result table returns a tibble. A query with
multiple primary result tables returns a named list of tibbles with
class `fabric_kql_tables`. Auxiliary protocol tables are validated but
not returned. A query with no primary table returns an empty tibble.
Management commands and ingestion endpoints are intentionally not
supported.

## Examples

``` r
if (FALSE) { # \dontrun{
database <- fabric_kql_databases("Telemetry workspace")[1, ]

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
