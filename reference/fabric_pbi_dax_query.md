# Query a Microsoft Fabric/Power BI semantic model with DAX

Runs a DAX query against a published semantic model and returns the
result as a tibble. A semantic model is the report-ready data behind
Power BI reports, including tables, relationships, measures, and
business calculations

## Usage

``` r
fabric_pbi_dax_query(
  connstr = NULL,
  dax,
  workspace_id = NULL,
  dataset_id = NULL,
  my_workspace = FALSE,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  token = NULL,
  auth_args = list(),
  include_nulls = TRUE,
  api_base = "https://api.powerbi.com/v1.0/myorg",
  impersonated_user = NULL,
  api = c("json", "arrow"),
  result = c("tibble", "arrow_stream"),
  arrow_options = list(),
  timeout = 300
)
```

## Arguments

- connstr:

  Optional semantic model object from
  [`fabric_semantic_models()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md)
  or
  [`fabric_item()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_item.md),
  or a Power BI connection string. A character connection string can be,
  for example,
  `"Data Source=powerbi://api.powerbi.com/v1.0/myorg/Workspace;Initial Catalog=Dataset;"`
  It may contain `Data Source=` and `Initial Catalog=` parts, or a bare
  `powerbi://...` source plus a `Dataset=`, `Catalog=`, or
  `Initial Catalog=` key. Omit it when `dataset_id` is supplied

- dax:

  One DAX query, normally beginning with `EVALUATE`. DAX table
  expressions determine which rows and columns are returned

- workspace_id:

  Optional shared-workspace GUID. Use with `dataset_id` to avoid
  name-based discovery. For a model in My Workspace, omit this and set
  `my_workspace = TRUE` explicitly

- dataset_id:

  Optional semantic model/dataset GUID. When supplied, no
  connection-string name lookup is performed

- my_workspace:

  Whether `dataset_id` belongs to the signed-in user's My Workspace.
  Leave `FALSE` for shared workspaces

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

- include_nulls:

  Logical. With `TRUE`, Power BI includes properties whose value is
  blank/null. With `FALSE`, those properties can be absent from a
  returned row; retaining `TRUE` usually gives a more consistent tibble
  Used only by `api = "json"`; Arrow has a schema and always represents
  nulls explicitly

- api_base:

  Power BI REST API base URL. The default
  `"https://api.powerbi.com/v1.0/myorg"` is correct for the commercial
  cloud; override it only for a test service that implements the same
  endpoint and authentication contract. Sovereign Microsoft clouds are
  not currently supported by this helper

- impersonated_user:

  Optional user principal name, such as `"analyst@example.com"`, sent as
  `impersonatedUserName` for supported JSON row-level-security scenarios
  or as `effectiveUsername` for Arrow Leave `NULL` for the normal
  identity context

- api:

  Response format provided by Power BI. Use `"json"` for ordinary
  queries or `"arrow"` for richer types and multiple result tables

- result:

  Return a `"tibble"`, or with `api = "arrow"`, a single-use
  `"arrow_stream"` for batch processing without first collecting all
  rows in R memory

- arrow_options:

  Named list of optional `executeDaxQueries` request properties.
  Supported names are `applicationContext`, `culture`, `customData`,
  `effectiveUsername`, `executionMetrics`, `memoryLimit`,
  `queryTimeout`, `resultSetRowCountLimit`, `roles`, and `schemaOnly`.
  The required `query` property is supplied from `dax`. Used only by
  `api = "arrow"`

- timeout:

  Positive finite client-side timeout in seconds for the DAX execution
  HTTP request. This is distinct from the Arrow API's server-side
  `arrow_options$queryTimeout` property

## Value

A tibble for one result table. Multiple Arrow result tables are returned
as a `fabric_pbi_dax_rowsets` list of tibbles or Arrow streams Power BI
column names are preserved. JSON result tables with no rows have no
column metadata and return a zero-row, zero-column tibble. Arrow results
preserve the column schema even when there are no rows. Missing results
and service-reported errors or truncation raise an error. Arrow results
respect `arrow_options$resultSetRowCountLimit`; the service default is
1,000,000 rows. Intentional row limits do not raise an error. Reaching
the cap does not establish whether more rows exist. For complete
extraction, verify expected row counts or query bounded partitions

## Choosing a model

The easiest input is an item from
[`fabric_semantic_models()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md).
You can instead supply workspace and dataset IDs, or a Power BI
connection string copied from the semantic model settings. IDs are the
most reliable choice for scheduled code. For a model in My Workspace,
supply `dataset_id` and set `my_workspace = TRUE`

## Choosing a response format

Keep `api = "json"` for ordinary queries and broad compatibility. It
returns one result table and is available to Pro, PPU, and
capacity-backed models Results are limited by Power BI; 'fabricQueryR'
raises an error instead of silently returning a partial result. Very
large whole numbers are returned as character values so they are not
rounded

Use `api = "arrow"` when exact semantic-model types matter, when a query
has several `EVALUATE` statements, or when you want an Arrow stream. It
requires the optional 'arrow' package and a model on Premium or Fabric
capacity. Decimal128 and Decimal256 columns are returned as exact
character values in a tibble. Power BI Variant columns are returned as
list-columns whose cells contain `type` and `value` fields and inherit
from `fabric_pbi_variant`, so mixed scalar types remain distinguishable.
Variant Currency values are exact character scalars. Variant whole
numbers are
[`bit64::integer64`](https://bit64.r-lib.org/reference/bit64-package.html)
scalars, except the minimum signed 64-bit value, which is character
because 'bit64' reserves that bit pattern for missing values.
`result = "arrow_stream"` retains native Arrow decimal and dense-union
types. The Power BI administrator must enable both **Dataset Execute
Queries REST API** under Developer settings and **Allow XMLA endpoints
and Analyze in Excel with on-premises semantic models** under
Integration settings. Multiple result tables are returned in statement
order as a `fabric_pbi_dax_rowsets` list

## Permissions and tenant settings

The signed-in identity needs Read and Build permission on the semantic
model Your Power BI administrator must enable **Dataset Execute Queries
REST API**; service principals also need the relevant service-principal
tenant setting. The Arrow endpoint has the additional XMLA tenant
setting and capacity prerequisites described above The APIs use the
Power BI scope and require `Dataset.Read.All` (or
`Dataset.ReadWrite.All`). Name lookup also requires workspace read
access Row-level security, SSO, user impersonation, and the Arrow
endpoint have additional Power BI restrictions; see the linked Microsoft
documentation

## References

[Power BI JSON Execute Queries REST
API](https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/execute-queries-in-group)

[Power BI Arrow Execute DAX Queries REST
API](https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/execute-dax-queries-in-group)

[Power BI Arrow API overview and capacity
requirements](https://learn.microsoft.com/en-us/power-bi/developer/execute-dax-queries-arrow/overview)

[Semantic model
permissions](https://learn.microsoft.com/en-us/power-bi/connect-data/service-datasets-permissions)

[Semantic Model Execute Queries tenant
setting](https://learn.microsoft.com/en-us/fabric/admin/service-admin-portal-integration#semantic-model-execute-queries-rest-api)

## Examples

``` r
if (FALSE) { # \dontrun{
# Discover the semantic model instead of copying workspace and model IDs
workspace <- fabric_workspaces()[[1L]]
model <- fabric_semantic_models(workspace)[[1L]]

# Supply a query tested in the model's DAX query view
dax <- Sys.getenv("FABRIC_DAX_QUERY")

# Evaluate the DAX query and collect the result as a tibble
df <- fabric_pbi_dax_query(
  model,
  dax = dax
)
dplyr::glimpse(df)

# Keep a larger result out of R memory with an Arrow stream
stream <- fabric_pbi_dax_query(
  model,
  dax = dax,
  api = "arrow",
  result = "arrow_stream"
)
reader <- arrow::as_record_batch_reader(stream)
} # }
```
