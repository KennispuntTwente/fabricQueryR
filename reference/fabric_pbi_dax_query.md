# Query a Microsoft Fabric/Power BI semantic model with DAX

Runs a Data Analysis Expressions (DAX) query against a published
semantic model and returns its result as a tibble. A semantic model is
the report-ready layer behind Power BI reports: it contains tables,
relationships, measures, and business calculations. Use DAX here, rather
than SQL intended for the underlying Lakehouse or Warehouse.

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
  allow_custom_endpoint = FALSE,
  impersonated_user = NULL,
  api = c("json", "arrow"),
  result = c("tibble", "arrow_stream"),
  arrow_options = list()
)
```

## Arguments

- connstr:

  Optional Power BI connection string or one SemanticModel record
  returned by
  [`fabric_semantic_models()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md)
  or
  [`fabric_item()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_item.md).
  For a discovered record, workspace and dataset IDs are used directly.
  A character connection string can be, for example,
  `"Data Source=powerbi://api.powerbi.com/v1.0/myorg/Workspace;Initial Catalog=Dataset;"`.
  It may contain `Data Source=` and `Initial Catalog=` parts, or a bare
  `powerbi://...` source plus a `Dataset=`, `Catalog=`, or
  `Initial Catalog=` key. Omit it when `dataset_id` is supplied.

- dax:

  One DAX query, normally beginning with `EVALUATE`. DAX table
  expressions determine which rows and columns are returned.

- workspace_id:

  Optional shared-workspace GUID. Use with `dataset_id` to avoid
  name-based discovery. For a model in My Workspace, omit this and set
  `my_workspace = TRUE` explicitly.

- dataset_id:

  Optional semantic model/dataset GUID. When supplied, no
  connection-string name lookup is performed.

- my_workspace:

  Logical. Confirm that `dataset_id` belongs to the signed-in user's My
  Workspace and use Power BI's unscoped dataset endpoint. This explicit
  opt-in prevents accidentally using that route for a shared model.

- tenant_id:

  Microsoft Entra tenant ID. Defaults to `FABRICQUERYR_TENANT_ID`.

- client_id:

  Microsoft Entra application/client ID. Defaults to
  `FABRICQUERYR_CLIENT_ID`, then the Azure CLI application ID.

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

- include_nulls:

  Logical. With `TRUE`, Power BI includes properties whose value is
  blank/null. With `FALSE`, those properties can be absent from a
  returned row; retaining `TRUE` usually gives a more consistent tibble.
  Used only by `api = "json"`; Arrow has a schema and always represents
  nulls explicitly.

- api_base:

  Power BI REST API base URL. The default
  `"https://api.powerbi.com/v1.0/myorg"` is correct for the commercial
  cloud; override it only for a test service that implements the same
  endpoint and authentication contract. Sovereign Microsoft clouds are
  not currently supported by this helper.

- allow_custom_endpoint:

  Logical. Set to `TRUE` only when `api_base` is a non-Microsoft HTTPS
  origin that you trust to receive a Power BI token.

- impersonated_user:

  Optional user principal name, such as `"analyst@example.com"`, sent as
  `impersonatedUserName` for supported JSON row-level-security scenarios
  or as `effectiveUsername` for Arrow. Leave `NULL` for the normal
  identity context.

- api:

  Response API. `"json"` uses `executeQueries`; `"arrow"` uses
  `executeDaxQueries`.

- result:

  Return format. `"tibble"` collects the result in R memory. With
  `api = "arrow"`, `"arrow_stream"` returns a `nanoarrow_array_stream`
  compatible with
  [`arrow::as_record_batch_reader()`](https://arrow.apache.org/docs/r/reference/as_record_batch_reader.html)
  and other Arrow C stream consumers. The HTTP response is streamed to a
  temporary file and record batches remain file-backed and lazy. The
  response is scanned one batch at a time first so concatenated data and
  error rowsets can be validated without collecting the data table.
  Dictionary-encoded columns remain native Arrow dictionaries in this
  mode; their R representation is chosen by the eventual consumer.

- arrow_options:

  Named list of optional `executeDaxQueries` request properties.
  Supported names are `applicationContext`, `culture`, `customData`,
  `effectiveUsername`, `executionMetrics`, `memoryLimit`,
  `queryTimeout`, `resultSetRowCountLimit`, `roles`, and `schemaOnly`.
  The required `query` property is supplied from `dax`. Used only by
  `api = "arrow"`.

## Value

With `result = "tibble"`, a tibble containing a single result table.
Multiple Arrow data rowsets are returned as a `fabric_pbi_dax_rowsets`
list of tibbles. With `api = "arrow", result = "arrow_stream"`, the same
rule applies to `nanoarrow_array_stream` objects. Power BI's column
names are preserved. An empty Arrow result becomes a typed zero-row
result. Because the JSON API does not provide column metadata for an
empty table, that path returns a zero-row, zero-column tibble. When
requested, Arrow execution metrics are attached as an
`execution_metrics` attribute. API errors and partial/truncated JSON
results raise an error rather than silently returning incomplete data.

## Details

- The easiest input is an item from
  [`fabric_semantic_models()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md).
  You can instead supply `workspace_id` and `dataset_id` (both GUIDs),
  or a Power BI connection string containing the workspace and
  semantic-model names. IDs avoid name lookup and are best for scheduled
  code.

- Personal workspaces use the current v2 XMLA form:
  `powerbi://api.powerbi.com/v2.0/{tenantId}/home/myworkspace/{owner}`,
  where `owner` is a URI-encoded UPN or object ID. Shared workspaces
  continue to use the v1 URL shown below.

- In Fabric/Power BI, open the semantic model's settings to find its
  server or XMLA connection information. The signed-in identity needs
  Read and Build permission on the semantic model, either through its
  workspace role or through **Manage permissions** on the model.

- AzureAuth is used to acquire the token. Be wary of caching behavior;
  you may want to call
  [`AzureAuth::clean_token_directory()`](https://rdrr.io/pkg/AzureAuth/man/get_azure_token.html)
  to clear cached tokens if the wrong account or tenant is being reused.

- Requests use the Power BI audience
  `https://analysis.windows.net/powerbi/api/.default` and require
  `Dataset.Read.All` (or `Dataset.ReadWrite.All`) plus dataset Read and
  Build permissions. Name lookup also requires `Workspace.Read.All` or
  equivalent.

- Both APIs require the Power BI tenant setting **Dataset Execute
  Queries REST API**. Service-principal authentication also requires
  **Allow service principals to use Power BI APIs**. The limitations
  differ by endpoint, as described below.

- Set `api = "json"` (the default) for the established `executeQueries`
  endpoint. It accepts one DAX query and one result table per request.
  Results are limited to 100,000 rows or 1,000,000 values (whichever is
  reached first), 15 MB, and 120 requests per minute per user. Partial
  results reported by Power BI are treated as errors by this function.
  JSON does not carry the full semantic-model type system. Whole Number
  values outside R's exactly representable numeric range are therefore
  returned as character strings rather than silently rounded. Use the
  Arrow endpoint when exact integer, fixed-decimal, currency, or date
  types matter. Service principals cannot use this JSON endpoint with
  models that have RLS or SSO enabled. Delegated users can supply
  `impersonated_user` for supported RLS scenarios.

- Set `api = "arrow"` for the newer `executeDaxQueries` endpoint. It
  preserves Arrow column types, raises errors carried in HTTP 200 Arrow
  error rowsets, and supports the additional documented request
  properties through `arrow_options`. The optional arrow package is
  required because Power BI compresses record batches with LZ4. This
  endpoint supports semantic models on Power BI's modern service
  infrastructure; deprecated Push models, legacy compatibility-level
  models, monitoring/usage models, and live connections to Analysis
  Services are excluded. The Arrow endpoint requires Premium or Fabric
  capacity. Pro and PPU models can use the JSON endpoint but do not
  satisfy the Arrow endpoint's capacity requirement. `effectiveUsername`
  is user-only and requires workspace admin. Users may specify only
  roles they belong to unless they are workspace admins; service
  principals may use `roles` only when they are workspace admins.
  **Allow XMLA endpoints and Analyze in Excel with on-premises semantic
  models** must also be enabled.

- Arrow queries may contain multiple `EVALUATE` statements. When Power
  BI returns multiple data rowsets, this helper returns them in
  statement order as a `fabric_pbi_dax_rowsets` list.

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
# Example is not executed since it requires configured credentials for Fabric
if (FALSE) { # \dontrun{
conn <- "Data Source=powerbi://api.powerbi.com/v1.0/myorg/Sales Workspace;Initial Catalog=SalesModel;"
df <- fabric_pbi_dax_query(
  connstr = conn,
  dax = "EVALUATE TOPN(1000, 'Customers')",
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID")
)
dplyr::glimpse(df)

# Arrow IPC endpoint, returned through the Arrow C stream interface
stream <- fabric_pbi_dax_query(
  connstr = conn,
  dax = "EVALUATE TOPN(1000, 'Customers')",
  api = "arrow",
  result = "arrow_stream"
)
reader <- arrow::as_record_batch_reader(stream)
} # }
```
