# Query a Microsoft Fabric API for GraphQL

Sends a GraphQL query or mutation to an API for GraphQL item configured
in Fabric. Fabric generates the API schema from selected Lakehouse,
Warehouse, or SQL Database objects; this function calls that API from R
and returns its nested response.

## Usage

``` r
fabric_graphql_query(
  api,
  query,
  variables = list(),
  operation_name = NULL,
  workspace_id = NULL,
  error_policy = c("return", "warn", "error"),
  timeout = 110,
  idempotent = FALSE,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  token = NULL,
  auth_args = list(),
  audience = NULL,
  api_base = .fabric_api_base,
  allow_custom_endpoint = FALSE
)
```

## Arguments

- api:

  GraphQL HTTPS endpoint, GraphQL API GUID, or one discovered GraphQLApi
  record. An item from
  [`fabric_graphql_apis()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md)
  is usually easiest because it supplies the endpoint and workspace ID.

- query:

  One GraphQL document containing a query or mutation. Use variables for
  changing values instead of pasting values into this string.

- variables:

  Named list of GraphQL variables. Values must be representable as JSON;
  names must match variables declared in `query`.

- operation_name:

  Optional operation name. Supply it when the document contains more
  than one named operation; otherwise leave `NULL`.

- workspace_id:

  Workspace GUID. Required when `api` is a GraphQL API GUID, and
  otherwise inferred from a discovered record.

- error_policy:

  How GraphQL-level errors are handled. `"return"` lets the caller
  inspect partial data and errors; `"warn"` also makes errors visible
  immediately; `"error"` stops and attaches the result to a
  `fabric_graphql_error`. HTTP/authentication failures always stop.

- timeout:

  Positive wall-clock HTTP timeout in seconds. The 110-second default
  leaves transfer overhead beyond Fabric's 100-second server execution
  limit, allowing the service's own timeout response to arrive.

- idempotent:

  Logical. Permit retries after transient HTTP failures. `TRUE` is
  normally suitable for a read-only query, but not for a mutation that
  could be applied twice.

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

- audience:

  OAuth audience/scope passed to the credential. `NULL` selects the
  documented scope from the authentication flow. Set this only for a
  custom token provider or unusual identity flow.

- api_base:

  Fabric REST API base URL used to derive endpoints from IDs. Most users
  should keep the default.

- allow_custom_endpoint:

  Logical. Permit a GraphQL endpoint outside the Microsoft Fabric API
  origin. Keep `FALSE` unless the origin is trusted; credentials are
  sent to the supplied endpoint.

## Value

A `fabric_graphql_result` list with `data`, `errors`, `extensions`, and
`response` (the complete parsed response). `data` follows the nested
shape requested in the GraphQL document and is usually a combination of
named lists and vectors, not a tibble. Because GraphQL can return
partial data, inspect `errors` even when `data` is present.

## Details

Before using this function, create an **API for GraphQL** item in a
Fabric workspace, connect its data source, and choose which tables,
fields, queries, and mutations the API exposes. Fabric's built-in
GraphQL editor and schema explorer are the easiest places to design and
test a document before copying it to R.

A direct endpoint has the form
`https://api.fabric.microsoft.com/v1/workspaces/{workspace-id}/graphqlapis/{api-id}/graphql`.
You can instead pass a GraphQL API GUID with `workspace_id`, or one item
from
[`fabric_graphql_apis()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md)
or
[`fabric_item()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_item.md).

Interactive/delegated authentication requires the Power BI delegated
scope `GraphQLApi.Execute.All`, plus **Run Queries and Mutations**
permission on the API. Service principals are also supported by Fabric:
request a Fabric API token with `auth_args` or pass one through `token`,
enable service principals for Fabric APIs in the tenant, and grant the
principal API Execute access or a suitable workspace role. With SSO
connectivity, the caller also needs the required access to the
underlying data source. Saved-credential APIs use the configured
connection instead.

When `audience = NULL`, the package selects the Fabric API scope for an
AzureAuth client-credentials flow and the delegated GraphQL scope
otherwise. Set `audience` explicitly when a custom token provider uses a
service-principal flow. The value is ignored for a static token string.

GraphQL POST requests are not retried by default because a document can
contain mutations. Set `idempotent = TRUE` only when the operation is
safe to repeat.

Fabric returns at most 100 items by default and permits at most 100,000
items across pagination. Each response is limited to 64 MB, each request
to 100 seconds, and query nesting to 10 levels. Use smaller pages and
filtered query partitions when a result could approach these service
limits.

JSON integers outside R's exact double-precision range are returned as
character values so identifiers and other large integer fields are not
rounded.

## References

[Fabric API for GraphQL
editor](https://learn.microsoft.com/en-us/fabric/data-engineering/api-graphql-editor)

[Fabric GraphQL schema
explorer](https://learn.microsoft.com/en-us/fabric/data-engineering/graphql-schema-view)

[Use service principals with Fabric API for
GraphQL](https://learn.microsoft.com/en-us/fabric/data-engineering/api-graphql-service-principal)

[Fabric API for GraphQL
limits](https://learn.microsoft.com/en-us/fabric/data-engineering/api-graphql-limits)

## Examples

``` r
if (FALSE) { # \dontrun{
api <- fabric_graphql_apis("Analytics workspace")[[1]]

result <- fabric_graphql_query(
  api,
  query = paste(
    "query Products($category: String!) {",
    "  products(filter: {category: {eq: $category}}) {",
    "    items { id name category }",
    "  }",
    "}"
  ),
  variables = list(category = "A"),
  operation_name = "Products"
)
result$data$products$items
result$errors
} # }
```
