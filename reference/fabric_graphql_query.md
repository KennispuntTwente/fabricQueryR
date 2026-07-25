# Query a Microsoft Fabric API for GraphQL

Executes a GraphQL document against a Fabric GraphQL endpoint and
preserves `data`, `errors`, and `extensions` independently. This is
important because a successful HTTP response can contain both partial
data and GraphQL errors.

## Usage

``` r
fabric_graphql_query(
  api,
  query,
  variables = list(),
  operation_name = NULL,
  workspace_id = NULL,
  error_policy = c("return", "warn", "error"),
  timeout = 100,
  idempotent = FALSE,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  token = NULL,
  auth_args = list(),
  audience = .fabric_audience$graphql,
  api_base = .fabric_api_base
)
```

## Arguments

- api:

  GraphQL HTTPS endpoint, GraphQL API GUID, or one discovered GraphQLApi
  record.

- query:

  One non-empty GraphQL document.

- variables:

  Named list of GraphQL variables.

- operation_name:

  Optional operation name for a multi-operation document.

- workspace_id:

  Workspace GUID. Required when `api` is a GraphQL API GUID, and
  otherwise inferred from a discovered record.

- error_policy:

  How GraphQL-level errors are handled. `"return"` preserves them in the
  result, `"warn"` also emits a warning, and `"error"` raises a
  `fabric_graphql_error` carrying the result.

- timeout:

  Positive request timeout in seconds.

- idempotent:

  Logical. Allow shared transient HTTP retries for this POST.

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

  OAuth audience/scope passed to the credential.

- api_base:

  Fabric REST API base URL, used to derive endpoints from IDs.

## Value

A `fabric_graphql_result` list with `data`, `errors`, `extensions`, and
`response` (the complete parsed response).

## Details

A direct endpoint has the form
`https://api.fabric.microsoft.com/v1/workspaces/{workspace-id}/graphqlapis/{api-id}/graphql`.
You can instead pass a GraphQL API GUID with `workspace_id`, or one row
from
[`fabric_graphql_apis()`](https://lukakoning.github.io/fabricQueryR/reference/fabric_typed_items.md)
or
[`fabric_item()`](https://lukakoning.github.io/fabricQueryR/reference/fabric_item.md).

Interactive/delegated authentication requires the Power BI delegated
scope `GraphQLApi.Execute.All`, plus **Run Queries and Mutations**
permission on the API. Service principals are also supported by Fabric:
request a Fabric API token with `auth_args` or pass one through `token`,
enable service principals for Fabric APIs in the tenant, and grant the
principal API Execute access or a suitable workspace role. With SSO
connectivity, the caller also needs the required access to the
underlying data source. Saved-credential APIs use the configured
connection instead.

The default `audience` is the delegated GraphQL scope. Set it to
`https://api.fabric.microsoft.com/.default` when a custom provider
obtains service-principal tokens. The value is ignored for a static
token string.

GraphQL POST requests are not retried by default because a document can
contain mutations. Set `idempotent = TRUE` only when the operation is
safe to repeat.

## Examples

``` r
if (FALSE) { # \dontrun{
api <- fabric_graphql_apis("Analytics workspace")[1, ]

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
