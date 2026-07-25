# Paginate a Microsoft Fabric GraphQL operation

Repeats
[`fabric_graphql_query()`](https://lukakoning.github.io/fabricQueryR/reference/fabric_graphql_query.md)
while `next_cursor` returns a cursor. The callback makes pagination
independent of the user's schema shape.

## Usage

``` r
fabric_graphql_paginate(
  api,
  query,
  next_cursor,
  variables = list(),
  cursor_variable = "after",
  operation_name = NULL,
  workspace_id = NULL,
  error_policy = c("return", "warn", "error"),
  max_pages = 100L,
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

- next_cursor:

  Function accepting a `fabric_graphql_result` and returning the next
  opaque cursor, or `NULL` when pagination is complete. Use
  [`fabric_graphql_cursor()`](https://lukakoning.github.io/fabricQueryR/reference/fabric_graphql_cursor.md)
  for Fabric's normal connection fields.

- variables:

  Named list of GraphQL variables.

- cursor_variable:

  Name of the GraphQL variable that receives the cursor.

- operation_name:

  Optional operation name for a multi-operation document.

- workspace_id:

  Workspace GUID. Required when `api` is a GraphQL API GUID, and
  otherwise inferred from a discovered record.

- error_policy:

  How GraphQL-level errors are handled. `"return"` preserves them in the
  result, `"warn"` also emits a warning, and `"error"` raises a
  `fabric_graphql_error` carrying the result.

- max_pages:

  Positive maximum number of requests.

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

A `fabric_graphql_pages` list with `pages`, combined `errors`, and the
final `variables`.

## Examples

``` r
if (FALSE) { # \dontrun{
pages <- fabric_graphql_paginate(
  api,
  query = paste(
    "query Products($first: Int!, $after: String) {",
    "  products(first: $first, after: $after) {",
    "    items { id name } hasNextPage endCursor",
    "  }",
    "}"
  ),
  variables = list(first = 100L, after = NULL),
  next_cursor = fabric_graphql_cursor("products")
)
} # }
```
