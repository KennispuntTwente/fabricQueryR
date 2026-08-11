# Paginate a Microsoft Fabric GraphQL operation

Repeats
[`fabric_graphql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_graphql_query.md)
while `next_cursor` returns a cursor. The callback tells the function
where the current page stores its next cursor, because that location
depends on the API schema.

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

- next_cursor:

  Function accepting a `fabric_graphql_result` and returning the next
  opaque cursor, or `NULL` when pagination is complete. Use
  [`fabric_graphql_cursor()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_graphql_cursor.md)
  for Fabric's normal connection fields.

- variables:

  Named list of GraphQL variables. Values must be representable as JSON;
  names must match variables declared in `query`. Scalar vectors are
  encoded as JSON scalars when they have length one. Wrap a one-element
  GraphQL list in [`I()`](https://rdrr.io/r/base/AsIs.html), for example
  `list(ids = I("x"))`, to preserve its JSON array shape. Vectors with
  two or more elements are arrays normally.

- cursor_variable:

  Name of the GraphQL variable that receives the next cursor, commonly
  `"after"`. It must match the variable declared in `query`.

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

- max_pages:

  Positive maximum number of requests. This guards against a faulty or
  unexpectedly large pagination loop.

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

A `fabric_graphql_pages` list with `pages`, combined `errors`, and the
final `variables`. `pages` contains one `fabric_graphql_result` per
request and `complete` is `TRUE` when the callback reported no next
page. Results are kept page-by-page because the requested schema shape
can vary.

## Examples

``` r
if (FALSE) { # \dontrun{
api <- fabric_graphql_apis("Analytics workspace")[[1]]

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
