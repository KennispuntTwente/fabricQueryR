# Read all pages from a Fabric GraphQL query

Repeats
[`fabric_graphql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_graphql_query.md)
until the API reports that no more pages are available. Because every
GraphQL schema can store pagination information in a different place,
`next_cursor` tells the function where to find it

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
  api_base = .fabric_api_base
)
```

## Arguments

- api:

  GraphQL endpoint, API ID, or one discovered GraphQLApi record. An item
  from
  [`fabric_graphql_apis()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md)
  is usually easiest because it supplies the endpoint and workspace ID

- query:

  One GraphQL document containing a query or mutation. Use variables for
  changing values instead of pasting values into this string

- next_cursor:

  Function accepting a `fabric_graphql_result` and returning the next
  opaque cursor, or `NULL` when pagination is complete. Use
  [`fabric_graphql_cursor()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_graphql_cursor.md)
  for Fabric's normal connection fields

- variables:

  Named list of values for variables declared in `query` One-element
  values are normally sent as scalars. Wrap a one-element list variable
  in [`I()`](https://rdrr.io/r/base/AsIs.html), for example
  `list(ids = I("x"))`, to send it as an array

- cursor_variable:

  Name of the GraphQL variable that receives the next cursor, commonly
  `"after"`. It must match the variable declared in `query`

- operation_name:

  Optional operation name. Supply it when the document contains more
  than one named operation; otherwise leave `NULL`

- workspace_id:

  Workspace GUID. Required when `api` is a GraphQL API GUID, and
  otherwise inferred from a discovered record

- error_policy:

  How GraphQL-level errors are handled. `"return"` lets the caller
  inspect partial data and errors; `"warn"` also makes errors visible
  immediately; `"error"` stops and attaches the result to a
  `fabric_graphql_error`. HTTP/authentication failures always stop

- max_pages:

  Positive maximum number of requests. This guards against a faulty or
  unexpectedly large pagination loop

- timeout:

  Maximum time in seconds for the request. The default allows Fabric's
  own 100-second query timeout response to arrive

- idempotent:

  Logical. Permit retries after transient HTTP failures `TRUE` is
  normally suitable for a read-only query, but not for a mutation that
  could be applied twice

- tenant_id:

  Microsoft Entra tenant ID. Defaults to `FABRICQUERYR_TENANT_ID`

- client_id:

  Microsoft Entra application/client ID. Defaults to
  `FABRICQUERYR_CLIENT_ID`, with the Azure CLI application ID as
  fallback

- token:

  Optional access token or token-provider function. Leave `NULL` to let
  'fabricQueryR' use its normal sign-in flow for a Microsoft Fabric
  host. A custom API endpoint, including an API Management gateway,
  requires an explicitly supplied token or provider so an automatically
  acquired Fabric credential is not forwarded to another host

- auth_args:

  Additional sign-in options passed to
  [`AzureAuth::get_azure_token()`](https://rdrr.io/pkg/AzureAuth/man/get_azure_token.html)

- audience:

  OAuth audience/scope passed to the credential. `NULL` selects the
  documented scope from the authentication flow. Set this only for a
  custom token provider or unusual identity flow

- api_base:

  Fabric REST API base URL used to derive endpoints from IDs Most users
  should keep the default

## Value

A `fabric_graphql_pages` list with `pages`, combined `errors`, and the
final `variables`. `pages` contains one `fabric_graphql_result` per
request and `complete` is `TRUE` when the callback reported no next page
Results are kept page-by-page because the requested schema shape can
vary

## Examples

``` r
if (FALSE) { # \dontrun{
# Discover the GraphQL API that exposes the Products query
workspace <- fabric_workspaces()[[1L]]
api <- fabric_graphql_apis(workspace)[[1L]]

# Fetch pages until the helper sees no next cursor
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
pages$complete
} # }
```
