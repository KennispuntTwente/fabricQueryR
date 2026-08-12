# Run a query against a Fabric GraphQL API

Sends a GraphQL query or mutation to an **API for GraphQL** item and
returns the result as a nested R list. Use this when a Fabric API
already exposes the Lakehouse, Warehouse, or SQL Database data you need

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

  GraphQL endpoint, API ID, or one discovered GraphQLApi record. An item
  from
  [`fabric_graphql_apis()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md)
  is usually easiest because it supplies the endpoint and workspace ID

- query:

  One GraphQL document containing a query or mutation. Use variables for
  changing values instead of pasting values into this string

- variables:

  Named list of values for variables declared in `query` One-element
  values are normally sent as scalars. Wrap a one-element list variable
  in [`I()`](https://rdrr.io/r/base/AsIs.html), for example
  `list(ids = I("x"))`, to send it as an array

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
  fabricQueryR use its normal sign-in flow

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

- allow_custom_endpoint:

  Logical. Permit a GraphQL endpoint outside the Microsoft Fabric API
  origin. Keep `FALSE` unless the origin is trusted; credentials are
  sent to the supplied endpoint

## Value

A `fabric_graphql_result` list with `data`, `errors`, `extensions`, and
`response` (the complete parsed response). `data` follows the nested
shape requested in the GraphQL document and is usually a combination of
named lists and vectors, not a tibble. Because GraphQL can return
partial data, inspect `errors` even when `data` is present

## Before you query

Before using this function, create an **API for GraphQL** item in a
Fabric workspace, connect its data source, and choose which tables,
fields, queries, and mutations the API exposes. Fabric's built-in
GraphQL editor and schema explorer are the easiest places to design and
test a document before copying it to R

Mutation availability depends on the configured source. Fabric Warehouse
and SQL Database sources can expose supported mutations, while Lakehouse
and mirrored SQL analytics endpoint sources are read-only and expose
queries only

The easiest input is an item from
[`fabric_graphql_apis()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md).
You can instead supply the API's endpoint, or its ID together with
`workspace_id`

## Permissions and authentication

Interactive authentication requires the Power BI delegated scope
`GraphQLApi.Execute.All`, plus **Run Queries and Mutations** permission
on the API. Service principals are also supported by Fabric: request a
Fabric API token with `auth_args` or pass one through `token`, enable
service principals for Fabric APIs in the tenant, and grant the
principal API Execute access or a suitable workspace role. With SSO
connectivity, the caller also needs the required access to the
underlying data source Saved-credential APIs use the configured
connection instead

Most users can leave `audience = NULL`; fabricQueryR chooses the
documented scope for the sign-in flow. Set it only for a custom identity
provider

## Retries and service limits

GraphQL POST requests are not retried by default because a document can
contain mutations. Set `idempotent = TRUE` only when the operation is
safe to repeat

Fabric returns at most 100 items by default and permits at most 100,000
items across pagination. Each response is limited to 64 MB, each request
to 100 seconds, and query nesting to 10 levels. Use smaller pages and
filtered query partitions when a result could approach these service
limits

Large integers outside R's exact numeric range are returned as character
values so identifiers and other large integer fields are not rounded

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
