# Inspect a Fabric GraphQL schema

Runs the standard GraphQL introspection query against an **API for
GraphQL** item. The returned schema retains the service's nested type
references, fields, input values, enum values, and directives so callers
can explore the API without assuming how Fabric named its generated
objects

## Usage

``` r
fabric_graphql_schema(
  api,
  workspace_id = NULL,
  timeout = 110,
  idempotent = TRUE,
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

- workspace_id:

  Workspace GUID. Required when `api` is a GraphQL API GUID, and
  otherwise inferred from a discovered record

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

A `fabric_graphql_schema` list containing the standard `__schema`
fields. The original GraphQL response and its (normally empty) errors
are available in the `response` and `errors` attributes

## Details

Microsoft Fabric disables runtime introspection by default. A workspace
administrator must enable it under **API Settings \> Introspection**.
When it must remain disabled, use **Export schema** in the Fabric portal
instead; schema export remains available independently of the runtime
setting

## References

[Fabric API for GraphQL introspection and schema
export](https://learn.microsoft.com/en-us/fabric/data-engineering/api-graphql-introspection-schema-export)

## Examples

``` r
if (FALSE) { # \dontrun{
# Discover the GraphQL API whose schema you want to inspect
workspace <- fabric_workspaces()[[1L]]
api <- fabric_graphql_apis(workspace)[[1L]]

# Request the standard GraphQL introspection schema
schema <- fabric_graphql_schema(api)

# List its named types to learn what can be queried
vapply(schema$types, `[[`, character(1), "name")
} # }
```
