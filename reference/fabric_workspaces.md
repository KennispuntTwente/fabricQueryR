# Discover Microsoft Fabric workspaces

Returns the Fabric workspaces available to the signed-in user or
application Use the result to choose a workspace for
[`fabric_items()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_items.md)
or one of the typed discovery helpers

## Usage

``` r
fabric_workspaces(
  roles = NULL,
  prefer_workspace_endpoints = FALSE,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  token = NULL,
  auth_args = list(),
  api_base = .fabric_api_base,
  allow_custom_endpoint = FALSE
)
```

## Arguments

- roles:

  Optional workspace roles to include, such as `"Viewer"`,
  `"Contributor"`, `"Member"`, or `"Admin"`. Leave `NULL` to return
  every visible workspace

- prefer_workspace_endpoints:

  Whether to request workspace-specific endpoints. Keep `FALSE` unless
  your organization uses workspace-level private links

- tenant_id:

  Microsoft Entra tenant ID. Defaults to `FABRICQUERYR_TENANT_ID`

- client_id:

  Microsoft Entra application/client ID. Defaults to
  `FABRICQUERYR_CLIENT_ID`, then the Azure CLI application ID

- token:

  Optional access token or token-provider function. Leave `NULL` to let
  fabricQueryR use its normal sign-in flow

- auth_args:

  Additional sign-in options passed to
  [`AzureAuth::get_azure_token()`](https://rdrr.io/pkg/AzureAuth/man/get_azure_token.html)

- api_base:

  Fabric REST API base URL. Leave unchanged unless using a different
  Fabric cloud or a test service

- allow_custom_endpoint:

  Logical. Set to `TRUE` only when `api_base` is a non-Microsoft HTTPS
  origin that you trust to receive a Fabric token

## Value

A list with one workspace record per visible workspace. Each record
includes its ID and display name, together with other details returned
by Fabric

## Details

The caller needs permission to read Fabric workspaces. Discovery uses
the Fabric API and requires `Workspace.Read.All` or
`Workspace.ReadWrite.All`

## References

[List workspaces REST
API](https://learn.microsoft.com/en-us/rest/api/fabric/core/workspaces/list-workspaces)

[Workspace
roles](https://learn.microsoft.com/en-us/fabric/fundamentals/roles-workspaces)
