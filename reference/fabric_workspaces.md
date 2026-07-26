# Discover Microsoft Fabric workspaces

Lists the Fabric workspaces the signed-in user or application can
access. A workspace is the top-level container that holds Lakehouses,
Warehouses, semantic models, notebooks, and other Fabric items.

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
  api_base = .fabric_api_base
)
```

## Arguments

- roles:

  Optional workspace roles to include, such as `"Viewer"`,
  `"Contributor"`, `"Member"`, or `"Admin"`. Leave `NULL` to return
  every visible workspace.

- prefer_workspace_endpoints:

  Logical. Set to `TRUE` to ask Fabric for a workspace-specific API
  endpoint, which can be needed with workspace-level private links. Most
  users should keep the default, `FALSE`.

- tenant_id:

  Microsoft Entra tenant ID. Defaults to `FABRICQUERYR_TENANT_ID`.

- client_id:

  Microsoft Entra application/client ID. Defaults to
  `FABRICQUERYR_CLIENT_ID`, then the Azure CLI application ID.

- token:

  Preferred token input: an
  [`AzureAuth::AzureToken`](https://rdrr.io/pkg/AzureAuth/man/AzureToken.html)
  object, bearer-token string, or token-provider function. With `NULL`
  (the default), `AzureAuth` reuses a matching cached token or starts
  its normal interactive login flow when a new token is required.

- auth_args:

  Named list of additional arguments passed to
  [`AzureAuth::get_azure_token()`](https://rdrr.io/pkg/AzureAuth/man/get_azure_token.html)
  when no token source is supplied. Discovery uses the
  `https://api.fabric.microsoft.com/.default` audience and requires
  `Workspace.Read.All` or `Workspace.ReadWrite.All`.

- api_base:

  Fabric REST API base URL. Leave unchanged unless using a different
  Fabric cloud or a test service.

## Value

A tibble with one row per workspace. Important columns include `id`
(useful for later API calls), `displayName`, `capacityRegion`, and
`apiEndpoint`. `tags` and the complete service response in `raw` are
list columns.

## References

[List workspaces REST
API](https://learn.microsoft.com/en-us/rest/api/fabric/core/workspaces/list-workspaces)

[Workspace
roles](https://learn.microsoft.com/en-us/fabric/fundamentals/roles-workspaces)
