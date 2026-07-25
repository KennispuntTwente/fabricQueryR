# Discover Microsoft Fabric workspaces

Lists every workspace visible to the authenticated principal, following
all Fabric continuation pages.

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

  Optional character vector of workspace roles used to filter the
  response.

- prefer_workspace_endpoints:

  Logical. Ask Fabric to include a workspace-specific API endpoint, when
  available.

- tenant_id:

  Entra tenant ID. Defaults to `FABRICQUERYR_TENANT_ID`.

- client_id:

  Entra application ID. Defaults to `FABRICQUERYR_CLIENT_ID`, then the
  Azure CLI application ID.

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

  Fabric REST API base URL.

## Value

A tibble with one row per workspace. Nested Fabric fields are kept in
list columns.
