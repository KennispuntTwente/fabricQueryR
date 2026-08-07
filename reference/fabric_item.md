# Discover one Microsoft Fabric item

Finds one item and retrieves the connection details that fabricQueryR
can use. This is convenient when you know the item's name and do not
need a collection of every item in the workspace.

## Usage

``` r
fabric_item(
  workspace,
  item,
  type = NULL,
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

- workspace:

  Workspace GUID, exact display name, or a workspace record returned by
  [`fabric_workspaces()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_workspaces.md).
  A record avoids an extra lookup and, if it contains `apiEndpoint`,
  routes workspace calls through that endpoint. A name is often easier
  for interactive use.

- item:

  Item GUID, exact display name, or an item record returned by a
  discovery function. A display name must identify exactly one item of
  the requested `type`; use a GUID or discovered record when names are
  duplicated.

- type:

  Optional Fabric API item type, for example `"Lakehouse"`,
  `"Warehouse"`, `"SemanticModel"`, or `"Notebook"`. Matching is done by
  Fabric, so use the API spelling. Leave `NULL` to list all item types.

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
  Fabric cloud or a test service. When `workspace` is a record
  containing `apiEndpoint`, that workspace-specific endpoint is used
  unless `api_base` is supplied explicitly.

- allow_custom_endpoint:

  Logical. Set to `TRUE` only when `api_base` is a non-Microsoft HTTPS
  origin that you trust to receive a Fabric token.

## Value

A `fabric_item` list. It contains common fields such as `id`,
`displayName`, `type`, and `workspaceId`, the nested workload
`properties`, and applicable connection targets such as
`sql_connection_string`, `livy_url`, or `query_service_uri`.
