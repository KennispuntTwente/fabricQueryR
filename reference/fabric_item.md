# Discover one Microsoft Fabric item

Resolves an item by GUID or by an exact/unique display name and
retrieves workload-specific properties when supported.

## Usage

``` r
fabric_item(
  workspace,
  item,
  type = NULL,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  access_token = NULL,
  token_provider = NULL,
  api_base = .fabric_api_base
)
```

## Arguments

- workspace:

  Workspace GUID, exact display name, or a workspace record returned by
  [`fabric_workspaces()`](https://lukakoning.github.io/fabricQueryR/reference/fabric_workspaces.md).

- item:

  Item GUID, exact display name, or a one-row item record returned by a
  discovery function.

- type:

  Optional Fabric item type, for example `"Lakehouse"` or `"Warehouse"`.

- tenant_id, client_id, access_token, token_provider:

  Authentication arguments. Discovery uses the
  `https://api.fabric.microsoft.com/.default` audience and requires
  `Workspace.Read.All` or `Workspace.ReadWrite.All`.

- api_base:

  Fabric REST API base URL.

## Value

A `fabric_item` list containing common metadata, workload properties,
and derived connection targets.
