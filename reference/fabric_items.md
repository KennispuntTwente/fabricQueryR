# Discover Microsoft Fabric items

Lists items in a workspace with optional server-side item-type
filtering. Set `detail = TRUE` to call each supported workload API and
include workload-specific connection metadata.

## Usage

``` r
fabric_items(
  workspace,
  type = NULL,
  detail = FALSE,
  recursive = TRUE,
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

- type:

  Optional Fabric item type, for example `"Lakehouse"` or `"Warehouse"`.

- detail:

  Logical. Retrieve workload-specific properties for supported types.

- recursive:

  Logical. Include items in nested folders.

- tenant_id, client_id, access_token, token_provider:

  Authentication arguments. Discovery uses the
  `https://api.fabric.microsoft.com/.default` audience and requires
  `Workspace.Read.All` or `Workspace.ReadWrite.All`.

- api_base:

  Fabric REST API base URL.

## Value

A tibble with one row per item. `properties` and `raw` are list columns.
Enriched rows also contain directly usable SQL, OneLake, DAX, Livy, and
KQL fields where Fabric exposes them.

## Details

Workload enrichment requires `Item.Read.All`/`Item.ReadWrite.All` or the
corresponding workload-specific read scope in addition to access to the
item.
