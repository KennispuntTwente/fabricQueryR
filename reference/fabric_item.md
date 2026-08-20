# Discover one Microsoft Fabric item

Finds one item and returns the connection details needed by fabricQueryR
Use this when you know the item's name or ID and do not need to list
every item in the workspace

## Usage

``` r
fabric_item(
  workspace,
  item,
  type = NULL,
  personal_workspace_tenant_id = NULL,
  personal_workspace_owner = NULL,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  token = NULL,
  auth_args = list(),
  api_base = .fabric_api_base
)
```

## Arguments

- workspace:

  Workspace name, ID, or record returned by
  [`fabric_workspaces()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_workspaces.md).
  A name is convenient for interactive use; a record avoids an extra
  lookup

- item:

  Item GUID, exact display name, or an item record returned by a
  discovery function. A display name must identify exactly one item of
  the requested `type`; use a GUID or discovered record when names are
  duplicated

- type:

  Optional Fabric API item type, for example `"Lakehouse"`,
  `"Warehouse"`, `"SemanticModel"`, or `"Notebook"`. Matching is done by
  Fabric, so use the API spelling. Leave `NULL` to list all item types

- personal_workspace_tenant_id:

  Optional Microsoft Entra tenant ID used to build the XMLA endpoint for
  a Personal workspace

- personal_workspace_owner:

  Optional owner UPN or Entra object ID used to build the XMLA endpoint
  for a Personal workspace. Microsoft Fabric's workspace API does not
  return either personal-workspace identifier, so supply this together
  with `personal_workspace_tenant_id` when a `dax_connection_string` is
  needed for a semantic model in My Workspace

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

  Fabric REST API base URL. When `workspace` is a record containing
  `apiEndpoint`, that workspace-specific endpoint is used unless
  `api_base` is supplied explicitly

## Value

One `fabric_item` record containing the item's name, ID, type,
workspace, and any connection details that Fabric makes available

## Details

The caller needs access to the workspace for the core item lookup. This
singular helper always performs workload-specific enrichment as well,
which additionally requires `Item.Read.All`/`Item.ReadWrite.All` or the
applicable workload-specific read scope and access to the item. Use
[`fabric_items()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_items.md)
with `detail = FALSE` when only core item metadata is needed

## Examples

``` r
if (FALSE) { # \dontrun{
# Discover a workspace and obtain a lightweight Warehouse record
workspace <- fabric_workspaces()[[1L]]
warehouses <- fabric_items(workspace, type = "Warehouse")

# Enrich that discovered record with connection details
warehouse <- fabric_item(workspace, warehouses[[1L]])

# The result can be passed directly to SQL helpers
fabric_sql_connection_info(warehouse)
} # }
```
