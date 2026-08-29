# Discover Microsoft Fabric items

Returns the Lakehouses, Warehouses, semantic models, notebooks, and
other items stored in a workspace. By default, actionable items are
returned as type-specific R6 objects whose methods perform the matching
query, connection, file, Spark, or job operations

## Usage

``` r
fabric_items(
  workspace,
  type = NULL,
  detail = FALSE,
  detail_errors = c("record", "abort"),
  recursive = TRUE,
  root_folder_id = NULL,
  include = NULL,
  personal_workspace_tenant_id = NULL,
  personal_workspace_owner = NULL,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  token = NULL,
  auth_args = list(),
  api_base = .fabric_api_base,
  output = c("r6", "list")
)
```

## Arguments

- workspace:

  Workspace name, ID, or object returned by
  [`fabric_workspaces()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_workspaces.md).
  A name is convenient for interactive use; an object avoids an extra
  lookup

- type:

  Optional Fabric API item type, for example `"Lakehouse"`,
  `"Warehouse"`, `"SemanticModel"`, or `"Notebook"`. Matching is done by
  Fabric, so use the API spelling. Leave `NULL` to list all item types

- detail:

  Whether to retrieve connection details as well as names and IDs. This
  takes more requests and may require additional permissions. The typed
  discovery helpers generally use `TRUE`; Semantic Model and GraphQL
  helpers default to lightweight records because their query targets can
  be derived without workload detail requests

- detail_errors:

  What to do if some connection details cannot be read `"record"`
  returns the available information and stores an error message with the
  affected item; `"abort"` stops the call

- recursive:

  Logical. `TRUE` includes items inside workspace folders; `FALSE` lists
  only items at the workspace root

- root_folder_id:

  Optional Fabric folder GUID used as the root of the listing. With
  `recursive = FALSE`, only direct children are returned; with `TRUE`,
  nested folders are included

- include:

  Optional character vector of additional item properties to request.
  Fabric currently documents `"DefaultIdentity"`; values are sent as the
  API's comma-separated `include` query parameter

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
  'fabricQueryR' use its normal sign-in flow

- auth_args:

  Additional sign-in options passed to
  [`AzureAuth::get_azure_token()`](https://rdrr.io/pkg/AzureAuth/man/get_azure_token.html)

- api_base:

  Fabric REST API base URL. When `workspace` is an object containing
  `apiEndpoint`, that workspace-specific endpoint is used unless
  `api_base` is supplied explicitly

- output:

  Discovery record representation. The default `"r6"` returns R6 objects
  with type-specific methods. Use `"list"` when a plain record is
  specifically required

## Value

A list with one item object per match. Every object includes common
fields such as `id`, `displayName`, `type`, and `workspaceId`. With
`output = "r6"`, results are
[FabricItem](https://kennispunttwente.github.io/fabricQueryR/reference/FabricItem.md)
objects or type-specific subclasses. With `output = "list"`, results are
`fabric_item` lists. With `detail = TRUE`, both representations include
connection details when Fabric makes them available

## Details

The caller needs at least access to the workspace (the Viewer role is
sufficient for the core list operation). Workload enrichment
additionally requires `Item.Read.All`/`Item.ReadWrite.All` or the
corresponding workload-specific read scope and access to the item
Personal-workspace semantic models use Microsoft's v2 XMLA endpoint and
require both `personal_workspace_tenant_id` and
`personal_workspace_owner`

## References

[List items REST
API](https://learn.microsoft.com/en-us/rest/api/fabric/core/items/list-items)

[Fabric item management
overview](https://learn.microsoft.com/en-us/rest/api/fabric/articles/item-management/item-management-overview)

[Personal-workspace XMLA
endpoints](https://learn.microsoft.com/en-us/fabric/enterprise/powerbi/service-premium-connect-tools#connecting-to-a-personal-workspace)

## Examples

``` r
if (FALSE) { # \dontrun{
# Start by discovering a workspace instead of copying its ID
workspaces <- fabric_workspaces()
workspace <- workspaces[[1L]]

# Continue through the workspace object and inspect service fields
items <- workspace$items()
vapply(items, `[[`, character(1), "displayName")

# Type-specific objects expose their next actions as methods
lakehouse <- workspace$lakehouses()[[1L]]
lakehouse$tables()
} # }
```
