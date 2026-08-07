# Discover Microsoft Fabric items

Lists the items stored in one workspace. In Fabric, an *item* is a
resource such as a Lakehouse, Warehouse, semantic model, notebook, or
Eventhouse.

## Usage

``` r
fabric_items(
  workspace,
  type = NULL,
  detail = FALSE,
  detail_errors = c("record", "abort"),
  recursive = TRUE,
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

- type:

  Optional Fabric API item type, for example `"Lakehouse"`,
  `"Warehouse"`, `"SemanticModel"`, or `"Notebook"`. Matching is done by
  Fabric, so use the API spelling. Leave `NULL` to list all item types.

- detail:

  Logical. `FALSE` makes the fewest API calls and is sufficient for
  names and IDs. `TRUE` also retrieves supported workload properties,
  such as SQL connection strings and Livy or KQL endpoints, but is
  slower and can require additional permissions. The typed helpers below
  use `TRUE`.

- detail_errors:

  How workload-detail or optional private SQL endpoint failures are
  handled. `"record"` retains every successfully retrieved field, stores
  the message in `detail_error`, and emits one summary warning.
  `"abort"` preserves strict all-or-nothing behavior.

- recursive:

  Logical. `TRUE` includes items inside workspace folders; `FALSE` lists
  only items at the workspace root.

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

A list with one `fabric_item` object per item. Each object is a named
list with common fields including `id`, `displayName`, `type`,
`workspaceId`, and `folderId`. With `detail = TRUE`, applicable objects
also contain ready-to-use `sql_connection_string`, `one_lake_*_path`,
`dax_connection_string`, `livy_url`, `query_service_uri`, or
`graphql_endpoint` values. When supplied by Fabric,
`workspaceApiEndpoint` preserves the workspace-specific API origin for
later job calls. Fields that do not apply to an item are absent;
`detail_error` records failed enrichment requests. Nested service data
is retained in place, including in `properties`.

## Details

The caller needs at least access to the workspace (the Viewer role is
sufficient for the core list operation). Workload enrichment
additionally requires `Item.Read.All`/`Item.ReadWrite.All` or the
corresponding workload-specific read scope and access to the item.

## References

[List items REST
API](https://learn.microsoft.com/en-us/rest/api/fabric/core/items/list-items)

[Fabric item management
overview](https://learn.microsoft.com/en-us/rest/api/fabric/articles/item-management/item-management-overview)
