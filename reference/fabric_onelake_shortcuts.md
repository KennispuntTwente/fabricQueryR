# Manage OneLake shortcuts

Lists, inspects, creates or updates, and deletes shortcuts on a Fabric
item. Discovered Fabric items can be used directly as OneLake targets. A
validated raw target list supports connection-backed shortcut types
already configured in Fabric without copying data into R.

## Usage

``` r
fabric_onelake_shortcuts(
  item,
  workspace = NULL,
  item_type = NULL,
  parent_path = NULL,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  token = NULL,
  auth_args = list(),
  api_base = .fabric_api_base
)

fabric_onelake_shortcut_get(
  item,
  path,
  name,
  workspace = NULL,
  item_type = NULL,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  token = NULL,
  auth_args = list(),
  api_base = .fabric_api_base
)

fabric_onelake_shortcut_create(
  item,
  path,
  name,
  target,
  workspace = NULL,
  item_type = NULL,
  target_workspace = NULL,
  target_path = NULL,
  target_item_type = NULL,
  conflict_policy = c("Abort", "GenerateUniqueName", "CreateOrOverwrite",
    "OverwriteOnly"),
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  token = NULL,
  auth_args = list(),
  api_base = .fabric_api_base
)

fabric_onelake_shortcuts_bulk_create(
  item,
  shortcuts,
  workspace = NULL,
  item_type = NULL,
  conflict_policy = c("Abort", "GenerateUniqueName", "CreateOrOverwrite",
    "OverwriteOnly"),
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  token = NULL,
  auth_args = list(),
  api_base = .fabric_api_base
)

fabric_onelake_shortcut_cache_reset(
  workspace,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  token = NULL,
  auth_args = list(),
  api_base = .fabric_api_base
)

fabric_onelake_shortcut_delete(
  item,
  path,
  name,
  workspace = NULL,
  item_type = NULL,
  confirm = FALSE,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  token = NULL,
  auth_args = list(),
  api_base = .fabric_api_base
)
```

## Arguments

- item:

  Destination Fabric item name, GUID, or object returned by a discovery
  function.

- workspace:

  Workspace name, GUID, or discovery object containing `item`. May be
  omitted when `item` contains `workspaceId`.

- item_type:

  Optional item type used to disambiguate a destination item supplied by
  name.

- parent_path:

  Optional `Files` or `Tables` path from which listing starts. Fabric
  still returns shortcuts below that path exhaustively.

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

  Fabric REST API base URL. Leave unchanged unless using a different
  Fabric cloud or a test service

- path:

  Parent `Files` or `Tables` path where the shortcut exists or will be
  created. Local checks validate path syntax, while Fabric applies item-
  and workload-specific placement rules. In a Lakehouse `Tables`
  section, use `Tables` because Fabric permits shortcuts only at that
  top level.

- name:

  Shortcut name.

- target:

  A discovered Fabric item, its name or GUID, or a raw named shortcut
  target list. A raw target must contain exactly one documented key such
  as `oneLake`, `adlsGen2`, `amazonS3`, `azureBlobStorage`,
  `googleCloudStorage`, `oneDriveSharePoint`, `s3Compatible`, or
  `dataverse`. Connection-backed targets must include the documented
  required fields, use a Fabric connection ID, and must not embed
  credentials. Local checks cover structure, required fields, identifier
  shape, and generic URL safety; they do not verify source-specific
  host/path rules or that a connection refers to the supplied location.
  A raw `oneLake` target may also include its documented optional
  `connectionId`. Fabric service validation is authoritative.

- target_workspace:

  Workspace containing a OneLake `target`. May be omitted when a
  discovered target contains `workspaceId`.

- target_path:

  Item-relative `Files` or `Tables` path for a OneLake target. Required
  when `target` is an item and unused for a raw target list.

- target_item_type:

  Optional Fabric item type used to disambiguate a OneLake target
  supplied by name.

- conflict_policy:

  `"Abort"` preserves an existing shortcut with the same path and name.
  `"GenerateUniqueName"` creates a uniquely named shortcut,
  `"CreateOrOverwrite"` creates or updates it, and `"OverwriteOnly"`
  updates an existing shortcut without creating one.

- shortcuts:

  For bulk creation, a non-empty list of shortcut request lists. Each
  request requires `path`, `name`, and `target`, accepts the same target
  companion fields as `fabric_onelake_shortcut_create()`, and may
  include a `transform` list. The only currently documented transform is
  `csvToDelta`; see Details.

- confirm:

  Logical. Deletion is disabled unless explicitly set to `TRUE`.
  Deleting a shortcut does not delete its destination data.

## Value

`fabric_onelake_shortcuts()` returns a tibble with one row per shortcut.
`fabric_onelake_shortcut_get()` and `fabric_onelake_shortcut_create()`
return the same one-row shape. `fabric_onelake_shortcut_delete()`
returns `TRUE` invisibly after success.
`fabric_onelake_shortcut_cache_reset()` returns a `fabric_operation`
handle for either immediate or asynchronous completion.

## Details

Shortcut names, parent paths, and OneLake target paths follow Fabric's
current shortcut limits: `%`, `+`, and non-ASCII characters are
rejected. Other source- and destination-specific restrictions are
intentionally left to Fabric so that newly supported connection types
and rules remain usable.

Listing follows Fabric continuation links and tokens until every
shortcut below `parent_path` is returned. Unknown target details and
transform fields are preserved in list columns for forward
compatibility.

Create is deliberately not replayed automatically because its POST
outcome can be ambiguous after a transport failure. The default conflict
policy is Fabric's non-destructive `Abort`; overwrite must be requested
explicitly. Deletion is also not replayed automatically, and a 404
confirms that the shortcut link is already absent.

Bulk creation is a preview Fabric API. Its optional `csvToDelta`
transform accepts `includeSubfolders` and a `properties` list containing
`delimiter`, `skipFilesWithErrors`, and `useFirstRowAsHeader`. Supported
delimiters are comma, space, tab, `|`, `&`, and `;`. A bulk request
returns a `fabric_operation`; use
[`fabric_operation_result()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_operation_status.md)
to retrieve the per-request statuses, created shortcuts, and errors
after completion.

These Core REST APIs require `OneLake.Read.All` or
`OneLake.ReadWrite.All` for reads, and `OneLake.ReadWrite.All` for
create and delete. Fabric documents support for users, service
principals, and managed identities. API scope is not sufficient by
itself: listing or reading also requires item Read permission or OneLake
Read permission on the destination path. Creating requires item Write or
OneLake ReadWrite on the destination, plus Read access to the target
path. Updating or deleting likewise requires item Write or
destination-path OneLake ReadWrite permission.

## References

[OneLake shortcuts REST
API](https://learn.microsoft.com/en-us/rest/api/fabric/core/onelake-shortcuts/)

[OneLake shortcut placement and
limitations](https://learn.microsoft.com/en-us/fabric/onelake/onelake-shortcuts)

[OneLake shortcut security and path
permissions](https://learn.microsoft.com/en-us/fabric/onelake/onelake-shortcut-security)

[Create shortcuts in
bulk](https://learn.microsoft.com/en-us/rest/api/fabric/core/onelake-shortcuts/creates-shortcuts-in-bulk)

[Reset shortcut
cache](https://learn.microsoft.com/en-us/rest/api/fabric/core/onelake-shortcuts/reset-shortcut-cache)

## Examples

``` r
if (FALSE) { # \dontrun{
# Discover two Lakehouses in the same workspace
workspace <- fabric_workspaces()[[1L]]
lakehouses <- fabric_lakehouses(workspace)
destination <- lakehouses[[1L]]
source <- lakehouses[[2L]]
source_paths <- fabric_onelake_list(workspace, source, path = "Tables")
source_table <- source_paths[source_paths$is_directory, ][1L, ]

# Create a shortcut whose target came from the source Lakehouse listing
created <- fabric_onelake_shortcut_create(
  destination,
  path = "Files",
  name = "shared-orders",
  target = source,
  target_path = source_table$path[[1L]]
)

# List the folder, then fetch the created shortcut by its returned identity
fabric_onelake_shortcuts(destination, parent_path = "Files")
shortcut <- fabric_onelake_shortcut_get(
  destination,
  path = created$path[[1L]],
  name = created$name[[1L]]
)

# Delete that same discovered shortcut explicitly
fabric_onelake_shortcut_delete(
  destination,
  path = created$path[[1L]],
  name = created$name[[1L]],
  confirm = TRUE
)
} # }
```
