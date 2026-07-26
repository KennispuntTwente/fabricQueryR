# Work with files in Microsoft Fabric OneLake

Lists, inspects, downloads, uploads, or deletes files in OneLake, the
storage layer shared by Fabric data items. These functions treat paths
as ordinary files through OneLake's ADLS Gen2-compatible API:

- `fabric_onelake_list()` lists paths and follows all continuation
  tokens.

- `fabric_onelake_metadata()` returns file or directory properties.

- `fabric_onelake_download()` reads a file into memory or streams it to
  disk.

- `fabric_onelake_upload()` creates or replaces a file.

- `fabric_onelake_delete()` explicitly deletes a file or directory.

## Usage

``` r
fabric_onelake_list(
  workspace,
  item = NULL,
  path = "",
  recursive = FALSE,
  page_size = 5000L,
  item_type = NULL,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  token = NULL,
  auth_args = list(),
  dfs_base = "https://onelake.dfs.fabric.microsoft.com"
)

fabric_onelake_metadata(
  workspace,
  item = NULL,
  path = "",
  item_type = NULL,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  token = NULL,
  auth_args = list(),
  dfs_base = "https://onelake.dfs.fabric.microsoft.com"
)

fabric_onelake_download(
  workspace,
  item = NULL,
  path = "",
  dest = NULL,
  range = NULL,
  overwrite = FALSE,
  if_match = NULL,
  item_type = NULL,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  token = NULL,
  auth_args = list(),
  dfs_base = "https://onelake.dfs.fabric.microsoft.com"
)

fabric_onelake_upload(
  workspace,
  item = NULL,
  path = "",
  source,
  overwrite = FALSE,
  if_match = NULL,
  content_type = NULL,
  create_parents = TRUE,
  item_type = NULL,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  token = NULL,
  auth_args = list(),
  dfs_base = "https://onelake.dfs.fabric.microsoft.com",
  chunk_size = getOption("fabricqueryr.onelake.chunk_size", 8 * 1024^2)
)

fabric_onelake_delete(
  workspace,
  item = NULL,
  path = "",
  recursive = FALSE,
  confirm = FALSE,
  if_match = NULL,
  item_type = NULL,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  token = NULL,
  auth_args = list(),
  dfs_base = "https://onelake.dfs.fabric.microsoft.com"
)
```

## Arguments

- workspace:

  Workspace display name, GUID, row from
  [`fabric_workspaces()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_workspaces.md),
  or complete OneLake HTTPS/ABFSS path. Names are convenient
  interactively; GUIDs avoid problems with spaces and renaming.

- item:

  Item name, GUID, or discovered Fabric item. Use `NULL` when
  `workspace` is a complete OneLake path. A row from
  [`fabric_lakehouses()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md)
  is the least ambiguous input.

- path:

  Path relative to the item, usually beginning with `Files/` or
  `Tables/`, for example `"Files/incoming/data.csv"`. Use forward
  slashes. A complete OneLake path already contains this value.

- recursive:

  Logical. For listing, include descendants rather than only direct
  children. For deletion, allow removal of a non-empty directory.

- page_size:

  Maximum paths requested from OneLake per API call, from 1 to 5000.
  Smaller values reduce each response size but require more requests.

- item_type:

  Optional Fabric item type appended to an item name unless that name
  already ends in the same suffix, for example `"Lakehouse"`. Usually
  unnecessary for a discovered item or a name such as
  `"Sales.Lakehouse"`.

- tenant_id:

  Entra tenant ID. Defaults to `FABRICQUERYR_TENANT_ID`.

- client_id:

  Entra application ID. Defaults to `FABRICQUERYR_CLIENT_ID`, then the
  Azure CLI application ID.

- token:

  Optional
  [`AzureAuth::AzureToken`](https://rdrr.io/pkg/AzureAuth/man/AzureToken.html),
  bearer-token string, or token-provider function. With `NULL`,
  `AzureAuth` reuses a matching cached token or starts its normal
  interactive login flow.

- auth_args:

  Named list of additional arguments passed to
  [`AzureAuth::get_azure_token()`](https://rdrr.io/pkg/AzureAuth/man/get_azure_token.html).

- dfs_base:

  OneLake DFS endpoint. Regional and workspace-private DFS endpoints are
  supported. Most users should keep the default.

- dest:

  Optional local destination. When `NULL`, download returns a raw vector
  held in R memory. Supply a path to stream large files to disk. A
  destination download is committed atomically.

- range:

  Optional inclusive zero-based byte range. Supply one value for all
  bytes from that offset onward, or two values for `start` through
  `end`. Leave `NULL` to download the entire file.

- overwrite:

  Logical. Whether an existing destination may be replaced. Both
  downloads and uploads protect existing files by default. Upload with
  `overwrite = FALSE` uses `If-None-Match: *`.

- if_match:

  Optional ETag for a conditional operation. Values returned by
  `fabric_onelake_metadata()` can be passed back unchanged. This
  prevents overwriting, downloading, or deleting a file that changed
  since it was inspected.

- source:

  Local file path or raw vector to upload. A path is streamed; a raw
  vector is already held in memory.

- content_type:

  Optional MIME type stored with an uploaded file, for example
  `"text/csv"`.

- create_parents:

  Logical. Create missing parent directories below the Fabric-managed
  first-level folder. Keep `TRUE` for normal uploads.

- chunk_size:

  Positive upload chunk size in bytes. Defaults to 8 MiB; larger chunks
  use fewer requests but more memory.

- confirm:

  Safety switch that must be explicitly set to `TRUE` before deletion is
  attempted.

## Value

`fabric_onelake_list()` returns one row per path, including its
item-relative `path`, file `name`, `is_directory`, `content_length`,
`etag`, and modification/permission fields. `fabric_onelake_metadata()`
and `fabric_onelake_upload()` return a one-row tibble with the resolved
path and available HTTP metadata. `fabric_onelake_download()` returns a
raw vector when `dest = NULL`, or invisibly returns the destination path
after writing to disk. `fabric_onelake_delete()` invisibly returns
`TRUE`.

## Details

A Lakehouse normally contains a `Files/` area for ordinary files and a
`Tables/` area managed as Delta tables. These helpers are well suited to
CSV, JSON, images, and other files under `Files/`. They do not interpret
the Delta transaction log: use
[`fabric_onelake_read_delta_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_read_delta_table.md)
to read a Delta table, and do not upload or delete individual files
under `Tables/`, because doing so can make the table inconsistent.

`workspace` and `item` may be names, GUIDs, or one-row discovery
results. Name-based items must include their item-type suffix (for
example, `"Sales.Lakehouse"`) or supply `item_type`. Microsoft requires
workspace and item GUIDs to be used together. As a convenience,
`workspace` may instead be a complete
`https://...dfs.fabric.microsoft.com/...` or `abfss://...` OneLake path,
in which case `item` must be `NULL`.

OneLake uses the Storage token audience
`https://storage.azure.com/.default`. Fabric manages the item root and
its first-level folders (such as `Files` and `Tables`), so upload and
delete operations are limited to descendants of a managed folder.
Deletion also requires `confirm = TRUE`. Give the signed-in user or
application access through a workspace role or through the item's
**Manage OneLake data access** roles. Write access is required for
uploads and deletes.

Uploads are streamed in chunks to a temporary sibling file. The
completed file is atomically renamed to its destination with the
requested overwrite or ETag precondition, so failed transfers do not
truncate an existing file.

## References

[Connect to OneLake with ADLS
APIs](https://learn.microsoft.com/en-us/fabric/onelake/onelake-access-api)

[Create and manage OneLake security
roles](https://learn.microsoft.com/en-us/fabric/onelake/security/create-manage-roles)

## Examples

``` r
if (FALSE) { # \dontrun{
files <- fabric_onelake_list(
  workspace = "Finance",
  item = "Curated.Lakehouse",
  path = "Files/incoming",
  recursive = TRUE
)

bytes <- fabric_onelake_download(
  "Finance",
  "Curated.Lakehouse",
  "Files/incoming/data.csv",
  range = c(0, 99)
)

fabric_onelake_upload(
  "Finance",
  "Curated.Lakehouse",
  "Files/incoming/data.csv",
  source = "data.csv"
)

fabric_onelake_delete(
  "Finance",
  "Curated.Lakehouse",
  "Files/incoming/data.csv",
  confirm = TRUE
)
} # }
```
