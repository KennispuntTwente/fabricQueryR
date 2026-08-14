# Work with files in Microsoft Fabric OneLake

List, inspect, download, upload, and delete ordinary files stored in
OneLake These helpers are intended for files such as CSV, JSON, images,
and model artifacts in a Fabric item's `Files/` area

- `fabric_onelake_list()` lists paths and follows all continuation
  tokens

- `fabric_onelake_metadata()` returns file or directory properties

- `fabric_onelake_download()` reads a file into memory or streams it to
  disk

- `fabric_onelake_upload()` creates or replaces a file

- `fabric_onelake_delete()` explicitly deletes a file or directory

## Usage

``` r
fabric_onelake_list(
  workspace,
  item = NULL,
  path = "",
  recursive = FALSE,
  page_size = 5000L,
  begin_from = NULL,
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
  allow_managed_tables = FALSE,
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
  dfs_base = "https://onelake.dfs.fabric.microsoft.com",
  allow_managed_tables = FALSE
)
```

## Arguments

- workspace:

  Workspace name, ID, record from
  [`fabric_workspaces()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_workspaces.md),
  or a complete OneLake HTTPS/ABFSS path

- item:

  Item name, GUID, or discovered Fabric item. Use `NULL` when
  `workspace` is a complete OneLake path. An item from
  [`fabric_lakehouses()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md)
  is the least ambiguous input

- path:

  Path relative to the item, usually beginning with `Files/` or
  `Tables/`, for example `"Files/incoming/data.csv"`. Use forward
  slashes A complete OneLake path already contains this value

- recursive:

  For listing, whether to include all descendants. For deletion, whether
  a non-empty directory may be removed

- page_size:

  Maximum paths requested from OneLake per API call, from 1 to 5000.
  Smaller values reduce each response size but require more requests

- begin_from:

  Optional path at which to begin a listing. Use this to resume a long,
  alphabetically ordered scan

- item_type:

  Optional Fabric item type appended to an item name unless that name
  already ends in the same suffix, for example `"Lakehouse"` Usually
  unnecessary for a discovered item or a name such as
  `"Sales.Lakehouse"`

- tenant_id:

  Entra tenant ID. Defaults to `FABRICQUERYR_TENANT_ID`

- client_id:

  Entra application ID. Defaults to `FABRICQUERYR_CLIENT_ID`, then the
  Azure CLI application ID

- token:

  Optional access token or token-provider function. Leave `NULL` to let
  fabricQueryR use its normal sign-in flow

- auth_args:

  Additional sign-in options passed to
  [`AzureAuth::get_azure_token()`](https://rdrr.io/pkg/AzureAuth/man/get_azure_token.html)

- dfs_base:

  OneLake service address. Most users should keep the default; a
  workspace-specific address discovered from Fabric is used when
  available

- dest:

  Optional local destination. When `NULL`, download returns a raw vector
  held in R memory. Supply a path to stream large files to disk. A
  destination download is staged before it replaces an existing file

- range:

  Optional inclusive zero-based byte range. Supply one value for all
  bytes from that offset onward, or two values for `start` through `end`
  Leave `NULL` to download the entire file

- overwrite:

  Whether an existing local or OneLake file may be replaced Existing
  files are protected by default

- if_match:

  Optional file version (`etag`) returned by
  `fabric_onelake_metadata()`. The operation proceeds only if the file
  still has that version

- source:

  Local file path or raw vector to upload. A path is streamed; a raw
  vector is already held in memory

- content_type:

  Optional MIME type stored with an uploaded file, for example
  `"text/csv"`

- create_parents:

  Logical. Create missing parent directories below the Fabric-managed
  first-level folder. Keep `TRUE` for normal uploads

- allow_managed_tables:

  Whether to allow direct changes below `Tables/` Keep `FALSE` for
  normal use: changing Delta files directly can corrupt a managed table

- chunk_size:

  Upload chunk size in bytes. The default suits most files; larger
  values make fewer requests but use more memory

- confirm:

  Safety switch that must be explicitly set to `TRUE` before deletion is
  attempted

## Value

`fabric_onelake_list()` returns one row per path, including its
item-relative `path`, file `name`, `is_directory`, `content_length`,
`etag`, and modification/permission fields `fabric_onelake_metadata()`
and `fabric_onelake_upload()` return a one-row tibble with the resolved
path and available HTTP metadata `fabric_onelake_download()` returns a
raw vector when `dest = NULL`, or invisibly returns the destination path
after writing to disk `fabric_onelake_delete()` invisibly returns `TRUE`

## Choosing a target

The easiest inputs are a workspace plus an item returned by
[`fabric_lakehouses()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md).
You can also use names, IDs, or a complete OneLake HTTPS/ABFSS path.
When using an item name, include its type suffix, such as
`"Sales.Lakehouse"`, or supply `item_type`

A Lakehouse's `Tables/` area is managed as Delta tables. Use
[`fabric_onelake_read_delta_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_read_delta_table.md)
to read those tables, and use SQL, Spark, or another Delta-aware tool to
change them. Uploading or deleting individual files below `Tables/` can
damage a table and is blocked by default

## Permissions

The signed-in user or application needs access through a workspace role
or the item's **OneLake security roles**, configured under **Manage
OneLake security**. Uploading and deleting need write permission. Your
Fabric administrator must also allow external apps to access OneLake. If
a call returns HTTP 403 after sign-in succeeds, check both that tenant
setting and the item's data permissions

## Safe file replacement

Existing files are protected unless `overwrite = TRUE`. Uploads and
downloads are staged before replacing their destination, so an
interrupted transfer does not normally leave a partial file. Use
`if_match` when a file should be replaced only if it has not changed
since you inspected it

## References

[Connect to OneLake with ADLS
APIs](https://learn.microsoft.com/en-us/fabric/onelake/onelake-access-api)

[ADLS Gen2 List
Paths](https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/list)

[Create and manage OneLake security
roles](https://learn.microsoft.com/en-us/fabric/onelake/security/create-manage-roles)

[OneLake security best
practices](https://learn.microsoft.com/en-us/fabric/onelake/security/best-practices-secure-data-in-onelake)

[OneLake tenant
settings](https://learn.microsoft.com/en-us/fabric/admin/service-admin-portal-onelake)

## Examples

``` r
if (FALSE) { # \dontrun{
# Discover the OneLake target instead of typing workspace and item names
workspace <- fabric_workspaces()[[1L]]
lakehouse <- fabric_lakehouses(workspace)[[1L]]

# Create a small local CSV and upload it to the discovered Lakehouse
local_csv <- tempfile(fileext = ".csv")
write.csv(data.frame(id = 1:3), local_csv, row.names = FALSE)
fabric_onelake_upload(
  workspace,
  lakehouse,
  "Files/incoming/example.csv",
  source = local_csv
)

# List the folder and inspect metadata for the uploaded file
files <- fabric_onelake_list(
  workspace = workspace,
  item = lakehouse,
  path = "Files/incoming",
  recursive = TRUE
)
metadata <- fabric_onelake_metadata(
  workspace,
  lakehouse,
  "Files/incoming/example.csv"
)

# Download the first 100 bytes when only a file sample is needed
bytes <- fabric_onelake_download(
  workspace,
  lakehouse,
  "Files/incoming/example.csv",
  range = c(0, 99)
)

# Deletion is explicit and requires confirm = TRUE
fabric_onelake_delete(
  workspace,
  lakehouse,
  "Files/incoming/example.csv",
  confirm = TRUE
)
} # }
```
