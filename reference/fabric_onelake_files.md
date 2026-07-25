# Work with files in Microsoft Fabric OneLake

These functions expose the ADLS Gen2-compatible OneLake filesystem
without applying Delta Lake transaction semantics:

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
  dfs_base = "https://onelake.dfs.fabric.microsoft.com"
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

  Workspace name, GUID, discovered workspace, or complete OneLake
  HTTPS/ABFSS path.

- item:

  Item name, GUID, or discovered Fabric item. Use `NULL` when
  `workspace` is a complete OneLake path.

- path:

  Path relative to the item. Complete OneLake paths already contain this
  value.

- recursive:

  Logical. Recurse into subdirectories.

- page_size:

  Maximum paths requested per ADLS page, from 1 to 5000.

- item_type:

  Optional Fabric item type appended to an item name unless that name
  already ends in the same suffix, for example `"Lakehouse"`.

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
  supported.

- dest:

  Optional local destination. When `NULL`, download returns a raw
  vector. A destination download is streamed and committed atomically.

- range:

  Optional inclusive zero-based byte range. Supply one value for
  `bytes=start-` or two values for `bytes=start-end`.

- overwrite:

  Logical. Whether an existing destination may be replaced. Upload
  defaults to `FALSE` and uses `If-None-Match: *`.

- if_match:

  Optional ETag for a conditional operation. Values returned by
  `fabric_onelake_metadata()` can be passed back unchanged; unquoted
  values are quoted as required by ADLS.

- source:

  A local file path or raw vector to upload.

- content_type:

  Optional MIME type stored with an uploaded file.

- create_parents:

  Logical. Create missing parent directories below the Fabric-managed
  first-level folder.

- confirm:

  Must be `TRUE` to enable deletion.

## Value

`fabric_onelake_list()` and `fabric_onelake_metadata()` return tibbles.
`fabric_onelake_download()` returns a raw vector, or invisibly returns
the destination path. `fabric_onelake_upload()` returns a one-row
metadata tibble. `fabric_onelake_delete()` invisibly returns `TRUE`.

## Details

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
Deletion also requires `confirm = TRUE`.

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
