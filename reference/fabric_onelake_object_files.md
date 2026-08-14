# Read and write R or Arrow objects in OneLake Files

These object-aware helpers sit above
[`fabric_onelake_download()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_files.md)
and
[`fabric_onelake_upload()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_files.md).
They serialize data frames, tibbles, and lazy Arrow inputs without
collecting the complete object in R memory, and decode supported OneLake
files directly to a tibble or Arrow stream.

## Usage

``` r
fabric_onelake_read_file(
  workspace,
  item = NULL,
  path = "",
  format = c("auto", "parquet", "csv", "arrow"),
  result = c("tibble", "arrow_stream"),
  item_type = NULL,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  token = NULL,
  auth_args = list(),
  dfs_base = "https://onelake.dfs.fabric.microsoft.com"
)

fabric_onelake_write_file(
  workspace,
  item = NULL,
  path = "",
  data,
  format = c("auto", "parquet", "csv", "arrow"),
  overwrite = FALSE,
  if_match = NULL,
  compression = "snappy",
  include_header = TRUE,
  na = "",
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
```

## Arguments

- workspace:

  Workspace name, ID, record from
  [`fabric_workspaces()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_workspaces.md),
  or a complete OneLake HTTPS/ABFSS path.

- item:

  Item name, GUID, or discovered Fabric item. Use `NULL` when
  `workspace` is a complete OneLake path.

- path:

  Item-relative path, normally below `Files/`.

- format:

  File format. `"auto"` infers `"parquet"`, `"csv"`, or `"arrow"` from
  the path extension.

- result:

  Return a `"tibble"` or a disk-backed, single-use `"arrow_stream"`.

- item_type:

  Optional Fabric item type used to resolve a named item.

- tenant_id:

  Entra tenant ID. Defaults to `FABRICQUERYR_TENANT_ID`.

- client_id:

  Entra application ID. Defaults to `FABRICQUERYR_CLIENT_ID`, then the
  Azure CLI application ID.

- token:

  Optional access token or audience-aware token-provider function.

- auth_args:

  Additional sign-in options passed to
  [`AzureAuth::get_azure_token()`](https://rdrr.io/pkg/AzureAuth/man/get_azure_token.html).

- dfs_base:

  OneLake DFS service address. A private or regional endpoint on a
  discovered record is preferred when this argument is omitted.

- data:

  A data frame, tibble, Arrow Table/RecordBatch, lazy Arrow
  Dataset/Scanner/query, RecordBatchReader, or Arrow-compatible array
  stream.

- overwrite:

  Whether an existing OneLake file may be replaced.

- if_match:

  Optional destination ETag for conditional replacement.

- compression:

  Parquet compression codec passed to Arrow.

- include_header:

  Whether a written CSV includes column names.

- na:

  Text used for missing values in a written CSV.

- create_parents:

  Whether missing parent directories are created.

- allow_managed_tables:

  Whether direct writes below `Tables/` are permitted. Keep the safe
  default, `FALSE`, for managed Delta tables.

- chunk_size:

  Upload chunk size in bytes.

## Value

`fabric_onelake_read_file()` returns a tibble or a disk-backed
`nanoarrow_array_stream`. `fabric_onelake_write_file()` returns OneLake
metadata with additional `format`, `rows`, and `columns` fields.

## References

[Connect to OneLake with ADLS
APIs](https://learn.microsoft.com/en-us/fabric/onelake/onelake-access-api)

[Get data into
OneLake](https://learn.microsoft.com/en-us/fabric/onelake/quickstart-get-data)

## Examples

``` r
if (FALSE) { # \dontrun{
lakehouse <- fabric_lakehouses("Analytics")[[1L]]
fabric_onelake_write_file(
  lakehouse$workspaceId,
  lakehouse,
  "Files/exports/orders.parquet",
  data.frame(id = 1:3, amount = c(10.5, NA, 30))
)
orders <- fabric_onelake_read_file(
  lakehouse$workspaceId,
  lakehouse,
  "Files/exports/orders.parquet"
)
} # }
```
