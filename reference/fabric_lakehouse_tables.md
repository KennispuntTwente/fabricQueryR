# Discover and load Microsoft Fabric Lakehouse tables

Use Fabric's table APIs to inspect Delta tables, load staged CSV or
Parquet files, or write an R data frame through a failure-aware staging
workflow.

- `fabric_lakehouse_tables()` combines Fabric's paginated List Tables
  API with the read-only OneLake Delta table API. The first supplies
  managed or external type, format, and location; the second supplies
  schemas and, with `detail = TRUE`, column metadata.

- `fabric_lakehouse_load_table()` starts the preview Fabric Load Table
  API for a file or folder that already exists below the Lakehouse
  `Files/` area. It returns a handle accepted by
  [`fabric_operation_status()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_operation_status.md).

- `fabric_lakehouse_write_table()` serializes a data frame to Parquet,
  uploads it to a unique `Files/` staging path, waits for the Delta
  load, and removes the staged file after confirmed success by default.

## Usage

``` r
fabric_lakehouse_tables(
  lakehouse,
  workspace = NULL,
  schema = NULL,
  detail = TRUE,
  page_size = NULL,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  token = NULL,
  auth_args = list(),
  api_base = .fabric_api_base,
  table_api_base = .fabric_onelake_table_base,
  allow_custom_endpoint = FALSE
)

fabric_lakehouse_load_table(
  lakehouse,
  table,
  path,
  workspace = NULL,
  schema = NULL,
  path_type = c("File", "Folder"),
  format = NULL,
  mode = c("Overwrite", "Append"),
  recursive = FALSE,
  header = TRUE,
  delimiter = ",",
  file_extension = NULL,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  token = NULL,
  auth_args = list(),
  api_base = .fabric_api_base,
  allow_custom_endpoint = FALSE
)

fabric_lakehouse_write_table(
  lakehouse,
  table,
  data,
  workspace = NULL,
  schema = NULL,
  mode = c("Overwrite", "Append"),
  staging_root = "Files/fabricqueryr-staging",
  cleanup = TRUE,
  keep_staging_on_failure = TRUE,
  compression = "snappy",
  poll_interval = NULL,
  timeout = 900,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  token = NULL,
  auth_args = list(),
  api_base = .fabric_api_base,
  dfs_base = "https://onelake.dfs.fabric.microsoft.com",
  allow_custom_endpoint = FALSE
)
```

## Arguments

- lakehouse:

  Lakehouse GUID, exact display name, or one Lakehouse record returned
  by
  [`fabric_lakehouses()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md).
  A discovered record is recommended because it includes the workspace
  and default schema.

- workspace:

  Workspace GUID, exact display name, or discovered workspace. Omit it
  when `lakehouse` is a record containing `workspaceId`.

- schema:

  Optional Lakehouse schema. When omitted from
  `fabric_lakehouse_tables()`, every schema is listed. For loading, a
  discovered schema-enabled Lakehouse supplies its documented default
  schema; otherwise provide the destination schema explicitly.

- detail:

  Whether table discovery should retrieve per-table column metadata. Set
  to `FALSE` to make only schema and table-list requests.

- page_size:

  Optional maximum records requested per table API page, from 1 to the
  Fabric List Tables maximum of 100. All continuation values are
  followed regardless of this value.

- tenant_id:

  Entra tenant ID. Defaults to `FABRICQUERYR_TENANT_ID`.

- client_id:

  Entra application ID. Defaults to `FABRICQUERYR_CLIENT_ID`, then the
  Azure CLI application ID.

- token:

  Optional access token or audience-aware token-provider function. Table
  discovery needs both Fabric- and Storage-audience tokens; staging
  needs Storage and loading needs Fabric.

- auth_args:

  Additional sign-in options passed to
  [`AzureAuth::get_azure_token()`](https://rdrr.io/pkg/AzureAuth/man/get_azure_token.html)
  when no token source is supplied.

- api_base:

  Fabric REST API base URL. Most users should keep the default.

- table_api_base:

  OneLake Delta table API base URL. Most users should keep the default.

- allow_custom_endpoint:

  Logical. Set to `TRUE` only when a supplied API base is a
  non-Microsoft HTTPS endpoint that you trust to receive a token.

- table:

  Destination Delta table name. Fabric's Load Table API permits 1 to 256
  ASCII letters, numbers, and underscores and requires at least one
  letter or underscore.

- path:

  Existing item-relative OneLake source path beginning with `Files/`,
  for example `"Files/incoming/orders.parquet"`.

- path_type:

  Whether `path` names one `"File"` or a `"Folder"`.

- format:

  Source format, `"Parquet"` or `"Csv"`. For a file, `NULL` infers the
  format from its extension. A folder should specify the format.

- mode:

  Load mode, `"Overwrite"` or `"Append"`. Overwrite and append behavior
  is performed by Fabric's managed Delta load, never by changing files
  below `Tables/` directly.

- recursive:

  Whether a folder load should include descendant folders.

- header:

  Whether the first CSV row contains column names.

- delimiter:

  CSV delimiter of 1 to 8 non-whitespace characters. Fabric does not
  allow parentheses, brackets, braces, or quotes in a delimiter.

- file_extension:

  Optional extension used to filter a folder load, without a leading
  dot.

- data:

  A data frame, tibble, or Arrow Table/RecordBatch to serialize as
  Parquet. The optional `arrow` package is required.

- staging_root:

  Item-relative directory below `Files/` used for unique staging files.

- cleanup:

  Whether to delete the staged Parquet file after Fabric confirms a
  successful load.

- keep_staging_on_failure:

  Whether to retain a completely uploaded staging file when the load
  fails. The raised condition includes `staging_path` and
  `staging_retained` fields.

- compression:

  Parquet compression passed to
  [`arrow::write_parquet()`](https://arrow.apache.org/docs/r/reference/write_parquet.html).

- poll_interval:

  Minimum seconds between load-operation status requests. `NULL` follows
  Fabric's `Retry-After` hint with the shared fallback.

- timeout:

  Maximum total seconds to wait for a data-frame load.

- dfs_base:

  OneLake DFS service address used for the staging upload. A
  workspace-specific endpoint from a discovered record is preferred when
  this argument is not supplied.

## Value

`fabric_lakehouse_tables()` returns a tibble with table `name`,
`schema`, `full_name`, `type`, `format`, `location`, timestamps,
list-column `columns`, `schema_metadata`, the unmodified OneLake `raw`
record, and the matching unmodified Fabric `fabric_raw` record. Unknown
future metadata remains available in those raw list columns.

`fabric_lakehouse_load_table()` returns a reusable `fabric_operation`.
Pass it to
[`fabric_operation_status()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_operation_status.md),
[`fabric_operation_wait()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_operation_status.md),
or
[`fabric_operation_result()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_operation_status.md).

`fabric_lakehouse_write_table()` returns a
`fabric_lakehouse_write_result` containing the destination, row count,
terminal operation state, staging path, and whether staging was
retained.

## Preview status and permissions

Microsoft marks Fabric's List Tables and Load Table routes as preview or
beta and does not recommend them for production use. Loading requires
write access to the Lakehouse and the `Lakehouse.ReadWrite.All`
delegated scope. Discovery requires `Lakehouse.Read.All` or
`Lakehouse.ReadWrite.All` for the Fabric list plus table read permission
for OneLake metadata.

Fabric currently rejects List Tables for some schema-enabled Lakehouses.
In that documented-endpoint/service mismatch, discovery still returns
OneLake schema, format, location, and column metadata; `type` can be
missing because OneLake currently returns a null table type for those
records.

Service principals and managed identities are supported by the Load
Table API. Tenant and item permissions still determine whether those
identities can use OneLake and the Lakehouse.

## Data types and names

Arrow determines the Parquet schema before Fabric infers the destination
Delta schema. Ordinary R logical, integer, double, character, `Date`,
`POSIXct`, and
[`bit64::integer64`](https://bit64.r-lib.org/reference/bit64-package.html)
columns map to their corresponding Parquet logical types. Factors are
written as strings. List columns are passed to Arrow as nested data and
can fail if their values do not have one consistent Arrow type. R
complex and `difftime` columns are rejected.

R has no native fixed-precision decimal vector. Supply an Arrow Table
with a decimal field when decimal precision and scale must be explicit.
Fabric's Load to Tables flow does not accept a caller-defined
destination schema, so use Spark or another schema-controlled writer
when inference is unsuitable.

To preserve names exactly, `fabric_lakehouse_write_table()` requires
unique column names containing only Unicode letters, numbers, and
underscores, up to Fabric's documented 128-character limit.

## Failure and cleanup behavior

The high-level writer uploads one complete Parquet file atomically to a
unique path and starts the managed load only after that upload succeeds.
A successful load is a committed Delta operation. On failure, the
destination is left to Fabric's transactional load behavior and
fabricQueryR never edits `Tables/` files.

Retained staging paths are included in `fabric_lakehouse_write_error`
conditions so the source can be inspected or passed to
`fabric_lakehouse_load_table()` again. Cleanup failures after a
successful load produce a warning and return `staging_retained = TRUE`;
they do not make a committed table load appear to have failed.

## References

[OneLake table APIs for
Delta](https://learn.microsoft.com/en-us/fabric/onelake/table-apis/delta-table-apis-overview)

[Getting started with OneLake Delta table
APIs](https://learn.microsoft.com/en-us/fabric/onelake/table-apis/delta-table-apis-get-started)

[List Lakehouse
tables](https://learn.microsoft.com/en-us/rest/api/fabric/lakehouse/tables/list-tables)

[Load a Lakehouse
table](https://learn.microsoft.com/en-us/rest/api/fabric/lakehouse/tables/load-table)

[Load a schema Lakehouse table
(beta)](https://learn.microsoft.com/en-us/rest/api/fabric/lakehouse/tables/load-schema-table%28beta%29)

[Load to Delta Lake
tables](https://learn.microsoft.com/en-us/fabric/data-engineering/load-to-tables)

## Examples

``` r
if (FALSE) { # \dontrun{
lakehouse <- fabric_lakehouses("Analytics")[[1L]]

tables <- fabric_lakehouse_tables(lakehouse)

operation <- fabric_lakehouse_load_table(
  lakehouse,
  table = "orders",
  path = "Files/incoming/orders.csv",
  format = "Csv",
  header = TRUE,
  delimiter = ","
)
fabric_operation_wait(operation, timeout = 900)

result <- fabric_lakehouse_write_table(
  lakehouse,
  table = "orders_from_r",
  data = data.frame(id = 1:3, amount = c(10.5, NA, 30))
)
result$operation_status$status
} # }
```
