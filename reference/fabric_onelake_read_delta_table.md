# Read a Delta table from OneLake

Loads a Lakehouse or compatible Warehouse table into R. By default the
result is a tibble; you can select columns, preview a limited number of
rows, read an earlier table version, or return an Arrow stream for
larger results

## Usage

``` r
fabric_onelake_read_delta_table(
  table_path,
  workspace_name,
  lakehouse_name,
  schema = NULL,
  item_type = NULL,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  token = NULL,
  auth_args = list(),
  version = NULL,
  verbose = TRUE,
  dfs_base = "https://onelake.dfs.fabric.microsoft.com",
  columns = NULL,
  limit = NULL,
  result = c("tibble", "arrow_stream")
)
```

## Arguments

- table_path:

  Table name. Supply its schema separately when needed

- workspace_name:

  Workspace name, ID, or a record returned by
  [`fabric_workspaces()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_workspaces.md)

- lakehouse_name:

  Lakehouse name, ID, or discovery record. Compatible Warehouse and
  mirrored database items are also accepted

- schema:

  Schema containing the table, or `NULL`. Warehouses and mirrored
  databases default to `"dbo"` when discovery provides no default

- item_type:

  `"Lakehouse"`, `"Warehouse"`, `"MirroredDatabase"`, or `NULL`. Usually
  inferred; specify it only when using an item name without a type
  suffix

- tenant_id:

  Microsoft Entra tenant ID. Defaults to `FABRICQUERYR_TENANT_ID`

- client_id:

  Microsoft Entra application/client ID. Defaults to
  `FABRICQUERYR_CLIENT_ID`, then the Azure CLI application ID

- token:

  Optional access token or token-provider function. Most users can leave
  this as `NULL` and let fabricQueryR sign in

- auth_args:

  Extra sign-in options passed to
  [`AzureAuth::get_azure_token()`](https://rdrr.io/pkg/AzureAuth/man/get_azure_token.html)

- version:

  Specific table version to read, or `NULL` for the latest

- verbose:

  Whether to show authentication and read progress

- dfs_base:

  OneLake service address. Most users should keep the default; a
  workspace-specific address discovered from Fabric is used when
  available

- columns:

  Column names to return, or `NULL` for all columns

- limit:

  Maximum number of rows to return, or `NULL` for all rows

- result:

  `"tibble"` (the default) or `"arrow_stream"` for batch processing

## Value

A tibble, or a disk-backed, lazy, single-use Arrow stream when
`result = "arrow_stream"`

## Basic use

Supply the table name, workspace, and Lakehouse. Names, IDs, and
discovery records are accepted. If the Lakehouse uses schemas, pass the
schema name separately. The function otherwise reads the latest version
and all columns and rows into a tibble

Use `columns` to keep only the fields you need, `limit` for a quick
preview, and `version` to read an earlier version. A row limit does not
guarantee which rows are selected

## Large and nested results

For a large table, or one containing nested data, set
`result = "arrow_stream"` to process rows in batches instead of
collecting them all into R memory. The stream is disk-backed and can be
read only once, so enough temporary disk space must be available for the
selected data

## Column types

Common dates, timestamps, numbers, text, and logical values are
converted to practical R types. Values that R cannot represent exactly,
including decimal and 64-bit integer values, are returned as character
data when collecting a tibble. Nested columns require an Arrow stream.
The complete mapping is:

|  |  |  |
|----|----|----|
| Delta/Arrow source | Arrow stream result | Tibble result |
| Decimal (any precision/scale) | UTF-8 text | character |
| Large UTF-8 / large binary | UTF-8 / binary with 32-bit offsets | character / blob list-column |
| Large-list variants | list with 32-bit offsets | rejected as nested |
| Signed/unsigned 64-bit integer | original integer type | exact character |
| Signed 32-bit integer | original integer type | double |
| Timestamp without timezone | original Arrow timestamp | character |
| Timestamp with timezone | original Arrow timestamp | UTC `POSIXct` |
| Date, Boolean, floating point, smaller integers, UTF-8, binary | corresponding Arrow scalar | nanoarrow's corresponding R scalar type |
| Struct, map, list, extension/Variant | corresponding normalized Arrow type when supported | rejected; request an Arrow stream |

Decimal text retains its scale and digits. Some large Arrow buffer types
are normalized for R compatibility and may fail if one value exceeds the
supported buffer size

## Permissions and supported tables

Direct reads require OneLake data access; item `Read` permission by
itself is not enough. The caller needs `ReadAll` or a suitable OneLake
security role, and the tenant setting for external OneLake apps must be
enabled. Callers restricted by row- or column-level security must use a
supported Fabric engine instead. See the [Fabric permission
model](https://learn.microsoft.com/en-us/fabric/security/permission-model)
and [OneLake tenant
settings](https://learn.microsoft.com/en-us/fabric/admin/service-admin-portal-onelake)

This function uses the Python
[deltalake](https://pypi.org/project/deltalake/) reader through
`reticulate` Some newer Delta features, including Type Widening, V2
Checkpoints, and Fabric Variant, are not supported by that reader. Use
SQL or Spark (Livy) if the function reports an unsupported table feature

Compatible Warehouse tables can also be read through their published
Delta logs. If the reader cannot open a Warehouse table, use
[`fabric_sql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_query.md)

## Examples

``` r
if (FALSE) { # \dontrun{
# Discover a Lakehouse and one of its Delta tables
workspace <- fabric_workspaces()[[1L]]
lakehouse <- fabric_lakehouses(workspace)[[1L]]
tables <- fabric_lakehouse_tables(lakehouse)
table <- tables[1L, ]

# Read the discovered table into a tibble
rows <- fabric_onelake_read_delta_table(
  table_path = table$name[[1L]],
  workspace_name = workspace,
  lakehouse_name = lakehouse,
  schema = table$schema[[1L]]
)

# Stream the same table when it may not fit in R memory
stream <- fabric_onelake_read_delta_table(
  table_path = table$name[[1L]],
  workspace_name = workspace,
  lakehouse_name = lakehouse,
  schema = table$schema[[1L]],
  result = "arrow_stream"
)
reader <- arrow::as_record_batch_reader(stream)
} # }
```
