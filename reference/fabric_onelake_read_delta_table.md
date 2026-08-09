# Read a Delta table from OneLake

Read a Delta table from Microsoft Fabric OneLake using the Python
[deltalake](https://pypi.org/project/deltalake/) package through the
`reticulate` R package. Return a tibble by default or a lazy Arrow
stream for batch processing. Column selection, row limits, and table
version reads are supported.

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

  Table name without a schema. Use `schema` separately when needed.

- workspace_name:

  Workspace name, ID, or a record returned by
  [`fabric_workspaces()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_workspaces.md).

- lakehouse_name:

  Lakehouse name, ID, or discovery record. Compatible Warehouse items
  are also accepted.

- schema:

  Schema containing the table, or `NULL`. Warehouses default to `"dbo"`.

- item_type:

  `"Lakehouse"`, `"Warehouse"`, or `NULL`. Usually inferred; specify it
  only when using an item name without a type suffix.

- tenant_id:

  Microsoft Entra tenant ID. Defaults to `FABRICQUERYR_TENANT_ID`.

- client_id:

  Microsoft Entra application/client ID. Defaults to
  `FABRICQUERYR_CLIENT_ID`, then the Azure CLI application ID.

- token:

  Optional access token or token-provider function. Most users can leave
  this as `NULL` and let fabricQueryR sign in.

- auth_args:

  Extra sign-in options passed to
  [`AzureAuth::get_azure_token()`](https://rdrr.io/pkg/AzureAuth/man/get_azure_token.html).

- version:

  Specific table version to read, or `NULL` for the latest.

- verbose:

  Whether to show authentication and read progress.

- dfs_base:

  Canonical HTTPS OneLake DFS origin, without credentials, path, query,
  or fragment. When omitted, a DFS endpoint returned by Fabric discovery
  is preferred over the global default.

- columns:

  Column names to return, or `NULL` for all columns.

- limit:

  Maximum number of rows to return, or `NULL` for all rows.

- result:

  `"tibble"` (the default) or `"arrow_stream"` for batch processing.

## Value

A tibble, or a disk-backed, lazy, single-use Arrow stream when
`result = "arrow_stream"`.

## Details

Most users only need to provide the table, workspace, and Lakehouse.
These can be names, IDs, or records returned by fabricQueryR's discovery
functions. If the Lakehouse uses schemas, provide the table's `schema`
separately.

By default, the function reads all rows and columns into memory. Use
`columns` to select only the fields you need and `limit` for a quick
preview. A limit does not guarantee which rows are returned. Use
`version` to read an earlier version of the table.

For a large table, or one containing nested data, set
`result = "arrow_stream"` to process the result in batches. The remote
data is staged in a temporary Arrow IPC file while the OneLake token is
current, and the returned disk-backed stream is lazy and can be read
only once. This avoids token expiry between creating and consuming a
stream, and keeps the result out of R memory, but requires enough
temporary disk space for the selected data. The temporary file is
removed when the stream is released.

Direct reads require OneLake data access; item `Read` permission by
itself is not enough. The caller needs `ReadAll` or a suitable OneLake
data-access role, and the tenant setting for external OneLake apps must
be enabled. Callers restricted by row- or column-level security must use
a supported Fabric engine instead. See the [Fabric permission
model](https://learn.microsoft.com/en-us/fabric/security/permission-model)
and [OneLake tenant
settings](https://learn.microsoft.com/en-us/fabric/admin/service-admin-portal-onelake).

Some tables use advanced Delta features that the deltalake Python
package does not support. The function will detect these features and
abort. Unsupported features include Type Widening, V2 Checkpoints, and
Fabric Variant. Use the SQL or Spark (Livy) functions to read these
tables.

Fabric publishes Warehouse user tables as read-only Delta logs
specifically for access by other engines, so Warehouse access is a
Fabric-supported scenario. This function nevertheless depends on its
pinned Python `deltalake` runtime and is limited to the Delta reader
features implemented by that package. A `fabric_delta_unsupported_error`
for a Warehouse table is therefore a fabricQueryR/runtime
interoperability limit, not a statement that Fabric Warehouse lacks open
Delta access. Use
[`fabric_sql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_query.md)
for the Warehouse when the pinned reader cannot open its table protocol.

## Examples

``` r
if (FALSE) { # \dontrun{
patients <- fabric_onelake_read_delta_table(
  table_path = "Patients",
  workspace_name = "PatientsWorkspace",
  lakehouse_name = "Clinical.Lakehouse"
)

stream <- fabric_onelake_read_delta_table(
  table_path = "Patients",
  workspace_name = "PatientsWorkspace",
  lakehouse_name = "Clinical.Lakehouse",
  columns = c("PatientId", "Status"),
  result = "arrow_stream"
)
reader <- arrow::as_record_batch_reader(stream)
} # }
```
