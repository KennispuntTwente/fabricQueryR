# Read a Delta table from Microsoft Fabric OneLake

Read a Delta table from a Fabric Lakehouse or Warehouse. By default, the
result is returned as a tibble. Use `columns` and `limit` to read less
data, or request an Arrow stream to process a large result in batches.

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

  Name of the table to read.

- workspace_name:

  Workspace name, ID, or a record from
  [`fabric_workspaces()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_workspaces.md).

- lakehouse_name:

  Lakehouse or Warehouse name, ID, or discovery record.

- schema:

  Optional schema name. Warehouses default to `"dbo"`.

- item_type:

  `"Lakehouse"` or `"Warehouse"`. Usually inferred; specify it when
  using an item name without a `.Lakehouse` or `.Warehouse` suffix.

- tenant_id:

  Microsoft Entra tenant ID. Defaults to `FABRICQUERYR_TENANT_ID`.

- client_id:

  Microsoft Entra application/client ID. Defaults to
  `FABRICQUERYR_CLIENT_ID`, then the Azure CLI application ID.

- token:

  Optional
  [`AzureAuth::AzureToken`](https://rdrr.io/pkg/AzureAuth/man/AzureToken.html),
  bearer-token string, or token-provider function.

- auth_args:

  Named list passed to
  [`AzureAuth::get_azure_token()`](https://rdrr.io/pkg/AzureAuth/man/get_azure_token.html)
  when fabricQueryR acquires a token.

- version:

  Optional Delta snapshot version to read.

- verbose:

  Logical. Show authentication and read progress.

- dfs_base:

  OneLake DFS endpoint. Most users can keep the default.

- columns:

  Optional character vector of columns to return. `NULL` returns all
  columns.

- limit:

  Optional maximum number of rows to return.

- result:

  Return a `"tibble"` or a lazy `"arrow_stream"`.

## Value

A tibble by default. With `result = "arrow_stream"`, a lazy, single-use
stream compatible with
[`arrow::as_record_batch_reader()`](https://arrow.apache.org/docs/r/reference/as_record_batch_reader.html).

## Details

Supply the table, workspace, and Lakehouse or Warehouse. Discovery
records returned by fabricQueryR can be used instead of names. The
`schema` and `item_type` are usually inferred when discovery records or
item suffixes are used.

Direct reads require OneLake data access; item `Read` permission by
itself is not enough. The caller needs `ReadAll` or a suitable OneLake
data-access role, and the tenant setting for external OneLake apps must
be enabled. This function cannot apply OneLake row- or column-level
security, so OneLake blocks reads for callers restricted by those
policies. See the [Fabric permission
model](https://learn.microsoft.com/en-us/fabric/security/permission-model)
and [OneLake tenant
settings](https://learn.microsoft.com/en-us/fabric/admin/service-admin-portal-onelake).

The first call may take longer while the required Python packages are
set up. If `RETICULATE_PYTHON` points to an environment you manage,
install the versions reported by
[`fabric_delta_config()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_delta_config.md)
in that environment.

A tibble is loaded fully into memory. `result = "arrow_stream"` instead
returns a lazy, single-use stream for batch processing. Consume it
promptly, because it uses the access token captured when the stream was
opened.

`limit` does not define an order. It is useful for previews, but not for
stable pagination. Use `version` to read a specific Delta snapshot.

Warehouse Delta snapshots are published asynchronously and current
exports require Deletion Vectors, which this function does not support.
See [Delta Lake logs in
Warehouse](https://learn.microsoft.com/en-us/fabric/data-warehouse/query-delta-lake-logs).

Tables that require Deletion Vectors, Type Widening, V2 Checkpoints, or
Fabric Variant preview features are not currently supported. Use Fabric
SQL or PySpark when broader Delta feature support is needed.

Tibble collection supports common scalar columns. Delta `integer` values
are returned as doubles, while `long`, decimal, and `timestamp_ntz`
values are returned as character vectors to avoid R sentinel and
precision ambiguity. Nested and extension columns require
`result = "arrow_stream"`.

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
