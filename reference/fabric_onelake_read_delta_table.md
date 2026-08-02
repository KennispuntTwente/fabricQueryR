# Read a Delta table from Microsoft Fabric OneLake

`fabric_onelake_read_delta_table()` resolves a Fabric Lakehouse or
Warehouse table, authenticates to OneLake, and reads the selected Delta
snapshot. The Delta transaction log and Parquet data are interpreted by
delta-rs (through the Python
'[deltalake](https://pypi.org/project/deltalake/)' package) and returned
as a tibble or a lazy Arrow stream.

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
  timestamp_partition_timezone = NULL,
  dest_dir = NULL,
  verbose = TRUE,
  dfs_base = "https://onelake.dfs.fabric.microsoft.com",
  columns = NULL,
  limit = NULL,
  result = c("tibble", "arrow_stream")
)
```

## Arguments

- table_path:

  Table name. For backward compatibility, a slash-separated value is
  accepted and its final segment is used; select a schema with `schema`.

- workspace_name:

  Fabric workspace display name or GUID, or a record from
  [`fabric_workspaces()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_workspaces.md).
  ABFSS-safe display names can be used directly; use paired
  workspace/item GUIDs or discovery records when the display name
  contains spaces or other special characters.

- lakehouse_name:

  Lakehouse or Warehouse name, GUID, or discovery record. The argument
  name is retained for backward compatibility.

- schema:

  Lakehouse or Warehouse schema, or `NULL`. A discovered schema-enabled
  Lakehouse's default schema is used automatically.

- item_type:

  `"Lakehouse"` or `"Warehouse"`. This is inferred from a discovery
  record or a `.Lakehouse`/`.Warehouse` suffix. Supply it for a
  suffixless item display name, especially a Warehouse name.

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

  Optional non-negative Delta transaction version. Values through `2^53`
  are represented exactly.

- timestamp_partition_timezone:

  Deprecated compatibility argument. delta-rs does not expose the
  previous R engine's timezone override; non-`NULL` values are rejected.

- dest_dir:

  Deprecated compatibility argument. Data is no longer staged locally. A
  non-`NULL` value is ignored with a warning.

- verbose:

  Logical. Show authentication and read progress.

- dfs_base:

  OneLake DFS endpoint. Use the workspace capacity's regional endpoint
  when endpoint-resolution data residency matters, or its required
  workspace-private FQDN for a workspace private link.

- columns:

  Optional character vector of logical Delta columns, in the requested
  order. `NULL` returns all columns.

- limit:

  Optional non-negative whole number limiting returned rows. No ordering
  is applied, so a partial result is an implementation-defined subset
  and is not suitable for stable pagination.

- result:

  `"tibble"` or `"arrow_stream"`.

## Value

A tibble, or a lazy single-use `nanoarrow_array_stream` compatible with
[`arrow::as_record_batch_reader()`](https://arrow.apache.org/docs/r/reference/as_record_batch_reader.html).

## Details

`deltalake` and `nanoarrow` are declared with
[`reticulate::py_require()`](https://rstudio.github.io/reticulate/reference/py_require.html)
when fabricQueryR loads, but Python is not started and packages are not
downloaded until this function is called. If `RETICULATE_PYTHON` selects
a user-managed environment, install the runtime dependencies in that
environment with:

OneLake credentials use the `https://storage.azure.com/.default`
audience and are passed to delta-rs as a bearer token with its Fabric
endpoint option enabled.

OneLake data access is separate from item visibility. A generic Fabric
item `Read` grant exposes metadata but is not sufficient for a direct
Delta read. Workspace Admin, Member, and Contributor roles have broad
OneLake access. Otherwise grant item `Read` plus `ReadAll`, or, when
OneLake security is enabled, item `Read` plus a OneLake role whose
`Read` scope contains the target table. Tables protected by OneLake row-
or column-level security can be blocked because this external delta-rs
reader does not enforce those policies. See the [Fabric permission
model](https://learn.microsoft.com/en-us/fabric/security/permission-model).

Fabric Warehouse publishes Delta logs asynchronously. Reads therefore
return the latest snapshot published to OneLake, which can lag the
current Warehouse transaction state or remain fixed while Delta-log
publishing is paused.

`result = "arrow_stream"` is lazy and single-use. Opening either result
is retried once with a refreshable credential when delta-rs reports an
authentication failure. Failures that occur after a lazy stream has been
returned and consumed cannot be retried.

`limit` is pushed down without an ordering expression. When it is
smaller than the snapshot, the returned rows are an
implementation-defined subset and can change with file layout, snapshot
version, or scan scheduling. It is not a stable pagination mechanism.

`dfs_base` defaults to OneLake's global endpoint. Microsoft notes that
data can leave the workspace's region during global-endpoint resolution.
For data-residency requirements, pass the workspace capacity's regional
endpoint, such as `https://westeurope-onelake.dfs.fabric.microsoft.com`;
use the workspace FQDN required by a workspace private link where
applicable. See [Connecting to Microsoft
OneLake](https://learn.microsoft.com/en-us/fabric/onelake/onelake-access-api).

The tested delta-rs runtime reads ordinary Delta snapshots, schema
evolution, typed partitions, classic checkpoints, column mapping,
deletion vectors, and shallow clones. Files that actually carry deletion
vectors must contain no more than 65,536 physical rows; larger or
unreadable vectors are rejected because the selected runtime can apply
their masks at incorrect record-batch offsets. Large files without
deletion vectors are not rejected. The pinned `deltalake` API
materializes deletion-vector masks while enumerating affected files, so
this preflight has memory cost proportional to those masks. Its current
reader also does not support Fabric tables requiring Type Widening, V2
Checkpoints, or Fabric's VariantShreddingPreview; those fail with
`fabric_delta_unsupported_feature_error`. Arrow Variant extension
columns in otherwise readable tables require `result = "arrow_stream"`.
Feature availability is protocol- and runtime-specific; consult the
[Fabric Delta interoperability
matrix](https://learn.microsoft.com/en-us/fabric/fundamentals/delta-lake-interoperability)
and use Fabric PySpark when the selected delta-rs runtime cannot read a
table.

Delta `integer` values normally use R integers and Delta `long` values
normally use
[`bit64::integer64()`](https://bit64.r-lib.org/reference/bit64-package.html).
Because those R representations reserve the respective minimum value as
`NA`, a column containing Delta's valid `-2147483648` integer is widened
to an exact R double and a column containing Delta's valid
`-9223372036854775808` long uses the exact character-backed
`fabric_delta_integer64` class. This applies recursively to nested
values. Delta decimals are returned as exact character values, including
when nested. Delta `timestamp_ntz` values use the character-backed
`fabric_delta_timestamp_ntz` class. The Arrow stream preserves
timezone-free timestamps as Arrow timestamps and represents decimals as
strings, matching the R result's exact-decimal contract. Nullable struct
columns retain their parent validity through the
`fabric_delta_struct_column` class, so a null struct remains distinct
from a present struct whose children are all null. For tables the
runtime can open, Arrow Variant extension columns are preserved by
`result = "arrow_stream"`; tibble collection rejects them explicitly
because exposing their physical `metadata` and `value` buffers as
ordinary R data would be misleading.

## Examples

``` r
if (FALSE) { # \dontrun{
rows <- fabric_onelake_read_delta_table(
  table_path = "PatientInfo",
  workspace_name = "PatientsWorkspace",
  lakehouse_name = "Clinical.Lakehouse",
  schema = "dbo",
  columns = c("PatientId", "Status"),
  limit = 1000
)

stream <- fabric_onelake_read_delta_table(
  table_path = "PatientInfo",
  workspace_name = "PatientsWorkspace",
  lakehouse_name = "Clinical.Lakehouse",
  schema = "dbo",
  result = "arrow_stream"
)
reader <- arrow::as_record_batch_reader(stream)
} # }
```
