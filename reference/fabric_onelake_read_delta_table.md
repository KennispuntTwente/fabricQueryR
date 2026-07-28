# Read a Delta table from Microsoft Fabric OneLake

Downloads a Lakehouse or Warehouse-exported Delta table from OneLake and
returns it as a tibble. Delta tables consist of Parquet data files plus
a transaction log that says which files make up the current table. This
function reads that log so deleted or superseded files are not
accidentally included.

## Usage

``` r
fabric_onelake_read_delta_table(
  table_path,
  workspace_name,
  lakehouse_name,
  schema = NULL,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  token = NULL,
  auth_args = list(),
  version = NULL,
  dest_dir = NULL,
  verbose = TRUE,
  dfs_base = "https://onelake.dfs.fabric.microsoft.com",
  columns = NULL,
  limit = NULL
)
```

## Arguments

- table_path:

  Table name, for example `"PatientInfo"`. For backward compatibility a
  nested string is accepted, but only its final segment is used; select
  a schema with `schema`, not by adding it to `table_path`.

- workspace_name:

  Fabric workspace display name or GUID, or a record from
  [`fabric_workspaces()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_workspaces.md).
  GUIDs are safest for scheduled code and names are convenient
  interactively.

- lakehouse_name:

  Lakehouse or Warehouse item name, GUID, or discovery record. A
  character name may include its `.Lakehouse` or `.Warehouse` suffix; a
  discovered record avoids suffix and renaming ambiguity. The argument
  name is retained for backward compatibility.

- schema:

  Lakehouse schema name, for example `"dbo"`, or `NULL`. When supplied,
  the table is resolved under `Tables/<schema>/<table>` instead of
  `Tables/<table>`. When `lakehouse_name` is a discovered schema-enabled
  Lakehouse and `schema` is `NULL`, its `defaultSchema` is used
  automatically. Use `NULL` with a name or GUID for a non-schema
  Lakehouse. Schema support in this reader is experimental.

- tenant_id:

  Microsoft Entra tenant ID. Defaults to `FABRICQUERYR_TENANT_ID`.

- client_id:

  Microsoft Entra application/client ID. Defaults to
  `FABRICQUERYR_CLIENT_ID`, then the Azure CLI application ID.

- token:

  Optional
  [`AzureAuth::AzureToken`](https://rdrr.io/pkg/AzureAuth/man/AzureToken.html),
  bearer-token string, or token-provider function. With `NULL`,
  `AzureAuth` reuses a matching cached token or starts its normal
  interactive login flow.

- auth_args:

  Named list of additional arguments passed to
  [`AzureAuth::get_azure_token()`](https://rdrr.io/pkg/AzureAuth/man/get_azure_token.html)
  when no token source is supplied.

- version:

  Optional non-negative Delta transaction version. `NULL` reads the
  latest snapshot; supplying a version provides time travel when that
  version and its active files are still available in OneLake. Versions
  through `2^53` are represented exactly; larger versions are rejected.

- dest_dir:

  Local staging directory for the Delta log and active data files, or
  `NULL`. The default creates a temporary directory and removes it on
  exit. Supply a new or empty directory to retain the downloaded files
  for inspection, and ensure it has enough free space. Non-empty
  directories are rejected so stale files cannot affect snapshot
  resolution.

- verbose:

  Logical. Show download and read progress.

- dfs_base:

  OneLake DFS endpoint. Keep the default unless using a regional or
  workspace-private endpoint.

- columns:

  Optional character vector of logical Delta column names to return, in
  the requested order. `NULL` returns every column.

- limit:

  Optional non-negative whole number limiting returned rows. `NULL`
  returns every row. This limits DuckDB collection but not OneLake file
  downloads.

## Value

A tibble containing the rows and logical schema of the selected Delta
snapshot. An empty table returns a zero-row tibble. Delta `long` columns
use
[`bit64::integer64`](https://bit64.r-lib.org/reference/bit64-package.html);
decimal columns use exact character values; other conversions follow
DuckDB. Schema evolution is applied and partition values are included as
columns.

## Details

- In Microsoft Fabric, OneLake exposes each workspace as an ADLS Gen2
  filesystem. Within a Lakehouse item, Delta tables are stored under
  `Tables/<table>` (non-schema lakehouse) or `Tables/<schema>/<table>`
  (schema-enabled lakehouse). The function first stages the transaction
  log, then downloads only the Parquet files active in the requested
  version.

- Checkpoint and data Parquet files are read with DuckDB. The staged
  reader supports Delta reader protocols 1 through 3; classic,
  multipart, and V2 checkpoints; name- and ID-based column mapping;
  inline, relative, and absolute deletion vectors; timestamps without
  time zones; supported type widening; and native Variant values,
  including Variant shredding. Absolute AddFile and deletion-vector URIs
  must point to Microsoft Fabric OneLake. This includes Fabric shallow
  clones and the reader 3/writer 7 Warehouse export profile.

- Unrecognised reader features, catalog-managed commits, non-OneLake
  absolute URIs, and unsupported schema types fail with a
  `fabric_delta_unsupported_error` before data is returned.

- The returned columns follow the logical schema in the selected Delta
  snapshot. Schema additions are filled with typed missing values,
  removed physical columns are omitted, and partition values come from
  Delta add-file actions rather than being inferred from directory
  names.

- Delta `long` values are returned as
  [`bit64::integer64`](https://bit64.r-lib.org/reference/bit64-package.html).
  Delta decimals are returned as character vectors, including decimals
  nested in complex types, so all 38 digits remain exact. Variant
  columns use DuckDB's native decoding and are returned as nested R
  list/data-frame values.

- Schema-enabled lakehouses (the default for new lakehouses) organise
  tables into named schemas. If the Fabric Lakehouse explorer shows the
  table under a schema such as `dbo`, supply that name in `schema`.

- Give the signed-in user or application Read access through a workspace
  role or **Lakehouse \> Manage OneLake data access**.

- Tokens use the `https://storage.azure.com/.default` audience.

- AzureAuth is used to acquire the token. Be wary of caching behavior;
  you may want to call
  [`AzureAuth::clean_token_directory()`](https://rdrr.io/pkg/AzureAuth/man/get_azure_token.html)
  if the wrong account or tenant is being reused.

- The active files are downloaded locally and the final table is
  collected into R memory. `columns` and `limit` can reduce the data
  read by DuckDB and materialised in R, but they do not reduce the
  active Parquet files downloaded from OneLake. For very large tables, a
  SQL query that filters rows in Fabric may transfer much less data.

## References

[Delta Transaction Log
Protocol](https://github.com/delta-io/delta/blob/master/PROTOCOL.md)

[Connect to OneLake with ADLS
APIs](https://learn.microsoft.com/en-us/fabric/onelake/onelake-access-api)

[Lakehouse
schemas](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-schemas)

[Delta Lake tables in
OneLake](https://learn.microsoft.com/en-us/fabric/fundamentals/delta-lake-interoperability)

[Schema evolution for Delta
tables](https://learn.microsoft.com/en-us/fabric/data-engineering/delta-lake-schema-evolution)

[Variant data type for Delta
tables](https://learn.microsoft.com/en-us/fabric/data-engineering/delta-lake-variant)

## Examples

``` r
# Example is not executed since it requires configured credentials for Fabric
if (FALSE) { # \dontrun{
df <- fabric_onelake_read_delta_table(
  table_path     = "PatientInfo",
  workspace_name = "PatientsWorkspace",
  lakehouse_name = "Lakehouse.Lakehouse",
  tenant_id      = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id      = Sys.getenv("FABRICQUERYR_CLIENT_ID")
)
dplyr::glimpse(df)

# Schema-enabled lakehouse: read from Tables/dbo/PatientInfo
df2 <- fabric_onelake_read_delta_table(
  table_path     = "PatientInfo",
  workspace_name = "PatientsWorkspace",
  lakehouse_name = "Lakehouse.Lakehouse",
  schema         = "dbo",
  columns        = c("PatientId", "Status"),
  limit          = 1000
)
} # }
```
