# Read a Delta table from a Microsoft Fabric Lakehouse

Downloads a Lakehouse Delta table from OneLake and returns it as a
tibble. Delta tables consist of Parquet data files plus a transaction
log that says which files make up the current table. This function reads
that log so that deleted or superseded files are not accidentally
included.

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
  dfs_base = "https://onelake.dfs.fabric.microsoft.com"
)
```

## Arguments

- table_path:

  Table name, for example `"PatientInfo"`. For backward compatibility a
  nested string is accepted, but only its final segment is used; select
  a schema with `schema`, not by adding it to `table_path`.

- workspace_name:

  Fabric workspace display name or GUID, or a row from
  [`fabric_workspaces()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_workspaces.md).
  GUIDs are safest for scheduled code and names are convenient
  interactively.

- lakehouse_name:

  Lakehouse item name or GUID, or a row from
  [`fabric_lakehouses()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md).
  A character name may include the `.Lakehouse` suffix; a discovered row
  avoids suffix and renaming ambiguity.

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
  exit. Supply a directory to retain the downloaded files for inspection
  or reuse, and ensure it has enough free space.

- verbose:

  Logical. Show download and read progress.

- dfs_base:

  OneLake DFS endpoint. Keep the default unless using a regional or
  workspace-private endpoint.

## Value

A tibble containing the rows and logical schema of the selected Delta
snapshot. An empty table returns a zero-row tibble. Delta/R type
conversion follows DuckDB; schema evolution is applied and partition
values are included as columns.

## Details

- In Microsoft Fabric, OneLake exposes each workspace as an ADLS Gen2
  filesystem. Within a Lakehouse item, Delta tables are stored under
  `Tables/<table>` (non-schema lakehouse) or `Tables/<schema>/<table>`
  (schema-enabled lakehouse). The function first stages the transaction
  log, then downloads only the Parquet files active in the requested
  version.

- Checkpoint Parquet and data Parquet files are read with DuckDB. The
  staged reader supports Delta reader protocols 1 through 3, name-based
  column mapping, deletion vectors stored inline or in table-relative
  sidecar files, timestamps without time zones, and supported type
  widening. This covers the reader 3/writer 7 format currently emitted
  by Fabric Warehouse Delta export. ID-based column mapping, absolute
  deletion-vector paths, v2 checkpoints, and unrecognised reader
  features are rejected with a `fabric_delta_unsupported_error` before
  any data is returned.

- The returned columns follow the logical schema in the selected Delta
  snapshot. Schema additions are filled with typed missing values,
  removed physical columns are omitted, and partition values come from
  Delta add-file actions rather than being inferred from directory
  names.

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
  collected into R memory. For very large tables, a SQL query that
  filters rows in Fabric may transfer much less data.

## References

[Connect to OneLake with ADLS
APIs](https://learn.microsoft.com/en-us/fabric/onelake/onelake-access-api)

[Lakehouse
schemas](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-schemas)

[Delta Lake tables in
OneLake](https://learn.microsoft.com/en-us/fabric/fundamentals/delta-lake-interoperability)

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
  schema         = "dbo"
)
} # }
```
