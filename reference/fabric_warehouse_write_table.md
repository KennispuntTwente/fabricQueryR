# Write an R or Arrow object to a Fabric Warehouse table

Serializes a data frame, tibble, or Arrow object to bounded Parquet
parts, stages them in a Lakehouse, and loads them into a Fabric
Warehouse table. Existing tables use the Warehouse `COPY INTO` command.
When creation or drop-based replacement is requested,
`CREATE TABLE AS SELECT` (CTAS) creates and loads the table directly
from the staged Parquet schema. Lazy Arrow inputs are consumed as record
batches and are not first collected into an R data frame.

## Usage

``` r
fabric_warehouse_write_table(
  warehouse,
  table,
  data,
  staging_lakehouse,
  workspace = NULL,
  staging_workspace = NULL,
  schema = "dbo",
  mode = c("Append", "Overwrite"),
  overwrite_method = c("Truncate", "Drop"),
  create_if_missing = FALSE,
  staging_root = "Files/fabricqueryr-staging",
  cleanup = TRUE,
  keep_staging_on_failure = TRUE,
  compression = "snappy",
  target_file_size = 512 * 1024^2,
  max_rows_per_file = NULL,
  backend = c("odbc", "adbc"),
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  token = NULL,
  auth_args = list(),
  api_base = .fabric_api_base,
  dfs_base = "https://onelake.dfs.fabric.microsoft.com",
  verbose = TRUE,
  storage_token = NULL,
  sql_token = NULL
)
```

## Arguments

- warehouse:

  A Warehouse object returned by
  [`fabric_warehouses()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md)
  or
  [`fabric_item()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_item.md),
  or its name or GUID when `workspace` is supplied.

- table:

  Destination table name.

- data:

  A data frame, tibble, Arrow Table, RecordBatch, Dataset, Scanner,
  RecordBatchReader, Arrow 'dplyr' query, or Arrow-compatible array
  stream.

- staging_lakehouse:

  A Lakehouse object returned by
  [`fabric_lakehouses()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md)
  or
  [`fabric_item()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_item.md),
  or its name or GUID. Fabric does not support a Warehouse item as the
  OneLake source of `COPY INTO`, so a Lakehouse staging item is
  required.

- workspace:

  Workspace name, GUID, or discovery object containing `warehouse`. May
  be omitted when `warehouse` is a discovery object.

- staging_workspace:

  Workspace containing `staging_lakehouse`. Defaults to the Warehouse
  workspace. May be omitted when `staging_lakehouse` is a discovery
  object.

- schema:

  Destination schema. Defaults to `"dbo"`.

- mode:

  `"Append"` adds rows. `"Overwrite"` replaces the table contents using
  `overwrite_method`.

- overwrite_method:

  For `mode = "Overwrite"`, `"Truncate"` preserves the existing table
  definition and loads it with `COPY INTO`; `"Drop"` drops and recreates
  the table from the staged Parquet schema with CTAS. Drop replacement
  also removes table-specific metadata such as constraints and grants.
  Ignored for append mode.

- create_if_missing:

  Whether to create and load a missing destination with CTAS. The
  default preserves the previous requirement that append and
  truncate-overwrite targets already exist. Drop-overwrite recreates an
  existing table; set this argument to `TRUE` if it may be absent.

- staging_root:

  Lakehouse path below `Files/` used for temporary Parquet directories.

- cleanup:

  Whether to remove remote staging after confirmed success.

- keep_staging_on_failure:

  Whether to retain staged files after a confirmed pre-load failure.
  Staging is always retained when SQL execution might have reached the
  Warehouse.

- compression:

  Parquet compression codec passed to Arrow.

- target_file_size:

  Soft maximum size in bytes for each staged Parquet part. Fabric
  recommends files between 100 MB and 1 GB for Warehouse loads.

- max_rows_per_file:

  Optional exact maximum rows per staged part.

- backend:

  SQL connection backend, `"odbc"` or `"adbc"`.

- tenant_id:

  Microsoft Entra tenant ID. Defaults to `FABRICQUERYR_TENANT_ID`

- client_id:

  Microsoft Entra application/client ID. Defaults to
  `FABRICQUERYR_CLIENT_ID`, then the Azure CLI application ID

- token:

  Optional access token or token-provider function. Leave `NULL` to let
  'fabricQueryR' use its normal sign-in flow

- auth_args:

  Additional sign-in options passed to
  [`AzureAuth::get_azure_token()`](https://rdrr.io/pkg/AzureAuth/man/get_azure_token.html)

- api_base:

  Fabric REST API base used when a Warehouse or staging Lakehouse name
  or GUID must be discovered.

- dfs_base:

  OneLake service address. Most users should keep the default; a
  workspace-specific address discovered from Fabric is used when
  available

- verbose:

  Whether to report SQL connection progress.

- storage_token:

  Optional separate Azure Storage token or token-provider function.
  Supply it when `token` is fixed rather than audience-aware.

- sql_token:

  Optional separate Azure SQL token or token-provider function. Supply
  it when `token` is fixed rather than audience-aware.

## Value

A `fabric_warehouse_write_result` list containing destination and
staging identifiers, row and byte counts, part paths, and cleanup state.

## Details

Existing-table writes map input fields by ordinal position to quoted
destination columns whose names must exactly match the names in `data`,
including letter case. The writer checks the Warehouse catalog before
any destructive SQL is issued. With `create_if_missing = TRUE`, a
missing table is created and populated by a single CTAS statement;
Fabric infers its names and types from the staged Parquet files.

Truncate overwrite preserves the table definition. Drop overwrite
recreates the table and therefore intentionally discards its previous
constraints, indexes, permissions, and other table-level metadata. Both
overwrite paths run in an explicit Warehouse transaction and roll back
on a confirmed SQL failure.

`COPY INTO` authenticates to OneLake as the identity executing the SQL
statement. That identity needs read access to the staged Lakehouse files
and the Warehouse T-SQL permissions required by the selected mode,
including the applicable bulk-load, DML, and DDL permissions. The
identity used to stage and clean up files also needs OneLake write
access to the staging folder. Contributor access to both workspaces is a
simple sufficient setup, but it is not required when equivalent granular
item, OneLake, and T-SQL permissions are granted.

Local staging is always removed. Remote staging is removed only after a
confirmed successful load unless `keep_staging_on_failure = FALSE` and
the failure occurred before SQL execution. Retaining files after an
ambiguous SQL error makes a retry or investigation possible without
changing the source while `COPY INTO` might still be completing.

## References

[COPY INTO in Fabric
Warehouse](https://learn.microsoft.com/en-us/sql/t-sql/statements/copy-into-transact-sql?view=fabric)

[Warehouse ingestion performance
guidance](https://learn.microsoft.com/en-us/fabric/data-warehouse/guidelines-warehouse-performance)

[Transactions in Fabric
Warehouse](https://learn.microsoft.com/en-us/fabric/data-warehouse/transactions)

[Create tables in Fabric
Warehouse](https://learn.microsoft.com/en-us/fabric/data-warehouse/create-table)

[Query Parquet files in Fabric
Warehouse](https://learn.microsoft.com/en-us/fabric/data-warehouse/query-parquet-files)

[OneLake security access-control
model](https://learn.microsoft.com/en-us/fabric/onelake/security/data-access-control-model)

[Warehouse
permissions](https://learn.microsoft.com/en-us/fabric/data-warehouse/share-warehouse-manage-permissions)

## Examples

``` r
if (FALSE) { # \dontrun{
# Discover both the destination Warehouse and staging Lakehouse
workspace <- fabric_workspaces()[[1L]]
warehouse <- fabric_warehouses(workspace)[[1L]]
staging <- fabric_lakehouses(workspace)[[1L]]

# Upload through OneLake staging and create a new Warehouse table
fabric_warehouse_write_table(
  warehouse,
  "orders_from_r",
  data.frame(id = 1:3, amount = c(10, 20, 30)),
  staging_lakehouse = staging,
  create_if_missing = TRUE
)
} # }
```
