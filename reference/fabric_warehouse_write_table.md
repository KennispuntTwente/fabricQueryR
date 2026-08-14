# Write an R or Arrow object to a Fabric Warehouse table

Serializes a data frame, tibble, or Arrow object to bounded Parquet
parts, stages them in a Lakehouse, and loads them into an existing
Fabric Warehouse table with the Warehouse `COPY INTO` command. Lazy
Arrow inputs are consumed as record batches and are not first collected
into an R data frame.

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
  allow_custom_endpoint = FALSE,
  verbose = TRUE
)
```

## Arguments

- warehouse:

  A Warehouse record returned by
  [`fabric_warehouses()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md)
  or
  [`fabric_item()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_item.md),
  or its name or GUID when `workspace` is supplied.

- table:

  Existing destination table name.

- data:

  A data frame, tibble, Arrow Table, RecordBatch, Dataset, Scanner,
  RecordBatchReader, Arrow dplyr query, or Arrow-compatible array
  stream.

- staging_lakehouse:

  A Lakehouse record returned by
  [`fabric_lakehouses()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md)
  or
  [`fabric_item()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_item.md),
  or its name or GUID. Fabric does not support a Warehouse item as the
  OneLake source of `COPY INTO`, so a Lakehouse staging item is
  required.

- workspace:

  Workspace name, GUID, or discovery record containing `warehouse`. May
  be omitted when `warehouse` is a discovery record.

- staging_workspace:

  Workspace containing `staging_lakehouse`. Defaults to the Warehouse
  workspace. May be omitted when `staging_lakehouse` is a discovery
  record.

- schema:

  Destination schema. Defaults to `"dbo"`.

- mode:

  `"Append"` adds rows. `"Overwrite"` runs `TRUNCATE TABLE` and
  `COPY INTO` in one Warehouse transaction so a failed copy can be
  rolled back.

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
  fabricQueryR use its normal sign-in flow

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

- allow_custom_endpoint:

  Logical. Fabric SQL and Microsoft SQL Database hostnames are trusted
  by default. Set to `TRUE` only when deliberately sending the SQL
  access token to another hostname, such as a controlled proxy or test
  server

- verbose:

  Whether to report SQL connection progress.

## Value

A `fabric_warehouse_write_result` list containing destination and
staging identifiers, row and byte counts, part paths, and cleanup state.

## Details

The destination table must already exist. Input fields are mapped by
ordinal position to quoted destination columns with the same names as
`data`.

`COPY INTO` authenticates to OneLake as the identity executing the SQL
statement. That identity therefore needs the documented Warehouse
bulk-load permissions and Contributor access to the source and
destination workspaces.

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

## Examples

``` r
if (FALSE) { # \dontrun{
warehouse <- fabric_warehouses("Analytics")[[1L]]
staging <- fabric_lakehouses("Analytics")[[1L]]
fabric_warehouse_write_table(
  warehouse,
  "orders",
  data.frame(id = 1:3, amount = c(10, 20, 30)),
  staging_lakehouse = staging
)
} # }
```
