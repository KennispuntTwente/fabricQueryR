# Work with Microsoft Fabric mirrored database tables

Discover schemas and Delta tables replicated into a Fabric mirrored
database, retrieve one table's detailed metadata, or read a table
directly from OneLake. The discovery helpers use the read-only OneLake
table metadata API; the reader uses the mirrored Delta log.

## Usage

``` r
fabric_mirrored_database_schemas(
  mirrored_database,
  workspace = NULL,
  page_size = NULL,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  token = NULL,
  auth_args = list(),
  api_base = .fabric_api_base,
  table_api_base = .fabric_onelake_table_base,
  storage_token = NULL
)

fabric_mirrored_database_tables(
  mirrored_database,
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
  storage_token = NULL
)

fabric_mirrored_database_table(
  mirrored_database,
  table,
  workspace = NULL,
  schema = NULL,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  token = NULL,
  auth_args = list(),
  api_base = .fabric_api_base,
  table_api_base = .fabric_onelake_table_base,
  storage_token = NULL
)

fabric_mirrored_database_read_table(
  mirrored_database,
  table,
  workspace = NULL,
  schema = NULL,
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
  result = c("tibble", "arrow_stream"),
  api_base = .fabric_api_base,
  storage_token = NULL
)
```

## Arguments

- mirrored_database:

  Mirrored Database GUID, exact display name, or one object returned by
  [`fabric_mirrored_databases()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md).
  A discovered object is recommended because it contains workspace,
  OneLake, and SQL details.

- workspace:

  Workspace GUID, exact display name, or discovered workspace. Omit it
  when `mirrored_database` contains `workspaceId`.

- page_size:

  Optional maximum records requested per OneLake metadata page, from 1
  to 100. All continuation tokens are followed.

- tenant_id:

  Entra tenant ID. Defaults to `FABRICQUERYR_TENANT_ID`.

- client_id:

  Entra application ID. Defaults to `FABRICQUERYR_CLIENT_ID`, then the
  Azure CLI application ID.

- token:

  Optional access token or audience-aware token-provider function.

- auth_args:

  Additional sign-in options passed to `fabric_credential()`.

- api_base:

  Fabric REST API base used when an item name or GUID must be resolved.
  Most users should keep the default.

- table_api_base:

  OneLake Delta table API base URL. Most users should keep the default.

- storage_token:

  Optional separate Azure Storage token or token-provider function.
  Supply it when `token` is fixed and item lookup is needed.

- schema:

  Optional schema filter. The singular metadata and read helpers use a
  discovered default schema when available, otherwise `"dbo"`.

- detail:

  Whether table discovery should retrieve column metadata for every
  table.

- table:

  Table name, or a one-row record containing `name` and optionally
  `schema`.

- version:

  Specific Delta table version to read, or `NULL` for latest.

- verbose:

  Whether to show authentication and read progress.

- dfs_base:

  OneLake service address. Most users should keep the default; a
  workspace-specific address discovered from Fabric is used when
  available.

- columns:

  Column names to return, or `NULL` for all columns.

- limit:

  Maximum number of rows to return, or `NULL` for all rows.

- result:

  `"tibble"` or `"arrow_stream"` for batch processing.

## Value

`fabric_mirrored_database_schemas()` returns the same schema tibble as
[`fabric_lakehouse_schemas()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_catalog.md).
The table metadata functions return the same table tibble as
[`fabric_warehouse_tables()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_warehouse_tables.md).
The reader returns a tibble or a single-use `nanoarrow_array_stream`.

## SQL alternative

Mirrored databases also expose a read-only SQL analytics endpoint. Pass
a discovered mirrored database object to
[`fabric_sql_tables()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_tables.md),
[`fabric_sql_read_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_tables.md),
or
[`fabric_sql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_query.md)
when SQL permissions or SQL views are required.

## References

[Get Mirrored
Database](https://learn.microsoft.com/en-us/rest/api/fabric/mirroreddatabase/items/get-mirrored-database)

[Mirroring in Microsoft
Fabric](https://learn.microsoft.com/en-us/fabric/mirroring/overview)

[OneLake catalog table
APIs](https://learn.microsoft.com/en-us/rest/api/fabric/articles/onelakecatalog/overview#explore-tables-within-an-item)

## Examples

``` r
if (FALSE) { # \dontrun{
workspace <- fabric_workspaces()[[1L]]
database <- fabric_mirrored_databases(workspace)[[1L]]

schemas <- fabric_mirrored_database_schemas(database)
tables <- fabric_mirrored_database_tables(database)
rows <- fabric_mirrored_database_read_table(database, tables[1L, ], limit = 1000)
} # }
```
