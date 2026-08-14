# Read a Microsoft Fabric Lakehouse table

Provides the symmetric read counterpart to
[`fabric_lakehouse_write_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_lakehouse_tables.md).
It resolves a discovered Lakehouse and table record, then delegates to
the authenticated OneLake Delta reader. Use `result = "arrow_stream"` to
keep a larger result out of R memory.

## Usage

``` r
fabric_lakehouse_read_table(
  lakehouse,
  table,
  workspace = NULL,
  schema = NULL,
  columns = NULL,
  limit = NULL,
  version = NULL,
  result = c("tibble", "arrow_stream"),
  verbose = TRUE,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  token = NULL,
  auth_args = list(),
  dfs_base = "https://onelake.dfs.fabric.microsoft.com"
)
```

## Arguments

- lakehouse:

  Lakehouse GUID, exact display name, or one Lakehouse record returned
  by
  [`fabric_lakehouses()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md).
  A discovered record is recommended because it carries its workspace ID
  and default schema.

- table:

  Table name or one row returned by
  [`fabric_lakehouse_tables()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_lakehouse_tables.md).

- workspace:

  Workspace GUID, exact display name, or discovered workspace. Omit it
  when `lakehouse` is a record containing `workspaceId`.

- schema:

  Optional schema. A table record supplies its schema when this argument
  is omitted.

- columns:

  Optional unique column names to project before collection.

- limit:

  Optional non-negative maximum number of rows to return.

- version:

  Optional non-negative Delta table version for time travel.

- result:

  Return a `"tibble"` or a disk-backed, single-use `"arrow_stream"`.

- verbose:

  Whether to report authentication and read progress.

- tenant_id:

  Entra tenant ID. Defaults to `FABRICQUERYR_TENANT_ID`.

- client_id:

  Entra application ID. Defaults to `FABRICQUERYR_CLIENT_ID`, then the
  Azure CLI application ID.

- token:

  Optional access token or audience-aware token-provider function.

- auth_args:

  Additional sign-in options passed to
  [`AzureAuth::get_azure_token()`](https://rdrr.io/pkg/AzureAuth/man/get_azure_token.html).

- dfs_base:

  OneLake DFS service address. A private or regional endpoint on a
  discovered record is preferred when this argument is omitted.

## Value

A tibble, or a disk-backed `nanoarrow_array_stream` when
`result = "arrow_stream"`.

## References

[OneLake table APIs for
Delta](https://learn.microsoft.com/en-us/fabric/onelake/table-apis/delta-table-apis-overview)

[Connect to
OneLake](https://learn.microsoft.com/en-us/fabric/onelake/onelake-access-api)

## Examples

``` r
if (FALSE) { # \dontrun{
# Discover both the Lakehouse and the table to read
workspace <- fabric_workspaces()[[1L]]
lakehouse <- fabric_lakehouses(workspace)[[1L]]
tables <- fabric_lakehouse_tables(lakehouse)
table <- tables[1L, ]

# Read the discovered table into a tibble
rows <- fabric_lakehouse_read_table(lakehouse, table)

# Stream selected columns when the full table may not fit in R memory
stream <- fabric_lakehouse_read_table(
  lakehouse,
  table,
  result = "arrow_stream"
)
reader <- arrow::as_record_batch_reader(stream)
} # }
```
