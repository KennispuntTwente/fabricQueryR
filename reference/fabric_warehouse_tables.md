# Discover Microsoft Fabric Warehouse tables

Lists schemas and Delta-backed tables in a Fabric Warehouse through the
read-only OneLake table metadata API. Set `detail = TRUE` to retrieve
column metadata for every table. A returned row can be passed directly
to
[`fabric_warehouse_read_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_warehouse_read_table.md).

## Usage

``` r
fabric_warehouse_tables(
  warehouse,
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
  table_api_base = .fabric_onelake_table_base
)
```

## Arguments

- warehouse:

  Warehouse GUID, exact display name, or one Warehouse object returned
  by
  [`fabric_warehouses()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md).
  A discovered object is recommended because it contains the workspace
  and item IDs.

- workspace:

  Workspace GUID, exact display name, or discovered workspace. Omit it
  when `warehouse` is an object containing `workspaceId`.

- schema:

  Optional Warehouse schema. When omitted, every schema is listed.

- detail:

  Whether to retrieve per-table column metadata.

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
  Warehouse lookup can require a Fabric-audience token; table metadata
  uses a Storage-audience token.

- auth_args:

  Additional sign-in options passed to
  [`AzureAuth::get_azure_token()`](https://rdrr.io/pkg/AzureAuth/man/get_azure_token.html)
  when no token source is supplied.

- api_base:

  Fabric REST API base URL used when a Warehouse name or GUID must be
  resolved. Most users should keep the default.

- table_api_base:

  OneLake Delta table API base URL. Most users should keep the default.

## Value

A tibble with table `name`, `schema`, `full_name`, `type`, `format`,
`location`, timestamps, list-column `columns`, `schema_metadata`, and
the unmodified OneLake `raw` record. `fabric_raw` is an empty
list-column because Fabric does not expose a Warehouse counterpart to
the Lakehouse List Tables REST route. Unknown future OneLake metadata
remains available in `raw`.

## Permissions

The OneLake table API uses the Azure Storage token audience and requires
the calling identity to have permission to read tables in the Warehouse
through OneLake. This permission is separate from Warehouse T-SQL
`ReadData` permission.

## References

[Explore tables with OneLake catalog
APIs](https://learn.microsoft.com/en-us/rest/api/fabric/articles/onelakecatalog/overview#explore-tables-within-an-item)

[OneLake table APIs for
Delta](https://learn.microsoft.com/en-us/fabric/onelake/table-apis/delta-table-apis-overview)

[Warehouse
permissions](https://learn.microsoft.com/en-us/fabric/data-warehouse/share-warehouse-manage-permissions)

## Examples

``` r
if (FALSE) { # \dontrun{
workspace <- fabric_workspaces()[[1L]]
warehouse <- fabric_warehouses(workspace)[[1L]]

tables <- fabric_warehouse_tables(warehouse)
orders <- fabric_warehouse_read_table(warehouse, tables[1L, ])
} # }
```
