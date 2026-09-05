# Discover OneLake schemas and individual tables

These helpers expose the read-only OneLake Delta table metadata API for
Lakehouses and Warehouses. The schema helpers follow every metadata
page. The singular table helpers retrieve one table's full column
metadata without listing every table in every schema.

## Usage

``` r
fabric_lakehouse_schemas(
  lakehouse,
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

fabric_warehouse_schemas(
  warehouse,
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

fabric_lakehouse_table(
  lakehouse,
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
  storage_token = NULL,
  enrich_fabric = FALSE
)

fabric_warehouse_table(
  warehouse,
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
```

## Arguments

- lakehouse:

  Lakehouse GUID, exact display name, or one Lakehouse object returned
  by
  [`fabric_lakehouses()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md).

- workspace:

  Workspace GUID, exact display name, or discovered workspace. Omit it
  when the item object contains `workspaceId`.

- page_size:

  Optional maximum schemas requested per metadata page, from 1 to 100.
  All continuation tokens are followed.

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
  Supply it when `token` is fixed and Fabric item lookup is needed.

- warehouse:

  Warehouse GUID, exact display name, or one Warehouse object returned
  by
  [`fabric_warehouses()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md).

- table:

  Table name, or a record containing a `name`, `table`, or `displayName`
  field. A record can also supply `schema`.

- schema:

  Schema containing `table`. Defaults to the Lakehouse default schema
  when available, otherwise `"dbo"`.

- enrich_fabric:

  For `fabric_lakehouse_table()`, also list the Fabric table inventory
  to enrich the result. Defaults to `FALSE`, so a complete discovered
  item needs only a Storage token. Setting `TRUE` requires both Fabric
  and Storage audiences; use an audience-aware provider or supply
  `token` and `storage_token` separately.

## Value

The schema functions return a tibble with `name`, `catalog`,
`full_name`, `comment`, `owner`, `schema_id`, timestamps, and the
unmodified metadata record in `raw`. The table functions return one row
with the same columns as
[`fabric_lakehouse_tables()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_lakehouse_tables.md)
or
[`fabric_warehouse_tables()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_warehouse_tables.md).

## Permissions

The OneLake table API uses the Azure Storage token audience and requires
permission to read the item's tables through OneLake.

## References

[Explore tables with OneLake catalog
APIs](https://learn.microsoft.com/en-us/rest/api/fabric/articles/onelakecatalog/overview#explore-tables-within-an-item)

[OneLake table APIs for
Delta](https://learn.microsoft.com/en-us/fabric/onelake/table-apis/delta-table-apis-overview)

## Examples

``` r
if (FALSE) { # \dontrun{
workspace <- fabric_workspaces()[[1L]]
lakehouse <- fabric_lakehouses(workspace)[[1L]]

schemas <- fabric_lakehouse_schemas(lakehouse)
orders <- fabric_lakehouse_table(lakehouse, "orders", schema = "dbo")
} # }
```
