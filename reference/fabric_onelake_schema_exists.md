# Check whether a OneLake schema or table exists

Uses the documented metadata-only `HEAD` operations from either the
Delta Unity Catalog-compatible API or the Iceberg REST Catalog API.
These helpers avoid downloading a complete schema or table record when
only existence is needed.

## Usage

``` r
fabric_onelake_schema_exists(
  item,
  schema,
  workspace = NULL,
  item_type = NULL,
  protocol = c("delta", "iceberg"),
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  token = NULL,
  auth_args = list(),
  api_base = .fabric_api_base,
  table_api_base = .fabric_onelake_table_origin
)

fabric_onelake_table_exists(
  item,
  table,
  workspace = NULL,
  schema = NULL,
  item_type = NULL,
  protocol = c("delta", "iceberg"),
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  token = NULL,
  auth_args = list(),
  api_base = .fabric_api_base,
  table_api_base = .fabric_onelake_table_origin
)
```

## Arguments

- item:

  Fabric data item GUID, exact display name, or a discovered item
  object. An object containing `workspaceId` avoids workspace discovery.

- schema:

  Schema or Iceberg namespace name. For a table, `NULL` uses the item's
  discovered default schema and otherwise falls back to `"dbo"`.

- workspace:

  Workspace GUID, exact display name, or discovered workspace. Omit it
  when `item` contains `workspaceId`.

- item_type:

  Optional item type used to disambiguate an item supplied by name.

- protocol:

  OneLake table metadata protocol: `"delta"` or `"iceberg"`.

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

  Fabric REST API base URL. Leave unchanged unless using a different
  Fabric cloud or a test service

- table_api_base:

  OneLake table API HTTPS origin, or a protocol-specific base ending in
  `/delta` or `/iceberg`. Most users should keep the default.

- table:

  Table name or a record containing `name`, `table`, or `displayName`. A
  record can also supply its schema.

## Value

One logical value: `TRUE` for a successful `HEAD` response and `FALSE`
for HTTP 404. Authentication, permission, throttling, and service errors
are not converted to `FALSE`.

## Details

The table APIs use the Azure Storage token audience and require
permission to read the item's tables through OneLake. If name-based item
discovery is necessary, use the package's normal audience-aware sign-in
or token provider because the Fabric Core and Storage audiences are both
involved.

Iceberg requests first call `GET /iceberg/v1/config` with the item's
workspace/item warehouse identity and validate the returned prefix
before issuing `HEAD`. Delta requests include `catalog_name` and, for
tables, `schema_name` even when names do not contain dots.

## References

[OneLake table APIs for
Delta](https://learn.microsoft.com/en-us/fabric/onelake/table-apis/delta-table-apis-overview)

[OneLake table APIs for
Iceberg](https://learn.microsoft.com/en-us/fabric/onelake/table-apis/iceberg-table-apis-overview)

## Examples

``` r
if (FALSE) { # \dontrun{
lakehouse <- fabric_lakehouses(fabric_workspaces()[[1L]])[[1L]]

fabric_onelake_schema_exists(lakehouse, "dbo")
fabric_onelake_table_exists(lakehouse, "orders", schema = "dbo")
fabric_onelake_table_exists(
  lakehouse,
  "orders",
  schema = "dbo",
  protocol = "iceberg"
)
} # }
```
