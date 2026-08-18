# Discover Microsoft Fabric KQL tables

Lists tables in a Fabric KQL database through Kusto's management
endpoint. With `detail = TRUE`, retrieves each table's JSON schema and
exposes its ordered columns while retaining the complete metadata
response.

## Usage

``` r
fabric_kql_tables(
  cluster,
  database = NULL,
  detail = TRUE,
  timeout = 60,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  token = NULL,
  auth_args = list(),
  allow_custom_endpoint = FALSE
)
```

## Arguments

- cluster:

  Query URI, or one Eventhouse or KQLDatabase record returned by
  [`fabric_eventhouses()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md),
  [`fabric_kql_databases()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md),
  or
  [`fabric_item()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_item.md).
  A KQLDatabase record also supplies `database`. Despite the argument
  name, use Fabric's **Query URI** here

- database:

  KQL database display name. Supply it with a copied Query URI or an
  Eventhouse record; omit it when `cluster` is a KQLDatabase record

- detail:

  Whether to retrieve the JSON schema for every table. Set to `FALSE` to
  issue only the database-level table-list command.

- timeout:

  Positive client-side HTTP timeout in seconds. This is separate from
  the Kusto `servertimeout` request property

- tenant_id:

  Microsoft Entra tenant ID. Defaults to `FABRICQUERYR_TENANT_ID`

- client_id:

  Microsoft Entra application/client ID. Defaults to
  `FABRICQUERYR_CLIENT_ID`, with the Azure CLI application ID as
  fallback

- token:

  Optional access token or token-provider function. Leave `NULL` to let
  fabricQueryR use its normal sign-in flow

- auth_args:

  Additional sign-in options passed to
  [`AzureAuth::get_azure_token()`](https://rdrr.io/pkg/AzureAuth/man/get_azure_token.html)

- allow_custom_endpoint:

  Logical. Permit a non-Microsoft Kusto HTTPS origin. Keep `FALSE`
  unless the endpoint is trusted; credentials are sent to the supplied
  origin

## Value

A tibble with table `name`, `database`, `folder`, `description`,
list-column `columns`, parsed `schema_metadata`, and the unmodified
listing row in `raw`.

## References

[Kusto `.show tables`
command](https://learn.microsoft.com/en-us/kusto/management/show-tables-command?view=microsoft-fabric)

[Kusto `.show table schema`
command](https://learn.microsoft.com/en-us/kusto/management/show-table-schema-command?view=microsoft-fabric)

## Examples

``` r
if (FALSE) { # \dontrun{
workspace <- fabric_workspaces()[[1L]]
database <- fabric_kql_databases(workspace)[[1L]]

tables <- fabric_kql_tables(database)
events <- fabric_kql_read_table(database, tables[1L, ], limit = 1000)
} # }
```
