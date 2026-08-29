# Search the OneLake catalog

Searches Fabric item metadata across every workspace visible to the
caller. Results are lightweight R6 discovery objects that contain the
item and workspace identity. Type-specific results expose the methods
that can run from that metadata.

## Usage

``` r
fabric_catalog_search(
  search = NULL,
  types = NULL,
  filter = NULL,
  page_size = NULL,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  token = NULL,
  auth_args = list(),
  api_base = .fabric_api_base,
  output = c("r6", "list")
)
```

## Arguments

- search:

  Optional non-empty text query. Fabric searches display names,
  workspace display names, and descriptions. Leave `NULL` to browse
  visible catalog entries without a text query.

- types:

  Optional unique vector of Fabric item types. This is converted to the
  catalog API's documented `Type eq ... or Type eq ...` filter.

- filter:

  Optional raw catalog filter string. Fabric currently supports `Type`,
  `eq`, `ne`, `or`, and parentheses. Supply either `types` or `filter`,
  not both.

- page_size:

  Optional number of results requested per page, from 1 to 1000. Leave
  `NULL` to use Fabric's service default.

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

- output:

  Discovery record representation. The default `"r6"` returns R6 objects
  with type-specific methods. Use `"list"` when a plain record is
  specifically required

## Value

With `output = "r6"`, a list of
[FabricItem](https://kennispunttwente.github.io/fabricQueryR/reference/FabricItem.md)
objects or type-specific subclasses. With `output = "list"`, a list of
`fabric_catalog_entry` records that also inherit from `fabric_item`.
Both representations preserve the fields returned by Fabric and add the
item workspace identity from the catalog hierarchy.

## Details

Catalog search is a preview Fabric API. It is for metadata discovery
only and does not grant access to item contents. The caller needs
`Catalog.Read.All`; Fabric returns only entries that the calling user,
service principal, or managed identity is authorized to see.

Pagination uses a new POST body for each continuation token. A repeated
or malformed token raises a `fabric_catalog_protocol_error` instead of
silently returning partial results or looping indefinitely.

## References

[Catalog search REST
API](https://learn.microsoft.com/en-us/rest/api/fabric/core/catalog/search)

[OneLake Catalog REST API
overview](https://learn.microsoft.com/en-us/rest/api/fabric/articles/onelakecatalog/overview)

## Examples

``` r
if (FALSE) { # \dontrun{
# Search all visible workspaces for Lakehouses related to sales
entries <- fabric_catalog_search(
  search = "sales",
  types = "Lakehouse",
  page_size = 100
)

# `$onelake_list()` calls fabric_onelake_list()
lakehouse <- entries[[1L]]
lakehouse$onelake_list(path = "Tables")
} # }
```
