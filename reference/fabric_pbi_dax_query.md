# Query a Microsoft Fabric/Power Bi semantic model with DAX

High-level helper that authenticates against Azure AD, resolves the
workspace & dataset from a Power BI (Microsoft Fabric) XMLA/connection
string, executes a DAX statement via the Power BI REST API, and returns
a tibble with the resulting data.

## Usage

``` r
fabric_pbi_dax_query(
  connstr,
  dax,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  include_nulls = TRUE,
  api_base = "https://api.powerbi.com/v1.0/myorg"
)
```

## Arguments

- connstr:

  Character. Power BI connection string, e.g.
  `"Data Source=powerbi://api.powerbi.com/v1.0/myorg/Workspace;Initial Catalog=Dataset;"`.
  The function accepts either `Data Source=` and `Initial Catalog=`
  parts, or a bare `powerbi://...` for the data source plus a
  `Dataset=`/`Catalog=`/`Initial Catalog=` key (see details).

- dax:

  Character scalar with a valid DAX query (see example).

- tenant_id:

  Microsoft Azure tenant ID. Defaults to
  `Sys.getenv("FABRICQUERYR_TENANT_ID")` if missing.

- client_id:

  Microsoft Azure application (client) ID used to authenticate. Defaults
  to `Sys.getenv("FABRICQUERYR_CLIENT_ID")`. You may be able to use the
  Azure CLI app id `"04b07795-8ddb-461a-bbee-02f9e1bf7b46"`, but may
  want to make your own app registration in your tenant for better
  control.

- include_nulls:

  Logical; pass-through to the REST serializer setting. Defaults to
  TRUE. If TRUE, null values are included in the response; if FALSE,
  they are omitted.

- api_base:

  API base URL. Defaults to "https://api.powerbi.com/v1.0/myorg".
  'myorg' is appropriate for most use cases and does not necessarily
  need to be changed.

## Value

A tibble with the query result (0 rows if the DAX query returned no
rows).

## Details

- In Microsoft Fabric/Power BI, you can find and copy the connection
  string by going to a 'Semantic model' item, then go to 'File' -\>
  'Settings' -\> 'Server settings'. Ensure that the account you use to
  authenticate has access to the workspace, or has been granted 'Build'
  permissions on the dataset (via sharing).

- AzureAuth is used to acquire the token. Be wary of caching behavior;
  you may want to call
  [`AzureAuth::clean_token_directory()`](https://rdrr.io/pkg/AzureAuth/man/get_azure_token.html)
  to clear cached tokens if you run into issues

## Examples

``` r
# Example is not executed since it requires configured credentials for Fabric
if (FALSE) { # \dontrun{
conn <- "Data Source=powerbi://api.powerbi.com/v1.0/myorg/My Workspace;Initial Catalog=SalesModel;"
df <- fabric_pbi_dax_query(
  connstr = conn,
  dax = "EVALUATE TOPN(1000, 'Customers')",
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID")
)
dplyr::glimpse(df)
} # }
```
