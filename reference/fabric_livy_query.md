# Run Spark code in a temporary Microsoft Fabric Livy session

Creates a session, waits for it to become ready, runs one statement, and
closes the session even when execution fails. For multiple statements or
explicit lifecycle control, use
[`fabric_livy_session()`](https://lukakoning.github.io/fabricQueryR/reference/fabric_livy_session.md).

## Usage

``` r
fabric_livy_query(
  livy_url,
  code,
  kind = c("spark", "pyspark", "sparkr", "sql"),
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  token = NULL,
  auth_args = list(),
  environment_id = NULL,
  conf = NULL,
  verbose = TRUE,
  poll_interval = 2,
  timeout = 600,
  ...
)
```

## Arguments

- livy_url:

  A Livy connection URL or an enriched Lakehouse record from
  [`fabric_lakehouses()`](https://lukakoning.github.io/fabricQueryR/reference/fabric_typed_items.md)
  or
  [`fabric_item()`](https://lukakoning.github.io/fabricQueryR/reference/fabric_item.md).

- code:

  One non-empty string containing Spark code.

- kind:

  Statement language: `"spark"`, `"pyspark"`, `"sparkr"`, or `"sql"`.

- tenant_id:

  Microsoft Entra tenant ID.

- client_id:

  Microsoft Entra application ID.

- token:

  Optional
  [`AzureAuth::AzureToken`](https://rdrr.io/pkg/AzureAuth/man/AzureToken.html),
  bearer-token string, or token-provider function. With `NULL`,
  `AzureAuth` reuses a matching cached token or starts its normal
  interactive login flow.

- auth_args:

  Named list of additional arguments passed to
  [`AzureAuth::get_azure_token()`](https://rdrr.io/pkg/AzureAuth/man/get_azure_token.html).

- environment_id:

  Optional Fabric Environment ID.

- conf:

  Optional named list of Spark configuration settings.

- verbose:

  Logical. Emit lifecycle progress.

- poll_interval:

  Polling interval in seconds.

- timeout:

  Maximum seconds for each readiness/execution wait.

- ...:

  Compatibility arguments. The former named `access_token` argument is
  accepted here as a deprecated alias for `token`; all other arguments
  are rejected.

## Value

An invisible `fabric_livy_statement_result` list.

## Details

Requests use the `https://api.fabric.microsoft.com/.default` audience.
Delegated authentication requires `Lakehouse.Execute.All`,
`Lakehouse.Read.All`, `Code.AccessFabric.All`, and
`Code.AccessStorage.All`; the caller also needs an appropriate workspace
role.

## See also

[Microsoft Fabric Livy API
overview](https://learn.microsoft.com/en-us/fabric/data-engineering/api-livy-overview),
[session
jobs](https://learn.microsoft.com/en-us/fabric/data-engineering/get-started-api-livy-session)

## Examples

``` r
# Livy can run SQL, SparkR, PySpark, and Spark code in Microsoft Fabric
# This function is not called automatically because it requires credentials and
# a real Lakehouse Livy endpoint
fabric_livy_query_example <- function() {
  # Find the URL under Lakehouse > Settings > Livy Endpoint
  session_url <- paste0(
    "https://api.fabric.microsoft.com/v1/workspaces/.../",
    "lakehouses/.../livyapi/..."
  )

  sql_result <- fabric_livy_query(
    livy_url = session_url,
    kind = "sql",
    code = "SELECT COUNT(*) AS row_count FROM dbo.example_table"
  )

  sparkr_result <- fabric_livy_query(
    livy_url = session_url,
    kind = "sparkr",
    code = "print(1 + 2)"
  )

  invisible(list(sql = sql_result, sparkr = sparkr_result))
}
```
