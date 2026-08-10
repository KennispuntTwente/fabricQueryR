# Run Spark code in a temporary Microsoft Fabric Livy session

Starts Fabric Spark compute, runs one statement, waits for its result,
and closes the session. This is the simplest Livy helper for a one-off
Spark operation. Spark is useful for distributed processing or changing
Lakehouse data; it has more startup overhead than querying an existing
SQL endpoint.

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
  audience = NULL,
  environment_id = NULL,
  conf = NULL,
  verbose = TRUE,
  poll_interval = 2,
  timeout = 600,
  allow_custom_endpoint = FALSE,
  ...
)
```

## Arguments

- livy_url:

  A Livy connection URL copied from the Lakehouse settings, or an
  enriched Lakehouse record from
  [`fabric_lakehouses()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md)
  or
  [`fabric_item()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_item.md).
  A discovered record avoids copying workspace and Lakehouse IDs.

- code:

  One string containing the Spark code to run. Objects created in this
  temporary session are lost after the function returns, although writes
  made to Lakehouse storage persist.

- kind:

  Statement language. Use `"sparkr"` for SparkR code, `"pyspark"` for
  Python with Spark, `"spark"` for Scala, or `"sql"` for Spark SQL. This
  must match the syntax in `code`.

- tenant_id:

  Microsoft Entra tenant ID. Defaults to `FABRICQUERYR_TENANT_ID`.

- client_id:

  Microsoft Entra application/client ID. Defaults to
  `FABRICQUERYR_CLIENT_ID`, then the Azure CLI application ID.

- token:

  Optional
  [`AzureAuth::AzureToken`](https://rdrr.io/pkg/AzureAuth/man/AzureToken.html),
  bearer-token string, or token-provider function. With `NULL`,
  `AzureAuth` reuses a matching cached token or starts its normal
  interactive login flow.

- auth_args:

  Named list of additional arguments passed to
  [`AzureAuth::get_azure_token()`](https://rdrr.io/pkg/AzureAuth/man/get_azure_token.html).

- audience:

  Optional OAuth audience/scope vector. With `NULL`, delegated
  authentication requests Microsoft's four required Livy scopes, while
  an AzureAuth client-credentials flow requests the Power BI `.default`
  audience documented for service principals. Supply this explicitly
  when a custom token provider or identity flow requires a different
  token target.

- environment_id:

  Optional GUID of a published Fabric Environment whose libraries and
  Spark settings should be used. Leave `NULL` to use the
  Lakehouse/workspace defaults.

- conf:

  Optional named list of Spark configuration overrides, for example
  `list("spark.sql.shuffle.partitions" = "100")`. Most users can leave
  this `NULL` and configure shared settings in a Fabric Environment.

- verbose:

  Logical. Show session startup, execution, and cleanup progress.

- poll_interval:

  Seconds between status checks. Lower values update sooner but make
  more API calls.

- timeout:

  Maximum seconds to wait for session readiness and, separately,
  statement completion.

- allow_custom_endpoint:

  Logical. Keep `FALSE` to require a Microsoft Fabric API host. Set
  `TRUE` only for a trusted custom HTTPS service, such as a test
  emulator; the Fabric bearer token is sent to this endpoint.

- ...:

  Compatibility arguments. The former named `access_token` argument is
  accepted here as a deprecated alias for `token`; all other arguments
  are rejected.

## Value

Invisibly, a `fabric_livy_statement_result` list with statement `state`,
timing information, submitted `code`, raw response, and `output`.
`output$parsed` contains Livy table MIME output as a tibble, JSON output
as an R object, or text output as a character vector; error details and
every original MIME value are retained in the other `output` fields.
Parsed tables retain their declared headers in
`attr(x, "spark_schema")`. Spark long and decimal columns are character
vectors, dates and timestamps use R temporal classes, binary and nested
types use list-columns, and primitive numeric, string, and Boolean
columns use their corresponding R vectors.

## Details

Fabric needs a workspace on supported Fabric capacity and a Lakehouse.
In the Fabric portal, open the Lakehouse settings, find **Livy
endpoint**, and copy the session-job connection string. For several
statements that reuse variables and Spark state, use
[`fabric_livy_session()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_livy_session.md).
To run a complete Python, Scala/Java, or R application file, use
[`fabric_livy_batch_submit()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_livy_batch_submit.md).

Delegated authentication requests `Lakehouse.Execute.All`,
`Lakehouse.Read.All`, `Code.AccessFabric.All`, and
`Code.AccessStorage.All`. AzureAuth client-credentials authentication
uses `https://analysis.windows.net/powerbi/api/.default`, as documented
by Microsoft for Livy service principals. The caller also needs an
appropriate workspace role.

## See also

[Microsoft Fabric Livy API
overview](https://learn.microsoft.com/en-us/fabric/data-engineering/api-livy-overview),
[session jobs and Fabric
setup](https://learn.microsoft.com/en-us/fabric/data-engineering/get-started-api-livy-session)

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
