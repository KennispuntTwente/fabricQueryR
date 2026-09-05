# Run Spark code in a temporary Microsoft Fabric Livy session

Starts Spark, runs one piece of code, returns its output, and closes the
Spark session. This is the simplest Livy helper for a one-off operation.
For quick reads from a Lakehouse or Warehouse, SQL is often faster to
start

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
  ...
)
```

## Arguments

- livy_url:

  A Livy connection URL copied from the Lakehouse settings, or an
  enriched Lakehouse object from
  [`fabric_lakehouses()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md)
  or
  [`fabric_item()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_item.md)
  A discovered object avoids copying workspace and Lakehouse IDs

- code:

  One string containing the Spark code to run. Objects created in this
  temporary session are lost after the function returns, although writes
  made to Lakehouse storage persist

- kind:

  Statement language. Use `"pyspark"` for Python with Spark, `"spark"`
  for Scala, `"sql"` for Spark SQL, or `"sparkr"` for SparkR. This must
  match the syntax in `code`. The `sparklyr` package is an R API, not a
  separate Livy language: experimental code that initializes `sparklyr`
  through item-scoped Livy still uses `"sparkr"`. SparkR is deprecated
  upstream in Spark 4.x; see **R on Runtime 2.0** below for the
  distinction

- tenant_id:

  Microsoft Entra tenant ID. Defaults to `FABRICQUERYR_TENANT_ID`

- client_id:

  Microsoft Entra application/client ID. Defaults to
  `FABRICQUERYR_CLIENT_ID`, then the Azure CLI application ID

- token:

  Optional access token or token-provider function. Leave `NULL` to let
  'fabricQueryR' use its normal sign-in flow. HTTPS validation does not
  prove ownership or token audience for a custom host; use one only when
  your organization controls it, with a token or provider issued for its
  intended audience

- auth_args:

  Additional sign-in options passed to
  [`AzureAuth::get_azure_token()`](https://rdrr.io/pkg/AzureAuth/man/get_azure_token.html)

- audience:

  Optional sign-in scopes. For delegated sign-in, `NULL` requests the
  four required Livy scopes listed below. An explicit vector replaces
  those defaults, so include every required scope plus any optional
  `Code.Access*` scope the Spark code needs. Client credentials require
  one `.default` audience

- environment_id:

  Optional GUID of a published Fabric Environment whose libraries and
  Spark settings should be used. Leave `NULL` to use the
  Lakehouse/workspace defaults

- conf:

  Optional named list of Spark configuration overrides, for example
  `list("spark.sql.shuffle.partitions" = "100")`. Most users can leave
  this `NULL` and configure shared settings in a Fabric Environment

- verbose:

  Logical. Show session startup, execution, and cleanup progress

- poll_interval:

  Seconds between status checks. Lower values update sooner but make
  more API calls

- timeout:

  Maximum seconds to wait for session readiness and, separately,
  statement completion

- ...:

  Compatibility arguments. The former named `access_token` argument is
  accepted here as a deprecated alias for `token`; all other arguments
  are rejected

## Value

Invisibly, a `fabric_livy_statement_result` list. The most useful
component is `output$parsed`: a tibble for tabular output, an R object
for JSON, or a character vector for text. The result also keeps status,
timing, submitted code, errors, and the original response. A successful
statement is still returned when session cleanup fails, with a
`fabric_livy_cleanup_warning` identifying the retained session. When
both execution and cleanup fail, a `fabric_livy_execution_cleanup_error`
retains the execution error and safe cleanup diagnostics

## Tabular column names

Duplicate SQL aliases and joined column names are repaired with
`make.unique(names, sep = "...")`: for example, `id, id` becomes
`id, id...1`. Every column retains its positional values. The
`spark_schema` attribute keeps the original header names and types, and
the result retains the original response.

## Before you run code

Fabric needs a workspace on supported capacity, a Lakehouse, and the
tenant admin setting for the Livy API enabled. In the Fabric portal,
open the Lakehouse settings, find **Livy endpoint**, and copy the
session-job connection string. For several statements that reuse
variables and Spark state, use
[`fabric_livy_session()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_livy_session.md).
To run a complete Python, Scala/Java, or R application file, use
[`fabric_livy_batch_submit()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_livy_batch_submit.md)

A delegated caller needs the `Lakehouse.Execute.All`,
`Lakehouse.Read.All`, `Code.AccessFabric.All`, and
`Code.AccessStorage.All` scopes and must be a Contributor in the
workspace. For session jobs, Microsoft's current guide also documents
service-principal (SPN) tokens. The service principal must be added to
the workspace as a Contributor, but that role alone does not override
tenant settings or other service-side identity restrictions. Add
`Code.AccessAzureKeyvault.All`, `Code.AccessAzureDataLake.All`,
`Code.AccessAzureDataExplorer.All`, or `Code.AccessSQL.All` only when
the Spark code accesses that Azure service at runtime

Spark long and decimal columns are returned as character values when
needed to preserve them exactly. Dates and timestamps with a time zone
use R temporal classes; timestamps without a time zone remain wall-clock
text Fabric's SQL JSON output represents non-finite floating-point
values as `null`, so those values are returned as typed missing values.
Binary and nested values use list-columns Nested decimal values retain
their JSON spelling. Fabric may round these values before sending SQL
JSON output; cast decimal leaves to STRING in Spark when full precision
is required across that service boundary.

## R on Runtime 2.0

Microsoft Fabric distributes `sparklyr` and documents
`sparklyr::spark_connect(method = "synapse")` for Fabric notebooks and
Spark job definitions. Microsoft does not currently document that
connection from an item-scoped Livy session, and this package's live
suite validates the `"sparkr"` interpreter but not a `sparklyr`
connection over it. Treat that adaptation as experimental and verify it
in the target runtime before use. It still depends on the SparkR JVM
bridge, which Spark 4.x deprecates. Prefer PySpark or Spark SQL when the
remote workload must be independent of that bridge

## See also

[Microsoft Fabric Livy API
overview](https://learn.microsoft.com/en-us/fabric/data-engineering/api-livy-overview),
[Livy API setup and
authorization](https://learn.microsoft.com/en-us/fabric/data-engineering/get-started-api-livy),
[Use sparklyr in
Fabric](https://learn.microsoft.com/en-us/fabric/data-science/r-use-sparklyr),
and [Fabric Runtime
2.0](https://learn.microsoft.com/en-us/fabric/data-engineering/runtime-2-0)

## Examples

``` r
# Livy can run SQL, PySpark, Spark, and SparkR code in Microsoft Fabric
# This function is not called automatically because it requires credentials
fabric_livy_query_example <- function() {
  # Discover a Lakehouse whose record contains its Fabric Livy endpoint
  workspace <- fabric_workspaces()[[1L]]
  lakehouse <- fabric_lakehouses(workspace)[[1L]]
  table <- fabric_lakehouse_tables(lakehouse)[1L, ]

  # Build SQL from the discovered table, then close the temporary session
  sql <- sprintf(
    "SELECT COUNT(*) AS row_count FROM `%s`.`%s`",
    table$schema[[1L]],
    table$name[[1L]]
  )
  sql_result <- fabric_livy_query(
    livy_url = lakehouse,
    kind = "sql",
    code = sql
  )

  # PySpark avoids the SparkR bridge. The Livy vignette separately labels the
  # item-scoped sparklyr adaptation experimental and not live-validated here
  pyspark_result <- fabric_livy_query(
    livy_url = lakehouse,
    kind = "pyspark",
    code = "print(1 + 2)"
  )

  invisible(list(sql = sql_result, pyspark = pyspark_result))
}
```
