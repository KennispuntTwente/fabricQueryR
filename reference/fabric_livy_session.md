# Create a Microsoft Fabric Livy session

Starts Spark compute that can run several statements while keeping
variables and Spark state between calls. Use
[`fabric_livy_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_livy_query.md)
instead for a single, self-contained operation

## Usage

``` r
fabric_livy_session(
  livy_url,
  high_concurrency = FALSE,
  session_tag = NULL,
  name = NULL,
  tags = NULL,
  conf = NULL,
  environment_id = NULL,
  archives = NULL,
  driver_memory = NULL,
  driver_cores = NULL,
  executor_memory = NULL,
  executor_cores = NULL,
  num_executors = NULL,
  artifact_name = NULL,
  file = NULL,
  class_name = NULL,
  args = NULL,
  jars = NULL,
  files = NULL,
  py_files = NULL,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  token = NULL,
  auth_args = list(),
  audience = NULL,
  verbose = TRUE
)
```

## Arguments

- livy_url:

  A copied session or batch connection URL, Livy API base URL, or
  enriched Lakehouse record from
  [`fabric_lakehouses()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md)
  or
  [`fabric_item()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_item.md)
  Copy the session-job URL from **Lakehouse settings \> Livy endpoint**,
  or use a discovered record to avoid handling IDs manually

- high_concurrency:

  Whether to let Fabric share Spark compute between several isolated
  workloads. Keep `FALSE` for a typical sequence of calls in one R
  process

- session_tag:

  Optional high-concurrency packing hint. Related requests with the same
  tag may share an underlying Livy session while keeping separate REPL
  state. Each call still returns a distinct HC session

- name:

  Optional readable session name shown in service metadata

- tags:

  Optional named list of string labels for monitoring

- conf:

  Optional named list of Spark settings. Prefer a published Fabric
  Environment for configuration shared by several jobs

- environment_id:

  Optional GUID of a published Fabric Environment whose libraries and
  Spark settings should be used

- archives:

  Optional character vector of archive URIs made available to Spark

- driver_memory, executor_memory:

  Optional Spark memory values such as `"4g"`. Leave `NULL` to use
  Fabric defaults

- driver_cores, executor_cores, num_executors:

  Optional Spark resource counts. Larger values consume more capacity;
  leave `NULL` unless the workload has been sized deliberately

- artifact_name:

  Optional Lakehouse/artifact label used for a high-concurrency job in
  the Fabric Monitoring hub

- file:

  Optional application file URI for a high-concurrency request

- class_name:

  Optional Java/Scala main class for `file`

- args:

  Optional character vector of application arguments

- jars, files, py_files:

  Optional character vectors of dependency URIs supplied to Spark

- tenant_id:

  Microsoft Entra tenant ID. Defaults to `FABRICQUERYR_TENANT_ID`

- client_id:

  Microsoft Entra application/client ID. Defaults to
  `FABRICQUERYR_CLIENT_ID`, then the Azure CLI application ID

- token:

  Optional access token or token-provider function. Leave `NULL` to let
  fabricQueryR use its normal sign-in flow

- auth_args:

  Additional sign-in options passed to
  [`AzureAuth::get_azure_token()`](https://rdrr.io/pkg/AzureAuth/man/get_azure_token.html)

- audience:

  Optional sign-in scope. Most users should leave this `NULL`; set it
  only for a custom token provider or identity flow

- verbose:

  Logical. Show session lifecycle messages

## Value

A newly created
[FabricLivySession](https://kennispunttwente.github.io/fabricQueryR/reference/FabricLivySession.md).
It may still be starting; call `$wait()` before `$submit()`/`$run()`,
and `$close()` when finished

## Choosing a session type

Use a standard session for a typical sequence in one R process. High
concurrency is for applications that run several independent Spark
workloads at the same time; it is not needed for several sequential
statements

## Cleanup and permissions

No network request is made when an open object is garbage collected.
Call `$close()` explicitly, and use `on.exit(session$close())` inside
functions The signed-in identity needs Lakehouse read and execute
access, permission for code to access Fabric and storage, and an
appropriate workspace role

## See also

[Microsoft session
jobs](https://learn.microsoft.com/en-us/fabric/data-engineering/get-started-api-livy-session),
[high-concurrency
Livy](https://learn.microsoft.com/en-us/fabric/data-engineering/high-concurrency-livy)

## Examples

``` r
if (FALSE) { # \dontrun{
# Discover the Lakehouse whose Livy endpoint will host the Spark session
workspace <- fabric_workspaces()[[1L]]
lakehouse <- fabric_lakehouses(workspace)[[1L]]

run_shared_state <- function(lakehouse) {
  # Keep one session alive so successive statements share Spark state
  session <- fabric_livy_session(lakehouse)
  on.exit(session$close(), add = TRUE)
  session$wait()
  session$run("shared_value = 40", kind = "pyspark")
  session$run("print(shared_value + 2)", kind = "pyspark")
}
run_shared_state(lakehouse)

run_high_concurrency <- function(lakehouse) {
  # A session tag lets compatible callers reuse high-concurrency compute
  session <- fabric_livy_session(
    lakehouse,
    high_concurrency = TRUE,
    session_tag = "report-workers"
  )
  on.exit(session$close(), add = TRUE)
  session$wait()
  session$run("SELECT current_timestamp()", kind = "sql")
}
run_high_concurrency(lakehouse)
} # }
```
