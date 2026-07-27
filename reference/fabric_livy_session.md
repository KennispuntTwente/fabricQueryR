# Create a Microsoft Fabric Livy session

Starts an interactive Spark context that can run several statements
while retaining variables and Spark state between them. This avoids
starting new compute for each call.

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
  verbose = TRUE
)
```

## Arguments

- livy_url:

  A copied session or batch connection URL, Livy API base URL, or
  enriched Lakehouse record from
  [`fabric_lakehouses()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md)
  or
  [`fabric_item()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_item.md).
  Copy the session-job URL from **Lakehouse settings \> Livy endpoint**,
  or use a discovered record to avoid handling IDs manually.

- high_concurrency:

  Logical. `FALSE` creates a standard session for sequential or
  low-concurrency work. `TRUE` creates an isolated REPL that Fabric can
  pack into shared Spark sessions, which is useful when an application
  runs several independent Spark workloads concurrently.

- session_tag:

  Optional high-concurrency packing hint. Related requests with the same
  tag may share an underlying Livy session while keeping separate REPL
  state. Each call still returns a distinct HC session.

- name:

  Optional readable session name shown in service metadata.

- tags:

  Optional named list of string labels for monitoring.

- conf:

  Optional named list of Spark settings. Prefer a published Fabric
  Environment for configuration shared by several jobs.

- environment_id:

  Optional GUID of a published Fabric Environment whose libraries and
  Spark settings should be used.

- archives:

  Optional character vector of archive URIs made available to Spark.

- driver_memory, executor_memory:

  Optional Spark memory values such as `"4g"`. Leave `NULL` to use
  Fabric defaults.

- driver_cores, executor_cores, num_executors:

  Optional Spark resource counts. Larger values consume more capacity;
  leave `NULL` unless the workload has been sized deliberately.

- artifact_name:

  Optional Lakehouse/artifact label used for a high-concurrency job in
  the Fabric Monitoring hub.

- file:

  Optional application file URI for a high-concurrency request.

- class_name:

  Optional Java/Scala main class for `file`.

- args:

  Optional character vector of application arguments.

- jars, files, py_files:

  Optional character vectors of dependency URIs supplied to Spark.

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

- verbose:

  Logical. Show session lifecycle messages.

## Value

A newly created
[FabricLivySession](https://kennispunttwente.github.io/fabricQueryR/reference/FabricLivySession.md).
It may still be starting; call `$wait()` before `$submit()`/`$run()`,
and `$close()` when finished.

## Details

Use a standard session for a typical interactive sequence in one R
process. High concurrency is intended for automation that needs multiple
isolated Spark statement streams at the same time; it is not necessary
merely to run several statements sequentially.

A finalizer attempts cleanup if an open object is garbage collected.
Call `$close()` explicitly, and use `on.exit(session$close())` in
functions, for deterministic cleanup. Requests use the
`https://api.fabric.microsoft.com/.default` audience. Delegated
authentication requires `Lakehouse.Execute.All`, `Lakehouse.Read.All`,
`Code.AccessFabric.All`, and `Code.AccessStorage.All`.

## See also

[Microsoft session
jobs](https://learn.microsoft.com/en-us/fabric/data-engineering/get-started-api-livy-session),
[high-concurrency
Livy](https://learn.microsoft.com/en-us/fabric/data-engineering/high-concurrency-livy)
