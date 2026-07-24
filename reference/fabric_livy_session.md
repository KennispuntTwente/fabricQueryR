# Create a Microsoft Fabric Livy session

Creates and returns an R6 object for an interactive Spark session. Set
`high_concurrency = TRUE` to acquire an isolated REPL in Fabric's
high-concurrency session pool.

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
  access_token = NULL,
  token_provider = NULL,
  verbose = TRUE
)
```

## Arguments

- livy_url:

  A copied session or batch connection URL, Livy API base URL, or
  enriched Lakehouse record from
  [`fabric_lakehouses()`](https://lukakoning.github.io/fabricQueryR/reference/fabric_typed_items.md)
  or
  [`fabric_item()`](https://lukakoning.github.io/fabricQueryR/reference/fabric_item.md).

- high_concurrency:

  Logical. Acquire a high-concurrency session.

- session_tag:

  Optional packing hint for high-concurrency sessions. Repeated requests
  with the same tag remain non-idempotent and return distinct HC session
  IDs.

- name:

  Optional session name.

- tags:

  Optional named list of string session tags.

- conf:

  Optional named list of Spark settings.

- environment_id:

  Optional Fabric Environment ID.

- archives:

  Optional character vector of archive URIs.

- driver_memory, executor_memory:

  Optional Spark memory strings.

- driver_cores, executor_cores, num_executors:

  Optional Spark resource counts.

- artifact_name, file, class_name, args, jars, files, py_files:

  Optional high-concurrency request fields. `artifact_name` controls the
  Monitoring hub label.

- tenant_id:

  Microsoft Entra tenant ID.

- client_id:

  Microsoft Entra application ID.

- access_token:

  Optional Fabric bearer token.

- token_provider:

  Optional callback returning a Fabric bearer token.

- verbose:

  Logical. Emit lifecycle messages.

## Value

A newly created
[FabricLivySession](https://lukakoning.github.io/fabricQueryR/reference/FabricLivySession.md).

## Details

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
