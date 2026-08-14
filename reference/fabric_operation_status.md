# Monitor Microsoft Fabric long-running operations

Check, wait for, and retrieve the result of a Fabric operation that
continues after its initiating request returns. Pass the operation
handle returned by a fabricQueryR function when possible. To resume work
later, pass either the operation ID or the `Location` URL returned by
Fabric

## Usage

``` r
fabric_operation_status(
  operation,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  token = NULL,
  auth_args = list(),
  api_base = .fabric_api_base,
  allow_custom_endpoint = FALSE,
  respect_retry_after = TRUE,
  .sleep = Sys.sleep,
  .now = Sys.time
)

fabric_operation_wait(
  operation,
  poll_interval = NULL,
  timeout = 300,
  error_on_failure = TRUE,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  token = NULL,
  auth_args = list(),
  api_base = .fabric_api_base,
  allow_custom_endpoint = FALSE,
  .sleep = Sys.sleep,
  .now = Sys.time
)

fabric_operation_result(
  operation,
  wait = TRUE,
  poll_interval = NULL,
  timeout = 300,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  token = NULL,
  auth_args = list(),
  api_base = .fabric_api_base,
  allow_custom_endpoint = FALSE,
  .sleep = Sys.sleep,
  .now = Sys.time
)
```

## Arguments

- operation:

  A `fabric_operation` handle, Fabric operation GUID, or operation
  state/result URL returned in a `Location` header

- tenant_id:

  Entra tenant ID. Defaults to `FABRICQUERYR_TENANT_ID`

- client_id:

  Entra application ID. Defaults to `FABRICQUERYR_CLIENT_ID`, then the
  Azure CLI application ID

- token:

  Optional access token or token-provider function. Leave `NULL` to let
  fabricQueryR use its normal sign-in flow. A `fabric_operation` handle
  reuses its stored credential unless authentication arguments are
  supplied explicitly

- auth_args:

  Additional sign-in options passed to
  [`AzureAuth::get_azure_token()`](https://rdrr.io/pkg/AzureAuth/man/get_azure_token.html)
  when no token source is supplied

- api_base:

  Fabric REST API base URL. Most users should keep the default

- allow_custom_endpoint:

  Logical. Set to `TRUE` only when `api_base` or the supplied operation
  URL is a non-Microsoft HTTPS origin that you trust to receive a Fabric
  token

- respect_retry_after:

  Whether to wait until Fabric's recommended next status-check time
  before making the request

- .sleep, .now:

  Internal hooks for deterministic tests

- poll_interval:

  Minimum seconds between status requests. `NULL` honors Fabric's
  `Retry-After` value and otherwise uses a two-second fallback

- timeout:

  Maximum total seconds to wait, including status requests

- error_on_failure:

  Whether a failed operation should raise a `fabric_operation_failed`
  condition. Set to `FALSE` to inspect the returned failed state
  directly

- wait:

  Whether to wait for a running operation. When `FALSE`, a non-terminal
  operation raises `fabric_operation_not_ready`

## Value

`fabric_operation_status()` and `fabric_operation_wait()` return a
`fabric_operation_state` record. `fabric_operation_result()` returns a
`fabric_operation_result` with `value`, `content_type`, `empty`, HTTP
and request identifiers, and the reusable operation handle. JSON results
are decoded as lists, binary results are raw vectors, and empty results
have a `NULL` value

## Typical workflow

A package function that starts asynchronous work may return a
`fabric_operation` handle. Use `fabric_operation_wait()` to wait for it
to finish and `fabric_operation_result()` to retrieve its output. Result
retrieval waits by default, so it is enough for the common case

If the R process restarts, save the handle's `id` or the
service-provided `location` and pass that value with fresh
authentication arguments

## Results and failures

`fabric_operation_status()` preserves Fabric's status, progress,
timestamps, request identifiers, and structured error. Status values
added by Fabric in the future remain inspectable, but
`fabric_operation_wait()` stops with a typed error instead of polling an
unfamiliar value indefinitely

Some workload APIs, including Lakehouse table loading, expose completion
in their state response and do not provide a separate `/result`
resource. For those operations, `fabric_operation_result()` returns the
terminal state payload as its `value`

A failed operation raises `fabric_operation_failed` by default. A
timeout raises `fabric_operation_timeout`; neither condition repeats the
request that originally started the operation

## Regional operation endpoints

Fabric can return a `Location` on a regional `*.analysis.windows.net`
cluster. fabricQueryR recognizes those Microsoft endpoints and
automatically uses the Power BI token audience they require. Normal
automatic sign-in or an audience-aware token-provider function handles
both audiences. A single static Fabric bearer token cannot authenticate
a regional operation URL

## References

[Get operation
state](https://learn.microsoft.com/en-us/rest/api/fabric/core/long-running-operations/get-operation-state)

[Get operation
result](https://learn.microsoft.com/en-us/rest/api/fabric/core/long-running-operations/get-operation-result)

[Regional Fabric LRO authentication
example](https://learn.microsoft.com/en-us/fabric/real-time-intelligence/map/tutorial-create-fabric-map-python)

## Examples

``` r
if (FALSE) { # \dontrun{
# Discover a Lakehouse and a CSV file that Fabric can load as a table
workspace <- fabric_workspaces()[[1L]]
lakehouse <- fabric_lakehouses(workspace)[[1L]]
files <- fabric_onelake_list(
  workspace,
  lakehouse,
  path = "Files/incoming"
)
csv_file <- files[grepl("[.]csv$", files$path), ][1L, ]

# The load call returns the long-running operation handle used below
operation <- fabric_lakehouse_load_table(
  lakehouse,
  table = "orders_imported",
  path = csv_file$path[[1L]],
  format = "Csv",
  header = TRUE
)

# Check once, wait for completion, then retrieve the operation result
state <- fabric_operation_status(operation)
completed <- fabric_operation_wait(state$operation, timeout = 900)
result <- fabric_operation_result(completed$operation)
result$value
} # }
```
