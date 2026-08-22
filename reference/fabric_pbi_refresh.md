# Refresh and monitor a Power BI semantic model

Start a semantic-model refresh, inspect recent refreshes and execution
details, wait for completion, or cancel an enhanced refresh. The easiest
target is a record returned by
[`fabric_semantic_models()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md)

## Usage

``` r
fabric_pbi_refresh(
  connstr = NULL,
  workspace_id = NULL,
  dataset_id = NULL,
  my_workspace = FALSE,
  mode = c("automatic", "standard", "enhanced"),
  notify_option = NULL,
  type = NULL,
  commit_mode = NULL,
  objects = NULL,
  apply_refresh_policy = NULL,
  effective_date = NULL,
  max_parallelism = NULL,
  retry_count = NULL,
  timeout = NULL,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  token = NULL,
  auth_args = list(),
  api_base = "https://api.powerbi.com/v1.0/myorg"
)

fabric_pbi_refresh_history(
  connstr = NULL,
  workspace_id = NULL,
  dataset_id = NULL,
  my_workspace = FALSE,
  top = NULL,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  token = NULL,
  auth_args = list(),
  api_base = "https://api.powerbi.com/v1.0/myorg"
)

fabric_pbi_refresh_status(
  refresh = NULL,
  connstr = NULL,
  workspace_id = NULL,
  dataset_id = NULL,
  my_workspace = FALSE,
  refresh_id = NULL,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  token = NULL,
  auth_args = list(),
  api_base = "https://api.powerbi.com/v1.0/myorg",
  .sleep = Sys.sleep,
  .now = Sys.time
)

fabric_pbi_refresh_wait(
  refresh,
  poll_interval = NULL,
  timeout = 1800,
  error_on_failure = TRUE,
  cancel_on_timeout = FALSE,
  cancel = NULL,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  token = NULL,
  auth_args = list(),
  api_base = "https://api.powerbi.com/v1.0/myorg",
  .sleep = Sys.sleep,
  .now = Sys.time
)

fabric_pbi_refresh_cancel(
  refresh = NULL,
  connstr = NULL,
  workspace_id = NULL,
  dataset_id = NULL,
  my_workspace = FALSE,
  refresh_id = NULL,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  token = NULL,
  auth_args = list(),
  api_base = "https://api.powerbi.com/v1.0/myorg"
)
```

## Arguments

- connstr:

  Optional semantic-model record from
  [`fabric_semantic_models()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md)
  or
  [`fabric_item()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_item.md),
  or a Power BI connection string. Omit it when `dataset_id` is supplied

- workspace_id:

  Optional shared-workspace GUID. For a semantic model in My Workspace,
  omit this and set `my_workspace = TRUE`

- dataset_id:

  Optional semantic-model/dataset GUID

- my_workspace:

  Whether `dataset_id` belongs to the signed-in user's My Workspace.
  Leave `FALSE` for shared workspaces

- mode:

  Refresh request kind. `"automatic"` chooses enhanced refresh when an
  enhanced option is supplied and standard refresh otherwise
  `"standard"` supports only `notify_option`; `"enhanced"` exposes
  processing controls and requires Premium, PPU, Embedded, or Fabric
  capacity

- notify_option:

  Standard-refresh email behavior for delegated calls:
  `"NoNotification"`, `"MailOnFailure"`, or `"MailOnCompletion"`. When
  omitted, standard refresh defaults to `"MailOnFailure"` unless
  fabricQueryR knows that it is using client credentials. For a
  caller-supplied service-principal token, pass `NULL` explicitly
  because its identity cannot be inferred. Omit this for enhanced
  refreshes

- type:

  Enhanced processing type: `"Full"`, `"ClearValues"`, `"Calculate"`,
  `"DataOnly"`, `"Automatic"`, or `"Defragment"`

- commit_mode:

  Enhanced commit behavior. `"Transactional"` preserves the previous
  model if processing fails. `"PartialBatch"` commits commands
  separately and can leave partially refreshed or empty tables after
  failure

- objects:

  Optional enhanced-refresh table or partition selection. Supply table
  names as a character vector, or records such as
  `list(list(table = "Sales", partition = "2026"))`

- apply_refresh_policy:

  Whether an incremental refresh policy should be applied. `TRUE` is
  incompatible with `commit_mode = "PartialBatch"`

- effective_date:

  Optional date-time used instead of the current date by an incremental
  refresh policy. Accepts a `Date`, `POSIXt`, or ISO 8601 string

- max_parallelism:

  Optional positive whole number of parallel processing threads for an
  enhanced refresh

- retry_count:

  Optional non-negative number of additional enhanced refresh attempts

- timeout:

  In `fabric_pbi_refresh()`, an optional `HH:MM:SS` limit for each
  enhanced attempt; Power BI defaults to five hours per attempt and
  limits all attempts to 24 hours. In `fabric_pbi_refresh_wait()`, the
  maximum number of seconds to wait on the client before raising a
  separate client-side timeout

- tenant_id:

  Microsoft Entra tenant ID. Defaults to `FABRICQUERYR_TENANT_ID`

- client_id:

  Microsoft Entra application/client ID. Defaults to
  `FABRICQUERYR_CLIENT_ID`, then the Azure CLI application ID

- token:

  Optional access token or token-provider function. Leave `NULL` to use
  the package's normal sign-in flow. Refresh handles reuse their
  in-process credential unless new authentication arguments are supplied

- auth_args:

  Additional sign-in options passed to
  [`AzureAuth::get_azure_token()`](https://rdrr.io/pkg/AzureAuth/man/get_azure_token.html)

- api_base:

  Power BI REST API base URL. The commercial-cloud default is normally
  correct

- top:

  Maximum history entries to return. Power BI retains 20 to 60 recent
  entries, depending on their age

- refresh:

  A `fabric_pbi_refresh` handle returned by `fabric_pbi_refresh()`, a
  `fabric_pbi_refresh_detail`, or a refresh GUID Raw GUIDs require the
  semantic-model target arguments as well

- refresh_id:

  Alternative refresh GUID. Do not combine it with a handle or GUID
  supplied through `refresh`

- .sleep, .now:

  Internal hooks for deterministic polling tests

- poll_interval:

  Minimum seconds between checks. `NULL` honors the service retry hint
  and otherwise checks every two seconds

- error_on_failure:

  Whether failed, timed-out, cancelled, or disabled refreshes raise a
  typed error. Use `FALSE` to inspect the returned detail

- cancel_on_timeout:

  Whether a client-side wait timeout should request cancellation before
  raising its timeout error. Cancellation is available only for enhanced
  refreshes

- cancel:

  Optional function checked between status updates. If it returns
  `TRUE`, fabricQueryR requests cancellation and stops waiting.
  Cancellation is available only for enhanced refreshes

## Value

`fabric_pbi_refresh()` returns a `fabric_pbi_refresh` handle Status and
wait return a `fabric_pbi_refresh_detail`; history returns a
`fabric_pbi_refresh_history` list. Cancel invisibly returns `TRUE`

## Standard and enhanced refresh

A standard refresh processes the complete model with Power BI defaults
and works on shared capacity, subject to the shared-capacity request
quota. An enhanced refresh is selected when any processing option is
supplied. It can target tables or partitions, retry, change commit
behavior, and set an attempt timeout, but requires a capacity-backed
model. Only one refresh can run for a semantic model at a time

Standard and service-principal refresh responses can expose the accepted
refresh ID through `RequestId` rather than `x-ms-request-id` or
`Location`. fabricQueryR recognizes either response form.
Standard-refresh status and waiting fall back to refresh history when
request-specific execution details are unavailable. Cancellation is
available only for enhanced refreshes

`Transactional` is the safe commit default. `PartialBatch` can expose a
partially refreshed model after failure and cannot apply an incremental
refresh policy. Each retry receives its own attempt timeout, while Power
BI limits the entire refresh including retries to 24 hours

## Results and diagnosis

`fabric_pbi_refresh()` returns a reusable handle
`fabric_pbi_refresh_status()` and `fabric_pbi_refresh_wait()` return a
`fabric_pbi_refresh_detail` with `state`, service status fields, UTC
times, processing objects, attempts, engine messages, parsed service
errors, a browser `details_url`, and the untouched response in `raw`.
When a standard refresh falls back to history, details are limited to
the fields available there `fabric_pbi_refresh_history()` returns a list
of the same detail records

Power BI can report a successful refresh with warnings, but Microsoft
notes that the history and execution-detail REST APIs do not always
include those warnings. When warning messages are returned, the
normalized state is `CompletedWithWarnings`; otherwise use `details_url`
to inspect the Fabric refresh-detail page

## Permissions and service limits

Starting any refresh and cancelling an enhanced refresh require
`Dataset.ReadWrite.All` and semantic-model Write permission. History and
status accept `Dataset.Read.All` or `Dataset.ReadWrite.All`, but history
callers still need model Write permission. A service principal may call
the APIs when the tenant allows it and the principal has sufficient
workspace/model access; email notification options do not apply to
service-principal requests

Shared capacity permits at most eight scheduled and API refresh requests
per day and does not support enhanced refresh. Capacity-backed models
have no fixed API-refresh count but can queue or throttle under load.
Enhanced-refresh cancellation is supported for Import and Composite
models in Premium, PPU, Embedded, or Fabric capacity and requires
Contributor, Member, or Admin workspace access

Direct Lake refresh is a usually short metadata framing operation, not
an import of OneLake data. Automatic Direct Lake updates are enabled by
default, so an explicit refresh can be unnecessary unless automatic
updates are disabled or a controlled point-in-time frame is required

## References

[Refresh Dataset
API](https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/refresh-dataset-in-group)

[Enhanced
refresh](https://learn.microsoft.com/en-us/power-bi/connect-data/asynchronous-refresh)

[Refresh
history](https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/get-refresh-history-in-group)

[Refresh execution
details](https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/get-refresh-execution-details-in-group)

[Data refresh and capacity
limits](https://learn.microsoft.com/en-us/power-bi/connect-data/refresh-data)

[How Direct Lake refresh
works](https://learn.microsoft.com/en-us/fabric/fundamentals/direct-lake-how-it-works)

## Examples

``` r
if (FALSE) { # \dontrun{
# Discover the semantic model instead of copying workspace and model IDs
workspace <- fabric_workspaces()[[1L]]
model <- fabric_semantic_models(workspace)[[1L]]

# Start a refresh, inspect it once, then wait for completion
refresh <- fabric_pbi_refresh(model)
current <- fabric_pbi_refresh_status(refresh)
current$state
result <- fabric_pbi_refresh_wait(refresh, timeout = 1800)
result$state
result$details_url

# An active enhanced refresh can be cancelled when it is no longer needed
refresh_to_cancel <- fabric_pbi_refresh(
  model,
  mode = "enhanced",
  type = "Full"
)
fabric_pbi_refresh_cancel(refresh_to_cancel)

# Choose a table shown in the model, then refresh only that table
refresh_table <- Sys.getenv("FABRIC_PBI_TABLE")
sales_only <- fabric_pbi_refresh(
  model,
  mode = "enhanced",
  type = "Full",
  objects = refresh_table,
  retry_count = 1L,
  timeout = "02:00:00"
)
fabric_pbi_refresh_wait(sales_only)

# Finally, inspect recent refreshes for the same discovered model
history <- fabric_pbi_refresh_history(model, top = 10L)
history[[1]]$attempts
} # }
```
