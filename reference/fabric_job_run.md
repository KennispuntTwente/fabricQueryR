# Run and monitor Microsoft Fabric item jobs

Starts one on-demand run of a supported Fabric item, then lets R
inspect, wait for, or cancel that run. Typical examples are running a
Notebook, data pipeline, or Spark job definition. This is for immediate
runs; configure a recurring timetable with Fabric's scheduler in the
portal or scheduler API.

## Usage

``` r
fabric_job_run(
  item,
  workspace = NULL,
  job_type = NULL,
  item_type = NULL,
  parameters = NULL,
  parameter_types = NULL,
  execution_data = NULL,
  default_lakehouse = NULL,
  default_lakehouse_workspace = NULL,
  compute = NULL,
  session_tag = NULL,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  token = NULL,
  auth_args = list(),
  api_base = .fabric_api_base
)

fabric_job_status(
  job = NULL,
  workspace = NULL,
  item = NULL,
  job_instance_id = NULL,
  item_type = NULL,
  job_type = NULL,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  token = NULL,
  auth_args = list(),
  api_base = .fabric_api_base
)

fabric_job_wait(
  job,
  poll_interval = NULL,
  timeout = 600,
  error_on_failure = TRUE,
  cancel_on_timeout = FALSE,
  cancel = NULL,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  token = NULL,
  auth_args = list(),
  api_base = .fabric_api_base,
  .sleep = Sys.sleep,
  .now = Sys.time
)

fabric_job_cancel(
  job = NULL,
  workspace = NULL,
  item = NULL,
  job_instance_id = NULL,
  item_type = NULL,
  job_type = NULL,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  token = NULL,
  auth_args = list(),
  api_base = .fabric_api_base
)
```

## Arguments

- item:

  Item GUID, exact display name, or a one-row item record returned by a
  discovery function. A discovered row is recommended because it already
  includes the item type and workspace ID.

- workspace:

  Workspace GUID, exact display name, or a workspace record. Omit it
  when `item` is a discovered row containing `workspaceId`.

- job_type:

  Fabric job type. Known defaults are `"RunNotebook"` for notebooks,
  `"Pipeline"` for data pipelines, and `"SparkJob"` for Spark job
  definitions. Normally omit it for those item types; supply the API's
  job type for another supported item.

- item_type:

  Optional Fabric item type when `item` is a GUID. A discovered item
  supplies this automatically. Examples are `"Notebook"`,
  `"DataPipeline"`, and `"SparkJobDefinition"`.

- parameters:

  A named list of scalar parameter values, or a list of records
  containing `name`, `value`, and `type`. Names are compared
  case-insensitively. The simple form, such as
  `list(run_date = as.Date("2026-01-31"), full_load = FALSE)`, infers
  types from R values and is appropriate for most runs. Parameter names
  must match those configured in the Fabric item.

- parameter_types:

  Optional named character vector overriding inferred parameter types.
  Supported values are `VariableReference`, `Integer`, `Number`, `Text`,
  `Boolean`, `DateTime`, `Guid`, and `Automatic`. Use this only when R's
  inferred type is not the type expected in Fabric.

- execution_data:

  Optional advanced workload configuration in the shape documented by
  Fabric. For notebooks this contains `compute` and optionally
  `computeConfiguration`; for Spark job definitions it is a named list
  of execution overrides. Data pipelines do not accept it here. Use the
  simpler arguments below for common notebook settings.

- default_lakehouse:

  Optional Lakehouse GUID or discovered record used to set the
  notebook's default Lakehouse for this run. This changes the run
  context, not the notebook's saved default.

- default_lakehouse_workspace:

  Optional workspace GUID or discovered record for `default_lakehouse`;
  defaults to the job workspace.

- compute:

  Notebook compute kind: `"Spark"`, `"Jupyter"`, or `"DataWarehouse"`.
  Use `"Spark"` (the default) for Spark notebooks, `"Jupyter"` for a
  Jupyter runtime, and `"DataWarehouse"` for a notebook attached to
  Warehouse compute. It must match what the notebook code needs.

- session_tag:

  Optional Spark high-concurrency session tag containing only letters,
  numbers, and underscores. Supplying it enables Fabric's
  high-concurrency mode so related notebook runs may reuse Spark
  compute.

- tenant_id:

  Entra tenant ID. Defaults to `FABRICQUERYR_TENANT_ID`.

- client_id:

  Entra application ID. Defaults to `FABRICQUERYR_CLIENT_ID`, then the
  Azure CLI application ID.

- token:

  Preferred token input: an
  [`AzureAuth::AzureToken`](https://rdrr.io/pkg/AzureAuth/man/AzureToken.html)
  object, bearer-token string, or token-provider function. With `NULL`,
  `AzureAuth` reuses a matching cached token or starts its normal
  interactive login flow.

- auth_args:

  Named list of additional arguments passed to
  [`AzureAuth::get_azure_token()`](https://rdrr.io/pkg/AzureAuth/man/get_azure_token.html)
  when no token source is supplied. Job submission and cancellation
  require `Item.Execute.All` or the corresponding workload-specific
  execute permission.

- api_base:

  Fabric REST API base URL. Most users should keep the default.

- job:

  A `fabric_job` returned by `fabric_job_run()`, or a job instance GUID.
  When a GUID is supplied, also provide `workspace`, `item`, and enough
  type information to reconstruct the status URL. The handle is simpler
  because it already stores that context.

- job_instance_id:

  Alternative argument for a job instance GUID. Do not supply it
  together with a `fabric_job` handle.

- poll_interval:

  Minimum seconds between status requests. `NULL` uses Fabric's
  recommended `Retry-After` value, falling back to two seconds. Setting
  a value never polls faster than Fabric requests.

- timeout:

  Maximum seconds to wait before raising a `fabric_job_timeout`.

- error_on_failure:

  Whether failed, cancelled, or deduplicated jobs raise typed errors.
  Set to `FALSE` to inspect those terminal results directly.

- cancel_on_timeout:

  Ask Fabric to cancel the job when the client-side timeout expires.
  `FALSE` stops waiting but leaves the Fabric job running.

- cancel:

  Optional callback checked between polls. Returning `TRUE` cancels the
  Fabric job and raises a `fabric_job_cancelled_by_caller` condition.
  This is useful for an application-specific stop button.

- .sleep, .now:

  Internal hooks for deterministic tests.

## Value

`fabric_job_run()` returns a `fabric_job` handle containing the job
instance ID and resolved workspace, item, job type, status URL, and
authentication context. `fabric_job_status()` and `fabric_job_wait()`
return a `fabric_job_instance` list with `status`, start/end times,
`failure_reason`, notebook `exit_value` when available, workload
`properties`, and `raw` response. `fabric_job_cancel()` returns `TRUE`
invisibly after Fabric accepts the cancellation request; terminal state
may not be visible immediately.

## Details

Notebook status uses Fabric's workload-specific beta endpoint first and
falls back to the core scheduler when that endpoint is unavailable.
Because Fabric may add job statuses over time, `fabric_job_wait()`
raises a `fabric_job_unknown_status` condition for an unrecognised state
instead of polling until timeout.

## References

[Core Job Scheduler REST
API](https://learn.microsoft.com/en-us/rest/api/fabric/core/job-scheduler/)

[Run an on-demand
notebook](https://learn.microsoft.com/en-us/rest/api/fabric/notebook/background-jobs/run-on-demand-notebook)

[Manage and execute notebooks with public
APIs](https://learn.microsoft.com/en-us/fabric/data-engineering/notebook-public-api)

[Fabric job
scheduler](https://learn.microsoft.com/en-us/fabric/fundamentals/job-scheduler)

## Examples

``` r
if (FALSE) { # \dontrun{
notebook <- fabric_notebooks("Analytics workspace")[1, ]

job <- fabric_job_run(
  notebook,
  parameters = list(run_date = Sys.Date(), full_load = FALSE)
)

completed <- fabric_job_wait(job, timeout = 900)
completed$status
completed$exit_value
} # }
```
