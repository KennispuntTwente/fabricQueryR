# Run and monitor Microsoft Fabric item jobs

Start a Notebook, data pipeline, Spark job definition, or another
supported Fabric item from R. The related functions check its progress,
wait for it to finish, or request cancellation. Use Fabric's scheduler
for recurring runs

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
  api_base = .fabric_api_base,
  respect_retry_after = TRUE,
  notebook_details = FALSE,
  .sleep = Sys.sleep,
  .now = Sys.time
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
  notebook_details = FALSE,
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

  Item GUID, exact display name, or an item record returned by a
  discovery function. A discovered record is recommended because it
  already includes the item type and workspace ID

- workspace:

  Workspace GUID, exact display name, or a workspace record Omit it when
  `item` is a discovered record containing `workspaceId`

- job_type:

  Fabric job type. 'fabricQueryR' uses the current typed `"Execute"`
  operation for data pipelines and knows the usual values for notebooks
  and Spark job definitions, so normally omit this unless running
  another item type. Set `job_type = "Pipeline"` for a data pipeline
  only when explicitly using Fabric's legacy core job endpoint

- item_type:

  Optional Fabric item type when `item` is a GUID. A discovered item
  supplies this automatically. Examples are `"Notebook"`,
  `"DataPipeline"`, and `"SparkJobDefinition"`

- parameters:

  A named list of values to pass to the job, such as
  `list(run_date = as.Date("2026-01-31"), full_load = FALSE)`, infers
  types from R and is appropriate for most runs. Names must match the
  parameters configured in Fabric. Advanced callers can instead supply
  records with `name`, `value`, and `type`. The typed DataPipeline
  `Execute` endpoint does not accept parameters

- parameter_types:

  Optional named character vector overriding inferred parameter types.
  Supported values are `VariableReference`, `Integer`, `Number`, `Text`,
  `Boolean`, `DateTime`, `Guid`, and `Automatic`. Use this only when R's
  inferred type is not the type expected in Fabric

- execution_data:

  Optional advanced job settings in the format documented for the Fabric
  item type. Use the simpler arguments below for common notebook
  settings. In custom payload fields, wrap a one-element atomic vector
  in [`I()`](https://rdrr.io/r/base/AsIs.html) (or use an unnamed list)
  when it must remain a JSON array. The typed DataPipeline `Execute`
  endpoint does not accept a request body

- default_lakehouse:

  Optional Lakehouse GUID or discovered record used to set the
  notebook's default Lakehouse for this run. This changes the run
  context, not the notebook's saved default

- default_lakehouse_workspace:

  Optional workspace GUID or discovered record for `default_lakehouse`;
  defaults to the job workspace

- compute:

  Notebook compute kind: `"Spark"`, `"Jupyter"`, or `"DataWarehouse"`.
  Use `"Spark"` (the default) for Spark notebooks, `"Jupyter"` for a
  Jupyter runtime, and `"DataWarehouse"` for a notebook attached to
  Warehouse compute. It must match what the notebook code needs

- session_tag:

  Optional tag that enables Spark high-concurrency mode, so related
  notebook runs may reuse compute. See Details for its effect on failure
  reporting

- tenant_id:

  Entra tenant ID. Defaults to `FABRICQUERYR_TENANT_ID`

- client_id:

  Entra application ID. Defaults to `FABRICQUERYR_CLIENT_ID`, then the
  Azure CLI application ID

- token:

  Optional access token or token-provider function. Leave `NULL` to let
  'fabricQueryR' use its normal sign-in flow A `fabric_job` handle
  reuses its stored credential unless `tenant_id`, `client_id`, `token`,
  or non-empty `auth_args` is supplied explicitly

- auth_args:

  Additional sign-in options passed to
  [`AzureAuth::get_azure_token()`](https://rdrr.io/pkg/AzureAuth/man/get_azure_token.html)
  when no token source is supplied

- api_base:

  Fabric REST API base URL. Most users should keep the default A
  discovered workspace-specific endpoint is used unless this argument is
  supplied explicitly

- job:

  A `fabric_job` returned by `fabric_job_run()` or a
  `fabric_job_instance` returned by a status, wait, or history function.
  Status and cancellation functions also accept a job instance GUID when
  `workspace`, `item`, and enough type information are supplied

- job_instance_id:

  Alternative argument for a job instance GUID. Do not supply it
  together with a `fabric_job` handle

- respect_retry_after:

  Whether to wait for Fabric's recommended first status-check time. Keep
  `TRUE` for normal use

- notebook_details:

  For Notebook jobs, whether to opt into the beta Notebook status
  endpoint for exit values and compute details. The default `FALSE` uses
  only the stable Core Job Scheduler status endpoint; `exit_value` and
  workload-specific `properties` may then be unavailable.

- .sleep, .now:

  Internal hooks for deterministic tests

- poll_interval:

  Minimum seconds between status checks. `NULL` follows Fabric's
  recommendation, with a two-second fallback

- timeout:

  Maximum seconds to wait before raising a `fabric_job_timeout`

- error_on_failure:

  Whether failed, cancelled, or deduplicated jobs raise typed errors.
  Set to `FALSE` to inspect those terminal results directly

- cancel_on_timeout:

  Ask Fabric to cancel the job when the client-side timeout expires.
  `FALSE` stops waiting but leaves the Fabric job running

- cancel:

  Optional function checked between status updates. If it returns
  `TRUE`, 'fabricQueryR' requests cancellation. This can support an
  application's stop button

## Value

`fabric_job_run()` returns a `fabric_job` handle for use with the other
job functions `fabric_job_status()` and `fabric_job_wait()` return a
`fabric_job_instance` record with status, times, failure information,
and a notebook exit value when available. `fabric_job_cancel()`
invisibly returns `TRUE` after Fabric accepts or confirms the
cancellation

## Typical workflow

Start a job with `fabric_job_run()`, then pass the returned handle to
`fabric_job_wait()`. The handle keeps the workspace, item, job type, and
sign-in context, so later calls do not need those details again
Parameterized Core jobs can return a collection `Location` without an
instance GUID. In that documented case, `fabric_job_run()` reads recent
job history and accepts only one matching manual run; it raises a
protocol error rather than guessing when recovery is ambiguous

## High-concurrency notebooks

A `session_tag` lets related notebook runs share Spark compute, but
Fabric may report a failed statement as a completed shared session with
no exit value Omit the tag when job status must reliably signal notebook
failure. Otherwise, have the notebook report its outcome with
`notebookutils.notebook.exit()`. The former `mssparkutils` namespace
remains backward compatible but Microsoft recommends migrating because
it will be retired

Notebook submission uses the released workload-specific route so Fabric
applies per-run parameters and compute settings. Status and waiting use
the stable Core endpoint by default. Set `notebook_details = TRUE` to
opt into the beta Notebook status endpoint when exit values or
workload-specific properties are required; the Core endpoint remains its
fallback.

## Permissions and status handling

Running and cancelling need an item execute permission. Checking or
waiting also needs an item read permission, as does resolving a
parameterized run's collection `Location`. 'fabricQueryR' reconciles
notebook status information from Fabric before returning it and stops
with a typed error if Fabric reports an unfamiliar state instead of
waiting indefinitely

## References

[Core Job Scheduler REST
API](https://learn.microsoft.com/en-us/rest/api/fabric/core/job-scheduler/)

[Run an on-demand item
job](https://learn.microsoft.com/en-us/rest/api/fabric/core/job-scheduler/run-on-demand-item-job)

[Run an on-demand
notebook](https://learn.microsoft.com/en-us/rest/api/fabric/notebook/background-jobs/run-on-demand-notebook)

[Get a Notebook job instance
(beta)](https://learn.microsoft.com/en-us/rest/api/fabric/notebook/background-jobs/get-notebook-job-instance%28beta%29)

[Manage and execute notebooks with public
APIs](https://learn.microsoft.com/en-us/fabric/data-engineering/notebook-public-api)

[Fabric job
scheduler](https://learn.microsoft.com/en-us/fabric/fundamentals/job-scheduler)

## Examples

``` r
if (FALSE) { # \dontrun{
# Discover the workspace and Notebook that will be run
workspace <- fabric_workspaces()[[1L]]
notebook <- fabric_notebooks(workspace)[[1L]]

# Start the discovered Notebook and keep the returned job handle
job <- fabric_job_run(notebook)

# Refresh the current state without waiting for completion
current <- fabric_job_status(job)
current$status

# Opt into beta Notebook details only when an exit value is required
completed <- fabric_job_wait(
  job,
  timeout = 900,
  notebook_details = TRUE
)
completed$status
completed$exit_value

# A separate active run can be cancelled when it is no longer needed
job_to_cancel <- fabric_job_run(notebook)
fabric_job_cancel(job_to_cancel)
} # }
```
