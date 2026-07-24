# Run and monitor Microsoft Fabric item jobs

Submit, inspect, wait for, and cancel on-demand Fabric item jobs through
the Fabric Job Scheduler APIs. Notebook jobs use the workload-specific
release endpoint so that their compute configuration and exit value are
available.

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
  access_token = NULL,
  token_provider = NULL,
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
  access_token = NULL,
  token_provider = NULL,
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
  access_token = NULL,
  token_provider = NULL,
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
  access_token = NULL,
  token_provider = NULL,
  api_base = .fabric_api_base
)
```

## Arguments

- item:

  Item GUID, exact display name, or a one-row item record returned by a
  discovery function.

- workspace:

  Workspace GUID, exact display name, or a workspace record. May be
  omitted when `item` contains `workspaceId`.

- job_type:

  Fabric job type. Known defaults are `"RunNotebook"` for notebooks,
  `"Pipeline"` for data pipelines, and `"SparkJob"` for Spark job
  definitions. It is required for other item types.

- item_type:

  Optional Fabric item type when `item` is a GUID. A discovered item
  supplies this automatically.

- parameters:

  A named list of scalar parameter values, or a list of records
  containing `name`, `value`, and `type`. Names are compared
  case-insensitively, as required by Fabric.

- parameter_types:

  Optional named character vector overriding inferred parameter types.
  Supported values are `VariableReference`, `Integer`, `Number`, `Text`,
  `Boolean`, `DateTime`, `Guid`, and `Automatic`.

- execution_data:

  Optional workload execution configuration. Notebook configuration is
  validated against the documented `compute` and `computeConfiguration`
  shape. Spark job definition configuration must be a named list. Data
  pipelines do not currently accept execution data here.

- default_lakehouse:

  Optional Lakehouse GUID or discovered record used to construct a
  notebook Spark `defaultLakehouse` configuration.

- default_lakehouse_workspace:

  Optional workspace GUID or discovered record for `default_lakehouse`;
  defaults to the job workspace.

- compute:

  Notebook compute kind: `"Spark"`, `"Jupyter"`, or `"DataWarehouse"`.

- session_tag:

  Optional Spark high-concurrency session tag.

- tenant_id, client_id, access_token, token_provider:

  Authentication arguments. Job submission and cancellation require
  `Item.Execute.All` or the corresponding workload-specific execute
  permission.

- api_base:

  Fabric REST API base URL.

- job:

  A `fabric_job` returned by `fabric_job_run()`, or a job instance GUID.
  When a GUID is supplied, also provide `workspace`, `item`, and
  optionally `item_type` and `job_type`.

- job_instance_id:

  Alternative to supplying the instance GUID as `job`.

- poll_interval:

  Minimum seconds between status requests. `NULL` uses Fabric's
  `Retry-After` header, falling back to two seconds.

- timeout:

  Maximum seconds to wait.

- error_on_failure:

  Whether failed, cancelled, or deduplicated jobs raise typed errors.
  Set to `FALSE` to inspect those terminal results directly.

- cancel_on_timeout:

  Ask Fabric to cancel the job when the client-side timeout expires.

- cancel:

  Optional callback checked between polls. Returning `TRUE` cancels the
  Fabric job and raises a `fabric_job_cancelled_by_caller` condition.

- .sleep, .now:

  Internal hooks for deterministic tests.

## Value

`fabric_job_run()` returns a `fabric_job` handle. `fabric_job_status()`
and `fabric_job_wait()` return a `fabric_job_instance`.
`fabric_job_cancel()` returns `TRUE` invisibly after Fabric accepts the
cancellation.

## References

[Core Job Scheduler REST
API](https://learn.microsoft.com/en-us/rest/api/fabric/core/job-scheduler/)

[Run an on-demand
notebook](https://learn.microsoft.com/en-us/rest/api/fabric/notebook/background-jobs/run-on-demand-notebook)

[Manage and execute notebooks with public
APIs](https://learn.microsoft.com/en-us/fabric/data-engineering/notebook-public-api)
