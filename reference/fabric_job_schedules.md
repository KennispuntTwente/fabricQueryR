# Manage Microsoft Fabric item schedules

List, create, update, or delete recurring schedules for a supported
Fabric item. Use
[`fabric_job_schedule_config()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_schedule_config.md)
to construct the four schedule types in the current REST contract.

## Usage

``` r
fabric_job_schedules(
  item,
  workspace = NULL,
  job_type = NULL,
  item_type = NULL,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  token = NULL,
  auth_args = list(),
  api_base = .fabric_api_base
)

fabric_job_schedule_create(
  item,
  configuration,
  workspace = NULL,
  job_type = NULL,
  item_type = NULL,
  enabled = TRUE,
  execution_data = NULL,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  token = NULL,
  auth_args = list(),
  api_base = .fabric_api_base
)

fabric_job_schedule_update(
  item,
  schedule_id,
  configuration = NULL,
  workspace = NULL,
  job_type = NULL,
  item_type = NULL,
  enabled = NULL,
  execution_data = NULL,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  token = NULL,
  auth_args = list(),
  api_base = .fabric_api_base
)

fabric_job_schedule_delete(
  item,
  schedule_id,
  workspace = NULL,
  job_type = NULL,
  item_type = NULL,
  confirm = FALSE,
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

  Item GUID, exact display name, or an item object returned by a
  discovery function. A discovered object is recommended because it
  includes the item type and workspace ID.

- workspace:

  Workspace GUID, exact display name, or a workspace object. Omit it
  when `item` is a discovered object containing `workspaceId`.

- job_type:

  Schedule job type. Notebooks default to `"RunNotebook"`, Spark job
  definitions to `"SparkJob"`, and data pipelines, Dataflows, and Data
  Build Tool Jobs to `"Execute"`. For a Dataflow publish schedule, set
  `job_type = "ApplyChanges"` explicitly. Unknown item types retain the
  Core Scheduler's `"DefaultJob"` fallback. Supply an explicit value for
  another workload-specific schedule job type. When passing one of these
  item types as a GUID instead of a discovered item, also supply
  `item_type` or set the documented `job_type` explicitly.

- item_type:

  Optional Fabric item type when `item` is a GUID. A discovered item
  supplies this automatically.

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

- configuration:

  A value returned by
  [`fabric_job_schedule_config()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_schedule_config.md).
  Advanced callers may pass a named list in the documented Fabric
  `ScheduleConfig` shape. Known types are validated; an unknown future
  `type` is passed through and remains inspectable.

- enabled:

  Whether the schedule is enabled. Fabric can automatically disable
  schedules after repeated failures; updating one with `enabled = TRUE`
  explicitly restarts it.

- execution_data:

  Optional named list of static, workload-specific execution data. Its
  schema is defined by the item's job type. The package preserves it
  without assuming that all workloads share one schema.

- schedule_id:

  Schedule GUID, or a `fabric_job_schedule` record returned by a
  schedule function.

- confirm:

  Must be explicitly set to `TRUE` before a schedule is deleted.

## Value

`fabric_job_schedules()` returns a list of `fabric_job_schedule`
records. Create and update return one such record. Delete invisibly
returns `TRUE`. Records expose normalized common fields and retain the
complete service response in `raw`.

## Details

List operations need an item read permission. Create and update require
item execute and read-write permissions; delete requires item read-write
permission. The current service limit is 20 schedules per item.

`fabric_job_schedule_update()` accepts partial R input for convenience,
but the Fabric PATCH contract requires `enabled` and a complete
`configuration`. When either is omitted, the function first reads the
current schedule and preserves the omitted value. An omitted or `NULL`
`execution_data` is also preserved; supply a named list to replace it.

The published REST response currently exposes `enabled` but no standard
auto-disable reason. `auto_disabled` is therefore `NA` unless Fabric
returns an explicit marker. The complete response stays available in
`raw`.

Semantic-model refresh schedules use the Power BI dataset schedule API,
not the Fabric Core Job Scheduler. These functions reject a discovered
semantic model unless `job_type` is supplied explicitly for a future or
custom route.

## References

[Fabric Job Scheduler REST
API](https://learn.microsoft.com/en-us/rest/api/fabric/core/job-scheduler/)

[Update a semantic-model refresh
schedule](https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/update-refresh-schedule-in-group)

[Schedule a Data
Pipeline](https://learn.microsoft.com/en-us/rest/api/fabric/datapipeline/background-jobs/schedule-execute)

[Schedule Dataflow
Execute](https://learn.microsoft.com/en-us/rest/api/fabric/dataflow/background-jobs/schedule-execute)

[Schedule Dataflow Apply
Changes](https://learn.microsoft.com/en-us/rest/api/fabric/dataflow/background-jobs/schedule-apply-changes)

[Schedule a Data Build Tool
Job](https://learn.microsoft.com/en-us/rest/api/fabric/databuildtooljob/background-jobs/schedule-data-build-tool-job)

[Fabric Data Pipeline REST API
capabilities](https://learn.microsoft.com/en-us/fabric/data-factory/pipeline-rest-api-capabilities)

[Fabric job scheduler
behavior](https://learn.microsoft.com/en-us/fabric/fundamentals/job-scheduler)

## Examples

``` r
if (FALSE) { # \dontrun{
# Discover the Notebook instead of copying workspace and item IDs
workspace <- fabric_workspaces()[[1L]]
notebook <- fabric_notebooks(workspace)[[1L]]

# Inspect existing schedules before creating another one
existing <- fabric_job_schedules(notebook)

# Build a weekly configuration using Fabric's Windows time-zone name
configuration <- fabric_job_schedule_config(
  "Weekly",
  start_time = "2026-10-01T00:00:00Z",
  end_time = "2027-10-01T00:00:00Z",
  time_zone = "W. Europe Standard Time",
  times = "07:30",
  weekdays = c("Monday", "Thursday")
)

# Create, disable, and finally delete the schedule returned by Fabric
schedule <- fabric_job_schedule_create(notebook, configuration)
fabric_job_schedule_update(notebook, schedule, enabled = FALSE)
fabric_job_schedule_delete(notebook, schedule, confirm = TRUE)
} # }
```
