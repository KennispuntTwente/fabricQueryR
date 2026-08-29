# Automate and inspect Microsoft Fabric jobs

A Fabric *job* is a run of an item such as a notebook, data pipeline, or
Spark job definition. ‘fabricQueryR’ can start a run, wait for it,
inspect recent runs, and manage recurring schedules.

Start with one on-demand run. Add a schedule only after that run
succeeds and its parameters and runtime are understood. This guide uses
a notebook, but the same pattern applies to other supported item types.

``` r

library(fabricQueryR)

notebook <- fabric_notebooks("Analytics workspace")[[1]]
```

Discovery returns a read-only `FabricJobItem` R6 object. Read its
service fields directly. Its `$run()`, `$status()`, `$wait()`, and
`$cancel()` methods correspond to
[`fabric_job_run()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_run.md),
[`fabric_job_status()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_run.md),
[`fabric_job_wait()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_run.md),
and
[`fabric_job_cancel()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_run.md);
its schedule methods correspond to the `fabric_job_schedule_*()`
functions.

## Run once and inspect history

Start an on-demand job with `$run()`
([`fabric_job_run()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_run.md))
and wait with `$wait()`
([`fabric_job_wait()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_run.md)):

``` r

job <- notebook$run(
  parameters = list(run_date = Sys.Date(), full_load = FALSE)
)
result <- notebook$wait(job, timeout = 900, cancel_on_timeout = TRUE)
result$status
```

The first call returns immediately with a job handle. The wait call
checks Fabric until the job finishes or the 15-minute local deadline is
reached. `cancel_on_timeout = TRUE` asks Fabric to cancel the run if
that deadline is exceeded.

Notebook submission uses Fabric’s released workload-specific route so
run parameters and compute settings are applied. Polling uses the stable
Core Job Scheduler by default. If a Notebook workflow needs the beta
status fields, such as its exit value, use `$status()`
([`fabric_job_status()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_run.md))
and opt in explicitly:

``` r

detailed <- notebook$status(
  job,
  notebook_details = TRUE,
  respect_retry_after = FALSE
)
detailed$exit_value
```

Running a job needs Execute permission. Reading history needs Read
permission, while changing schedules normally needs Write access. If an
on-demand run works but schedule creation does not, ask the item owner
to check your Write access.

List runs with `$instances()`
([`fabric_job_instances()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_instances.md))
and refresh a result with `$status()`
([`fabric_job_status()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_run.md)):

``` r

history <- notebook$instances()

history[[1]]$invoke_type
history[[1]]$status
history[[1]]$start_time
history[[1]]$failure_reason
```

Refresh a history entry directly without copying its instance ID:

``` r

latest <- notebook$status(history[[1]])
```

## Build schedule configurations

Build and validate a schedule configuration before creating the schedule
in Fabric. Boundaries are UTC instants, while recurring clock times use
a Windows time-zone identifier so Fabric can apply daylight-saving
rules.

``` r

daily <- fabric_job_schedule_config(
  "Daily",
  start_time = "2026-10-01T00:00:00Z",
  end_time = "2027-10-01T00:00:00Z",
  time_zone = "W. Europe Standard Time",
  times = "08:30"
)
```

Fabric supports minute-interval, daily, weekly, and monthly schedules.
Weekly schedules add `weekdays`; monthly schedules select a numbered day
or an ordinal weekday. See
[`?fabric_job_schedule_config`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_schedule_config.md)
for those shapes. Use a Windows time zone such as
`"W. Europe Standard Time"`, not an IANA name such as
`"Europe/Amsterdam"`.

``` r

weekly <- fabric_job_schedule_config(
  "Weekly",
  start_time = "2026-10-01T00:00:00Z",
  end_time = "2027-10-01T00:00:00Z",
  time_zone = "W. Europe Standard Time",
  times = "07:30",
  weekdays = c("Monday", "Thursday")
)
```

## Create, list, update, and disable

Create one schedule with `$schedule_create()`
([`fabric_job_schedule_create()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_schedules.md)),
then list schedules with `$schedules()`
([`fabric_job_schedules()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_schedules.md)):

``` r

schedule <- notebook$schedule_create(weekly, enabled = TRUE)
schedules <- notebook$schedules()
```

Disable or restart a schedule with `$schedule_update()`
([`fabric_job_schedule_update()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_schedules.md))
without rebuilding its configuration:

``` r

disabled <- notebook$schedule_update(
  schedule,
  enabled = FALSE
)

restarted <- notebook$schedule_update(
  schedule,
  enabled = TRUE
)
```

Fabric may disable a schedule after repeated failures. Re-enable it only
after inspecting recent job history and correcting the cause.

## Delete a schedule

Deletion with `$schedule_delete()`
([`fabric_job_schedule_delete()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_schedules.md))
is permanent and requires explicit confirmation:

``` r

notebook$schedule_delete(schedule, confirm = TRUE)
```

Microsoft documents scheduler throttling, maximum job duration and
concurrency, schedule expiry after prolonged user inactivity, and
auto-disable behavior in the [Fabric job scheduler
guide](https://learn.microsoft.com/en-us/fabric/fundamentals/job-scheduler).
