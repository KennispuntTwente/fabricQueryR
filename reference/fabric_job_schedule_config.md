# Build a Microsoft Fabric job schedule configuration

Creates a validated configuration for
[`fabric_job_schedule_create()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_schedules.md)
or
[`fabric_job_schedule_update()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_schedules.md).
Recurrence clock times use the supplied Windows time-zone identifier,
while schedule boundaries are sent to Fabric in UTC.

## Usage

``` r
fabric_job_schedule_config(
  type = "Cron",
  start_time,
  end_time,
  time_zone = "UTC",
  interval = NULL,
  times = NULL,
  weekdays = NULL,
  recurrence = NULL,
  day_of_month = NULL,
  week_index = NULL,
  weekday = NULL
)
```

## Arguments

- type:

  Schedule type: `"Cron"` for a minute interval, `"Daily"`, `"Weekly"`,
  or `"Monthly"`.

- start_time, end_time:

  A scalar `POSIXt` value or RFC 3339 string with an explicit `Z` or
  numeric offset. These boundaries are converted to UTC.

- time_zone:

  Windows time-zone identifier used to interpret `times`, such as
  `"UTC"`, `"W. Europe Standard Time"`, or `"Central Standard Time"`.
  Fabric validates the identifier.

- interval:

  For a `Cron` schedule, a whole-number interval in minutes from 1
  through 5,270,400.

- times:

  For daily, weekly, and monthly schedules, one or more local clock
  times in 24-hour `"HH:MM"` form. The REST contract permits up to 100.

- weekdays:

  For a weekly schedule, one or more English weekday names.

- recurrence:

  For a monthly schedule, the whole-number month interval from 1 through
  12.

- day_of_month:

  For a monthly schedule, a day from 1 through 31. Invalid dates in a
  particular month are skipped by Fabric. Supply this or the
  `week_index` and `weekday` pair.

- week_index:

  For an ordinal monthly schedule, one of `"First"` through `"Fifth"`.

- weekday:

  For an ordinal monthly schedule, one English weekday name.

## Value

A named list using the Fabric `ScheduleConfig` JSON field names.

## Details

A Cron schedule is Fabric's minute-interval schedule; this function does
not accept a cron expression because the REST API does not use one.
Daylight saving behavior is controlled by Fabric using `time_zone`, not
by the R process's local time zone.

## References

[Create item
schedule](https://learn.microsoft.com/en-us/rest/api/fabric/core/job-scheduler/create-item-schedule)

[Windows default time
zones](https://learn.microsoft.com/en-us/windows-hardware/manufacture/desktop/default-time-zones)

## Examples

``` r
daily <- fabric_job_schedule_config(
  "Daily",
  start_time = "2026-10-01T00:00:00Z",
  end_time = "2027-10-01T00:00:00Z",
  time_zone = "W. Europe Standard Time",
  times = c("08:30", "17:00")
)
```
