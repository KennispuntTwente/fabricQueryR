.fabric_schedule_types <- c("Cron", "Daily", "Weekly", "Monthly")
.fabric_schedule_weekdays <- c(
  "Monday",
  "Tuesday",
  "Wednesday",
  "Thursday",
  "Friday",
  "Saturday",
  "Sunday"
)
.fabric_schedule_week_indices <- c(
  "First",
  "Second",
  "Third",
  "Fourth",
  "Fifth"
)

#' Inspect Microsoft Fabric job history
#'
#' Lists recent and active job instances for a Fabric item. All pages returned by
#' Fabric are collected, and each result can be passed directly to
#' [fabric_job_status()], [fabric_job_wait()], or [fabric_job_cancel()].
#'
#' @param item Item GUID, exact display name, or an item record returned by a
#'   discovery function. A discovered record is recommended because it includes
#'   the item type and workspace ID.
#' @param workspace Workspace GUID, exact display name, or a workspace record.
#'   Omit it when `item` is a discovered record containing `workspaceId`.
#' @param item_type Optional Fabric item type when `item` is a GUID. A discovered
#'   item supplies this automatically.
#' @inheritParams fabric_job_run
#'
#' @return A list of `fabric_job_instance` records. Fabric usually retains at
#'   most 100 recently completed instances per item, plus active instances.
#'   Unknown future status and invocation values are returned unchanged.
#' @details
#' Reading history requires an item read permission. The returned records keep
#' an in-process reference to the supplied credential so they can be refreshed,
#' waited on, or cancelled. That credential is not retained when a record is
#' serialized.
#' @references
#' [List item job instances](https://learn.microsoft.com/en-us/rest/api/fabric/core/job-scheduler/list-item-job-instances)
#' @examples
#' \dontrun{
#' # Discover the Notebook whose run history you want to inspect
#' workspace <- fabric_workspaces()[[1L]]
#' notebook <- fabric_notebooks(workspace)[[1L]]
#'
#' # List runs, then refresh one returned job record
#' history <- fabric_job_instances(notebook)
#' history[[1]]$status
#' fabric_job_status(history[[1]])
#' }
#' @export
fabric_job_instances <- function(
  item,
  workspace = NULL,
  item_type = NULL,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  token = NULL,
  auth_args = list(),
  api_base = .fabric_api_base
) {
  context <- .fabric_job_scheduler_context(
    item = item,
    workspace = workspace,
    item_type = item_type,
    job_type = NULL,
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args,
    api_base = api_base,
    use_workspace_endpoint = missing(api_base),
    require_job_type = FALSE
  )
  url <- paste0(
    context$api_base,
    "/workspaces/",
    context$workspace_id,
    "/items/",
    context$item_id,
    "/jobs/instances"
  )
  records <- .httr2_collection(
    url,
    credential = context$credential,
    audience = .fabric_audience$fabric
  )
  instances <- lapply(records, function(record) {
    .fabric_job_history_instance(record, context)
  })
  structure(instances, class = c("fabric_job_instance_list", "list"))
}

#' Build a Microsoft Fabric job schedule configuration
#'
#' Creates a validated configuration for [fabric_job_schedule_create()] or
#' [fabric_job_schedule_update()]. Recurrence clock times use the supplied
#' Windows time-zone identifier, while schedule boundaries are sent to Fabric in
#' UTC.
#'
#' @param type Schedule type: `"Cron"` for a minute interval, `"Daily"`,
#'   `"Weekly"`, or `"Monthly"`.
#' @param start_time,end_time A scalar `POSIXt` value or RFC 3339 string with an
#'   explicit `Z` or numeric offset. These boundaries are converted to UTC.
#' @param time_zone Windows time-zone identifier used to interpret `times`, such
#'   as `"UTC"`, `"W. Europe Standard Time"`, or
#'   `"Central Standard Time"`. Fabric validates the identifier.
#' @param interval For a `Cron` schedule, a whole-number interval in minutes
#'   from 1 through 5,270,400.
#' @param times For daily, weekly, and monthly schedules, one or more local clock
#'   times in 24-hour `"HH:MM"` form. The REST contract permits up to 100.
#' @param weekdays For a weekly schedule, one or more English weekday names.
#' @param recurrence For a monthly schedule, the whole-number month interval
#'   from 1 through 12.
#' @param day_of_month For a monthly schedule, a day from 1 through 31. Invalid
#'   dates in a particular month are skipped by Fabric. Supply this or the
#'   `week_index` and `weekday` pair.
#' @param week_index For an ordinal monthly schedule, one of `"First"` through
#'   `"Fifth"`.
#' @param weekday For an ordinal monthly schedule, one English weekday name.
#'
#' @return A named list using the Fabric `ScheduleConfig` JSON field names.
#' @details
#' A Cron schedule is Fabric's minute-interval schedule; this function does not
#' accept a cron expression because the REST API does not use one. Daylight
#' saving behavior is controlled by Fabric using `time_zone`, not by the R
#' process's local time zone.
#' @references
#' [Create item schedule](https://learn.microsoft.com/en-us/rest/api/fabric/core/job-scheduler/create-item-schedule)
#'
#' [Windows default time zones](https://learn.microsoft.com/en-us/windows-hardware/manufacture/desktop/default-time-zones)
#' @examples
#' # Describe a schedule in the Windows time zone used by Fabric
#' daily <- fabric_job_schedule_config(
#'   "Daily",
#'   start_time = "2026-10-01T00:00:00Z",
#'   end_time = "2027-10-01T00:00:00Z",
#'   time_zone = "W. Europe Standard Time",
#'   times = c("08:30", "17:00")
#' )
#' @export
fabric_job_schedule_config <- function(
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
) {
  type <- .fabric_schedule_match(type, .fabric_schedule_types, "type")
  configuration <- list(
    startDateTime = .fabric_schedule_utc(start_time, "start_time"),
    endDateTime = .fabric_schedule_utc(end_time, "end_time"),
    localTimeZoneId = time_zone,
    type = type
  )

  if (identical(type, "Cron")) {
    .fabric_schedule_integer(interval, 1L, 5270400L, "interval")
    configuration$interval <- as.integer(interval)
  } else {
    configuration$times <- .fabric_schedule_times(times)
  }

  if (identical(type, "Weekly")) {
    configuration$weekdays <- .fabric_schedule_values(
      weekdays,
      .fabric_schedule_weekdays,
      "weekdays",
      multiple = TRUE
    )
  }

  if (identical(type, "Monthly")) {
    .fabric_schedule_integer(recurrence, 1L, 12L, "recurrence")
    configuration$recurrence <- as.integer(recurrence)
    if (!is.null(day_of_month)) {
      if (!is.null(week_index) || !is.null(weekday)) {
        .fabric_abort(
          "`day_of_month` cannot be combined with `week_index` or `weekday`"
        )
      }
      .fabric_schedule_integer(day_of_month, 1L, 31L, "day_of_month")
      configuration$occurrence <- list(
        occurrenceType = "DayOfMonth",
        dayOfMonth = as.integer(day_of_month)
      )
    } else {
      if (is.null(week_index) || is.null(weekday)) {
        .fabric_abort(
          "Monthly schedules need `day_of_month` or both `week_index` and `weekday`"
        )
      }
      configuration$occurrence <- list(
        occurrenceType = "OrdinalWeekday",
        weekIndex = .fabric_schedule_match(
          week_index,
          .fabric_schedule_week_indices,
          "week_index"
        ),
        weekday = .fabric_schedule_match(
          weekday,
          .fabric_schedule_weekdays,
          "weekday"
        )
      )
    }
  }

  .fabric_job_schedule_configuration(configuration)
}

#' Manage Microsoft Fabric item schedules
#'
#' List, create, update, or delete recurring schedules for a supported Fabric
#' item. Use [fabric_job_schedule_config()] to construct the four schedule types
#' in the current REST contract.
#'
#' @inheritParams fabric_job_instances
#' @param job_type Schedule job type. Data pipelines, Dataflows, and Data Build
#'   Tool Jobs default to `"Execute"`. For a Dataflow publish schedule, set
#'   `job_type = "ApplyChanges"` explicitly. Other item types default to
#'   `"DefaultJob"`, as shown in the Core Job Scheduler examples. Supply an
#'   explicit value for another workload-specific schedule job type. When
#'   passing one of these item types as a GUID instead of a discovered item,
#'   also supply `item_type` or set the documented `job_type` explicitly.
#' @param configuration A value returned by
#'   [fabric_job_schedule_config()]. Advanced callers may pass a named list in
#'   the documented Fabric `ScheduleConfig` shape. Known types are validated;
#'   an unknown future `type` is passed through and remains inspectable.
#' @param enabled Whether the schedule is enabled. Fabric can automatically
#'   disable schedules after repeated failures; updating one with
#'   `enabled = TRUE` explicitly restarts it.
#' @param execution_data Optional named list of static, workload-specific
#'   execution data. Its schema is defined by the item's job type. The package
#'   preserves it without assuming that all workloads share one schema.
#' @param schedule_id Schedule GUID, or a `fabric_job_schedule` record returned
#'   by a schedule function.
#' @param confirm Must be explicitly set to `TRUE` before a schedule is deleted.
#'
#' @return `fabric_job_schedules()` returns a list of `fabric_job_schedule`
#'   records. Create and update return one such record. Delete invisibly returns
#'   `TRUE`. Records expose normalized common fields and retain the complete
#'   service response in `raw`.
#' @details
#' List operations need an item read permission. Create and update require item
#' execute and read-write permissions; delete requires item read-write
#' permission. The current service limit is 20 schedules per item.
#'
#' `fabric_job_schedule_update()` accepts partial R input for convenience, but
#' the Fabric PATCH contract requires `enabled` and a complete `configuration`.
#' When either is omitted, the function first reads the current schedule and
#' preserves the omitted value. An omitted or `NULL` `execution_data` is also
#' preserved; supply a named list to replace it.
#'
#' The published REST response currently exposes `enabled` but no standard
#' auto-disable reason. `auto_disabled` is therefore `NA` unless Fabric returns
#' an explicit marker. The complete response stays available in `raw`.
#' @references
#' [Fabric Job Scheduler REST API](https://learn.microsoft.com/en-us/rest/api/fabric/core/job-scheduler/)
#'
#' [Schedule a Data Pipeline](https://learn.microsoft.com/en-us/rest/api/fabric/datapipeline/background-jobs/schedule-execute)
#'
#' [Schedule Dataflow Execute](https://learn.microsoft.com/en-us/rest/api/fabric/dataflow/background-jobs/schedule-execute)
#'
#' [Schedule Dataflow Apply Changes](https://learn.microsoft.com/en-us/rest/api/fabric/dataflow/background-jobs/schedule-apply-changes)
#'
#' [Schedule a Data Build Tool Job](https://learn.microsoft.com/en-us/rest/api/fabric/databuildtooljob/background-jobs/schedule-data-build-tool-job)
#'
#' [Fabric Data Pipeline REST API capabilities](https://learn.microsoft.com/en-us/fabric/data-factory/pipeline-rest-api-capabilities)
#'
#' [Fabric job scheduler behavior](https://learn.microsoft.com/en-us/fabric/fundamentals/job-scheduler)
#' @examples
#' \dontrun{
#' # Discover the Notebook instead of copying workspace and item IDs
#' workspace <- fabric_workspaces()[[1L]]
#' notebook <- fabric_notebooks(workspace)[[1L]]
#'
#' # Inspect existing schedules before creating another one
#' existing <- fabric_job_schedules(notebook)
#'
#' # Build a weekly configuration using Fabric's Windows time-zone name
#' configuration <- fabric_job_schedule_config(
#'   "Weekly",
#'   start_time = "2026-10-01T00:00:00Z",
#'   end_time = "2027-10-01T00:00:00Z",
#'   time_zone = "W. Europe Standard Time",
#'   times = "07:30",
#'   weekdays = c("Monday", "Thursday")
#' )
#'
#' # Create, disable, and finally delete the schedule returned by Fabric
#' schedule <- fabric_job_schedule_create(notebook, configuration)
#' fabric_job_schedule_update(notebook, schedule, enabled = FALSE)
#' fabric_job_schedule_delete(notebook, schedule, confirm = TRUE)
#' }
#' @export
fabric_job_schedules <- function(
  item,
  workspace = NULL,
  job_type = NULL,
  item_type = NULL,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  token = NULL,
  auth_args = list(),
  api_base = .fabric_api_base
) {
  context <- .fabric_job_scheduler_context(
    item = item,
    workspace = workspace,
    item_type = item_type,
    job_type = job_type,
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args,
    api_base = api_base,
    use_workspace_endpoint = missing(api_base)
  )
  records <- .httr2_collection(
    .fabric_job_schedules_url(context),
    credential = context$credential,
    audience = .fabric_audience$fabric
  )
  schedules <- lapply(records, .fabric_job_schedule_record, context = context)
  structure(schedules, class = c("fabric_job_schedule_list", "list"))
}

#' @rdname fabric_job_schedules
#' @export
fabric_job_schedule_create <- function(
  item,
  configuration,
  workspace = NULL,
  job_type = NULL,
  item_type = NULL,
  enabled = TRUE,
  execution_data = NULL,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  token = NULL,
  auth_args = list(),
  api_base = .fabric_api_base
) {
  .fabric_schedule_flag(enabled, "enabled")
  configuration <- .fabric_job_schedule_configuration(configuration)
  if (!is.null(execution_data)) {
    .fabric_job_named_list(execution_data, "execution_data")
  }
  context <- .fabric_job_scheduler_context(
    item = item,
    workspace = workspace,
    item_type = item_type,
    job_type = job_type,
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args,
    api_base = api_base,
    use_workspace_endpoint = missing(api_base)
  )
  payload <- list(enabled = enabled, configuration = configuration)
  if (!is.null(execution_data)) {
    payload$executionData <- execution_data
  }
  result <- .fabric_job_request(
    "POST",
    .fabric_job_schedules_url(context),
    context$credential,
    payload = payload,
    idempotent = FALSE
  )
  .fabric_job_schedule_response(result, context, expected_status = 201L)
}

#' @rdname fabric_job_schedules
#' @export
fabric_job_schedule_update <- function(
  item,
  schedule_id,
  configuration = NULL,
  workspace = NULL,
  job_type = NULL,
  item_type = NULL,
  enabled = NULL,
  execution_data = NULL,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  token = NULL,
  auth_args = list(),
  api_base = .fabric_api_base
) {
  execution_data_supplied <- !missing(execution_data) &&
    !is.null(execution_data)
  if (is.null(job_type) && inherits(schedule_id, "fabric_job_schedule")) {
    job_type <- schedule_id$job_type
  }
  context <- .fabric_job_scheduler_context(
    item = item,
    workspace = workspace,
    item_type = item_type,
    job_type = job_type,
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args,
    api_base = api_base,
    use_workspace_endpoint = missing(api_base)
  )
  id <- .fabric_job_schedule_id(schedule_id)
  current <- NULL
  if (is.null(configuration) || is.null(enabled) || !execution_data_supplied) {
    current <- .fabric_job_schedule_get(context, id)
  }
  # Fabric may return offset-free boundaries and list-backed JSON arrays, so
  # preserve the trusted service shape while validating caller input strictly
  configuration <- if (is.null(configuration)) {
    current$configuration
  } else {
    .fabric_job_schedule_configuration(configuration)
  }
  enabled <- enabled %||% current$enabled
  .fabric_schedule_flag(enabled, "enabled")
  if (!execution_data_supplied) {
    execution_data <- current$execution_data
  } else {
    .fabric_job_named_list(execution_data, "execution_data")
  }
  payload <- list(enabled = enabled, configuration = configuration)
  if (!is.null(execution_data)) {
    payload$executionData <- execution_data
  }
  result <- .fabric_job_request(
    "PATCH",
    .fabric_job_schedule_url(context, id),
    context$credential,
    payload = payload,
    idempotent = TRUE
  )
  .fabric_job_schedule_response(result, context, expected_status = 200L)
}

#' @rdname fabric_job_schedules
#' @export
fabric_job_schedule_delete <- function(
  item,
  schedule_id,
  workspace = NULL,
  job_type = NULL,
  item_type = NULL,
  confirm = FALSE,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  token = NULL,
  auth_args = list(),
  api_base = .fabric_api_base
) {
  if (!isTRUE(confirm) || length(confirm) != 1L) {
    .fabric_abort(
      "Set `confirm = TRUE` to delete this Fabric job schedule",
      class = "fabric_job_schedule_confirmation_error"
    )
  }
  if (is.null(job_type) && inherits(schedule_id, "fabric_job_schedule")) {
    job_type <- schedule_id$job_type
  }
  context <- .fabric_job_scheduler_context(
    item = item,
    workspace = workspace,
    item_type = item_type,
    job_type = job_type,
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args,
    api_base = api_base,
    use_workspace_endpoint = missing(api_base)
  )
  id <- .fabric_job_schedule_id(schedule_id)
  result <- .fabric_job_request(
    "DELETE",
    .fabric_job_schedule_url(context, id),
    context$credential,
    idempotent = TRUE,
    parse_json = FALSE
  )
  if (!result$status_code %in% c(200L, 204L)) {
    .fabric_abort(
      sprintf(
        "Fabric returned HTTP %d after schedule deletion",
        result$status_code
      ),
      class = c("fabric_job_schedule_protocol_error", "fabric_job_error")
    )
  }
  invisible(TRUE)
}

#' Print a Fabric job schedule
#'
#' @param x A `fabric_job_schedule` returned by a schedule function.
#' @param ... Reserved for the print method.
#' @return `x`, invisibly.
#' @export
print.fabric_job_schedule <- function(x, ...) {
  .fabric_print(
    "fabric_job_schedule",
    list(
      schedule = x$id,
      type = x$type %||% "unknown",
      state = x$state,
      `job type` = x$job_type
    )
  )
  invisible(x)
}

.fabric_job_scheduler_context <- function(
  item,
  workspace,
  item_type,
  job_type,
  tenant_id,
  client_id,
  token,
  auth_args,
  api_base,
  use_workspace_endpoint,
  require_job_type = TRUE
) {
  credential <- fabric_credential(
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args
  )
  base <- fabric_api_base(api_base)
  target <- .fabric_job_target(
    item,
    workspace,
    item_type,
    credential,
    base,
    use_workspace_endpoint = use_workspace_endpoint
  )
  schedule_job_type <- if (isTRUE(require_job_type)) {
    .fabric_job_schedule_type(target$item_type, job_type)
  } else {
    job_type
  }
  if (!is.null(schedule_job_type)) {
    .fabric_job_path_segment(schedule_job_type, "job_type")
  }
  list(
    workspace_id = target$workspace_id,
    item_id = target$item_id,
    item_type = target$item_type,
    job_type = schedule_job_type,
    route = .fabric_job_route_name(target$item_type),
    api_base = target$api_base,
    credential = credential
  )
}

# Infer the schedule job type for workloads whose Fabric job type differs from
# the Core Scheduler examples. Returns an explicit value unchanged
.fabric_job_schedule_type <- function(item_type, job_type) {
  if (!is.null(job_type)) {
    return(job_type)
  }

  normalized <- gsub("[^a-z0-9]", "", tolower(item_type %||% ""))
  if (
    normalized %in%
      c("datapipeline", "pipeline", "dataflow", "databuildtooljob")
  ) {
    "Execute"
  } else {
    "DefaultJob"
  }
}

.fabric_job_route_name <- function(item_type) {
  normalized <- gsub("[^a-z0-9]", "", tolower(item_type %||% ""))
  if (identical(normalized, "notebook")) {
    "notebook"
  } else if (identical(normalized, "sparkjobdefinition")) {
    "spark_job_definition"
  } else {
    "core"
  }
}

.fabric_job_history_instance <- function(body, context) {
  .fabric_job_guid(body$id, "job instance ID")
  job_type <- body$jobType %||% context$job_type
  credential_reference <- .fabric_job_credential_reference(context$credential)
  handle <- structure(
    list(
      id = body$id,
      workspace_id = context$workspace_id,
      item_id = body$itemId %||% context$item_id,
      item_type = context$item_type,
      job_type = job_type,
      api_base = context$api_base,
      route = context$route,
      credential = credential_reference$reference,
      .credential_key = credential_reference$key
    ),
    class = "fabric_job"
  )
  instance_context <- unclass(handle)
  instance_context$job <- handle
  .fabric_job_instance(
    body,
    instance_context,
    retry_after = NULL,
    visible = TRUE
  )
}

.fabric_job_schedules_url <- function(context) {
  paste0(
    context$api_base,
    "/workspaces/",
    context$workspace_id,
    "/items/",
    context$item_id,
    "/jobs/",
    context$job_type,
    "/schedules"
  )
}

.fabric_job_schedule_url <- function(context, schedule_id) {
  paste0(.fabric_job_schedules_url(context), "/", schedule_id)
}

.fabric_job_schedule_id <- function(schedule_id) {
  if (inherits(schedule_id, "fabric_job_schedule")) {
    schedule_id <- schedule_id$id
  }
  .fabric_job_guid(schedule_id, "schedule ID")
  schedule_id
}

.fabric_job_schedule_get <- function(context, schedule_id) {
  result <- .fabric_job_request(
    "GET",
    .fabric_job_schedule_url(context, schedule_id),
    context$credential,
    idempotent = TRUE
  )
  .fabric_job_schedule_response(result, context, expected_status = 200L)
}

.fabric_job_schedule_response <- function(result, context, expected_status) {
  if (
    !identical(result$status_code, expected_status) || !is.list(result$body)
  ) {
    .fabric_abort(
      sprintf(
        "Fabric returned an invalid schedule response (expected HTTP %d, received HTTP %d)",
        expected_status,
        result$status_code
      ),
      class = c("fabric_job_schedule_protocol_error", "fabric_job_error"),
      response = result$body
    )
  }
  .fabric_job_schedule_record(result$body, context)
}

.fabric_job_schedule_record <- function(body, context) {
  .fabric_job_guid(body$id, "schedule ID")
  .fabric_schedule_flag(body$enabled, "schedule enabled")
  configuration <- body$configuration %||% list()
  .fabric_job_named_list(configuration, "schedule configuration")
  service_state <- body$state %||% body$status
  auto_disabled_marker <- body$autoDisabled %||% body$isAutoDisabled
  auto_disabled <- if (
    is.logical(auto_disabled_marker) &&
      length(auto_disabled_marker) == 1L &&
      !is.na(auto_disabled_marker)
  ) {
    auto_disabled_marker
  } else if (
    is.character(service_state) &&
      length(service_state) == 1L &&
      tolower(gsub("[^a-z]", "", service_state)) == "autodisabled"
  ) {
    TRUE
  } else {
    NA
  }
  state <- if (isTRUE(auto_disabled)) {
    "AutoDisabled"
  } else if (isTRUE(body$enabled)) {
    "Enabled"
  } else if (identical(body$enabled, FALSE)) {
    "Disabled"
  } else {
    service_state %||% "Unknown"
  }
  structure(
    list(
      id = body$id,
      workspace_id = context$workspace_id,
      item_id = context$item_id,
      item_type = context$item_type,
      job_type = context$job_type,
      enabled = body$enabled,
      auto_disabled = auto_disabled,
      state = state,
      type = configuration$type,
      start_time = .fabric_job_time(configuration$startDateTime),
      end_time = .fabric_job_time(configuration$endDateTime),
      time_zone_id = configuration$localTimeZoneId,
      configuration = configuration,
      execution_data = body$executionData,
      owner = body$owner,
      created_time = .fabric_job_time(body$createdDateTime),
      raw = body
    ),
    class = "fabric_job_schedule"
  )
}

.fabric_job_schedule_configuration <- function(configuration) {
  if (inherits(configuration, "fabric_job_schedule_config")) {
    class(configuration) <- NULL
  }
  .fabric_job_named_list(configuration, "configuration")
  .fabric_job_nonempty(configuration$type, "configuration$type")
  type_index <- match(
    tolower(configuration$type),
    tolower(.fabric_schedule_types)
  )
  if (is.na(type_index)) {
    return(configuration)
  }
  configuration$type <- .fabric_schedule_types[[type_index]]
  required <- c("startDateTime", "endDateTime", "localTimeZoneId")
  missing_fields <- required[!required %in% names(configuration)]
  if (length(missing_fields)) {
    .fabric_abort(paste0(
      "Known schedule configurations are missing: ",
      paste(missing_fields, collapse = ", ")
    ))
  }
  configuration$startDateTime <- .fabric_schedule_utc(
    configuration$startDateTime,
    "configuration$startDateTime"
  )
  configuration$endDateTime <- .fabric_schedule_utc(
    configuration$endDateTime,
    "configuration$endDateTime"
  )
  if (
    .fabric_job_time(configuration$endDateTime) <=
      .fabric_job_time(configuration$startDateTime)
  ) {
    .fabric_abort("The schedule end time must be later than its start time")
  }
  .fabric_job_nonempty(
    configuration$localTimeZoneId,
    "configuration$localTimeZoneId"
  )

  if (identical(configuration$type, "Cron")) {
    .fabric_schedule_integer(configuration$interval, 1L, 5270400L, "interval")
    configuration$interval <- as.integer(configuration$interval)
  } else {
    configuration$times <- .fabric_schedule_times(configuration$times)
  }

  if (identical(configuration$type, "Weekly")) {
    configuration$weekdays <- .fabric_schedule_values(
      configuration$weekdays,
      .fabric_schedule_weekdays,
      "weekdays",
      multiple = TRUE
    )
  }

  if (identical(configuration$type, "Monthly")) {
    .fabric_schedule_integer(configuration$recurrence, 1L, 12L, "recurrence")
    configuration$recurrence <- as.integer(configuration$recurrence)
    occurrence <- configuration$occurrence
    .fabric_job_named_list(occurrence, "configuration$occurrence")
    occurrence_type <- .fabric_schedule_match(
      occurrence$occurrenceType,
      c("DayOfMonth", "OrdinalWeekday"),
      "configuration$occurrence$occurrenceType"
    )
    occurrence$occurrenceType <- occurrence_type
    if (identical(occurrence_type, "DayOfMonth")) {
      .fabric_schedule_integer(
        occurrence$dayOfMonth,
        1L,
        31L,
        "configuration$occurrence$dayOfMonth"
      )
      occurrence$dayOfMonth <- as.integer(occurrence$dayOfMonth)
    } else {
      occurrence$weekIndex <- .fabric_schedule_match(
        occurrence$weekIndex,
        .fabric_schedule_week_indices,
        "configuration$occurrence$weekIndex"
      )
      occurrence$weekday <- .fabric_schedule_match(
        occurrence$weekday,
        .fabric_schedule_weekdays,
        "configuration$occurrence$weekday"
      )
    }
    configuration$occurrence <- occurrence
  }
  structure(configuration, class = c("fabric_job_schedule_config", "list"))
}

.fabric_schedule_utc <- function(value, name) {
  if (inherits(value, "POSIXt")) {
    if (length(value) != 1L || is.na(value)) {
      .fabric_abort(sprintf("`%s` must be one non-missing date-time", name))
    }
    parsed <- as.POSIXct(value)
  } else if (
    is.character(value) &&
      length(value) == 1L &&
      !is.na(value) &&
      grepl("(Z|[+-][0-9]{2}:[0-9]{2})$", value)
  ) {
    parsed <- .fabric_job_time(value)
  } else {
    .fabric_abort(paste0(
      "`",
      name,
      "` must be a POSIX date-time or RFC 3339 string with an explicit offset"
    ))
  }
  if (is.na(parsed)) {
    .fabric_abort(sprintf("`%s` is not a valid date-time", name))
  }
  format(parsed, "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
}

.fabric_schedule_times <- function(times) {
  if (
    !is.character(times) ||
      !length(times) ||
      length(times) > 100L ||
      anyNA(times) ||
      !all(grepl("^(?:[01][0-9]|2[0-3]):[0-5][0-9]$", times))
  ) {
    .fabric_abort(
      "`times` must contain 1 to 100 unique clock times in HH:MM form"
    )
  }
  if (anyDuplicated(times)) {
    .fabric_abort("`times` must not contain duplicates")
  }
  unname(times)
}

.fabric_schedule_values <- function(value, choices, name, multiple) {
  if (
    !is.character(value) ||
      !length(value) ||
      anyNA(value) ||
      (!isTRUE(multiple) && length(value) != 1L)
  ) {
    .fabric_abort(sprintf("`%s` must contain valid schedule values", name))
  }
  index <- match(tolower(value), tolower(choices))
  if (anyNA(index) || anyDuplicated(index)) {
    .fabric_abort(sprintf(
      "`%s` must use unique values from: %s",
      name,
      paste(choices, collapse = ", ")
    ))
  }
  unname(choices[index])
}

.fabric_schedule_match <- function(value, choices, name) {
  .fabric_schedule_values(value, choices, name, multiple = FALSE)[[1L]]
}

.fabric_schedule_integer <- function(value, minimum, maximum, name) {
  if (
    !is.numeric(value) ||
      length(value) != 1L ||
      is.na(value) ||
      !is.finite(value) ||
      value != floor(value) ||
      value < minimum ||
      value > maximum
  ) {
    .fabric_abort(sprintf(
      "`%s` must be one whole number from %d through %d",
      name,
      minimum,
      maximum
    ))
  }
  invisible(value)
}

.fabric_schedule_flag <- function(value, name) {
  if (!is.logical(value) || length(value) != 1L || is.na(value)) {
    .fabric_abort(sprintf("`%s` must be TRUE or FALSE", name))
  }
  invisible(value)
}
