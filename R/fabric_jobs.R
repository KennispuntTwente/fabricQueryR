.fabric_job_terminal_states <- c(
  "Completed",
  "Failed",
  "Cancelled",
  "Deduped"
)
.fabric_job_active_states <- c("NotStarted", "InProgress")

.fabric_job_parameter_types <- c(
  "VariableReference",
  "Integer",
  "Number",
  "Text",
  "Boolean",
  "DateTime",
  "Guid",
  "Automatic"
)

#' Run and monitor Microsoft Fabric item jobs
#'
#' Starts one on-demand run of a supported Fabric item, then lets R inspect,
#' wait for, or cancel that run. Typical examples are running a Notebook, data
#' pipeline, or Spark job definition. This is for immediate runs; configure a
#' recurring timetable with Fabric's scheduler in the portal or scheduler API.
#'
#' @param item Item GUID, exact display name, or an item record returned
#'   by a discovery function. A discovered record is recommended because it
#'   already includes the item type and workspace ID.
#' @param workspace Workspace GUID, exact display name, or a workspace record.
#'   Omit it when `item` is a discovered record containing `workspaceId`.
#' @param job_type Fabric job type. Known defaults are `"RunNotebook"` for
#'   notebooks, `"Pipeline"` for data pipelines, and `"SparkJob"` for Spark job
#'   definitions. Normally omit it for those item types; supply the API's job
#'   type for another supported item.
#' @param item_type Optional Fabric item type when `item` is a GUID. A discovered
#'   item supplies this automatically. Examples are `"Notebook"`,
#'   `"DataPipeline"`, and `"SparkJobDefinition"`.
#' @param parameters A named list of scalar parameter values, or a list of
#'   records containing `name`, `value`, and `type`. Names are compared
#'   case-insensitively. The simple form, such as
#'   `list(run_date = as.Date("2026-01-31"), full_load = FALSE)`, infers types
#'   from R values and is appropriate for most runs. Parameter names must match
#'   those configured in the Fabric item.
#' @param parameter_types Optional named character vector overriding inferred
#'   parameter types. Supported values are `VariableReference`, `Integer`,
#'   `Number`, `Text`, `Boolean`, `DateTime`, `Guid`, and `Automatic`. Use this
#'   only when R's inferred type is not the type expected in Fabric.
#' @param execution_data Optional advanced workload configuration in the shape
#'   documented by Fabric. For notebooks this contains `compute` and optionally
#'   `computeConfiguration`; for Spark job definitions it is a named list of
#'   execution overrides. Data pipelines do not accept it here. Use the simpler
#'   arguments below for common notebook settings.
#' @param default_lakehouse Optional Lakehouse GUID or discovered record used to
#'   set the notebook's default Lakehouse for this run. This changes the run
#'   context, not the notebook's saved default.
#' @param default_lakehouse_workspace Optional workspace GUID or discovered
#'   record for `default_lakehouse`; defaults to the job workspace.
#' @param compute Notebook compute kind: `"Spark"`, `"Jupyter"`, or
#'   `"DataWarehouse"`. Use `"Spark"` (the default) for Spark notebooks,
#'   `"Jupyter"` for a Jupyter runtime, and `"DataWarehouse"` for a notebook
#'   attached to Warehouse compute. It must match what the notebook code needs.
#' @param session_tag Optional Spark high-concurrency session tag containing
#'   only letters, numbers, and underscores. Supplying it enables Fabric's
#'   high-concurrency mode so related notebook runs may reuse Spark compute.
#'   High-concurrency runs also change how failures are reported: Fabric keeps
#'   the shared session alive when a statement fails, so the run is reported as
#'   `Completed` with no exit value instead of `Failed`. Omit `session_tag`
#'   when the caller must detect a failed notebook from the job status, and
#'   have the notebook signal its own outcome through
#'   `mssparkutils.notebook.exit()` otherwise.
#' @param tenant_id Entra tenant ID. Defaults to
#'   `FABRICQUERYR_TENANT_ID`.
#' @param client_id Entra application ID. Defaults to
#'   `FABRICQUERYR_CLIENT_ID`, then the Azure CLI application ID.
#' @param token Preferred token input: an `AzureAuth::AzureToken` object,
#'   bearer-token string, or token-provider function. With `NULL`, `AzureAuth`
#'   reuses a matching cached token or starts its normal interactive login flow.
#' @param auth_args Named list of additional arguments passed to
#'   [AzureAuth::get_azure_token()] when no token source is supplied.
#'   Job submission and cancellation require `Item.Execute.All` or
#'   the corresponding workload-specific execute permission.
#' @param api_base Fabric REST API base URL. Most users should keep the default.
#' @details Notebook status uses Fabric's workload-specific beta endpoint first
#'   and falls back to the core scheduler when that endpoint is unavailable.
#'   A beta response that says `Completed` but contains neither an exit value
#'   nor failure details is reconciled with the core scheduler before it is
#'   returned. This prevents a failed notebook cell from being reported as a
#'   successful run while the two Fabric status stores converge.
#'   Because Fabric may add job statuses over time, [fabric_job_wait()] raises a
#'   `fabric_job_unknown_status` condition for an unrecognised state instead of
#'   polling until timeout.
#' @references
#' [Core Job Scheduler REST API](https://learn.microsoft.com/en-us/rest/api/fabric/core/job-scheduler/)
#'
#' [Run an on-demand notebook](https://learn.microsoft.com/en-us/rest/api/fabric/notebook/background-jobs/run-on-demand-notebook)
#'
#' [Manage and execute notebooks with public APIs](https://learn.microsoft.com/en-us/fabric/data-engineering/notebook-public-api)
#'
#' [Fabric job scheduler](https://learn.microsoft.com/en-us/fabric/fundamentals/job-scheduler)
#'
#' @return `fabric_job_run()` returns a `fabric_job` handle containing the job
#'   instance ID and resolved workspace, item, job type, status URL, and
#'   authentication context.
#'   `fabric_job_status()` and `fabric_job_wait()` return a
#'   `fabric_job_instance` list with `status`, start/end times,
#'   `failure_reason`, notebook `exit_value` when available, workload
#'   `properties`, and `raw` response. `fabric_job_cancel()` returns `TRUE`
#'   invisibly after Fabric accepts the cancellation request; terminal state
#'   may not be visible immediately.
#' @examples
#' \dontrun{
#' notebook <- fabric_notebooks("Analytics workspace")[[1]]
#'
#' job <- fabric_job_run(
#'   notebook,
#'   parameters = list(run_date = Sys.Date(), full_load = FALSE)
#' )
#'
#' completed <- fabric_job_wait(job, timeout = 900)
#' completed$status
#' completed$exit_value
#' }
#' @export
fabric_job_run <- function(
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
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  token = NULL,
  auth_args = list(),
  api_base = .fabric_api_base
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
    base
  )
  route <- .fabric_job_route(target$item_type, job_type)
  execution_data <- .fabric_job_execution_data(
    target = target,
    route = route,
    execution_data = execution_data,
    default_lakehouse = default_lakehouse,
    default_lakehouse_workspace = default_lakehouse_workspace,
    compute = compute,
    session_tag = session_tag
  )
  parameters <- .fabric_job_parameters(parameters, parameter_types)
  payload <- Filter(
    Negate(is.null),
    list(
      executionData = execution_data,
      parameters = if (length(parameters)) parameters else NULL
    )
  )
  if (!length(payload)) {
    payload <- NULL
  }
  url <- .fabric_job_run_url(base, target, route)
  result <- .fabric_job_request(
    "POST",
    url,
    credential,
    payload = payload,
    idempotent = FALSE,
    parse_json = FALSE
  )
  location <- result$location
  if (is.null(location) || !nzchar(location)) {
    rlang::abort(
      "Fabric accepted the job but did not return a Location header",
      class = "fabric_job_protocol_error"
    )
  }
  instance_id <- .fabric_job_id_from_location(location)
  if (!fabric_is_guid(instance_id)) {
    rlang::abort(
      "Fabric returned a Location header without a valid job instance ID",
      class = "fabric_job_protocol_error"
    )
  }

  structure(
    list(
      id = instance_id,
      workspace_id = target$workspace_id,
      item_id = target$item_id,
      item_type = target$item_type,
      job_type = route$job_type,
      location = location,
      retry_after = result$retry_after,
      submitted_at = Sys.time(),
      api_base = base,
      route = route$route,
      credential = credential
    ),
    class = "fabric_job"
  )
}

#' @param job A `fabric_job` returned by [fabric_job_run()], or a job instance
#'   GUID. When a GUID is supplied, also provide `workspace`, `item`, and
#'   enough type information to reconstruct the status URL. The handle is
#'   simpler because it already stores that context.
#' @param job_instance_id Alternative argument for a job instance GUID. Do not
#'   supply it together with a `fabric_job` handle.
#' @rdname fabric_job_run
#' @export
fabric_job_status <- function(
  job = NULL,
  workspace = NULL,
  item = NULL,
  job_instance_id = NULL,
  item_type = NULL,
  job_type = NULL,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  token = NULL,
  auth_args = list(),
  api_base = .fabric_api_base
) {
  context <- .fabric_job_context(
    job = job,
    workspace = workspace,
    item = item,
    job_instance_id = job_instance_id,
    item_type = item_type,
    job_type = job_type,
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args,
    api_base = api_base
  )
  .fabric_job_get_status(context, allow_not_found = FALSE)
}

#' @param poll_interval Minimum seconds between status requests. `NULL` uses
#'   Fabric's recommended `Retry-After` value, falling back to two seconds.
#'   Setting a value never polls faster than Fabric requests.
#' @param timeout Maximum seconds to wait before raising a
#'   `fabric_job_timeout`.
#' @param error_on_failure Whether failed, cancelled, or deduplicated jobs raise
#'   typed errors. Set to `FALSE` to inspect those terminal results directly.
#' @param cancel_on_timeout Ask Fabric to cancel the job when the client-side
#'   timeout expires. `FALSE` stops waiting but leaves the Fabric job running.
#' @param cancel Optional callback checked between polls. Returning `TRUE`
#'   cancels the Fabric job and raises a `fabric_job_cancelled_by_caller`
#'   condition. This is useful for an application-specific stop button.
#' @param .sleep,.now Internal hooks for deterministic tests.
#' @rdname fabric_job_run
#' @export
fabric_job_wait <- function(
  job,
  poll_interval = NULL,
  timeout = 600,
  error_on_failure = TRUE,
  cancel_on_timeout = FALSE,
  cancel = NULL,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  token = NULL,
  auth_args = list(),
  api_base = .fabric_api_base,
  .sleep = Sys.sleep,
  .now = Sys.time
) {
  if (!inherits(job, "fabric_job")) {
    rlang::abort("`job` must be a `fabric_job` returned by fabric_job_run()")
  }
  .fabric_job_scalar_number(timeout, "timeout", minimum = 0)
  if (!is.null(poll_interval)) {
    .fabric_job_scalar_number(
      poll_interval,
      "poll_interval",
      minimum = 0
    )
  }
  if (
    !is.logical(error_on_failure) ||
      length(error_on_failure) != 1L ||
      is.na(error_on_failure)
  ) {
    rlang::abort("`error_on_failure` must be TRUE or FALSE")
  }
  if (
    !is.logical(cancel_on_timeout) ||
      length(cancel_on_timeout) != 1L ||
      is.na(cancel_on_timeout)
  ) {
    rlang::abort("`cancel_on_timeout` must be TRUE or FALSE")
  }
  if (!is.null(cancel) && !is.function(cancel)) {
    rlang::abort("`cancel` must be a function or NULL")
  }

  context <- .fabric_job_context(
    job = job,
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args,
    api_base = api_base
  )
  started <- .now()
  last <- NULL
  retry_after <- job$retry_after
  repeat {
    if (!is.null(cancel) && isTRUE(cancel())) {
      try(fabric_job_cancel(context$job), silent = TRUE)
      rlang::abort(
        "Fabric job polling was cancelled by the caller",
        class = c("fabric_job_cancelled_by_caller", "fabric_job_error"),
        job = job,
        last_status = last
      )
    }
    elapsed <- as.numeric(difftime(.now(), started, units = "secs"))
    if (elapsed >= timeout) {
      if (isTRUE(cancel_on_timeout)) {
        try(fabric_job_cancel(context$job), silent = TRUE)
      }
      last_detail <- if (!is.null(last)) {
        paste0(
          " (last status: ",
          last$status,
          if (isFALSE(last$visible)) ", not visible in workload API" else "",
          ")"
        )
      } else {
        ""
      }
      rlang::abort(
        paste0(
          sprintf(
            "Timed out after %s seconds waiting for Fabric job %s",
            format(timeout, trim = TRUE),
            job$id
          ),
          last_detail
        ),
        class = c("fabric_job_timeout", "fabric_job_error"),
        job = job,
        last_status = last
      )
    }

    delay <- max(
      poll_interval %||% 0,
      retry_after %||% if (is.null(poll_interval)) 2 else 0
    )
    remaining <- timeout - elapsed
    if (delay > 0) {
      .sleep(min(delay, remaining))
    }
    if (as.numeric(difftime(.now(), started, units = "secs")) >= timeout) {
      next
    }

    last <- .fabric_job_get_status(
      context,
      allow_not_found = TRUE
    )
    retry_after <- last$retry_after
    if (!last$status %in% .fabric_job_terminal_states) {
      if (!last$status %in% .fabric_job_active_states) {
        rlang::abort(
          paste0(
            "Fabric returned an unknown job status: ",
            last$status
          ),
          class = c("fabric_job_unknown_status", "fabric_job_error"),
          job = job,
          job_status = last
        )
      }
      next
    }
    if (identical(last$status, "Completed") || !isTRUE(error_on_failure)) {
      return(last)
    }
    .fabric_job_abort_terminal(last, job)
  }
}

#' @rdname fabric_job_run
#' @export
fabric_job_cancel <- function(
  job = NULL,
  workspace = NULL,
  item = NULL,
  job_instance_id = NULL,
  item_type = NULL,
  job_type = NULL,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  token = NULL,
  auth_args = list(),
  api_base = .fabric_api_base
) {
  context <- .fabric_job_context(
    job = job,
    workspace = workspace,
    item = item,
    job_instance_id = job_instance_id,
    item_type = item_type,
    job_type = job_type,
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args,
    api_base = api_base
  )
  url <- paste0(
    context$api_base,
    "/workspaces/",
    context$workspace_id,
    "/items/",
    context$item_id,
    "/jobs/instances/",
    context$id,
    "/cancel"
  )
  .fabric_job_request(
    "POST",
    url,
    context$credential,
    payload = NULL,
    idempotent = TRUE,
    parse_json = FALSE
  )
  invisible(TRUE)
}

#' @export
print.fabric_job <- function(x, ...) {
  cat(
    "<fabric_job>\n",
    "  instance:  ",
    x$id,
    "\n",
    "  item:      ",
    x$item_id,
    " (",
    x$item_type %||% "unknown",
    ")\n",
    "  job type:  ",
    x$job_type,
    "\n",
    "  workspace: ",
    x$workspace_id,
    "\n",
    sep = ""
  )
  invisible(x)
}

#' @export
print.fabric_job_instance <- function(x, ...) {
  cat(
    "<fabric_job_instance>\n",
    "  instance: ",
    x$id,
    "\n",
    "  status:   ",
    x$status,
    "\n",
    if (!is.null(x$root_activity_id)) {
      paste0("  activity: ", x$root_activity_id, "\n")
    } else {
      ""
    },
    sep = ""
  )
  invisible(x)
}

.fabric_job_request <- function(
  method,
  url,
  credential,
  payload = NULL,
  idempotent = NULL,
  parse_json = TRUE,
  accepted_status = integer()
) {
  request <- httr2::request(url)
  request <- httr2::req_method(request, method)
  if (!is.null(payload)) {
    request <- httr2::req_body_json(
      request,
      payload,
      auto_unbox = TRUE,
      null = "null"
    )
  } else if (toupper(method) %in% c("POST", "PUT", "PATCH")) {
    request <- httr2::req_body_raw(request, raw())
  }
  response <- .httr2_perform(
    request,
    credential = credential,
    audience = .fabric_audience$fabric,
    idempotent = idempotent,
    accepted_status = accepted_status
  )
  status <- httr2::resp_status(response)
  list(
    status_code = status,
    location = httr2::resp_header(response, "location"),
    retry_after = .httr2_retry_after(response),
    body = if (isTRUE(parse_json) && status != 204L) {
      out <- httr2::resp_body_string(response)
      if (nzchar(out)) {
        httr2::resp_body_json(response, simplifyVector = FALSE)
      } else {
        list()
      }
    } else {
      NULL
    }
  )
}

.fabric_job_target <- function(
  item,
  workspace,
  item_type,
  credential,
  api_base
) {
  item_record <- fabric_as_record(item)
  workspace_record <- fabric_as_record(workspace)
  item_workspace_id <- fabric_record_value(
    item_record %||% list(),
    "workspaceId",
    "workspace_id"
  )
  if (!is.null(workspace)) {
    workspace_id <- fabric_record_value(
      workspace_record %||% list(),
      "id",
      "workspaceId",
      "workspace_id"
    )
    if (
      is.null(workspace_id) &&
        is.character(workspace) &&
        length(workspace) == 1L &&
        fabric_is_guid(workspace)
    ) {
      workspace_id <- workspace
    } else {
      workspace_id <- fabric_resolve_workspace(
        workspace,
        credential,
        api_base
      )$id
    }
    fabric_validate_item_workspace(item_record %||% list(), workspace_id)
  } else {
    workspace_id <- item_workspace_id
    if (is.null(workspace_id)) {
      rlang::abort(
        "`workspace` is required unless `item` contains `workspaceId`"
      )
    }
  }
  .fabric_job_nonempty(workspace_id, "workspace ID")
  .fabric_job_guid(workspace_id, "workspace ID")

  if (!is.null(item_record)) {
    item_id <- fabric_record_value(item_record, "id")
    resolved_type <- item_type %||% fabric_record_value(item_record, "type")
  } else {
    .fabric_job_nonempty(item, "item")
    if (fabric_is_guid(item)) {
      item_id <- item
      resolved_type <- item_type
    } else {
      query <- httr2::request(
        paste0(api_base, "/workspaces/", workspace_id, "/items")
      )
      if (!is.null(item_type)) {
        query <- httr2::req_url_query(query, type = item_type)
      }
      records <- .httr2_collection(
        query$url,
        credential = credential,
        audience = .fabric_audience$fabric
      )
      found <- fabric_unique_name(records, item, "item")
      item_id <- found$id
      resolved_type <- item_type %||% found$type
    }
  }
  .fabric_job_nonempty(item_id, "item ID")
  .fabric_job_guid(item_id, "item ID")
  if (!is.null(resolved_type)) {
    .fabric_job_nonempty(resolved_type, "item_type")
  }
  list(
    workspace_id = workspace_id,
    item_id = item_id,
    item_type = resolved_type
  )
}

.fabric_job_route <- function(item_type, job_type) {
  normalized <- gsub(
    "[^a-z0-9]",
    "",
    tolower(item_type %||% "")
  )
  if (identical(normalized, "notebook")) {
    expected <- "RunNotebook"
    if (
      !is.null(job_type) &&
        !tolower(job_type) %in%
          c("runnotebook", "execute")
    ) {
      rlang::abort(
        "Notebook jobs use job_type = \"RunNotebook\""
      )
    }
    return(list(route = "notebook", job_type = expected))
  }
  if (identical(normalized, "sparkjobdefinition")) {
    if (!is.null(job_type) && !identical(tolower(job_type), "sparkjob")) {
      rlang::abort(
        "SparkJobDefinition jobs use job_type = \"SparkJob\""
      )
    }
    return(list(route = "spark_job_definition", job_type = "SparkJob"))
  }
  if (normalized %in% c("datapipeline", "pipeline")) {
    if (!is.null(job_type) && !identical(tolower(job_type), "pipeline")) {
      rlang::abort(
        "DataPipeline jobs use job_type = \"Pipeline\""
      )
    }
    expected <- "Pipeline"
  } else {
    expected <- job_type
  }
  if (is.null(expected)) {
    rlang::abort(
      "`job_type` is required for item types without a known default"
    )
  }
  .fabric_job_path_segment(expected, "job_type")
  list(route = "core", job_type = expected)
}

.fabric_job_run_url <- function(api_base, target, route) {
  prefix <- paste0(api_base, "/workspaces/", target$workspace_id)
  switch(
    route$route,
    notebook = paste0(
      prefix,
      "/notebooks/",
      target$item_id,
      "/jobs/execute/instances?beta=false"
    ),
    spark_job_definition = paste0(
      prefix,
      "/sparkJobDefinitions/",
      target$item_id,
      "/jobs/sparkjob/instances"
    ),
    core = paste0(
      prefix,
      "/items/",
      target$item_id,
      "/jobs/",
      route$job_type,
      "/instances"
    )
  )
}

.fabric_job_status_url <- function(context) {
  if (identical(context$route, "notebook")) {
    paste0(
      context$api_base,
      "/workspaces/",
      context$workspace_id,
      "/notebooks/",
      context$item_id,
      "/jobs/execute/instances/",
      context$id,
      "?beta=true"
    )
  } else {
    paste0(
      context$api_base,
      "/workspaces/",
      context$workspace_id,
      "/items/",
      context$item_id,
      "/jobs/instances/",
      context$id
    )
  }
}

.fabric_job_core_status_url <- function(context) {
  paste0(
    context$api_base,
    "/workspaces/",
    context$workspace_id,
    "/items/",
    context$item_id,
    "/jobs/instances/",
    context$id
  )
}

.fabric_job_get_status <- function(context, allow_not_found) {
  url <- .fabric_job_status_url(context)
  notebook <- identical(context$route, "notebook")
  result <- .fabric_job_request(
    "GET",
    url,
    context$credential,
    idempotent = TRUE,
    accepted_status = if (notebook) {
      c(400L, 404L, 410L)
    } else if (isTRUE(allow_not_found)) {
      404L
    } else {
      integer()
    }
  )
  if (notebook && result$status_code %in% c(400L, 404L, 410L)) {
    result <- .fabric_job_request(
      "GET",
      .fabric_job_core_status_url(context),
      context$credential,
      idempotent = TRUE,
      accepted_status = if (isTRUE(allow_not_found)) 404L else integer()
    )
  }
  if (identical(result$status_code, 404L)) {
    return(.fabric_job_instance(
      list(id = context$id, status = "NotStarted"),
      context,
      result$retry_after,
      visible = FALSE
    ))
  }
  instance <- .fabric_job_instance(
    result$body,
    context,
    result$retry_after,
    visible = TRUE
  )
  if (
    notebook &&
      identical(instance$status, "Completed") &&
      is.null(instance$exit_value) &&
      !nzchar(.fabric_job_failure_text(instance$failure_reason))
  ) {
    core_result <- .fabric_job_request(
      "GET",
      .fabric_job_core_status_url(context),
      context$credential,
      idempotent = TRUE,
      accepted_status = 404L
    )
    if (!identical(core_result$status_code, 404L)) {
      core_instance <- .fabric_job_instance(
        core_result$body,
        context,
        core_result$retry_after,
        visible = TRUE
      )
      if (!identical(core_instance$status, "Completed")) {
        return(core_instance)
      }
    }
  }
  instance
}

.fabric_job_execution_data <- function(
  target,
  route,
  execution_data,
  default_lakehouse,
  default_lakehouse_workspace,
  compute,
  session_tag
) {
  if (
    !is.null(execution_data) &&
      any(vapply(
        list(
          default_lakehouse,
          default_lakehouse_workspace,
          compute,
          session_tag
        ),
        Negate(is.null),
        logical(1)
      ))
  ) {
    rlang::abort(
      "Supply either `execution_data` or the notebook convenience arguments"
    )
  }
  if (!identical(route$route, "notebook")) {
    if (
      any(vapply(
        list(
          default_lakehouse,
          default_lakehouse_workspace,
          compute,
          session_tag
        ),
        Negate(is.null),
        logical(1)
      ))
    ) {
      rlang::abort(
        "Notebook compute arguments can only be used with Notebook items"
      )
    }
    if (!is.null(execution_data)) {
      if (identical(route$route, "core")) {
        rlang::abort(paste0(
          "`execution_data` is not supported for generic item jobs; use the ",
          "typed Notebook or SparkJobDefinition route"
        ))
      }
      .fabric_job_named_list(execution_data, "execution_data")
      .fabric_job_validate_spark_definition(execution_data)
    }
    return(execution_data)
  }

  if (
    !is.null(default_lakehouse_workspace) &&
      is.null(default_lakehouse)
  ) {
    rlang::abort(
      "`default_lakehouse_workspace` requires `default_lakehouse`"
    )
  }
  if (
    is.null(execution_data) &&
      all(vapply(
        list(
          default_lakehouse,
          default_lakehouse_workspace,
          compute,
          session_tag
        ),
        is.null,
        logical(1)
      ))
  ) {
    return(NULL)
  }
  if (is.null(execution_data)) {
    compute <- compute %||% "Spark"
    configuration <- list()
    if (!is.null(default_lakehouse)) {
      lakehouse_record <- fabric_as_record(default_lakehouse)
      lakehouse_id <- fabric_record_value(
        lakehouse_record %||% list(),
        "id"
      ) %||%
        default_lakehouse
      lakehouse_workspace <- fabric_record_value(
        lakehouse_record %||% list(),
        "workspaceId",
        "workspace_id"
      )
      supplied_workspace <- fabric_as_record(default_lakehouse_workspace)
      lakehouse_workspace <- lakehouse_workspace %||%
        fabric_record_value(supplied_workspace %||% list(), "id") %||%
        default_lakehouse_workspace %||%
        target$workspace_id
      .fabric_job_nonempty(lakehouse_id, "default_lakehouse")
      .fabric_job_nonempty(
        lakehouse_workspace,
        "default_lakehouse_workspace"
      )
      .fabric_job_guid(lakehouse_id, "default_lakehouse")
      .fabric_job_guid(
        lakehouse_workspace,
        "default_lakehouse_workspace"
      )
      configuration$defaultLakehouse <- list(
        referenceType = "ById",
        itemId = lakehouse_id,
        workspaceId = lakehouse_workspace
      )
    }
    if (!is.null(session_tag)) {
      .fabric_job_validate_session_tag(session_tag, "session_tag")
      configuration$highConcurrencyModeOptions <- list(
        enabled = TRUE,
        sessionTag = session_tag
      )
    }
    execution_data <- list(compute = compute)
    if (length(configuration)) {
      execution_data$computeConfiguration <- configuration
    }
  }
  .fabric_job_named_list(execution_data, "execution_data")
  allowed <- c("compute", "computeConfiguration")
  unknown <- setdiff(names(execution_data), allowed)
  if (length(unknown)) {
    rlang::abort(paste0(
      "Unsupported notebook execution_data field(s): ",
      paste(unknown, collapse = ", ")
    ))
  }
  compute <- execution_data$compute %||% "Spark"
  allowed_compute <- c("Spark", "Jupyter", "DataWarehouse")
  match <- match(tolower(compute), tolower(allowed_compute))
  if (is.na(match)) {
    rlang::abort(
      "Notebook `compute` must be Spark, Jupyter, or DataWarehouse"
    )
  }
  execution_data$compute <- allowed_compute[[match]]
  if (!is.null(execution_data$computeConfiguration)) {
    .fabric_job_named_list(
      execution_data$computeConfiguration,
      "execution_data$computeConfiguration"
    )
    .fabric_job_validate_notebook_compute(
      execution_data$computeConfiguration,
      execution_data$compute
    )
  }
  if (
    identical(execution_data$compute, "DataWarehouse") &&
      !is.null(execution_data$computeConfiguration)
  ) {
    rlang::abort(
      "DataWarehouse notebooks do not support `computeConfiguration`"
    )
  }
  execution_data
}

.fabric_job_validate_notebook_compute <- function(configuration, compute) {
  common <- c(
    "name",
    "mountPoints",
    "defaultLakehouse",
    "attachedEnvironment"
  )
  allowed <- if (identical(compute, "Spark")) {
    c(
      common,
      "driverMemory",
      "driverCores",
      "executorMemory",
      "executorCores",
      "numExecutors",
      "jars",
      "pyFiles",
      "files",
      "archives",
      "sparkProperties",
      "instancePool",
      "highConcurrencyModeOptions"
    )
  } else if (identical(compute, "Jupyter")) {
    c(common, "numCores")
  } else {
    character()
  }
  unknown <- setdiff(names(configuration), allowed)
  if (length(unknown)) {
    rlang::abort(paste0(
      "Unsupported ",
      compute,
      " notebook compute configuration field(s): ",
      paste(unknown, collapse = ", ")
    ))
  }
  for (name in intersect(
    c("defaultLakehouse", "attachedEnvironment"),
    names(configuration)
  )) {
    .fabric_job_validate_item_reference(
      configuration[[name]],
      paste0("computeConfiguration$", name)
    )
  }
  if ("highConcurrencyModeOptions" %in% names(configuration)) {
    options <- configuration$highConcurrencyModeOptions
    .fabric_job_named_list(options, "highConcurrencyModeOptions")
    if (length(setdiff(names(options), c("enabled", "sessionTag")))) {
      rlang::abort(paste0(
        "`highConcurrencyModeOptions` only supports `enabled` and ",
        "`sessionTag`"
      ))
    }
    if (
      !is.null(options$enabled) &&
        (!is.logical(options$enabled) ||
          length(options$enabled) != 1L ||
          is.na(options$enabled))
    ) {
      rlang::abort("`highConcurrencyModeOptions$enabled` must be logical")
    }
    if (!is.null(options$sessionTag)) {
      .fabric_job_validate_session_tag(
        options$sessionTag,
        "highConcurrencyModeOptions$sessionTag"
      )
    }
  }
  invisible(TRUE)
}

.fabric_job_validate_session_tag <- function(value, name) {
  .fabric_job_nonempty(value, name)
  if (!grepl("^[A-Za-z0-9_]+$", value)) {
    rlang::abort(sprintf(
      "`%s` can only contain letters, numbers, and underscores",
      name
    ))
  }
  invisible(TRUE)
}

.fabric_job_validate_spark_definition <- function(execution_data) {
  allowed <- c(
    "executableFile",
    "mainClass",
    "commandLineArguments",
    "additionalLibraryUris",
    "defaultLakehouseId",
    "environmentId"
  )
  unknown <- setdiff(names(execution_data), allowed)
  if (length(unknown)) {
    rlang::abort(paste0(
      "Unsupported SparkJobDefinition execution_data field(s): ",
      paste(unknown, collapse = ", ")
    ))
  }
  for (name in intersect(
    c("executableFile", "mainClass", "commandLineArguments"),
    names(execution_data)
  )) {
    .fabric_job_nonempty(execution_data[[name]], name)
  }
  if (
    !is.null(execution_data$additionalLibraryUris) &&
      (!is.character(execution_data$additionalLibraryUris) ||
        !length(execution_data$additionalLibraryUris) ||
        anyNA(execution_data$additionalLibraryUris) ||
        !all(nzchar(execution_data$additionalLibraryUris)))
  ) {
    rlang::abort(
      "`additionalLibraryUris` must be a non-empty character vector"
    )
  }
  for (name in intersect(
    c("defaultLakehouseId", "environmentId"),
    names(execution_data)
  )) {
    .fabric_job_validate_item_reference(execution_data[[name]], name)
  }
  invisible(TRUE)
}

.fabric_job_validate_item_reference <- function(reference, name) {
  .fabric_job_named_list(reference, name)
  required <- c("referenceType", "itemId", "workspaceId")
  if (!setequal(names(reference), required)) {
    rlang::abort(sprintf(
      "`%s` must contain only referenceType, itemId, and workspaceId",
      name
    ))
  }
  if (!identical(reference$referenceType, "ById")) {
    rlang::abort(sprintf("`%s$referenceType` must be \"ById\"", name))
  }
  .fabric_job_nonempty(reference$itemId, paste0(name, "$itemId"))
  .fabric_job_nonempty(
    reference$workspaceId,
    paste0(name, "$workspaceId")
  )
  .fabric_job_guid(reference$itemId, paste0(name, "$itemId"))
  .fabric_job_guid(
    reference$workspaceId,
    paste0(name, "$workspaceId")
  )
  invisible(TRUE)
}

.fabric_job_parameters <- function(parameters, parameter_types = NULL) {
  if (is.null(parameters)) {
    if (!is.null(parameter_types) && length(parameter_types)) {
      rlang::abort("`parameter_types` requires `parameters`")
    }
    return(list())
  }
  if (!is.list(parameters)) {
    rlang::abort("`parameters` must be a named list or list of records")
  }
  record_form <- length(parameters) > 0L &&
    all(vapply(
      parameters,
      function(value) {
        is.list(value) &&
          all(c("name", "value", "type") %in% names(value))
      },
      logical(1)
    ))
  if (record_form && !is.null(parameter_types)) {
    rlang::abort(
      "`parameter_types` cannot be combined with parameter records"
    )
  }

  if (record_form) {
    records <- lapply(parameters, function(record) {
      if (!setequal(names(record), c("name", "value", "type"))) {
        rlang::abort(
          "Parameter records must contain only `name`, `value`, and `type`"
        )
      }
      .fabric_job_parameter(
        record$name,
        record$value,
        record$type
      )
    })
  } else {
    parameter_names <- names(parameters)
    if (is.null(parameter_names) || !all(nzchar(parameter_names))) {
      rlang::abort("Scalar `parameters` must have non-empty names")
    }
    if (!is.null(parameter_types)) {
      if (
        !is.character(parameter_types) ||
          is.null(names(parameter_types)) ||
          !all(nzchar(names(parameter_types)))
      ) {
        rlang::abort(
          "`parameter_types` must be a named character vector"
        )
      }
      if (anyDuplicated(tolower(names(parameter_types)))) {
        rlang::abort(
          "`parameter_types` names must be unique ignoring case"
        )
      }
      unknown <- setdiff(
        tolower(names(parameter_types)),
        tolower(parameter_names)
      )
      if (length(unknown)) {
        rlang::abort("`parameter_types` contains an unknown parameter name")
      }
    }
    records <- Map(
      function(name, value) {
        type_index <- if (is.null(parameter_types)) {
          integer()
        } else {
          match(tolower(name), tolower(names(parameter_types)))
        }
        type <- if (length(type_index) && !is.na(type_index)) {
          unname(parameter_types[[type_index]])
        } else {
          .fabric_job_infer_parameter_type(value)
        }
        .fabric_job_parameter(name, value, type)
      },
      parameter_names,
      parameters
    )
  }
  record_names <- vapply(records, `[[`, character(1), "name")
  if (anyDuplicated(tolower(record_names))) {
    rlang::abort(
      "Fabric parameter names must be unique ignoring case"
    )
  }
  unname(records)
}

.fabric_job_parameter <- function(name, value, type) {
  .fabric_job_nonempty(name, "parameter name")
  if (nchar(name) > 256L) {
    rlang::abort("Fabric parameter names cannot exceed 256 characters")
  }
  if (!is.character(type) || length(type) != 1L || is.na(type)) {
    rlang::abort("Every Fabric parameter must have one supported type")
  }
  type_index <- match(tolower(type), tolower(.fabric_job_parameter_types))
  if (is.na(type_index)) {
    rlang::abort(paste0(
      "Unsupported Fabric parameter type `",
      type,
      "`"
    ))
  }
  type <- .fabric_job_parameter_types[[type_index]]
  if (
    length(value) != 1L ||
      (is.list(value) && !inherits(value, "POSIXlt")) ||
      is.na(value)
  ) {
    rlang::abort(
      sprintf("Fabric parameter `%s` must be one non-missing scalar", name)
    )
  }
  if (inherits(value, c("POSIXct", "POSIXlt"))) {
    value <- format(
      as.POSIXct(value, tz = "UTC"),
      "%Y-%m-%dT%H:%M:%SZ",
      tz = "UTC"
    )
  } else if (inherits(value, "Date")) {
    value <- paste0(format(value, "%Y-%m-%d"), "T00:00:00Z")
  } else if (identical(type, "DateTime")) {
    if (
      !is.character(value) ||
        !grepl(
          "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z$",
          value
        )
    ) {
      rlang::abort(sprintf(
        "DateTime parameter `%s` must use YYYY-MM-DDTHH:mm:ssZ",
        name
      ))
    }
  } else if (identical(type, "Integer")) {
    if (
      !is.numeric(value) ||
        value != trunc(value) ||
        value < -.Machine$integer.max - 1 ||
        value > .Machine$integer.max
    ) {
      rlang::abort(sprintf(
        "Integer parameter `%s` must be a whole 32-bit number",
        name
      ))
    }
    value <- as.integer(value)
  } else if (identical(type, "Number")) {
    if (!is.numeric(value) || !is.finite(value)) {
      rlang::abort(sprintf("Number parameter `%s` must be numeric", name))
    }
    value <- as.numeric(value)
  } else if (identical(type, "Boolean")) {
    if (!is.logical(value)) {
      rlang::abort(sprintf("Boolean parameter `%s` must be logical", name))
    }
  } else if (identical(type, "Guid")) {
    if (!is.character(value) || !fabric_is_guid(value)) {
      rlang::abort(sprintf(
        "Guid parameter `%s` must use the canonical GUID format",
        name
      ))
    }
  } else if (type %in% c("Text", "VariableReference")) {
    if (!is.character(value)) {
      rlang::abort(sprintf(
        "%s parameter `%s` must be character",
        type,
        name
      ))
    }
  }
  list(name = name, value = value, type = type)
}

.fabric_job_infer_parameter_type <- function(value) {
  if (inherits(value, c("POSIXt", "Date"))) {
    "DateTime"
  } else if (is.logical(value)) {
    "Boolean"
  } else if (is.integer(value)) {
    "Integer"
  } else if (is.numeric(value)) {
    "Number"
  } else if (is.character(value)) {
    "Text"
  } else {
    rlang::abort(paste0(
      "Fabric parameter types can only be inferred from scalar logical, ",
      "integer, numeric, character, Date, or POSIXt values"
    ))
  }
}

.fabric_job_context <- function(
  job,
  workspace = NULL,
  item = NULL,
  job_instance_id = NULL,
  item_type = NULL,
  job_type = NULL,
  tenant_id = NULL,
  client_id = NULL,
  token = NULL,
  auth_args = list(),
  api_base = .fabric_api_base
) {
  if (inherits(job, "fabric_job")) {
    if (!is.null(job_instance_id)) {
      rlang::abort(
        "`job_instance_id` cannot be combined with a `fabric_job`"
      )
    }
    override_auth <- !is.null(token) ||
      length(auth_args) > 0L
    credential <- if (override_auth) {
      fabric_credential(
        tenant_id = tenant_id,
        client_id = client_id,
        token = token,
        auth_args = auth_args
      )
    } else {
      job$credential
    }
    handle <- job
    handle$credential <- credential
    context <- unclass(handle)
    context$job <- handle
    context$credential <- credential
    context$api_base <- fabric_api_base(job$api_base %||% api_base)
    return(context)
  }

  id <- job_instance_id %||% job
  .fabric_job_nonempty(id, "job instance ID")
  .fabric_job_guid(id, "job instance ID")
  if (is.null(item)) {
    rlang::abort(
      "`item` is required with a job instance ID"
    )
  }
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
    base
  )
  route <- .fabric_job_route(target$item_type, job_type)
  handle <- structure(
    list(
      id = id,
      workspace_id = target$workspace_id,
      item_id = target$item_id,
      item_type = target$item_type,
      job_type = route$job_type,
      api_base = base,
      route = route$route,
      credential = credential
    ),
    class = "fabric_job"
  )
  context <- unclass(handle)
  context$job <- handle
  context$credential <- credential
  context
}

.fabric_job_instance <- function(body, context, retry_after, visible) {
  status <- body$status %||% "Unknown"
  if (tolower(status) == "canceled") {
    status <- "Cancelled"
  }
  failure_reason <- body$failureReason
  # Failure details are authoritative when the service fields disagree.
  if (
    identical(status, "Completed") &&
      nzchar(.fabric_job_failure_text(failure_reason))
  ) {
    status <- "Failed"
  }
  exit_value <- body$exitValue %||%
    body$properties$exitValue
  structure(
    list(
      id = body$id %||% context$id,
      item_id = body$itemId %||% context$item_id,
      job_type = body$jobType %||% context$job_type,
      invoke_type = body$invokeType,
      status = status,
      root_activity_id = body$rootActivityId,
      start_time = .fabric_job_time(body$startTimeUtc %||% body$startTime),
      end_time = .fabric_job_time(body$endTimeUtc %||% body$endTime),
      failure_reason = failure_reason,
      exit_value = exit_value,
      properties = body$properties,
      retry_after = retry_after,
      visible = visible,
      raw = body,
      job = context$job
    ),
    class = "fabric_job_instance"
  )
}

.fabric_job_abort_terminal <- function(instance, job) {
  status <- tolower(instance$status)
  class <- switch(
    status,
    failed = "fabric_job_failed",
    cancelled = "fabric_job_cancelled",
    deduped = "fabric_job_deduped",
    "fabric_job_error"
  )
  detail <- .fabric_job_failure_text(instance$failure_reason)
  rlang::abort(
    paste0(
      "Fabric job ",
      instance$id,
      " ended in ",
      instance$status,
      if (!is.null(instance$root_activity_id)) {
        paste0(" (root activity ", instance$root_activity_id, ")")
      } else {
        ""
      },
      if (nzchar(detail)) paste0(": ", detail) else ""
    ),
    class = c(class, "fabric_job_error"),
    job = job,
    job_status = instance
  )
}

.fabric_job_failure_text <- function(value) {
  if (is.null(value)) {
    return("")
  }
  values <- unlist(value, recursive = TRUE, use.names = FALSE)
  paste(unique(as.character(values)), collapse = ": ")
}

.fabric_job_time <- function(value) {
  if (is.null(value) || !length(value) || is.na(value[[1L]])) {
    return(NULL)
  }
  text <- value[[1L]]
  if (grepl("(Z|[+-]\\d{2}:?\\d{2})$", text)) {
    text <- sub("Z$", "+0000", text)
    text <- sub("([+-]\\d{2}):(\\d{2})$", "\\1\\2", text)
    format <- "%Y-%m-%dT%H:%M:%OS%z"
  } else {
    format <- "%Y-%m-%dT%H:%M:%OS"
  }
  as.POSIXct(text, format = format, tz = "UTC")
}

.fabric_job_id_from_location <- function(location) {
  path <- sub("[?#].*$", "", location)
  sub(".*/", "", sub("/+$", "", path))
}

.fabric_job_named_list <- function(value, name) {
  if (
    !is.list(value) ||
      (!length(value) && !identical(value, list())) ||
      (length(value) && (is.null(names(value)) || !all(nzchar(names(value)))))
  ) {
    rlang::abort(sprintf("`%s` must be a named list", name))
  }
  invisible(TRUE)
}

.fabric_job_nonempty <- function(value, name) {
  if (
    !is.character(value) ||
      length(value) != 1L ||
      is.na(value) ||
      !nzchar(value)
  ) {
    rlang::abort(sprintf("`%s` must be one non-empty string", name))
  }
  invisible(TRUE)
}

.fabric_job_path_segment <- function(value, name) {
  .fabric_job_nonempty(value, name)
  if (!grepl("^[A-Za-z0-9._-]+$", value)) {
    rlang::abort(sprintf(
      "`%s` may contain only letters, numbers, dot, underscore, and hyphen",
      name
    ))
  }
  invisible(TRUE)
}

.fabric_job_guid <- function(value, name) {
  .fabric_job_nonempty(value, name)
  if (!fabric_is_guid(value)) {
    rlang::abort(sprintf(
      "`%s` must use the canonical GUID format",
      name
    ))
  }
  invisible(TRUE)
}

.fabric_job_scalar_number <- function(value, name, minimum) {
  if (
    !is.numeric(value) ||
      length(value) != 1L ||
      is.na(value) ||
      !is.finite(value) ||
      value < minimum
  ) {
    rlang::abort(sprintf(
      "`%s` must be one finite number greater than or equal to %s",
      name,
      minimum
    ))
  }
  invisible(TRUE)
}
