.fabric_job_terminal_states <- c(
  "Completed",
  "Failed",
  "Cancelled",
  "Deduped"
)
.fabric_job_active_states <- c("NotStarted", "InProgress")
.fabric_job_poll_floor <- 0.1

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
#' Start a Notebook, data pipeline, Spark job definition, or another supported
#' Fabric item from R. The related functions check its progress, wait for it to
#' finish, or request cancellation. Use Fabric's scheduler for recurring runs
#'
#' @param item Item GUID, exact display name, or an item object returned
#'   by a discovery function. A discovered object is recommended because it
#'   already includes the item type and workspace ID
#' @param workspace Workspace GUID, exact display name, or a discovered object
#'   Omit it when `item` is a discovered object containing `workspaceId`
#' @param job_type Fabric job type. 'fabricQueryR' uses the current typed
#'   `"Execute"` operation for data pipelines and knows the usual values for
#'   notebooks and Spark job definitions, so normally omit this unless running
#'   another item type. Set `job_type = "Pipeline"` for a data pipeline only when
#'   explicitly using Fabric's legacy core job endpoint
#' @param item_type Optional Fabric item type when `item` is a GUID. A discovered
#'   item supplies this automatically. Examples are `"Notebook"`,
#'   `"DataPipeline"`, and `"SparkJobDefinition"`
#' @param parameters A named list of values to pass to the job, such as
#'   `list(run_date = as.Date("2026-01-31"), full_load = FALSE)`, infers types
#'   from R and is appropriate for most runs. Names must match the parameters
#'   configured in Fabric. Advanced callers can instead supply records with
#'   `name`, `value`, and `type`. The typed DataPipeline `Execute` endpoint does
#'   not accept parameters
#' @param parameter_types Optional named character vector overriding inferred
#'   parameter types. Supported values are `VariableReference`, `Integer`,
#'   `Number`, `Text`, `Boolean`, `DateTime`, `Guid`, and `Automatic`. Use this
#'   only when R's inferred type is not the type expected in Fabric
#' @param execution_data Optional advanced job settings in the format documented
#'   for the Fabric item type. Use the simpler arguments below for common
#'   notebook settings. In custom payload fields, wrap a one-element atomic
#'   vector in [I()] (or use an unnamed list) when it must remain a JSON array.
#'   The typed DataPipeline `Execute` endpoint does not accept a request body
#' @param default_lakehouse Optional Lakehouse GUID or discovered object used to
#'   set the notebook's default Lakehouse for this run. This changes the run
#'   context, not the notebook's saved default
#' @param default_lakehouse_workspace Optional workspace GUID or discovered
#'   record for `default_lakehouse`; defaults to the job workspace
#' @param compute Notebook compute kind: `"Spark"`, `"Jupyter"`, or
#'   `"DataWarehouse"`. Use `"Spark"` (the default) for Spark notebooks,
#'   `"Jupyter"` for a Jupyter runtime, and `"DataWarehouse"` for a notebook
#'   attached to Warehouse compute. It must match what the notebook code needs
#' @param session_tag Optional tag that enables Spark high-concurrency mode, so
#'   related notebook runs may reuse compute. See Details for its effect on
#'   failure reporting
#' @param tenant_id Entra tenant ID. Defaults to
#'   `FABRICQUERYR_TENANT_ID`
#' @param client_id Entra application ID. Defaults to
#'   `FABRICQUERYR_CLIENT_ID`, then the Azure CLI application ID
#' @param token Optional access token or token-provider function. Leave `NULL`
#'   to let 'fabricQueryR' use its normal sign-in flow
#'   A `fabric_job` handle reuses its stored credential unless `tenant_id`,
#'   `client_id`, `token`, or non-empty `auth_args` is supplied explicitly
#' @param auth_args Additional sign-in options passed to
#'   [AzureAuth::get_azure_token()] when no token source is supplied
#' @param api_base Fabric REST API base URL. Most users should keep the default
#'   A discovered workspace-specific endpoint is used unless this argument is
#'   supplied explicitly
#' @param .sleep,.now Internal deterministic recovery hooks.
#' @section Typical workflow:
#' Start a job with `fabric_job_run()`, then pass the returned handle to
#' `fabric_job_wait()`. The handle keeps the workspace, item, job type, and
#' sign-in context, so later calls do not need those details again
#' Parameterized Core jobs can return a collection `Location` without an
#' instance GUID. In that documented case, `fabric_job_run()` honors
#' `Retry-After` and polls recent job history for one matching manual run. If
#' the accepted instance cannot be resolved safely, it raises a
#' `fabric_job_accepted_unresolved` condition rather than implying that the run
#' request failed or replaying it
#'
#' @section High-concurrency notebooks:
#' A `session_tag` lets related notebook runs share Spark compute, but Fabric may
#' report a failed statement as a completed shared session with no exit value
#' Omit the tag when job status must reliably signal notebook failure. Otherwise,
#' have the notebook report its outcome with `notebookutils.notebook.exit()`.
#' The former `mssparkutils` namespace remains backward compatible but Microsoft
#' recommends migrating because it will be retired
#'
#' Notebook submission uses the released workload-specific route so Fabric
#' applies per-run parameters and compute settings. Status and waiting use the
#' stable Core endpoint by default. Set
#' `notebook_details = TRUE` to opt into the beta Notebook status endpoint when
#' exit values or workload-specific properties are required; the Core endpoint
#' remains its fallback.
#'
#' @section Permissions and status handling:
#' Running and cancelling need an item execute permission. Checking or waiting
#' also needs an item read permission, as does resolving a parameterized run's
#' collection `Location`. For a parameterized Notebook, 'fabricQueryR' captures
#' recent history before submission so a collection `Location` cannot be
#' confused with an earlier run. Recovery stops with an accepted-but-unresolved
#' error when multiple new runs make the identity ambiguous. 'fabricQueryR'
#' reconciles notebook status information from Fabric before returning it and
#' stops with a typed error if Fabric reports an unfamiliar state instead of
#' waiting indefinitely
#' @references
#' [Core Job Scheduler REST API](https://learn.microsoft.com/en-us/rest/api/fabric/core/job-scheduler/)
#'
#' [Run an on-demand item job](https://learn.microsoft.com/en-us/rest/api/fabric/core/job-scheduler/run-on-demand-item-job)
#'
#' [Run an on-demand notebook](https://learn.microsoft.com/en-us/rest/api/fabric/notebook/background-jobs/run-on-demand-notebook)
#'
#' [Get a Notebook job instance (beta)](https://learn.microsoft.com/en-us/rest/api/fabric/notebook/background-jobs/get-notebook-job-instance%28beta%29)
#'
#' [Manage and execute notebooks with public APIs](https://learn.microsoft.com/en-us/fabric/data-engineering/notebook-public-api)
#'
#' [Fabric job scheduler](https://learn.microsoft.com/en-us/fabric/fundamentals/job-scheduler)
#'
#' @return `fabric_job_run()` returns a `fabric_job` handle for use with the
#'   other job functions
#'   `fabric_job_status()` and `fabric_job_wait()` return a
#'   `fabric_job_instance` record with status, times, failure information, and a
#'   notebook exit value when available. `fabric_job_cancel()` invisibly returns
#'   `TRUE` after Fabric accepts or confirms the cancellation
#' @examples
#' \dontrun{
#' # Discover the workspace and Notebook that will be run
#' workspace <- fabric_workspaces()[[1L]]
#' notebook <- fabric_notebooks(workspace)[[1L]]
#'
#' # Start the discovered Notebook and keep the returned job handle
#' job <- fabric_job_run(notebook)
#'
#' # Refresh the current state without waiting for completion
#' current <- fabric_job_status(job)
#' current$status
#'
#' # Opt into beta Notebook details only when an exit value is required
#' completed <- fabric_job_wait(
#'   job,
#'   timeout = 900,
#'   notebook_details = TRUE
#' )
#' completed$status
#' completed$exit_value
#'
#' # A separate active run can be cancelled when it is no longer needed
#' job_to_cancel <- fabric_job_run(notebook)
#' fabric_job_cancel(job_to_cancel)
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
  api_base = .fabric_api_base,
  .sleep = Sys.sleep,
  .now = Sys.time
) {
  # 1 Resolve authentication and job target --------------------------------------------------------

  # Discovery records can provide both item identity and workspace-specific
  # endpoints, avoiding repeated caller arguments

  if (!is.function(.sleep) || !is.function(.now)) {
    .fabric_abort(".sleep and .now must be functions")
  }

  api_base_supplied <- !missing(api_base)
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
    use_workspace_endpoint = !api_base_supplied
  )
  base <- target$api_base
  route <- .fabric_job_route(target$item_type, job_type)

  # 2 Build the execution payload ------------------------------------------------------------------

  # Normalize workload-specific execution settings before submitting the job

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
  if (
    identical(route$route, "data_pipeline") &&
      (!is.null(execution_data) || length(parameters))
  ) {
    .fabric_abort(
      paste0(
        "Typed DataPipeline Execute jobs do not support `parameters` or ",
        "`execution_data`; omit them or explicitly select the compatible ",
        "core route with job_type = \"Pipeline\""
      )
    )
  }
  if (identical(route$route, "spark_job_definition") && length(parameters)) {
    .fabric_abort(
      "SparkJobDefinition jobs do not support `parameters`; use `execution_data`"
    )
  }
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

  recovery_baseline <- if (
    identical(route$route, "notebook") && !is.null(payload)
  ) {
    tryCatch(
      .httr2_collection(
        .fabric_job_history_url(base, target),
        credential = credential,
        audience = .fabric_audience$fabric
      ),
      error = identity
    )
  } else {
    NULL
  }

  # 3 Submit the job -------------------------------------------------------------------------------

  # Submit the job only after authentication and payload fields are ready

  url <- .fabric_job_run_url(base, target, route)
  submitted_at <- .now()
  result <- .fabric_job_request(
    "POST",
    url,
    credential,
    payload = payload,
    idempotent = FALSE
  )
  location <- result$location
  instance_id <- .fabric_job_submitted_instance_id(
    result,
    target,
    route,
    base,
    credential,
    submitted_at,
    recovery_baseline = recovery_baseline,
    retry_after = result$retry_after,
    .sleep = .sleep,
    .now = .now
  )

  # 4 Return a reusable job handle -----------------------------------------------------------------

  # Store resolved context so status, wait, and cancel calls need only this
  # handle and do not have to reconstruct authentication or routing details

  next_poll_at <- if (is.null(result$retry_after)) {
    submitted_at
  } else {
    submitted_at + result$retry_after
  }
  credential_reference <- .fabric_job_credential_reference(credential)
  structure(
    list(
      id = instance_id,
      workspace_id = target$workspace_id,
      item_id = target$item_id,
      item_type = target$item_type,
      job_type = route$job_type,
      location = location,
      retry_after = result$retry_after,
      submitted_at = submitted_at,
      next_poll_at = next_poll_at,
      api_base = base,
      route = route$route,
      credential = credential_reference$reference,
      .credential_key = credential_reference$key
    ),
    class = "fabric_job"
  )
}

# Recover the instance GUID from a normal or parameterized submission response
.fabric_job_submitted_instance_id <- function(
  result,
  target,
  route,
  api_base,
  credential,
  submitted_at,
  recovery_baseline = NULL,
  retry_after = NULL,
  .sleep = Sys.sleep,
  .now = Sys.time
) {
  location <- result$location
  location_path <- if (
    is.character(location) &&
      length(location) == 1L &&
      !is.na(location) &&
      nzchar(location)
  ) {
    sub("[?#].*$", "", location)
  } else {
    ""
  }
  location_id <- sub(".*/", "", sub("/+$", "", location_path))
  body <- result$body %||% list()
  if (!is.list(body)) {
    body <- list()
  }
  body_id <- body$id %||% body$jobInstanceId %||% body$job_instance_id
  candidates <- unique(c(location_id, body_id))
  candidates <- candidates[vapply(
    candidates,
    function(value) {
      is.character(value) &&
        length(value) == 1L &&
        !is.na(value) &&
        fabric_is_guid(value)
    },
    logical(1)
  )]
  if (length(candidates) == 1L) {
    return(candidates[[1L]])
  }
  if (length(candidates) > 1L) {
    .fabric_abort(
      "Fabric returned conflicting job instance IDs",
      class = "fabric_job_protocol_error"
    )
  }
  collection_location <- nzchar(location_path) &&
    grepl("/jobs/instances/?$", location_path, ignore.case = TRUE) &&
    grepl("[?&]jobType=", location %||% "", ignore.case = TRUE)
  if (!collection_location) {
    message <- if (!nzchar(location_path)) {
      "Fabric accepted the job but did not return a Location header or instance ID"
    } else {
      "Fabric returned a Location header without a valid job instance ID"
    }
    .fabric_abort(message, class = "fabric_job_protocol_error")
  }
  history_url <- .fabric_job_history_url(api_base, target)
  if (is.null(recovery_baseline)) {
    .fabric_job_recovery_abort(
      result,
      target,
      route,
      message = paste0(
        "Fabric accepted the job with a collection Location, but no ",
        "pre-submission history was available to identify its instance"
      )
    )
  }
  if (inherits(recovery_baseline, "error")) {
    .fabric_job_recovery_abort(
      result,
      target,
      route,
      message = paste0(
        "Fabric accepted the parameterized job, but pre-submission history ",
        "could not be read to identify its instance"
      ),
      parent = recovery_baseline
    )
  }
  baseline_ids <- unique(vapply(
    Filter(
      function(record) {
        is.list(record) &&
          is.character(record$id) &&
          length(record$id) == 1L &&
          !is.na(record$id) &&
          fabric_is_guid(record$id)
      },
      recovery_baseline
    ),
    `[[`,
    character(1),
    "id"
  ))
  retry_after <- as.numeric(retry_after %||% 0)
  if (
    length(retry_after) != 1L ||
      is.na(retry_after) ||
      !is.finite(retry_after) ||
      retry_after < 0
  ) {
    .fabric_abort(
      "Fabric returned an invalid Retry-After value",
      class = "fabric_job_protocol_error"
    )
  }
  recovery_timeout <- getOption("fabricqueryr.job.recovery_timeout", 60)
  poll_interval <- getOption("fabricqueryr.job.recovery_poll_interval", 1)
  if (
    length(recovery_timeout) != 1L ||
      is.na(recovery_timeout) ||
      !is.finite(recovery_timeout) ||
      recovery_timeout <= 0 ||
      length(poll_interval) != 1L ||
      is.na(poll_interval) ||
      !is.finite(poll_interval) ||
      poll_interval <= 0
  ) {
    .fabric_abort(
      "Job recovery timeout and poll interval options must be positive numbers"
    )
  }
  not_before <- submitted_at + retry_after
  delay <- as.numeric(difftime(not_before, .now(), units = "secs"))
  if (is.finite(delay) && delay > 0) {
    .sleep(delay)
  }
  deadline <- submitted_at + max(recovery_timeout, retry_after)
  candidate_id <- NULL
  repeat {
    records <- tryCatch(
      .httr2_collection(
        history_url,
        credential = credential,
        audience = .fabric_audience$fabric
      ),
      error = function(error) {
        .fabric_job_recovery_abort(
          result,
          target,
          route,
          message = paste0(
            "Fabric accepted the parameterized job, but its instance could ",
            "not be recovered from recent history"
          ),
          parent = error
        )
      }
    )
    now <- .now()
    matches <- Filter(
      function(record) {
        scalar_text <- function(value) {
          is.character(value) &&
            length(value) == 1L &&
            !is.na(value) &&
            nzchar(value)
        }
        if (
          !is.list(record) ||
            !scalar_text(record$id) ||
            !fabric_is_guid(record$id)
        ) {
          return(FALSE)
        }
        if (
          !is.null(record$itemId) &&
            (!scalar_text(record$itemId) ||
              !identical(tolower(record$itemId), tolower(target$item_id)))
        ) {
          return(FALSE)
        }
        if (
          !scalar_text(record$jobType) ||
            !identical(tolower(record$jobType), tolower(route$job_type))
        ) {
          return(FALSE)
        }
        if (
          !is.null(record$invokeType) &&
            (!scalar_text(record$invokeType) ||
              !identical(tolower(record$invokeType), "manual"))
        ) {
          return(FALSE)
        }
        started <- try(
          .fabric_job_time(record$startTimeUtc %||% record$startTime),
          silent = TRUE
        )
        !inherits(started, "try-error") &&
          !is.null(started) &&
          !is.na(started) &&
          as.numeric(started) >= as.numeric(submitted_at) - 300 &&
          as.numeric(started) <= as.numeric(now) + 300
      },
      records
    )
    correlation_ids <- unique(c(result$request_id, result$activity_id))
    correlation_ids <- correlation_ids[vapply(
      correlation_ids,
      function(value) {
        is.character(value) &&
          length(value) == 1L &&
          !is.na(value) &&
          fabric_is_guid(value)
      },
      logical(1)
    )]
    if (length(correlation_ids)) {
      correlated <- Filter(
        function(record) {
          root_id <- record$rootActivityId
          is.character(root_id) &&
            length(root_id) == 1L &&
            !is.na(root_id) &&
            tolower(root_id) %in% tolower(correlation_ids)
        },
        matches
      )
      correlated_ids <- unique(vapply(
        correlated,
        `[[`,
        character(1),
        "id"
      ))
      if (length(correlated_ids) == 1L) {
        return(correlated_ids[[1L]])
      }
      if (length(correlated_ids) > 1L) {
        .fabric_job_recovery_abort(
          result,
          target,
          route,
          message = paste0(
            "Fabric accepted the parameterized job, but its response ",
            "correlated with multiple recent instances"
          ),
          matching_ids = correlated_ids
        )
      }
    }
    ids <- unique(vapply(matches, `[[`, character(1), "id"))
    ids <- ids[!tolower(ids) %in% tolower(baseline_ids)]
    if (length(ids) == 1L) {
      if (
        !is.null(candidate_id) &&
          identical(tolower(candidate_id), tolower(ids[[1L]]))
      ) {
        return(ids[[1L]])
      }
      candidate_id <- ids[[1L]]
    } else {
      candidate_id <- NULL
    }
    if (length(ids) > 1L) {
      .fabric_job_recovery_abort(
        result,
        target,
        route,
        message = paste0(
          "Fabric accepted the parameterized job, but recent history ",
          "contained multiple matching instances"
        ),
        matching_ids = ids
      )
    }
    remaining <- as.numeric(difftime(deadline, now, units = "secs"))
    if (!is.finite(remaining) || remaining <= 0) {
      .fabric_job_recovery_abort(
        result,
        target,
        route,
        message = paste0(
          "Fabric accepted the parameterized job, but its instance did not ",
          "appear in recent history before the recovery deadline"
        )
      )
    }
    .sleep(min(poll_interval, remaining))
  }
}

.fabric_job_history_url <- function(api_base, target) {
  paste0(
    api_base,
    "/workspaces/",
    target$workspace_id,
    "/items/",
    target$item_id,
    "/jobs/instances"
  )
}

.fabric_job_recovery_abort <- function(
  result,
  target,
  route,
  message,
  matching_ids = character(),
  parent = NULL
) {
  .fabric_abort(
    paste0(
      message,
      "; do not resubmit automatically because that can create a duplicate run"
    ),
    class = c(
      "fabric_job_accepted_unresolved",
      "fabric_job_protocol_error"
    ),
    accepted = TRUE,
    location = result$location,
    workspace_id = target$workspace_id,
    item_id = target$item_id,
    job_type = route$job_type,
    matching_ids = matching_ids,
    parent = parent
  )
}

#' @param job A `fabric_job` returned by [fabric_job_run()] or a
#'   `fabric_job_instance` returned by a status, wait, or history function.
#'   Status and cancellation functions also accept a job instance GUID when
#'   `workspace`, `item`, and enough type information are supplied
#' @param job_instance_id Alternative argument for a job instance GUID. Do not
#'   supply it together with a `fabric_job` handle
#' @param respect_retry_after Whether to wait for Fabric's recommended first
#'   status-check time. Keep `TRUE` for normal use
#' @param notebook_details For Notebook jobs, whether to opt into the beta
#'   Notebook status endpoint for exit values and compute details. The default
#'   `FALSE` uses only the stable Core Job Scheduler status endpoint;
#'   `exit_value` and workload-specific `properties` may then be unavailable.
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
  api_base = .fabric_api_base,
  respect_retry_after = TRUE,
  notebook_details = FALSE,
  .sleep = Sys.sleep,
  .now = Sys.time
) {
  # 1 Respect the service's first polling time -----------------------------------------------------

  # Waiting here avoids an immediate status call that Fabric already asked the
  # client to postpone

  if (
    !is.logical(respect_retry_after) ||
      length(respect_retry_after) != 1L ||
      is.na(respect_retry_after)
  ) {
    .fabric_abort("`respect_retry_after` must be TRUE or FALSE")
  }
  if (
    !is.logical(notebook_details) ||
      length(notebook_details) != 1L ||
      is.na(notebook_details)
  ) {
    .fabric_abort("`notebook_details` must be TRUE or FALSE")
  }

  if (
    isTRUE(respect_retry_after) &&
      (inherits(job, "fabric_job") || inherits(job, "fabric_job_instance"))
  ) {
    next_poll_at <- job$next_poll_at
    if (
      inherits(job, "fabric_job") &&
        is.null(next_poll_at) &&
        !is.null(job$submitted_at) &&
        !is.null(job$retry_after)
    ) {
      next_poll_at <- job$submitted_at + job$retry_after
    }

    if (!is.null(next_poll_at)) {
      delay <- as.numeric(difftime(next_poll_at, .now(), units = "secs"))
      if (is.finite(delay) && delay > 0) {
        .sleep(delay)
      }
    }
  }

  # 2 Resolve the job context ----------------------------------------------------------------------

  # Resolve the job context once so later steps use one consistent value

  api_base_supplied <- !missing(api_base)
  override_auth <- !missing(tenant_id) ||
    !missing(client_id) ||
    !is.null(token) ||
    length(auth_args) > 0L
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
    api_base = api_base,
    use_workspace_endpoint = !api_base_supplied,
    override_auth = override_auth
  )

  # 3 Read and return status -----------------------------------------------------------------------

  # Read and return status once so later checks use a consistent view

  instance <- .fabric_job_get_status(
    context,
    allow_not_found = FALSE,
    notebook_details = notebook_details
  )
  observed_at <- .now()
  instance$next_poll_at <- if (is.null(instance$retry_after)) {
    observed_at
  } else {
    observed_at + instance$retry_after
  }
  instance
}

#' @param poll_interval Minimum seconds between status checks. `NULL` follows
#'   Fabric's recommendation, with a two-second fallback
#' @param timeout Maximum seconds to wait before raising a
#'   `fabric_job_timeout`
#' @param error_on_failure Whether failed, cancelled, or deduplicated jobs raise
#'   typed errors. Set to `FALSE` to inspect those terminal results directly
#' @param cancel_on_timeout Ask Fabric to cancel the job when the client-side
#'   timeout expires. `FALSE` stops waiting but leaves the Fabric job running
#' @param cancel Optional function checked between status updates. If it returns
#'   `TRUE`, 'fabricQueryR' requests cancellation. This can support an
#'   application's stop button
#' @param .sleep,.now Internal hooks for deterministic tests
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
  notebook_details = FALSE,
  .sleep = Sys.sleep,
  .now = Sys.time
) {
  # 1 Validate polling options ---------------------------------------------------------------------

  # Validate before starting the clock so caller mistakes never consume timeout

  override_auth <- !missing(tenant_id) ||
    !missing(client_id) ||
    !is.null(token) ||
    length(auth_args) > 0L
  if (
    !inherits(job, "fabric_job") &&
      !inherits(job, "fabric_job_instance")
  ) {
    .fabric_abort(
      paste0(
        "`job` must be a `fabric_job` returned by fabric_job_run() or a ",
        "`fabric_job_instance` returned by a status, wait, or history function"
      )
    )
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
    .fabric_abort("`error_on_failure` must be TRUE or FALSE")
  }

  if (
    !is.logical(cancel_on_timeout) ||
      length(cancel_on_timeout) != 1L ||
      is.na(cancel_on_timeout)
  ) {
    .fabric_abort("`cancel_on_timeout` must be TRUE or FALSE")
  }

  if (!is.null(cancel) && !is.function(cancel)) {
    .fabric_abort("`cancel` must be a function or NULL")
  }
  if (
    !is.logical(notebook_details) ||
      length(notebook_details) != 1L ||
      is.na(notebook_details)
  ) {
    .fabric_abort("`notebook_details` must be TRUE or FALSE")
  }

  # 2 Prepare the job context ----------------------------------------------------------------------

  # Prepare one absolute deadline and job context for the remaining work

  started <- .now()
  deadline <- started + timeout
  context <- .fabric_job_context(
    job = job,
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args,
    api_base = api_base,
    override_auth = override_auth
  )
  last <- NULL
  next_poll_at <- job$next_poll_at
  if (
    is.null(next_poll_at) &&
      !is.null(job$submitted_at) &&
      !is.null(job$retry_after)
  ) {
    next_poll_at <- job$submitted_at + job$retry_after
  }
  retry_after <- if (is.null(next_poll_at)) {
    job$retry_after
  } else {
    max(
      0,
      as.numeric(difftime(next_poll_at, started, units = "secs"))
    )
  }

  # 3 Poll until the job finishes ------------------------------------------------------------------

  # Each pass checks caller cancellation and timeout before sleeping or making
  # another request

  progress <- .fabric_poll_progress("Fabric job", job$id)
  abort_timeout <- function(parent = NULL) {
    cancellation <- if (isTRUE(cancel_on_timeout)) {
      .fabric_job_cancel_outcome(context, .sleep = .sleep, .now = .now)
    } else {
      list(accepted = NULL, error = NULL)
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
    .fabric_abort(
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
      last_status = last,
      cancel_accepted = cancellation$accepted,
      cancel_error = cancellation$error,
      parent = parent
    )
  }
  repeat {
    # Caller cancellation gets a best-effort service cancellation as well
    if (!is.null(cancel) && isTRUE(cancel())) {
      cancellation <- .fabric_job_cancel_outcome(
        context,
        .sleep = .sleep,
        .now = .now
      )
      .fabric_abort(
        "Fabric job polling was cancelled by the caller",
        class = c("fabric_job_cancelled_by_caller", "fabric_job_error"),
        job = job,
        last_status = last,
        cancel_accepted = cancellation$accepted,
        cancel_error = cancellation$error
      )
    }

    # Timeout errors preserve the last visible state and cancellation outcome
    elapsed <- as.numeric(difftime(.now(), started, units = "secs"))
    if (elapsed >= timeout) {
      abort_timeout()
    }

    delay <- max(
      .fabric_job_poll_floor,
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

    result <- tryCatch(
      .fabric_job_get_status(
        context,
        allow_not_found = TRUE,
        notebook_details = notebook_details,
        deadline = deadline,
        .sleep = .sleep,
        .now = .now
      ),
      error = identity
    )
    if (inherits(result, "error")) {
      if (
        inherits(result, "fabric_http_deadline_error") ||
          .now() >= deadline
      ) {
        abort_timeout(parent = result)
      }
      rlang::cnd_signal(result)
    }
    last <- result
    if (.now() >= deadline) {
      abort_timeout()
    }
    retry_after <- last$retry_after
    .fabric_poll_progress_update(progress, last$status)
    if (!last$status %in% .fabric_job_terminal_states) {
      if (!last$status %in% .fabric_job_active_states) {
        .fabric_abort(
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
      .fabric_poll_progress_done(progress)
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
  # 1 Resolve the job context ----------------------------------------------------------------------

  # A job handle already contains most context; raw IDs are normalized here too

  api_base_supplied <- !missing(api_base)
  override_auth <- !missing(tenant_id) ||
    !missing(client_id) ||
    !is.null(token) ||
    length(auth_args) > 0L
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
    api_base = api_base,
    use_workspace_endpoint = !api_base_supplied,
    override_auth = override_auth
  )

  .fabric_job_cancel_context(context)
}

# Cancel a job from an already-resolved context. Returns TRUE invisibly when
# Fabric accepted cancellation or the job is already terminal
.fabric_job_cancel_context <- function(
  context,
  deadline = NULL,
  .sleep = Sys.sleep,
  .now = Sys.time
) {
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

  # 2 Request cancellation -------------------------------------------------------------------------

  # Some non-success replies are ambiguous, so retain their response for the
  # status check below instead of raising immediately

  result <- tryCatch(
    .fabric_job_request(
      "POST",
      url,
      context$credential,
      payload = NULL,
      idempotent = FALSE,
      parse_json = TRUE,
      accepted_status = c(400L, 404L, 409L),
      deadline = deadline,
      .sleep = .sleep,
      .now = .now
    ),
    error = identity
  )

  if (!inherits(result, "error") && result$status_code < 400L) {
    return(invisible(TRUE))
  }

  # 3 Resolve ambiguous outcomes -------------------------------------------------------------------

  # Resolve ambiguous outcomes once so later steps use one consistent value

  error_code <- if (inherits(result, "error")) {
    NULL
  } else {
    .fabric_job_error_code(result$body)
  }
  ambiguous <- inherits(result, "error") ||
    identical(tolower(error_code %||% ""), "jobalreadycompleted")
  if (ambiguous) {
    status <- tryCatch(
      .fabric_job_get_status(
        context,
        allow_not_found = FALSE,
        deadline = deadline,
        .sleep = .sleep,
        .now = .now
      ),
      error = identity
    )

    if (
      inherits(status, "fabric_job_instance") &&
        status$status %in% .fabric_job_terminal_states
    ) {
      return(invisible(TRUE))
    }
  }

  # 4 Return or report the outcome -----------------------------------------------------------------

  # Return or report the outcome in the stable form expected by the caller

  if (inherits(result, "error")) {
    rlang::cnd_signal(result)
  }
  .fabric_job_abort_cancel(result, context$job)
  invisible(TRUE)
}

#' Print a submitted Fabric job
#'
#' @param x A `fabric_job` handle returned by [fabric_job_run()]
#' @param ... Reserved for the print method
#' @return `x`, invisibly
#' @export
print.fabric_job <- function(x, ...) {
  .fabric_print(
    "fabric_job",
    list(
      instance = x$id,
      item = paste0(x$item_id, " (", x$item_type %||% "unknown", ")"),
      `job type` = x$job_type,
      workspace = x$workspace_id
    )
  )
  invisible(x)
}

#' Print Fabric job status
#'
#' @param x A `fabric_job_instance` returned by [fabric_job_status()] or
#'   [fabric_job_wait()]
#' @param ... Reserved for the print method
#' @return `x`, invisibly
#' @export
print.fabric_job_instance <- function(x, ...) {
  .fabric_print(
    "fabric_job_instance",
    list(
      instance = x$id,
      status = x$status,
      activity = x$root_activity_id
    )
  )
  invisible(x)
}

# Try to cancel `job` without replacing the caller's original error. Returns
# acceptance and error fields used by cancellation and timeout conditions
.fabric_job_cancel_outcome <- function(
  context,
  .sleep = Sys.sleep,
  .now = Sys.time
) {
  tryCatch(
    {
      cleanup_timeout <- getOption("fabricqueryr.wait.cleanup_timeout", 30)
      .fabric_job_scalar_number(
        cleanup_timeout,
        "wait cleanup timeout",
        minimum = 0
      )
      .fabric_job_cancel_context(
        context,
        deadline = .now() + cleanup_timeout,
        .sleep = .sleep,
        .now = .now
      )
      list(accepted = TRUE, error = NULL)
    },
    error = function(error) list(accepted = FALSE, error = error)
  )
}

# Send one job request from `method`, `url`, and optional `body`. Returns the
# response, decoded body, and retry hint used by all public job operations
.fabric_job_request <- function(
  method,
  url,
  credential,
  payload = NULL,
  idempotent = NULL,
  parse_json = TRUE,
  accepted_status = integer(),
  deadline = NULL,
  .sleep = Sys.sleep,
  .now = Sys.time
) {
  # 1 Build the HTTP request -----------------------------------------------------------------------

  # Start with the shared request and add a body only when the method needs one

  request <- httr2::request(url)
  request <- httr2::req_method(request, method)

  if (!is.null(payload)) {
    payload <- .fabric_job_preserve_json_arrays(payload)
    request <- httr2::req_body_json(
      request,
      payload,
      auto_unbox = TRUE,
      null = "null"
    )
  } else if (toupper(method) %in% c("POST", "PUT", "PATCH")) {
    request <- httr2::req_body_raw(request, raw())
  }

  # 2 Perform the request --------------------------------------------------------------------------

  # Authentication, retries, and accepted status codes stay in one HTTP layer

  response <- .httr2_perform(
    request,
    credential = credential,
    audience = .fabric_audience$fabric,
    idempotent = idempotent,
    accepted_status = accepted_status,
    deadline = deadline,
    .sleep = .sleep,
    .now = .now
  )

  # 3 Return response details ----------------------------------------------------------------------

  # Return only the response details the job workflows need

  status <- httr2::resp_status(response)
  list(
    status_code = status,
    location = httr2::resp_header(response, "location"),
    retry_after = .httr2_retry_after(response),
    request_id = httr2::resp_header(response, "x-ms-request-id") %||%
      httr2::resp_header(response, "request-id"),
    activity_id = httr2::resp_header(response, "x-ms-activity-id") %||%
      httr2::resp_header(response, "activity-id"),
    body = if (isTRUE(parse_json) && status != 204L) {
      if (httr2::resp_has_body(response)) {
        httr2::resp_body_json(response, simplifyVector = FALSE)
      } else {
        list()
      }
    } else {
      NULL
    }
  )
}

# Preserve lists that represent JSON arrays while cleaning `value`. Returns a
# JSON-ready object used before job request bodies are encoded
.fabric_job_preserve_json_arrays <- function(value, field = NULL) {
  array_fields <- c(
    "parameters",
    "times",
    "weekdays",
    "additionalLibraryUris",
    "jars",
    "pyFiles",
    "files",
    "archives",
    "sparkProperties",
    "mountPoints"
  )

  if (!is.null(field) && field %in% array_fields) {
    return(I(value))
  }

  if (!is.list(value)) {
    return(value)
  }
  value_names <- names(value)
  for (index in seq_along(value)) {
    child_field <- if (is.null(value_names)) NULL else value_names[[index]]
    value[index] <- list(.fabric_job_preserve_json_arrays(
      value[[index]],
      field = child_field
    ))
  }
  value
}

# Read a service error code from decoded `body`. Returns text or `NULL` so
# cancellation can distinguish an already-finished job
.fabric_job_error_code <- function(body) {
  if (!is.list(body)) {
    return(NULL)
  }
  nested <- if (is.list(body$error)) body$error else list()
  code <- body$errorCode %||% nested$code %||% body$code
  if (is.character(code) && length(code) == 1L && nzchar(code)) {
    code
  } else {
    NULL
  }
}

# Turn a rejected cancellation `result` into a typed condition. This function
# does not return and attaches `job` for caller troubleshooting
.fabric_job_abort_cancel <- function(result, job) {
  error_code <- .fabric_job_error_code(result$body) %||% "unknown"
  .fabric_abort(
    paste0(
      "Fabric did not accept the job cancellation (HTTP ",
      result$status_code,
      ", error code ",
      error_code,
      ")"
    ),
    class = c("fabric_job_cancel_error", "fabric_job_error"),
    job = job,
    error_code = error_code,
    response = result$body
  )
}

# Resolve item and workspace inputs into one job target. Returns IDs, item type,
# and API base used by job submission and raw-ID status operations
.fabric_job_target <- function(
  item,
  workspace,
  item_type,
  credential,
  api_base,
  use_workspace_endpoint = TRUE
) {
  # 1 Read supplied discovery records --------------------------------------------------------------

  # Records can carry workspace identity and a private API endpoint together

  item_record <- fabric_as_record(item)
  workspace_record <- fabric_as_record(workspace)
  item_workspace_id <- fabric_record_value(
    item_record %||% list(),
    "workspaceId",
    "workspace_id"
  )
  target_api_base <- if (isTRUE(use_workspace_endpoint)) {
    fabric_workspace_api_base(item_record %||% list(), api_base)
  } else {
    api_base
  }

  # 2 Resolve the workspace ------------------------------------------------------------------------

  # Resolve the workspace once so later steps use one consistent value

  if (!is.null(workspace)) {
    if (
      is.null(workspace_record) &&
        is.character(workspace) &&
        length(workspace) == 1L &&
        fabric_is_guid(workspace)
    ) {
      workspace_id <- workspace
    } else {
      resolved_workspace <- fabric_resolve_workspace(
        workspace,
        credential,
        api_base,
        use_workspace_endpoint = use_workspace_endpoint
      )
      workspace_id <- resolved_workspace$id
      target_api_base <- resolved_workspace$api_base %||% target_api_base
    }
    fabric_validate_item_workspace(item_record %||% list(), workspace_id)
  } else {
    workspace_id <- item_workspace_id
    if (is.null(workspace_id)) {
      .fabric_abort(
        "`workspace` is required unless `item` contains `workspaceId`"
      )
    }
  }
  .fabric_job_nonempty(workspace_id, "workspace ID")
  .fabric_job_guid(workspace_id, "workspace ID")

  # 3 Resolve the item -----------------------------------------------------------------------------

  # Resolve the item once so later steps use one consistent value

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
        paste0(target_api_base, "/workspaces/", workspace_id, "/items")
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

  # 4 Return the normalized target -----------------------------------------------------------------

  # Return the normalized target in the stable form expected by the caller

  list(
    workspace_id = workspace_id,
    item_id = item_id,
    item_type = resolved_type,
    api_base = target_api_base
  )
}

# Map `item_type` and optional `job_type` to the matching Fabric route. Returns
# normalized route details used to construct request URLs and payloads
.fabric_job_route <- function(item_type, job_type) {
  # 1 Normalize the item type ----------------------------------------------------------------------

  # Remove display punctuation so discovery and caller values match consistently

  normalized <- gsub(
    "[^a-z0-9]",
    "",
    tolower(item_type %||% "")
  )

  # 2 Handle item types with known routes ----------------------------------------------------------

  # Notebooks and Spark definitions have fixed routes and job type names

  if (identical(normalized, "notebook")) {
    expected <- "RunNotebook"
    if (
      !is.null(job_type) &&
        !tolower(job_type) %in%
          c("runnotebook", "execute")
    ) {
      .fabric_abort(
        "Notebook jobs use job_type = \"RunNotebook\""
      )
    }

    return(list(route = "notebook", job_type = expected))
  }

  if (identical(normalized, "sparkjobdefinition")) {
    if (!is.null(job_type) && !identical(tolower(job_type), "sparkjob")) {
      .fabric_abort(
        "SparkJobDefinition jobs use job_type = \"SparkJob\""
      )
    }

    return(list(route = "spark_job_definition", job_type = "SparkJob"))
  }

  if (identical(normalized, "datapipeline")) {
    if (is.null(job_type) || identical(tolower(job_type), "execute")) {
      return(list(route = "data_pipeline", job_type = "Execute"))
    }
    if (!identical(tolower(job_type), "pipeline")) {
      .fabric_abort(
        paste0(
          "DataPipeline jobs use job_type = \"Execute\"; use \"Pipeline\" ",
          "only for the legacy core endpoint"
        )
      )
    }
    expected <- "Pipeline"
  } else if (identical(normalized, "pipeline")) {
    if (!is.null(job_type) && !identical(tolower(job_type), "pipeline")) {
      .fabric_abort("Pipeline jobs use job_type = \"Pipeline\"")
    }
    expected <- "Pipeline"
  } else {
    expected <- job_type
  }

  # 3 Return the general job route -----------------------------------------------------------------

  # Other Fabric item types use the core route and require a job type

  if (is.null(expected)) {
    .fabric_abort(
      "`job_type` is required for item types without a known default"
    )
  }
  .fabric_job_path_segment(expected, "job_type")
  list(route = "core", job_type = expected)
}

# Build a submission URL from `api_base`, `target`, and `route`. Returns one URL
# for `fabric_job_run()` after every path segment has been validated
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
    data_pipeline = paste0(
      prefix,
      "/dataPipelines/",
      target$item_id,
      "/jobs/execute/instances"
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

# Build the workload-specific status URL from `context`. Returns one URL used by
# the primary status request
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

# Build the core fallback status URL from `context`. Returns one URL used when a
# workload route has not made a new job visible yet
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

# Read and reconcile job status from the workload and core APIs. Returns one
# normalized job instance for public status and wait operations
.fabric_job_get_status <- function(
  context,
  allow_not_found,
  notebook_details = FALSE,
  deadline = NULL,
  .sleep = Sys.sleep,
  .now = Sys.time
) {
  # 1 Read workload status -------------------------------------------------------------------------

  # Notebook status may briefly be unavailable from its workload route

  notebook <- identical(context$route, "notebook")
  detailed_notebook <- notebook && isTRUE(notebook_details)
  url <- if (detailed_notebook) {
    .fabric_job_status_url(context)
  } else {
    .fabric_job_core_status_url(context)
  }
  result <- .fabric_job_request(
    "GET",
    url,
    context$credential,
    idempotent = TRUE,
    accepted_status = if (detailed_notebook) {
      c(400L, 404L, 410L)
    } else if (isTRUE(allow_not_found)) {
      404L
    } else {
      integer()
    },
    deadline = deadline,
    .sleep = .sleep,
    .now = .now
  )

  if (detailed_notebook && result$status_code %in% c(400L, 404L, 410L)) {
    result <- .fabric_job_request(
      "GET",
      .fabric_job_core_status_url(context),
      context$credential,
      idempotent = TRUE,
      accepted_status = if (isTRUE(allow_not_found)) 404L else integer(),
      deadline = deadline,
      .sleep = .sleep,
      .now = .now
    )
  }

  # 2 Represent a not-yet-visible job --------------------------------------------------------------

  # Return a temporary status record while the workload API catches up

  if (identical(result$status_code, 404L)) {
    error_code <- .fabric_job_error_code(result$body)
    if (!is.null(error_code)) {
      .fabric_abort(
        paste0(
          "Fabric could not retrieve the job instance (HTTP 404, error code ",
          error_code,
          ")"
        ),
        class = c("fabric_job_status_error", "fabric_job_error"),
        job = context$job,
        error_code = error_code,
        response = result$body
      )
    }

    return(.fabric_job_instance(
      list(id = context$id, status = "NotStarted"),
      context,
      result$retry_after,
      visible = FALSE
    ))
  }

  # 3 Normalize and reconcile status ---------------------------------------------------------------

  # Normalize and reconcile status so later branches do not repeat the same conversion

  instance <- .fabric_job_instance(
    result$body,
    context,
    result$retry_after,
    visible = TRUE
  )

  if (
    detailed_notebook &&
      identical(instance$status, "Completed") &&
      is.null(instance$exit_value) &&
      !nzchar(.fabric_job_failure_text(instance$failure_reason))
  ) {
    core_result <- .fabric_job_request(
      "GET",
      .fabric_job_core_status_url(context),
      context$credential,
      idempotent = TRUE,
      accepted_status = 404L,
      deadline = deadline,
      .sleep = .sleep,
      .now = .now
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

# Validate and combine workload execution inputs. Returns a JSON-ready
# `executionData` object used when submitting notebook or Spark jobs
.fabric_job_execution_data <- function(
  target,
  route,
  execution_data,
  default_lakehouse,
  default_lakehouse_workspace,
  compute,
  session_tag
) {
  # 1 Reject conflicting input forms ---------------------------------------------------------------

  # Explicit execution data cannot be mixed with notebook convenience fields

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
    .fabric_abort(
      "Supply either `execution_data` or the notebook convenience arguments"
    )
  }

  # 2 Handle non-notebook jobs ---------------------------------------------------------------------

  # Handle non-notebook jobs separately so the common path stays simple

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
      .fabric_abort(
        "Notebook compute arguments can only be used with Notebook items"
      )
    }

    if (!is.null(execution_data)) {
      .fabric_job_named_list(execution_data, "execution_data")
      if (identical(route$route, "spark_job_definition")) {
        .fabric_job_validate_spark_definition(execution_data)
      }
    }

    return(execution_data)
  }

  # 3 Build notebook convenience data --------------------------------------------------------------

  # Build notebook convenience data from the validated values required by the next step

  # A cross-workspace lakehouse reference is incomplete without its item ID
  if (
    !is.null(default_lakehouse_workspace) &&
      is.null(default_lakehouse)
  ) {
    .fabric_abort(
      "`default_lakehouse_workspace` requires `default_lakehouse`"
    )
  }

  # No convenience values means the request does not need execution data
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

    # Normalize a discovery record or raw ID into one lakehouse reference
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

      # Check both parts before adding the reference to the request
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

    # A session tag enables Fabric high-concurrency mode
    if (!is.null(session_tag)) {
      .fabric_job_nonempty(session_tag, "session_tag")
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

  # 4 Validate notebook execution data -------------------------------------------------------------

  # Check notebook execution data now so later code can rely on safe input

  .fabric_job_named_list(execution_data, "execution_data")
  allowed <- c("compute", "computeConfiguration")
  unknown <- setdiff(names(execution_data), allowed)

  if (length(unknown)) {
    .fabric_abort(paste0(
      "Unsupported notebook execution_data field(s): ",
      paste(unknown, collapse = ", ")
    ))
  }

  # Normalize compute names without making callers match service casing
  compute <- execution_data$compute %||% "Spark"
  if (
    !is.character(compute) ||
      length(compute) != 1L ||
      is.na(compute) ||
      !nzchar(compute)
  ) {
    .fabric_abort(
      "Notebook `compute` must be one non-empty string",
      class = "fabric_job_validation_error"
    )
  }

  allowed_compute <- c("Spark", "Jupyter", "DataWarehouse")
  match <- match(tolower(compute), tolower(allowed_compute))

  if (is.na(match)) {
    .fabric_abort(
      "Notebook `compute` must be Spark, Jupyter, or DataWarehouse"
    )
  }

  execution_data$compute <- allowed_compute[[match]]

  # Validate nested settings against the normalized compute type
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
    .fabric_abort(
      "DataWarehouse notebooks do not support `computeConfiguration`"
    )
  }

  execution_data
}

# Validate notebook `configuration` and `compute` options. Returns invisibly
# after guarding mutually exclusive or unsupported combinations
.fabric_job_validate_notebook_compute <- function(configuration, compute) {
  # 1 Choose fields supported by this compute type -------------------------------------------------

  # Choose fields supported by this compute type from the validated configuration

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
    .fabric_abort(paste0(
      "Unsupported ",
      compute,
      " notebook compute configuration field(s): ",
      paste(unknown, collapse = ", ")
    ))
  }

  # 2 Validate nested item references --------------------------------------------------------------

  # Check nested item references now so later code can rely on safe input

  for (name in intersect(
    c("defaultLakehouse", "attachedEnvironment"),
    names(configuration)
  )) {
    .fabric_job_validate_item_reference(
      configuration[[name]],
      paste0("computeConfiguration$", name)
    )
  }

  # Validate shared scalar and mount-point fields before compute-specific ones
  if ("name" %in% names(configuration)) {
    .fabric_job_nonempty(configuration$name, "computeConfiguration$name")
  }
  if ("mountPoints" %in% names(configuration)) {
    mount_points <- configuration$mountPoints
    if (!is.list(mount_points) || !length(mount_points)) {
      .fabric_abort("`mountPoints` must be a non-empty list of mount records")
    }
    for (mount in mount_points) {
      .fabric_job_named_list(mount, "mount point")
      if (!setequal(names(mount), c("source", "mountPointPath"))) {
        .fabric_abort(
          "Every mount point must contain only source and mountPointPath"
        )
      }
      .fabric_job_abfss(mount$source, "mount point source")
      .fabric_job_nonempty(mount$mountPointPath, "mountPointPath")
    }
  }

  if (identical(compute, "Jupyter") && "numCores" %in% names(configuration)) {
    .fabric_job_enum_integer(
      configuration$numCores,
      c(2L, 4L, 8L, 16L, 32L, 64L),
      "Jupyter numCores"
    )
  }

  if (identical(compute, "Spark")) {
    for (name in intersect(
      c("driverCores", "executorCores"),
      names(configuration)
    )) {
      .fabric_job_enum_integer(
        configuration[[name]],
        c(4L, 8L, 16L, 32L, 64L),
        paste0("Spark ", name)
      )
    }
    for (name in intersect(
      c("driverMemory", "executorMemory"),
      names(configuration)
    )) {
      value <- configuration[[name]]
      allowed_memory <- c("28g", "56g", "112g", "224g", "400g")
      if (
        !is.character(value) ||
          length(value) != 1L ||
          is.na(value) ||
          !value %in% allowed_memory
      ) {
        .fabric_abort(paste0(
          "Spark ",
          name,
          " must be one of ",
          paste(allowed_memory, collapse = ", ")
        ))
      }
    }
    if ("numExecutors" %in% names(configuration)) {
      value <- configuration$numExecutors
      if (
        !is.numeric(value) ||
          length(value) != 1L ||
          is.na(value) ||
          !is.finite(value) ||
          value < 1 ||
          value != floor(value) ||
          value > .Machine$integer.max
      ) {
        .fabric_abort("Spark numExecutors must be one positive whole number")
      }
    }
    for (name in intersect(
      c("jars", "pyFiles", "files", "archives"),
      names(configuration)
    )) {
      .fabric_job_abfss_vector(configuration[[name]], paste0("Spark ", name))
    }
    if ("sparkProperties" %in% names(configuration)) {
      properties <- configuration$sparkProperties
      if (!is.list(properties) || !length(properties)) {
        .fabric_abort("`sparkProperties` must be a non-empty list")
      }
      for (property in properties) {
        .fabric_job_named_list(property, "Spark property")
        if (!setequal(names(property), c("key", "value"))) {
          .fabric_abort("Every Spark property must contain only key and value")
        }
        .fabric_job_nonempty(property$key, "Spark property key")
        .fabric_job_nonempty(property$value, "Spark property value")
      }
    }
    if ("instancePool" %in% names(configuration)) {
      pool <- configuration$instancePool
      .fabric_job_named_list(pool, "instancePool")
      if (length(setdiff(names(pool), c("id", "name", "type")))) {
        .fabric_abort("`instancePool` only supports id, name, and type")
      }
      if (is.null(pool$id) && is.null(pool$name)) {
        .fabric_abort("`instancePool` needs an id or name")
      }
      if (!is.null(pool$id)) {
        .fabric_job_guid(pool$id, "instancePool$id")
      }
      if (!is.null(pool$name)) {
        .fabric_job_nonempty(pool$name, "instancePool$name")
      }
      if (
        is.null(pool$type) ||
          !is.character(pool$type) ||
          length(pool$type) != 1L ||
          is.na(pool$type) ||
          !tolower(pool$type) %in% c("workspace", "capacity")
      ) {
        .fabric_abort("`instancePool$type` must be Workspace or Capacity")
      }
    }
  }

  # 3 Validate high-concurrency options ------------------------------------------------------------

  # Check high-concurrency options now so later code can rely on safe input

  if ("highConcurrencyModeOptions" %in% names(configuration)) {
    options <- configuration$highConcurrencyModeOptions
    .fabric_job_named_list(options, "highConcurrencyModeOptions")
    if (length(setdiff(names(options), c("enabled", "sessionTag")))) {
      .fabric_abort(paste0(
        "`highConcurrencyModeOptions` only supports `enabled` and ",
        "`sessionTag`"
      ))
    }

    if (is.null(options$enabled)) {
      .fabric_abort("`highConcurrencyModeOptions$enabled` is required")
    }

    if (
      !is.logical(options$enabled) ||
        length(options$enabled) != 1L ||
        is.na(options$enabled)
    ) {
      .fabric_abort("`highConcurrencyModeOptions$enabled` must be logical")
    }

    if (!is.null(options$sessionTag)) {
      .fabric_job_nonempty(
        options$sessionTag,
        "highConcurrencyModeOptions$sessionTag"
      )
    }
  }
  invisible(TRUE)
}

# Validate required Spark job definition fields in `execution_data`. Returns
# invisibly before a Spark job is submitted
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
    .fabric_abort(paste0(
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

  if (!is.null(execution_data$executableFile)) {
    .fabric_job_abfss(execution_data$executableFile, "executableFile")
  }

  if (
    !is.null(execution_data$additionalLibraryUris) &&
      (!is.character(execution_data$additionalLibraryUris) ||
        !length(execution_data$additionalLibraryUris) ||
        anyNA(execution_data$additionalLibraryUris) ||
        !all(nzchar(execution_data$additionalLibraryUris)))
  ) {
    .fabric_abort(
      "`additionalLibraryUris` must be a non-empty character vector"
    )
  }
  if (!is.null(execution_data$additionalLibraryUris)) {
    .fabric_job_abfss_vector(
      execution_data$additionalLibraryUris,
      "additionalLibraryUris"
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

# Validate an item-reference object such as a default lakehouse. Returns
# invisibly before the reference is added to notebook execution data
.fabric_job_validate_item_reference <- function(reference, name) {
  .fabric_job_named_list(reference, name)
  required <- c("referenceType", "itemId", "workspaceId")
  if (!setequal(names(reference), required)) {
    .fabric_abort(sprintf(
      "`%s` must contain only referenceType, itemId, and workspaceId",
      name
    ))
  }

  if (!identical(reference$referenceType, "ById")) {
    .fabric_abort(sprintf("`%s$referenceType` must be \"ById\"", name))
  }
  .fabric_job_guid(reference$itemId, paste0(name, "$itemId"))
  .fabric_job_guid(
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

# Normalize named parameter values and optional types. Returns a list of Fabric
# parameter records used by supported job routes
.fabric_job_parameters <- function(parameters, parameter_types = NULL) {
  # 1 Identify the accepted input form -------------------------------------------------------------

  # Callers may supply ready records or a simpler named list of scalar values

  if (is.null(parameters)) {
    if (!is.null(parameter_types) && length(parameter_types)) {
      .fabric_abort("`parameter_types` requires `parameters`")
    }

    return(list())
  }

  if (!is.list(parameters)) {
    .fabric_abort("`parameters` must be a named list or list of records")
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
    .fabric_abort(
      "`parameter_types` cannot be combined with parameter records"
    )
  }

  # 2 Normalize every parameter --------------------------------------------------------------------

  # Normalize every parameter so later branches do not repeat the same conversion

  if (record_form) {
    # Record form already names each field, so normalize records one by one
    records <- lapply(parameters, function(record) {
      if (!setequal(names(record), c("name", "value", "type"))) {
        .fabric_abort(
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
    # Named scalar form may use a separate case-insensitive type map
    parameter_names <- names(parameters)
    if (is.null(parameter_names) || !all(nzchar(parameter_names))) {
      .fabric_abort("Scalar `parameters` must have non-empty names")
    }

    if (!is.null(parameter_types)) {
      if (
        !is.character(parameter_types) ||
          is.null(names(parameter_types)) ||
          !all(nzchar(names(parameter_types)))
      ) {
        .fabric_abort(
          "`parameter_types` must be a named character vector"
        )
      }

      if (anyDuplicated(tolower(names(parameter_types)))) {
        .fabric_abort(
          "`parameter_types` names must be unique ignoring case"
        )
      }
      unknown <- setdiff(
        tolower(names(parameter_types)),
        tolower(parameter_names)
      )

      if (length(unknown)) {
        .fabric_abort("`parameter_types` contains an unknown parameter name")
      }
    }

    # Infer only the types that the caller did not specify
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

  # 3 Check the final record set -------------------------------------------------------------------

  # Check the final record set now so failures are reported close to their cause

  record_names <- vapply(records, `[[`, character(1), "name")
  if (anyDuplicated(tolower(record_names))) {
    .fabric_abort(
      "Fabric parameter names must be unique ignoring case"
    )
  }
  unname(records)
}

# Convert one named `value` and optional `type` into a Fabric parameter record
# Returns JSON-ready name, value, and type fields for job submission
.fabric_job_parameter <- function(name, value, type) {
  # 1 Validate the name and type -------------------------------------------------------------------

  # Check the name and type now so later code can rely on safe input

  .fabric_job_nonempty(name, "parameter name")
  if (nchar(name) > 256L) {
    .fabric_abort("Fabric parameter names cannot exceed 256 characters")
  }

  if (!is.character(type) || length(type) != 1L || is.na(type)) {
    .fabric_abort("Every Fabric parameter must have one supported type")
  }
  type_index <- match(tolower(type), tolower(.fabric_job_parameter_types))
  if (is.na(type_index)) {
    .fabric_abort(paste0(
      "Unsupported Fabric parameter type `",
      type,
      "`"
    ))
  }
  type <- .fabric_job_parameter_types[[type_index]]

  # 2 Validate the scalar value --------------------------------------------------------------------

  # Check the scalar value now so later code can rely on safe input

  value_length <- if (inherits(value, "POSIXlt")) {
    length(as.POSIXct(value))
  } else {
    length(value)
  }

  if (
    value_length != 1L ||
      (is.list(value) && !inherits(value, "POSIXlt")) ||
      is.na(value)
  ) {
    .fabric_abort(
      sprintf("Fabric parameter `%s` must be one non-missing scalar", name)
    )
  }
  date_time_value <- inherits(value, c("POSIXct", "POSIXlt", "Date"))
  if (date_time_value && !type %in% c("DateTime", "Automatic")) {
    .fabric_abort(sprintf(
      paste0(
        "%s parameter `%s` cannot use an R Date or POSIXt value; ",
        "use type DateTime or Automatic"
      ),
      type,
      name
    ))
  }

  # 3 Convert the value for JSON -------------------------------------------------------------------

  # Convert the validated value to Fabric's JSON-ready representation

  # R date objects become the UTC text format expected by Fabric
  if (date_time_value) {
    if (inherits(value, c("POSIXct", "POSIXlt"))) {
      value <- format(
        as.POSIXct(value),
        "%Y-%m-%dT%H:%M:%SZ",
        tz = "UTC"
      )
    } else {
      value <- paste0(format(value, "%Y-%m-%d"), "T00:00:00Z")
    }
  } else if (identical(type, "DateTime")) {
    # Character timestamps must already use the service's exact shape
    if (
      !is.character(value) ||
        !grepl(
          "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z$",
          value
        )
    ) {
      .fabric_abort(sprintf(
        "DateTime parameter `%s` must use YYYY-MM-DDTHH:mm:ssZ",
        name
      ))
    }
  } else if (identical(type, "Integer")) {
    # Integers must fit the full signed 32-bit range
    if (
      !is.numeric(value) ||
        value != trunc(value) ||
        value < -.Machine$integer.max - 1 ||
        value > .Machine$integer.max
    ) {
      .fabric_abort(sprintf(
        "Integer parameter `%s` must be a whole 32-bit number",
        name
      ))
    }
    value <- if (value == -.Machine$integer.max - 1) {
      as.numeric(value)
    } else {
      as.integer(value)
    }
  } else if (identical(type, "Number")) {
    # General numbers still need to be finite JSON values
    if (!is.numeric(value) || !is.finite(value)) {
      .fabric_abort(sprintf("Number parameter `%s` must be numeric", name))
    }
    value <- as.numeric(value)
  } else if (identical(type, "Boolean")) {
    # Fabric expects a real logical value rather than truth-like text
    if (!is.logical(value)) {
      .fabric_abort(sprintf("Boolean parameter `%s` must be logical", name))
    }
  } else if (identical(type, "Guid")) {
    # GUID parameters use the same canonical format as item IDs
    if (!is.character(value) || !fabric_is_guid(value)) {
      .fabric_abort(sprintf(
        "Guid parameter `%s` must use the canonical GUID format",
        name
      ))
    }
  } else if (type %in% c("Text", "VariableReference")) {
    # Text-like parameters remain character values
    if (!is.character(value)) {
      .fabric_abort(sprintf(
        "%s parameter `%s` must be character",
        type,
        name
      ))
    }
  }

  # 4 Return the parameter record ------------------------------------------------------------------

  # Return the parameter record in the stable form expected by the caller

  list(name = name, value = value, type = type)
}

# Infer Fabric's parameter type from one R `value`. Returns a type name when the
# caller did not supply `parameter_types`
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
    .fabric_abort(paste0(
      "Fabric parameter types can only be inferred from scalar logical, ",
      "integer, numeric, character, Date, or POSIXt values"
    ))
  }
}

# Reconstruct a complete job context from a handle or raw IDs. Returns resolved
# authentication, routing, and identity used by status, wait, and cancellation
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
  api_base = .fabric_api_base,
  use_workspace_endpoint = TRUE,
  override_auth = !is.null(token) || length(auth_args) > 0L
) {
  if (inherits(job, "fabric_job_instance")) {
    if (!is.null(job_instance_id)) {
      .fabric_abort(
        "`job_instance_id` cannot be combined with a `fabric_job_instance`"
      )
    }
    if (!inherits(job$job, "fabric_job")) {
      .fabric_abort(
        "This Fabric job instance does not contain refreshable job context",
        class = c("fabric_job_context_error", "fabric_job_error")
      )
    }
    job <- job$job
  }

  # 1 Reuse a submitted job handle -----------------------------------------------------------------

  # Handles already carry resolved IDs, route, endpoint, and usually credential

  if (inherits(job, "fabric_job")) {
    if (!is.null(job_instance_id)) {
      .fabric_abort(
        "`job_instance_id` cannot be combined with a `fabric_job`"
      )
    }
    credential <- if (override_auth) {
      fabric_credential(
        tenant_id = tenant_id,
        client_id = client_id,
        token = token,
        auth_args = auth_args
      )
    } else {
      .fabric_job_credential(job)
    }
    handle <- job
    if (override_auth) {
      credential_reference <- .fabric_job_credential_reference(credential)
      handle$credential <- credential_reference$reference
      handle$.credential_key <- credential_reference$key
    }
    context <- unclass(handle)
    context$job <- handle
    context$credential <- credential
    context$api_base <- fabric_api_base(job$api_base %||% api_base)
    return(context)
  }

  # 2 Reconstruct context from raw IDs -------------------------------------------------------------

  # Rebuild stable job context when only raw identifiers are available

  id <- job_instance_id %||% job
  .fabric_job_nonempty(id, "job instance ID")
  .fabric_job_guid(id, "job instance ID")
  if (is.null(item)) {
    .fabric_abort(
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
    base,
    use_workspace_endpoint = use_workspace_endpoint
  )
  base <- target$api_base
  route <- .fabric_job_route(target$item_type, job_type)
  credential_reference <- .fabric_job_credential_reference(credential)
  handle <- structure(
    list(
      id = id,
      workspace_id = target$workspace_id,
      item_id = target$item_id,
      item_type = target$item_type,
      job_type = route$job_type,
      api_base = base,
      route = route$route,
      credential = credential_reference$reference,
      .credential_key = credential_reference$key
    ),
    class = "fabric_job"
  )

  # 3 Return the shared context --------------------------------------------------------------------

  # Return the shared context in the stable form expected by the caller

  context <- unclass(handle)
  context$job <- handle
  context$credential <- credential
  context
}

# Store a credential behind a weak reference. The key keeps the credential
# available for the lifetime of the in-process job handle, while R serialization
# deliberately does not persist the weak-reference value containing secrets
.fabric_job_credential_reference <- function(credential) {
  key <- new.env(parent = emptyenv())
  list(
    reference = rlang::new_weakref(key, credential),
    key = key
  )
}

# Resolve an in-process job credential. Direct credentials remain accepted for
# internally constructed handles and objects created by older package versions
.fabric_job_credential <- function(job) {
  stored <- job$credential
  if (inherits(stored, "fabric_credential")) {
    return(stored)
  }

  credential <- if (rlang::is_weakref(stored)) {
    rlang::wref_value(stored)
  } else {
    NULL
  }
  if (is.null(credential)) {
    .fabric_abort(
      paste0(
        "This Fabric job handle no longer has an in-process credential; ",
        "supply `token`, `tenant_id`, or other authentication arguments"
      ),
      class = c("fabric_job_credential_error", "fabric_job_error")
    )
  }
  credential
}

# Convert a decoded status `body` and request `context` into a stable job
# instance. Returns the object exposed by status and wait calls
.fabric_job_instance <- function(body, context, retry_after, visible) {
  if (!is.list(body) || (length(body) && is.null(names(body)))) {
    .fabric_abort(
      "Fabric returned a malformed job status response: expected a JSON object",
      class = "fabric_job_protocol_error"
    )
  }
  status <- body$status
  if (
    !is.character(status) ||
      length(status) != 1L ||
      is.na(status) ||
      !nzchar(status)
  ) {
    .fabric_abort(
      "Fabric returned a malformed job status response: status must be one non-empty string",
      class = "fabric_job_protocol_error"
    )
  }
  if (tolower(status) == "canceled") {
    status <- "Cancelled"
  }
  failure_reason <- body$failureReason
  # Failure details are authoritative when the service fields disagree
  if (
    identical(status, "Completed") &&
      nzchar(.fabric_job_failure_text(failure_reason))
  ) {
    status <- "Failed"
  }
  properties <- body$properties
  if (
    !is.null(properties) &&
      (!is.list(properties) ||
        (length(properties) && is.null(names(properties))))
  ) {
    .fabric_abort(
      "Fabric returned a malformed job status response: properties must be a JSON object",
      class = "fabric_job_protocol_error"
    )
  }
  exit_value <- body$exitValue %||% properties$exitValue
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
      properties = properties,
      retry_after = retry_after,
      visible = visible,
      raw = body,
      job = context$job
    ),
    class = "fabric_job_instance"
  )
}

# Raise a typed condition for a failed terminal `instance`. This function does
# not return and attaches both the status object and original `job` handle
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
  .fabric_abort(
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

# Flatten nested failure `value` into readable text. Returns one string used in
# terminal job errors
.fabric_job_failure_text <- function(value) {
  if (is.null(value)) {
    return("")
  }
  values <- unlist(value, recursive = TRUE, use.names = FALSE)
  paste(unique(as.character(values)), collapse = ": ")
}

# Parse one Fabric timestamp `value`. Returns a UTC date-time or `NA` for missing
# input while preserving fractional-second timestamps
.fabric_job_time <- function(value) {
  if (is.null(value)) {
    return(NULL)
  }
  if (
    !is.character(value) ||
      length(value) != 1L ||
      is.na(value) ||
      !nzchar(value)
  ) {
    .fabric_abort(
      "Fabric returned a malformed job timestamp",
      class = "fabric_job_protocol_error"
    )
  }
  text <- value
  if (grepl("(Z|[+-]\\d{2}:?\\d{2})$", text)) {
    text <- sub("Z$", "+0000", text)
    text <- sub("([+-]\\d{2}):(\\d{2})$", "\\1\\2", text)
    format <- "%Y-%m-%dT%H:%M:%OS%z"
  } else {
    format <- "%Y-%m-%dT%H:%M:%OS"
  }
  parsed <- as.POSIXct(text, format = format, tz = "UTC")
  if (is.na(parsed)) {
    .fabric_abort(
      paste0("Fabric returned an invalid job timestamp: ", value),
      class = "fabric_job_protocol_error"
    )
  }
  parsed
}

# Check `value` as a fully named list identified by `name`. Returns the same list
# for execution-data and parameter normalization
.fabric_job_named_list <- function(value, name) {
  value_names <- names(value)
  if (
    !is.list(value) ||
      (!length(value) && !identical(value, list())) ||
      (length(value) &&
        (is.null(value_names) ||
          anyNA(value_names) ||
          !all(nzchar(value_names)) ||
          anyDuplicated(value_names)))
  ) {
    .fabric_abort(
      sprintf("`%s` must be a list with unique, non-missing names", name),
      class = "fabric_job_validation_error"
    )
  }
  invisible(TRUE)
}

# Validate an integer against a documented Fabric enumeration
.fabric_job_enum_integer <- function(value, allowed, name) {
  if (
    !is.numeric(value) ||
      length(value) != 1L ||
      is.na(value) ||
      !is.finite(value) ||
      value != floor(value) ||
      !value %in% allowed
  ) {
    .fabric_abort(paste0(
      name,
      " must be one of ",
      paste(allowed, collapse = ", ")
    ))
  }
  invisible(value)
}

# Validate one ABFSS URI used by Fabric Spark execution settings
.fabric_job_abfss <- function(value, name) {
  .fabric_job_nonempty(value, name)
  if (!grepl("^abfss://[^/]+/.+", value, ignore.case = TRUE)) {
    .fabric_abort(paste0(name, " must be an abfss:// URI"))
  }
  invisible(value)
}

# Validate a non-empty vector of ABFSS URIs
.fabric_job_abfss_vector <- function(value, name) {
  if (
    !is.character(value) ||
      !length(value) ||
      anyNA(value) ||
      !all(nzchar(value))
  ) {
    .fabric_abort(paste0(name, " must be a non-empty character vector"))
  }
  for (entry in value) {
    .fabric_job_abfss(entry, name)
  }
  invisible(value)
}

# Check `value` as one non-empty string identified by `name`. Returns invisibly
# for shared job target and execution-data validation
.fabric_job_nonempty <- function(value, name) {
  if (
    !is.character(value) ||
      length(value) != 1L ||
      is.na(value) ||
      !nzchar(value)
  ) {
    .fabric_abort(sprintf("`%s` must be one non-empty string", name))
  }
  invisible(TRUE)
}

# Check `value` as a safe URL path segment. Returns invisibly before job route
# text is inserted into a request URL
.fabric_job_path_segment <- function(value, name) {
  .fabric_job_nonempty(value, name)
  if (value %in% c(".", "..") || !grepl("^[A-Za-z0-9._-]+$", value)) {
    .fabric_abort(sprintf(
      paste0(
        "`%s` must be one safe path segment containing only letters, ",
        "numbers, dot, underscore, and hyphen"
      ),
      name
    ))
  }
  invisible(TRUE)
}

# Check `value` as one GUID identified by `name`. Returns invisibly for job and
# item identity validation
.fabric_job_guid <- function(value, name) {
  .fabric_job_nonempty(value, name)
  if (!fabric_is_guid(value)) {
    .fabric_abort(sprintf(
      "`%s` must use the canonical GUID format",
      name
    ))
  }
  invisible(TRUE)
}

# Check numeric `value` against `minimum`. Returns invisibly for polling and
# compute options that must contain one finite number
.fabric_job_scalar_number <- function(value, name, minimum) {
  if (
    !is.numeric(value) ||
      length(value) != 1L ||
      is.na(value) ||
      !is.finite(value) ||
      value < minimum
  ) {
    .fabric_abort(sprintf(
      "`%s` must be one finite number greater than or equal to %s",
      name,
      minimum
    ))
  }
  invisible(TRUE)
}
