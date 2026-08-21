.fabric_pbi_refresh_poll_floor <- 0.1
.fabric_pbi_refresh_active_states <- c("Queued", "InProgress")
.fabric_pbi_refresh_success_states <- c("Completed", "CompletedWithWarnings")
.fabric_pbi_refresh_terminal_states <- c(
  .fabric_pbi_refresh_success_states,
  "Failed",
  "TimedOut",
  "Cancelled",
  "Disabled"
)

#' Refresh and monitor a Power BI semantic model
#'
#' Start a semantic-model refresh, inspect recent refreshes and execution
#' details, wait for completion, or cancel a refresh. The easiest target is a
#' record returned by [fabric_semantic_models()]
#'
#' @param connstr Optional semantic-model record from
#'   [fabric_semantic_models()] or [fabric_item()], or a Power BI connection
#'   string. Omit it when `dataset_id` is supplied
#' @param workspace_id Optional shared-workspace GUID. For a semantic model in
#'   My Workspace, omit this and set `my_workspace = TRUE`
#' @param dataset_id Optional semantic-model/dataset GUID
#' @param my_workspace Whether `dataset_id` belongs to the signed-in user's My
#'   Workspace. Leave `FALSE` for shared workspaces
#' @param mode Refresh request kind. `"automatic"` chooses enhanced refresh
#'   when an enhanced option is supplied and standard refresh otherwise
#'   `"standard"` supports only `notify_option`; `"enhanced"` exposes processing
#'   controls and requires Premium, PPU, Embedded, or Fabric capacity
#' @param notify_option Standard-refresh email behavior for delegated calls:
#'   `"NoNotification"`, `"MailOnFailure"`, or `"MailOnCompletion"`. When
#'   omitted, standard refresh defaults to `"MailOnFailure"`. Pass `NULL`
#'   explicitly for a service-principal standard refresh. Omit this for
#'   enhanced refreshes
#' @param type Enhanced processing type: `"Full"`, `"ClearValues"`,
#'   `"Calculate"`, `"DataOnly"`, `"Automatic"`, or `"Defragment"`
#' @param commit_mode Enhanced commit behavior. `"Transactional"` preserves the
#'   previous model if processing fails. `"PartialBatch"` commits commands
#'   separately and can leave partially refreshed or empty tables after failure
#' @param objects Optional enhanced-refresh table or partition selection. Supply
#'   table names as a character vector, or records such as
#'   `list(list(table = "Sales", partition = "2026"))`
#' @param apply_refresh_policy Whether an incremental refresh policy should be
#'   applied. `TRUE` is incompatible with `commit_mode = "PartialBatch"`
#' @param effective_date Optional date-time used instead of the current date by
#'   an incremental refresh policy. Accepts a `Date`, `POSIXt`, or ISO 8601
#'   string
#' @param max_parallelism Optional positive whole number of parallel processing
#'   threads for an enhanced refresh
#' @param retry_count Optional non-negative number of additional enhanced
#'   refresh attempts
#' @param timeout In `fabric_pbi_refresh()`, an optional `HH:MM:SS` limit for
#'   each enhanced attempt; Power BI defaults to five hours per attempt and
#'   limits all attempts to 24 hours. In
#'   `fabric_pbi_refresh_wait()`, the maximum number of seconds to wait on the
#'   client before raising a separate client-side timeout
#' @param top Maximum history entries to return. Power BI retains 20 to 60
#'   recent entries, depending on their age
#' @param refresh A `fabric_pbi_refresh` handle returned by
#'   `fabric_pbi_refresh()`, a `fabric_pbi_refresh_detail`, or a refresh GUID
#'   Raw GUIDs require the semantic-model target arguments as well
#' @param refresh_id Alternative refresh GUID. Do not combine it with a handle
#'   or GUID supplied through `refresh`
#' @param poll_interval Minimum seconds between checks. `NULL` honors the
#'   service retry hint and otherwise checks every two seconds
#' @param error_on_failure Whether failed, timed-out, cancelled, or disabled
#'   refreshes raise a typed error. Use `FALSE` to inspect the returned detail
#' @param cancel_on_timeout Whether a client-side wait timeout should request
#'   cancellation before raising its timeout error
#' @param cancel Optional function checked between status updates. If it returns
#'   `TRUE`, fabricQueryR requests cancellation and stops waiting
#' @param tenant_id Microsoft Entra tenant ID. Defaults to
#'   `FABRICQUERYR_TENANT_ID`
#' @param client_id Microsoft Entra application/client ID. Defaults to
#'   `FABRICQUERYR_CLIENT_ID`, then the Azure CLI application ID
#' @param token Optional access token or token-provider function. Leave `NULL`
#'   to use the package's normal sign-in flow. Refresh handles reuse their
#'   in-process credential unless new authentication arguments are supplied
#' @param auth_args Additional sign-in options passed to
#'   [AzureAuth::get_azure_token()]
#' @param api_base Power BI REST API base URL. The commercial-cloud default is
#'   normally correct
#' @param .sleep,.now Internal hooks for deterministic polling tests
#'
#' @section Standard and enhanced refresh:
#' A standard refresh processes the complete model with Power BI defaults and
#' works on shared capacity, subject to the shared-capacity request quota. An
#' enhanced refresh is selected when any processing option is supplied. It can
#' target tables or partitions, retry, change commit behavior, and set an
#' attempt timeout, but requires a capacity-backed model. Only one refresh can
#' run for a semantic model at a time
#'
#' `Transactional` is the safe commit default. `PartialBatch` can expose a
#' partially refreshed model after failure and cannot apply an incremental
#' refresh policy. Each retry receives its own attempt timeout, while Power BI
#' limits the entire refresh including retries to 24 hours
#'
#' @section Results and diagnosis:
#' `fabric_pbi_refresh()` returns a reusable handle
#' `fabric_pbi_refresh_status()` and `fabric_pbi_refresh_wait()` return a
#' `fabric_pbi_refresh_detail` with `state`, service status fields, UTC times,
#' processing objects, attempts, engine messages, parsed service errors, a
#' browser `details_url`, and the untouched response in `raw`
#' `fabric_pbi_refresh_history()` returns a list of the same detail records
#'
#' Power BI can report a successful refresh with warnings, but Microsoft notes
#' that the history and execution-detail REST APIs do not always include those
#' warnings. When warning messages are returned, the normalized state is
#' `CompletedWithWarnings`; otherwise use `details_url` to inspect the Fabric
#' refresh-detail page
#'
#' @section Permissions and service limits:
#' Starting and cancelling require `Dataset.ReadWrite.All` and semantic-model
#' Write permission. History and status accept `Dataset.Read.All` or
#' `Dataset.ReadWrite.All`, but history callers still need model Write
#' permission. A service principal may call the APIs when the tenant allows it
#' and the principal has sufficient workspace/model access; email notification
#' options do not apply to service-principal requests
#'
#' Shared capacity permits at most eight scheduled and API refresh requests per
#' day and does not support enhanced refresh. Capacity-backed models have no
#' fixed API-refresh count but can queue or throttle under load. Cancellation
#' is supported for Import and Composite models in Premium, PPU, Embedded, or
#' Fabric capacity and requires Contributor, Member, or Admin workspace access
#'
#' Direct Lake refresh is a usually short metadata framing operation, not an
#' import of OneLake data. Automatic Direct Lake updates are enabled by default,
#' so an explicit refresh can be unnecessary unless automatic updates are
#' disabled or a controlled point-in-time frame is required
#'
#' @return `fabric_pbi_refresh()` returns a `fabric_pbi_refresh` handle
#'   Status and wait return a `fabric_pbi_refresh_detail`; history returns a
#'   `fabric_pbi_refresh_history` list. Cancel invisibly returns `TRUE`
#' @references
#' [Refresh Dataset API](https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/refresh-dataset-in-group)
#'
#' [Enhanced refresh](https://learn.microsoft.com/en-us/power-bi/connect-data/asynchronous-refresh)
#'
#' [Refresh history](https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/get-refresh-history-in-group)
#'
#' [Refresh execution details](https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/get-refresh-execution-details-in-group)
#'
#' [Data refresh and capacity limits](https://learn.microsoft.com/en-us/power-bi/connect-data/refresh-data)
#'
#' [How Direct Lake refresh works](https://learn.microsoft.com/en-us/fabric/fundamentals/direct-lake-how-it-works)
#' @examples
#' \dontrun{
#' # Discover the semantic model instead of copying workspace and model IDs
#' workspace <- fabric_workspaces()[[1L]]
#' model <- fabric_semantic_models(workspace)[[1L]]
#'
#' # Start a refresh, inspect it once, then wait for completion
#' refresh <- fabric_pbi_refresh(model)
#' current <- fabric_pbi_refresh_status(refresh)
#' current$state
#' result <- fabric_pbi_refresh_wait(refresh, timeout = 1800)
#' result$state
#' result$details_url
#'
#' # An active refresh can instead be cancelled when it is no longer needed
#' refresh_to_cancel <- fabric_pbi_refresh(model)
#' fabric_pbi_refresh_cancel(refresh_to_cancel)
#'
#' # Choose a table shown in the model, then refresh only that table
#' refresh_table <- Sys.getenv("FABRIC_PBI_TABLE")
#' sales_only <- fabric_pbi_refresh(
#'   model,
#'   mode = "enhanced",
#'   type = "Full",
#'   objects = refresh_table,
#'   retry_count = 1L,
#'   timeout = "02:00:00"
#' )
#' fabric_pbi_refresh_wait(sales_only)
#'
#' # Finally, inspect recent refreshes for the same discovered model
#' history <- fabric_pbi_refresh_history(model, top = 10L)
#' history[[1]]$attempts
#' }
#' @export
fabric_pbi_refresh <- function(
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
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  token = NULL,
  auth_args = list(),
  api_base = "https://api.powerbi.com/v1.0/myorg"
) {
  # 1 Resolve the semantic model -------------------------------------------------------------------

  # Reuse the DAX target rules so query and refresh workflows accept the same inputs

  notify_option_missing <- missing(notify_option)
  mode <- match.arg(mode)
  api_base <- pbi_api_base(api_base)
  credential <- fabric_credential(
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args
  )
  target <- .pbi_refresh_target(
    connstr,
    workspace_id,
    dataset_id,
    my_workspace,
    credential,
    api_base
  )

  # 2 Build the documented request body ------------------------------------------------------------

  # Standard and enhanced requests have different capacity and payload contracts

  request <- .pbi_refresh_payload(
    mode = mode,
    notify_option = notify_option,
    type = type,
    commit_mode = commit_mode,
    objects = objects,
    apply_refresh_policy = apply_refresh_policy,
    effective_date = effective_date,
    max_parallelism = max_parallelism,
    retry_count = retry_count,
    refresh_timeout = timeout,
    notify_option_missing = notify_option_missing
  )
  url <- .pbi_refresh_collection_url(api_base, target)

  # 3 Submit once and retain the refresh identity --------------------------------------------------

  # A refresh is not safe to replay because a lost response may still represent a running request

  response <- .pbi_refresh_request(
    "POST",
    url,
    credential,
    payload = request$payload,
    idempotent = FALSE
  )
  refresh_id <- .pbi_refresh_response_id(response)

  .pbi_refresh_handle(
    refresh_id = refresh_id,
    target = target,
    credential = credential,
    api_base = api_base,
    location = response$location,
    retry_after = response$retry_after,
    mode = request$mode
  )
}

#' @rdname fabric_pbi_refresh
#' @export
fabric_pbi_refresh_history <- function(
  connstr = NULL,
  workspace_id = NULL,
  dataset_id = NULL,
  my_workspace = FALSE,
  top = NULL,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  token = NULL,
  auth_args = list(),
  api_base = "https://api.powerbi.com/v1.0/myorg"
) {
  # 1 Resolve and validate inputs ------------------------------------------------------------------

  # History uses the same model selector and Power BI credential as refresh submission

  if (!is.null(top)) {
    .pbi_refresh_whole_number(top, "top", minimum = 1)
    top <- as.integer(top)
  }
  api_base <- pbi_api_base(api_base)
  credential <- fabric_credential(
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args
  )
  target <- .pbi_refresh_target(
    connstr,
    workspace_id,
    dataset_id,
    my_workspace,
    credential,
    api_base
  )

  # 2 Read and normalize recent refreshes ----------------------------------------------------------

  # Power BI returns a bounded history collection rather than paginated continuation links

  request <- httr2::request(.pbi_refresh_collection_url(api_base, target))
  if (!is.null(top)) {
    request <- httr2::req_url_query(request, `$top` = top)
  }
  response <- .pbi_refresh_request(
    "GET",
    request$url,
    credential,
    idempotent = TRUE
  )
  values <- response$body$value %||% list()
  details <- lapply(values, function(value) {
    refresh_id <- value$requestId
    if (
      !is.character(refresh_id) ||
        length(refresh_id) != 1L ||
        !fabric_is_guid(refresh_id)
    ) {
      .fabric_abort(
        "Power BI returned refresh history without a valid requestId",
        class = c(
          "fabric_pbi_refresh_protocol_error",
          "fabric_pbi_refresh_error"
        )
      )
    }
    handle <- .pbi_refresh_handle(
      refresh_id = refresh_id,
      target = target,
      credential = credential,
      api_base = api_base,
      mode = if (identical(value$refreshType, "ViaEnhancedApi")) {
        "enhanced"
      } else {
        "standard"
      }
    )
    .pbi_refresh_detail(value, handle, status_code = 200L, history = TRUE)
  })
  structure(details, class = c("fabric_pbi_refresh_history", "list"))
}

#' @rdname fabric_pbi_refresh
#' @export
fabric_pbi_refresh_status <- function(
  refresh = NULL,
  connstr = NULL,
  workspace_id = NULL,
  dataset_id = NULL,
  my_workspace = FALSE,
  refresh_id = NULL,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  token = NULL,
  auth_args = list(),
  api_base = "https://api.powerbi.com/v1.0/myorg",
  .sleep = Sys.sleep,
  .now = Sys.time
) {
  # 1 Resolve the reusable refresh context ---------------------------------------------------------

  # Handles retain their target and credential; raw request IDs reconstruct that context

  override_auth <- !missing(tenant_id) ||
    !missing(client_id) ||
    !is.null(token) ||
    length(auth_args) > 0L
  context <- .pbi_refresh_context(
    refresh = refresh,
    refresh_id = refresh_id,
    connstr = connstr,
    workspace_id = workspace_id,
    dataset_id = dataset_id,
    my_workspace = my_workspace,
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args,
    api_base = api_base,
    override_auth = override_auth
  )

  # 2 Honor the first service polling hint ---------------------------------------------------------

  # Delaying only the first handle check avoids querying sooner than the submission response asks

  next_poll_at <- context$refresh$next_poll_at
  if (!is.null(next_poll_at)) {
    delay <- as.numeric(difftime(next_poll_at, .now(), units = "secs"))
    if (is.finite(delay) && delay > 0) {
      .sleep(delay)
    }
  }

  # 3 Return one execution-detail snapshot ---------------------------------------------------------

  # Status requests accept both 200 terminal and 202 active responses

  .pbi_refresh_get_status(context)
}

#' @rdname fabric_pbi_refresh
#' @export
fabric_pbi_refresh_wait <- function(
  refresh,
  poll_interval = NULL,
  timeout = 1800,
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
  api_base = "https://api.powerbi.com/v1.0/myorg",
  .sleep = Sys.sleep,
  .now = Sys.time
) {
  # 1 Validate polling behavior --------------------------------------------------------------------

  # Invalid limits must fail before any status or cancellation request is made

  if (
    !inherits(refresh, "fabric_pbi_refresh") &&
      !inherits(refresh, "fabric_pbi_refresh_detail")
  ) {
    .fabric_abort(
      "refresh must be a fabric_pbi_refresh handle or detail record"
    )
  }
  .pbi_refresh_number(timeout, "timeout", minimum = 0)
  if (!is.null(poll_interval)) {
    .pbi_refresh_number(poll_interval, "poll_interval", minimum = 0)
  }
  .pbi_refresh_flag(error_on_failure, "error_on_failure")
  .pbi_refresh_flag(cancel_on_timeout, "cancel_on_timeout")
  if (!is.null(cancel) && !is.function(cancel)) {
    .fabric_abort("cancel must be a function or NULL")
  }

  override_auth <- !missing(tenant_id) ||
    !missing(client_id) ||
    !is.null(token) ||
    length(auth_args) > 0L
  context <- .pbi_refresh_context(
    refresh = refresh,
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args,
    api_base = api_base,
    override_auth = override_auth
  )
  started <- .now()
  last <- NULL
  next_poll_at <- if (is.null(context$refresh$retry_after)) {
    NULL
  } else {
    context$refresh$next_poll_at
  }
  if (
    is.null(next_poll_at) &&
      !is.null(context$refresh$submitted_at) &&
      !is.null(context$refresh$retry_after)
  ) {
    next_poll_at <- context$refresh$submitted_at +
      context$refresh$retry_after
  }
  retry_after <- if (is.null(next_poll_at)) {
    context$refresh$retry_after
  } else {
    max(
      0,
      as.numeric(difftime(next_poll_at, started, units = "secs"))
    )
  }

  # 2 Poll until the service reaches a terminal state ----------------------------------------------

  # Client cancellation and timeout remain distinct from service cancellation and timeout states

  progress <- .fabric_poll_progress("Power BI refresh", context$id)
  repeat {
    if (!is.null(cancel) && isTRUE(cancel())) {
      outcome <- .pbi_refresh_cancel_outcome(context)
      .fabric_abort(
        "Power BI refresh polling was cancelled by the caller",
        class = c(
          "fabric_pbi_refresh_cancelled_by_caller",
          "fabric_pbi_refresh_error"
        ),
        refresh = context$refresh,
        last_status = last,
        cancel_accepted = outcome$accepted,
        cancel_error = outcome$error
      )
    }

    elapsed <- as.numeric(difftime(.now(), started, units = "secs"))
    if (elapsed >= timeout) {
      outcome <- if (isTRUE(cancel_on_timeout)) {
        .pbi_refresh_cancel_outcome(context)
      } else {
        list(accepted = NULL, error = NULL)
      }
      .fabric_abort(
        sprintf(
          "Timed out after %s seconds waiting for Power BI refresh %s",
          format(timeout, trim = TRUE),
          context$id
        ),
        class = c(
          "fabric_pbi_refresh_wait_timeout",
          "fabric_pbi_refresh_error"
        ),
        refresh = context$refresh,
        last_status = last,
        cancel_accepted = outcome$accepted,
        cancel_error = outcome$error
      )
    }

    delay <- max(
      .fabric_pbi_refresh_poll_floor,
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

    last <- .pbi_refresh_get_status(context)
    retry_after <- last$retry_after
    .fabric_poll_progress_update(progress, last$state)
    if (last$state %in% .fabric_pbi_refresh_active_states) {
      next
    }
    if (!last$state %in% .fabric_pbi_refresh_terminal_states) {
      .fabric_abort(
        paste0("Power BI returned an unknown refresh state: ", last$state),
        class = c(
          "fabric_pbi_refresh_unknown_status",
          "fabric_pbi_refresh_error"
        ),
        refresh = context$refresh,
        refresh_status = last
      )
    }
    if (
      last$state %in%
        .fabric_pbi_refresh_success_states ||
        !isTRUE(error_on_failure)
    ) {
      .fabric_poll_progress_done(progress)
      return(last)
    }
    .pbi_refresh_abort_terminal(last, context$refresh)
  }
}

#' @rdname fabric_pbi_refresh
#' @export
fabric_pbi_refresh_cancel <- function(
  refresh = NULL,
  connstr = NULL,
  workspace_id = NULL,
  dataset_id = NULL,
  my_workspace = FALSE,
  refresh_id = NULL,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  token = NULL,
  auth_args = list(),
  api_base = "https://api.powerbi.com/v1.0/myorg"
) {
  # 1 Resolve the refresh --------------------------------------------------------------------------

  # Cancellation accepts either the reusable handle or a request ID plus model selectors

  override_auth <- !missing(tenant_id) ||
    !missing(client_id) ||
    !is.null(token) ||
    length(auth_args) > 0L
  context <- .pbi_refresh_context(
    refresh = refresh,
    refresh_id = refresh_id,
    connstr = connstr,
    workspace_id = workspace_id,
    dataset_id = dataset_id,
    my_workspace = my_workspace,
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args,
    api_base = api_base,
    override_auth = override_auth
  )

  # 2 Ask Power BI to cancel -----------------------------------------------------------------------

  # DELETE is safe to retry at the transport layer and a successful response has no body

  .pbi_refresh_cancel_context(context)
  invisible(TRUE)
}

#' Print a submitted Power BI refresh
#'
#' @param x A `fabric_pbi_refresh` returned by [fabric_pbi_refresh()]
#' @param ... Reserved for the print method
#' @return `x`, invisibly
#' @export
print.fabric_pbi_refresh <- function(x, ...) {
  .fabric_print(
    "fabric_pbi_refresh",
    list(
      refresh = x$id,
      dataset = x$dataset_id,
      workspace = x$workspace_id %||% "My Workspace",
      mode = x$mode
    )
  )
  invisible(x)
}

#' Print Power BI refresh details
#'
#' @param x A `fabric_pbi_refresh_detail` returned by a refresh status, wait,
#'   or history function
#' @param ... Reserved for the print method
#' @return `x`, invisibly
#' @export
print.fabric_pbi_refresh_detail <- function(x, ...) {
  .fabric_print(
    "fabric_pbi_refresh_detail",
    list(
      refresh = x$id,
      state = x$state,
      attempts = x$number_of_attempts %||% length(x$attempts)
    )
  )
  invisible(x)
}

# Resolve a model selector into workspace and dataset IDs. Returns the common
# target used by refresh submission, history, status, and cancellation
.pbi_refresh_target <- function(
  connstr,
  workspace_id,
  dataset_id,
  my_workspace,
  credential,
  api_base
) {
  # 1 Read a discovered semantic-model record ------------------------------------------------------

  # Records may carry IDs and a connection string, while explicit IDs must agree with them

  discovered <- fabric_as_record(connstr)
  if (!is.null(discovered)) {
    if (
      !identical(
        tolower(fabric_record_value(discovered, "type") %||% ""),
        "semanticmodel"
      )
    ) {
      .fabric_abort("connstr discovery record must be a SemanticModel item")
    }
    discovered_workspace_id <- fabric_record_value(discovered, "workspaceId")
    discovered_dataset_id <- fabric_record_value(discovered, "id")
    pbi_validate_optional_guid(
      discovered_workspace_id,
      "discovered workspaceId"
    )
    pbi_validate_optional_guid(discovered_dataset_id, "discovered dataset id")
    pbi_reject_conflicting_id(
      workspace_id,
      discovered_workspace_id,
      "workspace_id",
      "the discovered SemanticModel workspaceId"
    )
    pbi_reject_conflicting_id(
      dataset_id,
      discovered_dataset_id,
      "dataset_id",
      "the discovered SemanticModel id"
    )
    workspace_id <- workspace_id %||% discovered_workspace_id
    dataset_id <- dataset_id %||% discovered_dataset_id
    connstr <- if (is.null(dataset_id)) {
      fabric_record_value(discovered, "dax_connection_string")
    } else {
      NULL
    }
  }

  # 2 Validate direct selectors --------------------------------------------------------------------

  # Connection strings and explicit selectors are alternatives, matching DAX query behavior

  if (
    !is.null(connstr) &&
      (!is.character(connstr) ||
        length(connstr) != 1L ||
        is.na(connstr) ||
        !nzchar(connstr))
  ) {
    .fabric_abort("connstr must be one non-empty string")
  }
  pbi_validate_optional_guid(workspace_id, "workspace_id")
  pbi_validate_optional_guid(dataset_id, "dataset_id")
  .pbi_refresh_flag(my_workspace, "my_workspace")

  if (!is.null(workspace_id) && isTRUE(my_workspace)) {
    .fabric_abort("Supply workspace_id or set my_workspace = TRUE, not both")
  }
  if (
    is.null(discovered) &&
      !is.null(connstr) &&
      (!is.null(workspace_id) || !is.null(dataset_id) || isTRUE(my_workspace))
  ) {
    .fabric_abort(
      paste0(
        "Supply a connection string or explicit workspace/dataset selectors, ",
        "not both"
      ),
      class = "fabric_pbi_target_conflict"
    )
  }
  if (is.null(dataset_id) && is.null(connstr)) {
    .fabric_abort("Supply either connstr or dataset_id")
  }

  # 3 Resolve connection-string names --------------------------------------------------------------

  # Name lookup happens only when a dataset GUID was not already supplied

  if (is.null(dataset_id)) {
    ids <- pbi_resolve_ids_from_connstr(
      connstr = connstr,
      credential = credential,
      api_base = api_base
    )
    workspace_id <- ids$group_id
    dataset_id <- ids$dataset_id
  }
  if (is.null(workspace_id) && !isTRUE(my_workspace)) {
    .fabric_abort(
      "dataset_id requires workspace_id or explicit my_workspace = TRUE"
    )
  }

  list(
    workspace_id = workspace_id,
    dataset_id = dataset_id,
    my_workspace = isTRUE(my_workspace)
  )
}

# Validate refresh controls and return the exact standard or enhanced JSON body
.pbi_refresh_payload <- function(
  mode,
  notify_option,
  type,
  commit_mode,
  objects,
  apply_refresh_policy,
  effective_date,
  max_parallelism,
  retry_count,
  refresh_timeout,
  notify_option_missing = FALSE
) {
  mode <- match.arg(mode, c("automatic", "standard", "enhanced"))
  enhanced_values <- list(
    type = type,
    commitMode = commit_mode,
    objects = objects,
    applyRefreshPolicy = apply_refresh_policy,
    effectiveDate = effective_date,
    maxParallelism = max_parallelism,
    retryCount = retry_count,
    timeout = refresh_timeout
  )
  has_enhanced <- !all(vapply(enhanced_values, is.null, logical(1)))
  if (identical(mode, "automatic")) {
    mode <- if (has_enhanced) "enhanced" else "standard"
  }
  if (identical(mode, "standard") && has_enhanced) {
    .fabric_abort(
      "Enhanced refresh options cannot be used with mode = \"standard\""
    )
  }
  if (identical(mode, "enhanced") && !is.null(notify_option)) {
    .fabric_abort("notify_option cannot be used with an enhanced refresh")
  }
  if (identical(mode, "standard") && isTRUE(notify_option_missing)) {
    notify_option <- "MailOnFailure"
  }

  notify_option <- .pbi_refresh_choice(
    notify_option,
    "notify_option",
    c("NoNotification", "MailOnFailure", "MailOnCompletion")
  )
  if (identical(mode, "standard")) {
    return(list(
      mode = mode,
      payload = Filter(Negate(is.null), list(notifyOption = notify_option))
    ))
  }

  type <- .pbi_refresh_choice(
    type,
    "type",
    c(
      "Full",
      "ClearValues",
      "Calculate",
      "DataOnly",
      "Automatic",
      "Defragment"
    )
  )
  commit_mode <- .pbi_refresh_choice(
    commit_mode,
    "commit_mode",
    c("Transactional", "PartialBatch")
  )
  objects <- .pbi_refresh_objects(objects)
  if (!is.null(apply_refresh_policy)) {
    .pbi_refresh_flag(apply_refresh_policy, "apply_refresh_policy")
  }
  if (
    identical(commit_mode, "PartialBatch") &&
      isTRUE(apply_refresh_policy)
  ) {
    .fabric_abort(
      "apply_refresh_policy = TRUE requires commit_mode = \"Transactional\""
    )
  }
  if (
    identical(commit_mode, "PartialBatch") &&
      is.null(apply_refresh_policy)
  ) {
    apply_refresh_policy <- FALSE
  }
  effective_date <- .pbi_refresh_effective_date(effective_date)
  if (!is.null(max_parallelism)) {
    .pbi_refresh_whole_number(
      max_parallelism,
      "max_parallelism",
      minimum = 1
    )
    max_parallelism <- as.integer(max_parallelism)
  }
  if (!is.null(retry_count)) {
    .pbi_refresh_whole_number(retry_count, "retry_count", minimum = 0)
    retry_count <- as.integer(retry_count)
  }
  timeout_seconds <- .pbi_refresh_timeout(refresh_timeout)
  attempt_timeout_seconds <- timeout_seconds %||% (5 * 60 * 60)
  attempts <- (retry_count %||% 0L) + 1L
  if (attempt_timeout_seconds * attempts > 24 * 60 * 60) {
    .fabric_abort(
      "timeout multiplied by all attempts cannot exceed 24 hours"
    )
  }

  payload <- Filter(
    Negate(is.null),
    list(
      type = type,
      commitMode = commit_mode,
      objects = objects,
      applyRefreshPolicy = apply_refresh_policy,
      effectiveDate = effective_date,
      maxParallelism = max_parallelism,
      retryCount = retry_count,
      timeout = refresh_timeout
    )
  )
  if (!length(payload)) {
    payload$type <- "Automatic"
  }
  list(mode = mode, payload = payload)
}

# Reconstruct the credential and target from a handle or raw request ID
.pbi_refresh_context <- function(
  refresh,
  refresh_id = NULL,
  connstr = NULL,
  workspace_id = NULL,
  dataset_id = NULL,
  my_workspace = FALSE,
  tenant_id = NULL,
  client_id = NULL,
  token = NULL,
  auth_args = list(),
  api_base = "https://api.powerbi.com/v1.0/myorg",
  override_auth = !is.null(token) || length(auth_args) > 0L
) {
  if (inherits(refresh, "fabric_pbi_refresh_detail")) {
    if (!is.null(refresh_id)) {
      .fabric_abort("refresh_id cannot be combined with a refresh detail")
    }
    refresh <- refresh$refresh
  }

  if (inherits(refresh, "fabric_pbi_refresh")) {
    if (!is.null(refresh_id)) {
      .fabric_abort("refresh_id cannot be combined with a refresh handle")
    }
    if (
      !is.null(connstr) ||
        !is.null(workspace_id) ||
        !is.null(dataset_id) ||
        isTRUE(my_workspace)
    ) {
      .fabric_abort(
        "Semantic-model selectors cannot be combined with a refresh handle"
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
      .pbi_refresh_credential(refresh)
    }
    handle <- refresh
    if (override_auth) {
      reference <- .pbi_refresh_credential_reference(credential)
      handle$credential <- reference$reference
      handle$.credential_key <- reference$key
    }
    base <- pbi_api_base(handle$api_base %||% api_base)
    return(list(
      id = handle$id,
      target = list(
        workspace_id = handle$workspace_id,
        dataset_id = handle$dataset_id,
        my_workspace = handle$my_workspace
      ),
      api_base = base,
      credential = credential,
      refresh = handle
    ))
  }

  id <- refresh_id %||% refresh
  pbi_validate_optional_guid(id, "refresh ID")
  if (is.null(id)) {
    .fabric_abort("Supply a refresh handle or refresh_id")
  }
  base <- pbi_api_base(api_base)
  credential <- fabric_credential(
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args
  )
  target <- .pbi_refresh_target(
    connstr,
    workspace_id,
    dataset_id,
    my_workspace,
    credential,
    base
  )
  handle <- .pbi_refresh_handle(
    refresh_id = id,
    target = target,
    credential = credential,
    api_base = base,
    mode = "unknown"
  )
  list(
    id = id,
    target = target,
    api_base = base,
    credential = credential,
    refresh = handle
  )
}

# Submit one Power BI refresh request and return headers plus any decoded body
.pbi_refresh_request <- function(
  method,
  url,
  credential,
  payload = NULL,
  idempotent = NULL
) {
  request <- httr2::request(url) |>
    httr2::req_method(method)
  if (!is.null(payload) && length(payload)) {
    if (!is.null(payload$objects)) {
      payload$objects <- I(payload$objects)
    }
    request <- httr2::req_body_json(
      request,
      payload,
      auto_unbox = TRUE,
      null = "null"
    )
  }
  response <- .httr2_perform(
    request,
    credential = credential,
    audience = .fabric_audience$power_bi,
    idempotent = idempotent,
    accepted_status = 202L
  )
  status <- httr2::resp_status(response)
  text <- tryCatch(
    httr2::resp_body_string(response),
    error = function(error) ""
  )
  list(
    status_code = status,
    location = httr2::resp_header(response, "location"),
    request_id = httr2::resp_header(response, "x-ms-request-id"),
    retry_after = .httr2_retry_after(response),
    body = if (nzchar(text)) {
      httr2::resp_body_json(response, simplifyVector = FALSE)
    } else {
      list()
    }
  )
}

# Extract and validate the request ID returned by refresh submission
.pbi_refresh_response_id <- function(response) {
  location <- response$location
  location_id <- if (
    is.character(location) &&
      length(location) == 1L &&
      nzchar(location)
  ) {
    path <- sub("[?#].*$", "", location)
    sub(".*/", "", sub("/+$", "", path))
  } else {
    NULL
  }
  refresh_id <- response$request_id %||% location_id
  if (
    !is.character(refresh_id) ||
      length(refresh_id) != 1L ||
      !fabric_is_guid(refresh_id)
  ) {
    .fabric_abort(
      "Power BI accepted the refresh without a valid refresh request ID",
      class = c("fabric_pbi_refresh_protocol_error", "fabric_pbi_refresh_error")
    )
  }
  if (
    !is.null(location_id) &&
      fabric_is_guid(location_id) &&
      !identical(tolower(location_id), tolower(refresh_id))
  ) {
    .fabric_abort(
      "Power BI returned conflicting refresh IDs in response headers",
      class = c("fabric_pbi_refresh_protocol_error", "fabric_pbi_refresh_error")
    )
  }
  refresh_id
}

# Create a reusable, serialization-safe refresh handle
.pbi_refresh_handle <- function(
  refresh_id,
  target,
  credential,
  api_base,
  location = NULL,
  retry_after = NULL,
  mode
) {
  submitted_at <- Sys.time()
  reference <- .pbi_refresh_credential_reference(credential)
  structure(
    list(
      id = refresh_id,
      workspace_id = target$workspace_id,
      dataset_id = target$dataset_id,
      my_workspace = target$my_workspace,
      location = location,
      retry_after = retry_after,
      submitted_at = submitted_at,
      next_poll_at = if (is.null(retry_after)) {
        submitted_at
      } else {
        submitted_at + retry_after
      },
      mode = mode,
      api_base = api_base,
      credential = reference$reference,
      .credential_key = reference$key
    ),
    class = "fabric_pbi_refresh"
  )
}

# Store credentials behind a weak reference so serialized handles do not retain tokens
.pbi_refresh_credential_reference <- function(credential) {
  key <- new.env(parent = emptyenv())
  list(
    reference = rlang::new_weakref(key, credential),
    key = key
  )
}

# Recover an in-process credential or ask the caller to authenticate again
.pbi_refresh_credential <- function(refresh) {
  stored <- refresh$credential
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
        "This Power BI refresh handle no longer has an in-process credential; ",
        "supply token, tenant_id, or other authentication arguments"
      ),
      class = c(
        "fabric_pbi_refresh_credential_error",
        "fabric_pbi_refresh_error"
      )
    )
  }
  credential
}

# Read one status response and convert it to the public detail contract
.pbi_refresh_get_status <- function(context) {
  response <- .pbi_refresh_request(
    "GET",
    .pbi_refresh_item_url(context$api_base, context$target, context$id),
    context$credential,
    idempotent = TRUE
  )
  .pbi_refresh_detail(
    response$body,
    context$refresh,
    status_code = response$status_code,
    retry_after = response$retry_after
  )
}

# Send cancellation using an already-resolved refresh context
.pbi_refresh_cancel_context <- function(context) {
  .pbi_refresh_request(
    "DELETE",
    .pbi_refresh_item_url(context$api_base, context$target, context$id),
    context$credential,
    idempotent = TRUE
  )
  invisible(TRUE)
}

# Attempt cancellation without replacing the waiter's original condition
.pbi_refresh_cancel_outcome <- function(context) {
  tryCatch(
    {
      .pbi_refresh_cancel_context(context)
      list(accepted = TRUE, error = NULL)
    },
    error = function(error) list(accepted = FALSE, error = error)
  )
}

# Normalize one history or execution-detail response while retaining every raw field
.pbi_refresh_detail <- function(
  body,
  refresh,
  status_code,
  retry_after = NULL,
  history = FALSE
) {
  messages <- lapply(body$messages %||% list(), .pbi_refresh_message)
  attempts <- lapply(
    body$refreshAttempts %||% list(),
    .pbi_refresh_attempt
  )
  message_types <- tolower(vapply(
    messages,
    function(message) message$type %||% "",
    character(1)
  ))
  has_warnings <- "warning" %in% message_types
  has_errors <- "error" %in% message_types
  service_error <- .pbi_refresh_service_error(body$serviceExceptionJson)
  state <- .pbi_refresh_state(
    status = body$status,
    extended_status = body$extendedStatus,
    status_code = status_code,
    has_warnings = has_warnings,
    history = history
  )
  refresh_type <- body$refreshType %||% body$initiatedBy
  details_url <- .pbi_refresh_details_url(refresh)
  structure(
    list(
      id = body$requestId %||% refresh$id,
      request_id = body$requestId %||% refresh$id,
      dataset_id = refresh$dataset_id,
      workspace_id = refresh$workspace_id,
      my_workspace = refresh$my_workspace,
      state = state,
      status = body$status %||% "Unknown",
      extended_status = body$extendedStatus,
      refresh_type = refresh_type,
      type = body$type,
      current_refresh_type = body$currentRefreshType,
      commit_mode = body$commitMode,
      start_time = .pbi_refresh_time(body$startTime),
      end_time = .pbi_refresh_time(body$endTime),
      number_of_attempts = body$numberOfAttempts %||% length(attempts),
      attempts = attempts,
      objects = body$objects %||% list(),
      messages = messages,
      has_warnings = has_warnings,
      has_errors = has_errors || !is.null(service_error),
      service_exception = body$serviceExceptionJson,
      service_error = service_error,
      details_url = details_url,
      retry_after = retry_after,
      terminal = state %in% .fabric_pbi_refresh_terminal_states,
      refresh = refresh,
      raw = body
    ),
    class = "fabric_pbi_refresh_detail"
  )
}

# Normalize one automatic refresh attempt and its diagnostic information
.pbi_refresh_attempt <- function(attempt) {
  service_error <- .pbi_refresh_service_error(attempt$serviceExceptionJson)
  status <- attempt$status
  if (is.null(status)) {
    status <- if (!is.null(service_error)) {
      "Failed"
    } else if (!is.null(attempt$endTime)) {
      "Completed"
    } else {
      "InProgress"
    }
  }
  list(
    id = attempt$attemptId,
    type = attempt$type,
    status = status,
    start_time = .pbi_refresh_time(attempt$startTime),
    end_time = .pbi_refresh_time(attempt$endTime),
    service_exception = attempt$serviceExceptionJson,
    service_error = service_error,
    execution_metrics = attempt$executionMetrics %||% list(),
    raw = attempt
  )
}

# Normalize one engine message returned by enhanced refresh details
.pbi_refresh_message <- function(message) {
  list(
    code = message$code,
    type = message$type,
    message = message$message,
    raw = message
  )
}

# Convert service status fields into stable states used by wait and callers
.pbi_refresh_state <- function(
  status,
  extended_status,
  status_code,
  has_warnings,
  history
) {
  extended <- tolower(extended_status %||% "")
  general <- tolower(status %||% "unknown")
  if (extended %in% c("notstarted", "queued")) {
    return("Queued")
  }
  if (extended %in% c("inprogress", "running")) {
    return("InProgress")
  }
  if (extended == "timedout") {
    return("TimedOut")
  }
  if (extended %in% c("cancelled", "canceled")) {
    return("Cancelled")
  }
  if (extended == "failed") {
    return("Failed")
  }
  if (extended == "disabled") {
    return("Disabled")
  }
  if (extended == "completed") {
    return(if (isTRUE(has_warnings)) "CompletedWithWarnings" else "Completed")
  }
  if (general == "completed") {
    return(if (isTRUE(has_warnings)) "CompletedWithWarnings" else "Completed")
  }
  if (general == "failed") {
    return("Failed")
  }
  if (general == "disabled") {
    return("Disabled")
  }
  if (general %in% c("cancelled", "canceled")) {
    return("Cancelled")
  }
  if (general == "timedout") {
    return("TimedOut")
  }
  if (
    general == "unknown" && (identical(status_code, 202L) || isTRUE(history))
  ) {
    return("InProgress")
  }
  extended_status %||% status %||% "Unknown"
}

# Parse a JSON-encoded service exception without discarding its original text
.pbi_refresh_service_error <- function(value) {
  if (
    is.null(value) ||
      !is.character(value) ||
      length(value) != 1L ||
      is.na(value) ||
      !nzchar(value)
  ) {
    return(NULL)
  }
  tryCatch(
    jsonlite::fromJSON(value, simplifyVector = FALSE),
    error = function(error) list(message = value, parse_error = TRUE)
  )
}

# Convert a service timestamp to one UTC POSIXct value
.pbi_refresh_time <- function(value) {
  if (
    is.null(value) ||
      !is.character(value) ||
      length(value) != 1L ||
      is.na(value) ||
      !nzchar(value)
  ) {
    return(NULL)
  }
  normalized <- sub("Z$", "+0000", value, ignore.case = TRUE)
  normalized <- sub("([+-][0-9]{2}):([0-9]{2})$", "\\1\\2", normalized)
  has_offset <- grepl("[+-][0-9]{4}$", normalized)
  parsed <- as.POSIXct(
    normalized,
    format = if (has_offset) {
      "%Y-%m-%dT%H:%M:%OS%z"
    } else {
      "%Y-%m-%dT%H:%M:%OS"
    },
    tz = "UTC"
  )
  if (is.na(parsed)) {
    .fabric_abort(
      paste0("Power BI returned an invalid refresh timestamp: ", value),
      class = c("fabric_pbi_refresh_protocol_error", "fabric_pbi_refresh_error")
    )
  }
  parsed
}

# Build the documented Fabric refresh-details browser link
.pbi_refresh_details_url <- function(refresh) {
  workspace <- refresh$workspace_id %||%
    if (isTRUE(refresh$my_workspace)) "me" else NULL
  if (is.null(workspace)) {
    return(NULL)
  }
  sprintf(
    "https://app.powerbi.com/groups/%s/datasets/%s/refreshdetails/%s",
    workspace,
    refresh$dataset_id,
    refresh$id
  )
}

# Build the refresh collection URL for a shared or personal workspace
.pbi_refresh_collection_url <- function(api_base, target) {
  if (isTRUE(target$my_workspace)) {
    sprintf("%s/datasets/%s/refreshes", api_base, target$dataset_id)
  } else {
    sprintf(
      "%s/groups/%s/datasets/%s/refreshes",
      api_base,
      target$workspace_id,
      target$dataset_id
    )
  }
}

# Build one refresh request URL from its collection and request ID
.pbi_refresh_item_url <- function(api_base, target, refresh_id) {
  paste0(.pbi_refresh_collection_url(api_base, target), "/", refresh_id)
}

# Normalize a documented enumeration without making case significant
.pbi_refresh_choice <- function(value, name, choices) {
  if (is.null(value)) {
    return(NULL)
  }
  if (
    !is.character(value) ||
      length(value) != 1L ||
      is.na(value) ||
      !nzchar(value)
  ) {
    .fabric_abort(paste0(name, " must be one non-empty string"))
  }
  index <- match(tolower(value), tolower(choices))
  if (is.na(index)) {
    .fabric_abort(paste0(
      name,
      " must be one of ",
      paste(choices, collapse = ", ")
    ))
  }
  choices[[index]]
}

# Normalize ergonomic table selections into the documented object array
.pbi_refresh_objects <- function(objects) {
  if (is.null(objects)) {
    return(NULL)
  }
  if (is.character(objects)) {
    if (!length(objects) || anyNA(objects) || !all(nzchar(objects))) {
      .fabric_abort("objects table names must be non-empty strings")
    }
    return(lapply(unname(objects), function(table) list(table = table)))
  }
  if (!is.list(objects) || is.data.frame(objects) || !length(objects)) {
    .fabric_abort(
      "objects must be table names or a non-empty list of table records"
    )
  }
  lapply(objects, function(object) {
    if (
      !is.list(object) ||
        is.null(names(object)) ||
        !identical(
          sort(names(object)),
          sort(intersect(
            names(object),
            c("table", "partition")
          ))
        ) ||
        !"table" %in% names(object)
    ) {
      .fabric_abort(
        "Each refresh object must contain table and optional partition"
      )
    }
    for (field in names(object)) {
      value <- object[[field]]
      if (
        !is.character(value) ||
          length(value) != 1L ||
          is.na(value) ||
          !nzchar(value)
      ) {
        .fabric_abort(paste0("Refresh object ", field, " must be one string"))
      }
    }
    object[c("table", intersect("partition", names(object)))]
  })
}

# Convert an effective policy date to an unambiguous ISO 8601 value
.pbi_refresh_effective_date <- function(value) {
  if (is.null(value)) {
    return(NULL)
  }
  if (inherits(value, "Date")) {
    if (length(value) != 1L || is.na(value)) {
      .fabric_abort("effective_date must contain one non-missing date")
    }
    return(paste0(format(value, "%Y-%m-%d"), "T00:00:00Z"))
  }
  if (inherits(value, "POSIXt")) {
    if (length(value) != 1L || is.na(value)) {
      .fabric_abort("effective_date must contain one non-missing date-time")
    }
    return(format(value, "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"))
  }
  if (
    !is.character(value) ||
      length(value) != 1L ||
      is.na(value) ||
      !grepl(
        paste0(
          "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}",
          "(?:\\.[0-9]+)?(?:Z|[+-][0-9]{2}:[0-9]{2})$"
        ),
        value
      )
  ) {
    .fabric_abort(
      "effective_date must be a Date, POSIXt, or ISO 8601 date-time"
    )
  }
  normalized <- sub("Z$", "+0000", value, ignore.case = TRUE)
  normalized <- sub("([+-][0-9]{2}):([0-9]{2})$", "\\1\\2", normalized)
  parsed <- strptime(normalized, "%Y-%m-%dT%H:%M:%OS%z", tz = "UTC")
  if (is.na(parsed)) {
    .fabric_abort("effective_date must contain a valid ISO 8601 date-time")
  }
  value
}

# Validate an attempt timeout and return its duration in seconds
.pbi_refresh_timeout <- function(value) {
  if (is.null(value)) {
    return(NULL)
  }
  if (
    !is.character(value) ||
      length(value) != 1L ||
      is.na(value) ||
      !grepl("^(?:[01]?[0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]$", value)
  ) {
    .fabric_abort("timeout must use HH:MM:SS with hours from 0 through 23")
  }
  parts <- as.numeric(strsplit(value, ":", fixed = TRUE)[[1L]])
  parts[[1L]] * 3600 + parts[[2L]] * 60 + parts[[3L]]
}

# Validate a scalar logical option
.pbi_refresh_flag <- function(value, name) {
  if (!is.logical(value) || length(value) != 1L || is.na(value)) {
    .fabric_abort(paste0(name, " must be TRUE or FALSE"))
  }
  invisible(value)
}

# Validate a scalar finite number with an inclusive lower bound
.pbi_refresh_number <- function(value, name, minimum) {
  if (
    !is.numeric(value) ||
      length(value) != 1L ||
      is.na(value) ||
      !is.finite(value) ||
      value < minimum
  ) {
    .fabric_abort(paste0(name, " must be one number at least ", minimum))
  }
  invisible(value)
}

# Validate a scalar whole number with an inclusive lower bound
.pbi_refresh_whole_number <- function(value, name, minimum) {
  .pbi_refresh_number(value, name, minimum)
  if (value != floor(value) || value > .Machine$integer.max) {
    .fabric_abort(paste0(
      name,
      " must be a whole number within R's integer range"
    ))
  }
  invisible(value)
}

# Raise a typed condition for a terminal service failure
.pbi_refresh_abort_terminal <- function(detail, refresh) {
  class_name <- switch(
    detail$state,
    Failed = "fabric_pbi_refresh_failed",
    TimedOut = "fabric_pbi_refresh_service_timeout",
    Cancelled = "fabric_pbi_refresh_cancelled",
    Disabled = "fabric_pbi_refresh_disabled",
    "fabric_pbi_refresh_terminal_error"
  )
  message <- paste0(
    "Power BI refresh ",
    refresh$id,
    " finished with state ",
    detail$state
  )
  service_message <- detail$service_error$errorDescription %||%
    detail$service_error$message
  if (
    is.character(service_message) &&
      length(service_message) == 1L &&
      nzchar(service_message)
  ) {
    message <- paste0(message, ": ", service_message)
  }
  .fabric_abort(
    message,
    class = c(class_name, "fabric_pbi_refresh_error"),
    refresh = refresh,
    refresh_status = detail
  )
}
