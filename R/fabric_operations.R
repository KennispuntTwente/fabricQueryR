.fabric_operation_active_states <- c("NotStarted", "Running")
.fabric_operation_poll_fallback <- 2

#' Monitor Microsoft Fabric long-running operations
#'
#' Check, wait for, and retrieve the result of a Fabric operation that continues
#' after its initiating request returns. Pass the operation handle returned by a
#' fabricQueryR function when possible. To resume work later, pass either the
#' operation ID or the `Location` URL returned by Fabric
#'
#' @param operation A `fabric_operation` handle, Fabric operation GUID, or
#'   operation state/result URL returned in a `Location` header
#' @param tenant_id Entra tenant ID. Defaults to `FABRICQUERYR_TENANT_ID`
#' @param client_id Entra application ID. Defaults to
#'   `FABRICQUERYR_CLIENT_ID`, then the Azure CLI application ID
#' @param token Optional access token or token-provider function. Leave `NULL`
#'   to let fabricQueryR use its normal sign-in flow. A `fabric_operation`
#'   handle reuses its stored credential unless authentication arguments are
#'   supplied explicitly
#' @param auth_args Additional sign-in options passed to
#'   [AzureAuth::get_azure_token()] when no token source is supplied
#' @param api_base Fabric REST API base URL. Most users should keep the default
#' @param allow_custom_endpoint Logical. Set to `TRUE` only when `api_base` or
#'   the supplied operation URL is a non-Microsoft HTTPS origin that you trust
#'   to receive a Fabric token
#' @param respect_retry_after Whether to wait until Fabric's recommended next
#'   status-check time before making the request
#' @param .sleep,.now Internal hooks for deterministic tests
#'
#' @section Typical workflow:
#' A package function that starts asynchronous work may return a
#' `fabric_operation` handle. Use `fabric_operation_wait()` to wait for it to
#' finish and `fabric_operation_result()` to retrieve its output. Result
#' retrieval waits by default, so it is enough for the common case
#'
#' If the R process restarts, save the handle's `id` or the service-provided
#' `location` and pass that value with fresh authentication arguments
#'
#' @section Results and failures:
#' `fabric_operation_status()` preserves Fabric's status, progress, timestamps,
#' request identifiers, and structured error. Status values added by Fabric in
#' the future remain inspectable, but `fabric_operation_wait()` stops with a
#' typed error instead of polling an unfamiliar value indefinitely
#'
#' Some workload APIs, including Lakehouse table loading, expose completion in
#' their state response and do not provide a separate `/result` resource. For
#' those operations, `fabric_operation_result()` returns the terminal state
#' payload as its `value`
#'
#' A failed operation raises `fabric_operation_failed` by default. A timeout
#' raises `fabric_operation_timeout`; neither condition repeats the request that
#' originally started the operation
#'
#' @section Regional operation endpoints:
#' Fabric can return a `Location` on a regional `*.analysis.windows.net`
#' cluster. fabricQueryR recognizes those Microsoft endpoints and automatically
#' uses the Power BI token audience they require. Normal automatic sign-in or an
#' audience-aware token-provider function handles both audiences. A single
#' static Fabric bearer token cannot authenticate a regional operation URL
#'
#' @references
#' [Get operation state](https://learn.microsoft.com/en-us/rest/api/fabric/core/long-running-operations/get-operation-state)
#'
#' [Get operation result](https://learn.microsoft.com/en-us/rest/api/fabric/core/long-running-operations/get-operation-result)
#'
#' [Regional Fabric LRO authentication example](https://learn.microsoft.com/en-us/fabric/real-time-intelligence/map/tutorial-create-fabric-map-python)
#'
#' @return `fabric_operation_status()` and `fabric_operation_wait()` return a
#'   `fabric_operation_state` record. `fabric_operation_result()` returns a
#'   `fabric_operation_result` with `value`, `content_type`, `empty`, HTTP and
#'   request identifiers, and the reusable operation handle. JSON results are
#'   decoded as lists, binary results are raw vectors, and empty results have a
#'   `NULL` value
#' @examples
#' \dontrun{
#' state <- fabric_operation_status(
#'   "00000000-0000-0000-0000-000000000000"
#' )
#' completed <- fabric_operation_wait(state$operation, timeout = 900)
#' result <- fabric_operation_result(completed$operation)
#' result$value
#' }
#' @export
fabric_operation_status <- function(
  operation,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  token = NULL,
  auth_args = list(),
  api_base = .fabric_api_base,
  allow_custom_endpoint = FALSE,
  respect_retry_after = TRUE,
  .sleep = Sys.sleep,
  .now = Sys.time
) {
  # 1 Resolve the operation context ----------------------------------------------------------------

  # A handle can reuse its endpoint and credential, while a saved ID or URL
  # needs those details reconstructed from the caller's current sign-in

  .fabric_operation_logical(respect_retry_after, "respect_retry_after")
  context <- .fabric_operation_context(
    operation = operation,
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args,
    api_base = api_base,
    allow_custom_endpoint = allow_custom_endpoint,
    api_base_supplied = !missing(api_base),
    override_auth = !missing(tenant_id) ||
      !missing(client_id) ||
      !is.null(token) ||
      length(auth_args) > 0L
  )

  # 2 Read the current state -----------------------------------------------------------------------

  # The shared reader also handles already-completed immediate responses, so
  # callers see the same state shape for synchronous and asynchronous work

  .fabric_operation_read_state(
    context$operation,
    context$credential,
    respect_retry_after = respect_retry_after,
    .sleep = .sleep,
    .now = .now
  )
}

#' @param poll_interval Minimum seconds between status requests. `NULL` honors
#'   Fabric's `Retry-After` value and otherwise uses a two-second fallback
#' @param timeout Maximum total seconds to wait, including status requests
#' @param error_on_failure Whether a failed operation should raise a
#'   `fabric_operation_failed` condition. Set to `FALSE` to inspect the returned
#'   failed state directly
#' @rdname fabric_operation_status
#' @export
fabric_operation_wait <- function(
  operation,
  poll_interval = NULL,
  timeout = 300,
  error_on_failure = TRUE,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  token = NULL,
  auth_args = list(),
  api_base = .fabric_api_base,
  allow_custom_endpoint = FALSE,
  .sleep = Sys.sleep,
  .now = Sys.time
) {
  # 1 Validate waiting limits ----------------------------------------------------------------------

  # Validate before authentication or network work so invalid limits cannot
  # leave the caller with an operation whose behavior is unclear

  .fabric_operation_poll_interval(poll_interval)
  .fabric_operation_timeout(timeout)
  .fabric_operation_logical(error_on_failure, "error_on_failure")

  # 2 Resolve the operation context ----------------------------------------------------------------

  # Resolve authentication once, then reuse it for every idempotent status call

  context <- .fabric_operation_context(
    operation = operation,
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args,
    api_base = api_base,
    allow_custom_endpoint = allow_custom_endpoint,
    api_base_supplied = !missing(api_base),
    override_auth = !missing(tenant_id) ||
      !missing(client_id) ||
      !is.null(token) ||
      length(auth_args) > 0L
  )

  # 3 Wait for a terminal state --------------------------------------------------------------------

  # Only the operation-state GET is repeated; the initiating request is not
  # retained in the handle and cannot be replayed by this polling loop

  deadline <- .now() + timeout
  .fabric_operation_wait_context(
    context,
    poll_interval = poll_interval,
    deadline = deadline,
    error_on_failure = error_on_failure,
    .sleep = .sleep,
    .now = .now
  )
}

#' @param wait Whether to wait for a running operation. When `FALSE`, a
#'   non-terminal operation raises `fabric_operation_not_ready`
#' @rdname fabric_operation_status
#' @export
fabric_operation_result <- function(
  operation,
  wait = TRUE,
  poll_interval = NULL,
  timeout = 300,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  token = NULL,
  auth_args = list(),
  api_base = .fabric_api_base,
  allow_custom_endpoint = FALSE,
  .sleep = Sys.sleep,
  .now = Sys.time
) {
  # 1 Validate result options ----------------------------------------------------------------------

  # Apply the same limits as wait() because result retrieval may include the
  # complete polling phase before the final result request

  .fabric_operation_logical(wait, "wait")
  .fabric_operation_poll_interval(poll_interval)
  .fabric_operation_timeout(timeout)

  # 2 Resolve the operation context ----------------------------------------------------------------

  # Immediate responses carry a cached, credential-free value, while a
  # long-running operation needs authentication for state and result requests

  context <- .fabric_operation_context(
    operation = operation,
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args,
    api_base = api_base,
    allow_custom_endpoint = allow_custom_endpoint,
    api_base_supplied = !missing(api_base),
    override_auth = !missing(tenant_id) ||
      !missing(client_id) ||
      !is.null(token) ||
      length(auth_args) > 0L
  )
  if (isTRUE(context$operation$immediate)) {
    return(.fabric_operation_result_object(
      context$operation$.result,
      context$operation
    ))
  }

  # 3 Reach a successful state ---------------------------------------------------------------------

  # Fabric documents the result endpoint for successful operations only, so a
  # running or failed state must be handled before retrieving the result

  deadline <- .now() + timeout
  state <- if (isTRUE(wait)) {
    .fabric_operation_wait_context(
      context,
      poll_interval = poll_interval,
      deadline = deadline,
      error_on_failure = TRUE,
      .sleep = .sleep,
      .now = .now
    )
  } else {
    .fabric_operation_read_state(
      context$operation,
      context$credential,
      respect_retry_after = TRUE,
      deadline = deadline,
      .sleep = .sleep,
      .now = .now
    )
  }

  if (!identical(state$status, "Succeeded")) {
    if (identical(state$status, "Failed")) {
      .fabric_operation_abort_failure(state)
    }
    .fabric_abort(
      paste0(
        "Fabric operation ",
        state$id %||% "<unknown>",
        " is not ready for result retrieval (status ",
        state$status,
        ")"
      ),
      class = c("fabric_operation_not_ready", "fabric_operation_error"),
      operation_status = state
    )
  }

  # 4 Retrieve and normalize the result ------------------------------------------------------------

  # The result body may be JSON, binary, or empty; all three become the same
  # stable result envelope used for an immediate 200 or 201 response

  if (is.null(state$operation$result_url)) {
    return(.fabric_operation_result_object(
      list(
        value = state$raw,
        content_type = "application/json",
        empty = FALSE,
        status_code = 200L,
        request_id = state$request_id,
        activity_id = state$activity_id,
        completed_at = state$last_updated_time %||% .now()
      ),
      state$operation
    ))
  }

  response <- .httr2_perform(
    httr2::request(state$operation$result_url),
    credential = context$credential,
    audience = .fabric_operation_audience(state$operation$result_url),
    idempotent = TRUE,
    deadline = deadline,
    .sleep = .sleep,
    .now = .now
  )
  if (httr2::resp_status(response) != 200L) {
    .fabric_operation_abort_protocol(
      paste0(
        "Fabric returned HTTP ",
        httr2::resp_status(response),
        " from the operation result endpoint"
      ),
      operation = state$operation,
      response = response
    )
  }
  cached <- .fabric_operation_decode_result(response, .now = .now)
  .fabric_operation_result_object(cached, state$operation)
}

# Submit one request that may follow Fabric's long-running-operation protocol
# Returns a handle for either an immediate response or an asynchronous operation
.fabric_operation_submit <- function(
  request,
  credential,
  api_base = NULL,
  allow_custom_endpoint = FALSE,
  idempotent = FALSE,
  .now = Sys.time
) {
  # 1 Validate the initiating request --------------------------------------------------------------

  # Derive a trusted API base from the request unless its caller already has a
  # normalized base, which is needed when only an operation ID is returned

  .fabric_operation_logical(idempotent, "idempotent")
  if (!inherits(request, "httr2_request")) {
    .fabric_abort("`request` must be an httr2 request")
  }
  base <- if (is.null(api_base)) {
    .fabric_operation_api_base(request$url, allow_custom_endpoint)
  } else {
    fabric_api_base(api_base, allow_custom_endpoint)
  }

  # 2 Perform the initiating request once ----------------------------------------------------------

  # Non-idempotent initiation gets exactly one transport attempt. Later polling
  # repeats only safe GET requests and never retains this request for replay

  response <- .httr2_perform(
    request,
    credential = credential,
    audience = .fabric_audience$fabric,
    idempotent = idempotent,
    max_tries = if (isTRUE(idempotent)) 4L else 1L
  )
  status <- httr2::resp_status(response)
  if (!status %in% c(200L, 201L, 202L)) {
    .fabric_operation_abort_protocol(
      paste0(
        "Fabric operation initiation returned unsupported HTTP status ",
        status
      ),
      response = response
    )
  }

  # 3 Normalize immediate completion ---------------------------------------------------------------

  # Cache only the decoded value and safe metadata, never the authenticated
  # response object that may retain request headers

  submitted_at <- .now()
  credential_reference <- .fabric_operation_credential_reference(credential)
  if (status %in% c(200L, 201L)) {
    return(structure(
      list(
        id = NULL,
        location = httr2::resp_header(response, "location"),
        status_url = NULL,
        result_url = NULL,
        retry_after = NULL,
        submitted_at = submitted_at,
        next_poll_at = NULL,
        api_base = base,
        allow_custom_endpoint = allow_custom_endpoint,
        immediate = TRUE,
        .result = .fabric_operation_decode_result(response, .now = .now),
        credential = credential_reference$reference,
        .credential_key = credential_reference$key,
        .credential_required = !is.null(credential)
      ),
      class = "fabric_operation"
    ))
  }

  # 4 Build an asynchronous operation handle -------------------------------------------------------

  # Either documented header can identify the operation. When both are present
  # they must agree so the client never polls or retrieves a different resource

  operation_id <- httr2::resp_header(response, "x-ms-operation-id")
  location <- httr2::resp_header(response, "location")
  urls <- .fabric_operation_urls(
    operation_id = operation_id,
    location = location,
    current_url = request$url,
    api_base = base,
    allow_custom_endpoint = allow_custom_endpoint
  )
  retry_after <- .httr2_retry_after(response)
  structure(
    list(
      id = urls$id,
      location = urls$location,
      status_url = urls$status_url,
      result_url = urls$result_url,
      retry_after = retry_after,
      submitted_at = submitted_at,
      next_poll_at = if (is.null(retry_after)) {
        submitted_at
      } else {
        submitted_at + retry_after
      },
      api_base = base,
      allow_custom_endpoint = allow_custom_endpoint,
      immediate = FALSE,
      .result = NULL,
      credential = credential_reference$reference,
      .credential_key = credential_reference$key,
      .credential_required = !is.null(credential)
    ),
    class = "fabric_operation"
  )
}

# Submit and finish one Fabric operation from an httr2 request
# Returns the same result envelope for immediate and asynchronous completion
.fabric_operation_perform <- function(
  request,
  credential,
  api_base = NULL,
  allow_custom_endpoint = FALSE,
  idempotent = FALSE,
  poll_interval = NULL,
  timeout = 300,
  .sleep = Sys.sleep,
  .now = Sys.time
) {
  operation <- .fabric_operation_submit(
    request,
    credential,
    api_base = api_base,
    allow_custom_endpoint = allow_custom_endpoint,
    idempotent = idempotent,
    .now = .now
  )
  fabric_operation_result(
    operation,
    poll_interval = poll_interval,
    timeout = timeout,
    .sleep = .sleep,
    .now = .now
  )
}

# Wait using one resolved operation and credential
# Returns the terminal state for public wait and result workflows
.fabric_operation_wait_context <- function(
  context,
  poll_interval,
  deadline,
  error_on_failure,
  .sleep,
  .now
) {
  operation <- context$operation
  last_state <- NULL
  progress <- .fabric_poll_progress("Fabric operation", operation$id)

  repeat {
    if (.now() >= deadline) {
      .fabric_operation_abort_timeout(operation, last_state)
    }
    state <- .fabric_operation_read_state(
      operation,
      context$credential,
      respect_retry_after = TRUE,
      deadline = deadline,
      .sleep = .sleep,
      .now = .now
    )
    operation <- state$operation
    last_state <- state
    .fabric_poll_progress_update(progress, state$status)

    if (identical(state$status, "Succeeded")) {
      .fabric_poll_progress_done(progress)
      return(state)
    }
    if (identical(state$status, "Failed")) {
      if (isTRUE(error_on_failure)) {
        .fabric_operation_abort_failure(state)
      }
      .fabric_poll_progress_done(progress)
      return(state)
    }
    if (!state$status %in% .fabric_operation_active_states) {
      .fabric_abort(
        paste0(
          "Fabric operation ",
          state$id %||% "<unknown>",
          " returned an unfamiliar wait status: ",
          state$status
        ),
        class = c(
          "fabric_operation_unknown_status",
          "fabric_operation_protocol_error",
          "fabric_operation_error"
        ),
        operation_status = state
      )
    }

    delay <- max(
      state$retry_after %||% .fabric_operation_poll_fallback,
      poll_interval %||% 0
    )
    remaining <- as.numeric(difftime(deadline, .now(), units = "secs"))
    if (!is.finite(remaining) || remaining <= 0 || delay >= remaining) {
      .fabric_operation_abort_timeout(operation, last_state)
    }
    .sleep(delay)

    # The retry delay has now been consumed, so the next reader must not sleep
    # for the same service hint a second time
    operation$next_poll_at <- NULL
  }
}

# Read one state response or synthesize the state of an immediate response
# Returns a fabric_operation_state with an updated reusable handle
.fabric_operation_read_state <- function(
  operation,
  credential,
  respect_retry_after,
  deadline = NULL,
  .sleep,
  .now
) {
  if (isTRUE(operation$immediate)) {
    completed_at <- operation$.result$completed_at %||% operation$submitted_at
    return(structure(
      list(
        id = operation$id,
        status = "Succeeded",
        percent_complete = 100,
        created_time = operation$submitted_at,
        last_updated_time = completed_at,
        error = NULL,
        retry_after = NULL,
        location = operation$location,
        request_id = operation$.result$request_id,
        activity_id = operation$.result$activity_id,
        raw = list(status = "Succeeded", percentComplete = 100),
        operation = operation
      ),
      class = "fabric_operation_state"
    ))
  }

  if (isTRUE(respect_retry_after) && !is.null(operation$next_poll_at)) {
    delay <- as.numeric(difftime(
      operation$next_poll_at,
      .now(),
      units = "secs"
    ))
    if (is.finite(delay) && delay > 0) {
      if (!is.null(deadline)) {
        remaining <- as.numeric(difftime(deadline, .now(), units = "secs"))
        if (delay >= remaining) {
          .fabric_operation_abort_timeout(operation, NULL)
        }
      }
      .sleep(delay)
    }
  }

  response <- .httr2_perform(
    httr2::request(operation$status_url),
    credential = credential,
    audience = .fabric_operation_audience(operation$status_url),
    idempotent = TRUE,
    deadline = deadline,
    .sleep = .sleep,
    .now = .now
  )
  status_code <- httr2::resp_status(response)
  if (!status_code %in% c(200L, 202L)) {
    .fabric_operation_abort_protocol(
      paste0(
        "Fabric returned HTTP ",
        status_code,
        " from the operation state endpoint"
      ),
      operation = operation,
      response = response
    )
  }
  operation <- .fabric_operation_update_urls(operation, response)
  retry_after <- .httr2_retry_after(response)
  operation$retry_after <- retry_after
  operation$next_poll_at <- if (is.null(retry_after)) {
    NULL
  } else {
    .now() + retry_after
  }
  body <- if (status_code == 202L && !length(response$body %||% base::raw())) {
    list(status = "Running")
  } else {
    .fabric_operation_json_body(response, "operation state")
  }
  .fabric_operation_state(body, operation, response, retry_after)
}

# Convert a decoded state response into the public operation-state record
# Returns progress, timestamps, identifiers, structured failure, and raw fields
.fabric_operation_state <- function(body, operation, response, retry_after) {
  status <- body$status %||% body$Status
  if (is.null(operation$result_url) && is.numeric(status)) {
    numeric_status <- c(
      "1" = "NotStarted",
      "2" = "Running",
      "3" = "Succeeded",
      "4" = "Failed"
    )
    status_key <- if (
      length(status) == 1L &&
        !is.na(status) &&
        is.finite(status) &&
        status == floor(status)
    ) {
      as.character(as.integer(status))
    } else {
      ""
    }
    status_index <- match(status_key, names(numeric_status))
    status <- if (is.na(status_index)) {
      NULL
    } else {
      unname(numeric_status[[status_index]])
    }
  }
  if (
    !is.character(status) ||
      length(status) != 1L ||
      is.na(status) ||
      !nzchar(status)
  ) {
    .fabric_operation_abort_protocol(
      "Fabric operation state did not contain one non-empty status",
      operation = operation,
      response = response
    )
  }

  progress <- body$percentComplete %||% body$PercentComplete
  if (
    !is.null(progress) &&
      (!is.numeric(progress) ||
        length(progress) != 1L ||
        is.na(progress) ||
        !is.finite(progress) ||
        progress < 0 ||
        progress > 100)
  ) {
    .fabric_operation_abort_protocol(
      "Fabric operation state contained invalid percentComplete",
      operation = operation,
      response = response
    )
  }

  safe_body <- .httr2_redact_object(body)
  safe_error <- safe_body$error %||% safe_body$Error
  error <- if (is.list(safe_error)) safe_error else NULL
  request_id <- httr2::resp_header(response, "x-ms-request-id") %||%
    httr2::resp_header(response, "request-id") %||%
    error$requestId
  activity_id <- httr2::resp_header(response, "x-ms-activity-id") %||%
    httr2::resp_header(response, "activity-id")
  created_time <- body$createdTimeUtc %||% body$CreatedTimeUtc
  updated_time <- body$lastUpdatedTimeUtc %||% body$LastUpdatedTimeUtc
  if (is.null(operation$result_url)) {
    if (identical(created_time, "")) {
      created_time <- NULL
    }
    if (identical(updated_time, "")) updated_time <- NULL
  }
  structure(
    list(
      id = operation$id,
      status = status,
      percent_complete = progress,
      created_time = .fabric_operation_time(
        created_time,
        "createdTimeUtc",
        operation
      ),
      last_updated_time = .fabric_operation_time(
        updated_time,
        "lastUpdatedTimeUtc",
        operation
      ),
      error = error,
      retry_after = retry_after,
      location = httr2::resp_header(response, "location") %||%
        operation$location,
      request_id = request_id,
      activity_id = activity_id,
      raw = safe_body,
      operation = operation
    ),
    class = "fabric_operation_state"
  )
}

# Resolve a handle, operation ID, or service-provided URL into request context
# Returns a validated operation handle and its in-process or replacement credential
.fabric_operation_context <- function(
  operation,
  tenant_id,
  client_id,
  token,
  auth_args,
  api_base,
  allow_custom_endpoint,
  api_base_supplied,
  override_auth
) {
  .fabric_operation_logical(allow_custom_endpoint, "allow_custom_endpoint")
  if (inherits(operation, "fabric_operation_state")) {
    operation <- operation$operation
  }
  if (inherits(operation, "fabric_operation_result")) {
    operation <- operation$operation
  }

  if (inherits(operation, "fabric_operation")) {
    credential <- if (isTRUE(operation$immediate) && !isTRUE(override_auth)) {
      NULL
    } else if (isTRUE(override_auth)) {
      fabric_credential(
        tenant_id = tenant_id,
        client_id = client_id,
        token = token,
        auth_args = auth_args
      )
    } else {
      .fabric_operation_credential(operation)
    }
    handle <- operation
    custom <- operation$allow_custom_endpoint %||% allow_custom_endpoint
    handle$api_base <- fabric_api_base(
      operation$api_base %||% api_base,
      custom
    )
    if (isTRUE(override_auth)) {
      reference <- .fabric_operation_credential_reference(credential)
      handle$credential <- reference$reference
      handle$.credential_key <- reference$key
      handle$.credential_required <- !is.null(credential)
    }
    return(list(operation = handle, credential = credential))
  }

  if (
    !is.character(operation) ||
      length(operation) != 1L ||
      is.na(operation) ||
      !nzchar(operation)
  ) {
    .fabric_abort(
      "`operation` must be a fabric_operation, operation GUID, or Location URL"
    )
  }
  credential <- fabric_credential(
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args
  )

  is_id <- fabric_is_guid(operation)
  base <- if (!is_id && !isTRUE(api_base_supplied)) {
    .fabric_operation_api_base(operation, allow_custom_endpoint)
  } else {
    fabric_api_base(api_base, allow_custom_endpoint)
  }
  urls <- if (is_id) {
    list(
      id = operation,
      location = NULL,
      status_url = paste0(base, "/operations/", operation),
      result_url = paste0(base, "/operations/", operation, "/result")
    )
  } else {
    .fabric_operation_urls(
      operation_id = NULL,
      location = operation,
      current_url = base,
      api_base = base,
      allow_custom_endpoint = allow_custom_endpoint
    )
  }
  reference <- .fabric_operation_credential_reference(credential)
  handle <- structure(
    list(
      id = urls$id,
      location = urls$location,
      status_url = urls$status_url,
      result_url = urls$result_url,
      retry_after = NULL,
      submitted_at = NULL,
      next_poll_at = NULL,
      api_base = base,
      allow_custom_endpoint = allow_custom_endpoint,
      immediate = FALSE,
      .result = NULL,
      credential = reference$reference,
      .credential_key = reference$key,
      .credential_required = TRUE
    ),
    class = "fabric_operation"
  )
  list(operation = handle, credential = credential)
}

# Read and validate the operation headers used to build state and result URLs
# Returns one operation ID plus distinct trusted state and result endpoints
.fabric_operation_urls <- function(
  operation_id,
  location,
  current_url,
  api_base,
  allow_custom_endpoint
) {
  if (!is.null(operation_id)) {
    operation_id <- trimws(operation_id)
    if (!fabric_is_guid(operation_id)) {
      .fabric_operation_abort_protocol(
        "Fabric returned an invalid x-ms-operation-id header"
      )
    }
  }
  parsed <- if (is.null(location) || !nzchar(location)) {
    NULL
  } else {
    .fabric_operation_parse_location(
      location,
      current_url,
      api_base,
      allow_custom_endpoint
    )
  }
  id <- operation_id %||% parsed$id
  if (is.null(id)) {
    .fabric_operation_abort_protocol(
      paste0(
        "Fabric accepted the operation but returned neither a valid ",
        "Location nor x-ms-operation-id header"
      )
    )
  }
  if (!is.null(parsed) && !identical(tolower(parsed$id), tolower(id))) {
    .fabric_operation_abort_protocol(
      "Fabric returned different operation IDs in Location and x-ms-operation-id"
    )
  }

  default_status <- paste0(api_base, "/operations/", id)
  status_url <- if (!is.null(parsed) && !isTRUE(parsed$is_result)) {
    parsed$url
  } else if (!is.null(parsed)) {
    sub("/result/?$", "", parsed$url, ignore.case = TRUE)
  } else {
    default_status
  }
  result_url <- if (!is.null(parsed) && identical(parsed$kind, "state_only")) {
    NULL
  } else if (!is.null(parsed) && isTRUE(parsed$is_result)) {
    parsed$url
  } else {
    paste0(sub("/+$", "", status_url), "/result")
  }
  list(
    id = id,
    location = if (is.null(parsed)) location else parsed$url,
    status_url = status_url,
    result_url = result_url
  )
}

# Validate one service Location against the Fabric operation route and host
# Returns its absolute URL, embedded operation ID, and whether it is a result URL
.fabric_operation_parse_location <- function(
  location,
  current_url,
  api_base,
  allow_custom_endpoint
) {
  candidate <- try(
    httr2::url_modify_relative(current_url, location),
    silent = TRUE
  )
  parsed <- if (inherits(candidate, "try-error")) {
    candidate
  } else {
    try(httr2::url_parse(candidate), silent = TRUE)
  }
  base <- httr2::url_parse(api_base)
  host <- if (inherits(parsed, "try-error")) "" else parsed$hostname %||% ""
  official <- fabric_host_matches(host, "api.fabric.microsoft.com") ||
    fabric_host_matches(host, "analysis.windows.net")
  same_custom_origin <- !inherits(parsed, "try-error") &&
    identical(tolower(parsed$scheme %||% ""), tolower(base$scheme %||% "")) &&
    identical(tolower(host), tolower(base$hostname %||% "")) &&
    identical(parsed$port %||% "", base$port %||% "")
  guid <- "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
  core_route <- if (inherits(parsed, "try-error")) {
    NULL
  } else {
    regexec(
      paste0("^/(?:v1/)?operations/(", guid, ")(/result)?/?$"),
      parsed$path %||% "",
      ignore.case = TRUE
    )
  }
  core_match <- if (is.null(core_route)) {
    character()
  } else {
    regmatches(parsed$path, core_route)[[1L]]
  }
  lakehouse_route <- if (inherits(parsed, "try-error")) {
    NULL
  } else {
    regexec(
      paste0(
        "^/(?:v1/)?workspaces/",
        guid,
        "/lakehouses/",
        guid,
        "/operations/(",
        guid,
        ")/?$"
      ),
      parsed$path %||% "",
      ignore.case = TRUE
    )
  }
  lakehouse_match <- if (is.null(lakehouse_route)) {
    character()
  } else {
    regmatches(parsed$path, lakehouse_route)[[1L]]
  }
  match <- if (length(core_match) >= 2L) core_match else lakehouse_match
  kind <- if (length(core_match) >= 2L) "core" else "state_only"
  valid <- !inherits(parsed, "try-error") &&
    identical(tolower(parsed$scheme %||% ""), "https") &&
    nzchar(host) &&
    !nzchar(parsed$username %||% "") &&
    !nzchar(parsed$password %||% "") &&
    (parsed$port %||% "") %in% c("", "443") &&
    length(parsed$query %||% list()) == 0L &&
    !nzchar(parsed$fragment %||% "") &&
    length(match) >= 2L &&
    (official || (isTRUE(allow_custom_endpoint) && same_custom_origin))
  if (!valid) {
    .fabric_operation_abort_protocol(
      "Fabric returned an invalid or untrusted operation Location header"
    )
  }
  list(
    url = candidate,
    id = match[[2L]],
    is_result = identical(kind, "core") &&
      length(match) >= 3L &&
      nzchar(match[[3L]]),
    kind = kind
  )
}

# Apply a status response Location without confusing state and result endpoints
# Returns an updated operation handle while retaining its validated identity
.fabric_operation_update_urls <- function(operation, response) {
  location <- httr2::resp_header(response, "location")
  operation_id <- httr2::resp_header(response, "x-ms-operation-id") %||%
    operation$id
  if (is.null(location) || !nzchar(location)) {
    if (!identical(tolower(operation_id), tolower(operation$id))) {
      .fabric_operation_abort_protocol(
        "Fabric changed x-ms-operation-id while polling an operation",
        operation = operation,
        response = response
      )
    }
    return(operation)
  }
  urls <- .fabric_operation_urls(
    operation_id = operation_id,
    location = location,
    current_url = operation$status_url,
    api_base = operation$api_base,
    allow_custom_endpoint = operation$allow_custom_endpoint
  )
  if (!identical(tolower(urls$id), tolower(operation$id))) {
    .fabric_operation_abort_protocol(
      "Fabric changed the operation ID while polling",
      operation = operation,
      response = response
    )
  }
  operation$location <- urls$location
  operation$status_url <- urls$status_url
  operation$result_url <- urls$result_url
  operation
}

# Derive and validate an API base from a complete Fabric request or Location URL
# Returns an HTTPS v1 base suitable for ID-only operation endpoints
.fabric_operation_api_base <- function(url, allow_custom_endpoint) {
  parsed <- try(httr2::url_parse(url), silent = TRUE)
  if (inherits(parsed, "try-error")) {
    .fabric_abort("The operation URL is not a valid URL")
  }
  if (fabric_host_matches(parsed$hostname, "analysis.windows.net")) {
    return(.fabric_api_base)
  }
  port <- parsed$port %||% ""
  origin <- paste0(
    parsed$scheme,
    "://",
    parsed$hostname,
    if (nzchar(port)) paste0(":", port) else ""
  )
  fabric_api_base(origin, allow_custom_endpoint)
}

# Choose the token audience required by one validated operation endpoint
# Returns the Power BI audience for regional clusters and Fabric otherwise
.fabric_operation_audience <- function(url) {
  parsed <- httr2::url_parse(url)
  if (fabric_host_matches(parsed$hostname, "analysis.windows.net")) {
    .fabric_audience$power_bi
  } else {
    .fabric_audience$fabric
  }
}

# Decode one JSON state body and convert malformed payloads to a protocol error
# Returns the named list required by state normalization
.fabric_operation_json_body <- function(response, purpose) {
  body <- try(
    httr2::resp_body_json(
      response,
      simplifyVector = FALSE,
      bigint_as_char = TRUE
    ),
    silent = TRUE
  )
  if (inherits(body, "try-error") || !is.list(body)) {
    .fabric_operation_abort_protocol(
      paste0("Fabric returned malformed JSON for the ", purpose),
      response = response
    )
  }
  body
}

# Decode an immediate or asynchronous result response without retaining secrets
# Returns cached value and response metadata used by the public result envelope
.fabric_operation_decode_result <- function(response, .now = Sys.time) {
  raw <- if (is.null(response$body) || !length(response$body)) {
    base::raw()
  } else {
    httr2::resp_body_raw(response)
  }
  content_type <- httr2::resp_content_type(response)
  value <- if (!length(raw)) {
    NULL
  } else if (
    !is.null(content_type) && grepl("json", content_type, ignore.case = TRUE)
  ) {
    parsed <- try(
      httr2::resp_body_json(
        response,
        simplifyVector = FALSE,
        bigint_as_char = TRUE
      ),
      silent = TRUE
    )
    if (inherits(parsed, "try-error")) {
      .fabric_operation_abort_protocol(
        "Fabric returned malformed JSON for the operation result",
        response = response
      )
    }
    parsed
  } else {
    raw
  }
  list(
    value = value,
    content_type = content_type,
    empty = !length(raw),
    status_code = httr2::resp_status(response),
    request_id = httr2::resp_header(response, "x-ms-request-id") %||%
      httr2::resp_header(response, "request-id"),
    activity_id = httr2::resp_header(response, "x-ms-activity-id") %||%
      httr2::resp_header(response, "activity-id"),
    completed_at = .now()
  )
}

# Attach one reusable handle to cached result fields
# Returns the stable caller-facing result for every completion mode and body type
.fabric_operation_result_object <- function(cached, operation) {
  structure(
    list(
      value = cached$value,
      content_type = cached$content_type,
      empty = cached$empty,
      status_code = cached$status_code,
      request_id = cached$request_id,
      activity_id = cached$activity_id,
      operation_id = operation$id,
      operation = operation
    ),
    class = "fabric_operation_result"
  )
}

# Store a credential behind a weak reference for the in-process operation handle
# Returns the reference plus its lifetime key without making secrets serializable
.fabric_operation_credential_reference <- function(credential) {
  if (is.null(credential)) {
    return(list(reference = NULL, key = NULL))
  }
  key <- new.env(parent = emptyenv())
  list(
    reference = rlang::new_weakref(key, credential),
    key = key
  )
}

# Resolve an operation handle's credential or explain how to replace a lost one
# Returns a fabric credential or NULL for deliberately unauthenticated test handles
.fabric_operation_credential <- function(operation) {
  if (!isTRUE(operation$.credential_required)) {
    return(NULL)
  }
  stored <- operation$credential
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
        "This Fabric operation handle no longer has an in-process credential; ",
        "supply `token`, `tenant_id`, or other authentication arguments"
      ),
      class = c("fabric_operation_credential_error", "fabric_operation_error")
    )
  }
  credential
}

# Parse one documented Fabric operation timestamp
# Returns UTC POSIXct, NULL when absent, or raises for a malformed service value
.fabric_operation_time <- function(value, field, operation) {
  if (is.null(value)) {
    return(NULL)
  }
  if (
    !is.character(value) ||
      length(value) != 1L ||
      is.na(value) ||
      !nzchar(value)
  ) {
    .fabric_operation_abort_protocol(
      paste0("Fabric operation state contained invalid ", field),
      operation = operation
    )
  }
  parsed <- .fabric_job_time(value)
  if (is.na(parsed)) {
    .fabric_operation_abort_protocol(
      paste0("Fabric operation state contained invalid ", field),
      operation = operation
    )
  }
  parsed
}

# Raise a typed failure using the service's structured error and identifiers
# This function does not return and keeps the last state available to the caller
.fabric_operation_abort_failure <- function(state) {
  detail <- .fabric_job_failure_text(state$error)
  .fabric_abort(
    paste0(
      "Fabric operation ",
      state$id %||% "<unknown>",
      " failed",
      if (nzchar(detail)) paste0(": ", detail) else ""
    ),
    class = c("fabric_operation_failed", "fabric_operation_error"),
    operation_status = state,
    operation_error = state$error,
    request_id = state$request_id,
    activity_id = state$activity_id
  )
}

# Raise a typed timeout with the latest known operation state
# This function does not return and never attempts cancellation or initiation
.fabric_operation_abort_timeout <- function(operation, state) {
  .fabric_abort(
    paste0(
      "Timed out waiting for Fabric operation ",
      operation$id %||% "<unknown>"
    ),
    class = c("fabric_operation_timeout", "fabric_operation_error"),
    operation = operation,
    operation_status = state
  )
}

# Raise a redacted protocol error for malformed long-running-operation responses
# This function does not return and attaches only safe response metadata
.fabric_operation_abort_protocol <- function(
  message,
  operation = NULL,
  response = NULL
) {
  .fabric_abort(
    message,
    class = c("fabric_operation_protocol_error", "fabric_operation_error"),
    operation = operation,
    response_metadata = if (is.null(response)) {
      NULL
    } else {
      .httr2_response_metadata(response)
    }
  )
}

# Validate a public operation flag
# Returns invisibly after requiring one non-missing logical value
.fabric_operation_logical <- function(value, name) {
  if (!is.logical(value) || length(value) != 1L || is.na(value)) {
    .fabric_abort(paste0("`", name, "` must be TRUE or FALSE"))
  }
  invisible(TRUE)
}

# Validate the optional minimum polling interval
# Returns invisibly for NULL or one finite non-negative number
.fabric_operation_poll_interval <- function(value) {
  if (
    !is.null(value) &&
      (!is.numeric(value) ||
        length(value) != 1L ||
        is.na(value) ||
        !is.finite(value) ||
        value < 0)
  ) {
    .fabric_abort("`poll_interval` must be NULL or one non-negative number")
  }
  invisible(TRUE)
}

# Validate the total operation waiting limit
# Returns invisibly after requiring one finite non-negative number of seconds
.fabric_operation_timeout <- function(value) {
  if (
    !is.numeric(value) ||
      length(value) != 1L ||
      is.na(value) ||
      !is.finite(value) ||
      value < 0
  ) {
    .fabric_abort("`timeout` must be one non-negative number")
  }
  invisible(TRUE)
}
