#' Discover and reattach to Microsoft Fabric Livy work
#'
#' Lists existing Livy sessions or batches and creates a newly authenticated
#' handle for work that was started by an earlier R process. Listing never
#' returns authentication credentials. Attaching retrieves current service
#' state and does not create a new session or batch.
#'
#' @param livy_url A copied session or batch connection URL, Livy API base URL,
#'   or enriched Lakehouse object from [fabric_lakehouses()] or [fabric_item()]
#' @param high_concurrency Whether to list or attach to high-concurrency
#'   sessions instead of regular sessions
#' @param top Maximum records requested for this page
#' @param skip Number of matching records to skip
#' @param count Whether Fabric should include the total matching record count
#' @param session_id,batch_id Service GUID returned by a list or submit operation
#' @param tenant_id Microsoft Entra tenant ID. Defaults to
#'   `FABRICQUERYR_TENANT_ID`
#' @param client_id Microsoft Entra application/client ID. Defaults to
#'   `FABRICQUERYR_CLIENT_ID`, then the Azure CLI application ID
#' @param token Optional access token or token-provider function. Leave `NULL`
#'   to use the normal sign-in flow for a Microsoft Fabric host. A custom
#'   `livy_url` requires an explicitly supplied token or provider
#' @param auth_args Additional sign-in options passed to
#'   [AzureAuth::get_azure_token()]
#' @param audience Optional sign-in scopes. Delegated sign-in defaults to the
#'   required Fabric Livy scopes; client credentials require one `.default`
#'   audience
#' @param verbose Logical. Show handle lifecycle messages
#'
#' @return `fabric_livy_sessions()` and `fabric_livy_batches()` return one page
#'   as a tibble with columns `id`, `name`, `state`, `result`, `app_id`, service
#'   timestamps, and `raw`. The tibble has `total_count`, `page_size`, and
#'   `skip` attributes. The attach functions return a [FabricLivySession] or
#'   [FabricLivyBatch] with a fresh in-process credential.
#'
#' @section Restart recovery:
#' Livy handles intentionally do not serialize their credentials. Store the
#' service ID, then call the corresponding attach function after restarting R.
#' Attaching only reconstructs the local handle; it never submits new Spark
#' work.
#'
#' @seealso
#' [Microsoft Fabric Livy API specification](https://github.com/microsoft/fabric-samples/blob/main/docs-samples/data-engineering/Livy-API-swagger/swagger.json)
#'
#' @examples
#' \dontrun{
#' workspace <- fabric_workspaces()[[1L]]
#' lakehouse <- fabric_lakehouses(workspace)[[1L]]
#'
#' sessions <- fabric_livy_sessions(lakehouse)
#' session <- fabric_livy_session_attach(lakehouse, sessions$id[[1L]])
#' session$status()
#'
#' batches <- fabric_livy_batches(lakehouse)
#' batch <- fabric_livy_batch_attach(lakehouse, batches$id[[1L]])
#' batch$status()
#' }
#'
#' @export
fabric_livy_sessions <- function(
  livy_url,
  high_concurrency = FALSE,
  top = 100L,
  skip = 0L,
  count = TRUE,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  token = NULL,
  auth_args = list(),
  audience = NULL
) {
  fabric_livy_check_flag(high_concurrency, "high_concurrency")
  type <- if (high_concurrency) {
    "highConcurrencySessions"
  } else {
    "sessions"
  }
  context <- fabric_livy_recovery_context(
    livy_url,
    type,
    tenant_id,
    client_id,
    token,
    auth_args,
    audience
  )
  fabric_livy_list(
    context$collection,
    context$credential,
    top,
    skip,
    count
  )
}

#' @rdname fabric_livy_sessions
#' @export
fabric_livy_batches <- function(
  livy_url,
  top = 100L,
  skip = 0L,
  count = TRUE,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  token = NULL,
  auth_args = list(),
  audience = NULL
) {
  context <- fabric_livy_recovery_context(
    livy_url,
    "batches",
    tenant_id,
    client_id,
    token,
    auth_args,
    audience
  )
  fabric_livy_list(
    context$collection,
    context$credential,
    top,
    skip,
    count
  )
}

#' @rdname fabric_livy_sessions
#' @export
fabric_livy_session_attach <- function(
  livy_url,
  session_id,
  high_concurrency = FALSE,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  token = NULL,
  auth_args = list(),
  audience = NULL,
  verbose = TRUE
) {
  fabric_livy_check_guid(session_id, "session_id", allow_null = FALSE)
  fabric_livy_check_flag(high_concurrency, "high_concurrency")
  fabric_livy_check_flag(verbose, "verbose")
  type <- if (high_concurrency) {
    "highConcurrencySessions"
  } else {
    "sessions"
  }
  context <- fabric_livy_recovery_context(
    livy_url,
    type,
    tenant_id,
    client_id,
    token,
    auth_args,
    audience
  )
  response <- fabric_livy_json(
    "GET",
    paste0(context$collection, "/", session_id),
    context$credential
  )
  fabric_livy_validate_attached_response(response, session_id, "session")
  FabricLivySession$new(
    livy_url = context$collection,
    credential = context$credential,
    payload = NULL,
    response = response,
    high_concurrency = high_concurrency,
    verbose = verbose
  )
}

#' @rdname fabric_livy_sessions
#' @export
fabric_livy_batch_attach <- function(
  livy_url,
  batch_id,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  token = NULL,
  auth_args = list(),
  audience = NULL,
  verbose = TRUE
) {
  fabric_livy_check_guid(batch_id, "batch_id", allow_null = FALSE)
  fabric_livy_check_flag(verbose, "verbose")
  context <- fabric_livy_recovery_context(
    livy_url,
    "batches",
    tenant_id,
    client_id,
    token,
    auth_args,
    audience
  )
  response <- fabric_livy_json(
    "GET",
    paste0(context$collection, "/", batch_id),
    context$credential
  )
  fabric_livy_validate_attached_response(response, batch_id, "batch")
  FabricLivyBatch$new(
    response = response,
    url = context$collection,
    credential = context$credential,
    verbose = verbose
  )
}

fabric_livy_recovery_context <- function(
  livy_url,
  type,
  tenant_id,
  client_id,
  token,
  auth_args,
  audience
) {
  endpoint <- fabric_livy_resolve_url(livy_url)
  fabric_require_explicit_custom_token(endpoint, token, "livy_url")
  credential <- fabric_livy_credential(
    tenant_id,
    client_id,
    token,
    auth_args,
    audience
  )
  list(
    collection = fabric_livy_endpoint(endpoint, type),
    credential = credential
  )
}

fabric_livy_list <- function(collection, credential, top, skip, count) {
  fabric_livy_check_integer(top, "top")
  fabric_livy_check_integer(skip, "skip", minimum = 0L)
  fabric_livy_check_flag(count, "count")
  response <- fabric_livy_json(
    "GET",
    collection,
    credential,
    query = list(
      `$top` = as.integer(top),
      `$skip` = as.integer(skip),
      `$count` = tolower(as.character(count))
    )
  )
  fabric_livy_list_result(response, skip)
}

fabric_livy_list_result <- function(response, skip) {
  if (
    !is.list(response) ||
      is.null(names(response))
  ) {
    .fabric_abort(
      "Livy returned a malformed activity collection",
      class = "fabric_livy_protocol_error"
    )
  }
  items <- response$items
  if (
    is.null(items) ||
      !is.list(items) ||
      !is.null(names(items)) ||
      !all(vapply(
        items,
        function(item) is.list(item) && !is.null(names(item)),
        logical(1)
      ))
  ) {
    .fabric_abort(
      "Livy returned a malformed activity collection",
      class = "fabric_livy_protocol_error"
    )
  }

  total_count <- fabric_livy_optional_count(
    response$totalCountOfMatchedItems,
    "totalCountOfMatchedItems"
  )
  page_size <- fabric_livy_optional_count(response$pageSize, "pageSize")
  ids <- vapply(items, fabric_livy_item_field, character(1), name = "id")
  if (anyNA(ids) || any(!nzchar(ids))) {
    .fabric_abort(
      "Livy returned an activity without a valid id",
      class = "fabric_livy_protocol_error"
    )
  }
  value <- tibble::tibble(
    id = ids,
    name = vapply(items, fabric_livy_item_field, character(1), name = "name"),
    state = vapply(items, fabric_livy_item_state, character(1)),
    result = vapply(
      items,
      fabric_livy_item_field,
      character(1),
      name = "result"
    ),
    app_id = vapply(
      items,
      fabric_livy_item_field,
      character(1),
      name = "appId"
    ),
    submitted_at = vapply(
      items,
      fabric_livy_item_field,
      character(1),
      name = "submittedAt"
    ),
    started_at = vapply(
      items,
      fabric_livy_item_field,
      character(1),
      name = "startedAt"
    ),
    ended_at = vapply(
      items,
      fabric_livy_item_field,
      character(1),
      name = "endedAt"
    ),
    raw = items
  )
  attr(value, "total_count") <- total_count
  attr(value, "page_size") <- page_size
  attr(value, "skip") <- as.integer(skip)
  value
}

fabric_livy_item_field <- function(item, name) {
  value <- item[[name]]
  if (is.null(value)) {
    return(NA_character_)
  }
  if (
    !(is.character(value) || is.numeric(value)) ||
      length(value) != 1L ||
      is.na(value) ||
      (is.numeric(value) && !is.finite(value))
  ) {
    .fabric_abort(
      paste0("Livy returned an invalid activity ", name),
      class = "fabric_livy_protocol_error"
    )
  }
  as.character(value)
}

fabric_livy_item_state <- function(item) {
  for (name in c("state", "schedulerState", "pluginState")) {
    value <- fabric_livy_item_field(item, name)
    if (!is.na(value)) {
      return(value)
    }
  }
  NA_character_
}

fabric_livy_optional_count <- function(value, name) {
  if (is.null(value)) {
    return(NA_integer_)
  }
  if (
    !is.numeric(value) ||
      length(value) != 1L ||
      is.na(value) ||
      !is.finite(value) ||
      value < 0 ||
      value > .Machine$integer.max ||
      value != trunc(value)
  ) {
    .fabric_abort(
      paste0("Livy returned an invalid ", name),
      class = "fabric_livy_protocol_error"
    )
  }
  as.integer(value)
}

fabric_livy_validate_attached_response <- function(response, id, kind) {
  if (
    !is.list(response) ||
      is.null(names(response)) ||
      !identical(as.character(response$id %||% ""), id)
  ) {
    .fabric_abort(
      paste0("Livy returned an invalid ", kind, " response for ", id),
      class = "fabric_livy_protocol_error"
    )
  }
  invisible(response)
}
