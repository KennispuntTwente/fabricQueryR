# Shared Fabric Livy helpers ------------------------------------------------

.fabric_livy_session_terminal_states <- c(
  "dead",
  "error",
  "failed",
  "killed",
  "shutting_down",
  "cancelled"
)

.fabric_livy_statement_failure_states <- c(
  "error",
  "cancelling",
  "cancelled"
)

.fabric_livy_batch_success_states <- c("success")

.fabric_livy_batch_failure_states <- c(
  "dead",
  "error",
  "failed",
  "killed",
  "cancelled"
)

fabric_livy_resolve_url <- function(livy_url) {
  discovered <- fabric_as_record(livy_url)
  if (!is.null(discovered)) {
    if (
      !identical(
        tolower(fabric_record_value(discovered, "type") %||% ""),
        "lakehouse"
      )
    ) {
      rlang::abort(
        "livy_url discovery record must be a Lakehouse item"
      )
    }
    livy_url <- fabric_record_value(discovered, "livy_url")
  }
  fabric_livy_check_string(livy_url, "livy_url")
  livy_url
}

fabric_livy_check_string <- function(value, name, allow_null = FALSE) {
  if (is.null(value) && isTRUE(allow_null)) {
    return(invisible(value))
  }
  if (
    !is.character(value) ||
      length(value) != 1L ||
      is.na(value) ||
      !nzchar(trimws(value))
  ) {
    rlang::abort(cli::format_inline("{name} must be one non-empty string"))
  }
  invisible(value)
}

fabric_livy_check_number <- function(value, name, minimum = 0) {
  if (
    length(value) != 1L ||
      is.na(value) ||
      !is.numeric(value) ||
      !is.finite(value) ||
      value < minimum
  ) {
    rlang::abort(paste0(
      name,
      " must be one finite number greater than or equal to ",
      minimum
    ))
  }
  invisible(value)
}

fabric_livy_check_flag <- function(value, name) {
  if (!is.logical(value) || length(value) != 1L || is.na(value)) {
    rlang::abort(cli::format_inline("{name} must be TRUE or FALSE"))
  }
  invisible(value)
}

fabric_livy_normalize_named_list <- function(value, name) {
  if (is.null(value)) {
    return(NULL)
  }
  if (!is.list(value) || is.null(names(value)) || !all(nzchar(names(value)))) {
    rlang::abort(cli::format_inline("{name} must be a named list"))
  }
  value
}

fabric_livy_conf <- function(conf = NULL, environment_id = NULL) {
  conf <- fabric_livy_normalize_named_list(conf, "conf")
  if (!is.null(environment_id)) {
    fabric_livy_check_string(environment_id, "environment_id")
    conf <- conf %||% list()
    conf[["spark.fabric.environmentDetails"]] <- jsonlite::toJSON(
      list(id = environment_id),
      auto_unbox = TRUE
    )
  }
  conf
}

fabric_livy_payload <- function(...) {
  Filter(Negate(is.null), list(...))
}

fabric_livy_json_payload <- function(payload) {
  array_fields <- intersect(
    names(payload),
    c("args", "jars", "files", "pyFiles", "archives")
  )
  for (field in array_fields) {
    payload[[field]] <- I(payload[[field]])
  }
  payload
}

fabric_livy_credential <- function(
  tenant_id,
  client_id,
  access_token,
  token_provider
) {
  fabric_credential(
    tenant_id = tenant_id,
    client_id = client_id,
    access_token = access_token,
    token_provider = token_provider
  )
}

fabric_livy_json <- function(
  method,
  url,
  credential,
  payload = NULL,
  idempotent = NULL
) {
  req <- httr2::request(url) |>
    httr2::req_method(method)
  if (!is.null(payload)) {
    req <- httr2::req_body_json(
      req,
      fabric_livy_json_payload(payload)
    )
  }
  .httr2_json(
    req,
    simplifyVector = FALSE,
    credential = credential,
    audience = .fabric_audience$fabric,
    idempotent = idempotent
  )
}

fabric_livy_ok <- function(
  method,
  url,
  credential,
  payload = NULL,
  idempotent = NULL
) {
  req <- httr2::request(url) |>
    httr2::req_method(method)
  if (!is.null(payload)) {
    req <- httr2::req_body_json(
      req,
      fabric_livy_json_payload(payload)
    )
  }
  .httr2_ok(
    req,
    credential = credential,
    audience = .fabric_audience$fabric,
    idempotent = idempotent
  )
}

# Normalize a copied session/batch URL to a collection endpoint
fabric_livy_endpoint <- function(
  url,
  type = c("sessions", "batches", "highConcurrencySessions")
) {
  fabric_livy_check_string(url, "url")
  type <- match.arg(type)
  value <- sub("/+$", "", trimws(url))
  collection_pattern <- paste0(
    "(?i)/(sessions|batches|highConcurrencySessions)$"
  )
  if (grepl(collection_pattern, value, perl = TRUE)) {
    sub(collection_pattern, paste0("/", type), value, perl = TRUE)
  } else {
    paste0(value, "/", type)
  }
}

fabric_livy_state <- function(response) {
  tolower(response$state %||% "")
}

fabric_livy_error_text <- function(response, fallback) {
  output <- response$output %||% list()
  data <- output$data %||% list()
  fabric_state <- response$fabricSessionStateInfo %||%
    response$fabricBatchStateInfo %||%
    list()
  candidates <- c(
    output$evalue,
    output$error,
    fabric_state$errorMessage,
    response$cancellationReason,
    unlist(response$errorInfo %||% list(), recursive = TRUE),
    data[["text/plain"]],
    response$log
  )
  candidates <- as.character(candidates)
  candidates <- candidates[!is.na(candidates) & nzchar(candidates)]
  if (!length(candidates)) {
    fallback
  } else {
    paste(unique(candidates), collapse = "\n")
  }
}

fabric_livy_abort_statement <- function(response) {
  state <- response$state %||% "unknown"
  rlang::abort(
    fabric_livy_error_text(
      response,
      paste0("Livy statement ended with state ", state)
    ),
    class = "fabric_livy_statement_error",
    statement = response,
    output = response$output %||% list(),
    traceback = response$output$traceback %||% character()
  )
}

fabric_livy_abort_session <- function(response) {
  state <- response$state %||% "unknown"
  rlang::abort(
    fabric_livy_error_text(
      response,
      paste0("Livy session ended with state ", state)
    ),
    class = "fabric_livy_session_error",
    session = response
  )
}

fabric_livy_abort_batch <- function(response) {
  state <- response$state %||% "unknown"
  rlang::abort(
    fabric_livy_error_text(
      response,
      paste0("Livy batch ended with state ", state)
    ),
    class = "fabric_livy_batch_error",
    batch = response,
    logs = response$log %||% character(),
    error_info = response$errorInfo %||% list()
  )
}

fabric_livy_output <- function(response, started_local, completed_local, url) {
  out <- response$output %||% list()
  data <- out$data %||% list()
  parsed <- NULL
  if (!is.null(data[["application/json"]])) {
    obj <- try(
      jsonlite::fromJSON(
        jsonlite::toJSON(data[["application/json"]], auto_unbox = TRUE),
        simplifyVector = TRUE
      ),
      silent = TRUE
    )
    if (!inherits(obj, "try-error")) {
      parsed <- if (is.data.frame(obj)) tibble::as_tibble(obj) else obj
    }
  } else if (!is.null(data[["text/plain"]])) {
    parsed <- as.character(data[["text/plain"]])
  }
  structure(
    list(
      id = response$id,
      state = response$state,
      started_local = started_local,
      completed_local = completed_local,
      duration_sec = as.numeric(difftime(
        completed_local,
        started_local,
        units = "secs"
      )),
      output = list(
        status = out$status %||% NULL,
        execution_count = out$execution_count %||% NULL,
        data = data,
        parsed = parsed,
        ename = out$ename %||% NULL,
        evalue = out$evalue %||% NULL,
        traceback = out$traceback %||% character()
      ),
      code = response$code %||% NULL,
      source_id = response$sourceId %||% NULL,
      url = url,
      raw = response
    ),
    class = c("fabric_livy_statement_result", "list")
  )
}

#' Run Spark code in a temporary Microsoft Fabric Livy session
#'
#' Creates a session, waits for it to become ready, runs one statement, and
#' closes the session even when execution fails. For multiple statements or
#' explicit lifecycle control, use [fabric_livy_session()].
#'
#' @param livy_url A Livy connection URL or an enriched Lakehouse record from
#'   [fabric_lakehouses()] or [fabric_item()].
#' @param code One non-empty string containing Spark code.
#' @param kind Statement language: `"spark"`, `"pyspark"`, `"sparkr"`, or
#'   `"sql"`.
#' @param tenant_id Microsoft Entra tenant ID.
#' @param client_id Microsoft Entra application ID.
#' @param access_token Optional Fabric bearer token.
#' @param token_provider Optional callback returning a Fabric bearer token.
#' @param environment_id Optional Fabric Environment ID.
#' @param conf Optional named list of Spark configuration settings.
#' @param verbose Logical. Emit lifecycle progress.
#' @param poll_interval Polling interval in seconds.
#' @param timeout Maximum seconds for each readiness/execution wait.
#'
#' @return An invisible `fabric_livy_statement_result` list.
#' @details Requests use the
#'   `https://api.fabric.microsoft.com/.default` audience. Delegated
#'   authentication requires `Lakehouse.Execute.All`, `Lakehouse.Read.All`,
#'   `Code.AccessFabric.All`, and `Code.AccessStorage.All`; the caller also
#'   needs an appropriate workspace role.
#'
#' @seealso
#' [Microsoft Fabric Livy API overview](https://learn.microsoft.com/en-us/fabric/data-engineering/api-livy-overview),
#' [session jobs](https://learn.microsoft.com/en-us/fabric/data-engineering/get-started-api-livy-session)
#'
#' @export
#' @example inst/examples/fabric_livy_query.R
fabric_livy_query <- function(
  livy_url,
  code,
  kind = c("spark", "pyspark", "sparkr", "sql"),
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  access_token = NULL,
  token_provider = NULL,
  environment_id = NULL,
  conf = NULL,
  verbose = TRUE,
  poll_interval = 2,
  timeout = 600
) {
  kind <- match.arg(kind)
  session <- fabric_livy_session(
    livy_url = livy_url,
    tenant_id = tenant_id,
    client_id = client_id,
    access_token = access_token,
    token_provider = token_provider,
    environment_id = environment_id,
    conf = conf,
    verbose = verbose
  )
  on.exit(try(session$close(), silent = TRUE), add = TRUE)
  session$wait(
    poll_interval = poll_interval,
    timeout = timeout
  )
  invisible(session$run(
    code = code,
    kind = kind,
    poll_interval = poll_interval,
    timeout = timeout
  ))
}
