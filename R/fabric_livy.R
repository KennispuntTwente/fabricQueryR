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

fabric_livy_resolve_url <- function(
  livy_url,
  allow_custom_endpoint = FALSE
) {
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
  fabric_livy_validate_endpoint(livy_url, allow_custom_endpoint)
}

fabric_livy_validate_endpoint <- function(
  url,
  allow_custom_endpoint = FALSE
) {
  fabric_livy_check_string(url, "livy_url")
  fabric_livy_check_flag(allow_custom_endpoint, "allow_custom_endpoint")
  value <- sub("/+$", "", trimws(url))
  parsed <- try(httr2::url_parse(value), silent = TRUE)
  if (
    inherits(parsed, "try-error") ||
      !identical(tolower(parsed$scheme %||% ""), "https") ||
      !nzchar(parsed$hostname %||% "")
  ) {
    rlang::abort("livy_url must be a valid HTTPS endpoint")
  }
  host <- tolower(parsed$hostname)
  fabric_host <- grepl("(^|\\.)api\\.fabric\\.microsoft\\.com$", host)
  if (!fabric_host && !isTRUE(allow_custom_endpoint)) {
    rlang::abort(paste0(
      "livy_url is not a Microsoft Fabric API host; set ",
      "allow_custom_endpoint = TRUE only for a trusted HTTPS service"
    ))
  }
  value
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
  token = NULL,
  auth_args = list()
) {
  fabric_credential(
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args
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
  type = c("sessions", "batches", "highConcurrencySessions"),
  allow_custom_endpoint = FALSE
) {
  type <- match.arg(type)
  value <- fabric_livy_validate_endpoint(url, allow_custom_endpoint)
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
#' Starts Fabric Spark compute, runs one statement, waits for its result, and
#' closes the session. This is the simplest Livy helper for a one-off Spark
#' operation. Spark is useful for distributed processing or changing Lakehouse
#' data; it has more startup overhead than querying an existing SQL endpoint.
#'
#' @param livy_url A Livy connection URL copied from the Lakehouse settings, or
#'   an enriched Lakehouse record from [fabric_lakehouses()] or [fabric_item()].
#'   A discovered record avoids copying workspace and Lakehouse IDs.
#' @param code One string containing the Spark code to run. Objects created in
#'   this temporary session are lost after the function returns, although
#'   writes made to Lakehouse storage persist.
#' @param kind Statement language. Use `"sparkr"` for SparkR code, `"pyspark"`
#'   for Python with Spark, `"spark"` for Scala, or `"sql"` for Spark SQL. This
#'   must match the syntax in `code`.
#' @param tenant_id Microsoft Entra tenant ID. Defaults to
#'   `FABRICQUERYR_TENANT_ID`.
#' @param client_id Microsoft Entra application/client ID. Defaults to
#'   `FABRICQUERYR_CLIENT_ID`, then the Azure CLI application ID.
#' @param token Optional `AzureAuth::AzureToken`, bearer-token string, or
#'   token-provider function. With `NULL`, `AzureAuth` reuses a matching cached
#'   token or starts its normal interactive login flow.
#' @param auth_args Named list of additional arguments passed to
#'   [AzureAuth::get_azure_token()].
#' @param environment_id Optional GUID of a published Fabric Environment whose
#'   libraries and Spark settings should be used. Leave `NULL` to use the
#'   Lakehouse/workspace defaults.
#' @param conf Optional named list of Spark configuration overrides, for example
#'   `list("spark.sql.shuffle.partitions" = "100")`. Most users can leave this
#'   `NULL` and configure shared settings in a Fabric Environment.
#' @param verbose Logical. Show session startup, execution, and cleanup progress.
#' @param poll_interval Seconds between status checks. Lower values update
#'   sooner but make more API calls.
#' @param timeout Maximum seconds to wait for session readiness and, separately,
#'   statement completion.
#' @param allow_custom_endpoint Logical. Keep `FALSE` to require a Microsoft
#'   Fabric API host. Set `TRUE` only for a trusted custom HTTPS service, such
#'   as a test emulator; the Fabric bearer token is sent to this endpoint.
#' @param ... Compatibility arguments. The former named `access_token`
#'   argument is accepted here as a deprecated alias for `token`; all other
#'   arguments are rejected.
#'
#' @return Invisibly, a `fabric_livy_statement_result` list with statement
#'   `state`, timing information, submitted `code`, raw response, and `output`.
#'   `output$parsed` contains JSON output converted to an R object or plain-text
#'   output as a character vector; error details are retained in the other
#'   `output` fields.
#' @details
#' Fabric needs a workspace on supported Fabric capacity and a Lakehouse. In
#' the Fabric portal, open the Lakehouse settings, find **Livy endpoint**, and
#' copy the session-job connection string. For several statements that reuse
#' variables and Spark state, use [fabric_livy_session()]. To run a complete
#' Python, Scala/Java, or R application file, use
#' [fabric_livy_batch_submit()].
#'
#' Requests use the
#'   `https://api.fabric.microsoft.com/.default` audience. Delegated
#'   authentication requires `Lakehouse.Execute.All`, `Lakehouse.Read.All`,
#'   `Code.AccessFabric.All`, and `Code.AccessStorage.All`; the caller also
#'   needs an appropriate workspace role.
#'
#' @seealso
#' [Microsoft Fabric Livy API overview](https://learn.microsoft.com/en-us/fabric/data-engineering/api-livy-overview),
#' [session jobs and Fabric setup](https://learn.microsoft.com/en-us/fabric/data-engineering/get-started-api-livy-session)
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
  token = NULL,
  auth_args = list(),
  environment_id = NULL,
  conf = NULL,
  verbose = TRUE,
  poll_interval = 2,
  timeout = 600,
  allow_custom_endpoint = FALSE,
  ...
) {
  kind <- match.arg(kind)
  resolved <- fabric_resolve_token_alias(
    token = token,
    dots = list(...),
    caller = "fabric_livy_query()"
  )
  token <- resolved$token
  if (length(resolved$dots)) {
    rlang::abort("fabric_livy_query() received unused arguments in ...")
  }
  session <- fabric_livy_session(
    livy_url = livy_url,
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args,
    environment_id = environment_id,
    conf = conf,
    verbose = verbose,
    allow_custom_endpoint = allow_custom_endpoint
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
