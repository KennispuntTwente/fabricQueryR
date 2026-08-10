.httr2_secret_fields <- c(
  "accesstoken",
  "refreshtoken",
  "authorization",
  "clientsecret",
  "clientassertion",
  "password",
  "token",
  "apikey",
  "sig",
  "signature",
  "sharedaccesssignature",
  "sastoken"
)

.httr2_secret_field_pattern <- paste0(
  "(?:access[_. -]*token|refresh[_. -]*token|authorization|",
  "client[_. -]*(?:secret|assertion)|password|token|",
  "api[_. -]*key|sig(?:nature)?|",
  "shared[_. -]*access[_. -]*signature|sas[_. -]*token)"
)

.httr2_is_secret_field <- function(name) {
  normalized <- gsub("[^[:alnum:]]", "", tolower(name))
  normalized %in% .httr2_secret_fields
}

.httr2_redact <- function(text) {
  text <- gsub(
    "(?i)(bearer\\s+)[A-Za-z0-9._~+/-]+",
    "\\1<redacted>",
    text,
    perl = TRUE
  )
  gsub(
    paste0(
      "(?is)(?<![[:alnum:]_])([\"']?(?:",
      .httr2_secret_field_pattern,
      ")[\"']?\\s*[:=]\\s*)",
      "(?:\"(?:\\\\.|[^\"\\\\])*\"|",
      "'(?:\\\\.|[^'\\\\])*'|[^\\s&,}\\]]+)"
    ),
    "\\1<redacted>",
    text,
    perl = TRUE
  )
}

.httr2_body_preview <- function(resp, max_chars = 8000L) {
  safe_string <- function() {
    out <- try(httr2::resp_body_string(resp), silent = TRUE)
    if (inherits(out, "try-error") || is.null(out) || is.na(out)) "" else out
  }

  ctype <- try(httr2::resp_content_type(resp), silent = TRUE)
  if (inherits(ctype, "try-error") || is.null(ctype) || is.na(ctype)) {
    ctype <- ""
  }

  txt <- if (grepl("json", ctype, ignore.case = TRUE)) {
    out <- try(
      {
        obj <- httr2::resp_body_json(resp, simplifyVector = FALSE)
        jsonlite::toJSON(
          .httr2_redact_object(obj),
          auto_unbox = TRUE,
          pretty = TRUE
        )
      },
      silent = TRUE
    )
    if (inherits(out, "try-error")) safe_string() else out
  } else {
    safe_string()
  }
  txt <- .httr2_redact(txt)

  if (identical(txt, "")) {
    "<empty body>"
  } else if (nchar(txt) > max_chars) {
    paste0(substr(txt, 1L, max_chars), "\n<truncated>")
  } else {
    txt
  }
}

# Compose a helpful error with endpoint, status, request IDs, and body
.httr2_stop_http <- function(resp, prefix = "HTTP request failed") {
  status <- httr2::resp_status(resp)
  reason <- httr2::resp_status_desc(resp)
  rid <- httr2::resp_header(resp, "x-ms-request-id") %||%
    httr2::resp_header(resp, "request-id")
  act <- httr2::resp_header(resp, "x-ms-activity-id") %||%
    httr2::resp_header(resp, "activity-id")
  endpoint <- .httr2_redact(resp$url %||% resp$request$url)
  body <- .httr2_body_preview(resp)
  payload <- try(
    httr2::resp_body_json(
      resp,
      simplifyVector = FALSE,
      bigint_as_char = TRUE
    ),
    silent = TRUE
  )
  if (inherits(payload, "try-error") || !is.list(payload)) {
    payload <- NULL
  }
  redacted_payload <- if (is.null(payload)) {
    NULL
  } else {
    .httr2_redact_object(payload)
  }
  nested_error <- if (is.list(payload$error)) payload$error else list()
  error_code <- payload$errorCode %||% nested_error$code %||% NULL
  is_retriable <- payload$isRetriable %||%
    nested_error$isRetriable %||%
    NULL
  rid <- rid %||% payload$requestId %||% nested_error$requestId %||% NULL
  headers <- httr2::resp_headers(resp)
  redacted_headers <- lapply(names(headers), function(name) {
    if (.httr2_is_secret_field(name)) {
      "<redacted>"
    } else {
      .httr2_redact(as.character(headers[[name]]))
    }
  })
  names(redacted_headers) <- tolower(names(headers))

  hdr <- paste0(prefix, ": HTTP ", status, " ", reason, ".")
  mid <- paste(
    if (!is.null(endpoint)) paste0("Endpoint: ", endpoint) else NULL,
    if (!is.null(rid)) paste0("Request ID: ", rid) else NULL,
    if (!is.null(act)) paste0("Activity ID: ", act) else NULL,
    sep = "\n"
  )
  msg <- paste0(
    hdr,
    if (isTRUE(nzchar(mid))) paste0("\n", mid) else "",
    "\n--- Response body ---\n",
    body
  )
  rlang::abort(
    msg,
    class = "fabric_http_error",
    status = status,
    error_code = error_code,
    errorCode = error_code,
    is_retriable = is_retriable,
    isRetriable = is_retriable,
    request_id = rid,
    activity_id = act,
    response_metadata = list(
      endpoint = endpoint,
      status = status,
      content_type = httr2::resp_content_type(resp),
      headers = redacted_headers,
      body = redacted_payload
    )
  )
}

.httr2_retry_after <- function(resp, now = Sys.time()) {
  value <- httr2::resp_header(resp, "retry-after")
  if (is.null(value) || !nzchar(value)) {
    return(NULL)
  }
  seconds <- suppressWarnings(as.numeric(value))
  if (!is.na(seconds) && is.finite(seconds)) {
    return(max(0, seconds))
  }
  old_locale <- Sys.getlocale("LC_TIME")
  on.exit(
    suppressWarnings(Sys.setlocale("LC_TIME", old_locale)),
    add = TRUE
  )
  suppressWarnings(Sys.setlocale("LC_TIME", "C"))
  when <- suppressWarnings(as.POSIXct(
    value,
    format = "%a, %d %b %Y %H:%M:%S",
    tz = "GMT"
  ))
  if (is.na(when)) {
    NULL
  } else {
    max(0, as.numeric(difftime(when, now, units = "secs")))
  }
}

.httr2_is_idempotent <- function(req, idempotent = NULL) {
  if (!is.null(idempotent)) {
    return(isTRUE(idempotent))
  }
  method <- toupper(req$method %||% "GET")
  method %in% c("GET", "HEAD", "OPTIONS", "PUT", "DELETE")
}

# Perform an authenticated request with bounded service-aware retries
.httr2_perform <- function(
  req,
  credential = NULL,
  audience = NULL,
  idempotent = NULL,
  accepted_status = integer(),
  download_path = NULL,
  max_tries = 4L,
  request_timeout = getOption("fabricqueryr.http.timeout", 300),
  max_retry_delay = getOption("fabricqueryr.http.max_retry_delay", 120),
  deadline = NULL,
  .sleep = Sys.sleep,
  .runif = stats::runif,
  .now = Sys.time
) {
  max_tries <- as.integer(max_tries)
  if (is.na(max_tries) || max_tries < 1L) {
    rlang::abort("max_tries must be at least 1")
  }
  if (
    !is.null(request_timeout) &&
      (length(request_timeout) != 1L ||
        is.na(request_timeout) ||
        !is.numeric(request_timeout) ||
        !is.finite(request_timeout) ||
        request_timeout <= 0)
  ) {
    rlang::abort("request_timeout must be NULL or one positive number")
  }
  if (
    length(max_retry_delay) != 1L ||
      is.na(max_retry_delay) ||
      !is.numeric(max_retry_delay) ||
      !is.finite(max_retry_delay) ||
      max_retry_delay < 0
  ) {
    rlang::abort("max_retry_delay must be one non-negative number")
  }
  if (
    !is.null(deadline) &&
      (!inherits(deadline, "POSIXt") ||
        length(deadline) != 1L ||
        is.na(deadline))
  ) {
    rlang::abort("deadline must be NULL or one POSIX date-time")
  }
  can_retry <- .httr2_is_idempotent(req, idempotent)
  refresh_attempted <- FALSE
  force_refresh <- FALSE
  last_failure <- NULL

  for (attempt in seq_len(max_tries)) {
    remaining <- if (is.null(deadline)) {
      Inf
    } else {
      as.numeric(difftime(deadline, .now(), units = "secs"))
    }
    if (remaining <= 0) {
      rlang::abort(
        "The HTTP request deadline was exhausted",
        class = "fabric_http_deadline_error",
        parent = last_failure
      )
    }
    retry_after <- NULL
    attempt_req <- req
    if (!is.null(credential)) {
      token <- fabric_get_token(
        credential,
        audience,
        force_refresh = force_refresh
      )
      attempt_req <- httr2::req_headers(
        attempt_req,
        Authorization = paste("Bearer", token)
      )
      force_refresh <- FALSE
    }
    remaining <- if (is.null(deadline)) {
      Inf
    } else {
      as.numeric(difftime(deadline, .now(), units = "secs"))
    }
    if (remaining <= 0) {
      rlang::abort(
        "The HTTP request deadline was exhausted",
        class = "fabric_http_deadline_error",
        parent = last_failure
      )
    }
    timeout_limits <- c(request_timeout, remaining)
    if (!is.null(req$options$timeout_ms)) {
      timeout_limits <- c(timeout_limits, req$options$timeout_ms / 1000)
    }
    timeout_limits <- timeout_limits[is.finite(timeout_limits)]
    if (length(timeout_limits)) {
      attempt_req <- httr2::req_timeout(attempt_req, min(timeout_limits))
    }
    attempt_req <- httr2::req_error(
      attempt_req,
      is_error = function(resp) FALSE
    )
    response <- tryCatch(
      if (is.null(download_path)) {
        httr2::req_perform(attempt_req)
      } else {
        httr2::req_perform(attempt_req, path = download_path)
      },
      error = function(error) error
    )
    if (inherits(response, "error")) {
      last_failure <- response
      if (!can_retry || attempt == max_tries) {
        rlang::cnd_signal(response)
      }
    } else {
      status <- httr2::resp_status(response)
      if (status < 400L || status %in% accepted_status) {
        return(response)
      }
      if (
        status == 401L &&
          !is.null(credential) &&
          isTRUE(credential$refreshable) &&
          !refresh_attempted &&
          attempt < max_tries
      ) {
        refresh_attempted <- TRUE
        force_refresh <- TRUE
        next
      }
      transient <- status %in% c(408L, 429L, 500L, 502L, 503L, 504L)
      if (!can_retry || !transient || attempt == max_tries) {
        .httr2_stop_http(response)
      }
      retry_after <- .httr2_retry_after(response, now = .now())
    }

    delay <- if (!is.null(retry_after)) {
      min(retry_after, max_retry_delay)
    } else {
      min(30, 0.5 * (2^(attempt - 1L))) * .runif(1L, 0.5, 1.5)
    }
    if (!is.null(deadline)) {
      remaining <- as.numeric(difftime(deadline, .now(), units = "secs"))
      if (remaining <= 0) {
        rlang::abort(
          "The HTTP request deadline was exhausted",
          class = "fabric_http_deadline_error",
          parent = last_failure
        )
      }
      delay <- min(delay, remaining)
    }
    .sleep(delay)
  }
  rlang::cnd_signal(last_failure)
}

# Perform a request and parse JSON after applying shared auth/retry behavior
.httr2_json <- function(
  req,
  simplifyVector = TRUE,
  bigint_as_char = FALSE,
  credential = NULL,
  audience = NULL,
  idempotent = NULL,
  ...
) {
  resp <- .httr2_perform(
    req,
    credential = credential,
    audience = audience,
    idempotent = idempotent,
    ...
  )
  httr2::resp_body_json(
    resp,
    simplifyVector = simplifyVector,
    bigint_as_char = bigint_as_char
  )
}

.httr2_redact_object <- function(value) {
  if (!is.list(value)) {
    return(value)
  }
  value_names <- names(value)
  for (index in seq_along(value)) {
    secret <- !is.null(value_names) &&
      !is.na(value_names[[index]]) &&
      nzchar(value_names[[index]]) &&
      .httr2_is_secret_field(value_names[[index]])
    value[[index]] <- if (secret) {
      "<redacted>"
    } else {
      .httr2_redact_object(value[[index]])
    }
  }
  value
}

# Perform a request where no response body is needed
.httr2_ok <- function(
  req,
  credential = NULL,
  audience = NULL,
  idempotent = NULL,
  ...
) {
  .httr2_perform(
    req,
    credential = credential,
    audience = audience,
    idempotent = idempotent,
    ...
  )
  invisible(TRUE)
}

# Read a complete paged REST collection
.httr2_continuation_url <- function(origin_url, current_url, next_link) {
  candidate <- httr2::url_modify_relative(current_url, next_link)
  origin <- httr2::url_parse(origin_url)
  next_url <- httr2::url_parse(candidate)
  normalized_port <- function(parsed) {
    parsed$port %||%
      switch(
        tolower(parsed$scheme %||% ""),
        http = 80L,
        https = 443L,
        NA_integer_
      )
  }
  same_origin <- identical(
    tolower(origin$scheme %||% ""),
    tolower(next_url$scheme %||% "")
  ) &&
    identical(
      tolower(origin$hostname %||% ""),
      tolower(next_url$hostname %||% "")
    ) &&
    identical(normalized_port(origin), normalized_port(next_url))
  if (!same_origin) {
    rlang::abort(
      "The service returned a continuation URL on a different origin"
    )
  }
  candidate
}

.httr2_pagination_guard <- function(
  url,
  seen_urls,
  page,
  max_pages = 10000L
) {
  if (page > as.integer(max_pages)) {
    rlang::abort(sprintf(
      "Pagination exceeded the maximum of %d pages",
      as.integer(max_pages)
    ))
  }
  if (url %in% seen_urls) {
    rlang::abort(
      "The service returned a repeated pagination URL or continuation token"
    )
  }
  c(seen_urls, url)
}

.httr2_collection <- function(
  url,
  credential,
  audience,
  value_key = "value",
  offset_pagination = FALSE,
  page_size = 5000L,
  max_pages = 10000L
) {
  values <- list()
  next_url <- url
  continuation_token <- NULL
  skip <- 0L
  page_number <- 0L
  seen_urls <- character()
  repeat {
    req <- httr2::request(next_url)
    if (!is.null(continuation_token)) {
      req <- httr2::req_url_query(
        req,
        continuationToken = continuation_token
      )
    } else if (isTRUE(offset_pagination) && identical(next_url, url)) {
      req <- httr2::req_url_query(
        req,
        `$top` = page_size,
        `$skip` = skip
      )
    }
    page_number <- page_number + 1L
    seen_urls <- .httr2_pagination_guard(
      req$url,
      seen_urls,
      page_number,
      max_pages
    )
    page <- .httr2_json(
      req,
      simplifyVector = FALSE,
      credential = credential,
      audience = audience
    )
    page_values <- page[[value_key]] %||% list()
    values <- c(values, page_values)

    next_link <- page[["@odata.nextLink"]] %||%
      page[["odata.nextLink"]] %||%
      page$continuationUri
    if (!is.null(next_link) && nzchar(next_link)) {
      next_url <- .httr2_continuation_url(url, next_url, next_link)
      continuation_token <- NULL
      offset_pagination <- FALSE
      next
    }
    continuation_token <- page$continuationToken
    if (!is.null(continuation_token) && nzchar(continuation_token)) {
      continuation_token <- utils::URLdecode(as.character(continuation_token))
      next_url <- url
      offset_pagination <- FALSE
      next
    }
    if (
      isTRUE(offset_pagination) &&
        length(page_values) == as.integer(page_size)
    ) {
      skip <- skip + as.integer(page_size)
      next
    }
    break
  }
  values
}

# Poll a Fabric long-running operation until it reaches a terminal state
.httr2_poll_lro <- function(
  operation_url,
  credential,
  audience = .fabric_audience$fabric,
  poll_interval = 2,
  timeout = 600,
  cancel = NULL,
  .sleep = Sys.sleep,
  .now = Sys.time
) {
  deadline <- .now() + timeout
  last_body <- NULL
  last_state <- NULL
  abort_timeout <- function() {
    rlang::abort(
      "Timed out waiting for the Fabric operation",
      class = "fabric_lro_timeout_error",
      last_response = last_body,
      last_state = last_state,
      operation_url = operation_url
    )
  }
  repeat {
    if (!is.null(cancel) && isTRUE(cancel())) {
      rlang::abort(
        "Fabric long-running operation polling was cancelled"
      )
    }
    if (.now() >= deadline) {
      abort_timeout()
    }
    response <- tryCatch(
      .httr2_perform(
        httr2::request(operation_url),
        credential = credential,
        audience = audience,
        idempotent = TRUE,
        deadline = deadline
      ),
      fabric_http_deadline_error = function(error) abort_timeout()
    )
    body <- httr2::resp_body_json(
      response,
      simplifyVector = FALSE,
      bigint_as_char = TRUE
    )
    last_body <- body
    state <- tolower(body$status %||% body$state %||% "")
    last_state <- state
    if (state %in% c("succeeded", "success", "completed")) {
      result_location <- body$resourceLocation %||%
        body$resultLocation %||%
        body$location %||%
        httr2::resp_header(response, "location")
      if (!is.null(result_location) && nzchar(result_location)) {
        result_url <- .httr2_continuation_url(
          operation_url,
          operation_url,
          result_location
        )
        if (!identical(result_url, operation_url)) {
          result_response <- tryCatch(
            .httr2_perform(
              httr2::request(result_url),
              credential = credential,
              audience = audience,
              idempotent = TRUE,
              deadline = deadline
            ),
            fabric_http_deadline_error = function(error) abort_timeout()
          )
          return(httr2::resp_body_json(
            result_response,
            simplifyVector = FALSE,
            bigint_as_char = TRUE
          ))
        }
      }
      return(body)
    }
    if (state %in% c("failed", "cancelled", "canceled")) {
      detail <- unlist(
        body$error %||% body,
        recursive = TRUE,
        use.names = FALSE
      )
      rlang::abort(
        paste0(
          "Fabric long-running operation ended with state ",
          state,
          ": ",
          paste(detail, collapse = ": ")
        )
      )
    }
    delay <- .httr2_retry_after(response, now = .now()) %||% poll_interval
    remaining <- as.numeric(difftime(deadline, .now(), units = "secs"))
    if (remaining <= 0) {
      abort_timeout()
    }
    .sleep(min(delay, remaining))
  }
}
