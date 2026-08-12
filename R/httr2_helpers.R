.httr2_secret_fields <- c(
  "accesstoken",
  "refreshtoken",
  "authorization",
  "clientsecret",
  "clientassertion",
  "cookie",
  "password",
  "setcookie",
  "token",
  "apikey",
  "sig",
  "signature",
  "sharedaccesssignature",
  "sastoken"
)

.httr2_secret_field_pattern <- paste0(
  "(?:access[_. -]*token|refresh[_. -]*token|authorization|",
  "client[_. -]*(?:secret|assertion)|cookie|password|set[_. -]*cookie|token|",
  "api[_. -]*key|sig(?:nature)?|",
  "shared[_. -]*access[_. -]*signature|sas[_. -]*token)"
)

# Perform authenticated `req` with bounded service-aware retries. Returns an
# httr2 response or raises the final safe, typed error for all service helpers
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
  # 1 Validate retry settings ----------------------------------------------------------------------

  # Reject impossible limits before requesting a token or touching the network

  if (
    !is.numeric(max_tries) ||
      length(max_tries) != 1L ||
      is.na(max_tries) ||
      !is.finite(max_tries) ||
      max_tries < 1 ||
      max_tries > .Machine$integer.max ||
      max_tries != floor(max_tries)
  ) {
    rlang::abort(
      "max_tries must be one whole number between 1 and .Machine$integer.max"
    )
  }
  max_tries <- as.integer(max_tries)

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

  # 2 Prepare retry state --------------------------------------------------------------------------

  # Prepare retry state once for reuse in the remaining work

  can_retry <- if (is.null(idempotent)) {
    method <- toupper(req$method %||% "GET")
    method %in% c("GET", "HEAD", "OPTIONS", "PUT", "DELETE")
  } else {
    isTRUE(idempotent)
  }
  refresh_attempted <- FALSE
  force_refresh <- FALSE
  last_failure <- NULL

  # 3 Perform request attempts ---------------------------------------------------------------------

  # Rebuild authentication and timeout settings for every attempt so refreshed
  # tokens and shrinking deadlines take effect

  for (attempt in seq_len(max_tries)) {
    # Stop before starting work that cannot finish inside the caller's deadline
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

    # Read a fresh token when authentication is managed by a credential
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

    # Authentication may take time, so calculate the usable timeout again
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

    # Keep HTTP error responses available so retry policy can inspect them
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

    # Network errors and HTTP errors use different retry decisions
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

      # A refreshable credential gets one chance to replace a rejected token
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

      service_retriable <- .httr2_response_is_retriable(response)
      transient <- if (is.null(service_retriable)) {
        status %in% c(408L, 429L, 500L, 502L, 503L, 504L)
      } else {
        service_retriable
      }
      if (!can_retry || !transient || attempt == max_tries) {
        .httr2_stop_http(response)
      }

      retry_after <- .httr2_retry_after(response, now = .now())
    }

    ## 3.1 Choose a retry delay --------------------------------------------------------------------

    # Prefer the service's Retry-After value; otherwise use bounded exponential
    # backoff with jitter to avoid synchronized retries

    delay <- if (!is.null(retry_after)) {
      retry_after
    } else {
      min(
        max_retry_delay,
        min(30, 0.5 * (2^(attempt - 1L))) * .runif(1L, 0.5, 1.5)
      )
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

      if (!is.null(retry_after) && retry_after > remaining) {
        rlang::abort(
          paste0(
            "The service Retry-After interval exceeds the HTTP request ",
            "deadline"
          ),
          class = "fabric_http_deadline_error",
          retry_after = retry_after,
          remaining = remaining
        )
      }
    }

    if (!is.null(retry_after) && retry_after > max_retry_delay) {
      rlang::abort(
        sprintf(
          "The service requested a %s-second retry delay, exceeding the %s-second client limit",
          format(retry_after, trim = TRUE),
          format(max_retry_delay, trim = TRUE)
        ),
        class = "fabric_http_retry_after_error",
        retry_after = retry_after,
        max_retry_delay = max_retry_delay,
        response_metadata = .httr2_response_metadata(response)
      )
    }

    ## 3.2 Wait before retrying --------------------------------------------------------------------

    # Pause for the chosen delay before building the next request attempt

    .sleep(delay)
  }
  rlang::cnd_signal(last_failure)
}

# Perform `req` with shared authentication/retry behavior and decode JSON
# Returns an R object for service helpers that expect a response body
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

# Read a complete REST collection from `url`. Returns all values after following
# next links, continuation tokens, or supported offset pagination
.httr2_collection <- function(
  url,
  credential,
  audience,
  value_key = "value",
  offset_pagination = FALSE,
  page_size = 5000L,
  max_pages = 10000L
) {
  # 1 Prepare pagination state ---------------------------------------------------------------------

  # Prepare pagination state once for reuse in the remaining work

  values <- list()
  next_url <- url
  continuation_token <- NULL
  skip <- 0L
  page_number <- 0L
  seen_urls <- character()

  # 2 Read pages -----------------------------------------------------------------------------------

  # Build each next request from trusted state and stop on a repeated URL

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

  # 3 Return collected values ----------------------------------------------------------------------

  # Return collected values in the stable form expected by the caller

  values
}

# Check whether `name` identifies a secret field. Returns one logical value used
# while redacting headers and structured response bodies
.httr2_is_secret_field <- function(name) {
  normalized <- gsub("[^[:alnum:]]", "", tolower(name))
  normalized %in% .httr2_secret_fields
}

# Remove bearer tokens and named secrets from `text`. Returns safe text for
# errors, logs, and metadata exposed by shared HTTP helpers
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

# Read a bounded, redacted body preview from `resp`. Returns text included in a
# helpful HTTP error without exposing credentials or unlimited response data
.httr2_body_preview <- function(resp, max_chars = 8000L) {
  # Read the response as text without raising; returns empty text on failure
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

# Compose a helpful error from `resp` with safe endpoint, status, request IDs,
# headers, and body. This function raises a typed condition and does not return
.httr2_stop_http <- function(resp, prefix = "HTTP request failed") {
  # 1 Read response context ------------------------------------------------------------------------

  # Collect identifiers and body content before building the condition, while
  # redacting both plain text and structured data

  status <- httr2::resp_status(resp)
  reason <- httr2::resp_status_desc(resp)
  rid <- httr2::resp_header(resp, "x-ms-request-id") %||%
    httr2::resp_header(resp, "request-id")
  act <- httr2::resp_header(resp, "x-ms-activity-id") %||%
    httr2::resp_header(resp, "activity-id")
  endpoint <- .httr2_redact(resp$url %||% resp$request$url)
  body <- .httr2_body_preview(resp)

  # Decode structured errors when possible but keep text-only responses valid
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

  # Fabric may place error details at the top level or inside `error`
  nested_error <- if (is.list(payload$error)) payload$error else list()
  error_code <- payload$errorCode %||% nested_error$code %||% NULL
  is_retriable <- payload$isRetriable %||%
    nested_error$isRetriable %||%
    NULL
  rid <- rid %||% payload$requestId %||% nested_error$requestId %||% NULL

  # Redact sensitive response headers before attaching them to the condition
  headers <- httr2::resp_headers(resp)
  redacted_headers <- lapply(names(headers), function(name) {
    if (.httr2_is_secret_field(name)) {
      "<redacted>"
    } else {
      .httr2_redact(as.character(headers[[name]]))
    }
  })
  names(redacted_headers) <- tolower(names(headers))

  # 2 Build a safe error message -------------------------------------------------------------------

  # Build a safe error message from the validated values required by the next step

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

  # 3 Raise the typed HTTP condition ---------------------------------------------------------------

  # Turn the final state into clear output for the caller

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

# Build a bounded, credential-free response summary suitable for attaching to
# conditions. The authenticated httr2 request object is intentionally omitted.
.httr2_response_metadata <- function(resp) {
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
  } else {
    payload <- .httr2_redact_object(payload)
  }
  headers <- httr2::resp_headers(resp)
  redacted_headers <- lapply(names(headers), function(name) {
    if (.httr2_is_secret_field(name)) {
      "<redacted>"
    } else {
      .httr2_redact(as.character(headers[[name]]))
    }
  })
  names(redacted_headers) <- tolower(names(headers))
  list(
    endpoint = .httr2_redact(resp$url %||% resp$request$url),
    status = httr2::resp_status(resp),
    content_type = httr2::resp_content_type(resp),
    headers = redacted_headers,
    body = payload
  )
}

# Parse the Retry-After header in `resp` as seconds from `now`. Returns a
# non-negative number or `NULL` when the header is absent or invalid
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

# Read Fabric's explicit retry decision from a structured error response.
# Returns NULL when the service did not provide a valid logical value.
.httr2_response_is_retriable <- function(resp) {
  payload <- try(
    httr2::resp_body_json(resp, simplifyVector = FALSE),
    silent = TRUE
  )
  if (inherits(payload, "try-error") || !is.list(payload)) {
    return(NULL)
  }
  nested <- if (is.list(payload$error)) payload$error else list()
  value <- payload$isRetriable %||% nested$isRetriable
  if (is.logical(value) && length(value) == 1L && !is.na(value)) {
    value
  } else {
    NULL
  }
}

# Recursively redact secret-named fields in structured `value`. Returns a safe
# object suitable for response metadata attached to errors
.httr2_redact_object <- function(value) {
  if (!is.list(value)) {
    return(if (is.character(value)) .httr2_redact(value) else value)
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

# Resolve `next_link` relative to `current_url` and enforce `origin_url`'s
# origin. Returns a safe continuation URL used by collection readers
.httr2_continuation_url <- function(origin_url, current_url, next_link) {
  candidate <- httr2::url_modify_relative(current_url, next_link)
  origin <- httr2::url_parse(origin_url)
  next_url <- httr2::url_parse(candidate)
  # Return explicit or scheme-default port text for same-origin comparison
  normalized_port <- function(parsed) {
    as.character(
      parsed$port %||%
        switch(
          tolower(parsed$scheme %||% ""),
          http = "80",
          https = "443",
          NA_character_
        )
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

# Check page count and repeated URLs before a paged request. Returns updated
# `seen_urls` so malformed pagination cannot loop forever
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
