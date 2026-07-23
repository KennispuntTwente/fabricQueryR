.httr2_redact <- function(text) {
  text <- gsub(
    "(?i)(bearer\\s+)[A-Za-z0-9._~+/-]+",
    "\\1<redacted>",
    text,
    perl = TRUE
  )
  gsub(
    paste0(
      "(?i)(\"?(?:access_token|refresh_token|authorization|",
      "client_secret|password|token)\"?\\s*[:=]\\s*\"?)",
      "[^\"&,}\\s]+"
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
        jsonlite::toJSON(obj, auto_unbox = TRUE, pretty = TRUE)
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
    paste0(substr(txt, 1L, max_chars), "\n... <truncated> ...")
  } else {
    txt
  }
}

# Compose a helpful error with endpoint, status, request IDs, and body.
.httr2_stop_http <- function(resp, prefix = "HTTP request failed") {
  status <- httr2::resp_status(resp)
  reason <- httr2::resp_status_desc(resp)
  rid <- httr2::resp_header(resp, "x-ms-request-id") %||%
    httr2::resp_header(resp, "request-id")
  act <- httr2::resp_header(resp, "x-ms-activity-id") %||%
    httr2::resp_header(resp, "activity-id")
  endpoint <- .httr2_redact(resp$url %||% resp$request$url)
  body <- .httr2_body_preview(resp)

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
  stop(msg, call. = FALSE)
}

.httr2_retry_after <- function(resp, now = Sys.time()) {
  value <- httr2::resp_header(resp, "retry-after")
  if (is.null(value) || !nzchar(value)) {
    return(NULL)
  }
  seconds <- suppressWarnings(as.numeric(value))
  if (!is.na(seconds)) {
    return(max(0, seconds))
  }
  when <- suppressWarnings(as.POSIXct(
    value,
    format = "%a, %d %b %Y %H:%M:%S",
    tz = "GMT"
  ))
  if (is.na(when)) NULL else max(0, as.numeric(difftime(when, now, units = "secs")))
}

.httr2_is_idempotent <- function(req, idempotent = NULL) {
  if (!is.null(idempotent)) {
    return(isTRUE(idempotent))
  }
  method <- toupper(req$method %||% "GET")
  method %in% c("GET", "HEAD", "OPTIONS", "PUT", "DELETE")
}

# Perform an authenticated request with bounded service-aware retries.
.httr2_perform <- function(
  req,
  credential = NULL,
  audience = NULL,
  idempotent = NULL,
  max_tries = 4L,
  .sleep = Sys.sleep,
  .runif = stats::runif,
  .now = Sys.time
) {
  max_tries <- as.integer(max_tries)
  if (is.na(max_tries) || max_tries < 1L) {
    stop("max_tries must be at least 1.", call. = FALSE)
  }
  can_retry <- .httr2_is_idempotent(req, idempotent)
  refresh_attempted <- FALSE
  force_refresh <- FALSE
  last_failure <- NULL

  for (attempt in seq_len(max_tries)) {
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
    attempt_req <- httr2::req_error(
      attempt_req,
      is_error = function(resp) FALSE
    )
    response <- tryCatch(
      httr2::req_perform(attempt_req),
      error = function(error) error
    )
    if (inherits(response, "error")) {
      last_failure <- response
      if (!can_retry || attempt == max_tries) {
        stop(response)
      }
    } else {
      status <- httr2::resp_status(response)
      if (status < 400L) {
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
      min(retry_after, 120)
    } else {
      min(30, 0.5 * (2 ^ (attempt - 1L))) * .runif(1L, 0.5, 1.5)
    }
    .sleep(delay)
  }
  stop(last_failure)
}

# Perform a request and parse JSON after applying shared auth/retry behavior.
.httr2_json <- function(
  req,
  simplifyVector = TRUE,
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
  httr2::resp_body_json(resp, simplifyVector = simplifyVector)
}

# Perform a request where no response body is needed.
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

# Read a complete paged REST collection.
.httr2_collection <- function(
  url,
  credential,
  audience,
  value_key = "value",
  offset_pagination = FALSE,
  page_size = 5000L
) {
  values <- list()
  next_url <- url
  continuation_token <- NULL
  skip <- 0L
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
      next_url <- next_link
      continuation_token <- NULL
      offset_pagination <- FALSE
      next
    }
    continuation_token <- page$continuationToken
    if (!is.null(continuation_token) && nzchar(continuation_token)) {
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

# Poll a Fabric long-running operation until it reaches a terminal state.
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
  repeat {
    if (!is.null(cancel) && isTRUE(cancel())) {
      stop("Fabric long-running operation polling was cancelled.", call. = FALSE)
    }
    if (.now() > deadline) {
      stop("Timed out waiting for the Fabric operation.", call. = FALSE)
    }
    body <- .httr2_json(
      httr2::request(operation_url),
      simplifyVector = FALSE,
      credential = credential,
      audience = audience
    )
    state <- tolower(body$status %||% body$state %||% "")
    if (state %in% c("succeeded", "success", "completed")) {
      return(body)
    }
    if (state %in% c("failed", "cancelled", "canceled")) {
      detail <- unlist(body$error %||% body, recursive = TRUE, use.names = FALSE)
      stop(
        paste0(
          "Fabric long-running operation ended with state ",
          state,
          ": ",
          paste(detail, collapse = ": ")
        ),
        call. = FALSE
      )
    }
    .sleep(poll_interval)
  }
}
