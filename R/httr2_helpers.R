.httr2_body_preview <- function(resp, max_chars = 8000L) {
  safe_string <- function() {
    out <- try(httr2::resp_body_string(resp), silent = TRUE)
    if (inherits(out, "try-error") || is.null(out) || is.na(out)) "" else out
  }

  ctype <- try(httr2::resp_content_type(resp), silent = TRUE)
  if (inherits(ctype, "try-error") || is.null(ctype) || is.na(ctype))
    ctype <- ""

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

  if (identical(txt, "")) "<empty body>" else if (nchar(txt) > max_chars)
    paste0(substr(txt, 1L, max_chars), "\n... <truncated> ...") else txt
}


# Compose a helpful error with status, request-id, and body
.httr2_stop_http <- function(resp, prefix = "HTTP request failed") {
  status <- httr2::resp_status(resp)
  reason <- httr2::resp_status_desc(resp)
  rid <- httr2::resp_header(resp, "x-ms-request-id") %||%
    httr2::resp_header(resp, "request-id")
  act <- httr2::resp_header(resp, "x-ms-activity-id")
  body <- .httr2_body_preview(resp)

  hdr <- paste0(prefix, ": HTTP ", status, " ", reason, ".")
  mid <- paste(
    if (!is.null(rid)) paste0("x-ms-request-id: ", rid) else NULL,
    if (!is.null(act)) paste0("x-ms-activity-id: ", act) else NULL,
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

# Perform a request and parse JSON; do NOT throw until we format a great error
.httr2_json <- function(req) {
  req <- httr2::req_error(req, is_error = function(resp) FALSE) # don’t auto-stop
  resp <- httr2::req_perform(req)
  if (httr2::resp_status(resp) >= 400L) .httr2_stop_http(resp)
  httr2::resp_body_json(resp, simplifyVector = TRUE)
}

# Perform a request where we don't need a body back (DELETE, etc.)
.httr2_ok <- function(req) {
  req <- httr2::req_error(req, is_error = function(resp) FALSE)
  resp <- httr2::req_perform(req)
  if (httr2::resp_status(resp) >= 400L) .httr2_stop_http(resp)
  invisible(TRUE)
}
