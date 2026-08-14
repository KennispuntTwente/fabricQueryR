# Display one optional informational alert. The public workflow supplies
# `verbose`; this helper evaluates cli markup in that workflow's environment
inform <- function(verbose, msg, type = c("info", "success")) {
  if (!isTRUE(verbose)) {
    return(invisible())
  }

  type <- match.arg(type)
  .envir <- rlang::caller_env()

  switch(
    type,
    info = cli::cli_alert_info(msg, .envir = .envir),
    success = cli::cli_alert_success(msg, .envir = .envir)
  )
  invisible()
}

# Format a trusted condition template with cli markup. Runtime messages remain
# literal by default so service text can never be evaluated as glue expressions
.fabric_condition_message <- function(message, .envir, .format) {
  if (isTRUE(.format)) {
    cli::format_message(message, .envir = .envir)
  } else {
    message
  }
}

# Raise a formatted rlang error while preserving the public caller and any
# typed condition fields supplied through `...`
.fabric_abort <- function(
  message,
  ...,
  .format = FALSE,
  call = rlang::caller_env(),
  .envir = rlang::caller_env()
) {
  rlang::abort(
    .fabric_condition_message(message, .envir, .format),
    ...,
    call = call
  )
}

# Raise a formatted rlang warning while preserving the public caller and any
# typed condition fields supplied through `...`
.fabric_warn <- function(
  message,
  ...,
  .format = FALSE,
  call = rlang::caller_env(),
  .envir = rlang::caller_env()
) {
  rlang::warn(
    .fabric_condition_message(message, .envir, .format),
    ...,
    call = call
  )
}

# Print a concise package object with one consistent cli definition-list layout
.fabric_print <- function(class, fields) {
  fields <- fields[!vapply(fields, is.null, logical(1))]
  fields <- vapply(fields, as.character, character(1))

  app <- cli::start_app(output = "stdout", .auto_close = FALSE)
  on.exit(cli::stop_app(app), add = TRUE)
  cli::cli_text("{.strong <{class}>}")
  cli::cli_dl(fields)
  invisible()
}

# Start a delayed cli spinner for a polling loop. CLI's global progress options
# control whether and when it appears, while `verbose = FALSE` keeps R6
# workflows that already expose that setting completely quiet
.fabric_poll_progress <- function(resource, id = NULL, verbose = TRUE) {
  if (!isTRUE(verbose)) {
    return(NULL)
  }

  msg <- if (is.null(id)) {
    "Waiting for {resource}"
  } else {
    "Waiting for {resource} {.val {id}}"
  }
  msg <- cli::format_inline(msg, .envir = environment())
  msg_done <- cli::format_inline("{resource} finished")
  msg_failed <- cli::format_inline("Stopped waiting for {resource}")

  cli::cli_progress_step(
    msg,
    msg_done = msg_done,
    msg_failed = msg_failed,
    spinner = TRUE,
    .envir = rlang::caller_env()
  )
}

# Update a polling spinner with the latest service state
.fabric_poll_progress_update <- function(id, status) {
  if (is.null(id)) {
    return(invisible())
  }

  status <- cli::format_inline("Status: {.val {status}}")
  cli::cli_progress_update(id = id, status = status)
  invisible()
}

# Finish a polling spinner explicitly before returning a terminal state
.fabric_poll_progress_done <- function(id) {
  if (!is.null(id)) {
    cli::cli_progress_done(id = id)
  }
  invisible()
}
