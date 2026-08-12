# Show `msg` through cli when `verbose` is enabled. Returns invisibly and is
# used by long-running public functions for optional progress messages.
inform <- function(
  verbose,
  msg,
  type = c("info", "warning", "danger", "success")
) {
  if (!isTRUE(verbose)) {
    return(invisible())
  }
  type <- match.arg(type)

  # Evaluate `{}` expressions where `inform()` was called so message values
  # such as `{item_name}` refer to the public function's local variables.
  .envir <- rlang::caller_env()

  switch(
    type,
    info = cli::cli_alert_info(msg, .envir = .envir),
    warning = cli::cli_alert_warning(msg, .envir = .envir),
    danger = cli::cli_alert_danger(msg, .envir = .envir),
    success = cli::cli_alert_success(msg, .envir = .envir)
  )
  invisible()
}
