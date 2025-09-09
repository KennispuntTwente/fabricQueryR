inform <- function(
    verbose,
    msg,
    type = c("info", "warning", "danger", "success")
) {
  if (!isTRUE(verbose)) return(invisible())
  type <- match.arg(type)

  # evaluate { } in the caller of `inform()`
  .envir <- rlang::caller_env()

  switch(
    type,
    info    = cli::cli_alert_info(msg, .envir = .envir),
    warning = cli::cli_alert_warning(msg, .envir = .envir),
    danger  = cli::cli_alert_danger(msg, .envir = .envir),
    success = cli::cli_alert_success(msg, .envir = .envir)
  )
  invisible()
}
