inform <- function(
  verbose,
  msg,
  type = c("info", "warning", "danger", "success")
) {
  if (!isTRUE(verbose)) return(invisible())
  type <- match.arg(type)
  switch(
    type,
    info = cli::cli_alert_info(msg),
    warning = cli::cli_alert_warning(msg),
    danger = cli::cli_alert_danger(msg),
    success = cli::cli_alert_success(msg)
  )
  invisible()
}
