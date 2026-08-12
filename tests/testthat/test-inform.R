test_that("inform dispatches every message type and honors verbose", {
  calls <- list()
  capture <- function(type) {
    force(type)
    function(msg, .envir) {
      calls[[length(calls) + 1L]] <<- list(
        type = type,
        message = rlang::englue(msg, env = .envir)
      )
      invisible(NULL)
    }
  }
  local_mocked_bindings(
    cli_alert_info = capture("info"),
    cli_alert_warning = capture("warning"),
    cli_alert_danger = capture("danger"),
    cli_alert_success = capture("success"),
    .package = "cli"
  )

  item_name <- "Lakehouse"
  for (type in c("info", "warning", "danger", "success")) {
    expect_invisible(inform(TRUE, "Handled {item_name}", type = type))
  }
  expect_invisible(inform(FALSE, "hidden", type = "info"))

  expect_identical(
    vapply(calls, `[[`, character(1), "type"),
    c("info", "warning", "danger", "success")
  )
  expect_identical(
    vapply(calls, `[[`, character(1), "message"),
    rep("Handled Lakehouse", 4L)
  )
})
