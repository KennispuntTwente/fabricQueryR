test_that("inform displays optional informational and success alerts", {
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
    cli_alert_success = capture("success"),
    .package = "cli"
  )

  item_name <- "Lakehouse"
  expect_invisible(inform(TRUE, "Opening {item_name}"))
  expect_invisible(
    inform(TRUE, "Opened {item_name}", type = "success")
  )
  expect_invisible(inform(FALSE, "Hidden {item_name}"))

  expect_identical(
    vapply(calls, `[[`, character(1), "type"),
    c("info", "success")
  )
  expect_identical(
    vapply(calls, `[[`, character(1), "message"),
    c("Opening Lakehouse", "Opened Lakehouse")
  )
})

test_that("condition helpers use cli markup and rlang conditions", {
  local_reproducible_output()
  argument <- "workspace"
  value <- "missing"

  expect_snapshot(
    .fabric_warn(
      c(
        "Could not resolve {.arg {argument}}",
        "i" = "Received {.val {value}}"
      ),
      .format = TRUE,
      class = "fabric_test_warning",
      call = NULL
    )
  )
  expect_snapshot(
    error = TRUE,
    .fabric_abort(
      c(
        "Could not resolve {.arg {argument}}",
        "x" = "Received {.val {value}}"
      ),
      .format = TRUE,
      class = "fabric_test_error",
      detail = 42L,
      call = NULL
    )
  )
})

test_that("polling progress has one consistent lifecycle", {
  calls <- list()
  local_mocked_bindings(
    cli_progress_step = function(...) {
      calls[[length(calls) + 1L]] <<- list(action = "start", args = list(...))
      "progress-id"
    },
    cli_progress_update = function(...) {
      calls[[length(calls) + 1L]] <<- list(action = "update", args = list(...))
      invisible(NULL)
    },
    cli_progress_done = function(...) {
      calls[[length(calls) + 1L]] <<- list(action = "done", args = list(...))
      invisible(NULL)
    },
    .package = "cli"
  )

  progress <- .fabric_poll_progress("Fabric job", "job-1")
  expect_identical(progress, "progress-id")
  expect_invisible(.fabric_poll_progress_update(progress, "Running"))
  expect_invisible(.fabric_poll_progress_done(progress))
  expect_null(.fabric_poll_progress("Fabric job", "job-2", verbose = FALSE))

  expect_identical(
    vapply(calls, `[[`, character(1), "action"),
    c("start", "update", "done")
  )
  expect_identical(calls[[2L]]$args$id, "progress-id")
  expect_identical(calls[[2L]]$args$status, "Status: \"Running\"")
  expect_identical(calls[[3L]]$args$id, "progress-id")
})

test_that("package objects use one cli summary layout", {
  local_reproducible_output()

  expect_snapshot(
    .fabric_print(
      "fabric_example",
      list(id = "item-1", state = "Running", omitted = NULL)
    )
  )
})

test_that("condition rethrows preserve class and custom fields", {
  original <- structure(
    list(message = "original failure", call = NULL, marker = 42L),
    class = c("fabric_test_error", "error", "condition")
  )

  error <- tryCatch(.fabric_rethrow(original), error = identity)

  expect_s3_class(error, "fabric_test_error")
  expect_identical(conditionMessage(error), "original failure")
  expect_identical(error$marker, 42L)
  expect_false(is.data.frame(error$trace))
})

test_that("package code does not bypass the presentation layer", {
  if (nzchar(Sys.getenv("R_COVR"))) {
    skip("covr instruments direct condition calls in package source")
  }
  source_dir <- test_path("..", "..", "R")
  if (!dir.exists(source_dir)) {
    skip("Package source is not available in installed test runs")
  }
  source_dir <- normalizePath(source_dir, mustWork = TRUE)
  paths <- list.files(source_dir, pattern = "[.]R$", full.names = TRUE)
  lines <- unlist(lapply(paths, readLines, warn = FALSE), use.names = FALSE)

  base_output <- grep(
    paste0(
      "(^|[^[:alnum:]_.])",
      "(stop|warning|message|cat|writeLines)[[:space:]]*\\("
    ),
    lines,
    value = TRUE
  )
  cli_conditions <- grep(
    "cli::cli_(abort|warn)[[:space:]]*\\(",
    lines,
    value = TRUE
  )
  direct_rlang <- grep(
    "rlang::(abort|warn)[[:space:]]*\\(",
    lines,
    value = TRUE
  )

  expect_length(base_output, 0L)
  expect_length(cli_conditions, 0L)
  expect_length(direct_rlang, 2L)
})
