# Report executed coverage separately from optional tests that only skipped.
fabric_ci_integration_summary <- function(results, filter) {
  rows <- as.data.frame(results)
  exercised <- sum(!rows$skipped & rows$passed > 0L)
  skipped <- sum(rows$skipped)
  failed <- sum(rows$failed) + sum(rows$error)
  status <- if (failed > 0L) {
    "FAILED"
  } else if (exercised == 0L) {
    "NOT EXERCISED"
  } else if (skipped > 0L) {
    "PARTIALLY EXERCISED"
  } else {
    "EXERCISED"
  }
  list(
    status = status,
    failed = failed,
    markdown = c(
      paste0("### ", filter, ": ", status),
      "",
      paste0(
        "Tests exercised: ",
        exercised,
        "; tests skipped: ",
        skipped,
        "; passed assertions: ",
        sum(rows$passed),
        "; failures: ",
        failed,
        "."
      ),
      "",
      if (skipped > 0L) paste0("- Skipped: ", rows$test[rows$skipped]),
      ""
    )
  )
}

# Only the unconfigured UDF lane is optional; supplying any fixture makes it required.
fabric_ci_lane_required <- function(filter) {
  !identical(filter, "integration-fabric-functions") ||
    any(nzchar(Sys.getenv(c(
      "FABRIC_TEST_FUNCTION_SCALAR_URL",
      "FABRIC_TEST_FUNCTION_STRUCTURED_URL",
      "FABRIC_TEST_FUNCTION_ERROR_URL"
    ))))
}

run_fabric_ci_integration <- function(
  filter = Sys.getenv("FABRIC_TEST_FILTER"),
  required = fabric_ci_lane_required(filter),
  .test = testthat::test_local
) {
  results <- .test(filter = filter, stop_on_failure = FALSE)
  summary <- fabric_ci_integration_summary(results, filter)
  path <- Sys.getenv("GITHUB_STEP_SUMMARY")
  if (nzchar(path)) {
    cat(summary$markdown, file = path, sep = "\n", append = TRUE)
  }
  if (summary$status %in% c("NOT EXERCISED", "PARTIALLY EXERCISED")) {
    cat(
      "::warning::",
      filter,
      ": ",
      summary$status,
      "; skipped tests are not live validation.\n",
      sep = ""
    )
  }
  if (summary$failed > 0L) {
    stop("Fabric integration tests failed", call. = FALSE)
  }
  if (identical(summary$status, "NOT EXERCISED")) {
    if (isTRUE(required)) {
      stop(
        "Required Fabric integration lane executed zero tests",
        call. = FALSE
      )
    }
    cat(
      "Optional Fabric integration lane unavailable: no fixtures configured.\n"
    )
  }
  invisible(results)
}
