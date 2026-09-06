test_that("CI distinguishes skipped integration tests from executed coverage", {
  helper <- test_path("..", "..", "tools", "fabric-sandbox", "ci-integration.R")
  skip_if_not(
    file.exists(helper),
    "tools/ is intentionally excluded from the built package"
  )
  env <- new.env(parent = baseenv())
  sys.source(helper, env)
  rows <- data.frame(
    test = c("scalar function", "structured function"),
    skipped = c(TRUE, TRUE),
    passed = c(0L, 0L),
    failed = c(0L, 0L),
    error = c(FALSE, FALSE)
  )
  summary <- env$fabric_ci_integration_summary(rows, "functions")
  expect_identical(summary$status, "NOT EXERCISED")
  expect_contains(summary$markdown, "- Skipped: scalar function")
  rows$passed[1L] <- 1L
  expect_identical(
    env$fabric_ci_integration_summary(rows, "functions")$status,
    "NOT EXERCISED"
  )
  rows$skipped[1L] <- FALSE
  expect_identical(
    env$fabric_ci_integration_summary(rows, "functions")$status,
    "PARTIALLY EXERCISED"
  )
  rows$skipped[2L] <- FALSE
  rows$passed[2L] <- 1L
  expect_identical(
    env$fabric_ci_integration_summary(rows, "functions")$status,
    "EXERCISED"
  )
  rows$error[2L] <- TRUE
  expect_identical(
    env$fabric_ci_integration_summary(rows, "functions")$status,
    "FAILED"
  )
})
