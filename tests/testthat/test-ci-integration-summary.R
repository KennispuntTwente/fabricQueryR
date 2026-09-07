test_that("CI runner exit status enforces required execution", {
  helper <- test_path("..", "..", "tools", "fabric-sandbox", "ci-integration.R")
  skip_if_not(file.exists(helper), "tools/ is excluded from the built package")
  skip_if_not_installed("processx")
  withr::local_envvar(LC_ALL = "C")
  helper <- normalizePath(helper, winslash = "/")
  directory <- withr::local_tempdir()
  test_file <- file.path(directory, "test-lane.R")
  script <- withr::local_tempfile(fileext = ".R")
  for (scenario in c("required", "optional", "configured", "pass", "failure")) {
    writeLines(
      switch(
        scenario,
        pass = 'testthat::test_that("live", testthat::expect_equal(1, 1))',
        failure = 'testthat::test_that("live", testthat::expect_equal(1, 2))',
        'testthat::test_that("unavailable", testthat::skip("fixture absent"))'
      ),
      test_file
    )
    writeLines(
      c(
        paste0(".libPaths(", paste(deparse(.libPaths()), collapse = "\n"), ")"),
        paste0("source(", deparse(helper), ")"),
        'Sys.unsetenv(c("GITHUB_STEP_SUMMARY", "FABRIC_TEST_FUNCTION_SCALAR_URL", "FABRIC_TEST_FUNCTION_STRUCTURED_URL", "FABRIC_TEST_FUNCTION_ERROR_URL"))',
        if (scenario == "configured") {
          'Sys.setenv(FABRIC_TEST_FUNCTION_SCALAR_URL = "https://function.test")'
        },
        paste0(
          "run_fabric_ci_integration(filter = ",
          deparse(
            if (scenario %in% c("optional", "configured")) {
              "integration-fabric-functions"
            } else {
              "integration-fabric-livy"
            }
          ),
          ","
        ),
        paste0(
          ".test = function(...) testthat::test_dir(",
          deparse(normalizePath(directory, winslash = "/")),
          ", stop_on_failure = FALSE))"
        )
      ),
      script
    )
    result <- processx::run(
      file.path(R.home("bin"), "Rscript"),
      c("--vanilla", script),
      error_on_status = FALSE
    )
    expect_identical(
      result$status,
      if (scenario %in% c("optional", "pass")) 0L else 1L,
      info = paste(result$stdout, result$stderr)
    )
    if (scenario %in% c("required", "configured")) {
      expect_match(result$stderr, "executed zero tests", fixed = TRUE)
    }
    if (scenario == "optional") {
      expect_match(
        result$stdout,
        "Optional Fabric integration lane unavailable",
        fixed = TRUE
      )
    }
  }
})

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
