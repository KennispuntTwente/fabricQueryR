test_that("the check entry point keeps unit coverage and isolates external suites", {
  entry <- parse(test_path("..", "testthat.R"))
  runner <- new.env(parent = baseenv())
  runner$library <- function(...) invisible(NULL)
  runner$test_check <- function(package, ...) list(package = package, ...)
  cases <- c(
    "fabric_sql_connect",
    "fabric_jobs",
    "fabric_livy",
    "http-auth",
    "integration-fabric-sql",
    "integration-fabric-future-workload",
    "delta-rs-oracle",
    "local-integration-runner",
    "vignettes"
  )

  for (value in c(NA_character_, "false")) {
    withr::local_envvar(NOT_CRAN = value)
    args <- eval(entry, runner)
    expect_identical(args$package, "fabricQueryR")
    expect_identical(args$invert, TRUE)
    expect_identical(
      cases[!grepl(args$filter, cases)],
      cases[seq_len(4L)]
    )
  }

  withr::local_envvar(NOT_CRAN = "true")
  expect_identical(eval(entry, runner), list(package = "fabricQueryR"))
})

test_that("CRAN skips adbi even when integration dependencies are mandatory", {
  withr::local_envvar(c(
    NOT_CRAN = "false",
    FABRIC_INTEGRATION_REQUIRED = "true"
  ))
  condition <- tryCatch(fabric_test_require_package("adbi"), skip = identity)
  expect_s3_class(condition, "skip")
})
