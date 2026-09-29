# Exercise progress lifecycles without rendering polling spinners in test logs.
# Restore the caller's handlers when the test run ends.
withr::local_options(
  cli.progress_handlers_only = character(),
  .local_envir = testthat::teardown_env()
)
