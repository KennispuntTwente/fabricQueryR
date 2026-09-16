test_that("required Functions coverage rejects completely absent fixtures", {
  withr::local_envvar(c(
    FABRIC_TEST_REQUIRED_FEATURES = "functions",
    FABRIC_TEST_FUNCTION_SCALAR_URL = NA,
    FABRIC_TEST_FUNCTION_STRUCTURED_URL = NA,
    FABRIC_TEST_FUNCTION_ERROR_URL = NA
  ))
  expect_error(
    fabric_test_function_url("FABRIC_TEST_FUNCTION_SCALAR_URL"),
    "Published Fabric function fixtures are not configured",
    fixed = TRUE
  )
})
