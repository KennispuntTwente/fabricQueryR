# Once any function is configured, missing companion URLs are configuration
# errors in a required lane rather than silently disappearing coverage.
fabric_test_function_url <- function(variable) {
  variables <- c(
    "FABRIC_TEST_FUNCTION_SCALAR_URL",
    "FABRIC_TEST_FUNCTION_STRUCTURED_URL",
    "FABRIC_TEST_FUNCTION_ERROR_URL"
  )
  if (!any(nzchar(Sys.getenv(variables)))) {
    fabric_test_feature_unavailable(
      "functions",
      "Published Fabric function fixtures are not configured"
    )
  }
  value <- Sys.getenv(variable)
  if (!nzchar(value) && fabric_test_feature_required("functions")) {
    rlang::abort(paste("Missing published function fixture:", variable))
  }
  fabric_test_skip_or_fail(
    !nzchar(value),
    paste("Missing published function fixture:", variable)
  )
  fabric_test_manifest()
  value
}

fabric_test_function_credential <- function() {
  auth <- fabric_test_azure_auth_config()
  do.call(fabric_credential, auth)
}
