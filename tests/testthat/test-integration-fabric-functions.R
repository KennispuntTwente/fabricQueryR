# Fabric integration coverage: invoking published user data functions
# Live coverage is opt-in because Microsoft's current
# item-management API does not support the service principal used to provision
# the disposable CI sandbox. Point these variables at three published public
# functions with the signatures exercised below and provide the normal Power BI
# integration token. Local delegated runs can provision the fixture in a Fabric
# workspace without weakening the package's standard integration environment

test_that("Fabric public functions return scalar and structured live outputs", {
  scalar_url <- fabric_test_optional_environment(
    "FABRIC_TEST_FUNCTION_SCALAR_URL",
    "Live Fabric user data function coverage"
  )
  structured_url <- fabric_test_optional_environment(
    "FABRIC_TEST_FUNCTION_STRUCTURED_URL",
    "Live Fabric user data function coverage"
  )
  token <- fabric_test_token_provider()

  scalar <- fabric_function_invoke(
    scalar_url,
    parameters = list(value = "fabricqueryr-live-scalar"),
    token = token,
    audience = .fabric_audience$power_bi
  )
  structured <- fabric_function_invoke(
    structured_url,
    parameters = list(
      label = "fabricqueryr-live-structured",
      values = I(c(2L, 3L, 5L)),
      metadata = list(active = TRUE, missing = NULL)
    ),
    token = token,
    audience = .fabric_audience$power_bi
  )

  expect_s3_class(scalar, "fabric_function_result")
  expect_identical(scalar$status, "Succeeded")
  expect_identical(scalar$output, "fabricqueryr-live-scalar")
  expect_true(fabric_is_guid(scalar$invocation_id))
  expect_length(scalar$errors, 0L)

  expect_s3_class(structured, "fabric_function_result")
  expect_identical(structured$status, "Succeeded")
  expect_identical(
    structured$output$label,
    "fabricqueryr-live-structured"
  )
  expect_equal(unlist(structured$output$values), c(2L, 3L, 5L))
  expect_identical(structured$output$total, 10L)
  expect_true(structured$output$metadata$active)
  expect_null(structured$output$metadata$missing)
  expect_true(fabric_is_guid(structured$invocation_id))
  expect_length(structured$errors, 0L)
})

test_that("Fabric UserThrownError remains an inspectable live result", {
  error_url <- fabric_test_optional_environment(
    "FABRIC_TEST_FUNCTION_ERROR_URL",
    "Live Fabric user data function error coverage"
  )
  token <- fabric_test_token_provider()

  result <- fabric_function_invoke(
    error_url,
    parameters = list(value = -1L),
    token = token,
    audience = .fabric_audience$power_bi
  )

  expect_s3_class(result, "fabric_function_result")
  expect_identical(result$http_status, 422L)
  expect_true(result$status %in% c("BadRequest", "Failed"))
  expect_true(fabric_is_guid(result$invocation_id))
  expect_length(result$errors, 1L)
  expect_true(result$errors[[1L]]$name %in% c("UserThrown", "UserThrownError"))
  expect_match(
    result$errors[[1L]]$message,
    "value must be non-negative",
    ignore.case = TRUE
  )
  expect_equal(result$errors[[1L]]$properties$value, -1L)
})
