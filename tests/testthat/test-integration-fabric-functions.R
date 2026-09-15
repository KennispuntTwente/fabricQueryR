# Fabric integration coverage: invoking published user data functions
# Live coverage is opt-in because Microsoft's UDF-specific create and
# definition APIs do not support the service principal used by the persistent
# sandbox. Point these variables at three manually published public functions
# with the signatures in tests/fixtures/user-data-functions.py. These tests use
# automatic authentication and its default audience, including reused credentials.

test_that("Fabric public functions return scalar and structured live outputs", {
  scalar_url <- fabric_test_function_url("FABRIC_TEST_FUNCTION_SCALAR_URL")
  structured_url <- fabric_test_function_url(
    "FABRIC_TEST_FUNCTION_STRUCTURED_URL"
  )
  token <- fabric_test_function_credential()

  scalar <- fabric_function_invoke(
    scalar_url,
    parameters = list(value = "fabricqueryr-live-scalar"),
    token = token
  )
  structured <- fabric_function_invoke(
    structured_url,
    parameters = list(
      label = "fabricqueryr-live-structured",
      values = I(c(2L, 3L, 5L)),
      metadata = list(active = TRUE, missing = NULL)
    ),
    token = token
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
  error_url <- fabric_test_function_url("FABRIC_TEST_FUNCTION_ERROR_URL")
  token <- fabric_test_function_credential()

  result <- fabric_function_invoke(
    error_url,
    parameters = list(value = -1L),
    token = token
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
  expect_true("output" %in% names(result$response))
  expect_null(result$response$output)
})

test_that("Fabric functions acquire the default application audience directly", {
  url <- fabric_test_function_url("FABRIC_TEST_FUNCTION_SCALAR_URL")
  auth <- fabric_test_azure_auth_config()
  result <- do.call(
    fabric_function_invoke,
    c(
      list(
        function_url = url,
        parameters = list(value = "automatic-auth")
      ),
      auth
    )
  )
  expect_identical(result$status, "Succeeded")
  expect_identical(result$output, "automatic-auth")
})

test_that("Fabric function server failures remain inspectable execution envelopes", {
  url <- fabric_test_function_url("FABRIC_TEST_FUNCTION_ERROR_URL")
  result <- fabric_function_invoke(
    url,
    parameters = list(value = -2L),
    token = fabric_test_function_credential()
  )
  expect_s3_class(result, "fabric_function_result")
  expect_identical(result$http_status, 500L)
  expect_false(identical(result$status, "Succeeded"))
  expect_true(length(result$errors) >= 1L)
  expect_true("output" %in% names(result$response))
  expect_true(fabric_is_guid(result$invocation_id))
})

test_that("delegated Fabric function calls use the default Execute scope", {
  url <- fabric_test_function_url("FABRIC_TEST_FUNCTION_SCALAR_URL")
  auth <- fabric_test_delegated_auth_config()
  result <- do.call(
    fabric_function_invoke,
    c(
      list(
        function_url = url,
        parameters = list(value = "delegated-default-scope")
      ),
      auth
    )
  )
  expect_identical(result$status, "Succeeded")
  expect_identical(result$output, "delegated-default-scope")
})
