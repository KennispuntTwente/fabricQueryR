test_that("fabric_function_invoke sends scalar and structured parameters", {
  captured <- NULL
  audiences <- character()
  httr2::local_mocked_responses(function(req) {
    captured <<- req
    function_test_response(
      function_success_body(
        output = list(
          text = "Ada",
          values = list(2L, 3L),
          metadata = list(active = TRUE)
        )
      ),
      url = req$url
    )
  })

  result <- fabric_function_invoke(
    function_test_url,
    parameters = list(
      text = "Ada",
      count = 2L,
      values = I(c(2L, 3L)),
      metadata = list(active = TRUE, note = NULL)
    ),
    timeout = 17,
    token = function(audience, force_refresh = FALSE) {
      audiences <<- c(audiences, audience)
      "function-token"
    }
  )

  expect_s3_class(result, "fabric_function_result")
  expect_identical(result$function_name, "echoInput")
  expect_identical(
    result$invocation_id,
    "c63f7f60-1ce4-4b30-9694-dcdcec871bba"
  )
  expect_identical(result$status, "Succeeded")
  expect_equal(result$output$metadata$active, TRUE)
  expect_length(result$errors, 0L)
  expect_identical(result$http_status, 200L)
  expect_identical(result$response$functionName, "echoInput")
  expect_identical(
    audiences,
    .fabric_audience$user_data_function
  )
  expect_equal(captured$options$timeout_ms, 17000)
  expect_identical(captured$headers$accept, "application/json")
  expect_identical(captured$body$content_type, "application/json")

  request_body <- jsonlite::fromJSON(
    rawToChar(captured$body$data),
    simplifyVector = FALSE
  )
  expect_identical(request_body$text, "Ada")
  expect_identical(request_body$count, 2L)
  expect_equal(unlist(request_body$values), c(2L, 3L))
  expect_true(request_body$metadata$active)
  expect_null(request_body$metadata$note)
})

test_that("function parameters support empty, named-vector, and data-frame objects", {
  expect_identical(function_serialize_parameters(list()), "{}")
  expect_identical(
    function_serialize_parameters(c(first = 1L, second = 2L)),
    '{"first":1,"second":2}'
  )
  expect_identical(
    function_serialize_parameters(data.frame(
      id = 1:2,
      label = c("a", "b")
    )),
    '{"id":[1,2],"label":["a","b"]}'
  )
})

test_that("public function URLs enforce the documented trusted route", {
  expect_identical(function_validate_url(function_test_url), function_test_url)
  expect_identical(
    function_validate_url(paste0(function_test_url, "/")),
    function_test_url
  )
  expect_identical(
    function_validate_url(
      sub(
        "api.fabric.microsoft.com",
        "trusted.example",
        function_test_url,
        fixed = TRUE
      ),
      allow_custom_endpoint = TRUE
    ),
    sub(
      "api.fabric.microsoft.com",
      "trusted.example",
      function_test_url,
      fixed = TRUE
    )
  )

  expect_error(
    function_validate_url(sub(
      "https",
      "http",
      function_test_url,
      fixed = TRUE
    )),
    "valid HTTPS",
    fixed = TRUE
  )
  expect_error(
    function_validate_url(sub(
      "api.fabric.microsoft.com",
      "api.fabric.microsoft.com.attacker.example",
      function_test_url,
      fixed = TRUE
    )),
    "Microsoft Fabric endpoint",
    fixed = TRUE
  )
  expect_error(
    function_validate_url(paste0(function_test_url, "?token=secret")),
    "valid HTTPS",
    fixed = TRUE
  )
  expect_error(
    function_validate_url(sub("/invoke", "/status", function_test_url)),
    "documented public function invocation route",
    fixed = TRUE
  )
  expect_error(
    function_validate_url(sub("echoInput", "echo%2Finput", function_test_url)),
    "documented public function invocation route",
    fixed = TRUE
  )
  expect_error(
    fabric_function_invoke(
      list(
        id = "5b218778-e7a5-4d73-8187-f10824047715",
        type = "UserDataFunction"
      ),
      token = "token"
    ),
    "function_url must be one non-empty string",
    fixed = TRUE
  )
})

test_that("function execution failures remain inspectable results", {
  responses <- list(
    function_test_response(
      list(
        functionName = "echoInput",
        invocationId = "disabled-id",
        status = "BadRequest",
        output = NULL,
        errors = list(list(
          name = "PublicAccessDisabled",
          message = "Public access is disabled",
          properties = list(setting = "isPublicEndpointEnabled")
        ))
      ),
      status = 400L
    ),
    function_test_response(
      list(
        functionName = "raiseValidation",
        invocationId = "user-error-id",
        status = "Failed",
        output = NULL,
        errors = list(list(
          errorCode = "UserThrown",
          message = "Value is invalid",
          properties = list(value = -1L)
        ))
      ),
      status = 422L
    ),
    function_test_response(
      list(
        functionName = "slowFunction",
        invocationId = "timeout-id",
        status = "Timeout",
        output = NULL,
        errors = list(list(
          name = "FunctionTimeout",
          message = "Execution exceeded the public endpoint limit"
        ))
      ),
      status = 408L
    ),
    function_test_response(
      list(
        functionName = "largeFunction",
        invocationId = "large-id",
        status = "ResponseTooLarge",
        output = NULL,
        errors = list(list(
          name = "ResponseTooLarge",
          message = "The return value exceeded the service limit"
        ))
      ),
      status = 403L
    )
  )
  index <- 0L
  httr2::local_mocked_responses(function(req) {
    index <<- index + 1L
    responses[[index]]
  })

  disabled <- fabric_function_invoke(function_test_url, token = "token")
  user_error <- fabric_function_invoke(function_test_url, token = "token")
  timed_out <- fabric_function_invoke(function_test_url, token = "token")
  too_large <- fabric_function_invoke(function_test_url, token = "token")

  expect_identical(disabled$status, "BadRequest")
  expect_identical(disabled$http_status, 400L)
  expect_identical(disabled$errors[[1L]]$name, "PublicAccessDisabled")
  expect_identical(user_error$status, "Failed")
  expect_identical(user_error$http_status, 422L)
  expect_identical(user_error$errors[[1L]]$name, "UserThrown")
  expect_identical(user_error$errors[[1L]]$errorCode, "UserThrown")
  expect_equal(user_error$errors[[1L]]$properties$value, -1L)
  expect_identical(timed_out$status, "Timeout")
  expect_identical(timed_out$http_status, 408L)
  expect_identical(too_large$status, "ResponseTooLarge")
  expect_identical(too_large$http_status, 403L)
})

test_that("function responses preserve future fields and exact large integers", {
  local_mocked_bindings(
    .httr2_perform = function(req, ...) {
      function_test_response(charToRaw(paste0(
        '{"functionName":"futureFunction",',
        '"invocationId":"future-id","status":"FutureStatus",',
        '"output":{"identifier":9007199254740993},',
        '"errors":[{"errorCode":"FutureError"}],',
        '"future":{"mode":"new"}}'
      )))
    }
  )

  result <- fabric_function_invoke(function_test_url, token = "token")

  expect_identical(result$status, "FutureStatus")
  expect_identical(result$output$identifier, "9007199254740993")
  expect_identical(result$errors[[1L]]$name, "FutureError")
  expect_identical(result$response$future$mode, "new")
})

test_that("function retries require an explicit idempotency decision", {
  calls <- 0L
  httr2::local_mocked_responses(function(req) {
    calls <<- calls + 1L
    function_test_response(
      list(errorCode = "TooManyRequests", message = "try later"),
      status = 429L,
      headers = list("retry-after" = "0")
    )
  })
  expect_error(
    fabric_function_invoke(function_test_url, token = "token"),
    class = "fabric_http_error"
  )
  expect_identical(calls, 1L)

  calls <- 0L
  httr2::local_mocked_responses(function(req) {
    calls <<- calls + 1L
    if (calls == 1L) {
      return(function_test_response(
        list(errorCode = "TooManyRequests", message = "try later"),
        status = 429L,
        headers = list("retry-after" = "0")
      ))
    }
    function_test_response(function_success_body(), url = req$url)
  })

  result <- fabric_function_invoke(
    function_test_url,
    token = "token",
    idempotent = TRUE
  )
  expect_identical(calls, 2L)
  expect_identical(result$status, "Succeeded")
})

test_that("function response limits check headers and actual body bytes", {
  httr2::local_mocked_responses(function(req) {
    function_test_response(
      function_success_body(),
      headers = list("content-length" = "1000")
    )
  })
  header_error <- expect_error(
    fabric_function_invoke(
      function_test_url,
      token = "token",
      max_response_bytes = 100
    ),
    class = "fabric_function_response_too_large"
  )
  expect_identical(header_error$response_bytes, 1000)
  expect_identical(header_error$max_response_bytes, 100)

  httr2::local_mocked_responses(function(req) {
    function_test_response(function_success_body(output = strrep("x", 200)))
  })
  body_error <- expect_error(
    fabric_function_invoke(
      function_test_url,
      token = "token",
      max_response_bytes = 100
    ),
    class = "fabric_function_response_too_large"
  )
  expect_gt(body_error$response_bytes, 100)
})

test_that("function results and conditions redact bearer tokens and secrets", {
  secret <- "function-super-secret"
  httr2::local_mocked_responses(function(req) {
    function_test_response(function_success_body(
      output = list(
        token = secret,
        nested = list(
          safe = "visible",
          message = paste("Bearer", secret)
        )
      )
    ))
  })

  result <- fabric_function_invoke(function_test_url, token = "request-token")
  expect_identical(result$output$token, "<redacted>")
  expect_identical(result$output$nested$safe, "visible")
  expect_identical(result$output$nested$message, "Bearer <redacted>")
  expect_false(grepl(secret, paste(capture.output(str(result)), collapse = "")))

  httr2::local_mocked_responses(function(req) {
    function_test_response(
      list(
        message = paste("Authorization: Bearer", secret),
        token = secret
      ),
      status = 401L
    )
  })
  error <- expect_error(
    fabric_function_invoke(function_test_url, token = "request-token"),
    class = "fabric_http_error"
  )
  expect_false(grepl(secret, conditionMessage(error), fixed = TRUE))
  expect_identical(error$response_metadata$body$token, "<redacted>")
})

test_that("function authentication chooses flow-appropriate audiences", {
  calls <- list()
  local_mocked_bindings(
    get_azure_token = function(...) {
      calls[[length(calls) + 1L]] <<- list(...)
      function_fake_azure_token()
    },
    .package = "AzureAuth"
  )
  httr2::local_mocked_responses(function(req) {
    function_test_response(function_success_body(), url = req$url)
  })

  fabric_function_invoke(
    function_test_url,
    tenant_id = "tenant",
    client_id = "client",
    auth_args = list(password = "secret", auth_type = "client_credentials")
  )
  fabric_function_invoke(
    function_test_url,
    tenant_id = "tenant",
    client_id = "client",
    auth_args = list(auth_type = "device_code", use_cache = FALSE)
  )

  expect_identical(calls[[1L]]$resource, .fabric_audience$power_bi)
  expect_equal(
    calls[[2L]]$resource,
    c(.fabric_audience$user_data_function, "offline_access")
  )
})

test_that("function invocation validates arguments before authentication", {
  expect_identical(formals(fabric_function_invoke)$timeout, 110)
  expect_identical(
    formals(fabric_function_invoke)$max_response_bytes,
    quote(.fabric_function_response_limit)
  )

  invalid <- list(
    list(parameters = list(1L), pattern = "unique, non-empty names"),
    list(
      parameters = structure(list(1L, 2L), names = c("x", "x")),
      pattern = "unique, non-empty names"
    ),
    list(parameters = "scalar", pattern = "named R object")
  )
  for (case in invalid) {
    expect_error(
      fabric_function_invoke(
        function_test_url,
        parameters = case$parameters,
        token = "token"
      ),
      case$pattern,
      fixed = TRUE
    )
  }
  expect_error(
    fabric_function_invoke(function_test_url, timeout = 0, token = "token"),
    "timeout must be one positive number",
    fixed = TRUE
  )
  expect_error(
    fabric_function_invoke(
      function_test_url,
      idempotent = NA,
      token = "token"
    ),
    "idempotent must be TRUE or FALSE",
    fixed = TRUE
  )
  expect_error(
    fabric_function_invoke(
      function_test_url,
      max_response_bytes = 1.5,
      token = "token"
    ),
    "max_response_bytes must be one positive whole number",
    fixed = TRUE
  )

  request_error <- expect_error(
    fabric_function_invoke(
      function_test_url,
      parameters = list(
        value = strrep(
          "x",
          .fabric_function_request_limit + 1L
        )
      ),
      token = function(...) stop("authentication should not run")
    ),
    class = "fabric_function_request_too_large"
  )
  expect_gt(request_error$request_bytes, .fabric_function_request_limit)
})

test_that("malformed function responses keep standard safe HTTP behavior", {
  httr2::local_mocked_responses(function(req) {
    function_test_response(list(message = "service unavailable"), status = 500L)
  })
  expect_error(
    fabric_function_invoke(function_test_url, token = "token"),
    class = "fabric_http_error"
  )

  httr2::local_mocked_responses(function(req) {
    function_test_response(function_success_body(), status = 401L)
  })
  expect_error(
    fabric_function_invoke(function_test_url, token = "token"),
    class = "fabric_http_error"
  )

  httr2::local_mocked_responses(function(req) {
    function_test_response(list(status = "Succeeded"), status = 200L)
  })
  error <- expect_error(
    fabric_function_invoke(function_test_url, token = "token"),
    class = "fabric_function_response_error"
  )
  expect_identical(error$response_metadata$status, 200L)
})

test_that("UDF documentation separates invocation and managed connections", {
  source_root <- test_path("..", "..")
  if (!dir.exists(file.path(source_root, "vignettes"))) {
    skip("Package vignettes are not available in installed test runs")
  }

  documentation <- paste(
    readLines(
      file.path(source_root, "R", "fabric_function_invoke.R"),
      warn = FALSE
    ),
    readLines(
      file.path(source_root, "vignettes", "user-data-functions.Rmd"),
      warn = FALSE
    ),
    collapse = "\n"
  )

  expect_match(documentation, "public invocation endpoint", fixed = TRUE)
  expect_match(documentation, "UDF-managed connections", fixed = TRUE)
  expect_identical(
    grepl("corresponding application permission", documentation, fixed = TRUE),
    FALSE
  )
})
