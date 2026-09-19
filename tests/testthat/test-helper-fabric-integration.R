test_that("repeatable Livy fixtures retry submission and wait on the accepted ID once", {
  submissions <- 0L
  waits <- 0L
  result <- list(id = 42L, output = list(status = "ok"))
  session <- list(submit = function(code, kind) {
    submissions <<- submissions + 1L
    expect_identical(code, "print(42)")
    expect_identical(kind, "pyspark")
    if (submissions == 1L) {
      rlang::abort("Unavailable", class = "fabric_http_error", status = 503L)
    }
    list(
      wait = function(timeout, poll_interval) {
        waits <<- waits + 1L
        expect_identical(timeout, 300)
        expect_identical(poll_interval, 2)
      },
      result = function(refresh) {
        expect_identical(refresh, FALSE)
        result
      }
    )
  })

  expect_message(
    actual <- fabric_test_livy_run_idempotent(session, "print(42)", delay = 0),
    "HTTP 503 (attempt 2/3)",
    fixed = TRUE
  )
  expect_identical(actual, result)
  expect_identical(submissions, 2L)
  expect_identical(waits, 1L)
})

test_that("repeatable Livy fixtures preserve permanent and exhausted errors", {
  errors <- list(
    rlang::error_cnd("fabric_http_error", status = 400L),
    rlang::error_cnd("fabric_http_error", status = 403L),
    rlang::error_cnd("fabric_http_error", status = 503L, is_retriable = FALSE),
    rlang::error_cnd("fabric_livy_statement_error"),
    rlang::error_cnd("fabric_http_error", status = 503L)
  )
  expected_attempts <- c(1L, 1L, 1L, 1L, 3L)
  for (index in seq_along(errors)) {
    submissions <- 0L
    session <- list(submit = function(...) {
      submissions <<- submissions + 1L
      stop(errors[[index]])
    })
    error <- tryCatch(
      suppressMessages(fabric_test_livy_run_idempotent(
        session,
        "print(42)",
        delay = 0
      )),
      error = identity
    )
    expect_identical(error, errors[[index]])
    expect_identical(submissions, expected_attempts[[index]])
  }
})

test_that("repeatable Livy fixtures never resubmit after an accepted statement fails", {
  for (class in c("fabric_http_error", "fabric_livy_statement_error")) {
    failure <- rlang::error_cnd(class, status = 503L)
    submissions <- 0L
    session <- list(submit = function(...) {
      submissions <<- submissions + 1L
      list(wait = function(...) stop(failure))
    })
    error <- tryCatch(
      fabric_test_livy_run_idempotent(session, "print(42)", delay = 0),
      error = identity
    )
    expect_identical(error, failure)
    expect_identical(submissions, 1L)
  }
})

test_that("required Fabric integration mode fails instead of skipping", {
  withr::local_options(fabricQueryR.integration_token_provider = NULL)
  old_required <- Sys.getenv("FABRIC_INTEGRATION_REQUIRED", unset = NA)
  old_manifest <- Sys.getenv("FABRIC_TEST_MANIFEST", unset = NA)
  old_token <- Sys.getenv("FABRICQUERYR_TEST_MISSING_TOKEN", unset = NA)
  on.exit(
    {
      if (is.na(old_required)) {
        Sys.unsetenv("FABRIC_INTEGRATION_REQUIRED")
      } else {
        Sys.setenv(FABRIC_INTEGRATION_REQUIRED = old_required)
      }
      if (is.na(old_manifest)) {
        Sys.unsetenv("FABRIC_TEST_MANIFEST")
      } else {
        Sys.setenv(FABRIC_TEST_MANIFEST = old_manifest)
      }
      if (is.na(old_token)) {
        Sys.unsetenv("FABRICQUERYR_TEST_MISSING_TOKEN")
      } else {
        Sys.setenv(FABRICQUERYR_TEST_MISSING_TOKEN = old_token)
      }
    },
    add = TRUE
  )
  Sys.setenv(
    FABRIC_INTEGRATION_REQUIRED = "true",
    FABRIC_TEST_MANIFEST = tempfile("missing-manifest-")
  )
  Sys.unsetenv("FABRICQUERYR_TEST_MISSING_TOKEN")

  expect_error(
    fabric_test_manifest(),
    "Fabric integration manifest not found",
    fixed = TRUE
  )
  expect_error(
    fabric_test_token("FABRICQUERYR_TEST_MISSING_TOKEN"),
    "Fabric integration token not set",
    fixed = TRUE
  )
})

test_that("required live fixtures fail when CI configuration is incomplete", {
  withr::local_envvar(c(
    FABRIC_INTEGRATION_REQUIRED = "true",
    FABRICQUERYR_TEST_MISSING_FIXTURE = NA
  ))

  error <- tryCatch(
    fabric_test_required_environment(
      "FABRICQUERYR_TEST_MISSING_FIXTURE",
      "Live fixture"
    ),
    error = identity
  )

  expect_s3_class(error, "error")
  expect_match(
    conditionMessage(error),
    "Live fixture requires FABRICQUERYR_TEST_MISSING_FIXTURE",
    fixed = TRUE
  )
})

test_that("opt-in live fixtures skip even in required integration mode", {
  withr::local_envvar(c(
    FABRIC_INTEGRATION_REQUIRED = "true",
    FABRICQUERYR_TEST_MISSING_FIXTURE = NA
  ))

  condition <- tryCatch(
    fabric_test_optional_environment(
      "FABRICQUERYR_TEST_MISSING_FIXTURE",
      "Opt-in fixture"
    ),
    skip = identity
  )

  expect_s3_class(condition, "skip")
  expect_match(condition$message, "Opt-in fixture is opt-in")
})

test_that("required runtime lanes reject missing or inconsistent configuration", {
  withr::local_envvar(c(
    FABRIC_INTEGRATION_REQUIRED = "true",
    FABRIC_SPARK_RUNTIME_LANE = "typo",
    FABRIC_SPARK_RUNTIME_VERSION = "1.3"
  ))
  expect_error(fabric_test_runtime_lane("core"), "must be 'core' or 'runtime2'")

  withr::local_envvar(c(
    FABRIC_SPARK_RUNTIME_LANE = "preview",
    FABRIC_SPARK_RUNTIME_VERSION = "1.3"
  ))
  expect_error(fabric_test_runtime_lane("runtime2"), "requires version 2.0")
})

test_that("the preview runtime lane remains a Runtime 2.0 alias", {
  withr::local_envvar(c(
    FABRIC_INTEGRATION_REQUIRED = "true",
    FABRIC_SPARK_RUNTIME_LANE = "preview",
    FABRIC_SPARK_RUNTIME_VERSION = "2.0"
  ))

  expect_no_error(fabric_test_runtime_lane("runtime2"))
})

test_that("live token providers delegate expiry checks and forced refresh", {
  calls <- character()
  refreshes <- logical()
  provider <- fabric_test_token_provider(function(audience, force_refresh) {
    calls <<- c(calls, audience)
    refreshes <<- c(refreshes, force_refresh)
    paste0("token-", length(calls))
  })

  expect_identical(provider("scope-a"), "token-1")
  expect_identical(provider("scope-a"), "token-2")
  expect_identical(provider("scope-b"), "token-3")
  expect_identical(
    provider("scope-a", force_refresh = TRUE),
    "token-4"
  )
  expect_identical(calls, c("scope-a", "scope-a", "scope-b", "scope-a"))
  expect_identical(refreshes, c(FALSE, FALSE, FALSE, TRUE))
})

test_that("live token providers use the token provisioned for each audience", {
  expect_identical(
    fabric_test_token_variable(.fabric_audience$fabric),
    "FABRIC_TEST_API_TOKEN"
  )
  expect_identical(
    fabric_test_token_variable(.fabric_audience$power_bi),
    "FABRIC_TEST_PBI_TOKEN"
  )
  expect_identical(
    fabric_test_token_variable(.fabric_audience$sql),
    "FABRIC_TEST_SQL_TOKEN"
  )
  expect_identical(
    fabric_test_token_variable(.fabric_audience$storage),
    "FABRIC_TEST_STORAGE_TOKEN"
  )
  expect_identical(
    fabric_test_token_variable(.fabric_audience$kusto),
    "FABRIC_TEST_KUSTO_TOKEN"
  )
  expect_error(
    fabric_test_token_variable(.fabric_audience$graphql),
    "No provisioned Fabric integration token",
    fixed = TRUE
  )
  expect_identical(
    fabric_test_token_audience("FABRIC_TEST_API_TOKEN"),
    .fabric_audience$fabric
  )
  expect_error(
    fabric_test_token_audience("FABRIC_TEST_UNKNOWN_TOKEN"),
    "No Fabric integration audience",
    fixed = TRUE
  )
})

test_that("local integration token provider takes precedence over environment", {
  old_provider <- getOption("fabricQueryR.integration_token_provider")
  old_token <- Sys.getenv("FABRIC_TEST_API_TOKEN", unset = NA)
  on.exit(
    {
      options(fabricQueryR.integration_token_provider = old_provider)
      if (is.na(old_token)) {
        Sys.unsetenv("FABRIC_TEST_API_TOKEN")
      } else {
        Sys.setenv(FABRIC_TEST_API_TOKEN = old_token)
      }
    },
    add = TRUE
  )
  Sys.setenv(FABRIC_TEST_API_TOKEN = "environment-token")
  options(
    fabricQueryR.integration_token_provider = function(audience) {
      paste0("provider-token:", audience)
    }
  )

  expect_identical(
    fabric_test_token("FABRIC_TEST_API_TOKEN"),
    paste0("provider-token:", .fabric_audience$fabric)
  )
})

test_that("local AzureAuth context enables the acquisition integration test", {
  old_config <- getOption("fabricQueryR.integration_auth_config")
  on.exit(
    options(fabricQueryR.integration_auth_config = old_config),
    add = TRUE
  )
  expected <- list(
    tenant_id = "tenant-id",
    client_id = "client-id",
    auth_args = list(use_cache = TRUE)
  )
  options(fabricQueryR.integration_auth_config = expected)

  expect_identical(fabric_test_azure_auth_config(), expected)
})

test_that("delegated auth detection excludes application credentials", {
  configs <- list(
    delegated = list(auth_args = list(use_cache = TRUE)),
    client_secret = list(
      auth_args = list(
        auth_type = "client_credentials",
        password = "secret"
      )
    ),
    certificate = list(auth_args = list(certificate = "certificate.pem")),
    missing = NULL
  )

  expect_identical(
    vapply(configs, fabric_test_is_delegated_auth, logical(1)),
    c(
      delegated = TRUE,
      client_secret = FALSE,
      certificate = FALSE,
      missing = FALSE
    )
  )
})

test_that("required delegated coverage rejects application authentication", {
  withr::local_envvar(FABRIC_DELEGATED_INTEGRATION_REQUIRED = "true")
  withr::local_options(
    fabricQueryR.integration_auth_config = list(
      tenant_id = "tenant-id",
      client_id = "client-id",
      auth_args = list(
        auth_type = "client_credentials",
        password = "secret"
      )
    )
  )

  error <- tryCatch(
    fabric_test_delegated_auth_config(),
    error = identity
  )

  expect_s3_class(error, "error")
  expect_match(
    conditionMessage(error),
    "Delegated Fabric integration requires an interactive user identity",
    fixed = TRUE
  )
})

test_that("required integration mode rejects missing AzureAuth credentials", {
  old_required <- Sys.getenv("FABRIC_INTEGRATION_REQUIRED", unset = NA)
  old_secret <- Sys.getenv("FABRIC_TEST_AUTH_CLIENT_SECRET", unset = NA)
  old_config <- getOption("fabricQueryR.integration_auth_config")
  on.exit(
    {
      options(fabricQueryR.integration_auth_config = old_config)
      if (is.na(old_required)) {
        Sys.unsetenv("FABRIC_INTEGRATION_REQUIRED")
      } else {
        Sys.setenv(FABRIC_INTEGRATION_REQUIRED = old_required)
      }
      if (is.na(old_secret)) {
        Sys.unsetenv("FABRIC_TEST_AUTH_CLIENT_SECRET")
      } else {
        Sys.setenv(FABRIC_TEST_AUTH_CLIENT_SECRET = old_secret)
      }
    },
    add = TRUE
  )
  options(fabricQueryR.integration_auth_config = NULL)
  Sys.setenv(FABRIC_INTEGRATION_REQUIRED = "true")
  Sys.unsetenv("FABRIC_TEST_AUTH_CLIENT_SECRET")

  expect_error(
    fabric_test_azure_auth_config(),
    "FABRIC_TEST_AUTH_CLIENT_SECRET",
    fixed = TRUE
  )
})

test_that("the default manifest path resolves from nested test directories", {
  root <- tempfile("fabricqueryr-root-")
  nested <- file.path(root, "tests", "testthat")
  dir.create(nested, recursive = TRUE)
  on.exit(unlink(root, recursive = TRUE), add = TRUE)
  writeLines(
    c("Package: fabricQueryR", "Version: 0.0.0"),
    file.path(root, "DESCRIPTION")
  )

  expected_root <- normalizePath(root, winslash = "/", mustWork = TRUE)
  expect_identical(fabric_test_repository_root(nested), expected_root)
  expect_identical(
    fabric_test_manifest_path(start = nested, configured = ""),
    file.path(expected_root, ".fabric-test-manifest.json")
  )
  expect_identical(
    fabric_test_manifest_path(
      start = nested,
      configured = "explicit-manifest.json"
    ),
    "explicit-manifest.json"
  )
})

test_that("manifest lookup can skip cleanly outside a source checkout", {
  installed_tests <- tempfile("fabricqueryr-installed-tests-")
  dir.create(installed_tests)
  on.exit(unlink(installed_tests, recursive = TRUE), add = TRUE)

  expected <- file.path(
    normalizePath(installed_tests, winslash = "/", mustWork = TRUE),
    ".fabric-test-manifest.json"
  )
  expect_identical(
    fabric_test_manifest_path(
      start = installed_tests,
      configured = ""
    ),
    expected
  )
})
