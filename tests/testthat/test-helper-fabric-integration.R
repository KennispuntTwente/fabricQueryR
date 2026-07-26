test_that("required Fabric integration mode fails instead of skipping", {
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

test_that("live token providers acquire by audience and cache until refresh", {
  calls <- character()
  provider <- fabric_test_token_provider(function(audience) {
    calls <<- c(calls, audience)
    paste0("token-", length(calls))
  })

  expect_identical(provider("scope-a"), "token-1")
  expect_identical(provider("scope-a"), "token-1")
  expect_identical(provider("scope-b"), "token-2")
  expect_identical(
    provider("scope-a", force_refresh = TRUE),
    "token-3"
  )
  expect_identical(calls, c("scope-a", "scope-b", "scope-a"))
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
