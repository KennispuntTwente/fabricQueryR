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
