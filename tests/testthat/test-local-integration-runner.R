test_that("local client secrets require an explicit application identity", {
  root <- tryCatch(
    fabric_test_repository_root(),
    error = function(error) {
      skip("Local integration runner tests require a repository checkout")
    }
  )
  runner <- file.path(
    root,
    "tools",
    "fabric-sandbox",
    "local-integration.R"
  )
  if (!file.exists(runner)) {
    skip("Local integration runner tests require a repository checkout")
  }
  environment <- new.env(parent = globalenv())
  sys.source(runner, envir = environment)

  expect_error(
    environment$fabric_local_validate_secret_identity(
      tenant_id = "",
      client_id = "",
      client_secret = "secret",
      auth_args = list()
    ),
    "cached AzureAuth identities are never combined"
  )
  expect_error(
    environment$fabric_local_validate_secret_identity(
      tenant_id = "tenant",
      client_id = "",
      client_secret = "secret",
      auth_args = list()
    ),
    "FABRICQUERYR_CLIENT_ID"
  )
  expect_no_error(
    environment$fabric_local_validate_secret_identity(
      tenant_id = "tenant",
      client_id = "client",
      client_secret = "secret",
      auth_args = list()
    )
  )
  expect_no_error(
    environment$fabric_local_validate_secret_identity(
      tenant_id = "",
      client_id = "",
      client_secret = "secret",
      auth_args = list(auth_type = "device_code")
    )
  )
})
