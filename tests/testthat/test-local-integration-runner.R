test_that("local client secrets require an explicit application identity", {
  runner <- file.path(
    fabric_test_repository_root(),
    "tools",
    "fabric-sandbox",
    "local-integration.R"
  )
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
