fabric_test_local_runner <- function() {
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
  environment
}

fabric_test_local_jwt <- function(claims) {
  payload <- jsonlite::toJSON(claims, auto_unbox = TRUE)
  payload <- jsonlite::base64_enc(charToRaw(payload))
  payload <- gsub("[[:space:]=]", "", payload)
  payload <- chartr("+/", "-_", payload)
  paste("header", payload, "signature", sep = ".")
}

test_that("local client secrets require an explicit application identity", {
  environment <- fabric_test_local_runner()

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

test_that("local auth context selects cached identities deterministically", {
  environment <- fabric_test_local_runner()
  environment$fabric_local_cached_contexts <- function() {
    list(list(tenant_id = "tenant-a", client_id = "client-a"))
  }

  expect_message(
    context <- environment$fabric_local_auth_context("", ""),
    "cached Fabric AzureAuth token"
  )
  expect_equal(
    context,
    list(tenant_id = "tenant-a", client_id = "client-a")
  )

  environment$fabric_local_cached_contexts <- function() {
    list(
      list(tenant_id = "tenant-a", client_id = "client-a"),
      list(tenant_id = "tenant-b", client_id = "client-b")
    )
  }
  expect_error(
    environment$fabric_local_auth_context("", ""),
    "Multiple cached Fabric identities"
  )
  expect_equal(
    environment$fabric_local_auth_context("tenant-b", ""),
    list(tenant_id = "tenant-b", client_id = "client-b")
  )
})

test_that("local auth arguments resolve explicit and environment flows", {
  environment <- fabric_test_local_runner()

  expect_message(
    resolved <- environment$fabric_local_resolve_auth_args(
      list(use_cache = FALSE),
      client_id = "client-id",
      client_secret = "client-secret"
    ),
    "local Fabric authentication"
  )
  expect_identical(resolved$password, "client-secret")
  expect_identical(resolved$auth_type, "client_credentials")
  expect_false(resolved$use_cache)

  explicit <- list(auth_type = "device_code", use_cache = FALSE)
  expect_identical(
    environment$fabric_local_resolve_auth_args(
      explicit,
      client_id = "client-id",
      client_secret = "ignored-secret"
    ),
    explicit
  )
  expect_error(
    environment$fabric_local_resolve_auth_args(
      list("device_code"),
      client_id = "client-id",
      client_secret = ""
    ),
    "fully named"
  )
  expect_error(
    environment$fabric_local_resolve_auth_args(
      list(),
      client_id = "",
      client_secret = "client-secret"
    ),
    "FABRICQUERYR_CLIENT_ID"
  )

  expect_true(environment$fabric_local_uses_client_credentials(resolved))
  expect_true(environment$fabric_local_uses_client_credentials(list(
    certificate = "certificate.pem"
  )))
  expect_false(environment$fabric_local_uses_client_credentials(explicit))
})

test_that("local cached token lookup filters and deduplicates identities", {
  environment <- fabric_test_local_runner()
  fabric_audience <- "https://api.fabric.microsoft.com/.default"
  first <- list(
    scope = fabric_audience,
    tenant = "tenant-a",
    client = list(client_id = "client-a"),
    marker = "first"
  )
  duplicate <- utils::modifyList(first, list(marker = "duplicate"))
  other <- list(
    resource = "https://storage.azure.com/.default",
    tenant = "tenant-b",
    client = list(client_id = "client-b")
  )
  local_mocked_bindings(
    list_azure_tokens = function() list(first, duplicate, other),
    .package = "AzureAuth"
  )

  expect_equal(
    environment$fabric_local_cached_contexts(),
    list(list(tenant_id = "tenant-a", client_id = "client-a"))
  )
  expect_identical(
    environment$fabric_local_cached_token(
      fabric_audience,
      "tenant-a",
      "client-a"
    )$marker,
    "first"
  )
  expect_null(environment$fabric_local_cached_token(
    fabric_audience,
    "tenant-b",
    "client-b"
  ))
})

test_that("local refresh-token exchange sends the requested audience", {
  environment <- fabric_test_local_runner()
  request <- NULL
  httr2::local_mocked_responses(function(req) {
    request <<- req
    operation_test_response(
      body = list(
        access_token = "new-access-token",
        refresh_token = "new-refresh-token",
        expires_in = 3600
      ),
      url = req$url
    )
  })

  audience <- "https://database.windows.net//.default"
  exchanged <- environment$fabric_local_exchange_token(
    list(
      tenant_id = "tenant-id",
      client_id = "client-id",
      refresh_token = "old-refresh-token"
    ),
    audience
  )

  expect_s3_class(exchanged, "fabric_local_token")
  expect_identical(exchanged$audience, audience)
  expect_identical(exchanged$access_token, "new-access-token")
  expect_match(request$url, "/tenant-id/oauth2/v2.0/token", fixed = TRUE)
  expect_identical(request$body$type, "form")
  expect_identical(
    unclass(request$body$data$grant_type),
    "refresh_token"
  )
  expect_identical(
    utils::URLdecode(unclass(request$body$data$scope)),
    paste(audience, "offline_access")
  )
  expect_error(
    environment$fabric_local_exchange_token(
      list(tenant_id = "tenant-id", client_id = "client-id"),
      audience
    ),
    "cannot be refreshed"
  )
})

test_that("local acquisition reuses and exchanges matching cached tokens", {
  environment <- fabric_test_local_runner()
  fabric_audience <- "https://api.fabric.microsoft.com/.default"
  sql_audience <- "https://database.windows.net//.default"
  source <- structure(
    list(
      tenant_id = "tenant-id",
      client_id = "client-id",
      audience = fabric_audience,
      access_token = "fabric-token",
      refresh_token = "refresh-token",
      expires_on = as.numeric(Sys.time()) + 3600
    ),
    class = "fabric_local_token"
  )
  environment$fabric_local_cached_token <- function(audience, ...) {
    if (identical(audience, fabric_audience)) source else NULL
  }
  exchanged_audiences <- character()
  environment$fabric_local_exchange_token <- function(token, audience) {
    exchanged_audiences <<- c(exchanged_audiences, audience)
    utils::modifyList(
      token,
      list(audience = audience, access_token = "sql-token")
    )
  }

  tokens <- suppressMessages(
    environment$fabric_local_acquire_tokens(
      tenant_id = "tenant-id",
      client_id = "client-id",
      audiences = c(Fabric = fabric_audience, SQL = sql_audience)
    )
  )

  expect_identical(tokens[[fabric_audience]], source)
  expect_identical(tokens[[sql_audience]]$access_token, "sql-token")
  expect_identical(exchanged_audiences, sql_audience)
})

test_that("local JWT and identity validation execute both principal paths", {
  environment <- fabric_test_local_runner()
  claims <- environment$fabric_local_jwt_claims(fabric_test_local_jwt(list(
    oid = "user-id",
    appid = "application-id"
  )))

  expect_identical(claims$oid, "user-id")
  expect_identical(claims$appid, "application-id")
  expect_error(
    environment$fabric_local_jwt_claims("not-a-jwt"),
    "not a JWT"
  )
  expect_error(
    environment$fabric_local_jwt_claims("header.invalid.signature"),
    "Could not decode"
  )

  expect_message(
    environment$fabric_local_validate_identity(
      claims,
      tenant_id = "tenant-id",
      client_id = "APPLICATION-ID",
      expected_user_id = "",
      auth_args = list(auth_type = "client_credentials")
    ),
    "Authenticated to Fabric as application"
  )
  expect_error(
    environment$fabric_local_validate_identity(
      claims,
      tenant_id = "tenant-id",
      client_id = "wrong-application",
      expected_user_id = "",
      auth_args = list(auth_type = "client_credentials")
    ),
    "instead of"
  )
  expect_invisible(environment$fabric_local_validate_identity(
    claims,
    tenant_id = "tenant-id",
    client_id = "client-id",
    expected_user_id = "USER-ID",
    auth_args = list(auth_type = "device_code")
  ))
  expect_error(
    environment$fabric_local_validate_identity(
      claims,
      tenant_id = "tenant-id",
      client_id = "client-id",
      expected_user_id = "wrong-user",
      auth_args = list(auth_type = "device_code")
    ),
    "use_cache = FALSE"
  )
})

test_that("local token providers refresh both supported token representations", {
  environment <- fabric_test_local_runner()
  azure_audience <- "https://api.fabric.microsoft.com/.default"
  azure_token <- fake_azure_token(valid = FALSE)
  azure_provider <- environment$fabric_local_token_provider(setNames(
    list(azure_token),
    azure_audience
  ))

  expect_identical(azure_provider(azure_audience), "azure-token-refreshed")
  expect_identical(azure_token$refreshes, 1L)
  expect_identical(
    azure_provider(azure_audience, force_refresh = TRUE),
    "azure-token-refreshed"
  )
  expect_identical(azure_token$refreshes, 2L)

  local_audience <- "https://database.windows.net//.default"
  local_token <- structure(
    list(
      tenant_id = "tenant-id",
      client_id = "client-id",
      audience = local_audience,
      access_token = "expired-token",
      refresh_token = "refresh-token",
      expires_on = 0
    ),
    class = "fabric_local_token"
  )
  exchanges <- 0L
  environment$fabric_local_exchange_token <- function(token, audience) {
    exchanges <<- exchanges + 1L
    token$access_token <- "fresh-token"
    token$expires_on <- as.numeric(Sys.time()) + 3600
    token
  }
  local_provider <- environment$fabric_local_token_provider(setNames(
    list(local_token),
    local_audience
  ))

  expect_identical(local_provider(local_audience), "fresh-token")
  expect_identical(local_provider(local_audience), "fresh-token")
  expect_identical(exchanges, 1L)
  expect_error(local_provider("missing-audience"), "No local AzureAuth token")
})

test_that("local runner selects only the audiences needed by its filter", {
  environment <- fabric_test_local_runner()

  all <- environment$fabric_local_test_audiences("integration-fabric")
  onelake <- environment$fabric_local_test_audiences(
    "integration-fabric-onelake"
  )

  expect_named(all, c("Fabric", "Power BI", "SQL", "OneLake", "Kusto"))
  expect_named(onelake, c("Fabric", "SQL", "OneLake"))
  expect_identical(
    unname(onelake[["SQL"]]),
    "https://database.windows.net//.default"
  )
})
