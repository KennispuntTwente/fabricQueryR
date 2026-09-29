test_that("the provider routes declared resources with synchronous forced refresh", {
  skip_if_no_shiny_targets()
  connection <- shiny_test_connection()
  provider <- fabric_shiny_token_provider(
    connection,
    services = c("fabric", "sql", "graphql"),
    min_valid_for = 90
  )
  credential <- fabric_credential(token = provider)
  expect_identical(
    fabric_get_token(credential, .fabric_audience$sql, TRUE),
    "synthetic-user-token"
  )
  expect_identical(
    connection$state$calls[[1L]],
    list(
      target = "sql",
      scopes = "https://database.windows.net//user_impersonation",
      force_refresh = TRUE,
      async = FALSE,
      min_valid_for = 90
    )
  )
  fabric_get_token(credential, .fabric_audience$graphql)
  expect_identical(connection$state$calls[[2L]]$target, "power_bi")
  connection$state$token <- "rotated-user-token"
  expect_identical(provider(.fabric_audience$fabric), "rotated-user-token")
  expect_length(connection$state$calls, 3L)
  connection$state$active <- FALSE
  error <- rlang::catch_cnd(provider(.fabric_audience$sql))
  expect_s3_class(error, "shinyOAuth_access_error")
  expect_identical(error$context$reason, "authorization_unavailable")
})

test_that("undeclared services and scope bundles never acquire a token", {
  skip_if_no_shiny_targets()
  connection <- shiny_test_connection()
  provider <- fabric_shiny_token_provider(connection, services = "fabric")
  for (audience in c(
    .fabric_audience[c(
      "sql",
      "storage",
      "power_bi",
      "kusto",
      "user_data_function"
    )],
    list(
      .fabric_audience$livy_delegated,
      "https://api.fabric.microsoft.com/Item.ReadWrite.All"
    )
  )) {
    error <- rlang::catch_cnd(provider(audience))
    expect_s3_class(error, "fabric_shiny_error")
    expect_identical(error$reason, "unconfigured_service")
  }
  expect_length(connection$state$calls, 0L)
  expect_identical(
    provider("https://api.fabric.microsoft.com/Item.Read.All"),
    "synthetic-user-token"
  )
})

test_that("endpoint policy survives credential adaptation and secondary services", {
  skip_if_no_shiny_targets()
  connection <- shiny_test_connection()
  provider <- fabric_shiny_token_provider(
    connection,
    endpoint_hosts = list(sql = "gateway.example")
  )
  credential <- fabric_service_credential(
    fabric_credential(token = provider),
    argument = "sql_token",
    caller = "test"
  )
  expect_identical(
    credential$endpoint_policy,
    attr(provider, "fabric_endpoint_policy")
  )
  for (endpoint in c(
    "https://warehouse.datawarehouse.fabric.microsoft.com",
    "https://gateway.example"
  )) {
    expect_invisible(fabric_require_trusted_credential_endpoint(
      endpoint,
      credential,
      .fabric_audience$sql
    ))
  }
  for (endpoint in c(
    "http://api.fabric.microsoft.com",
    "https://api.fabric.microsoft.com.evil.test",
    "https://api.fabric.microsoft.com:8443",
    "https://user:pass@api.fabric.microsoft.com",
    "https://gateway.example",
    "https://sub.gateway.example"
  )) {
    error <- rlang::catch_cnd(fabric_require_trusted_credential_endpoint(
      endpoint,
      credential,
      .fabric_audience$fabric
    ))
    expect_identical(error$reason, "untrusted_endpoint")
  }
  expect_length(connection$state$calls, 0L)
})

test_that("HTTP refresh is bounded and redirects cannot forward Shiny credentials", {
  skip_if_no_shiny_targets()
  connection <- shiny_test_connection()
  provider <- fabric_shiny_token_provider(connection, services = "fabric")
  requests <- 0L
  httr2::local_mocked_responses(function(req) {
    requests <<- requests + 1L
    expect_identical(req$options$followlocation, FALSE)
    json_response(
      status = if (requests == 1L) 401L else 200L,
      body = list(value = list()),
      url = req$url
    )
  })
  expect_length(fabric_workspaces(token = provider), 0L)
  expect_identical(
    vapply(connection$state$calls, `[[`, logical(1), "force_refresh"),
    c(FALSE, TRUE)
  )
  httr2::local_mocked_responses(function(req) {
    json_response(
      status = 302L,
      headers = list(Location = "https://evil.example"),
      url = req$url
    )
  })
  error <- rlang::catch_cnd(fabric_workspaces(token = provider))
  expect_identical(error$reason, "untrusted_redirect")
})

test_that("untrusted HTTP and SQL destinations fail before acquiring or sending credentials", {
  skip_if_no_shiny_targets()
  connection <- shiny_test_connection()
  provider <- fabric_shiny_token_provider(connection)
  local_mocked_bindings(
    fabric_sql_require_backend = function(...) NULL,
    .fabric_sql_db_connect = function(...) stop("must not connect")
  )
  error <- rlang::catch_cnd(fabric_workspaces(
    token = provider,
    api_base = "https://evil.example/v1"
  ))
  expect_identical(error$reason, "untrusted_endpoint")
  for (fun in list(
    function() {
      fabric_sql_connect("evil.example", token = provider, verbose = FALSE)
    },
    function() {
      fabric_sql_query(
        "evil.example",
        "SELECT 1",
        token = provider,
        verbose = FALSE
      )
    }
  )) {
    error <- rlang::catch_cnd(fun())
    expect_identical(error$reason, "untrusted_endpoint")
  }
  expect_length(connection$state$calls, 0L)
})

test_that("SQL query retry wrappers retain the endpoint policy", {
  skip_if_no_shiny_targets()
  connection <- shiny_test_connection()
  provider <- fabric_shiny_token_provider(connection, services = "sql")
  captured <- list()
  local_mocked_bindings(
    fabric_sql_require_backend = function(...) NULL,
    fabric_sql_connect = function(token, ...) {
      captured[[length(captured) + 1L]] <<- fabric_credential(token = token)
      fabric_get_token(tail(captured, 1L)[[1L]], .fabric_audience$sql)
      list()
    },
    .fabric_sql_db_get_query = function(...) {
      if (length(captured) == 1L) {
        rlang::abort("Error 24804: operation interrupted by a system update")
      }
      tibble::tibble(value = 1)
    },
    .fabric_sql_db_disconnect = function(...) NULL,
    .fabric_sql_sleep = function(...) NULL
  )
  result <- fabric_sql_query(
    "warehouse.datawarehouse.fabric.microsoft.com",
    "SELECT 1",
    token = provider,
    idempotent = TRUE,
    verbose = FALSE,
    numeric_policy = "driver"
  )
  expect_identical(result$value, 1)
  expect_length(captured, 2L)
  expect_identical(
    captured[[2L]]$endpoint_policy,
    attr(provider, "fabric_endpoint_policy")
  )
  expect_identical(connection$state$calls[[2L]]$force_refresh, TRUE)
})

test_that("Delta cannot bypass the Shiny service policy", {
  skip_if_no_shiny_targets()
  connection <- shiny_test_connection()
  provider <- fabric_shiny_token_provider(connection)
  local_mocked_bindings(fabric_delta_read_uri = function(...) {
    stop("must not initialize the reader")
  })
  error <- rlang::catch_cnd(fabric_onelake_read_delta_table(
    "orders",
    "workspace",
    "lakehouse",
    token = provider,
    verbose = FALSE
  ))
  expect_identical(error$reason, "unconfigured_service")
  expect_length(connection$state$calls, 0L)
})
