test_that("query tokens resolve asynchronously and retain service policy after serialization", {
  skip_if_no_shiny_targets()
  skip_if_not_installed("promises")
  config <- shiny_test_config(c("fabric", "sql"))
  connection <- shiny_test_connection()
  connection$has_scopes <- function(...) TRUE
  shiny::testServer(
    function(input, output, session) {
      fabric <- fabric_shiny_session(
        list(connection = function() connection),
        config$profiles,
        90
      )
    },
    {
      session$flushReact()
      pending <- fabric$access_token("sql", async = TRUE)
      expect_s3_class(pending, "promise")
      token <- unserialize(serialize(shiny_test_await(pending), NULL))
      expect_identical(as.character(token), "synthetic-user-token")
      expect_identical(
        tail(connection$state$calls, 1L)[[1L]],
        list(
          target = "sql",
          scopes = "https://database.windows.net//user_impersonation",
          force_refresh = FALSE,
          async = TRUE,
          min_valid_for = 90
        )
      )
      credential <- fabric_credential(token = token)
      expect_identical(credential$refreshable, FALSE)
      expect_identical(credential$endpoint_policy, config$profiles["sql"])
      expect_invisible(fabric_require_trusted_credential_endpoint(
        "https://warehouse.datawarehouse.fabric.microsoft.com",
        credential,
        .fabric_audience$sql
      ))
      error <- rlang::catch_cnd(fabric_require_trusted_credential_endpoint(
        "https://other.example",
        credential,
        .fabric_audience$sql
      ))
      expect_identical(error$reason, "untrusted_endpoint")
      error <- rlang::catch_cnd(fabric_get_token(
        credential,
        .fabric_audience$fabric
      ))
      expect_identical(error$reason, "unconfigured_service")

      connection$state$token <- "renewed-user-token"
      expect_identical(
        as.character(fabric$access_token("sql")),
        "renewed-user-token"
      )
      expect_identical(as.character(token), "synthetic-user-token")
      error <- rlang::catch_cnd(shiny_test_await(fabric$access_token(
        "graphql",
        async = TRUE
      )))
      expect_identical(error$reason, "unconfigured_service")
    }
  )
})

test_that("pending query tokens reject logout, replacement and session closure", {
  skip_if_no_shiny_targets()
  skip_if_not_installed("promises")
  config <- shiny_test_config("sql")
  for (change in c("logout", "replacement", "closed")) {
    connection <- shiny_test_connection()
    connection$has_scopes <- function(...) TRUE
    connection$access_token <- function(..., async, target) {
      if (!async) {
        return("synthetic-user-token")
      }
      promises::promise(function(resolve, reject) {
        connection$state$resolve <- resolve
      })
    }
    shiny::testServer(
      function(input, output, session) {
        active <- shiny::reactiveVal(connection)
        fabric <- fabric_shiny_session(
          list(connection = active),
          config$profiles,
          60
        )
      },
      {
        session$flushReact()
        pending <- fabric$access_token("sql", async = TRUE)
        if (change == "logout") {
          active(NULL)
        }
        if (change == "replacement") {
          replacement <- connection
          replacement$id <- "replacement-authorization"
          active(replacement)
        }
        if (change == "closed") {
          session$close()
        }
        connection$state$resolve("synthetic-user-token")
        error <- rlang::catch_cnd(shiny_test_await(pending))
        expect_identical(
          error$reason,
          "authorization_unavailable",
          info = change
        )
      }
    )
  }
})

test_that("query token failures reject the promise without falling back to sign-in", {
  skip_if_no_shiny_targets()
  skip_if_not_installed("promises")
  config <- shiny_test_config("sql")
  connection <- shiny_test_connection()
  connection$has_scopes <- function(...) TRUE
  shiny::testServer(
    function(input, output, session) {
      active <- shiny::reactiveVal(connection)
      fabric <- fabric_shiny_session(
        list(connection = active),
        config$profiles,
        60
      )
    },
    {
      session$flushReact()
      connection$state$active <- FALSE
      pending <- fabric$access_token("sql", async = TRUE)
      expect_s3_class(pending, "promise")
      error <- rlang::catch_cnd(shiny_test_await(pending))
      expect_s3_class(error, "shinyOAuth_access_error")
      expect_identical(error$context$reason, "authorization_unavailable")
      active(NULL)
      error <- rlang::catch_cnd(shiny_test_await(fabric$access_token(
        "sql",
        async = TRUE
      )))
      expect_identical(error$reason, "authorization_unavailable")
    }
  )
})
