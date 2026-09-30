skip_if_no_shiny_targets <- function() {
  skip_if_not_installed("shiny")
  skip_if_not_installed("shinyOAuth")
  skip_if(!"token_targets" %in% names(formals(shinyOAuth::oauth_client)))
}

shiny_test_connection <- function() {
  state <- new.env(parent = emptyenv())
  state$calls <- list()
  state$active <- TRUE
  state$token <- "synthetic-user-token"
  structure(
    list(
      id = "synthetic-authorization",
      state = state,
      access_token = function(
        required_scopes,
        min_valid_for,
        force_refresh,
        async,
        target
      ) {
        if (!state$active) {
          rlang::abort(
            "Authorization unavailable",
            class = "shinyOAuth_access_error",
            context = list(reason = "authorization_unavailable")
          )
        }
        state$calls[[length(state$calls) + 1L]] <- list(
          target = target,
          scopes = required_scopes,
          force_refresh = force_refresh,
          async = async,
          min_valid_for = min_valid_for
        )
        state$token
      }
    ),
    class = "OAuthConnection"
  )
}

shiny_test_config <- function(services = "fabric") {
  fabric_shiny_config(
    "11111111-1111-1111-1111-111111111111",
    "app",
    "synthetic-secret",
    "http://localhost:8100/",
    services = services
  )
}

shiny_test_token <- function(
  access = "synthetic-user-token",
  scopes = c("Workspace.Read.All", "Item.Read.All"),
  subject = "synthetic-user"
) {
  claims <- jsonlite::toJSON(
    list(
      iss = "https://login.microsoftonline.com/11111111-1111-1111-1111-111111111111/v2.0",
      sub = subject,
      name = "Test User"
    ),
    auto_unbox = TRUE
  )
  payload <- chartr(
    "+/",
    "-_",
    gsub("[=\r\n]", "", jsonlite::base64_enc(charToRaw(claims)))
  )
  shinyOAuth::OAuthToken(
    access_token = access,
    refresh_token = "synthetic-refresh",
    token_type = "Bearer",
    expires_at = as.numeric(Sys.time()) + 3600,
    granted_scopes = c(
      "openid",
      "profile",
      paste0("https://api.fabric.microsoft.com/", scopes)
    ),
    granted_scopes_verified = TRUE,
    id_token_validated = TRUE,
    id_token = paste0(
      "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.",
      payload,
      ".c3ludGhldGlj"
    )
  )
}

shiny_test_await <- function(promise, timeout = 10) {
  complete <- FALSE
  value <- error <- NULL
  promises::then(
    promise,
    function(result) {
      value <<- result
      complete <<- TRUE
      NULL
    },
    function(condition) {
      error <<- condition
      complete <<- TRUE
      NULL
    }
  )
  deadline <- Sys.time() + timeout
  while (!complete && Sys.time() < deadline) {
    later::run_now(0.01)
  }
  if (!complete) {
    stop("Timed out waiting for the test promise")
  }
  if (!is.null(error)) {
    stop(error)
  }
  value
}

shiny_test_wait <- function(ready, timeout = 30) {
  deadline <- Sys.time() + timeout
  while (!ready() && Sys.time() < deadline) {
    later::run_now(0.01)
  }
  if (!ready()) {
    stop("Timed out waiting for the background test task")
  }
  invisible(NULL)
}
