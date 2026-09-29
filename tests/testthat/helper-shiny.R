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
