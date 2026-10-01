#' Wrap a Shiny UI for Microsoft Fabric sign-in
#'
#' Uses 'shinyOAuth' callback routing with the client from
#' [fabric_shiny_config()]. Add your own sign-in, sign-out and retry buttons;
#' call the matching methods returned by [fabric_shiny_server()].
#'
#' @param ui A Shiny UI object or function accepted by `shinyOAuth::oauth_ui()`.
#' @param id Module identifier, matching [fabric_shiny_server()].
#' @param config Configuration returned by [fabric_shiny_config()].
#' @return The UI function returned by `shinyOAuth::oauth_ui()`. Use
#'   `shiny::shinyApp(ui, server, uiPattern = ".*")` to route callback paths.
#' @export
fabric_shiny_ui <- function(ui, id, config) {
  fabric_shiny_check_config(config)
  shinyOAuth::oauth_ui(ui, id = id, client = config$client)
}

#' Use the signed-in visitor's Fabric access in a Shiny server
#'
#' Call once inside `server()`, using the same ID and configuration as
#' [fabric_shiny_ui()]. Returns helpers for the current user's authorization
#' and the package's existing discovery and query interfaces.
#'
#' @inheritParams fabric_shiny_ui
#' @param auto_redirect Redirect to sign-in automatically. The default `FALSE`
#'   lets the application show its sign-in button first.
#' @param reauth_after_seconds Maximum local authorization age in seconds.
#'   Defaults to eight hours. Must be finite and positive; choose a lifetime
#'   appropriate for your application.
#' @param refresh_proactively Let 'shinyOAuth' refresh credentials proactively.
#' @return A session-local list of callable helpers:
#'   * `ready(service = NULL)`: reactive recorded scope coverage for all selected
#'     services, or one service. Does not prove remote access or token freshness.
#'   * `status()`: reactive per-service status and acquisition error reason,
#'     without tokens. `not_acquired` does not establish missing consent.
#'   * `identity()`: selected validated ID-token fields from 'shinyOAuth'.
#'   * `generation()`: the current connection ID, or `NULL` when signed out.
#'     Unchanged by refresh; use it to guard event-bound results and caches.
#'   * `token_provider()`: a provider bound to the current connection, for
#'     existing functions' `token` arguments.
#'   * `access_token(service, async = FALSE)`: a fixed bearer token for one
#'     configured service, or a promise resolving to it when `async = TRUE`.
#'     Acquire it before invoking a background query. The token retains the
#'     service's endpoint policy and can be serialized to a worker.
#'   * `workspaces(...)` and `item(workspace, item, ...)`: discovery with the
#'     current user's credentials, returning the ordinary R6 objects by default.
#'   * `request(path, method = "GET", query = list(), body = NULL,
#'     idempotent = FALSE)`: an advanced Fabric REST call beneath `/v1`, returning
#'     an httr2 response. `path` is relative, without query/fragment; credentials
#'     and destinations cannot be overridden through this helper.
#'   * `prepare(service = NULL)`: retry acquisition for selected services without
#'     running a data query. Returns invisibly whether acquisition succeeded.
#'   * `login()`, `logout()`, `reauthorize()`: explicit authorization actions.
#' @details
#' Each new authorization prefetches the configured resource tokens. One target's
#' failure leaves other services available. `prepare()` can retry a transient
#' acquisition failure; interaction/consent failures need explicit reauthorization
#' or a registration change. `prepare()` and `reauthorize()` do not run or replay
#' data operations. HTTP calls retain the package's retry policy: one 401 can
#' trigger forced acquisition and a retry even when `idempotent = FALSE`.
#' Retrying transient failures separately requires an idempotent request.
#'
#' Uses one fixed tenant and session-only retention. OAuth connections stay in
#' the owning R process. Use `access_token(service, async = TRUE)` with
#' [shiny::ExtendedTask] with [mirai::mirai()] or [promises::future_promise()];
#' pass the resolved token and ordinary query inputs to the worker. Fixed tokens
#' do not refresh in a worker. Acquire a new token for each task invocation and
#' compare its captured `generation()` before displaying the result. Logout does
#' not cancel an operation already running in a worker.
#'
#' Refresh alone does not invalidate generation-dependent queries. Existing
#' providers and items remain bound to their original authorization and fail after logout,
#' replacement or session closure. Do not share them across users or workers.
#'
#' Keep query results session-local. Store `generation()` beside event-bound
#' results and compare it reactively before display, so logout or replacement
#' clears old output. Open DBI connections and Arrow streams have their own
#' lifetimes: close them explicitly. No logout operation can undo work already
#' accepted by Fabric. See `vignette("shiny-integration")` for a complete app.
#' @seealso [fabric_shiny_token_provider()], [fabric_item()], [fabric_sql_query()]
#' @export
fabric_shiny_server <- function(
  id,
  config,
  auto_redirect = FALSE,
  reauth_after_seconds = 8 * 60 * 60,
  refresh_proactively = TRUE
) {
  fabric_shiny_check_config(config)
  if (is.null(shiny::getDefaultReactiveDomain())) {
    fabric_shiny_error(
      "Call fabric_shiny_server() inside a Shiny server session."
    )
  }
  if (
    !is.numeric(reauth_after_seconds) ||
      length(reauth_after_seconds) != 1L ||
      is.na(reauth_after_seconds) ||
      !is.finite(reauth_after_seconds) ||
      reauth_after_seconds <= 0
  ) {
    fabric_shiny_error(
      "reauth_after_seconds must be one finite positive number."
    )
  }
  auth <- shinyOAuth::oauth_module_server(
    id,
    config$client,
    auto_redirect = auto_redirect,
    async = FALSE,
    indefinite_session = FALSE,
    reauth_after_seconds = reauth_after_seconds,
    refresh_proactively = refresh_proactively,
    revoke_on_session_end = FALSE
  )
  fabric_shiny_session(auth, config$profiles, config$min_valid_for)
}

fabric_shiny_check_config <- function(config) {
  fabric_shiny_require()
  if (!inherits(config, "fabric_shiny_config")) {
    fabric_shiny_error("config must be returned by fabric_shiny_config().")
  }
}

fabric_shiny_session <- function(auth, profiles, min_valid_for) {
  session <- shiny::getDefaultReactiveDomain()
  current <- shiny::reactive(auth$connection())
  acquisition_errors <- shiny::reactiveVal(list())
  select_services <- function(service) {
    if (is.null(service)) {
      return(names(profiles))
    }
    if (
      !is.character(service) ||
        length(service) != 1L ||
        is.na(service) ||
        !service %in% names(profiles)
    ) {
      fabric_shiny_error(
        "service must name one configured service.",
        "unconfigured_service"
      )
    }
    service
  }
  provider <- function() {
    fabric_shiny_provider(shiny::req(current()), profiles, min_valid_for)
  }
  access_token <- function(service, async = FALSE) {
    if (!is.logical(async) || length(async) != 1L || is.na(async)) {
      fabric_shiny_error("async must be TRUE or FALSE.")
    }
    if (async && !requireNamespace("promises", quietly = TRUE)) {
      fabric_shiny_error("Install promises to acquire tokens asynchronously.")
    }
    acquire <- function() {
      services <- select_services(service)
      if (length(services) != 1L) {
        fabric_shiny_error("service must name one configured service.")
      }
      connection <- current()
      if (is.null(connection) || isTRUE(session$isClosed())) {
        fabric_shiny_error(
          "Sign in before acquiring a query token.",
          "authorization_unavailable"
        )
      }
      profile <- profiles[[services]]
      finish <- function(token) {
        if (
          isTRUE(session$isClosed()) ||
            !identical(shiny::isolate(current())$id, connection$id)
        ) {
          fabric_shiny_error(
            "The authorization changed before token acquisition completed.",
            "authorization_unavailable"
          )
        }
        fabric_validate_bearer_token(token, "The query token")
        attr(token, "fabric_endpoint_policy") <- profiles[services]
        token
      }
      token <- connection$access_token(
        target = profile$target,
        required_scopes = profile$scopes,
        min_valid_for = min_valid_for,
        force_refresh = FALSE,
        async = async
      )
      if (async) {
        promises::then(promises::promise_resolve(token), finish)
      } else {
        finish(token)
      }
    }
    if (async) {
      tryCatch(acquire(), error = function(error) {
        promises::promise_reject(error)
      })
    } else {
      acquire()
    }
  }
  ready <- function(service = NULL) {
    services <- select_services(service)
    connection <- current()
    if (is.null(connection)) {
      return(FALSE)
    }
    all(vapply(
      services,
      function(name) {
        profile <- profiles[[name]]
        connection$has_scopes(profile$scopes, target = profile$target)
      },
      logical(1)
    ))
  }
  prepare <- function(service = NULL) {
    services <- select_services(service)
    connection <- shiny::req(current())
    token <- fabric_shiny_provider(connection, profiles, min_valid_for)
    errors <- shiny::isolate(acquisition_errors())
    for (name in services) {
      errors[[name]] <- tryCatch(
        {
          token(profiles[[name]]$audience)
          NULL
        },
        error = function(error) {
          error$context$reason %||% error$reason %||% "acquisition_failed"
        }
      )
    }
    acquisition_errors(errors)
    invisible(!any(services %in% names(errors)))
  }
  shiny::observeEvent(
    current(),
    {
      acquisition_errors(list())
      if (!is.null(current())) prepare()
    },
    ignoreNULL = FALSE,
    priority = 100
  )

  discovery <- function(fun, args) {
    if (
      any(names(args) %in% c("token", "tenant_id", "client_id", "auth_args"))
    ) {
      fabric_shiny_error(
        "Session discovery methods supply their own authentication arguments."
      )
    }
    do.call(fun, c(args, list(token = provider())))
  }
  list(
    ready = ready,
    status = function() {
      connection <- current()
      if (is.null(connection)) {
        return(list(connection_id = NULL, services = list()))
      }
      targets <- connection$targets()
      errors <- acquisition_errors()
      list(
        connection_id = connection$id,
        services = lapply(
          stats::setNames(names(profiles), names(profiles)),
          function(name) {
            profile <- profiles[[name]]
            list(
              status = targets[[profile$target]]$status %||% "not_acquired",
              ready = ready(name),
              reason = errors[[name]]
            )
          }
        )
      )
    },
    identity = function() {
      shiny::req(current())$identity(
        claims = c("iss", "sub", "tid", "oid", "name", "preferred_username")
      )
    },
    generation = function() {
      connection <- current()
      if (is.null(connection)) NULL else connection$id
    },
    token_provider = provider,
    access_token = access_token,
    workspaces = function(...) discovery(fabric_workspaces, list(...)),
    item = function(workspace, item, ...) {
      discovery(
        fabric_item,
        c(list(workspace = workspace, item = item), list(...))
      )
    },
    request = function(
      path,
      method = "GET",
      query = list(),
      body = NULL,
      idempotent = FALSE
    ) {
      if (
        !is.character(path) ||
          length(path) != 1L ||
          is.na(path) ||
          !nzchar(path) ||
          grepl("[\\\\?#:]|^//|(^|/)\\.\\.?(/|$)", path)
      ) {
        fabric_shiny_error(
          "path must be a relative Fabric REST path without a query or fragment."
        )
      }
      if (
        !is.list(query) ||
          (length(query) &&
            (is.null(names(query)) || !all(nzchar(names(query)))))
      ) {
        fabric_shiny_error("query must be a named list.")
      }
      req <- httr2::request(paste0(.fabric_api_base, "/", sub("^/", "", path)))
      if (length(query)) {
        req <- do.call(httr2::req_url_query, c(list(req), query))
      }
      if (!is.null(body)) {
        req <- httr2::req_body_json(req, body)
      }
      req <- httr2::req_method(req, method)
      .httr2_perform(
        req,
        credential = fabric_credential(token = provider()),
        audience = .fabric_audience$fabric,
        idempotent = idempotent
      )
    },
    prepare = prepare,
    login = function() auth$request_login(),
    logout = function() auth$logout(),
    reauthorize = function() auth$reauthorize()
  )
}
