#' Use a shinyOAuth connection with Fabric functions
#'
#' Adapt a module-managed `OAuthConnection` without adding another login module.
#' Create the provider inside the owning Shiny session, then pass it as `token`
#' to existing Fabric functions. Each operation reads the current credential.
#'
#' @param connection A connection returned by 'shinyOAuth'
#'   `auth$connection()`, or `auth$connection(id)` for a manager.
#' @inheritParams fabric_shiny_config
#' @param targets Optional named character vector mapping configured service
#'   names to the connection's declared target names. Defaults are `fabric`,
#'   `sql`, `power_bi` (DAX and GraphQL), `kusto` (KQL), and `storage` (OneLake).
#'   Target declarations on the existing
#'   OAuth client must use the matching resources and scopes.
#' @return A synchronous `function(audience, force_refresh = FALSE)` suitable
#'   for the package's `token` arguments. The provider carries endpoint policy
#'   metadata, which package HTTP and SQL transports enforce before acquisition.
#' @details
#' Captures this connection, never a lookup of a later login. Logout, replacement,
#' a foreign session or session closure makes it unusable. Token refresh retains
#' the reference. Do not put providers, authenticated items or query results in
#' global app state, serialize them to workers, or remove their attributes.
#'
#' Unknown resources and undeclared explicit scopes fail before acquisition.
#' `force_refresh` is forwarded to 'shinyOAuth'; acquisition never redirects or
#' falls back to AzureAuth. Typed `shinyOAuth_access_error` conditions propagate,
#' including `refresh_pending` and `interaction_required`. SQL may wrap them as
#' the parent of a `fabric_sql_authentication_error`.
#'
#' Only bearer tokens are supported. HTTP redirects are rejected for these
#' providers; pagination and operation URLs are checked independently. Custom
#' hosts require explicit application configuration. This protects normal
#' package transports; it is not a sandbox for untrusted R code, which can
#' export tokens or override an item's authentication arguments.
#' @seealso [fabric_shiny_config()], [fabric_shiny_server()]
#' @export
fabric_shiny_token_provider <- function(
  connection,
  services = c("fabric", "sql"),
  scopes = list(),
  targets = NULL,
  endpoint_hosts = list(),
  min_valid_for = 60
) {
  fabric_shiny_require()
  profiles <- fabric_shiny_profiles(services, scopes, endpoint_hosts)
  if (!is.null(targets)) {
    if (
      !is.character(targets) ||
        anyNA(targets) ||
        any(!nzchar(targets)) ||
        is.null(names(targets)) ||
        anyDuplicated(names(targets)) ||
        !all(names(targets) %in% services)
    ) {
      fabric_shiny_error(
        "targets must be a character vector named by configured services."
      )
    }
    for (service in names(targets)) {
      profiles[[service]]$target <- targets[[service]]
    }
  }
  fabric_shiny_provider(connection, profiles, min_valid_for)
}

fabric_shiny_provider <- function(connection, profiles, min_valid_for) {
  if (
    !inherits(connection, "OAuthConnection") ||
      !is.function(connection$access_token) ||
      !"target" %in% names(formals(connection$access_token))
  ) {
    fabric_shiny_error(
      "connection must come from a shinyOAuth module's connection() method with token-target support."
    )
  }
  fabric_shiny_lifetime(min_valid_for)
  force(connection)
  force(profiles)
  force(min_valid_for)
  provider <- function(audience, force_refresh = FALSE) {
    profile <- fabric_shiny_route(audience, profiles)
    connection$access_token(
      target = profile$target,
      required_scopes = profile$scopes,
      min_valid_for = min_valid_for,
      force_refresh = force_refresh,
      async = FALSE
    )
  }
  attr(provider, "fabric_endpoint_policy") <- profiles
  class(provider) <- c("fabric_shiny_token_provider", "function")
  provider
}

# Route exact known callback aliases or an explicitly declared scope bundle.
# Never interpret a resource prefix as consent for another operation.
fabric_shiny_route <- function(audience, profiles) {
  if (is.character(audience) && length(audience) && !anyNA(audience)) {
    for (profile in profiles) {
      if (identical(audience, profile$audience)) {
        return(profile)
      }
    }
    for (profile in profiles) {
      if (all(audience %in% profile$scopes)) {
        return(profile)
      }
    }
  }
  fabric_shiny_error(
    "This token audience or scope bundle is not configured for the Shiny application.",
    "unconfigured_service"
  )
}

# Policy is retained by credential adaptation and secondary-service reuse.
fabric_check_endpoint_policy <- function(endpoint, policy, audience) {
  profile <- fabric_shiny_route(audience, policy)
  parsed <- tryCatch(httr2::url_parse(endpoint), error = function(...) NULL)
  host <- tolower(parsed$hostname %||% "")
  secure <- !is.null(parsed) &&
    identical(parsed$scheme, "https") &&
    is.null(parsed$username) &&
    is.null(parsed$password) &&
    (is.null(parsed$port) || identical(as.character(parsed$port), "443"))
  trusted <- host %in%
    profile$extra_hosts ||
    any(vapply(
      profile$hosts,
      function(suffix) fabric_host_matches(host, suffix),
      logical(1)
    ))
  if (!secure || !trusted) {
    fabric_shiny_error(
      "The request destination is outside the configured service's HTTPS endpoint policy.",
      "untrusted_endpoint"
    )
  }
  invisible(endpoint)
}
