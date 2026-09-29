#' Configure per-user Fabric access in a Shiny application
#'
#' Configure one Entra Web registration outside `server()`. Each visitor signs
#' in separately through 'shinyOAuth'; Fabric requests use that visitor's
#' delegated permissions. Requires a version of 'shinyOAuth' with token targets
#' and module-managed `OAuthConnection$access_token()`.
#'
#' @param tenant_id One fixed Entra directory GUID. Guests sign in to this tenant.
#' @param client_id Entra Web application's client ID.
#' @param client_secret Server-side client secret. Never send this to the browser.
#' @param redirect_uri Exact registered Web callback URI; HTTPS in deployment.
#' @param services Character vector selecting `"fabric"`, `"sql"`, and/or
#'   `"graphql"`. SQL alone supports a configured server/database without discovery.
#' @param scopes Named list of explicit delegated scopes, overriding each
#'   service's defaults. Short names are qualified with the service resource.
#'   Defaults are Fabric `Workspace.Read.All` and `Item.Read.All`, SQL
#'   `user_impersonation`, and GraphQL `GraphQLApi.Execute.All`. This initial
#'   integration does not support `.default` consent or incremental consent.
#' @param default_service Service used for initial code redemption. Defaults to
#'   the first entry of `services`; later calls select their own token target.
#' @param endpoint_hosts Named list of additional exact hostnames per service.
#'   Configure trusted gateways in application code, never from user input.
#'   Built-in Microsoft service hosts remain allowed. HTTPS is required.
#' @param min_valid_for Minimum remaining token lifetime in seconds at acquisition.
#' @return A `fabric_shiny_config` containing the 'shinyOAuth' client and resource
#'   profiles. Printing it does not display credentials.
#' @details
#' Only 'shinyOAuth' owns sign-in, validated OIDC identity, token caches and
#' refresh. This configuration disables Graph UserInfo while retaining ID-token
#' validation, PKCE and nonce handling. The client secret authenticates the app;
#' it does not turn the user's data requests into service-principal requests.
#'
#' Scope selection does not grant Fabric item or SQL permissions. SQL's delegated
#' scope can allow writes when the user has the corresponding SQL grants.
#' See `vignette("shiny-integration")` for setup and session lifetime guidance.
#' @seealso [fabric_shiny_ui()], [fabric_shiny_server()],
#'   [fabric_shiny_token_provider()]
#' @export
#' @examples
#' \dontrun{
#' config <- fabric_shiny_config(
#'   tenant_id = Sys.getenv("ENTRA_TENANT_ID"),
#'   client_id = Sys.getenv("ENTRA_CLIENT_ID"),
#'   client_secret = Sys.getenv("ENTRA_CLIENT_SECRET"),
#'   redirect_uri = "http://localhost:8100/",
#'   services = c("fabric", "sql")
#' )
#' }
fabric_shiny_config <- function(
  tenant_id,
  client_id,
  client_secret,
  redirect_uri,
  services = c("fabric", "sql"),
  scopes = list(),
  default_service = services[[1L]],
  endpoint_hosts = list(),
  min_valid_for = 60
) {
  fabric_shiny_require()
  if (!fabric_is_guid(tenant_id)) {
    fabric_shiny_error("tenant_id must be one fixed Entra directory GUID.")
  }
  for (value in list(client_id, client_secret, redirect_uri)) {
    if (
      !is.character(value) ||
        length(value) != 1L ||
        is.na(value) ||
        !nzchar(value)
    ) {
      fabric_shiny_error(
        "client_id, client_secret and redirect_uri must be non-empty strings."
      )
    }
  }
  profiles <- fabric_shiny_profiles(services, scopes, endpoint_hosts)
  if (length(default_service) != 1L || !default_service %in% names(profiles)) {
    fabric_shiny_error("default_service must select a configured service.")
  }
  fabric_shiny_lifetime(min_valid_for)
  token_targets <- lapply(profiles, function(profile) {
    list(resource = profile$resource, scopes = profile$scopes)
  })
  names(token_targets) <- vapply(profiles, `[[`, character(1), "target")
  client <- shinyOAuth::oauth_client(
    provider = shinyOAuth::oauth_provider_microsoft(
      tenant = tenant_id,
      id_token_validation = TRUE,
      userinfo_required = FALSE
    ),
    client_id = client_id,
    client_secret = client_secret,
    redirect_uri = redirect_uri,
    scopes = unique(c(
      "openid",
      "profile",
      "offline_access",
      unlist(lapply(profiles, `[[`, "scopes"), use.names = FALSE)
    )),
    token_targets = token_targets,
    default_token_target = profiles[[default_service]]$target
  )
  structure(
    list(client = client, profiles = profiles, min_valid_for = min_valid_for),
    class = "fabric_shiny_config"
  )
}

#' @export
print.fabric_shiny_config <- function(x, ...) {
  .fabric_print(
    "fabric_shiny_config",
    list(Services = paste(names(x$profiles), collapse = ", "))
  )
  invisible(x)
}

fabric_shiny_require <- function() {
  if (
    !requireNamespace("shiny", quietly = TRUE) ||
      !requireNamespace("shinyOAuth", quietly = TRUE)
  ) {
    fabric_shiny_error(
      "Install shiny and shinyOAuth to use the Shiny integration."
    )
  }
  if (
    !"token_targets" %in% names(formals(shinyOAuth::oauth_client)) ||
    !"userinfo_required" %in%
      names(formals(shinyOAuth::oauth_provider_microsoft))
  ) {
    fabric_shiny_error(
      "Install a shinyOAuth version with token_targets and OAuthConnection$access_token(target = ...)."
    )
  }
}

fabric_shiny_error <- function(message, reason = "invalid_configuration") {
  .fabric_abort(
    message,
    class = c("fabric_shiny_error", "fabric_auth_error"),
    reason = reason,
    call = NULL
  )
}

fabric_shiny_lifetime <- function(value) {
  if (
    !is.numeric(value) ||
      length(value) != 1L ||
      is.na(value) ||
      !is.finite(value) ||
      value < 0
  ) {
    fabric_shiny_error("min_valid_for must be one finite non-negative number.")
  }
}

fabric_shiny_profiles <- function(
  services,
  scopes = list(),
  endpoint_hosts = list()
) {
  catalog <- list(
    fabric = list(
      resource = "https://api.fabric.microsoft.com",
      target = "fabric",
      scopes = c("Workspace.Read.All", "Item.Read.All"),
      audience = .fabric_audience$fabric,
      hosts = "api.fabric.microsoft.com"
    ),
    sql = list(
      resource = "https://database.windows.net/",
      target = "sql",
      scopes = "user_impersonation",
      audience = .fabric_audience$sql,
      hosts = c(
        "database.windows.net",
        "database.fabric.microsoft.com",
        "datawarehouse.fabric.microsoft.com",
        "datawarehouse.pbidedicated.windows.net",
        "datawarehouse.pbidedicated.microsoft.com"
      )
    ),
    graphql = list(
      resource = "https://analysis.windows.net/powerbi/api",
      target = "power_bi",
      scopes = "GraphQLApi.Execute.All",
      audience = .fabric_audience$graphql,
      hosts = "graphql.fabric.microsoft.com"
    )
  )
  if (
    !is.character(services) ||
      !length(services) ||
      anyNA(services) ||
      anyDuplicated(services) ||
      !all(services %in% names(catalog))
  ) {
    fabric_shiny_error(
      "services must select distinct entries from fabric, sql and graphql."
    )
  }
  for (overrides in list(scopes, endpoint_hosts)) {
    if (
      !is.list(overrides) ||
        (length(overrides) &&
          (is.null(names(overrides)) ||
            anyDuplicated(names(overrides)) ||
            !all(names(overrides) %in% services)))
    ) {
      fabric_shiny_error(
        "scopes and endpoint_hosts must be lists named by configured services."
      )
    }
  }
  profiles <- catalog[services]
  for (service in services) {
    profile <- profiles[[service]]
    selected <- scopes[[service]] %||% profile$scopes
    if (
      !is.character(selected) ||
        !length(selected) ||
        anyNA(selected) ||
        any(!nzchar(selected)) ||
        any(grepl("[[:space:]?#]", selected))
    ) {
      fabric_shiny_error(
        "Each service needs a non-empty vector of explicit delegated scopes."
      )
    }
    prefix <- paste0(profile$resource, "/")
    qualified <- startsWith(selected, prefix)
    short <- ifelse(
      qualified,
      substring(selected, nchar(prefix) + 1L),
      selected
    )
    if (any(!grepl("^[A-Za-z][A-Za-z0-9_.]*$", short))) {
      fabric_shiny_error(
        "Scopes must belong to their service and use explicit permissions, not .default."
      )
    }
    if (service == "graphql" && !"GraphQLApi.Execute.All" %in% short) {
      fabric_shiny_error("The graphql service requires GraphQLApi.Execute.All.")
    }
    extra_hosts <- endpoint_hosts[[service]] %||% character()
    if (
      !is.character(extra_hosts) ||
        anyNA(extra_hosts) ||
        any(!grepl("^[A-Za-z0-9]+(?:[.-][A-Za-z0-9]+)*$", extra_hosts))
    ) {
      fabric_shiny_error(
        "endpoint_hosts entries must be exact hostnames without schemes, paths or wildcards."
      )
    }
    profile$scopes <- unique(paste0(prefix, short))
    profile$extra_hosts <- tolower(extra_hosts)
    profiles[[service]] <- profile
  }
  profiles
}
