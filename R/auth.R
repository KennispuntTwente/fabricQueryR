.fabric_audience <- list(
  fabric = "https://api.fabric.microsoft.com/.default",
  graphql = paste0(
    "https://analysis.windows.net/powerbi/api/",
    "GraphQLApi.Execute.All"
  ),
  user_data_function = paste0(
    "https://analysis.windows.net/powerbi/api/",
    "UserDataFunction.Execute.All"
  ),
  power_bi = "https://analysis.windows.net/powerbi/api/.default",
  livy_delegated = c(
    "https://api.fabric.microsoft.com/Lakehouse.Execute.All",
    "https://api.fabric.microsoft.com/Lakehouse.Read.All",
    "https://api.fabric.microsoft.com/Code.AccessFabric.All",
    "https://api.fabric.microsoft.com/Code.AccessStorage.All"
  ),
  sql = "https://database.windows.net//.default",
  storage = "https://storage.azure.com/.default",
  kusto = "https://api.kusto.windows.net/.default"
)

.fabric_azure_auth_args <- setdiff(
  names(formals(AzureAuth::get_azure_token)),
  c("resource", "tenant", "app", "version")
)

.fabric_audience_hosts <- list(
  fabric = "api.fabric.microsoft.com",
  power_bi = c(
    "api.fabric.microsoft.com",
    "api.powerbi.com",
    "analysis.windows.net"
  ),
  storage = c(
    "onelake.dfs.fabric.microsoft.com",
    "dfs.fabric.microsoft.com",
    "onelake.fabric.microsoft.com",
    "onelake.table.fabric.microsoft.com"
  ),
  kusto = c(
    "kusto.fabric.microsoft.com",
    "kusto.windows.net",
    "kusto.data.microsoft.com"
  ),
  sql = c(
    "database.windows.net",
    "fabric.microsoft.com",
    "pbidedicated.microsoft.com",
    "pbidedicated.windows.net"
  )
)

#' Create an internal audience-aware credential
#'
#' @param tenant_id Entra tenant ID
#' @param client_id Entra application ID
#' @param token Optional token supplied as an `AzureAuth::AzureToken` object, a
#'   bearer-token string, or a token-provider function. `NULL` delegates token
#'   acquisition, cache reuse, and interactive login to 'AzureAuth'
#' @param auth_args Named list of additional arguments passed to
#'   [AzureAuth::get_azure_token()]. The package supplies `resource`, `tenant`,
#'   `app`, and `version`
#' @return An internal `fabric_credential` object
#' @details Public API functions use this object so authentication, refresh,
#'   and token validation behave consistently for every Fabric service
#' @keywords internal
#' @noRd
fabric_credential <- function(
  tenant_id = NULL,
  client_id = NULL,
  token = NULL,
  auth_args = list()
) {
  # 1 Validate inputs ------------------------------------------------------------------------------

  # Extra AzureAuth settings only apply when this package performs sign-in

  fabric_validate_auth_args(auth_args)
  if (!is.null(token) && length(auth_args)) {
    .fabric_abort(
      "auth_args can only be used when fabricQueryR acquires the token"
    )
  }

  # 2 Adapt a supplied token -----------------------------------------------------------------------

  # Turn each supported token form into the same small provider interface

  if (!is.null(token)) {
    # AzureAuth objects already provide refresh behavior
    if (AzureAuth::is_azure_token(token)) {
      return(fabric_azure_token_credential(token))
    }

    # Static strings are validated once and returned unchanged
    if (is.character(token)) {
      fabric_validate_bearer_token(token, "token")
      audience_ref <- new.env(parent = emptyenv())
      return(structure(
        list(
          provider = function(audience, force_refresh = FALSE) token,
          refreshable = FALSE,
          type = "static",
          audience_ref = audience_ref
        ),
        class = "fabric_credential"
      ))
    } else if (is.function(token)) {
      # Callbacks are adapted to the package's audience-aware interface
      return(structure(
        list(
          provider = function(audience, force_refresh = FALSE) {
            fabric_call_token_provider(
              token,
              audience,
              force_refresh
            )
          },
          refreshable = TRUE,
          type = "callback"
        ),
        class = "fabric_credential"
      ))
    } else if (inherits(token, "fabric_credential")) {
      # Internal credentials can pass through without another adapter
      return(token)
    } else {
      .fabric_abort(
        paste0(
          "token must be an AzureAuth AzureToken object, one bearer-token ",
          "string, or a token-provider function"
        )
      )
    }
  }

  # 3 Validate automatic sign-in settings ----------------------------------------------------------

  # Tenant and application IDs are required only when no token was supplied

  if (
    !is.character(tenant_id) ||
      length(tenant_id) != 1L ||
      is.na(tenant_id) ||
      !nzchar(tenant_id)
  ) {
    .fabric_abort(
      "tenant_id must be one non-empty string (or set FABRICQUERYR_TENANT_ID)",
      class = "fabric_auth_validation_error"
    )
  }

  if (
    !is.character(client_id) ||
      length(client_id) != 1L ||
      is.na(client_id) ||
      !nzchar(client_id)
  ) {
    .fabric_abort(
      "client_id must be one non-empty string (or set FABRICQUERYR_CLIENT_ID)",
      class = "fabric_auth_validation_error"
    )
  }

  # 4 Build a cached token provider ----------------------------------------------------------------

  # Keep one AzureAuth token per audience, then refresh it only when needed

  cache <- new.env(parent = emptyenv())
  # Acquire or refresh a token for `audience`; returns one bearer-token string
  provider <- function(audience, force_refresh = FALSE) {
    key <- .fabric_audience_cache_key(audience)
    azure_token <- cache[[key]]
    if (is.null(azure_token)) {
      args <- c(
        list(
          resource = fabric_azure_scopes(audience, auth_args),
          tenant = tenant_id,
          app = client_id,
          version = 2
        ),
        auth_args
      )
      azure_token <- do.call(AzureAuth::get_azure_token, args)
      cache[[key]] <- azure_token
    } else if (
      isTRUE(force_refresh) ||
        (is.function(azure_token$validate) && !isTRUE(azure_token$validate()))
    ) {
      azure_token$refresh()
    }
    fabric_extract_azure_token(azure_token)
  }
  structure(
    list(
      provider = provider,
      refreshable = TRUE,
      type = "AzureAuth",
      client_credentials = fabric_uses_client_credentials(auth_args)
    ),
    class = "fabric_credential"
  )
}

# Keep automatic Azure sign-in inside the Microsoft Fabric credential boundary.
# Returns `endpoint` invisibly after requiring an explicit credential for any
# caller-selected host outside the supplied Microsoft service suffixes.
fabric_require_explicit_custom_token <- function(
  endpoint,
  token,
  argument = "endpoint",
  allowed_hosts = .fabric_audience_hosts$fabric
) {
  if (!is.null(token)) {
    return(invisible(endpoint))
  }

  parsed <- try(httr2::url_parse(endpoint), silent = TRUE)
  host <- if (inherits(parsed, "try-error")) {
    ""
  } else {
    parsed$hostname %||% ""
  }
  trusted <- any(vapply(
    allowed_hosts,
    function(suffix) fabric_host_matches(host, suffix),
    logical(1)
  ))
  if (trusted) {
    return(invisible(endpoint))
  }

  .fabric_abort(
    paste0(
      argument,
      " uses a custom host; supply token explicitly so fabricQueryR does ",
      "not send an automatically acquired Fabric credential to that host"
    ),
    class = "fabric_custom_endpoint_requires_token",
    endpoint_host = host,
    argument = argument
  )
}

# Choose the Microsoft-owned host suffixes associated with one token audience.
# Unknown audiences stay inside the narrow Fabric REST boundary by default.
fabric_trusted_hosts_for_audience <- function(audience) {
  if (identical(audience, .fabric_audience$power_bi)) {
    return(.fabric_audience_hosts$power_bi)
  }
  if (identical(audience, .fabric_audience$storage)) {
    return(.fabric_audience_hosts$storage)
  }
  if (identical(audience, .fabric_audience$kusto)) {
    return(.fabric_audience_hosts$kusto)
  }
  if (identical(audience, .fabric_audience$sql)) {
    return(.fabric_audience_hosts$sql)
  }
  .fabric_audience_hosts$fabric
}

# Prevent an automatically acquired token from crossing its Microsoft service
# boundary. Caller-supplied credentials remain an explicit opt-in.
fabric_require_trusted_credential_endpoint <- function(
  endpoint,
  credential,
  audience,
  argument = "request endpoint"
) {
  if (
    is.null(credential) ||
      !inherits(credential, "fabric_credential") ||
      !identical(credential$type, "AzureAuth")
  ) {
    return(invisible(endpoint))
  }
  fabric_require_explicit_custom_token(
    endpoint,
    token = NULL,
    argument = argument,
    allowed_hosts = fabric_trusted_hosts_for_audience(audience)
  )
}

# Resolve a secondary service credential without reusing a fixed token for a
# different audience. Automatic and callback credentials are audience-aware.
fabric_service_credential <- function(
  credential,
  service_token = NULL,
  argument,
  caller
) {
  if (!is.null(service_token)) {
    return(fabric_credential(token = service_token))
  }
  if (
    inherits(credential, "fabric_credential") &&
      credential$type %in% c("AzureAuth", "callback")
  ) {
    return(credential)
  }
  .fabric_abort(
    paste0(
      caller,
      " requires ",
      argument,
      " when token is a fixed bearer token or AzureToken; supply the ",
      "service token separately or use an audience-aware token provider"
    ),
    class = c("fabric_multi_audience_auth_error", "fabric_auth_error"),
    argument = argument
  )
}

# Serialize an audience vector into a collision-free environment key
# Returns a portable ASCII key while preserving punctuation and vector bounds
.fabric_audience_cache_key <- function(audience) {
  bytes <- serialize(enc2utf8(audience), NULL, version = 2)
  paste(sprintf("%02x", as.integer(bytes)), collapse = "")
}

#' Consume the legacy static-token argument from dots
#'
#' `access_token` was the public argument name used by `fabric_livy_query()`,
#' `fabric_sql_connect()`, and `fabric_sql_query()` through version 0.2.1
#' Accept its named form through `...` without restoring it to public formals
#'
#' @param token A token supplied through the current public argument
#' @param dots Extra arguments that may contain the old `access_token` name
#' @param caller Public function name used in any error message
#' @return A list containing the chosen token and the remaining extra arguments
#' @keywords internal
#' @noRd
fabric_resolve_token_alias <- function(
  token = NULL,
  dots = list(),
  caller
) {
  # Separate the old argument from the other extra arguments so callers can
  # handle backward compatibility in one consistent place
  dot_names <- names(dots)
  if (is.null(dot_names)) {
    dot_names <- rep("", length(dots))
  }
  positions <- which(dot_names == "access_token")
  if (length(positions) > 1L) {
    .fabric_abort(
      paste0(caller, " received access_token more than once")
    )
  }
  access_token <- if (length(positions)) {
    dots[[positions]]
  } else {
    NULL
  }

  if (!is.null(token) && !is.null(access_token)) {
    .fabric_abort(
      paste0(
        caller,
        " received both token and the deprecated access_token alias; ",
        "supply only token"
      )
    )
  }

  if (length(positions)) {
    dots <- dots[-positions]
  }
  list(token = token %||% access_token, dots = dots)
}

#' Validate 'AzureAuth' passthrough arguments
#'
#' @param auth_args Named list passed on to 'AzureAuth'
#' @return `auth_args`, invisibly, after checking names and reserved settings
#' @details Called by `fabric_credential()` before any automatic sign-in
#' @keywords internal
#' @noRd
fabric_validate_auth_args <- function(auth_args) {
  if (!is.list(auth_args)) {
    .fabric_abort("auth_args must be a named list")
  }

  if (!length(auth_args)) {
    return(invisible(auth_args))
  }

  if (
    is.null(names(auth_args)) ||
      anyNA(names(auth_args)) ||
      !all(nzchar(names(auth_args)))
  ) {
    .fabric_abort("auth_args must be a fully named list")
  }

  if (anyDuplicated(names(auth_args))) {
    .fabric_abort("auth_args names must be unique")
  }
  reserved <- intersect(
    names(auth_args),
    c("resource", "tenant", "app", "version")
  )

  if (length(reserved)) {
    .fabric_abort(paste0(
      "auth_args cannot override ",
      paste(reserved, collapse = ", "),
      "; use the function's authentication arguments instead"
    ))
  }
  unknown <- setdiff(names(auth_args), .fabric_azure_auth_args)
  if (length(unknown)) {
    .fabric_abort(paste0(
      "Unknown AzureAuth argument",
      if (length(unknown) == 1L) " " else "s ",
      paste(unknown, collapse = ", ")
    ))
  }
  invisible(auth_args)
}

#' Choose Azure v2 scopes for an 'AzureAuth' flow
#'
#' @param audience Service scopes requested by the calling API function
#' @param auth_args Named 'AzureAuth' settings that identify the sign-in flow
#' @return The scopes 'AzureAuth' should request
#' @details Interactive flows need `offline_access`; application-only flows do
#'   not. `fabric_credential()` uses this distinction when acquiring a token
#' @keywords internal
#' @noRd
fabric_azure_scopes <- function(audience, auth_args) {
  if (fabric_uses_client_credentials(auth_args)) {
    audience
  } else {
    c(audience, "offline_access")
  }
}

#' Detect an 'AzureAuth' client-credentials flow
#'
#' @param auth_args Named 'AzureAuth' settings
#' @return `TRUE` for an application-only sign-in, otherwise `FALSE`
#' @details Used when choosing both token scopes and the GraphQL audience
#' @keywords internal
#' @noRd
fabric_uses_client_credentials <- function(auth_args) {
  auth_type <- auth_args$auth_type
  identical(auth_type, "client_credentials") ||
    (is.null(auth_type) &&
      (!is.null(auth_args$password) || !is.null(auth_args$certificate)) &&
      is.null(auth_args$username) &&
      is.null(auth_args$on_behalf_of))
}

#' Adapt a refreshable 'AzureAuth' token
#'
#' @param token An `AzureAuth::AzureToken` object
#' @return An internal credential that refreshes and reads `token` on demand
#' @details `fabric_credential()` uses this for caller-supplied 'AzureAuth'
#'   tokens
#' @keywords internal
#' @noRd
fabric_azure_token_credential <- function(token) {
  audience_ref <- new.env(parent = emptyenv())
  # Refresh the supplied Azure token when requested; returns its bearer token
  provider <- function(audience, force_refresh = FALSE) {
    if (
      isTRUE(force_refresh) ||
        (is.function(token$validate) && !isTRUE(token$validate()))
    ) {
      token$refresh()
    }
    fabric_extract_azure_token(token)
  }
  structure(
    list(
      provider = provider,
      refreshable = TRUE,
      type = "AzureToken",
      audience_ref = audience_ref
    ),
    class = "fabric_credential"
  )
}

#' Extract and validate a bearer token from an 'AzureAuth' token
#'
#' @param token Value expected to be an `AzureAuth::AzureToken` object
#' @return One validated bearer-token string
#' @details Used by both forms of 'AzureAuth'-backed internal credentials
#' @keywords internal
#' @noRd
fabric_extract_azure_token <- function(token) {
  if (!AzureAuth::is_azure_token(token)) {
    .fabric_abort("AzureAuth did not return an AzureToken object")
  }
  access_token <- token$credentials$access_token
  fabric_validate_bearer_token(access_token, "AzureToken access token")
  access_token
}

#' Validate a bearer-token string
#'
#' @param token Value expected to contain one bearer token
#' @param label Friendly input name used in an error message
#' @return `token`, invisibly, after validation
#' @details Called before any supplied or acquired token is sent in a request
#' @keywords internal
#' @noRd
fabric_validate_bearer_token <- function(token, label) {
  if (
    !is.character(token) ||
      length(token) != 1L ||
      is.na(token) ||
      !nzchar(token)
  ) {
    .fabric_abort(paste0(label, " must be one non-empty string"))
  }
  unsafe <- tryCatch(
    grepl("[\\p{Z}\\p{C}]", token, perl = TRUE),
    warning = function(...) TRUE,
    error = function(...) TRUE
  )
  if (isTRUE(unsafe)) {
    .fabric_abort(
      paste0(label, " must not contain whitespace or control characters")
    )
  }
  invisible(token)
}

#' Invoke token callbacks with their supported arguments
#'
#' @param provider User-supplied token function
#' @param audience Service audience requested by the current API call
#' @param force_refresh Whether the caller is retrying after authentication
#'   failed
#' @return One validated bearer-token string
#' @details `fabric_credential()` uses this adapter so callbacks may accept
#'   zero, one, or both supported arguments
#' @keywords internal
#' @noRd
fabric_call_token_provider <- function(provider, audience, force_refresh) {
  args <- names(formals(provider))
  if (is.null(args) || !length(args)) {
    token <- provider()
  } else if ("..." %in% args) {
    token <- provider(audience = audience, force_refresh = force_refresh)
  } else {
    supplied <- list()
    if ("audience" %in% args) {
      supplied$audience <- audience
    }

    if ("force_refresh" %in% args) {
      supplied$force_refresh <- force_refresh
    }

    if (!length(supplied) && length(args)) {
      supplied[[args[[1L]]]] <- audience
    }
    token <- do.call(provider, supplied)
  }

  if (is.list(token)) {
    token <- token$access_token %||% token$token
  }

  if (
    !is.character(token) ||
      length(token) != 1L ||
      is.na(token) ||
      !nzchar(token)
  ) {
    .fabric_abort(
      "The token provider must return one non-empty bearer token"
    )
  }
  fabric_validate_bearer_token(token, "The token provider result")
  token
}

#' Obtain a bearer token from an internal credential
#'
#' @param credential Internal credential created by `fabric_credential()`
#' @param audience Service audience needed for the current request
#' @param force_refresh Whether to request a fresh token
#' @return One bearer-token string
#' @details Shared HTTP and service helpers call this immediately before a
#'   request, keeping raw tokens out of their stored state
#' @keywords internal
#' @noRd
fabric_get_token <- function(credential, audience, force_refresh = FALSE) {
  if (!inherits(credential, "fabric_credential")) {
    .fabric_abort("Invalid Fabric credential")
  }
  fabric_bind_fixed_credential_audience(credential, audience)
  token <- credential$provider(audience, force_refresh = force_refresh)
  fabric_validate_bearer_token(token, "The credential provider result")
  token
}

# Prevent an opaque, caller-supplied token from being forwarded to two OAuth
# resources. Entra access tokens have one audience; only automatic credentials
# and provider callbacks can acquire a different token for each request.
fabric_bind_fixed_credential_audience <- function(credential, audience) {
  if (
    is.null(credential$type) ||
      !credential$type %in% c("static", "AzureToken") ||
      !is.environment(credential$audience_ref)
  ) {
    return(invisible(audience))
  }

  key <- paste(sort(unique(sub("/[^/]+$", "", audience))), collapse = " ")
  if (is.null(credential$audience_ref$key)) {
    credential$audience_ref$key <- key
    credential$audience_ref$audience <- audience
    return(invisible(audience))
  }
  if (identical(credential$audience_ref$key, key)) {
    return(invisible(audience))
  }

  .fabric_abort(
    paste0(
      "A fixed bearer token or AzureToken cannot be reused for a different ",
      "audience; supply the service token separately (for example with ",
      "storage_token or sql_token) or use an audience-aware token-provider ",
      "function"
    ),
    class = c("fabric_multi_audience_auth_error", "fabric_auth_error"),
    audience = audience,
    bound_audience = credential$audience_ref$audience
  )
}
