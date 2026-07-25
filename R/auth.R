.fabric_audience <- list(
  fabric = "https://api.fabric.microsoft.com/.default",
  graphql = paste0(
    "https://analysis.windows.net/powerbi/api/",
    "GraphQLApi.Execute.All"
  ),
  power_bi = "https://analysis.windows.net/powerbi/api/.default",
  sql = "https://database.windows.net/.default",
  storage = "https://storage.azure.com/.default",
  kusto = "https://api.kusto.windows.net/.default"
)

.fabric_azure_auth_args <- setdiff(
  names(formals(AzureAuth::get_azure_token)),
  c("resource", "tenant", "app", "version")
)

#' Create an internal audience-aware credential
#'
#' @param tenant_id Entra tenant ID.
#' @param client_id Entra application ID.
#' @param token Optional token supplied as an `AzureAuth::AzureToken` object, a
#'   bearer-token string, or a token-provider function. `NULL` delegates token
#'   acquisition, cache reuse, and interactive login to `AzureAuth`.
#' @param auth_args Named list of additional arguments passed to
#'   [AzureAuth::get_azure_token()]. The package supplies `resource`, `tenant`,
#'   `app`, and `version`.
#' @return An internal `fabric_credential` object.
#' @keywords internal
#' @noRd
fabric_credential <- function(
  tenant_id = NULL,
  client_id = NULL,
  token = NULL,
  auth_args = list()
) {
  fabric_validate_auth_args(auth_args)
  if (!is.null(token) && length(auth_args)) {
    rlang::abort(
      "auth_args can only be used when fabricQueryR acquires the token"
    )
  }

  if (!is.null(token)) {
    if (AzureAuth::is_azure_token(token)) {
      return(fabric_azure_token_credential(token))
    }
    if (is.character(token)) {
      fabric_validate_bearer_token(token, "token")
      return(structure(
        list(
          provider = function(audience, force_refresh = FALSE) token,
          refreshable = FALSE,
          type = "static"
        ),
        class = "fabric_credential"
      ))
    } else if (is.function(token)) {
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
      return(token)
    } else {
      rlang::abort(
        paste0(
          "token must be an AzureAuth AzureToken object, one bearer-token ",
          "string, or a token-provider function"
        )
      )
    }
  }

  if (is.null(tenant_id) || !nzchar(tenant_id)) {
    rlang::abort(
      "tenant_id is required (or set FABRICQUERYR_TENANT_ID env var)"
    )
  }
  if (is.null(client_id) || !nzchar(client_id)) {
    rlang::abort(
      "client_id is required (or set FABRICQUERYR_CLIENT_ID env var)"
    )
  }

  cache <- new.env(parent = emptyenv())
  provider <- function(audience, force_refresh = FALSE) {
    key <- gsub("[^A-Za-z0-9]", "_", audience)
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
    list(provider = provider, refreshable = TRUE, type = "AzureAuth"),
    class = "fabric_credential"
  )
}

#' Validate AzureAuth passthrough arguments
#' @keywords internal
#' @noRd
fabric_validate_auth_args <- function(auth_args) {
  if (!is.list(auth_args)) {
    rlang::abort("auth_args must be a named list")
  }
  if (!length(auth_args)) {
    return(invisible(auth_args))
  }
  if (
    is.null(names(auth_args)) ||
      anyNA(names(auth_args)) ||
      !all(nzchar(names(auth_args)))
  ) {
    rlang::abort("auth_args must be a fully named list")
  }
  if (anyDuplicated(names(auth_args))) {
    rlang::abort("auth_args names must be unique")
  }
  reserved <- intersect(
    names(auth_args),
    c("resource", "tenant", "app", "version")
  )
  if (length(reserved)) {
    rlang::abort(paste0(
      "auth_args cannot override ",
      paste(reserved, collapse = ", "),
      "; use the function's authentication arguments instead"
    ))
  }
  unknown <- setdiff(names(auth_args), .fabric_azure_auth_args)
  if (length(unknown)) {
    rlang::abort(paste0(
      "Unknown AzureAuth argument",
      if (length(unknown) == 1L) " " else "s ",
      paste(unknown, collapse = ", ")
    ))
  }
  invisible(auth_args)
}

#' Choose Azure v2 scopes for an AzureAuth flow
#' @keywords internal
#' @noRd
fabric_azure_scopes <- function(audience, auth_args) {
  auth_type <- auth_args$auth_type
  inferred_client_credentials <- is.null(auth_type) &&
    (!is.null(auth_args$password) || !is.null(auth_args$certificate)) &&
    is.null(auth_args$username) &&
    is.null(auth_args$on_behalf_of)
  if (
    identical(auth_type, "client_credentials") ||
      inferred_client_credentials
  ) {
    audience
  } else {
    c(audience, "offline_access")
  }
}

#' Adapt a refreshable AzureAuth token
#' @keywords internal
#' @noRd
fabric_azure_token_credential <- function(token) {
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
    list(provider = provider, refreshable = TRUE, type = "AzureToken"),
    class = "fabric_credential"
  )
}

#' Extract and validate a bearer token from an AzureAuth token
#' @keywords internal
#' @noRd
fabric_extract_azure_token <- function(token) {
  if (!AzureAuth::is_azure_token(token)) {
    rlang::abort("AzureAuth did not return an AzureToken object")
  }
  access_token <- token$credentials$access_token
  fabric_validate_bearer_token(access_token, "AzureToken access token")
  access_token
}

#' Validate a bearer-token string
#' @keywords internal
#' @noRd
fabric_validate_bearer_token <- function(token, label) {
  if (
    !is.character(token) ||
      length(token) != 1L ||
      is.na(token) ||
      !nzchar(token)
  ) {
    rlang::abort(paste0(label, " must be one non-empty string"))
  }
  invisible(token)
}

#' Invoke token callbacks with their supported arguments
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
    rlang::abort(
      "The token provider must return one non-empty bearer token"
    )
  }
  token
}

#' Obtain a bearer token from an internal credential
#' @keywords internal
#' @noRd
fabric_get_token <- function(credential, audience, force_refresh = FALSE) {
  if (!inherits(credential, "fabric_credential")) {
    rlang::abort("Invalid Fabric credential")
  }
  credential$provider(audience, force_refresh = force_refresh)
}
