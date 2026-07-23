.fabric_audience <- list(
  fabric = "https://api.fabric.microsoft.com/.default",
  power_bi = "https://analysis.windows.net/powerbi/api/.default",
  sql = "https://database.windows.net/.default",
  storage = "https://storage.azure.com/.default",
  kusto = "https://kusto.kusto.windows.net/.default"
)

#' Create an internal audience-aware credential
#'
#' @param tenant_id Entra tenant ID.
#' @param client_id Entra application ID.
#' @param access_token Optional static bearer token.
#' @param token_provider Optional callback that returns a bearer token. It may
#'   accept `audience` and `force_refresh` arguments.
#' @return An internal `fabric_credential` object.
#' @keywords internal
#' @noRd
fabric_credential <- function(
  tenant_id = NULL,
  client_id = NULL,
  access_token = NULL,
  token_provider = NULL
) {
  if (!is.null(access_token) && !is.null(token_provider)) {
    stop(
      "Supply only one of access_token and token_provider.",
      call. = FALSE
    )
  }
  if (!is.null(access_token)) {
    stopifnot(
      is.character(access_token),
      length(access_token) == 1L,
      nzchar(access_token)
    )
    return(structure(
      list(
        provider = function(audience, force_refresh = FALSE) access_token,
        refreshable = FALSE,
        type = "static"
      ),
      class = "fabric_credential"
    ))
  }
  if (!is.null(token_provider)) {
    if (!is.function(token_provider)) {
      stop("token_provider must be a function.", call. = FALSE)
    }
    return(structure(
      list(
        provider = function(audience, force_refresh = FALSE) {
          fabric_call_token_provider(
            token_provider,
            audience,
            force_refresh
          )
        },
        refreshable = TRUE,
        type = "callback"
      ),
      class = "fabric_credential"
    ))
  }

  if (is.null(tenant_id) || !nzchar(tenant_id)) {
    stop(
      "tenant_id is required (or set FABRICQUERYR_TENANT_ID env var).",
      call. = FALSE
    )
  }
  if (is.null(client_id) || !nzchar(client_id)) {
    stop(
      "client_id is required (or set FABRICQUERYR_CLIENT_ID env var).",
      call. = FALSE
    )
  }

  cache <- new.env(parent = emptyenv())
  provider <- function(audience, force_refresh = FALSE) {
    key <- gsub("[^A-Za-z0-9]", "_", audience)
    token <- cache[[key]]
    if (is.null(token) || isTRUE(force_refresh)) {
      token <- AzureAuth::get_azure_token(
        tenant = tenant_id,
        app = client_id,
        version = 2,
        resource = c(audience, "offline_access")
      )
      if (isTRUE(force_refresh) && is.function(token$refresh)) {
        refreshed <- try(token$refresh(), silent = TRUE)
        if (!inherits(refreshed, "try-error") && !is.null(refreshed)) {
          token <- refreshed
        }
      }
      cache[[key]] <- token
    }
    token$credentials$access_token
  }
  structure(
    list(provider = provider, refreshable = TRUE, type = "AzureAuth"),
    class = "fabric_credential"
  )
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
    stop(
      "token_provider must return one non-empty bearer token.",
      call. = FALSE
    )
  }
  token
}

#' Obtain a bearer token from an internal credential
#' @keywords internal
#' @noRd
fabric_get_token <- function(credential, audience, force_refresh = FALSE) {
  if (!inherits(credential, "fabric_credential")) {
    stop("Invalid Fabric credential.", call. = FALSE)
  }
  credential$provider(audience, force_refresh = force_refresh)
}
