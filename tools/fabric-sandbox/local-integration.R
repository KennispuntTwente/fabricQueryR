# Local bootstrap for the persistent Fabric integration sandbox.
#
# From the repository root:
# source("tools/fabric-sandbox/local-integration.R")
# run_fabric_integration_tests()

.fabric_local_script <- tryCatch(
  normalizePath(
    sys.frame(1)$ofile,
    winslash = "/",
    mustWork = TRUE
  ),
  error = function(error) ""
)

.fabric_local_repository <- if (nzchar(.fabric_local_script)) {
  normalizePath(
    file.path(dirname(.fabric_local_script), "..", ".."),
    winslash = "/",
    mustWork = TRUE
  )
} else {
  normalizePath(getwd(), winslash = "/", mustWork = TRUE)
}

fabric_local_cached_contexts <- function() {
  tokens <- unname(AzureAuth::list_azure_tokens())
  contexts <- lapply(tokens, function(token) {
    scopes <- c(token[["scope"]], token[["resource"]])
    if (!"https://api.fabric.microsoft.com/.default" %in% scopes) {
      return(NULL)
    }
    tenant <- tryCatch(token[["tenant"]], error = function(error) "")
    client <- tryCatch(
      token[["client"]][["client_id"]],
      error = function(error) ""
    )
    if (!nzchar(tenant) || !nzchar(client)) {
      return(NULL)
    }
    list(tenant_id = tenant, client_id = client)
  })
  contexts <- Filter(Negate(is.null), contexts)
  if (!length(contexts)) {
    return(contexts)
  }
  keys <- vapply(
    contexts,
    function(context) {
      paste(context$tenant_id, context$client_id, sep = "/")
    },
    character(1)
  )
  contexts[!duplicated(keys)]
}

fabric_local_auth_context <- function(
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID")
) {
  contexts <- fabric_local_cached_contexts()
  selected <- NULL

  if (!nzchar(tenant_id)) {
    if (length(contexts) == 1L) {
      selected <- contexts[[1L]]
      tenant_id <- selected$tenant_id
      message("Using tenant and client from the cached Fabric AzureAuth token.")
    } else if (length(contexts) > 1L) {
      stop(
        paste(
          "Multiple cached Fabric identities were found.",
          "Set FABRICQUERYR_TENANT_ID to select one."
        ),
        call. = FALSE
      )
    } else {
      tenant_id <- "dhrkoninghotmail628.onmicrosoft.com"
      message(
        "No cached Fabric token found; AzureAuth will prompt for sign-in."
      )
    }
  }

  if (!nzchar(client_id)) {
    matching <- Filter(
      function(context) identical(context$tenant_id, tenant_id),
      contexts
    )
    if (length(matching) == 1L) {
      selected <- matching[[1L]]
    }
    client_id <- if (!is.null(selected)) {
      selected$client_id
    } else {
      "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
    }
  }

  list(tenant_id = tenant_id, client_id = client_id)
}

fabric_local_cached_token <- function(audience, tenant_id, client_id) {
  tokens <- unname(AzureAuth::list_azure_tokens())
  matches <- Filter(
    function(token) {
      scopes <- c(token[["scope"]], token[["resource"]])
      tenant <- tryCatch(token[["tenant"]], error = function(error) "")
      client <- tryCatch(
        token[["client"]][["client_id"]],
        error = function(error) ""
      )
      audience %in%
        scopes &&
        identical(tenant, tenant_id) &&
        identical(client, client_id)
    },
    tokens
  )
  if (length(matches)) matches[[1L]] else NULL
}

fabric_local_exchange_token <- function(token, audience) {
  source <- if (AzureAuth::is_azure_token(token)) {
    list(
      tenant_id = token[["tenant"]],
      client_id = token[["client"]][["client_id"]],
      refresh_token = token[["credentials"]][["refresh_token"]]
    )
  } else {
    token
  }
  if (
    is.null(source$refresh_token) ||
      !is.character(source$refresh_token) ||
      length(source$refresh_token) != 1L ||
      !nzchar(source$refresh_token)
  ) {
    stop("The cached AzureAuth token cannot be refreshed", call. = FALSE)
  }

  response <- httr2::request(sprintf(
    "https://login.microsoftonline.com/%s/oauth2/v2.0/token",
    source$tenant_id
  )) |>
    httr2::req_body_form(
      client_id = source$client_id,
      grant_type = "refresh_token",
      refresh_token = source$refresh_token,
      scope = paste(audience, "offline_access")
    ) |>
    httr2::req_perform()
  body <- httr2::resp_body_json(response, simplifyVector = TRUE)
  if (
    is.null(body$access_token) ||
      !is.character(body$access_token) ||
      length(body$access_token) != 1L ||
      !nzchar(body$access_token)
  ) {
    stop("Microsoft Entra returned an empty access token", call. = FALSE)
  }

  structure(
    list(
      tenant_id = source$tenant_id,
      client_id = source$client_id,
      audience = audience,
      access_token = body$access_token,
      refresh_token = body$refresh_token,
      expires_on = as.numeric(Sys.time()) + as.numeric(body$expires_in)
    ),
    class = "fabric_local_token"
  )
}

fabric_local_acquire_tokens <- function(
  tenant_id,
  client_id,
  auth_args = list()
) {
  if (!is.list(auth_args)) {
    stop("auth_args must be a list", call. = FALSE)
  }
  if (
    length(auth_args) &&
      (is.null(names(auth_args)) ||
        anyNA(names(auth_args)) ||
        !all(nzchar(names(auth_args))))
  ) {
    stop("auth_args must be fully named", call. = FALSE)
  }
  reserved <- intersect(
    names(auth_args),
    c("resource", "tenant", "app", "version")
  )
  if (length(reserved)) {
    stop(
      paste(
        "auth_args cannot override",
        paste(reserved, collapse = ", ")
      ),
      call. = FALSE
    )
  }

  audiences <- c(
    Fabric = "https://api.fabric.microsoft.com/.default",
    `Power BI` = "https://analysis.windows.net/powerbi/api/.default",
    SQL = "https://database.windows.net/.default",
    OneLake = "https://storage.azure.com/.default",
    Kusto = "https://api.kusto.windows.net/.default"
  )
  tokens <- list()
  refresh_source <- fabric_local_cached_token(
    "https://api.fabric.microsoft.com/.default",
    tenant_id,
    client_id
  )
  use_cache <- !identical(auth_args$use_cache, FALSE)
  exchange_cached <- use_cache && is.null(auth_args$auth_type)
  for (label in names(audiences)) {
    audience <- unname(audiences[[label]])
    message("Acquiring ", label, " token (cached tokens are reused)...")
    cached <- if (use_cache) {
      fabric_local_cached_token(audience, tenant_id, client_id)
    } else {
      NULL
    }
    if (!is.null(cached)) {
      tokens[[audience]] <- cached
      if (is.null(refresh_source)) {
        refresh_source <- cached
      }
      next
    }
    if (exchange_cached && !is.null(refresh_source)) {
      exchanged <- tryCatch(
        fabric_local_exchange_token(refresh_source, audience),
        error = identity
      )
      if (!inherits(exchanged, "error")) {
        tokens[[audience]] <- exchanged
        refresh_source <- exchanged
        next
      }
      message(
        "Cached refresh could not acquire the ",
        label,
        " audience; AzureAuth will prompt for sign-in."
      )
    }
    args <- c(
      list(
        resource = c(audience, "offline_access"),
        tenant = tenant_id,
        app = client_id,
        version = 2
      ),
      auth_args
    )
    if (is.null(args$use_cache)) {
      args$use_cache <- TRUE
    }
    token <- do.call(AzureAuth::get_azure_token, args)
    if (!AzureAuth::is_azure_token(token)) {
      stop("AzureAuth did not return an AzureToken", call. = FALSE)
    }
    tokens[[audience]] <- token
    refresh_source <- token
  }
  tokens
}

fabric_local_jwt_claims <- function(token) {
  parts <- strsplit(token, ".", fixed = TRUE)[[1L]]
  if (length(parts) != 3L) {
    stop("Fabric access token is not a JWT", call. = FALSE)
  }
  payload <- chartr("-_", "+/", parts[[2L]])
  padding <- (4L - nchar(payload) %% 4L) %% 4L
  payload <- paste0(payload, strrep("=", padding))
  tryCatch(
    jsonlite::fromJSON(rawToChar(jsonlite::base64_dec(payload))),
    error = function(error) {
      stop("Could not decode Fabric access-token claims", call. = FALSE)
    }
  )
}

fabric_local_token_provider <- function(tokens) {
  force(tokens)
  function(audience, force_refresh = FALSE) {
    token <- tokens[[audience]]
    if (is.null(token)) {
      stop(
        paste("No local AzureAuth token for audience", audience),
        call. = FALSE
      )
    }
    if (inherits(token, "fabric_local_token")) {
      valid <- is.numeric(token$expires_on) &&
        length(token$expires_on) == 1L &&
        token$expires_on > as.numeric(Sys.time()) + 60
      if (isTRUE(force_refresh) || !valid) {
        token <- fabric_local_exchange_token(token, audience)
        tokens[[audience]] <<- token
      }
      value <- token$access_token
    } else {
      valid <- is.function(token$validate) && isTRUE(token$validate())
      if (isTRUE(force_refresh) || !valid) {
        token$refresh()
      }
      value <- token$credentials$access_token
    }
    if (
      !is.character(value) ||
        length(value) != 1L ||
        is.na(value) ||
        !nzchar(value)
    ) {
      stop("AzureAuth returned an empty access token", call. = FALSE)
    }
    value
  }
}

fabric_local_run <- function(command, args, label) {
  status <- system2(command, args)
  if (!identical(status, 0L)) {
    stop(
      sprintf("%s failed with exit status %s", label, status),
      call. = FALSE
    )
  }
  invisible(status)
}

fabric_local_require_dependencies <- function(install_adbc_driver) {
  packages <- c(
    "devtools",
    "AzureAuth",
    "DBI",
    "duckdb",
    "fs",
    "odbc",
    "adbi",
    "adbcdrivermanager",
    "nanoarrow",
    "arrow"
  )
  missing <- packages[
    !vapply(packages, requireNamespace, logical(1), quietly = TRUE)
  ]
  if (length(missing)) {
    stop(
      paste(
        "Install the missing R integration dependencies:",
        paste(missing, collapse = ", ")
      ),
      call. = FALSE
    )
  }

  drivers <- odbc::odbcListDrivers()
  if (
    !"name" %in% names(drivers) ||
      !any(grepl("ODBC Driver 18 for SQL Server", drivers$name, fixed = TRUE))
  ) {
    stop(
      "Microsoft ODBC Driver 18 for SQL Server is required.",
      call. = FALSE
    )
  }

  adbc <- try(
    adbcdrivermanager::adbc_driver("mssql"),
    silent = TRUE
  )
  if (inherits(adbc, "try-error")) {
    if (!isTRUE(install_adbc_driver)) {
      stop(
        paste(
          "The ADBC mssql driver is missing.",
          "Run `uvx dbc==0.3.0 install \"mssql>=1.5,<2\"`",
          "or use install_adbc_driver = TRUE."
        ),
        call. = FALSE
      )
    }
    uvx <- Sys.which("uvx")
    if (!nzchar(uvx)) {
      stop("uvx is required to install the ADBC mssql driver.", call. = FALSE)
    }
    message("Installing the locked ADBC mssql driver...")
    fabric_local_run(
      uvx,
      c(
        "dbc==0.3.0",
        "install",
        shQuote("mssql>=1.5,<2")
      ),
      "ADBC driver installation"
    )
    adbc <- try(
      adbcdrivermanager::adbc_driver("mssql"),
      silent = TRUE
    )
    if (inherits(adbc, "try-error")) {
      stop(
        "The installed ADBC mssql driver could not be loaded.",
        call. = FALSE
      )
    }
  }
  invisible(TRUE)
}

run_fabric_integration_tests <- function(
  workspace_name = "fabricqueryr-dev-dhrkoning",
  expected_user_id = "9b7dcb13-8485-4429-8b4f-7f1f6ce6ebf5",
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID"),
  auth_args = list(),
  install_adbc_driver = TRUE,
  filter = "integration-fabric",
  repository_root = .fabric_local_repository
) {
  if (
    !is.character(filter) ||
      length(filter) != 1L ||
      is.na(filter) ||
      !nzchar(filter)
  ) {
    stop("filter must be one non-empty testthat filter", call. = FALSE)
  }
  repository_root <- normalizePath(
    repository_root,
    winslash = "/",
    mustWork = TRUE
  )
  description <- file.path(repository_root, "DESCRIPTION")
  if (
    !file.exists(description) ||
      !identical(
        read.dcf(description, fields = "Package")[[1L]],
        "fabricQueryR"
      )
  ) {
    stop("repository_root is not a fabricQueryR checkout", call. = FALSE)
  }

  old_directory <- setwd(repository_root)
  on.exit(setwd(old_directory), add = TRUE)
  fabric_local_require_dependencies(install_adbc_driver)

  uv <- Sys.which("uv")
  if (!nzchar(uv)) {
    stop("uv is required for local sandbox discovery.", call. = FALSE)
  }

  devtools::load_all(repository_root, quiet = TRUE)
  context <- fabric_local_auth_context(tenant_id, client_id)
  tokens <- fabric_local_acquire_tokens(
    context$tenant_id,
    context$client_id,
    auth_args
  )
  provider <- fabric_local_token_provider(tokens)
  fabric_token <- provider(
    "https://api.fabric.microsoft.com/.default"
  )
  claims <- fabric_local_jwt_claims(fabric_token)
  actual_user_id <- if (is.null(claims$oid)) "<missing>" else claims$oid
  if (!identical(tolower(actual_user_id), tolower(expected_user_id))) {
    stop(
      paste(
        "AzureAuth signed in as object ID",
        shQuote(actual_user_id),
        "instead of",
        shQuote(expected_user_id),
        "Rerun with auth_args = list(use_cache = FALSE) to sign in again."
      ),
      call. = FALSE
    )
  }

  token_variables <- c(
    "https://api.fabric.microsoft.com/.default" = "FABRIC_TEST_API_TOKEN",
    "https://analysis.windows.net/powerbi/api/.default" = "FABRIC_TEST_PBI_TOKEN",
    "https://database.windows.net/.default" = "FABRIC_TEST_SQL_TOKEN",
    "https://storage.azure.com/.default" = "FABRIC_TEST_STORAGE_TOKEN",
    "https://api.kusto.windows.net/.default" = "FABRIC_TEST_KUSTO_TOKEN"
  )
  managed_variables <- c(
    unname(token_variables),
    "FABRIC_SANDBOX_USE_ENV_TOKENS",
    "FABRIC_WORKSPACE_ID",
    "FABRIC_WORKSPACE_NAME",
    "FABRIC_TEST_MANIFEST",
    "FABRIC_INTEGRATION_REQUIRED"
  )
  old_environment <- Sys.getenv(managed_variables, unset = NA_character_)
  old_provider <- getOption("fabricQueryR.integration_token_provider")
  old_auth_config <- getOption("fabricQueryR.integration_auth_config")
  on.exit(
    {
      options(fabricQueryR.integration_token_provider = old_provider)
      options(fabricQueryR.integration_auth_config = old_auth_config)
      for (variable in names(old_environment)) {
        value <- old_environment[[variable]]
        if (is.na(value)) {
          Sys.unsetenv(variable)
        } else {
          do.call(Sys.setenv, setNames(list(value), variable))
        }
      }
    },
    add = TRUE
  )

  environment_tokens <- vapply(
    names(token_variables),
    provider,
    character(1)
  )
  names(environment_tokens) <- unname(token_variables)
  do.call(Sys.setenv, as.list(environment_tokens))
  Sys.setenv(
    FABRIC_SANDBOX_USE_ENV_TOKENS = "true",
    FABRIC_WORKSPACE_NAME = workspace_name,
    FABRIC_TEST_MANIFEST = file.path(
      repository_root,
      ".fabric-test-manifest.json"
    ),
    FABRIC_INTEGRATION_REQUIRED = "true"
  )
  options(fabricQueryR.integration_token_provider = provider)
  options(
    fabricQueryR.integration_auth_config = list(
      tenant_id = context$tenant_id,
      client_id = context$client_id,
      auth_args = list(use_cache = TRUE)
    )
  )

  workspaces <- fabric_workspaces(
    roles = "Admin",
    tenant_id = context$tenant_id,
    client_id = context$client_id,
    token = provider
  )
  matches <- Filter(
    function(workspace) identical(workspace$displayName, workspace_name),
    workspaces
  )
  if (!length(matches)) {
    stop(
      paste(
        "Persistent workspace",
        shQuote(workspace_name),
        "was not found. Run the GitHub workflow with action=rebuild first."
      ),
      call. = FALSE
    )
  }
  if (length(matches) != 1L) {
    stop(
      paste(
        "Expected exactly one persistent workspace, found",
        length(matches)
      ),
      call. = FALSE
    )
  }
  workspace <- matches[[1L]]
  marker <- workspace$description
  if (
    is.null(marker) ||
      !startsWith(marker, "fabricqueryr-persistent;")
  ) {
    stop(
      "The matching workspace does not carry the persistent ownership marker.",
      call. = FALSE
    )
  }
  Sys.setenv(FABRIC_WORKSPACE_ID = workspace$id)

  message("Refreshing the local integration manifest...")
  fabric_local_run(
    uv,
    c(
      "--directory",
      "tools/fabric-sandbox",
      "sync",
      "--locked"
    ),
    "Sandbox environment sync"
  )
  fabric_local_run(
    uv,
    c(
      "--directory",
      "tools/fabric-sandbox",
      "run",
      "fabric-sandbox",
      "discover"
    ),
    "Fabric endpoint discovery"
  )

  message(
    "Running Fabric integration tests matching ",
    shQuote(filter),
    " against ",
    workspace_name,
    " (",
    workspace$id,
    ")..."
  )
  devtools::test(
    filter = filter,
    stop_on_failure = TRUE
  )
}
