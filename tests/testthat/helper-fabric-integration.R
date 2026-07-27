fabric_test_required <- function() {
  tolower(Sys.getenv("FABRIC_INTEGRATION_REQUIRED")) %in%
    c("1", "true", "yes")
}

fabric_test_skip_or_fail <- function(condition, message) {
  if (!isTRUE(condition)) {
    return(invisible(FALSE))
  }
  if (fabric_test_required()) {
    rlang::abort(message)
  }
  testthat::skip(message)
}

fabric_test_repository_root <- function(start = getwd()) {
  current <- normalizePath(start, winslash = "/", mustWork = TRUE)
  repeat {
    description <- file.path(current, "DESCRIPTION")
    if (file.exists(description)) {
      package <- tryCatch(
        read.dcf(description, fields = "Package")[[1L]],
        error = function(error) ""
      )
      if (identical(package, "fabricQueryR")) {
        return(current)
      }
    }
    parent <- dirname(current)
    if (identical(parent, current)) {
      rlang::abort(
        paste("Could not locate the fabricQueryR repository from", start)
      )
    }
    current <- parent
  }
}

fabric_test_manifest_path <- function(
  start = getwd(),
  configured = Sys.getenv("FABRIC_TEST_MANIFEST")
) {
  if (nzchar(configured)) {
    return(configured)
  }
  root <- tryCatch(
    fabric_test_repository_root(start),
    error = function(error) {
      normalizePath(start, winslash = "/", mustWork = TRUE)
    }
  )
  file.path(
    root,
    ".fabric-test-manifest.json"
  )
}

fabric_test_manifest <- function() {
  path <- fabric_test_manifest_path()
  fabric_test_skip_or_fail(
    !file.exists(path),
    paste("Fabric integration manifest not found:", path)
  )
  jsonlite::fromJSON(path, simplifyVector = FALSE)
}

fabric_test_token_variables <- c(
  "https://api.fabric.microsoft.com/.default" = "FABRIC_TEST_API_TOKEN",
  "https://analysis.windows.net/powerbi/api/.default" = "FABRIC_TEST_PBI_TOKEN",
  "https://database.windows.net/.default" = "FABRIC_TEST_SQL_TOKEN",
  "https://storage.azure.com/.default" = "FABRIC_TEST_STORAGE_TOKEN",
  "https://api.kusto.windows.net/.default" = "FABRIC_TEST_KUSTO_TOKEN"
)

fabric_test_token <- function(variable) {
  provider <- getOption("fabricQueryR.integration_token_provider")
  token <- if (is.null(provider)) {
    Sys.getenv(variable)
  } else {
    if (!is.function(provider)) {
      rlang::abort(
        "fabricQueryR.integration_token_provider must be a function"
      )
    }
    provider(fabric_test_token_audience(variable))
  }
  fabric_test_skip_or_fail(
    !nzchar(token),
    paste("Fabric integration token not set:", variable)
  )
  token
}

fabric_test_token_variable <- function(audience) {
  index <- match(audience, names(fabric_test_token_variables))
  if (is.na(index)) {
    rlang::abort(
      paste("No provisioned Fabric integration token for audience:", audience)
    )
  }
  unname(fabric_test_token_variables[[index]])
}

fabric_test_token_audience <- function(variable) {
  index <- match(variable, unname(fabric_test_token_variables))
  if (is.na(index)) {
    rlang::abort(
      paste("No Fabric integration audience for token variable:", variable)
    )
  }
  names(fabric_test_token_variables)[[index]]
}

fabric_test_provisioned_token <- function(audience) {
  fabric_test_token(fabric_test_token_variable(audience))
}

fabric_test_token_provider <- function(
  acquire = fabric_test_provisioned_token
) {
  cache <- new.env(parent = emptyenv())
  function(audience, force_refresh = FALSE) {
    key <- gsub("[^A-Za-z0-9]", "_", audience)
    if (isTRUE(force_refresh) || is.null(cache[[key]])) {
      cache[[key]] <- acquire(audience)
    }
    cache[[key]]
  }
}

fabric_test_azure_auth_config <- function() {
  local <- getOption("fabricQueryR.integration_auth_config")
  if (!is.null(local)) {
    required <- c("tenant_id", "client_id", "auth_args")
    if (
      !is.list(local) ||
        !all(required %in% names(local)) ||
        !nzchar(local$tenant_id) ||
        !nzchar(local$client_id) ||
        !is.list(local$auth_args)
    ) {
      rlang::abort(
        "fabricQueryR.integration_auth_config is invalid"
      )
    }
    return(local)
  }

  secret <- Sys.getenv("FABRIC_TEST_AUTH_CLIENT_SECRET")
  if (!nzchar(secret)) {
    testthat::skip(
      paste(
        "AzureAuth integration is optional; set",
        "FABRIC_TEST_AUTH_CLIENT_SECRET to enable it"
      )
    )
  }
  tenant_id <- Sys.getenv("FABRIC_TEST_AUTH_TENANT_ID")
  client_id <- Sys.getenv("FABRIC_TEST_AUTH_CLIENT_ID")
  missing <- c(
    FABRIC_TEST_AUTH_TENANT_ID = tenant_id,
    FABRIC_TEST_AUTH_CLIENT_ID = client_id
  )
  missing <- names(missing)[!nzchar(missing)]
  if (length(missing)) {
    rlang::abort(paste(
      "AzureAuth integration configuration is incomplete; missing",
      paste(missing, collapse = ", ")
    ))
  }
  list(
    tenant_id = tenant_id,
    client_id = client_id,
    auth_args = list(
      password = secret,
      auth_type = "client_credentials",
      use_cache = FALSE
    )
  )
}

fabric_test_require_package <- function(package) {
  fabric_test_skip_or_fail(
    !requireNamespace(package, quietly = TRUE),
    paste("Fabric integration package is not installed:", package)
  )
  invisible(TRUE)
}

fabric_test_sql_backends <- function() {
  for (package in c(
    "DBI",
    "odbc",
    "adbi",
    "adbcdrivermanager",
    "nanoarrow",
    "arrow"
  )) {
    fabric_test_require_package(package)
  }
  c("odbc", "adbc")
}

fabric_test_spark_table <- function(manifest, lakehouse) {
  paste(
    sprintf(
      "`%s`",
      c(
        manifest$workspace_name,
        lakehouse$display_name,
        lakehouse$schema,
        lakehouse$tables$basic
      )
    ),
    collapse = "."
  )
}

fabric_test_manifest_item <- function(manifest, name) {
  item <- manifest$items[[name]]
  if (is.null(item)) {
    rlang::abort(
      sprintf(
        "Fabric integration manifest does not provision required item '%s'",
        name
      )
    )
  }
  item
}
