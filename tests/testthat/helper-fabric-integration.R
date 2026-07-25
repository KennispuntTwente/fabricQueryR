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

fabric_test_manifest <- function() {
  path <- Sys.getenv(
    "FABRIC_TEST_MANIFEST",
    unset = file.path(getwd(), ".fabric-test-manifest.json")
  )
  fabric_test_skip_or_fail(
    !file.exists(path),
    paste("Fabric integration manifest not found:", path)
  )
  jsonlite::fromJSON(path, simplifyVector = FALSE)
}

fabric_test_token <- function(variable) {
  token <- Sys.getenv(variable)
  fabric_test_skip_or_fail(
    !nzchar(token),
    paste("Fabric integration token not set:", variable)
  )
  token
}

fabric_test_azure_cli_token <- function(audience) {
  fabric_test_skip_or_fail(
    nzchar(Sys.which("az")),
    "Azure CLI is required for live package-managed authentication tests"
  )
  output <- suppressWarnings(system2(
    "az",
    c(
      "account",
      "get-access-token",
      "--scope",
      shQuote(audience),
      "--query",
      "accessToken",
      "--output",
      "tsv",
      "--only-show-errors"
    ),
    stdout = TRUE,
    stderr = TRUE
  ))
  status <- attr(output, "status") %||% 0L
  fabric_test_skip_or_fail(
    identical(as.integer(status), 0L),
    paste(
      "Azure CLI could not acquire a Fabric integration token for",
      audience,
      paste(output, collapse = "\n")
    )
  )
  token <- trimws(paste(output, collapse = ""))
  fabric_test_skip_or_fail(
    nzchar(token),
    paste("Azure CLI returned an empty integration token for", audience)
  )
  token
}

fabric_test_token_provider <- function(
  acquire = fabric_test_azure_cli_token
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

fabric_test_require_package <- function(package) {
  fabric_test_skip_or_fail(
    !requireNamespace(package, quietly = TRUE),
    paste("Fabric integration package is not installed:", package)
  )
  invisible(TRUE)
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
