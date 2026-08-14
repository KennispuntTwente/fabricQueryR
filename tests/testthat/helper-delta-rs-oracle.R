# Helpers for generating deterministic local Delta tables with the locked
# Python development environment. These fixtures test the production Arrow
# bridge against static expectations; deltalake is no longer an independent
# oracle because the package reader itself uses deltalake

fabric_test_delta_oracle_enabled <- function() {
  fabric_test_required() ||
    tolower(Sys.getenv("FABRIC_DELTA_RS_ORACLE_TESTS")) %in%
      c("1", "true", "yes")
}

fabric_test_delta_oracle_root <- function() {
  configured <- Sys.getenv("FABRIC_DELTA_RS_ORACLE_ROOT")
  if (nzchar(configured)) {
    return(normalizePath(configured, winslash = "/", mustWork = TRUE))
  }
  file.path(
    fabric_test_repository_root(),
    "tools",
    "fabric-sandbox"
  )
}

fabric_test_require_delta_oracle <- function() {
  if (!fabric_test_delta_oracle_enabled()) {
    testthat::skip(
      "delta-rs runtime tests are opt-in outside CI and Fabric integration"
    )
  }
  fabric_test_require_package("arrow")
  root <- fabric_test_delta_oracle_root()
  fabric_test_skip_or_fail(
    !file.exists(file.path(root, "pyproject.toml")),
    paste("Delta fixture project not found:", root)
  )
  uv <- Sys.which("uv")
  fabric_test_skip_or_fail(!nzchar(uv), "uv is required for Delta fixtures")
  invisible(list(command = unname(uv), root = root))
}

fabric_test_delta_oracle_run <- function(arguments) {
  fixture <- fabric_test_require_delta_oracle()
  output <- suppressWarnings(system2(
    fixture$command,
    c(
      "--directory",
      shQuote(fixture$root),
      "run",
      "--locked",
      "python",
      "-m",
      "fabricqueryr_sandbox.delta_oracle",
      arguments
    ),
    stdout = TRUE,
    stderr = TRUE
  ))
  status <- attr(output, "status") %||% 0L
  if (!identical(status, 0L)) {
    rlang::abort(c(
      "The deterministic Delta fixture command failed",
      "x" = paste(output, collapse = "\n")
    ))
  }
  invisible(output)
}
