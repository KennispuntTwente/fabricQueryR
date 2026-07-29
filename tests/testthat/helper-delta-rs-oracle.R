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
      "delta-rs oracle tests are opt-in outside the Fabric integration job"
    )
  }
  fabric_test_require_package("arrow")
  root <- fabric_test_delta_oracle_root()
  fabric_test_skip_or_fail(
    !file.exists(file.path(root, "pyproject.toml")),
    paste("delta-rs oracle project not found:", root)
  )
  uv <- Sys.which("uv")
  fabric_test_skip_or_fail(
    !nzchar(uv),
    "uv is required for delta-rs oracle tests"
  )
  invisible(list(command = unname(uv), root = root))
}

fabric_test_delta_oracle_run <- function(arguments) {
  oracle <- fabric_test_require_delta_oracle()
  output <- system2(
    oracle$command,
    c(
      "--directory",
      shQuote(oracle$root),
      "run",
      "--locked",
      "python",
      "-m",
      "fabricqueryr_sandbox.delta_oracle",
      arguments
    ),
    stdout = TRUE,
    stderr = TRUE
  )
  status <- attr(output, "status") %||% 0L
  if (!identical(status, 0L)) {
    rlang::abort(c(
      "The delta-rs Python oracle failed",
      "x" = paste(output, collapse = "\n")
    ))
  }
  invisible(output)
}

fabric_test_delta_oracle_read <- function(
  uri,
  version = NULL,
  columns = NULL,
  limit = NULL
) {
  path <- tempfile("delta-rs-oracle-", fileext = ".arrow")
  on.exit(unlink(path, force = TRUE), add = TRUE)
  arguments <- c("read", "--uri", shQuote(uri), "--output", shQuote(path))
  if (!is.null(version)) {
    arguments <- c(arguments, "--version", as.character(version))
  }
  if (length(columns)) {
    arguments <- c(
      arguments,
      unlist(lapply(columns, function(column) c("--column", shQuote(column))))
    )
  }
  if (!is.null(limit)) {
    arguments <- c(arguments, "--limit", as.character(limit))
  }
  fabric_test_delta_oracle_run(arguments)
  arrow::read_ipc_file(path, as_data_frame = TRUE)
}

fabric_test_delta_oracle_uri <- function(manifest, lakehouse, table) {
  paste0(
    "abfss://",
    utils::URLencode(manifest$workspace_id, reserved = TRUE),
    "@onelake.dfs.fabric.microsoft.com/",
    utils::URLencode(lakehouse$id, reserved = TRUE),
    ".Lakehouse/Tables/",
    onelake_encode_path(c(lakehouse$schema, table))
  )
}

fabric_test_delta_canonical_scalar <- function(value) {
  if (is.raw(value)) {
    return(list(
      type = "binary",
      value = paste(format(value), collapse = "")
    ))
  }
  if (is.double(value) && length(value) == 1L && is.nan(value)) {
    return(list(type = "double", value = "NaN"))
  }
  if (
    length(value) == 0L ||
      is.null(value) ||
      (length(value) == 1L && is.na(value))
  ) {
    return(list(type = "null"))
  }
  if (inherits(value, "integer64")) {
    return(list(type = "integer64", value = as.character(value)))
  }
  if (inherits(value, "POSIXct")) {
    return(list(
      type = "timestamp",
      value = format(value, "%Y-%m-%dT%H:%M:%OS6Z", tz = "UTC")
    ))
  }
  if (inherits(value, "Date")) {
    return(list(type = "date", value = as.character(value)))
  }
  if (is.logical(value)) {
    return(list(type = "logical", value = if (value) "true" else "false"))
  }
  if (is.integer(value)) {
    return(list(type = "integer", value = as.character(value)))
  }
  if (is.double(value)) {
    formatted <- if (is.infinite(value)) {
      if (value > 0) "Infinity" else "-Infinity"
    } else {
      sprintf("%.17g", value)
    }
    return(list(type = "double", value = formatted))
  }
  if (is.character(value)) {
    return(list(type = "character", value = value))
  }
  list(type = typeof(value), value = as.character(value))
}

fabric_test_delta_canonical_value <- function(value) {
  if (is.data.frame(value)) {
    return(lapply(
      seq_len(nrow(value)),
      function(index) fabric_test_delta_canonical_row(value, index)
    ))
  }
  if (is.list(value) && !is.raw(value)) {
    return(lapply(value, fabric_test_delta_canonical_value))
  }
  if (length(value) != 1L && !is.raw(value)) {
    return(lapply(
      seq_along(value),
      function(index) fabric_test_delta_canonical_scalar(value[index])
    ))
  }
  fabric_test_delta_canonical_scalar(value)
}

fabric_test_delta_canonical_cell <- function(column, index) {
  if (is.data.frame(column)) {
    return(fabric_test_delta_canonical_row(column, index))
  }
  if (is.list(column) && !is.raw(column)) {
    return(fabric_test_delta_canonical_value(column[[index]]))
  }
  fabric_test_delta_canonical_scalar(column[index])
}

fabric_test_delta_canonical_row <- function(value, index) {
  stats::setNames(
    lapply(value, fabric_test_delta_canonical_cell, index = index),
    names(value)
  )
}

fabric_test_delta_canonical_rows <- function(value) {
  rows <- lapply(
    seq_len(nrow(value)),
    function(index) fabric_test_delta_canonical_row(value, index)
  )
  sort(vapply(
    rows,
    jsonlite::toJSON,
    character(1),
    auto_unbox = TRUE,
    null = "null"
  ))
}

fabric_test_expect_delta_oracle_equal <- function(actual, oracle, info = NULL) {
  expect_named(actual, names(oracle), info = info)
  expect_equal(
    fabric_test_delta_canonical_rows(actual),
    fabric_test_delta_canonical_rows(oracle),
    info = info
  )
  invisible(actual)
}
