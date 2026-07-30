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
  output <- suppressWarnings(
    system2(
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
  metadata_path <- tempfile("delta-rs-oracle-", fileext = ".json")
  on.exit(unlink(c(path, metadata_path), force = TRUE), add = TRUE)
  arguments <- c(
    "read",
    "--uri",
    shQuote(uri),
    "--output",
    shQuote(path),
    "--metadata-output",
    shQuote(metadata_path)
  )
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
  old_options <- options(arrow.int64_downcast = FALSE)
  on.exit(options(old_options), add = TRUE)
  table <- arrow::read_ipc_file(path, as_data_frame = FALSE)
  value <- as.data.frame(table)
  for (name in names(value)) {
    column <- table$GetColumnByName(name)
    array <- do.call(arrow::concat_arrays, column$chunks)
    value[[name]] <- fabric_test_delta_attach_arrow_validity(
      value[[name]],
      array
    )
  }
  metadata <- jsonlite::fromJSON(
    metadata_path,
    simplifyVector = FALSE
  )
  oracle_fields <- metadata$delta_schema$fields %||% list()
  oracle_field_names <- vapply(oracle_fields, `[[`, character(1), "name")
  for (name in names(value)) {
    field <- oracle_fields[[match(name, oracle_field_names)]]
    value[[name]] <- fabric_delta_restore_timestamp_ntz(
      value[[name]],
      field$type
    )
  }
  attr(value, "fabric_delta_oracle_metadata") <- metadata
  value
}

#' Attach Arrow struct validity before conversion can erase it
#' @keywords internal
#' @noRd
fabric_test_delta_attach_arrow_validity <- function(value, array) {
  if (inherits(array, "StructArray")) {
    if (!is.data.frame(value)) {
      rlang::abort("delta-rs returned an invalid struct representation")
    }
    for (index in seq_along(value)) {
      value[[index]] <- fabric_test_delta_attach_arrow_validity(
        value[[index]],
        array$field(index - 1L)
      )
    }
    validity <- !vapply(
      seq_len(length(array)) - 1L,
      array$IsNull,
      logical(1)
    )
    class(value) <- unique(c("fabric_delta_struct_column", class(value)))
    attr(value, "fabric_delta_struct_validity") <- validity
    return(value)
  }
  if (inherits(array, c("ListArray", "LargeListArray"))) {
    value <- as.list(value)
    child <- array$values()
    for (index in seq_along(value)) {
      if (array$IsNull(index - 1L) || is.null(value[[index]])) {
        next
      }
      offset <- array$value_offset(index - 1L)
      length <- array$value_length(index - 1L)
      value[index] <- list(fabric_test_delta_attach_arrow_validity(
        value[[index]],
        child$Slice(offset, length)
      ))
    }
    return(value)
  }
  if (inherits(array, "MapArray")) {
    value <- as.list(value)
    items <- array$items()
    for (index in seq_along(value)) {
      if (array$IsNull(index - 1L) || is.null(value[[index]])) {
        next
      }
      offset <- array$value_offset(index - 1L)
      length <- array$value_length(index - 1L)
      value[[index]]$value <- fabric_test_delta_attach_arrow_validity(
        value[[index]]$value,
        items$Slice(offset, length)
      )
    }
    return(value)
  }
  value
}

fabric_test_delta_oracle_uri <- function(
  manifest,
  item,
  table,
  item_type = item$type %||% "Lakehouse",
  schema = item$schema %||% "dbo"
) {
  guid <- function(value) {
    grepl(
      paste0(
        "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-",
        "[0-9a-f]{4}-[0-9a-f]{12}$"
      ),
      value,
      ignore.case = TRUE
    )
  }
  item_reference <- if (guid(manifest$workspace_id) && guid(item$id)) {
    item$id
  } else {
    paste0(item$id, ".", item_type)
  }
  paste0(
    "abfss://",
    utils::URLencode(manifest$workspace_id, reserved = TRUE),
    "@onelake.dfs.fabric.microsoft.com/",
    utils::URLencode(item_reference, reserved = TRUE),
    "/Tables/",
    onelake_encode_path(c(schema, table))
  )
}

fabric_test_delta_canonical_scalar <- function(value) {
  if (inherits(value, "fabric_delta_timestamp_ntz")) {
    return(list(type = "timestamp_ntz", value = unclass(value)))
  }
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
    rows <- lapply(
      seq_len(nrow(value)),
      function(index) {
        if (
          inherits(value, "fabric_delta_struct_column") &&
            is.na(value)[[index]]
        ) {
          list(type = "struct_null")
        } else {
          fabric_test_delta_canonical_row(value, index)
        }
      }
    )
    # Arrow represents a Delta map as a data frame with `key` and `value`
    # fields. Map entry order is not part of Delta's logical value, so compare
    # those entries as a multiset while preserving order for structs.
    if (identical(names(value), c("key", "value"))) {
      order_keys <- vapply(
        rows,
        jsonlite::toJSON,
        character(1),
        auto_unbox = TRUE,
        null = "null"
      )
      rows <- rows[order(order_keys)]
    }
    return(rows)
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
    if (
      inherits(column, "fabric_delta_struct_column") &&
        is.na(column)[[index]]
    ) {
      return(list(type = "struct_null"))
    }
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

fabric_test_delta_column_signature <- function(value) {
  if (inherits(value, "fabric_delta_timestamp_ntz")) {
    return("timestamp_ntz")
  }
  if (inherits(value, "integer64")) {
    return("integer64")
  }
  if (inherits(value, "POSIXct")) {
    return("timestamp")
  }
  if (inherits(value, "Date")) {
    return("date")
  }
  if (is.data.frame(value)) {
    fields <- vapply(
      value,
      fabric_test_delta_column_signature,
      character(1)
    )
    return(paste0(
      "struct<",
      paste(paste(names(fields), fields, sep = ":"), collapse = ","),
      ">"
    ))
  }
  if (is.list(value)) {
    return("list")
  }
  typeof(value)
}

fabric_test_delta_column_signatures <- function(value) {
  vapply(value, fabric_test_delta_column_signature, character(1))
}

#' Canonicalize a Delta schema for exact parity comparison
#' @keywords internal
#' @noRd
fabric_test_delta_schema_canonical <- function(value) {
  if (!is.list(value)) {
    return(value)
  }
  result <- lapply(value, fabric_test_delta_schema_canonical)
  if (!is.null(names(result))) {
    result <- result[order(names(result))]
  }
  result
}

fabric_test_delta_oracle_metadata <- function(value) {
  metadata <- attr(value, "fabric_delta_oracle_metadata", exact = TRUE)
  expect_true(is.list(metadata))
  metadata
}

fabric_test_expect_delta_oracle_profile <- function(
  oracle,
  version = NULL,
  reader_features = character(),
  partition_columns = NULL,
  column_mapping_mode = NULL,
  min_active_files = NULL,
  info = NULL
) {
  metadata <- fabric_test_delta_oracle_metadata(oracle)
  expect_true(
    as.numeric(metadata$version) >= 0,
    info = info
  )
  expect_true(
    as.numeric(metadata$min_reader_version) %in% 1:3,
    info = info
  )
  if (!is.null(version)) {
    expect_equal(as.numeric(metadata$version), version, info = info)
  }
  expect_equal(
    as.numeric(metadata$row_count),
    nrow(oracle),
    info = info
  )
  expect_identical(
    unlist(metadata$column_names, use.names = FALSE),
    names(oracle),
    info = info
  )
  actual_features <- unlist(
    metadata$reader_features %||% list(),
    use.names = FALSE
  )
  expect_true(
    all(reader_features %in% actual_features),
    info = info
  )
  if (!is.null(partition_columns)) {
    expect_equal(
      sort(unique(unlist(
        metadata$partition_columns,
        use.names = FALSE
      ))),
      sort(unique(partition_columns)),
      info = info
    )
  }
  if (!is.null(column_mapping_mode)) {
    configuration <- metadata$configuration %||% list()
    expect_identical(
      tolower(configuration[["delta.columnMapping.mode"]] %||% "none"),
      column_mapping_mode,
      info = info
    )
  }
  if (!is.null(min_active_files)) {
    expect_true(
      as.numeric(metadata$active_file_count) >= min_active_files,
      info = info
    )
  }
  invisible(metadata)
}

fabric_test_expect_delta_oracle_equal <- function(actual, oracle, info = NULL) {
  expect_true(is.data.frame(actual), info = info)
  expect_true(is.data.frame(oracle), info = info)
  expect_false(anyDuplicated(names(actual)) > 0L, info = info)
  expect_false(anyDuplicated(names(oracle)) > 0L, info = info)
  expect_named(actual, names(oracle), info = info)
  expect_equal(nrow(actual), nrow(oracle), info = info)
  actual_schema <- attr(actual, "fabric_delta_schema", exact = TRUE)
  oracle_metadata <- fabric_test_delta_oracle_metadata(oracle)
  oracle_schema <- oracle_metadata$delta_schema %||% NULL
  expect_true(is.list(actual_schema), info = info)
  expect_true(is.list(oracle_schema), info = info)
  oracle_fields <- oracle_schema$fields %||% list()
  oracle_field_names <- vapply(oracle_fields, `[[`, character(1), "name")
  oracle_schema$fields <- oracle_fields[match(
    names(oracle),
    oracle_field_names
  )]
  expect_identical(
    fabric_test_delta_schema_canonical(actual_schema[c("type", "fields")]),
    fabric_test_delta_schema_canonical(oracle_schema[c("type", "fields")]),
    info = info
  )
  expect_identical(
    fabric_test_delta_column_signatures(actual),
    fabric_test_delta_column_signatures(oracle),
    info = info
  )
  expect_equal(
    fabric_test_delta_canonical_rows(actual),
    fabric_test_delta_canonical_rows(oracle),
    info = info
  )
  invisible(actual)
}
