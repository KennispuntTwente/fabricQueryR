# Select lossless R representations before nanoarrow converts any buffers.
.fabric_arrow_exact_ptype <- function(schema) {
  if (!is.null(schema$dictionary)) {
    return(.fabric_arrow_exact_ptype(schema$dictionary))
  }
  format <- schema$format
  if (grepl("^d:", format) || format %in% c("l", "L")) {
    return(character())
  }
  if (format %in% c("i", "I")) {
    return(double())
  }
  if (identical(format, "+s")) {
    columns <- lapply(schema$children, .fabric_arrow_exact_ptype)
    return(structure(columns, class = "data.frame", row.names = integer()))
  }
  prototype <- nanoarrow::infer_nanoarrow_ptype(schema)
  if (inherits(prototype, "vctrs_list_of") && length(schema$children) == 1L) {
    attr(prototype, "ptype") <- .fabric_arrow_exact_ptype(schema$children[[1L]])
  }
  prototype
}

.fabric_arrow_exact_tibble <- function(stream) {
  schema <- stream$get_schema()
  has_uint64 <- .fabric_arrow_has_uint64(schema)
  conversion_schema <- if (has_uint64) {
    # Signed and unsigned 64-bit integers share their physical buffer layout.
    # Reinterpret those buffers so nanoarrow's exact signed-to-text converter
    # can retain validity, slicing, and dictionary/list structure for us.
    .fabric_arrow_uint64_signed_schema(schema)
  } else {
    schema
  }
  # Struct offsets and parent validity apply to all child types, including
  # ordinary signed integers and decimals in streams with no unsigned columns.
  batches <- nanoarrow::collect_array_stream(stream, schema = conversion_schema)
  batches <- lapply(batches, .fabric_arrow_normalize_struct_slices)
  stream <- nanoarrow::basic_array_stream(batches, schema = conversion_schema)
  on.exit(nanoarrow::nanoarrow_pointer_release(stream), add = TRUE)
  values <- nanoarrow::convert_array_stream(
    stream,
    to = .fabric_arrow_exact_ptype(schema)
  )
  if (has_uint64) {
    values <- .fabric_arrow_uint64_restore(values, schema)
  }
  tibble::as_tibble(.fabric_arrow_exact_restore(values, schema))
}

.fabric_arrow_has_uint64 <- function(schema) {
  identical(schema$format, "L") ||
    (!is.null(schema$dictionary) &&
      .fabric_arrow_has_uint64(schema$dictionary)) ||
    any(vapply(schema$children, .fabric_arrow_has_uint64, logical(1)))
}

.fabric_arrow_uint64_signed_schema <- function(schema) {
  updates <- list()
  if (identical(schema$format, "L")) {
    updates$format <- "l"
  }
  if (!is.null(schema$dictionary)) {
    updates$dictionary <- .fabric_arrow_uint64_signed_schema(schema$dictionary)
  }
  if (length(schema$children)) {
    updates$children <- lapply(
      schema$children,
      .fabric_arrow_uint64_signed_schema
    )
  }
  nanoarrow::nanoarrow_schema_modify(schema, updates)
}

# nanoarrow's nested data-frame conversion does not apply a struct's offset to
# its children. Normalize those slices before converting any nested buffers.
.fabric_arrow_normalize_struct_slices <- function(array) {
  schema <- nanoarrow::infer_nanoarrow_schema(array)
  children <- array$children
  updates <- list()
  if (identical(schema$format, "+s")) {
    validity <- nanoarrow::convert_buffer(array$buffers[[1L]], logical())
    if (length(validity)) {
      validity <- validity[array$offset + seq_len(array$length)]
    }
    children <- lapply(children, function(child) {
      child_updates <- list(
        offset = child$offset + array$offset,
        length = array$length,
        null_count = if (child$null_count == 0L) 0L else -1L
      )
      if (any(!validity)) {
        child_validity <- nanoarrow::convert_buffer(
          child$buffers[[1L]],
          logical()
        )
        child_validity <- if (length(child_validity)) {
          child_validity[child_updates$offset + seq_len(array$length)] &
            validity
        } else {
          validity
        }
        child_updates$buffers <- child$buffers
        child_updates$buffers[[1L]] <- nanoarrow::as_nanoarrow_array(c(
          rep(TRUE, child_updates$offset),
          child_validity
        ))$buffers[[2L]]
        child_updates$null_count <- sum(!child_validity)
      }
      nanoarrow::nanoarrow_array_modify(
        child,
        child_updates
      )
    })
    if (array$offset != 0L) {
      if (length(validity)) {
        updates$buffers <- list(
          nanoarrow::as_nanoarrow_array(validity)$buffers[[2L]]
        )
        updates$null_count <- sum(!validity)
      }
      updates$offset <- 0L
    }
  }
  if (length(children)) {
    updates$children <- lapply(children, .fabric_arrow_normalize_struct_slices)
  }
  if (!is.null(array$dictionary)) {
    updates$dictionary <- .fabric_arrow_normalize_struct_slices(
      array$dictionary
    )
  }
  nanoarrow::nanoarrow_array_modify(array, updates)
}

# Restore unsigned leaves without changing the existing signed/decimal types
# selected for nested values. Subtract in base 1e9 so all arithmetic is exact.
.fabric_arrow_uint64_restore <- function(value, schema) {
  if (is.null(value)) {
    return(NULL)
  }
  if (!is.null(schema$dictionary)) {
    return(.fabric_arrow_uint64_restore(value, schema$dictionary))
  }
  if (identical(schema$format, "L")) {
    negative <- !is.na(value) & startsWith(value, "-")
    magnitude <- substring(value[negative], 2L)
    magnitude <- paste0(strrep("0", 27L - nchar(magnitude)), magnitude)
    low <- 709551616 - as.numeric(substring(magnitude, 19L, 27L))
    middle <- 446744073 -
      as.numeric(substring(magnitude, 10L, 18L)) -
      (low < 0)
    high <- 18 - as.numeric(substring(magnitude, 1L, 9L)) - (middle < 0)
    value[negative] <- sprintf(
      "%.0f%09.0f%09.0f",
      high,
      middle + (middle < 0) * 1e9,
      low + (low < 0) * 1e9
    )
  } else if (identical(schema$format, "+s")) {
    for (index in seq_along(value)) {
      value[[index]] <- .fabric_arrow_uint64_restore(
        value[[index]],
        schema$children[[index]]
      )
    }
  } else if (is.list(value) && length(schema$children) == 1L) {
    for (index in seq_along(value)) {
      value[index] <- list(.fabric_arrow_uint64_restore(
        value[[index]],
        schema$children[[1L]]
      ))
    }
  }
  value
}

# Use familiar scalar R types where their reserved NA encodings cannot collide.
# Nested lists retain the lossless prototypes selected above.
.fabric_arrow_exact_restore <- function(value, schema) {
  if (!is.null(schema$dictionary)) {
    return(.fabric_arrow_exact_restore(value, schema$dictionary))
  }
  if (identical(schema$format, "+s")) {
    for (index in seq_along(value)) {
      value[[index]] <- .fabric_arrow_exact_restore(
        value[[index]],
        schema$children[[index]]
      )
    }
  } else if (identical(schema$format, "i")) {
    if (!any(value == -2147483648, na.rm = TRUE)) {
      value <- as.integer(value)
    }
  } else if (identical(schema$format, "l")) {
    if (!any(value == "-9223372036854775808", na.rm = TRUE)) {
      value <- bit64::as.integer64(value)
    }
  }
  value
}
