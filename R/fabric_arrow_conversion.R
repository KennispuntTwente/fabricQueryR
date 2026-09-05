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
  values <- nanoarrow::convert_array_stream(
    stream,
    to = .fabric_arrow_exact_ptype(schema)
  )
  tibble::as_tibble(.fabric_arrow_exact_restore(values, schema))
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
