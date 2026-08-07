# Split a semicolon-delimited connection string without breaking quoted values
fabric_split_connection_string <- function(value) {
  characters <- strsplit(value, "", fixed = TRUE)[[1L]]
  tokens <- character()
  current <- character()
  quote <- NULL
  braced <- FALSE
  has_equals <- FALSE
  can_open_value <- TRUE
  index <- 1L

  while (index <= length(characters)) {
    char <- characters[[index]]
    if (isTRUE(braced)) {
      current <- c(current, char)
      if (identical(char, "}")) {
        if (
          index < length(characters) &&
            identical(characters[[index + 1L]], "}")
        ) {
          current <- c(current, characters[[index + 1L]])
          index <- index + 1L
        } else {
          braced <- FALSE
          can_open_value <- FALSE
        }
      }
    } else if (!is.null(quote)) {
      current <- c(current, char)
      if (identical(char, quote)) {
        if (
          index < length(characters) &&
            identical(characters[[index + 1L]], quote)
        ) {
          current <- c(current, characters[[index + 1L]])
          index <- index + 1L
        } else {
          quote <- NULL
          can_open_value <- FALSE
        }
      }
    } else if (identical(char, ";")) {
      tokens <- c(tokens, paste0(current, collapse = ""))
      current <- character()
      has_equals <- FALSE
      can_open_value <- TRUE
    } else {
      current <- c(current, char)
      if (identical(char, "=") && !isTRUE(has_equals)) {
        has_equals <- TRUE
        can_open_value <- TRUE
      } else if (isTRUE(can_open_value) && grepl("^\\s$", char)) {
        # Quoted or braced values may have whitespace after the equals sign.
      } else if (isTRUE(can_open_value) && char %in% c("\"", "'")) {
        quote <- char
      } else if (isTRUE(can_open_value) && identical(char, "{")) {
        braced <- TRUE
      } else {
        can_open_value <- FALSE
      }
    }
    index <- index + 1L
  }

  if (!is.null(quote) || isTRUE(braced)) {
    rlang::abort(
      "Connection string contains an unterminated quoted or braced value"
    )
  }
  c(tokens, paste0(current, collapse = ""))
}

fabric_unquote_connection_value <- function(value) {
  value <- trimws(value)
  size <- nchar(value)
  if (size >= 2L && startsWith(value, "{") && endsWith(value, "}")) {
    value <- substr(value, 2L, size - 1L)
    return(gsub("}}", "}", value, fixed = TRUE))
  }
  if (
    size >= 2L &&
      substr(value, 1L, 1L) %in% c("\"", "'") &&
      identical(substr(value, 1L, 1L), substr(value, size, size))
  ) {
    quote <- substr(value, 1L, 1L)
    value <- substr(value, 2L, size - 1L)
    return(gsub(paste0(quote, quote), quote, value, fixed = TRUE))
  }
  value
}

# Encode a connection-string value using ODBC brace quoting
fabric_quote_connection_value <- function(value) {
  if (
    !is.character(value) ||
      length(value) != 1L ||
      is.na(value)
  ) {
    rlang::abort("Connection string values must be single, non-missing strings")
  }
  paste0("{", gsub("}", "}}", value, fixed = TRUE), "}")
}
