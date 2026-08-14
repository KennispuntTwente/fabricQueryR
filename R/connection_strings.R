# Split `value` into fields while preserving quoted semicolons. Returns the
# fields used by the SQL and Power BI connection-string parsers
fabric_split_connection_string <- function(value) {
  # 1 Prepare parser state -------------------------------------------------------------------------

  # Read one character at a time because semicolons inside quotes are data,
  # while semicolons outside quotes separate fields

  characters <- strsplit(value, "", fixed = TRUE)[[1L]]
  tokens <- character()
  current <- character()
  quote <- NULL
  braced <- FALSE
  has_equals <- FALSE
  can_open_value <- TRUE
  index <- 1L

  # 2 Read fields ----------------------------------------------------------------------------------

  # Track quoted and braced values until their matching closing character

  while (index <= length(characters)) {
    char <- characters[[index]]

    # Braced ODBC values escape a closing brace by doubling it
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
      # Quoted values use the quote character itself as the escape
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
      # An unquoted semicolon completes the current field
      tokens <- c(tokens, paste0(current, collapse = ""))
      current <- character()
      has_equals <- FALSE
      can_open_value <- TRUE
    } else {
      # Outside a quoted value, look for the key/value boundary and opener
      current <- c(current, char)
      if (identical(char, "=") && !isTRUE(has_equals)) {
        has_equals <- TRUE
        can_open_value <- TRUE
      } else if (isTRUE(can_open_value) && grepl("^\\s$", char)) {
        # Quoted or braced values may have whitespace after the equals sign
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

  # 3 Return complete fields -----------------------------------------------------------------------

  # An unfinished quote would make later key/value parsing ambiguous

  if (!is.null(quote) || isTRUE(braced)) {
    .fabric_abort(
      "Connection string contains an unterminated quoted or braced value"
    )
  }

  c(tokens, paste0(current, collapse = ""))
}

# Remove ODBC brace or quote escaping from `value`. Returns plain text for the
# SQL and Power BI connection-string parsers
fabric_unquote_connection_value <- function(value) {
  value <- trimws(value)
  size <- nchar(value)

  # ODBC braces use doubled closing braces for literal text
  if (size >= 2L && startsWith(value, "{") && endsWith(value, "}")) {
    value <- substr(value, 2L, size - 1L)
    return(gsub("}}", "}", value, fixed = TRUE))
  }

  # Single and double quotes escape themselves by doubling
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

# Encode one string with ODBC brace quoting. Returns safe connection-string
# text used when fabricQueryR builds a connection string
fabric_quote_connection_value <- function(value) {
  if (
    !is.character(value) ||
      length(value) != 1L ||
      is.na(value)
  ) {
    .fabric_abort(
      "Connection string values must be single, non-missing strings"
    )
  }
  paste0("{", gsub("}", "}}", value, fixed = TRUE), "}")
}
