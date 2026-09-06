# Quote JSON number tokens without changing quoted strings. Returns valid JSON
# whose numeric values decode as their exact source text
fabric_json_quote_numbers <- function(value) {
  string <- '"(?:\\\\.|[^"\\\\])*"(*SKIP)(*F)'
  number <- paste0(
    "(-?(?:0|[1-9][0-9]*)(?:\\.[0-9]+)?",
    "(?:[eE][+-]?[0-9]+)?)"
  )
  gsub(paste(string, number, sep = "|"), '"\\1"', value, perl = TRUE)
}

# Restore decimal and exponent JSON leaves from a parallel lexical tree.
# Returns the original topology with only numeric source tokens replaced
fabric_json_restore_decimal_tokens <- function(value, lexical) {
  if (is.list(value) && is.list(lexical)) {
    for (index in seq_len(min(length(value), length(lexical)))) {
      value[index] <- list(fabric_json_restore_decimal_tokens(
        value[[index]],
        lexical[[index]]
      ))
    }
    return(value)
  }
  if (
    (is.integer(value) || is.double(value)) &&
      length(value) == 1L &&
      is.character(lexical) &&
      length(lexical) == 1L &&
      grepl("[.eE]", lexical)
  ) {
    return(lexical)
  }
  value
}

# Restore integer JSON leaves outside the consecutive binary64 integer range.
# Returns exact character tokens without parsing the comparison through double
fabric_json_restore_unsafe_integer_tokens <- function(value, lexical) {
  if (is.list(value) && is.list(lexical)) {
    for (index in seq_len(min(length(value), length(lexical)))) {
      value[index] <- list(fabric_json_restore_unsafe_integer_tokens(
        value[[index]],
        lexical[[index]]
      ))
    }
    return(value)
  }
  if (fabric_json_is_unsafe_integer_token(lexical)) {
    return(lexical)
  }
  value
}

# Test one lexical JSON integer against 2^53 - 1 using decimal digits only.
# Returns FALSE for non-integer tokens and values in the safe consecutive range
fabric_json_is_unsafe_integer_token <- function(value) {
  if (
    !is.character(value) ||
      length(value) != 1L ||
      !grepl("^-?(?:0|[1-9][0-9]*)$", value)
  ) {
    return(FALSE)
  }
  magnitude <- sub("^-", "", value)
  limit <- "9007199254740991"
  if (nchar(magnitude, type = "bytes") != nchar(limit, type = "bytes")) {
    return(nchar(magnitude, type = "bytes") > nchar(limit, type = "bytes"))
  }
  digits <- utf8ToInt(magnitude)
  limit_digits <- utf8ToInt(limit)
  difference <- which(digits != limit_digits)
  length(difference) > 0L &&
    digits[[difference[[1L]]]] > limit_digits[[difference[[1L]]]]
}
