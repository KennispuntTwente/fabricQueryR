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
