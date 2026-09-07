# R's decimal parser uses extended precision when available. Use the JSON
# decoder for wire numbers so tests also work where long double is binary64.
numeric_test_decode <- function(text) {
  vapply(
    text,
    function(value) {
      if (is.na(value)) NA_real_ else as.double(jsonlite::fromJSON(value))
    },
    numeric(1),
    USE.NAMES = FALSE
  )
}
