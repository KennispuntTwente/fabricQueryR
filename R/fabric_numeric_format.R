# Render one finite base numeric scalar without depending on R's display
# options or fixed-decimal formatting. Callers dispatch typed numeric classes
# and missing/non-finite values before requesting a numeric literal.
fabric_format_number <- function(value) {
  if (
    !(is.integer(value) || is.double(value)) ||
      is.object(value) ||
      length(value) != 1L ||
      !is.finite(value)
  ) {
    .fabric_abort(
      "value must be one finite base numeric value",
      class = "fabric_numeric_format_error"
    )
  }
  value <- unname(value)
  if (value == 0 && identical(1 / value, -Inf)) {
    return("-0.0")
  }
  as.character(jsonlite::toJSON(value, auto_unbox = TRUE, digits = 22))
}

# Expand a correctly rounded numeric literal without floating-point arithmetic.
# Kusto timespan literals require fixed notation for their numeric component.
fabric_format_number_fixed <- function(value) {
  text <- fabric_format_number(value)
  pieces <- strsplit(text, "[eE]")[[1L]]
  if (length(pieces) == 1L) {
    return(text)
  }
  exponent <- as.integer(pieces[[2L]])
  negative <- startsWith(pieces[[1L]], "-")
  mantissa <- sub("^-", "", pieces[[1L]])
  dot <- regexpr(".", mantissa, fixed = TRUE)[[1L]]
  position <- if (dot == -1L) nchar(mantissa) else dot - 1L
  position <- position + exponent
  digits <- gsub(".", "", mantissa, fixed = TRUE)

  expanded <- if (position <= 0L) {
    paste0("0.", strrep("0", -position), digits)
  } else if (position >= nchar(digits)) {
    paste0(digits, strrep("0", position - nchar(digits)))
  } else {
    paste0(substr(digits, 1L, position), ".", substring(digits, position + 1L))
  }
  paste0(if (negative) "-" else "", expanded)
}

# Format a POSIX date-time at Kusto's 100-nanosecond resolution. Fractional
# ticks are rounded separately from whole epoch seconds so large epoch values
# do not lose their fractional component during scaling.
fabric_format_kusto_datetime <- function(value) {
  seconds <- as.numeric(value)
  if (length(seconds) != 1L || !is.finite(seconds)) {
    .fabric_abort(
      "value must be one finite POSIX date-time",
      class = "fabric_numeric_format_error"
    )
  }

  whole <- floor(seconds)
  ticks <- floor((seconds - whole) * 1e7 + 0.5)
  if (ticks == 1e7) {
    whole <- whole + 1
    ticks <- 0
  }

  paste0(
    format(
      as.POSIXct(whole, origin = "1970-01-01", tz = "UTC"),
      "%Y-%m-%dT%H:%M:%S",
      tz = "UTC"
    ),
    ".",
    sprintf("%07d", as.integer(ticks)),
    "Z"
  )
}
