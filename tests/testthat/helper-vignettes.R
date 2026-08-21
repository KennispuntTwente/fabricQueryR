vignette_r_chunks <- function(path) {
  lines <- readLines(path, warn = FALSE)
  starts <- grep("^```\\{r(?:[ ,].*)?\\}$", lines)
  lapply(starts, function(start) {
    following <- which(seq_along(lines) > start & lines == "```")
    stopifnot(length(following) > 0L)
    end <- following[[1L]]
    list(
      header = lines[[start]],
      body = lines[seq.int(start + 1L, end - 1L)]
    )
  })
}

vignette_safe_setup <- function(chunk) {
  code <- trimws(paste(chunk$body, collapse = "\n"))
  grepl("^knitr::opts_chunk\\$set\\(", code) &&
    !inherits(try(parse(text = code), silent = TRUE), "try-error")
}
