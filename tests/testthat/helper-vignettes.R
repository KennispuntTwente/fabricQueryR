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

vignette_evaluate_chunks <- function(
  path,
  indices,
  bindings = list(),
  values = list()
) {
  chunks <- vignette_r_chunks(path)
  stopifnot(
    is.numeric(indices),
    length(indices) > 0L,
    all(indices == as.integer(indices)),
    all(indices >= 1L),
    all(indices <= length(chunks)),
    is.list(bindings),
    is.list(values),
    length(bindings) == 0L || !is.null(names(bindings)),
    length(values) == 0L || !is.null(names(values))
  )
  environment <- new.env(parent = baseenv())
  environment$library <- function(...) invisible(TRUE)
  environment$head <- utils::head
  list2env(values, envir = environment)
  list2env(bindings, envir = environment)

  for (index in indices) {
    code <- paste(chunks[[index]]$body, collapse = "\n")
    eval(parse(text = code), envir = environment)
  }
  environment
}
