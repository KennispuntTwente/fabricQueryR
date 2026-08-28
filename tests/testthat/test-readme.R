test_that("README R examples parse and match exported signatures", {
  path <- test_path("..", "..", "README.md")
  if (!file.exists(path)) {
    skip("Package README source is not available in installed test runs")
  }

  lines <- readLines(path, warn = FALSE)
  starts <- grep("^```[[:space:]]+[rR][[:space:]]*$", lines)
  chunks <- lapply(starts, function(start) {
    following <- which(seq_along(lines) > start & lines == "```")
    stopifnot(length(following) > 0L)
    paste(lines[seq.int(start + 1L, following[[1L]] - 1L)], collapse = "\n")
  })
  expressions <- lapply(chunks, function(chunk) {
    parse(text = chunk, keep.source = FALSE)
  })
  expect_length(expressions, length(starts))

  collect_calls <- function(value) {
    calls <- list()
    visit <- function(node) {
      if (is.call(node)) {
        if (is.symbol(node[[1L]])) {
          name <- as.character(node[[1L]])
          if (grepl("^(fabric|onelake)_", name)) {
            calls[[length(calls) + 1L]] <<- node
          }
        }
        lapply(as.list(node)[-1L], visit)
      } else if (is.expression(node) || is.list(node)) {
        lapply(node, visit)
      }
      invisible(NULL)
    }
    visit(value)
    calls
  }

  exports <- getNamespaceExports("fabricQueryR")
  calls <- unlist(lapply(expressions, collect_calls), recursive = FALSE)
  expect_gt(length(calls), 0L)
  for (call in calls) {
    name <- as.character(call[[1L]])
    expect_in(name, exports)
    if (!name %in% exports) {
      next
    }
    supplied <- names(as.list(call)[-1L])
    supplied <- supplied[nzchar(supplied)]
    parameters <- names(formals(getExportedValue("fabricQueryR", name)))
    if (!"..." %in% parameters) {
      expect_length(
        setdiff(supplied, parameters),
        0L
      )
    }
  }
})
