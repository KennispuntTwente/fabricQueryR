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

  exports <- getNamespaceExports("fabricQueryR")
  registry <- documentation_r6_method_registry()
  calls <- unlist(
    lapply(expressions, documentation_calls, "^(fabric|onelake)_"),
    recursive = FALSE
  )
  expect_gt(length(calls), 0L)
  method_calls <- 0L
  for (record in calls) {
    call <- record$call
    name <- record$name
    if (identical(record$kind, "method")) {
      signatures <- registry[[name]]
      if (is.null(signatures)) {
        expect_true(
          name %in% documentation_external_methods,
          info = deparse1(call)
        )
        next
      }
      method_calls <- method_calls + 1L
      expect_true(
        documentation_r6_call_matches(call, signatures),
        info = deparse1(call)
      )
      next
    }
    expect_in(name, exports)
    if (!name %in% exports) {
      next
    }
    supplied <- documentation_call_arguments(call)
    parameters <- names(formals(getExportedValue("fabricQueryR", name)))
    if (!"..." %in% parameters) {
      expect_length(
        setdiff(supplied, parameters),
        0L
      )
    }
  }
  expect_gt(method_calls, 0L)
})
