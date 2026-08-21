test_that("vignette examples are excluded from check-time execution", {
  vignette_dir <- test_path("..", "..", "vignettes")
  if (!dir.exists(vignette_dir)) {
    skip("Package vignettes are not available in installed test runs")
  }

  paths <- list.files(vignette_dir, pattern = "[.]Rmd$", full.names = TRUE)
  unsafe <- unlist(lapply(paths, function(path) {
    chunks <- vignette_r_chunks(path)
    vapply(
      chunks,
      function(chunk) {
        executable <- !grepl(
          "eval[[:space:]]*=[[:space:]]*FALSE",
          chunk$header
        )
        executable && !vignette_safe_setup(chunk)
      },
      logical(1)
    )
  }))

  expect_false(any(unsafe))
  expect_false(vignette_safe_setup(list(
    header = "```{r, include = FALSE}",
    body = "fabric_workspaces()"
  )))
})

test_that("documented vignette calls match exported function signatures", {
  vignette_dir <- test_path("..", "..", "vignettes")
  if (!dir.exists(vignette_dir)) {
    skip("Package vignettes are not available in installed test runs")
  }
  collect_calls <- function(value) {
    calls <- list()
    visit <- function(node) {
      if (is.call(node)) {
        if (is.symbol(node[[1L]])) {
          name <- as.character(node[[1L]])
          if (grepl("^(fabric|kusto|onelake)_", name)) {
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
  paths <- list.files(vignette_dir, pattern = "[.]Rmd$", full.names = TRUE)
  for (path in paths) {
    chunks <- vignette_r_chunks(path)
    expressions <- lapply(chunks, function(chunk) {
      parse(text = paste(chunk$body, collapse = "\n"))
    })
    calls <- collect_calls(expressions)
    for (call in calls) {
      name <- as.character(call[[1L]])
      expect_true(name %in% exports, info = paste(basename(path), name))
      if (!name %in% exports) {
        next
      }
      arguments <- names(as.list(call)[-1L])
      arguments <- arguments[nzchar(arguments)]
      parameters <- names(formals(getExportedValue("fabricQueryR", name)))
      if (!"..." %in% parameters) {
        expect_true(
          !length(setdiff(arguments, parameters)),
          info = paste(basename(path), deparse1(call))
        )
      }
    }
  }
})

test_that("every vignette knits and all example code parses", {
  skip_if_not_installed("knitr")
  vignette_dir <- test_path("..", "..", "vignettes")
  if (!dir.exists(vignette_dir)) {
    skip("Package vignettes are not available in installed test runs")
  }

  paths <- list.files(vignette_dir, pattern = "[.]Rmd$", full.names = TRUE)
  for (path in paths) {
    knitted <- tempfile(fileext = ".md")
    tangled <- tempfile(fileext = ".R")
    withr::defer(unlink(c(knitted, tangled)), envir = test_env())

    expect_no_error(knitr::knit(path, output = knitted, quiet = TRUE))
    expect_no_error(
      knitr::purl(
        path,
        output = tangled,
        documentation = 0L,
        quiet = TRUE
      )
    )
    expect_no_error(parse(tangled))
  }
})

test_that("vignettes do not index unnamed discovery results by display name", {
  vignette_dir <- test_path("..", "..", "vignettes")
  if (!dir.exists(vignette_dir)) {
    skip("Package vignettes are not available in installed test runs")
  }
  paths <- list.files(vignette_dir, pattern = "[.]Rmd$", full.names = TRUE)
  text <- unlist(lapply(paths, readLines, warn = FALSE), use.names = FALSE)

  expect_false(any(grepl(
    "fabric_workspaces\\([^)]*\\)\\[\\[\"",
    text,
    perl = TRUE
  )))
})

test_that("semantic-model refresh separates Lakehouse schema and table", {
  path <- test_path("..", "..", "vignettes", "semantic-model-refresh.Rmd")
  if (!file.exists(path)) {
    skip("Package vignette source is not available in installed test runs")
  }
  source <- paste(readLines(path, warn = FALSE), collapse = "\n")

  expect_match(source, 'table = "Sales"', fixed = TRUE)
  expect_match(source, 'schema = "dbo"', fixed = TRUE)
  expect_equal(grepl('"dbo.Sales"', source, fixed = TRUE), FALSE)
})

test_that("Livy vignette documents current access prerequisites", {
  path <- test_path("..", "..", "vignettes", "spark-with-livy.Rmd")
  if (!file.exists(path)) {
    skip("Package vignette source is not available in installed test runs")
  }
  source <- paste(readLines(path, warn = FALSE), collapse = "\n")

  expect_match(source, "tenant admin setting", fixed = TRUE)
  expect_match(source, "Contributor", fixed = TRUE)
  expect_match(source, "Lakehouse.Execute.All", fixed = TRUE)
  expect_match(source, "Lakehouse.Read.All", fixed = TRUE)
  expect_match(source, "Code.AccessFabric.All", fixed = TRUE)
  expect_match(source, "Code.AccessStorage.All", fixed = TRUE)
})
