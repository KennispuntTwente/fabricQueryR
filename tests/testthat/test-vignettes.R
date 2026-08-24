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

test_that("the getting-started workflow executes against its documented API", {
  path <- test_path("..", "..", "vignettes", "getting-started.Rmd")
  if (!file.exists(path)) {
    skip("Package vignette source is not available in installed test runs")
  }

  labels <- paste0(
    "tutorial-test-",
    c(
      "list-workspaces",
      "select-first",
      "select-by-name",
      "list-items",
      "read-lakehouse"
    )
  )
  chunks <- vignette_r_chunks(path)
  chunk_labels <- sub(
    "^```\\{r[ ,]+([^, }]+).*$",
    "\\1",
    vapply(chunks, `[[`, character(1), "header")
  )
  expect_identical(chunk_labels[chunk_labels %in% labels], labels)
  if (!all(labels %in% chunk_labels)) {
    return(invisible(NULL))
  }
  selected <- chunks[match(labels, chunk_labels)]
  expect_length(selected, 5L)

  tutorial <- new.env(parent = baseenv())
  tutorial$calls <- character()
  tutorial$head <- utils::head
  tutorial$fabric_workspaces <- function() {
    tutorial$calls <- c(tutorial$calls, "workspaces")
    list(
      list(displayName = "Analytics workspace", id = "workspace-1"),
      list(displayName = "Archive", id = "workspace-2")
    )
  }
  tutorial$fabric_items <- function(workspace) {
    tutorial$calls <- c(tutorial$calls, "items")
    expect_identical(workspace$id, "workspace-1")
    list(list(displayName = "Patients", type = "Lakehouse"))
  }
  tutorial$fabric_lakehouses <- function(workspace) {
    tutorial$calls <- c(tutorial$calls, "lakehouses")
    expect_identical(workspace$id, "workspace-1")
    list(list(displayName = "Clinical", id = "lakehouse-1"))
  }
  tutorial$fabric_lakehouse_tables <- function(lakehouse) {
    tutorial$calls <- c(tutorial$calls, "tables")
    expect_identical(lakehouse$id, "lakehouse-1")
    data.frame(
      schema = "dbo",
      name = "Patients",
      type = "Managed",
      stringsAsFactors = FALSE
    )
  }
  tutorial$fabric_lakehouse_read_table <- function(
    lakehouse,
    table,
    limit
  ) {
    tutorial$calls <- c(tutorial$calls, "read")
    tutorial$read <- list(
      lakehouse = lakehouse,
      table = table,
      limit = limit
    )
    data.frame(id = 1:2, name = c("Ada", "Grace"))
  }

  values <- lapply(selected, function(chunk) {
    eval(
      parse(text = paste(chunk$body, collapse = "\n")),
      envir = tutorial
    )
  })

  expect_length(values, 5L)
  expect_identical(
    tutorial$calls,
    c("workspaces", "workspaces", "items", "lakehouses", "tables", "read")
  )
  expect_identical(tutorial$workspace$id, "workspace-1")
  expect_identical(tutorial$lakehouse$id, "lakehouse-1")
  expect_identical(tutorial$read$table$name, "Patients")
  expect_identical(tutorial$read$limit, 100L)
  expect_identical(tutorial$rows$name, c("Ada", "Grace"))
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
  expect_match(source, "Code.AccessAzureKeyvault.All", fixed = TRUE)
  expect_match(source, "Code.AccessAzureDataLake.All", fixed = TRUE)
  expect_match(source, "Code.AccessAzureDataExplorer.All", fixed = TRUE)
  expect_match(source, "Code.AccessSQL.All", fixed = TRUE)
  expect_match(source, "replaces the defaults", fixed = TRUE)
})

test_that("GraphQL documentation states the attached-object limit", {
  path <- test_path(
    "..",
    "..",
    "vignettes",
    "graphql-schema-and-rows.Rmd"
  )
  if (!file.exists(path)) {
    skip("Package vignette source is not available in installed test runs")
  }
  source <- paste(readLines(path, warn = FALSE), collapse = "\n")
  normalized <- gsub("[[:space:]]+", " ", source)

  expect_match(normalized, "at most 1,000 source", fixed = TRUE)
  expect_match(normalized, "not a limit of 1,000 data sources", fixed = TRUE)
  expect_match(normalized, "multiple API items", fixed = TRUE)
  expect_match(normalized, "stored procedures", fixed = TRUE)
})

test_that("user-data-function docs distinguish delegated execution scopes", {
  path <- test_path(
    "..",
    "..",
    "vignettes",
    "user-data-functions.Rmd"
  )
  if (!file.exists(path)) {
    skip("Package vignette source is not available in installed test runs")
  }
  source <- paste(readLines(path, warn = FALSE), collapse = "\n")
  normalized <- gsub("[[:space:]]+", " ", source)

  expect_match(normalized, "least-privilege", fixed = TRUE)
  expect_match(normalized, "UserDataFunction.Execute.All", fixed = TRUE)
  expect_match(normalized, "Item.Execute.All", fixed = TRUE)
  expect_match(normalized, "select it explicitly", fixed = TRUE)
  expect_match(normalized, "item Execute permission", fixed = TRUE)
})

test_that("authentication docs define the custom-endpoint trust boundary", {
  path <- test_path("..", "..", "vignettes", "authentication.Rmd")
  if (!file.exists(path)) {
    skip("Package vignette source is not available in installed test runs")
  }
  source <- paste(readLines(path, warn = FALSE), collapse = "\n")
  normalized <- gsub("[[:space:]]+", " ", source)

  expect_match(normalized, "Prefer discovered records", fixed = TRUE)
  expect_match(normalized, "Fabric portal", fixed = TRUE)
  expect_match(normalized, "Azure API Management", fixed = TRUE)
  expect_match(normalized, "organization controls the host", fixed = TRUE)
  expect_match(normalized, "token issued for", fixed = TRUE)
  expect_match(normalized, "do not prove", fixed = TRUE)
  expect_match(normalized, "correct audience", fixed = TRUE)
})
