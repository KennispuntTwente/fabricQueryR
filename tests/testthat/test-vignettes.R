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

test_that("authentication vignette executes discovery with one identity", {
  path <- test_path("..", "..", "vignettes", "authentication.Rmd")
  if (!file.exists(path)) {
    skip("Package vignette source is not available in installed test runs")
  }
  calls <- character()
  workspace <- list(displayName = "Analytics", id = "workspace-id")
  item <- list(displayName = "Orders", type = "Lakehouse", id = "item-id")

  example <- vignette_evaluate_chunks(
    path,
    c(4L, 5L),
    bindings = list(
      fabric_workspaces = function(...) {
        calls <<- c(calls, "workspaces")
        list(workspace)
      },
      fabric_items = function(selected) {
        calls <<- c(calls, "items")
        expect_identical(selected$id, workspace$id)
        list(item)
      }
    )
  )

  expect_identical(calls, c("workspaces", "items"))
  expect_identical(example$items[[1L]]$id, item$id)
})

test_that("ingestion vignette carries rows through Lakehouse writes", {
  path <- test_path("..", "..", "vignettes", "ingesting-data.Rmd")
  if (!file.exists(path)) {
    skip("Package vignette source is not available in installed test runs")
  }
  workspace <- list(displayName = "Analytics workspace", id = "workspace-id")
  lakehouse <- list(displayName = "Lakehouse", id = "lakehouse-id")
  written <- NULL

  example <- vignette_evaluate_chunks(
    path,
    2:4,
    bindings = list(
      fabric_workspaces = function(...) list(workspace),
      fabric_lakehouses = function(selected, ...) {
        expect_identical(selected$id, workspace$id)
        list(lakehouse)
      },
      fabric_lakehouse_write_table = function(
        target,
        table,
        data,
        mode,
        ...
      ) {
        written <<- list(
          target = target,
          table = table,
          data = data,
          mode = mode
        )
        list(rows = nrow(data), staging_retained = FALSE)
      },
      fabric_lakehouse_read_table = function(target, table, limit, ...) {
        expect_identical(target$id, lakehouse$id)
        expect_identical(table, "orders_from_r")
        expect_identical(limit, 10L)
        written$data
      }
    )
  )

  expect_identical(written$table, "orders_from_r")
  expect_identical(written$mode, "Overwrite")
  expect_identical(example$check$order_id, 1:3)
})

test_that("OneLake vignette executes listing and table-read data flow", {
  path <- test_path("..", "..", "vignettes", "onelake-and-lakehouse.Rmd")
  if (!file.exists(path)) {
    skip("Package vignette source is not available in installed test runs")
  }
  workspace <- list(displayName = "Analytics workspace", id = "workspace-id")
  lakehouse <- list(displayName = "Lakehouse", id = "lakehouse-id")
  files <- data.frame(
    path = "Files/incoming/orders.csv",
    is_directory = FALSE,
    content_length = 42
  )
  tables <- data.frame(
    schema = "dbo",
    name = "orders",
    type = "Managed",
    format = "delta"
  )

  example <- vignette_evaluate_chunks(
    path,
    c(2L, 5L, 9L, 10L),
    bindings = list(
      fabric_workspaces = function(...) list(workspace),
      fabric_lakehouses = function(selected, ...) list(lakehouse),
      fabric_onelake_list = function(selected, item, path, ...) {
        expect_identical(selected$id, workspace$id)
        expect_identical(item$id, lakehouse$id)
        expect_identical(path, "Files/incoming")
        files
      },
      fabric_lakehouse_tables = function(item, ...) tables,
      fabric_lakehouse_read_table = function(
        item,
        table,
        columns,
        limit,
        ...
      ) {
        expect_identical(table$name, "orders")
        expect_identical(columns, c("order_id", "amount"))
        expect_identical(limit, 100L)
        data.frame(order_id = 1L, amount = 10.5)
      }
    )
  )

  expect_identical(example$files$path, files$path)
  expect_identical(example$rows$order_id, 1L)
})

test_that("reading vignette executes every non-connection backend", {
  path <- test_path("..", "..", "vignettes", "reading-data.Rmd")
  if (!file.exists(path)) {
    skip("Package vignette source is not available in installed test runs")
  }
  workspace <- list(displayName = "Analytics workspace", id = "workspace-id")
  item <- function(type) list(type = type, id = paste0(type, "-id"))
  calls <- character()
  rows <- data.frame(order_id = 1L, amount = 10.5)

  example <- vignette_evaluate_chunks(
    path,
    c(2L, 3L, 5:11),
    bindings = list(
      fabric_workspaces = function(...) list(workspace),
      fabric_lakehouses = function(...) list(item("lakehouse")),
      fabric_warehouses = function(...) list(item("warehouse")),
      fabric_kql_databases = function(...) list(item("kql")),
      fabric_semantic_models = function(...) list(item("model")),
      fabric_sql_query = function(...) {
        calls <<- c(calls, "sql")
        rows
      },
      fabric_lakehouse_read_table = function(...) {
        calls <<- c(calls, "lakehouse")
        rows
      },
      fabric_warehouse_read_table = function(...) {
        calls <<- c(calls, "warehouse")
        rows
      },
      fabric_kql_read_table = function(...) {
        calls <<- c(calls, "kql-table")
        rows
      },
      fabric_kql_query = function(...) {
        calls <<- c(calls, "kql-query")
        rows
      },
      fabric_pbi_dax_query = function(...) {
        calls <<- c(calls, "dax")
        rows
      },
      fabric_onelake_read_file = function(...) {
        calls <<- c(calls, "onelake")
        rows
      },
      fabric_graphql_apis = function(...) list(item("graphql")),
      fabric_graphql_query = function(...) {
        calls <<- c(calls, "graphql")
        list(data = list(products = list(items = rows)))
      },
      fabric_livy_query = function(...) {
        calls <<- c(calls, "livy")
        list(output = list(parsed = rows))
      }
    )
  )

  expect_identical(
    calls,
    c(
      "sql",
      "lakehouse",
      "warehouse",
      "kql-table",
      "kql-query",
      "dax",
      "onelake",
      "graphql",
      "livy"
    )
  )
  expect_identical(example$counts$order_id, 1L)
})

test_that("Warehouse vignette executes discovery, query, and staged writes", {
  path <- test_path("..", "..", "vignettes", "warehouse.Rmd")
  if (!file.exists(path)) {
    skip("Package vignette source is not available in installed test runs")
  }
  workspace <- list(displayName = "Analytics", id = "workspace-id")
  warehouse <- list(displayName = "Warehouse", id = "warehouse-id")
  lakehouse <- list(displayName = "Lakehouse", id = "lakehouse-id")
  writes <- list()

  example <- vignette_evaluate_chunks(
    path,
    c(2L, 3L, 5:7),
    bindings = list(
      fabric_workspaces = function(...) list(workspace),
      fabric_warehouses = function(...) list(warehouse),
      fabric_lakehouses = function(...) list(lakehouse),
      fabric_sql_query = function(...) data.frame(id = 1:2),
      fabric_warehouse_write_table = function(...) {
        arguments <- list(...)
        writes[[length(writes) + 1L]] <<- arguments
        list(
          rows = nrow(arguments$data),
          file_count = 1L,
          staging_retained = FALSE
        )
      }
    )
  )

  expect_length(writes, 2L)
  expect_identical(writes[[1L]]$schema, "dbo")
  expect_true(writes[[2L]]$create_if_missing)
  expect_identical(example$created$rows, 2L)
})

test_that("Eventhouse vignette executes write, ingestion, and export flow", {
  path <- test_path("..", "..", "vignettes", "eventhouse-ingestion.Rmd")
  if (!file.exists(path)) {
    skip("Package vignette source is not available in installed test runs")
  }
  database <- list(id = "database-id", ingestion_service_uri = "https://ingest")
  lakehouse <- list(id = "lakehouse-id")
  calls <- character()

  example <- vignette_evaluate_chunks(
    path,
    c(2:9, 11L),
    bindings = list(
      fabric_kql_databases = function(...) list(database),
      fabric_kql_write_table = function(..., data) {
        calls <<- c(calls, "write")
        list(
          status = list(state = "Succeeded"),
          rows = nrow(data),
          staging_retained = FALSE
        )
      },
      fabric_kql_ingest = function(...) {
        calls <<- c(calls, "ingest")
        list(
          id = "ingestion-id",
          sources = data.frame(source_id = "source-id")
        )
      },
      fabric_kql_ingestion_status = function(...) {
        calls <<- c(calls, "status")
        list(
          state = "Succeeded",
          counts = list(succeeded = 1L),
          details = data.frame(
            source_id = "source-id",
            status = "Succeeded",
            error_code = NA_character_,
            failure_status = NA_character_,
            message = NA_character_
          )
        )
      },
      fabric_kql_query = function(...) {
        calls <<- c(calls, "query")
        data.frame(id = 1L)
      },
      fabric_lakehouses = function(...) list(lakehouse),
      fabric_kql_export = function(...) {
        calls <<- c(calls, "export")
        list(
          state = "Succeeded",
          records = 1L,
          artifacts = "events.parquet"
        )
      }
    )
  )

  expect_identical(
    calls,
    c("write", "ingest", "status", "status", "status", "query", "export")
  )
  expect_identical(example$exported$artifacts, "events.parquet")
})

test_that("GraphQL vignette executes query, schema, and pagination flow", {
  path <- test_path("..", "..", "vignettes", "graphql-schema-and-rows.Rmd")
  if (!file.exists(path)) {
    skip("Package vignette source is not available in installed test runs")
  }
  api <- list(id = "graphql-id")
  pages <- structure(list(page = 1L), class = "mock_pages")
  products <- data.frame(id = 1L, name = "Widget")
  products$category <- I(list(list(id = 10L, name = "Tools")))
  products$tags <- I(list(c("new", "sale")))
  attr(products, "complete") <- TRUE
  attr(products, "page_count") <- 1L
  attr(products, "errors") <- list()

  example <- vignette_evaluate_chunks(
    path,
    2:7,
    bindings = list(
      fabric_graphql_apis = function(...) list(api),
      fabric_graphql_query = function(...) {
        list(
          data = list(products = list(items = products)),
          errors = list()
        )
      },
      fabric_graphql_schema = function(...) {
        list(
          queryType = list(name = "Query"),
          types = list(list(name = "Query"), list(name = "Product"))
        )
      },
      fabric_graphql_cursor = function(...) "cursor-resolver",
      fabric_graphql_paginate = function(...) pages,
      fabric_graphql_collect = function(received, ...) {
        expect_identical(received, pages)
        products
      }
    )
  )

  expect_identical(example$schema$queryType$name, "Query")
  expect_true(attr(example$products, "complete"))
  expect_identical(example$products$category[[1L]]$name, "Tools")
})

test_that("job vignette executes run, history, and schedule lifecycle", {
  path <- test_path("..", "..", "vignettes", "job-automation.Rmd")
  if (!file.exists(path)) {
    skip("Package vignette source is not available in installed test runs")
  }
  notebook <- list(id = "notebook-id")
  job <- list(id = "job-id")
  schedule <- list(id = "schedule-id")
  updates <- logical()
  deleted <- FALSE

  example <- vignette_evaluate_chunks(
    path,
    2:11,
    bindings = list(
      fabric_notebooks = function(...) list(notebook),
      fabric_job_run = function(item, parameters, ...) {
        expect_identical(item$id, notebook$id)
        expect_false(parameters$full_load)
        job
      },
      fabric_job_wait = function(...) list(status = "Completed"),
      fabric_job_status = function(...) {
        list(exit_value = "ok", status = "Completed")
      },
      fabric_job_instances = function(...) {
        list(list(
          invoke_type = "Manual",
          status = "Completed",
          start_time = as.POSIXct("2026-08-26", tz = "UTC"),
          failure_reason = NULL
        ))
      },
      fabric_job_schedule_config = function(frequency, ...) {
        list(frequency = frequency, ...)
      },
      fabric_job_schedule_create = function(...) schedule,
      fabric_job_schedules = function(...) list(schedule),
      fabric_job_schedule_update = function(..., enabled) {
        updates <<- c(updates, enabled)
        list(enabled = enabled)
      },
      fabric_job_schedule_delete = function(...) {
        deleted <<- TRUE
        invisible(TRUE)
      }
    )
  )

  expect_identical(example$result$status, "Completed")
  expect_identical(example$daily$frequency, "Daily")
  expect_identical(example$weekly$frequency, "Weekly")
  expect_identical(updates, c(FALSE, TRUE))
  expect_true(deleted)
})

test_that("semantic-model vignette executes query and refresh lifecycle", {
  path <- test_path("..", "..", "vignettes", "semantic-model-refresh.Rmd")
  if (!file.exists(path)) {
    skip("Package vignette source is not available in installed test runs")
  }
  model <- list(id = "model-id")
  refresh <- list(id = "refresh-id")
  waits <- 0L

  example <- vignette_evaluate_chunks(
    path,
    c(2:5, 7:10),
    bindings = list(
      fabric_semantic_models = function(...) list(model),
      fabric_pbi_dax_query = function(...) data.frame(value = 1L),
      fabric_pbi_refresh = function(...) refresh,
      fabric_pbi_refresh_wait = function(...) {
        waits <<- waits + 1L
        list(
          state = "Completed",
          start_time = as.POSIXct("2026-08-26", tz = "UTC"),
          end_time = as.POSIXct("2026-08-26 00:01:00", tz = "UTC"),
          details_url = "https://app.powerbi.test/details"
        )
      },
      fabric_pbi_refresh_status = function(...) {
        list(
          state = "Completed",
          attempts = list(),
          messages = character(),
          service_error = NULL,
          objects = list(),
          details_url = "https://app.powerbi.test/details"
        )
      },
      fabric_pbi_refresh_history = function(...) {
        list(list(
          refresh_type = "ViaEnhancedApi",
          state = "Completed",
          attempts = list()
        ))
      }
    )
  )

  expect_identical(example$completed$state, "Completed")
  expect_identical(example$status$state, "Completed")
  expect_identical(example$latest$state, "Completed")
  expect_identical(waits, 3L)
})

test_that("Livy vignette executes query and shared-session examples", {
  path <- test_path("..", "..", "vignettes", "spark-with-livy.Rmd")
  if (!file.exists(path)) {
    skip("Package vignette source is not available in installed test runs")
  }
  workspace <- list(displayName = "Analytics workspace", id = "workspace-id")
  lakehouse <- list(id = "lakehouse-id")
  closed <- FALSE
  runs <- 0L
  session <- list(
    close = function() {
      closed <<- TRUE
    },
    wait = function() invisible(TRUE),
    run = function(code, kind) {
      runs <<- runs + 1L
      list(output = list(parsed = if (runs == 2L) 42L else NULL))
    }
  )

  example <- vignette_evaluate_chunks(
    path,
    c(3L, 2L, 4:6),
    bindings = list(
      fabric_workspaces = function(...) list(workspace),
      fabric_lakehouses = function(...) list(lakehouse),
      fabric_livy_query = function(...) {
        list(output = list(parsed = data.frame(id = 1L)))
      },
      fabric_livy_session = function(...) session
    )
  )

  expect_identical(example$answer$output$parsed, 42L)
  expect_true(closed)
})

test_that("user-data-function vignette executes scalar and structured calls", {
  path <- test_path("..", "..", "vignettes", "user-data-functions.Rmd")
  if (!file.exists(path)) {
    skip("Package vignette source is not available in installed test runs")
  }
  calls <- list()

  example <- vignette_evaluate_chunks(
    path,
    2:7,
    bindings = list(
      Sys.getenv = function(...) "https://function.test/invoke",
      fabric_function_invoke = function(url, parameters, ...) {
        calls[[length(calls) + 1L]] <<- list(
          url = url,
          parameters = parameters,
          options = list(...)
        )
        list(
          function_name = "createOrder",
          invocation_id = "invocation-id",
          status = "Succeeded",
          output = parameters,
          errors = list(),
          http_status = 200L
        )
      }
    )
  )

  expect_length(calls, 4L)
  expect_identical(calls[[1L]]$url, "https://function.test/invoke")
  expect_identical(example$structured_result$status, "Succeeded")
  expect_true(calls[[4L]]$options$idempotent)
})

test_that("every feature vignette has a semantic execution test", {
  vignette_dir <- test_path("..", "..", "vignettes")
  if (!dir.exists(vignette_dir)) {
    skip("Package vignettes are not available in installed test runs")
  }
  paths <- basename(list.files(
    vignette_dir,
    pattern = "[.]Rmd$",
    full.names = TRUE
  ))
  covered <- c(
    "authentication.Rmd",
    "eventhouse-ingestion.Rmd",
    "getting-started.Rmd",
    "graphql-schema-and-rows.Rmd",
    "ingesting-data.Rmd",
    "job-automation.Rmd",
    "onelake-and-lakehouse.Rmd",
    "reading-data.Rmd",
    "semantic-model-refresh.Rmd",
    "spark-with-livy.Rmd",
    "user-data-functions.Rmd",
    "warehouse.Rmd"
  )

  expect_setequal(covered, paths)
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
