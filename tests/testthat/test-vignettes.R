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
  exports <- getNamespaceExports("fabricQueryR")
  registry <- documentation_r6_method_registry()
  method_calls <- 0L
  paths <- list.files(vignette_dir, pattern = "[.]Rmd$", full.names = TRUE)
  for (path in paths) {
    chunks <- vignette_r_chunks(path)
    expressions <- lapply(chunks, function(chunk) {
      parse(text = paste(chunk$body, collapse = "\n"))
    })
    calls <- documentation_calls(
      expressions,
      "^(fabric|kusto|onelake)_"
    )
    for (record in calls) {
      call <- record$call
      name <- record$name
      if (identical(record$kind, "method")) {
        signatures <- registry[[name]]
        if (is.null(signatures)) {
          expect_true(
            name %in% documentation_external_methods,
            info = paste(basename(path), deparse1(call))
          )
          next
        }
        method_calls <- method_calls + 1L
        expect_true(
          documentation_r6_call_matches(call, signatures),
          info = paste(basename(path), deparse1(call))
        )
        next
      }
      expect_true(name %in% exports, info = paste(basename(path), name))
      if (!name %in% exports) {
        next
      }
      arguments <- documentation_call_arguments(call)
      parameters <- names(formals(getExportedValue("fabricQueryR", name)))
      if (!"..." %in% parameters) {
        expect_true(
          !length(setdiff(arguments, parameters)),
          info = paste(basename(path), deparse1(call))
        )
      }
    }
  }
  expect_gt(method_calls, 0L)
})

test_that("R6 documentation signature checks reject unknown named arguments", {
  registry <- documentation_r6_method_registry()
  call <- parse(
    text = 'model$dax_query(dax = "EVALUATE ROW()", datset_id = "typo")'
  )[[1L]]

  expect_false(documentation_r6_call_matches(call, registry$dax_query))
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
  item <- vignette_mock_r6(
    fields = list(displayName = "Patients", type = "Lakehouse")
  )
  lakehouse <- vignette_mock_r6(
    fields = list(displayName = "Clinical", id = "lakehouse-1"),
    methods = list(
      tables = function() {
        tutorial$calls <- c(tutorial$calls, "tables")
        data.frame(
          schema = "dbo",
          name = "Patients",
          type = "Managed",
          stringsAsFactors = FALSE
        )
      },
      read_table = function(table, limit) {
        tutorial$calls <- c(tutorial$calls, "read")
        tutorial$read <- list(
          lakehouse = lakehouse,
          table = table,
          limit = limit
        )
        data.frame(id = 1:2, name = c("Ada", "Grace"))
      }
    ),
    class = "FabricLakehouse"
  )
  workspace <- vignette_mock_r6(
    fields = list(
      displayName = "Analytics workspace",
      id = "workspace-1"
    ),
    methods = list(
      items = function() {
        tutorial$calls <- c(tutorial$calls, "items")
        list(item)
      },
      lakehouses = function() {
        tutorial$calls <- c(tutorial$calls, "lakehouses")
        list(lakehouse)
      }
    ),
    class = "FabricWorkspace"
  )
  archive <- vignette_mock_r6(
    fields = list(displayName = "Archive", id = "workspace-2"),
    class = "FabricWorkspace"
  )
  tutorial$fabric_workspaces <- function() {
    tutorial$calls <- c(tutorial$calls, "workspaces")
    list(workspace, archive)
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
  item <- vignette_mock_r6(
    fields = list(
      displayName = "Orders",
      type = "Lakehouse",
      id = "item-id"
    ),
    class = "FabricLakehouse"
  )
  workspace <- vignette_mock_r6(
    fields = list(displayName = "Analytics", id = "workspace-id"),
    methods = list(
      items = function() {
        calls <<- c(calls, "items")
        list(item)
      }
    ),
    class = "FabricWorkspace"
  )

  example <- vignette_evaluate_chunks(
    path,
    c(4L, 5L),
    bindings = list(
      fabric_workspaces = function(...) {
        calls <<- c(calls, "workspaces")
        list(workspace)
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
  written <- NULL
  lakehouse <- vignette_mock_r6(
    fields = list(displayName = "Lakehouse", id = "lakehouse-id"),
    methods = list(
      write_table = function(table, data, mode, ...) {
        written <<- list(
          target = lakehouse,
          table = table,
          data = data,
          mode = mode
        )
        list(rows = nrow(data), staging_retained = FALSE)
      },
      read_table = function(table, limit, ...) {
        expect_identical(table, "orders_from_r")
        expect_identical(limit, 10L)
        written$data
      }
    ),
    class = "FabricLakehouse"
  )
  workspace <- vignette_mock_r6(
    fields = list(
      displayName = "Analytics workspace",
      id = "workspace-id"
    ),
    methods = list(lakehouses = function() list(lakehouse)),
    class = "FabricWorkspace"
  )

  example <- vignette_evaluate_chunks(
    path,
    2:4,
    bindings = list(
      fabric_workspaces = function(...) list(workspace)
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
  lakehouse <- vignette_mock_r6(
    fields = list(displayName = "Lakehouse", id = "lakehouse-id"),
    methods = list(
      onelake_list = function(path, ...) {
        expect_identical(path, "Files/incoming")
        files
      },
      tables = function(...) tables,
      read_table = function(table, columns, limit, ...) {
        expect_identical(table$name, "orders")
        expect_identical(columns, c("order_id", "amount"))
        expect_identical(limit, 100L)
        data.frame(order_id = 1L, amount = 10.5)
      }
    ),
    class = "FabricLakehouse"
  )
  workspace <- vignette_mock_r6(
    fields = list(
      displayName = "Analytics workspace",
      id = "workspace-id"
    ),
    methods = list(lakehouses = function() list(lakehouse)),
    class = "FabricWorkspace"
  )

  example <- vignette_evaluate_chunks(
    path,
    c(2L, 5L, 9L, 10L),
    bindings = list(
      fabric_workspaces = function(...) list(workspace)
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
  calls <- character()
  rows <- data.frame(order_id = 1L, amount = 10.5)
  lakehouse <- vignette_mock_r6(
    fields = list(type = "Lakehouse", id = "lakehouse-id"),
    methods = list(
      read_table = function(...) {
        calls <<- c(calls, "lakehouse")
        rows
      },
      onelake_read_file = function(...) {
        calls <<- c(calls, "onelake")
        rows
      },
      livy_query = function(...) {
        calls <<- c(calls, "livy")
        list(output = list(parsed = rows))
      }
    ),
    class = "FabricLakehouse"
  )
  warehouse <- vignette_mock_r6(
    fields = list(type = "Warehouse", id = "warehouse-id"),
    methods = list(
      sql_query = function(...) {
        calls <<- c(calls, "sql")
        rows
      },
      read_table = function(...) {
        calls <<- c(calls, "warehouse")
        rows
      }
    ),
    class = "FabricWarehouse"
  )
  kql_database <- vignette_mock_r6(
    fields = list(type = "KQLDatabase", id = "kql-id"),
    methods = list(
      read_table = function(...) {
        calls <<- c(calls, "kql-table")
        rows
      },
      query = function(...) {
        calls <<- c(calls, "kql-query")
        rows
      }
    ),
    class = "FabricKqlDatabase"
  )
  model <- vignette_mock_r6(
    fields = list(type = "SemanticModel", id = "model-id"),
    methods = list(
      dax_query = function(...) {
        calls <<- c(calls, "dax")
        rows
      }
    ),
    class = "FabricSemanticModel"
  )
  api <- vignette_mock_r6(
    fields = list(type = "GraphQLApi", id = "graphql-id"),
    methods = list(
      query = function(...) {
        calls <<- c(calls, "graphql")
        list(data = list(products = list(items = rows)))
      }
    ),
    class = "FabricGraphQLApi"
  )
  workspace <- vignette_mock_r6(
    fields = list(
      displayName = "Analytics workspace",
      id = "workspace-id"
    ),
    methods = list(
      lakehouses = function(...) list(lakehouse),
      warehouses = function(...) list(warehouse),
      kql_databases = function(...) list(kql_database),
      semantic_models = function(...) list(model),
      graphql_apis = function(...) list(api)
    ),
    class = "FabricWorkspace"
  )

  example <- vignette_evaluate_chunks(
    path,
    c(2L, 3L, 5:11),
    bindings = list(
      fabric_workspaces = function(...) list(workspace)
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
  writes <- list()
  lakehouse <- vignette_mock_r6(
    fields = list(displayName = "Lakehouse", id = "lakehouse-id"),
    class = "FabricLakehouse"
  )
  warehouse <- vignette_mock_r6(
    fields = list(displayName = "Warehouse", id = "warehouse-id"),
    methods = list(
      sql_query = function(...) data.frame(id = 1:2),
      write_table = function(...) {
        arguments <- list(...)
        writes[[length(writes) + 1L]] <<- arguments
        list(
          rows = nrow(arguments$data),
          file_count = 1L,
          staging_retained = FALSE
        )
      }
    ),
    class = "FabricWarehouse"
  )
  workspace <- vignette_mock_r6(
    fields = list(displayName = "Analytics", id = "workspace-id"),
    methods = list(
      warehouses = function(...) list(warehouse),
      lakehouses = function(...) list(lakehouse)
    ),
    class = "FabricWorkspace"
  )

  example <- vignette_evaluate_chunks(
    path,
    c(2L, 3L, 5:7),
    bindings = list(
      fabric_workspaces = function(...) list(workspace)
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
  calls <- character()
  status <- function(...) {
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
  }
  database <- vignette_mock_r6(
    fields = list(
      id = "database-id",
      ingestion_service_uri = "https://ingest"
    ),
    methods = list(
      write_table = function(..., data) {
        calls <<- c(calls, "write")
        list(
          status = list(state = "Succeeded"),
          rows = nrow(data),
          staging_retained = FALSE
        )
      },
      ingest = function(...) {
        calls <<- c(calls, "ingest")
        list(
          id = "ingestion-id",
          sources = data.frame(source_id = "source-id")
        )
      },
      ingestion_status = status,
      ingestion_wait = status,
      query = function(...) {
        calls <<- c(calls, "query")
        data.frame(id = 1L)
      },
      export = function(...) {
        calls <<- c(calls, "export")
        list(
          state = "Succeeded",
          records = 1L,
          artifacts = "events.parquet"
        )
      }
    ),
    class = "FabricKqlDatabase"
  )
  lakehouse <- vignette_mock_r6(
    fields = list(id = "lakehouse-id"),
    class = "FabricLakehouse"
  )

  example <- vignette_evaluate_chunks(
    path,
    c(2:9, 11L),
    bindings = list(
      fabric_kql_databases = function(...) list(database),
      fabric_lakehouses = function(...) list(lakehouse)
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
  pages <- structure(list(page = 1L), class = "mock_pages")
  products <- data.frame(id = 1L, name = "Widget")
  products$category <- I(list(list(id = 10L, name = "Tools")))
  products$tags <- I(list(c("new", "sale")))
  attr(products, "complete") <- TRUE
  attr(products, "page_count") <- 1L
  attr(products, "errors") <- list()
  api <- vignette_mock_r6(
    fields = list(id = "graphql-id"),
    methods = list(
      query = function(...) {
        list(
          data = list(products = list(items = products)),
          errors = list()
        )
      },
      schema = function(...) {
        list(
          queryType = list(name = "Query"),
          types = list(list(name = "Query"), list(name = "Product"))
        )
      },
      paginate = function(...) pages
    ),
    class = "FabricGraphQLApi"
  )

  example <- vignette_evaluate_chunks(
    path,
    2:7,
    bindings = list(
      fabric_graphql_apis = function(...) list(api),
      fabric_graphql_cursor = function(...) "cursor-resolver",
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
  job <- list(id = "job-id")
  schedule <- list(id = "schedule-id")
  updates <- logical()
  deleted <- FALSE
  notebook <- vignette_mock_r6(
    fields = list(id = "notebook-id"),
    methods = list(
      run = function(parameters, ...) {
        expect_false(parameters$full_load)
        job
      },
      wait = function(...) list(status = "Completed"),
      status = function(...) {
        list(exit_value = "ok", status = "Completed")
      },
      instances = function(...) {
        list(list(
          invoke_type = "Manual",
          status = "Completed",
          start_time = as.POSIXct("2026-08-26", tz = "UTC"),
          failure_reason = NULL
        ))
      },
      schedule_create = function(...) schedule,
      schedules = function(...) list(schedule),
      schedule_update = function(..., enabled) {
        updates <<- c(updates, enabled)
        list(enabled = enabled)
      },
      schedule_delete = function(...) {
        deleted <<- TRUE
        invisible(TRUE)
      }
    ),
    class = "FabricJobItem"
  )

  example <- vignette_evaluate_chunks(
    path,
    2:11,
    bindings = list(
      fabric_notebooks = function(...) list(notebook),
      fabric_job_schedule_config = function(frequency, ...) {
        list(frequency = frequency, ...)
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
  refresh <- list(id = "refresh-id")
  waits <- 0L
  model <- vignette_mock_r6(
    fields = list(id = "model-id"),
    methods = list(
      dax_query = function(...) data.frame(value = 1L),
      refresh = function(...) refresh,
      refresh_wait = function(...) {
        waits <<- waits + 1L
        list(
          state = "Completed",
          start_time = as.POSIXct("2026-08-26", tz = "UTC"),
          end_time = as.POSIXct("2026-08-26 00:01:00", tz = "UTC"),
          details_url = "https://app.powerbi.test/details"
        )
      },
      refresh_status = function(...) {
        list(
          state = "Completed",
          attempts = list(),
          messages = character(),
          service_error = NULL,
          objects = list(),
          details_url = "https://app.powerbi.test/details"
        )
      },
      refresh_history = function(...) {
        list(list(
          refresh_type = "ViaEnhancedApi",
          state = "Completed",
          attempts = list()
        ))
      }
    ),
    class = "FabricSemanticModel"
  )

  example <- vignette_evaluate_chunks(
    path,
    c(2:5, 7:10),
    bindings = list(
      fabric_semantic_models = function(...) list(model)
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
  source <- paste(readLines(path, warn = FALSE), collapse = "\n")
  expect_match(
    source,
    "documents\\nservice-principal \\(SPN\\) tokens for session jobs"
  )
  expect_match(source, "batch guide is internally inconsistent")
  expect_match(source, "role alone does not guarantee service-side acceptance")
  discovery_calls <- 0L
  lakehouse_workspace <- NULL
  query_calls <- list()
  batch_calls <- list()
  session_lakehouse <- NULL
  session_events <- character()
  session_runs <- list()
  closed <- FALSE
  session <- list(
    close = function() {
      closed <<- TRUE
      session_events <<- c(session_events, "close")
    },
    wait = function() {
      session_events <<- c(session_events, "wait")
      invisible(TRUE)
    },
    run = function(code, kind) {
      session_events <<- c(session_events, "run")
      session_runs[[length(session_runs) + 1L]] <<- list(
        code = code,
        kind = kind
      )
      parsed <- if (identical(code, "print(shared_value + 2)")) 42L else NULL
      list(output = list(parsed = parsed))
    }
  )
  lakehouse <- vignette_mock_r6(
    fields = list(id = "22222222-2222-4222-8222-222222222222"),
    methods = list(
      livy_query = function(code, kind, ...) {
        query_calls[[length(query_calls) + 1L]] <<- list(
          lakehouse = lakehouse,
          code = code,
          kind = kind,
          options = list(...)
        )
        list(output = list(parsed = data.frame(id = 1L)))
      },
      livy_session = function(...) {
        session_lakehouse <<- lakehouse
        session
      },
      livy_batch_submit = function(file, ...) {
        batch_calls[[length(batch_calls) + 1L]] <<- list(
          file = file,
          options = list(...)
        )
        list(result = function() list(state = "success"))
      }
    ),
    class = "FabricLakehouse"
  )
  workspace <- vignette_mock_r6(
    fields = list(
      displayName = "Analytics workspace",
      id = "11111111-1111-4111-8111-111111111111"
    ),
    methods = list(
      lakehouses = function(...) {
        lakehouse_workspace <<- workspace
        list(lakehouse)
      }
    ),
    class = "FabricWorkspace"
  )
  archive <- vignette_mock_r6(
    fields = list(displayName = "Archive", id = "archive-id"),
    class = "FabricWorkspace"
  )

  chunks <- vignette_r_chunks(path)
  bodies <- vapply(
    chunks,
    function(chunk) paste(chunk$body, collapse = "\n"),
    character(1)
  )
  index_for <- function(text) {
    matches <- grep(text, bodies, fixed = TRUE)
    expect_equal(length(matches), 1L, info = text)
    matches[[1L]]
  }
  indices <- vapply(
    c(
      "workspaces <- fabric_workspaces()",
      "SELECT * FROM external_sql_table",
      "SELECT 1 AS id, 'hello from Spark' AS message",
      "df <- sql('SELECT * FROM orders LIMIT 100')",
      "session <- lakehouse$livy_session()",
      "batch <- lakehouse$livy_batch_submit("
    ),
    index_for,
    integer(1)
  )

  example <- vignette_evaluate_chunks(
    path,
    indices,
    bindings = list(
      fabric_workspaces = function(...) {
        discovery_calls <<- discovery_calls + 1L
        list(archive, workspace)
      }
    )
  )

  expect_identical(example$answer$output$parsed, 42L)
  expect_identical(discovery_calls, 1L)
  expect_identical(lakehouse_workspace, workspace)
  expect_length(query_calls, 3L)
  expect_identical(query_calls[[1L]]$lakehouse, lakehouse)
  expect_identical(query_calls[[1L]]$kind, "sql")
  expect_identical(query_calls[[1L]]$code, "SELECT * FROM external_sql_table")
  expect_identical(
    query_calls[[1L]]$options$audience,
    paste0(
      "https://api.fabric.microsoft.com/",
      c(
        "Lakehouse.Execute.All",
        "Lakehouse.Read.All",
        "Code.AccessFabric.All",
        "Code.AccessStorage.All",
        "Code.AccessSQL.All"
      )
    )
  )
  expect_identical(query_calls[[2L]]$kind, "sql")
  expect_identical(
    query_calls[[2L]]$code,
    "SELECT 1 AS id, 'hello from Spark' AS message"
  )
  expect_identical(query_calls[[3L]]$kind, "sparkr")
  expect_match(query_calls[[3L]]$code, "SELECT [*] FROM orders LIMIT 100")
  expect_identical(session_lakehouse, lakehouse)
  expect_identical(session_events, c("wait", "run", "run", "close"))
  expect_identical(
    session_runs,
    list(
      list(code = "shared_value = 40", kind = "pyspark"),
      list(code = "print(shared_value + 2)", kind = "pyspark")
    )
  )
  expect_true(closed)
  expect_length(batch_calls, 1L)
  expect_identical(
    batch_calls[[1L]]$file,
    paste0(
      "abfss://11111111-1111-4111-8111-111111111111",
      "@onelake.dfs.fabric.microsoft.com/",
      "22222222-2222-4222-8222-222222222222",
      "/Files/jobs/daily_transform.py"
    )
  )
  expect_false(grepl(".Lakehouse/", batch_calls[[1L]]$file, fixed = TRUE))
  expect_identical(batch_calls[[1L]]$options$name, "daily-transform")
  expect_true(batch_calls[[1L]]$options$wait)
  expect_identical(batch_calls[[1L]]$options$timeout, 1800)
})

test_that("user-data-function vignette executes scalar and structured calls", {
  path <- test_path("..", "..", "vignettes", "user-data-functions.Rmd")
  if (!file.exists(path)) {
    skip("Package vignette source is not available in installed test runs")
  }
  source <- paste(readLines(path, warn = FALSE), collapse = "\n")
  expect_match(source, "default to\\n`detail = FALSE`", perl = TRUE)
  expect_match(source, "[$]details\\(detail = TRUE\\)")
  expect_match(source, "not service principals or managed identities")
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

test_that("Livy vignette explains the sparklyr migration boundary", {
  path <- test_path("..", "..", "vignettes", "spark-with-livy.Rmd")
  if (!file.exists(path)) {
    skip("Package vignette source is not available in installed test runs")
  }
  source <- paste(readLines(path, warn = FALSE), collapse = "\n")

  expect_match(source, "`sparklyr` is not another Livy `kind`", fixed = TRUE)
  expect_match(source, "method = 'synapse'", fixed = TRUE)
  expect_match(source, "SparkR\\s+JVM bridge", perl = TRUE)
  expect_match(source, "kind = \"sparkr\"", fixed = TRUE)
  expect_match(source, "experimental", fixed = TRUE)
  expect_match(
    source,
    "does not document it for item-scoped Livy",
    fixed = TRUE
  )
  expect_match(source, "does\nnot currently live-test it", perl = TRUE)
  expect_match(
    source,
    "do not treat this\nexample as a supported production contract"
  )
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

  expect_match(normalized, "Prefer discovered objects", fixed = TRUE)
  expect_match(normalized, "Fabric portal", fixed = TRUE)
  expect_match(normalized, "Azure API Management", fixed = TRUE)
  expect_match(normalized, "organization controls the host", fixed = TRUE)
  expect_match(normalized, "token issued for", fixed = TRUE)
  expect_match(normalized, "do not prove", fixed = TRUE)
  expect_match(normalized, "correct audience", fixed = TRUE)
})
