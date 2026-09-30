test_that("the example runs queries in a worker while its Shiny session remains responsive", {
  skip_if_no_shiny_targets()
  skip_if_not_installed("shiny", "1.8.1")
  skip_if_not_installed("bslib", "0.7.0")
  skip_if_not_installed("future")
  skip_if_not_installed("promises")
  old_plan <- future::plan()
  withr::defer(future::plan(old_plan))
  withr::local_envvar(LC_ALL = "C", LANG = "C", LANGUAGE = "en")
  workers <- parallelly::makeClusterPSOCK(
    1,
    rscript_startup = quote(suppressWarnings(library(future)))
  )
  withr::defer(parallel::stopCluster(workers))
  future::plan(future::cluster, workers = workers)
  started_file <- withr::local_tempfile()
  release_file <- withr::local_tempfile()
  withr::defer(writeLines("released", release_file))
  example <- system.file(
    "examples",
    "shiny-fabric",
    "app.R",
    package = "fabricQueryR"
  )
  env <- new.env(parent = globalenv())
  state <- new.env(parent = emptyenv())
  state$generation <- shiny::reactiveVal("first-user")
  state$ping <- 0L
  env$Sys.getenv <- function(name, unset = "") {
    switch(
      name,
      FABRIC_SQL_SERVER = "warehouse.datawarehouse.fabric.microsoft.com",
      FABRIC_SQL_DATABASE = "orders",
      unset
    )
  }
  env$fabric_shiny_config <- function(...) list()
  env$fabric_shiny_ui <- function(ui, ...) ui
  env$fabric_shiny_server <- function(...) {
    list(
      generation = function() state$generation(),
      ready = function(...) !is.null(state$generation()),
      access_token = function(...) {
        promises::promise_resolve(state$generation())
      },
      logout = function() state$generation(NULL),
      login = function() state$generation("next-user")
    )
  }
  env$fabric_sql_query <- function(server, sql, database, token, params, ...) {
    stopifnot(
      server == "warehouse.datawarehouse.fabric.microsoft.com",
      database == "orders",
      grepl("TABLE_NAME LIKE ?", sql, fixed = TRUE)
    )
    writeLines("started", started_file)
    deadline <- Sys.time() + 20
    while (!file.exists(release_file) && Sys.time() < deadline) {
      Sys.sleep(0.01)
    }
    if (!file.exists(release_file)) {
      stop("Worker was not released by the test")
    }
    data.frame(TABLE_NAME = token, filter = params[[1L]], pid = Sys.getpid())
  }
  environment(env$fabric_sql_query) <- list2env(
    list(started_file = started_file, release_file = release_file),
    parent = baseenv()
  )
  app <- with_mocked_bindings(
    source(example, local = env)$value,
    plan = function(...) invisible(NULL),
    .package = "future"
  )
  shiny::testServer(app$serverFuncSource(), {
    session$flushReact()
    session$setInputs(table_name = "orders", load = 1L)
    shiny_test_wait(function() file.exists(started_file))
    expect_identical(query$status(), "running")
    shiny::observeEvent(input$ping, state$ping <- input$ping)
    session$setInputs(ping = 1L)
    expect_identical(state$ping, 1L)
    expect_identical(query$status(), "running")
    session$setInputs(logout = 1L)
    expect_identical(fabric$ready("sql"), FALSE)
    session$setInputs(login = 1L)
    writeLines("released", release_file)
    shiny_test_wait(function() identical(query$status(), "success"))
    session$flushReact()
    result <- query$result()
    expect_identical(result$generation, "first-user")
    expect_identical(result$data$TABLE_NAME, "first-user")
    expect_identical(result$data$filter, "%orders%")
    expect_equal(result$data$pid == Sys.getpid(), FALSE)
    expect_s3_class(
      rlang::catch_cnd(output$tables, classes = "error"),
      "shiny.silent.error"
    )
    session$setInputs(load = 2L)
    shiny_test_wait(function() identical(query$status(), "success"))
    session$flushReact()
    expect_identical(query$result()$generation, "next-user")
    expect_match(output$tables, "next-user", fixed = TRUE)
    session$setInputs(logout = 2L)
    expect_s3_class(
      rlang::catch_cnd(output$tables, classes = "error"),
      "shiny.silent.error"
    )
  })
})
