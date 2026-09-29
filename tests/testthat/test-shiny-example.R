test_that("the example clears loaded results on logout and does not replay reads", {
  skip_if_no_shiny_targets()
  example <- system.file(
    "examples",
    "shiny-fabric",
    "app.R",
    package = "fabricQueryR"
  )
  env <- new.env(parent = globalenv())
  state <- new.env(parent = emptyenv())
  state$generation <- shiny::reactiveVal("first-user")
  state$queries <- 0L
  env$fabric_shiny_config <- function(...) list()
  env$fabric_shiny_ui <- function(ui, ...) ui
  env$fabric_shiny_server <- function(...) {
    list(
      generation = function() state$generation(),
      ready = function(...) !is.null(state$generation()),
      identity = function() {
        list(id_token_claims = list(name = state$generation()))
      },
      token_provider = function() function(...) "synthetic-token",
      logout = function() state$generation(NULL),
      login = function() state$generation("next-user"),
      reauthorize = function() state$generation("replacement-user"),
      prepare = function(...) TRUE
    )
  }
  env$fabric_sql_query <- function(sql, params, ...) {
    state$queries <- state$queries + 1L
    expect_match(sql, "TABLE_NAME LIKE ?", fixed = TRUE)
    expect_identical(params, list("%orders%"))
    data.frame(TABLE_SCHEMA = "dbo", TABLE_NAME = state$generation())
  }
  app <- source(example, local = env)$value
  shiny::testServer(app$serverFuncSource(), {
    session$flushReact()
    session$setInputs(table_name = "orders", load = 1L)
    expect_identical(rows()$generation, "first-user")
    expect_match(output$tables, "first-user", fixed = TRUE)
    session$setInputs(logout = 1L)
    expect_null(rows())
    expect_s3_class(
      rlang::catch_cnd(output$tables, classes = "error"),
      "shiny.silent.error"
    )
    session$setInputs(login = 1L)
    expect_null(rows())
    expect_identical(state$queries, 1L)
    session$setInputs(load = 2L)
    expect_identical(rows()$generation, "next-user")
    expect_match(output$tables, "next-user", fixed = TRUE)
    expect_identical(state$queries, 2L)
  })
})
