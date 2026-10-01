.playground_test_path <- function(...) {
  playground <- test_path("..", "..", "playground")
  skip_if_not(
    dir.exists(playground),
    "playground/ is intentionally excluded from the built package"
  )

  file.path(playground, ...)
}

playground_shiny_test_environment <- function() {
  skip_if_not_installed("shiny", "1.8.1")
  skip_if_not_installed("bslib", "0.7.0")
  skip_if_not_installed("promises", "1.3.0")
  env <- new.env(parent = globalenv())
  sys.source(.playground_test_path("shiny", "queries.R"), envir = env)
  sys.source(.playground_test_path("shiny", "application.R"), envir = env)
  env
}
