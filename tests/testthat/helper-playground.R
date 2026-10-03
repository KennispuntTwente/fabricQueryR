.playground_test_path <- function(...) {
  playground <- test_path("..", "..", "playground")
  skip_if_not(
    dir.exists(playground),
    "playground/ is intentionally excluded from the built package"
  )

  file.path(playground, ...)
}
