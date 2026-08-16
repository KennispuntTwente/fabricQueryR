test_that("vignette examples are excluded from check-time execution", {
  vignette_dir <- test_path("..", "..", "vignettes")
  if (!dir.exists(vignette_dir)) {
    skip("Package vignettes are not available in installed test runs")
  }

  paths <- list.files(vignette_dir, pattern = "[.]Rmd$", full.names = TRUE)
  unsafe <- unlist(
    lapply(paths, function(path) {
      headers <- grep(
        "^```\\{r(?:[ ,].*)?\\}$",
        readLines(path, warn = FALSE),
        value = TRUE
      )
      headers[
        !grepl("eval[[:space:]]*=[[:space:]]*FALSE", headers) &
          !grepl("include[[:space:]]*=[[:space:]]*FALSE", headers)
      ]
    }),
    use.names = FALSE
  )

  expect_length(unsafe, 0L)
})
