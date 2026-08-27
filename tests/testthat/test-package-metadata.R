test_that("package title represents read and write capabilities", {
  description_path <- test_path("..", "..", "DESCRIPTION")
  if (!file.exists(description_path)) {
    skip("Package source DESCRIPTION is not available")
  }

  title <- read.dcf(description_path, fields = "Title")[[1L]]

  expect_match(title, "Microsoft Fabric", fixed = TRUE)
  expect_match(title, "Access|Manage")
})

test_that("declared dependency floors cover APIs used by the package", {
  description_path <- test_path("..", "..", "DESCRIPTION")
  if (!file.exists(description_path)) {
    skip("Package source DESCRIPTION is not available")
  }

  description <- read.dcf(description_path)

  expect_match(description[[1L, "Imports"]], "httr2 \\(>= 1\\.2\\.0\\)")
  expect_match(description[[1L, "Imports"]], "cli \\(>= 3\\.4\\.0\\)")
  expect_match(description[[1L, "Imports"]], "rlang \\(>= 0\\.4\\.10\\)")
  expect_match(description[[1L, "Suggests"]], "testthat \\(>= 3\\.2\\.0\\)")
})
