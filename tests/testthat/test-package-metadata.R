test_that("package title represents read and write capabilities", {
  description_path <- test_path("..", "..", "DESCRIPTION")
  if (!file.exists(description_path)) {
    skip("Package source DESCRIPTION is not available")
  }

  title <- read.dcf(description_path, fields = "Title")[[1L]]

  expect_match(title, "Microsoft Fabric", fixed = TRUE)
  expect_match(title, "Access|Manage")
})
