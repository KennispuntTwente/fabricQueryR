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
  expect_match(description[[1L, "Suggests"]], "lifecycle")
})

test_that("experimental lifecycle badge asset is packaged", {
  badge_path <- test_path(
    "..",
    "..",
    "man",
    "figures",
    "lifecycle-experimental.svg"
  )
  if (!file.exists(badge_path)) {
    skip("Package source lifecycle badge is not available")
  }

  badge <- paste(readLines(badge_path, warn = FALSE), collapse = "\n")
  expect_match(badge, "lifecycle: experimental", fixed = TRUE)
})

test_that("User Data Function help topics carry experimental badges", {
  paths <- test_path(
    "..",
    "..",
    "man",
    c(
      "fabric_function_invoke.Rd",
      "fabric_user_data_functions.Rd",
      "FabricItem.Rd"
    )
  )
  if (!all(file.exists(paths))) {
    skip("Package source help topics are not available")
  }

  for (path in paths) {
    topic <- paste(readLines(path, warn = FALSE), collapse = "\n")
    expect_match(topic, "lifecycle-experimental.svg", fixed = TRUE)
  }
})
