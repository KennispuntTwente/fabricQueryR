# This file is part of the standard setup for testthat
# It is recommended that you do not modify it
#
# Where should you do additional test configuration?
# Learn more about the roles of various files in:
# * https://r-pkgs.org/testing-design.html#sec-tests-files-overview
# * https://testthat.r-lib.org/articles/special-files.html

library(testthat)
library(fabricQueryR)

if (identical(Sys.getenv("NOT_CRAN"), "true")) {
  test_check("fabricQueryR")
} else {
  # Live services, Python fixtures, and repository tooling belong in CI.
  # Keep the ordinary unit tests; individual costly tests use skip_on_cran().
  test_check(
    "fabricQueryR",
    filter = paste(
      "^integration-fabric-",
      "^delta-rs-oracle$",
      "^(ci-integration-summary|local-integration-runner|playground)$",
      "^(readme|vignettes)$",
      sep = "|"
    ),
    invert = TRUE
  )
}
