fabric_test_authorization_matrix <- function() {
  path <- fabric_test_feature_environment(
    "authorization",
    "FABRIC_TEST_AUTHORIZATION_MATRIX"
  )
  config <- jsonlite::fromJSON(path, simplifyVector = FALSE)
  if (length(config$identities) != 2L || length(config$dax) != 2L) {
    stop(
      "Authorization matrix requires exactly two restricted identities and two DAX row sets"
    )
  }
  if (
    identical(config$dax[[1L]]$expected, config$dax[[2L]]$expected) ||
      !all(vapply(config$dax, function(x) length(x$expected) > 0L, logical(1)))
  ) {
    stop(
      "DAX authorization expectations must be nonempty and distinguish the two row sets"
    )
  }
  config
}

fabric_test_identity_token <- function(variable) {
  if (!is.character(variable) || length(variable) != 1L || !nzchar(variable)) {
    stop("Authorization fixture must name a token environment variable")
  }
  fabric_test_feature_environment("authorization", variable)
}
