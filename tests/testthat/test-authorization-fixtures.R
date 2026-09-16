test_that("authorization fixtures cannot equate both expected RLS row sets", {
  path <- withr::local_tempfile(fileext = ".json")
  withr::local_envvar(c(
    FABRIC_TEST_REQUIRED_FEATURES = "authorization",
    FABRIC_TEST_AUTHORIZATION_MATRIX = path
  ))
  write <- function(expected) {
    jsonlite::write_json(
      list(
        identities = list(list(name = "a"), list(name = "b")),
        dax = lapply(expected, function(x) list(expected = x))
      ),
      path,
      auto_unbox = TRUE
    )
  }
  write(list(list(1L), list(1L)))
  expect_error(fabric_test_authorization_matrix(), "distinguish")
  write(list(list(), list(2L)))
  expect_error(fabric_test_authorization_matrix(), "nonempty")
  write(list(list(1L), list(2L)))
  expect_length(fabric_test_authorization_matrix()$dax, 2L)
  expect_error(fabric_test_identity_token(NULL), "token environment variable")
})
