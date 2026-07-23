test_that("Livy statement output errors are raised", {
  local_mocked_bindings(
    .httr2_json = function(request, ...) {
      list(
        id = 1L,
        state = "available",
        output = list(
          status = "error",
          evalue = "table was not found"
        )
      )
    }
  )

  expect_error(
    fabric_livy_statement(
      session = list(
        url = "https://api.fabric.microsoft.com/sessions/1",
        token = "token"
      ),
      code = "spark.sql('SELECT 1')",
      verbose = FALSE
    ),
    "table was not found",
    fixed = TRUE
  )
})
