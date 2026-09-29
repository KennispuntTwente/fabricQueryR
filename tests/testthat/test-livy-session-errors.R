test_that("Livy ready sessions distinguish empty and actual Fabric errors", {
  # Exercises wall-clock waits with one-second deadlines.
  skip_on_cran()
  withr::local_options(warnPartialMatchDollar = TRUE)
  for (hc in c(FALSE, TRUE)) {
    for (message in list(NULL, "", "  \t\n", "Spark initialization failed")) {
      response <- list(
        id = "session",
        state = "idle",
        sessionId = "backing",
        replId = "repl",
        fabricSessionStateInfo = list(state = "unknown", errorMessage = message)
      )
      local_mocked_bindings(fabric_livy_json = function(...) response)
      session <- FabricLivySession$new(
        response,
        livy_url = "https://example.test/livy/sessions",
        high_concurrency = hc,
        credential = fabric_credential(token = "token"),
        verbose = FALSE
      )
      if (identical(message, "Spark initialization failed")) {
        expect_error(
          session$wait(timeout = 1, poll_interval = 0),
          class = "fabric_livy_session_error"
        )
      } else {
        expect_identical(session$wait(timeout = 1, poll_interval = 0), session)
      }
    }
  }
})
