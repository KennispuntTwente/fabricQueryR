test_that("eventual integration checks retain successful candidates", {
  attempt <- 0L
  result <- fabric_test_eventually(
    function() {
      attempt <<- attempt + 1L
      data.frame(id = seq_len(attempt), state = "running")
    },
    attempts = 3L,
    delay = 0,
    ready = function(value) nrow(value) == 2L
  )

  expect_identical(attempt, 2L)
  expect_identical(result$id, 1:2)
})

test_that("eventual integration timeouts describe the last safe candidate", {
  error <- expect_error(
    fabric_test_eventually(
      function() {
        data.frame(
          id = c("session-a", "session-b"),
          state = c("starting", "idle"),
          display_name = c("secret one", "secret two")
        )
      },
      attempts = 2L,
      delay = 0,
      ready = function(value) FALSE
    )
  )

  expect_match(conditionMessage(error), "count=2", fixed = TRUE)
  expect_match(conditionMessage(error), "session-a, session-b", fixed = TRUE)
  expect_match(conditionMessage(error), "starting, idle", fixed = TRUE)
  expect_false(grepl("secret", conditionMessage(error), fixed = TRUE))
})
