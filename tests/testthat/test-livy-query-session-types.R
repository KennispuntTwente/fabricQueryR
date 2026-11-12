test_that("one-off Livy queries select, execute, and release either session type", {
  livy_url <- "https://example.test/livy/sessions"
  record <- r6_test_record()$as_list()
  record$livy_url <- livy_url
  lakehouse <- fabric_r6_record(
    record,
    class(record),
    fabric_credential(token = "discovery-token")
  )
  calls <- list()
  local_mocked_bindings(
    fabric_livy_json = function(method, url, credential, payload = NULL, ...) {
      calls[[length(calls) + 1L]] <<- list(
        method = method,
        url = url,
        payload = payload,
        credential = credential
      )
      if (identical(url, collection_url) && method == "POST") {
        return(list(id = "query-context", state = "starting"))
      }
      if (identical(url, context_url) && method == "GET") {
        return(list(
          id = "query-context",
          state = "idle",
          sessionId = "shared-session",
          replId = "isolated-repl"
        ))
      }
      if (identical(url, statement_url) && method == "POST") {
        return(list(id = 3L, state = "waiting"))
      }
      if (identical(url, paste0(statement_url, "/3")) && method == "GET") {
        return(list(
          id = 3L,
          state = "available",
          output = list(status = "ok", data = list("text/plain" = "42"))
        ))
      }
      rlang::abort(paste("Unexpected mocked call:", method, url))
    },
    fabric_livy_ok = function(method, url, deadline = NULL, ...) {
      expect_s3_class(deadline, "POSIXct")
      calls[[length(calls) + 1L]] <<- list(method = method, url = url)
      TRUE
    }
  )

  modes <- list(
    list(),
    list(high_concurrency = FALSE),
    list(high_concurrency = TRUE),
    list(high_concurrency = TRUE, session_tag = "query-workers")
  )
  for (use_object in c(FALSE, TRUE)) {
    query <- if (use_object) {
      lakehouse$livy_query
    } else {
      function(...) fabric_livy_query(livy_url, token = "token", ...)
    }
    for (options in modes) {
      high_concurrency <- isTRUE(options$high_concurrency)
      collection_url <- if (high_concurrency) {
        sub("/sessions$", "/highConcurrencySessions", livy_url)
      } else {
        livy_url
      }
      context_url <- paste0(collection_url, "/query-context")
      statement_url <- if (high_concurrency) {
        paste0(collection_url, "/shared-session/repls/isolated-repl/statements")
      } else {
        paste0(context_url, "/statements")
      }
      calls <- list()
      returned <- withVisible(do.call(query, c(
        list(
          code = "print(42)",
          kind = "pyspark",
          conf = list("spark.sql.shuffle.partitions" = "2"),
          environment_id = "11111111-1111-4111-8111-111111111111",
          poll_interval = 0,
          verbose = FALSE
        ),
        options
      )))

      expect_false(returned$visible)
      expect_s3_class(returned$value, "fabric_livy_statement_result")
      expect_identical(returned$value$output$parsed, "42")
      expect_identical(returned$value$url, paste0(statement_url, "/3"))
      expect_identical(
        vapply(calls, `[[`, character(1), "method"),
        c("POST", "GET", "POST", "GET", "DELETE")
      )
      expect_identical(
        vapply(calls, `[[`, character(1), "url"),
        c(collection_url, context_url, statement_url,
          paste0(statement_url, "/3"), context_url)
      )
      expect_identical(calls[[1L]]$payload$sessionTag, options$session_tag)
      expect_identical(
        calls[[1L]]$payload$conf[["spark.sql.shuffle.partitions"]],
        "2"
      )
      expect_match(
        calls[[1L]]$payload$conf[["spark.fabric.environmentDetails"]],
        "11111111-1111-4111-8111-111111111111",
        fixed = TRUE
      )
      expect_identical(calls[[3L]]$payload, list(code = "print(42)", kind = "pyspark"))
      expect_identical(
        fabric_get_token(calls[[1L]]$credential, .fabric_audience$fabric),
        if (use_object) "discovery-token" else "token"
      )
    }
  }
})

test_that("one-off Livy queries reject invalid concurrency options locally", {
  requests <- 0L
  local_mocked_bindings(
    fabric_livy_json = function(...) {
      requests <<- requests + 1L
      rlang::abort("Unexpected network request")
    }
  )
  invalid <- list(
    list(options = list(high_concurrency = NA), pattern = "high_concurrency"),
    list(options = list(high_concurrency = "true"), pattern = "high_concurrency"),
    list(options = list(high_concurrency = c(TRUE, FALSE)), pattern = "high_concurrency"),
    list(options = list(session_tag = "workers"), pattern = "only available for high-concurrency"),
    list(options = list(high_concurrency = TRUE, session_tag = ""), pattern = "session_tag"),
    list(options = list(high_concurrency = TRUE, session_tag = NA_character_), pattern = "session_tag"),
    list(options = list(high_concurrency = TRUE, session_tag = 1), pattern = "session_tag")
  )
  for (case in invalid) {
    expect_error(
      do.call(fabric_livy_query, c(
        list(
          livy_url = "https://example.test/livy/sessions",
          code = "print(42)",
          token = "token",
          verbose = FALSE
        ),
        case$options
      )),
      case$pattern
    )
  }
  expect_identical(requests, 0L)
})

test_that("one-off HC queries release their context after startup or execution errors", {
  context_url <- "https://example.test/livy/highConcurrencySessions/query-context"
  closed <- character()
  local_mocked_bindings(
    fabric_livy_json = function(method, url, ...) {
      if (method == "POST" && endsWith(url, "/highConcurrencySessions")) {
        return(list(id = "query-context", state = "starting"))
      }
      if (method == "GET" && identical(url, context_url)) {
        if (failure == "startup") {
          rlang::abort("startup failed")
        }
        return(list(
          id = "query-context", state = "idle",
          sessionId = "shared-session", replId = "isolated-repl"
        ))
      }
      if (method == "POST" && endsWith(url, "/statements")) {
        rlang::abort("execution failed")
      }
      rlang::abort(paste("Unexpected mocked call:", method, url))
    },
    fabric_livy_ok = function(method, url, deadline = NULL, ...) {
      expect_identical(method, "DELETE")
      expect_s3_class(deadline, "POSIXct")
      closed <<- c(closed, url)
      TRUE
    }
  )
  for (failure in c("startup", "execution")) {
    closed <- character()
    expect_error(
      fabric_livy_query(
        "https://example.test/livy/sessions",
        "print(42)",
        high_concurrency = TRUE,
        session_tag = "query-workers",
        token = "token",
        poll_interval = 0,
        verbose = FALSE
      ),
      paste(failure, "failed"),
      fixed = TRUE
    )
    expect_identical(closed, context_url)
  }
})
