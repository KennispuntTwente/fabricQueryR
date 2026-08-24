test_that("Livy selects identity-aware OAuth audiences", {
  expect_identical(
    fabric_livy_audience(NULL, NULL, list(auth_type = "device_code")),
    .fabric_audience$livy_delegated
  )
  expect_identical(
    fabric_livy_audience(
      NULL,
      NULL,
      list(auth_type = "client_credentials", password = "secret")
    ),
    .fabric_audience$power_bi
  )
  expect_identical(
    fabric_livy_audience("https://custom.test/.default", "token"),
    "https://custom.test/.default"
  )
  expect_error(
    fabric_livy_audience(
      .fabric_audience$livy_delegated,
      NULL,
      list(auth_type = "client_credentials", password = "secret")
    ),
    "requires one .default audience",
    fixed = TRUE
  )
  expect_error(
    fabric_livy_audience(
      "https://api.fabric.microsoft.com/Lakehouse.Execute.All",
      NULL,
      list(auth_type = "client_credentials", password = "secret")
    ),
    "requires one .default audience",
    fixed = TRUE
  )
  expect_error(
    fabric_livy_audience(c("scope", "scope"), "token"),
    "without duplicates",
    fixed = TRUE
  )
})

test_that("custom Livy hosts require an explicit credential", {
  session_error <- rlang::catch_cnd(fabric_livy_session(
    "https://livy.example/livy",
    verbose = FALSE
  ))
  batch_error <- rlang::catch_cnd(fabric_livy_batch_submit(
    "https://livy.example/livy",
    file = "abfss://container@example.dfs.core.windows.net/job.py",
    verbose = FALSE
  ))

  expect_s3_class(session_error, "fabric_custom_endpoint_requires_token")
  expect_identical(session_error$endpoint_host, "livy.example")
  expect_identical(session_error$argument, "livy_url")
  expect_s3_class(batch_error, "fabric_custom_endpoint_requires_token")
  expect_identical(batch_error$endpoint_host, "livy.example")
  expect_identical(batch_error$argument, "livy_url")
})

test_that("Livy requests use the audience stored on the credential", {
  requested <- NULL
  bigint_as_char <- NULL
  credential <- fabric_livy_credential(
    tenant_id = NULL,
    client_id = NULL,
    token = "token"
  )
  local_mocked_bindings(
    .httr2_json = function(req, audience, bigint_as_char, ...) {
      requested <<- audience
      bigint_as_char <<- bigint_as_char
      list(ok = TRUE)
    }
  )

  fabric_livy_json(
    "GET",
    "https://api.fabric.microsoft.com/livy/sessions/1",
    credential
  )

  expect_identical(requested, .fabric_audience$livy_delegated)
  expect_true(bigint_as_char)
})

test_that("regular session runs multiple statements and closes", {
  calls <- list()
  session_gets <- 0L
  statement_gets <- new.env(parent = emptyenv())

  local_mocked_bindings(
    fabric_livy_json = function(
      method,
      url,
      credential,
      payload = NULL,
      idempotent = NULL,
      deadline = NULL
    ) {
      calls[[length(calls) + 1L]] <<- list(
        method = method,
        url = url,
        payload = payload,
        idempotent = idempotent
      )
      if (method == "POST" && grepl("/sessions$", url)) {
        return(list(id = "session-1", state = "starting"))
      }
      if (method == "GET" && grepl("/sessions/session-1$", url)) {
        session_gets <<- session_gets + 1L
        return(list(
          id = "session-1",
          state = if (session_gets == 1L) "starting" else "idle"
        ))
      }
      if (method == "POST" && grepl("/statements$", url)) {
        id <- if (grepl("first", payload$code)) 1L else 2L
        return(list(id = id, state = "waiting", code = payload$code))
      }
      if (method == "GET" && grepl("/statements/[12]$", url)) {
        id <- sub(".*/", "", url)
        count <- statement_gets[[id]] %||% 0L
        statement_gets[[id]] <- count + 1L
        if (count == 0L) {
          return(list(id = as.integer(id), state = "running"))
        }
        return(list(
          id = as.integer(id),
          state = "available",
          output = list(
            status = "ok",
            execution_count = as.integer(id),
            data = list("text/plain" = paste("result", id))
          )
        ))
      }
      rlang::abort(paste("Unexpected mocked call:", method, url))
    },
    fabric_livy_ok = function(
      method,
      url,
      credential,
      payload = NULL,
      idempotent = NULL,
      accepted_status = integer(),
      deadline = NULL
    ) {
      calls[[length(calls) + 1L]] <<- list(
        method = method,
        url = url,
        idempotent = idempotent
      )
      TRUE
    }
  )

  session <- fabric_livy_session(
    "https://api.fabric.microsoft.com/livy/batches",
    token = "token",
    conf = list("spark.sql.shuffle.partitions" = "2"),
    environment_id = "11111111-1111-4111-8111-111111111111",
    tags = list(owner = "unit-test"),
    verbose = FALSE
  )
  expect_s3_class(session, "FabricLivySession")
  expect_equal(
    session$url,
    paste0(
      "https://api.fabric.microsoft.com/livy/sessions/",
      "session-1"
    )
  )
  expect_false(calls[[1L]]$idempotent)
  expect_equal(
    calls[[1L]]$payload$conf[["spark.sql.shuffle.partitions"]],
    "2"
  )
  expect_match(
    calls[[1L]]$payload$conf[["spark.fabric.environmentDetails"]],
    "11111111-1111-4111-8111-111111111111",
    fixed = TRUE
  )
  expect_equal(calls[[1L]]$payload$tags$owner, "unit-test")

  session$wait(timeout = 1, poll_interval = 0)
  first <- session$run(
    "print('first')",
    kind = "pyspark",
    timeout = 1,
    poll_interval = 0
  )
  second <- session$run(
    "print('second')",
    kind = "pyspark",
    timeout = 1,
    poll_interval = 0
  )

  expect_s3_class(first, "fabric_livy_statement_result")
  expect_equal(first$id, 1L)
  expect_equal(first$output$parsed, "result 1")
  expect_equal(second$id, 2L)
  expect_equal(second$output$execution_count, 2L)
  expect_true(is.finite(first$duration_sec))
  expect_gte(first$duration_sec, 0)

  expect_true(session$close())
  expect_false(session$close())
  expect_true(session$closed)
  expect_error(session$status(), "closed")
  delete_calls <- Filter(
    function(call) identical(call$method, "DELETE"),
    calls
  )
  expect_length(delete_calls, 1L)
  expect_true(delete_calls[[1L]]$idempotent)
})

test_that("submit returns an inspectable and cancellable statement", {
  calls <- list()
  local_mocked_bindings(
    fabric_livy_json = function(
      method,
      url,
      credential,
      payload = NULL,
      idempotent = NULL,
      deadline = NULL
    ) {
      calls[[length(calls) + 1L]] <<- list(
        method = method,
        url = url,
        payload = payload
      )
      if (method == "POST" && grepl("/sessions$", url)) {
        return(list(id = "s", state = "idle"))
      }
      if (method == "POST" && grepl("/statements$", url)) {
        return(list(id = 9L, state = "waiting"))
      }
      if (method == "POST" && grepl("/cancel$", url)) {
        return(list(msg = "canceled"))
      }
      if (method == "GET" && grepl("/statements$", url)) {
        return(list(
          total_statements = 1L,
          statements = list(list(id = 9L, state = "waiting"))
        ))
      }
      rlang::abort("Unexpected mocked call")
    },
    fabric_livy_ok = function(...) TRUE
  )

  session <- fabric_livy_session(
    "https://example.test/livy/sessions",
    token = "token",
    verbose = FALSE
  )
  statement <- session$submit(
    "1 + 1",
    kind = "spark",
    source_id = "request-42"
  )
  expect_s3_class(statement, "FabricLivyStatement")
  expect_equal(
    calls[[2L]]$payload,
    list(code = "1 + 1", kind = "spark", sourceId = "request-42")
  )
  expect_equal(statement$cancel()$msg, "canceled")
  expect_match(calls[[3L]]$url, "/statements/9/cancel$")
  expect_length(session$statements()$statements, 1L)
  session$close()
})

test_that("session statement listing follows the Livy collection contract", {
  urls <- character()
  local_mocked_bindings(
    fabric_livy_json = function(method, url, ...) {
      if (method == "POST") {
        return(list(id = "session", state = "idle"))
      }
      urls <<- c(urls, url)
      list(
        total_statements = 3L,
        statements = lapply(1:3, function(id) {
          list(id = id, state = "available")
        })
      )
    },
    fabric_livy_ok = function(...) TRUE
  )
  session <- fabric_livy_session(
    "https://example.test/livy/sessions",
    token = "token",
    verbose = FALSE
  )

  result <- session$statements()

  expect_identical(result$total_statements, 3L)
  expect_equal(vapply(result$statements, `[[`, integer(1), "id"), 1:3)
  expect_length(urls, 1L)
  expect_match(urls, "/statements$")
  expect_false(grepl("?", urls, fixed = TRUE))
  session$close()
})

test_that("session statement listing rejects inconsistent collections", {
  local_mocked_bindings(
    fabric_livy_json = local({
      responses <- list(
        list(id = "session", state = "idle"),
        list(
          total_statements = 2L,
          statements = list(list(id = 1L, state = "available"))
        )
      )
      function(...) {
        response <- responses[[1L]]
        responses <<- responses[-1L]
        response
      }
    }),
    fabric_livy_ok = function(...) TRUE
  )
  session <- fabric_livy_session(
    "https://example.test/livy/sessions",
    token = "token",
    verbose = FALSE
  )

  expect_error(
    session$statements(),
    class = "fabric_livy_protocol_error"
  )
  session$close()
})

test_that("statement errors preserve output and traceback", {
  responses <- list(
    list(id = "s", state = "idle"),
    list(id = 1L, state = "waiting"),
    list(
      id = 1L,
      state = "available",
      output = list(
        status = "error",
        ename = "AnalysisException",
        evalue = "table was not found",
        traceback = c("line one", "line two")
      )
    )
  )
  local_mocked_bindings(
    fabric_livy_json = function(...) {
      response <- responses[[1L]]
      responses <<- responses[-1L]
      response
    },
    fabric_livy_ok = function(...) TRUE
  )

  session <- fabric_livy_session(
    "https://example.test/livy/sessions",
    token = "token",
    verbose = FALSE
  )
  statement <- session$submit("spark.table('missing')", "pyspark")
  error <- expect_error(
    statement$wait(timeout = 1, poll_interval = 0),
    class = "fabric_livy_statement_error"
  )
  expect_match(conditionMessage(error), "table was not found", fixed = TRUE)
  expect_equal(error$output$ename, "AnalysisException")
  expect_equal(error$traceback, c("line one", "line two"))

  result <- statement$result(refresh = FALSE, error_on_failure = FALSE)
  expect_equal(result$output$status, "error")
  expect_equal(result$output$evalue, "table was not found")
  expect_equal(result$output$traceback, c("line one", "line two"))
  session$close()
})

test_that("statement JSON output is parsed independently of lifecycle", {
  result <- fabric_livy_output(
    response = list(
      id = 4L,
      state = "available",
      output = list(
        status = "ok",
        data = list(
          "application/json" = list(
            list(id = 1L, value = "alpha"),
            list(id = 2L, value = "beta")
          )
        )
      )
    ),
    started_local = as.POSIXct("2026-01-01", tz = "UTC"),
    completed_local = as.POSIXct("2026-01-01 00:00:02", tz = "UTC"),
    url = "https://example.test/statements/4"
  )
  expect_s3_class(result$output$parsed, "tbl_df")
  expect_equal(result$output$parsed$id, c(1L, 2L))
  expect_equal(result$output$parsed$value, c("alpha", "beta"))
  expect_equal(result$duration_sec, 2)
})

test_that("generic statement JSON preserves null values", {
  result <- fabric_livy_output(
    response = list(
      id = 4L,
      state = "available",
      output = list(
        status = "ok",
        data = list(
          "application/json" = list(
            present = "value",
            missing = NULL,
            nested = list(missing = NULL),
            values = list(1L, NULL, 3L)
          )
        )
      )
    ),
    started_local = as.POSIXct("2026-01-01", tz = "UTC"),
    completed_local = as.POSIXct("2026-01-01 00:00:01", tz = "UTC"),
    url = "https://example.test/statements/4"
  )

  expect_identical(result$output$parsed$present, "value")
  expect_null(result$output$parsed$missing)
  expect_null(result$output$parsed$nested$missing)
  expect_identical(result$output$parsed$values, c(1L, NA_integer_, 3L))
})

test_that("Livy fallback columns simplify only uniform scalar types", {
  simplifiable <- list(
    character = list(
      values = list("one", NULL, "two"),
      expected = c("one", NA_character_, "two")
    ),
    integer = list(
      values = list(1L, NULL, 2L),
      expected = c(1L, NA_integer_, 2L)
    ),
    double = list(
      values = list(1, NULL, 2.5),
      expected = c(1, NA_real_, 2.5)
    ),
    logical = list(
      values = list(TRUE, NULL, FALSE),
      expected = c(TRUE, NA, FALSE)
    ),
    complex = list(
      values = list(1 + 2i, NULL, 3 + 4i),
      expected = c(1 + 2i, NA_complex_, 3 + 4i)
    )
  )
  for (case in simplifiable) {
    expect_identical(
      fabric_livy_simplify_column(case$values),
      case$expected
    )
  }

  fallback <- list(
    mixed = list(1L, "two"),
    numeric_mixture = list(1L, 2),
    raw = list(as.raw(1L), NULL),
    nested = list(list(value = 1L), NULL),
    non_scalar = list(c(1L, 2L), 3L),
    all_null = list(NULL, NULL)
  )
  for (case in fallback) {
    expect_identical(fabric_livy_simplify_column(case), case)
  }
})

test_that("Livy table MIME output is parsed into a tibble", {
  result <- fabric_livy_output(
    response = list(
      id = 5L,
      state = "available",
      output = list(
        status = "ok",
        data = list(
          "application/vnd.livy.table.v1+json" = list(
            headers = list(
              list(name = "id", type = "BIGINT_TYPE"),
              list(name = "label", type = "STRING_TYPE")
            ),
            data = list(
              list("9007199254740993", "alpha"),
              list("-9007199254740993", "beta")
            )
          ),
          "text/plain" = "fallback rendering"
        )
      )
    ),
    started_local = as.POSIXct("2026-01-01", tz = "UTC"),
    completed_local = as.POSIXct("2026-01-01 00:00:01", tz = "UTC"),
    url = "https://example.test/statements/5"
  )

  expect_s3_class(result$output$parsed, "tbl_df")
  expect_equal(
    result$output$parsed$id,
    c("9007199254740993", "-9007199254740993")
  )
  expect_equal(result$output$parsed$label, c("alpha", "beta"))
})

test_that("Livy raw JSON boundaries preserve Spark BIGINT values", {
  raw_table <- paste0(
    '{"headers":[{"name":"id","type":"BIGINT_TYPE"}],',
    '"data":[[9007199254740993],[-9007199254740993]]}'
  )
  parsed_table <- fabric_livy_parse_table(raw_table)
  expect_identical(
    parsed_table$id,
    c("9007199254740993", "-9007199254740993")
  )

  raw_sql <- paste0(
    '{"schema":{"type":"struct","fields":[',
    '{"name":"id","type":"long","nullable":false}]},',
    '"data":[[9007199254740993]]}'
  )
  parsed_sql <- fabric_livy_parse_sql_json(raw_sql)
  expect_identical(parsed_sql$id, "9007199254740993")
})

test_that("Livy table conversion follows the declared Spark schema", {
  parsed <- fabric_livy_parse_table(list(
    headers = list(
      list(name = "all_null_date", type = "DATE_TYPE"),
      list(name = "long", type = "BIGINT_TYPE"),
      list(name = "amount", type = "decimal(38,15)"),
      list(name = "at", type = "timestamp"),
      list(name = "local_at", type = "timestamp_ntz"),
      list(name = "measurement", type = "double"),
      list(name = "bytes", type = "binary"),
      list(name = "nested", type = list(type = "array", elementType = "long"))
    ),
    data = list(
      list(
        NULL,
        "9007199254740993",
        "12345678901234567890.123456789012345",
        "2026-08-10T12:30:01.125Z",
        "2026-08-10T12:30:01.125",
        "NaN",
        jsonlite::base64_enc(charToRaw("abc")),
        list("9007199254740993", NULL)
      ),
      list(NULL, "-1", "-0.0100", NULL, NULL, "Infinity", NULL, NULL)
    )
  ))

  expect_s3_class(parsed$all_null_date, "Date")
  expect_true(all(is.na(parsed$all_null_date)))
  expect_identical(parsed$long, c("9007199254740993", "-1"))
  expect_identical(
    parsed$amount,
    c("12345678901234567890.123456789012345", "-0.0100")
  )
  expect_s3_class(parsed$at, "POSIXct")
  expect_equal(
    format(parsed$at[[1L]], "%Y-%m-%d %H:%M:%OS3", tz = "UTC"),
    "2026-08-10 12:30:01.125"
  )
  expect_identical(
    parsed$local_at,
    c("2026-08-10 12:30:01.125", NA_character_)
  )
  expect_true(is.nan(parsed$measurement[[1L]]))
  expect_identical(parsed$measurement[[2L]], Inf)
  expect_identical(rawToChar(parsed$bytes[[1L]]), "abc")
  expect_null(parsed$bytes[[2L]])
  expect_identical(parsed$nested[[1L]][[1L]], "9007199254740993")
  expect_identical(attr(parsed, "spark_schema")[[2L]]$type, "BIGINT_TYPE")

  empty <- fabric_livy_parse_table(list(
    headers = list(
      list(name = "id", type = "long"),
      list(name = "day", type = "date")
    ),
    data = list()
  ))
  expect_identical(empty$id, character())
  expect_s3_class(empty$day, "Date")
  expect_length(empty$day, 0L)
  expect_length(attr(empty, "spark_schema"), 2L)
})

test_that("Fabric SQL nulls remain distinguishable from textual NaN", {
  parsed <- fabric_livy_parse_sql_json(list(
    schema = list(
      type = "struct",
      fields = list(
        list(name = "nan_value", type = "double", nullable = TRUE),
        list(name = "local_at", type = "timestamp_ntz", nullable = TRUE)
      )
    ),
    data = list(list(NULL, "2026-08-10T12:30:01.125"))
  ))

  expect_identical(parsed$nan_value, NA_real_)
  expect_identical(parsed$local_at, "2026-08-10 12:30:01.125")
})

test_that("Spark SQL JSON output is parsed into a tibble", {
  result <- fabric_livy_output(
    response = list(
      id = 6L,
      state = "available",
      output = list(
        status = "ok",
        data = list(
          "application/json" = list(
            schema = list(
              type = "struct",
              fields = list(
                list(
                  name = "fabricqueryr_sql_value",
                  type = "integer",
                  nullable = FALSE
                )
              )
            ),
            data = list(list(42L))
          )
        )
      )
    ),
    started_local = as.POSIXct("2026-01-01", tz = "UTC"),
    completed_local = as.POSIXct("2026-01-01 00:00:01", tz = "UTC"),
    url = "https://example.test/statements/6"
  )

  expect_s3_class(result$output$parsed, "tbl_df")
  expect_identical(result$output$parsed$fabricqueryr_sql_value, 42L)
})

test_that("Livy table MIME output rejects malformed rows", {
  expect_error(
    fabric_livy_parse_table(list(
      headers = list(list(name = "id"), list(name = "label")),
      data = list(list(1L))
    )),
    "row width",
    class = "fabric_livy_protocol_error"
  )
})

test_that("session finalizer does not perform network cleanup", {
  deleted <- character()
  local_mocked_bindings(
    fabric_livy_json = function(...) list(id = "finalize-me", state = "idle"),
    fabric_livy_ok = function(method, url, ...) {
      if (method == "DELETE") {
        deleted <<- c(deleted, url)
      }
      TRUE
    }
  )
  session <- fabric_livy_session(
    "https://example.test/livy/sessions",
    token = "token",
    verbose = FALSE
  )
  rm(session)
  gc()
  expect_length(deleted, 0L)
})

test_that("high-concurrency sessions use HC and REPL endpoints", {
  calls <- list()
  local_mocked_bindings(
    fabric_livy_json = function(
      method,
      url,
      credential,
      payload = NULL,
      idempotent = NULL,
      deadline = NULL
    ) {
      calls[[length(calls) + 1L]] <<- list(
        method = method,
        url = url,
        payload = payload,
        idempotent = idempotent
      )
      if (method == "POST" && grepl("highConcurrencySessions$", url)) {
        return(list(id = "hc-id", state = "NotStarted"))
      }
      if (method == "GET" && grepl("highConcurrencySessions/hc-id$", url)) {
        return(list(
          id = "hc-id",
          state = "Idle",
          sessionId = "shared-session",
          replId = "isolated-repl"
        ))
      }
      if (method == "POST" && grepl("/statements$", url)) {
        return(list(id = 3L, state = "waiting"))
      }
      rlang::abort(paste("Unexpected mocked call:", method, url))
    },
    fabric_livy_ok = function(...) TRUE
  )

  session <- fabric_livy_session(
    "https://example.test/livy/sessions",
    high_concurrency = TRUE,
    session_tag = "packed-work",
    artifact_name = "TestLakehouse",
    tags = list(run = "42"),
    token = "token",
    verbose = FALSE
  )
  expect_equal(calls[[1L]]$payload$sessionTag, "packed-work")
  expect_equal(calls[[1L]]$payload$artifactName, "TestLakehouse")
  expect_false(calls[[1L]]$idempotent)

  session$wait(timeout = 1, poll_interval = 0)
  statement <- session$submit("print(1)", "pyspark")
  expect_s3_class(statement, "FabricLivyStatement")
  expect_match(
    calls[[3L]]$url,
    paste0(
      "/highConcurrencySessions/shared-session/",
      "repls/isolated-repl/statements$"
    )
  )
  expect_error(session$reset_timeout(), "not supported")
  session$close()
})

test_that("session reset timeout uses its documented endpoint", {
  reset_url <- NULL
  local_mocked_bindings(
    fabric_livy_json = function(...) list(id = "s", state = "idle"),
    fabric_livy_ok = function(method, url, ...) {
      if (method == "POST") {
        reset_url <<- url
      }
      TRUE
    }
  )
  session <- fabric_livy_session(
    "https://example.test/livy/sessions",
    token = "token",
    verbose = FALSE
  )
  expect_identical(session$reset_timeout(), session)
  expect_equal(reset_url, paste0(session$url, "/reset-timeout"))
  session$close()
})

test_that("closing an auto-terminated Livy session accepts 404", {
  accepted <- NULL
  received_deadline <- NULL
  local_mocked_bindings(
    fabric_livy_json = function(...) list(id = "expired", state = "idle"),
    fabric_livy_ok = function(method, accepted_status, deadline = NULL, ...) {
      expect_identical(method, "DELETE")
      accepted <<- accepted_status
      received_deadline <<- deadline
      TRUE
    }
  )
  session <- fabric_livy_session(
    "https://example.test/livy/sessions",
    token = "token",
    verbose = FALSE
  )

  deadline <- Sys.time() + 5
  expect_true(session$close(deadline = deadline))
  expect_identical(accepted, 404L)
  expect_identical(received_deadline, deadline)
  expect_true(session$closed)
})

test_that("fabric_livy_query closes temporary session after failure", {
  closed <- FALSE
  fake_session <- new.env(parent = emptyenv())
  fake_session$wait <- function(...) invisible(fake_session)
  fake_session$run <- function(...) rlang::abort("spark failed")
  fake_session$close <- function(deadline = NULL) {
    closed <<- TRUE
    TRUE
  }
  local_mocked_bindings(
    fabric_livy_session = function(...) fake_session
  )

  expect_error(
    fabric_livy_query(
      "https://example.test/livy/sessions",
      "raise Exception()",
      token = "token",
      verbose = FALSE
    ),
    "spark failed",
    fixed = TRUE
  )
  expect_true(closed)
})

test_that("fabric_livy_query warns without losing a successful result", {
  result <- structure(list(state = "available"), class = "livy-result")
  fake_session <- new.env(parent = emptyenv())
  fake_session$wait <- function(...) invisible(fake_session)
  fake_session$run <- function(...) result
  fake_session$close <- function(deadline = NULL) rlang::abort("delete failed")
  local_mocked_bindings(
    fabric_livy_session = function(...) fake_session
  )

  returned <- NULL
  expect_warning(
    returned <- fabric_livy_query(
      "https://api.fabric.microsoft.com/livy/sessions",
      "print(1)",
      token = "token",
      verbose = FALSE
    ),
    class = "fabric_livy_cleanup_warning"
  )

  expect_identical(returned, result)
})

test_that("fabric_livy_query retains execution and bounded cleanup failures", {
  cleanup_deadline <- NULL
  fake_session <- new.env(parent = emptyenv())
  fake_session$url <- "https://api.fabric.microsoft.com/livy/sessions/42"
  fake_session$wait <- function(...) invisible(fake_session)
  fake_session$run <- function(...) rlang::abort("spark failed")
  fake_session$close <- function(deadline = NULL) {
    cleanup_deadline <<- deadline
    rlang::abort("delete failed")
  }
  local_mocked_bindings(
    fabric_livy_session = function(...) fake_session
  )
  withr::local_options(fabricqueryr.livy.cleanup_timeout = 5)

  error <- expect_error(
    fabric_livy_query(
      "https://api.fabric.microsoft.com/livy/sessions",
      "raise Exception()",
      token = "token",
      verbose = FALSE
    ),
    class = "fabric_livy_execution_cleanup_error"
  )

  expect_match(conditionMessage(error$parent), "spark failed", fixed = TRUE)
  expect_s3_class(error$cleanup_error, "fabric_livy_cleanup_error")
  expect_match(
    conditionMessage(error$cleanup_error),
    "delete failed",
    fixed = TRUE
  )
  expect_s3_class(cleanup_deadline, "POSIXct")
  expect_gt(cleanup_deadline, Sys.time())
  expect_lte(cleanup_deadline, Sys.time() + 5)
  expect_identical(error$session_url, fake_session$url)
})

test_that("batch jobs expose success logs and structured results", {
  calls <- list()
  gets <- 0L
  local_mocked_bindings(
    fabric_livy_json = function(
      method,
      url,
      credential,
      payload = NULL,
      idempotent = NULL,
      deadline = NULL
    ) {
      calls[[length(calls) + 1L]] <<- list(
        method = method,
        url = url,
        payload = payload,
        idempotent = idempotent
      )
      if (method == "POST") {
        return(list(id = "batch-1", state = "starting"))
      }
      gets <<- gets + 1L
      if (gets == 1L) {
        return(list(id = "batch-1", state = "running", log = "starting"))
      }
      list(
        id = "batch-1",
        state = "success",
        result = "Succeeded",
        appId = "application-1",
        log = c("starting", "FABRICQUERYR_BATCH_SUCCESS")
      )
    },
    fabric_livy_ok = function(...) TRUE
  )

  batch <- fabric_livy_batch_submit(
    "https://example.test/livy/sessions",
    file = livy_test_application_uri,
    name = "unit-batch",
    args = c("success"),
    conf = list("spark.test" = "yes"),
    environment_id = "11111111-1111-4111-8111-111111111111",
    target_lakehouse_id = "22222222-2222-4222-8222-222222222222",
    token = "token",
    verbose = FALSE
  )
  expect_s3_class(batch, "FabricLivyBatch")
  expect_match(calls[[1L]]$url, "/batches$")
  expect_false(calls[[1L]]$idempotent)
  expect_equal(calls[[1L]]$payload$args, "success")
  expect_equal(
    calls[[1L]]$payload$conf[["spark.targetLakehouse"]],
    "22222222-2222-4222-8222-222222222222"
  )

  batch$wait(timeout = 1, poll_interval = 0)
  result <- batch$result(refresh = FALSE)
  expect_s3_class(result, "fabric_livy_batch_result")
  expect_equal(result$result, "Succeeded")
  expect_equal(result$app_id, "application-1")
  expect_match(
    paste(batch$logs(refresh = FALSE), collapse = "\n"),
    "FABRICQUERYR_BATCH_SUCCESS"
  )
})

test_that("Livy vector fields remain JSON arrays when length one", {
  request <- NULL
  local_mocked_bindings(
    .httr2_json = function(req, ...) {
      request <<- req
      list(id = "batch-1", state = "starting")
    }
  )

  fabric_livy_json(
    "POST",
    "https://example.test/batches",
    livy_test_credential(),
    payload = list(
      file = "fixture.py",
      args = "success",
      jars = "dependency.jar"
    )
  )

  expect_s3_class(request$body$data$args, "AsIs")
  expect_s3_class(request$body$data$jars, "AsIs")
  expect_false(inherits(request$body$data$file, "AsIs"))
  expect_equal(
    as.character(jsonlite::toJSON(
      request$body$data,
      auto_unbox = request$body$params$auto_unbox
    )),
    '{"file":"fixture.py","args":["success"],"jars":["dependency.jar"]}'
  )
})

test_that("batch failures and cancellation preserve service details", {
  mode <- "failure"
  accepted <- NULL
  local_mocked_bindings(
    fabric_livy_json = function(method, ...) {
      if (method == "POST") {
        return(list(id = "batch-2", state = "starting"))
      }
      list(
        id = "batch-2",
        state = "dead",
        result = "Failed",
        log = c("driver log", "intentional batch failure"),
        errorInfo = list(list(message = "python exited with status 1"))
      )
    },
    fabric_livy_ok = function(
      method,
      url,
      ...,
      accepted_status = integer()
    ) {
      mode <<- paste(method, url)
      accepted <<- accepted_status
      TRUE
    }
  )
  batch <- fabric_livy_batch_submit(
    "https://example.test/livy/batches",
    file = livy_test_application_uri,
    token = "token",
    verbose = FALSE
  )
  error <- expect_error(
    batch$wait(timeout = 1, poll_interval = 0),
    class = "fabric_livy_batch_error"
  )
  expect_match(conditionMessage(error), "intentional batch failure")
  expect_equal(error$logs[[1L]], "driver log")
  expect_length(error$error_info, 1L)

  expect_true(batch$cancel())
  expect_true(batch$cancel_requested)
  expect_match(mode, paste0("^DELETE ", batch$url, "$"))
  expect_identical(accepted, 404L)
})

test_that("batch timeout can request cancellation", {
  cancelled <- FALSE
  cancel_deadline <- NULL
  local_mocked_bindings(
    fabric_livy_json = function(method, ...) {
      if (method == "POST") {
        list(id = "slow-batch", state = "starting")
      } else {
        list(id = "slow-batch", state = "running")
      }
    },
    fabric_livy_ok = function(..., deadline = NULL) {
      cancelled <<- TRUE
      cancel_deadline <<- deadline
      TRUE
    }
  )
  batch <- fabric_livy_batch_submit(
    "https://example.test/livy/batches",
    file = livy_test_application_uri,
    token = "token",
    verbose = FALSE
  )
  error <- expect_error(
    batch$wait(
      timeout = 0,
      poll_interval = 0,
      cancel_on_timeout = TRUE
    ),
    class = "fabric_livy_timeout_error"
  )
  expect_s3_class(error$batch, "fabric_livy_batch_metadata")
  expect_identical(error$batch$id, batch$id)
  expect_identical(error$batch$url, batch$url)
  expect_identical(error$handle, batch)
  expect_identical(error$kind, "batch")
  expect_identical(error$last_state, "starting")
  expect_identical(error$last_response, batch$response)
  expect_true(cancelled)
  expect_true(batch$cancel_requested)
  expect_s3_class(cancel_deadline, "POSIXct")
  expect_gt(cancel_deadline, Sys.time())
  expect_true(error$cancel_accepted)
  expect_null(error$cancel_error)
})

test_that("batch timeout retains a bounded cleanup cancellation failure", {
  local_mocked_bindings(
    fabric_livy_json = function(...) {
      list(id = "slow-batch", state = "running")
    },
    fabric_livy_ok = function(..., deadline = NULL) {
      expect_s3_class(deadline, "POSIXct")
      expect_gt(deadline, Sys.time())
      rlang::abort(
        paste0(
          "cancellation deadline exhausted; ",
          "Authorization: Bearer sentinel-cancel-secret"
        )
      )
    }
  )
  batch <- fabric_livy_batch_submit(
    "https://example.test/livy/batches",
    file = livy_test_application_uri,
    token = "token",
    verbose = FALSE
  )

  error <- expect_error(
    batch$wait(timeout = 0, cancel_on_timeout = TRUE),
    class = "fabric_livy_timeout_error"
  )
  expect_false(error$cancel_accepted)
  expect_match(
    conditionMessage(error$cancel_error),
    "cancellation deadline exhausted",
    fixed = TRUE
  )
  expect_s3_class(error$cancel_error, "fabric_livy_cancellation_error")
  expect_false(
    grepl(
      "sentinel-cancel-secret",
      rawToChar(serialize(error, NULL, ascii = TRUE)),
      fixed = TRUE
    )
  )
  expect_identical(error$handle, batch)
  expect_false(batch$cancel_requested)
})

test_that("statement wait polls through cancelling until cancelled", {
  responses <- list(
    list(id = "session", state = "idle"),
    list(id = 7L, state = "running"),
    list(id = 7L, state = "cancelling"),
    list(id = 7L, state = "cancelled")
  )
  calls <- 0L
  local_mocked_bindings(
    fabric_livy_json = function(...) {
      calls <<- calls + 1L
      response <- responses[[1L]]
      responses <<- responses[-1L]
      response
    },
    fabric_livy_ok = function(...) TRUE
  )

  session <- fabric_livy_session(
    "https://example.test/livy/sessions",
    token = "token",
    verbose = FALSE
  )
  statement <- session$submit("print(1)", "pyspark")
  statement$wait(
    timeout = 1,
    poll_interval = 0,
    error_on_failure = FALSE
  )

  expect_identical(statement$state, "cancelled")
  expect_identical(calls, 4L)
  result <- statement$result(refresh = FALSE, error_on_failure = FALSE)
  expect_identical(result$state, "cancelled")
  session$close()
})

test_that("top-level batch waiting cancels on timeout and exposes its handle", {
  calls <- character()
  local_mocked_bindings(
    fabric_livy_json = function(method, ...) {
      calls <<- c(calls, method)
      if (method == "POST") {
        list(id = "slow-batch", state = "starting")
      } else {
        list(id = "slow-batch", state = "running")
      }
    },
    fabric_livy_ok = function(method, ...) {
      calls <<- c(calls, method)
      TRUE
    }
  )

  error <- expect_error(
    fabric_livy_batch_submit(
      "https://example.test/livy/batches",
      file = livy_test_application_uri,
      token = "token",
      verbose = FALSE,
      wait = TRUE,
      timeout = 0,
      poll_interval = 0
    ),
    class = "fabric_livy_timeout_error"
  )

  expect_s3_class(error$batch, "fabric_livy_batch_metadata")
  expect_identical(error$batch$id, "slow-batch")
  expect_true(error$batch$cancel_requested)
  expect_s3_class(error$handle, "FabricLivyBatch")
  expect_identical(error$handle$status(refresh = FALSE)$id, "slow-batch")
  expect_identical(calls, c("POST", "DELETE"))
  expect_identical(error$handle$status()$state, "running")
  expect_identical(calls, c("POST", "DELETE", "GET"))
})

test_that("Livy handles and timeout errors do not serialize credentials", {
  secrets <- c(
    static = "sentinel-livy-static-token",
    callback = "sentinel-livy-callback-token",
    client = "sentinel-livy-client-secret"
  )
  credentials <- list(
    fabric_credential(token = secrets[["static"]]),
    fabric_credential(token = function(...) secrets[["callback"]]),
    fabric_credential(
      tenant_id = "tenant-id",
      client_id = "client-id",
      auth_args = list(
        auth_type = "client_credentials",
        password = secrets[["client"]]
      )
    )
  )
  handles <- lapply(credentials, function(credential) {
    FabricLivyBatch$new(
      response = list(id = "batch-id", state = "starting"),
      url = "https://example.test/livy/batches",
      credential = credential,
      verbose = FALSE
    )
  })

  serialized_handles <- vapply(
    handles,
    function(handle) rawToChar(serialize(handle, NULL, ascii = TRUE)),
    character(1)
  )
  expect_false(any(vapply(
    secrets,
    function(secret) any(grepl(secret, serialized_handles, fixed = TRUE)),
    logical(1)
  )))

  error <- tryCatch(
    fabric_livy_abort_timeout(
      "batch",
      handles[[1L]],
      handles[[1L]]$response
    ),
    error = identity
  )
  serialized_error <- rawToChar(serialize(error, NULL, ascii = TRUE))
  expect_s3_class(error$batch, "fabric_livy_batch_metadata")
  expect_identical(error$batch$id, "batch-id")
  expect_identical(error$handle, handles[[1L]])
  expect_false(grepl(secrets[["static"]], serialized_error, fixed = TRUE))

  restored_error <- unserialize(serialize(error, NULL))
  credential_error <- rlang::catch_cnd(
    restored_error$handle$status(),
    classes = "error"
  )
  expect_s3_class(credential_error, "fabric_livy_credential_error")
  expect_match(
    conditionMessage(credential_error),
    "no longer has an in-process credential",
    fixed = TRUE
  )

  restored <- unserialize(serialize(handles[[1L]], NULL))
  expect_error(
    restored$status(),
    "no longer has an in-process credential",
    class = "fabric_livy_credential_error"
  )
})

test_that("Livy polling sleep is clamped to the remaining budget", {
  slept <- numeric()
  now <- as.POSIXct("2026-01-01 00:00:00", tz = "UTC")
  remaining <- fabric_livy_poll_sleep(
    now + 2,
    poll_interval = 10,
    .now = function() now,
    .sleep = function(seconds) {
      slept <<- c(slept, seconds)
      invisible(NULL)
    }
  )
  expect_equal(remaining, 2)
  expect_equal(slept, 2)
})

test_that("Livy input and endpoint validation is explicit", {
  expect_null(fabric_livy_normalize_named_list(list(), "tags"))
  expect_null(fabric_livy_conf(list()))
  expect_equal(
    fabric_livy_endpoint(
      "https://example.test/base/sessions/",
      "batches"
    ),
    "https://example.test/base/batches"
  )
  expect_equal(
    fabric_livy_endpoint(
      "https://example.test/base/batches",
      "highConcurrencySessions"
    ),
    "https://example.test/base/highConcurrencySessions"
  )
  expect_error(
    fabric_livy_endpoint("http://api.fabric.microsoft.com/livy", "sessions"),
    "valid HTTPS endpoint"
  )
  for (url in c(
    "https://api.fabric.microsoft.com/livy?token=value",
    "https://api.fabric.microsoft.com/livy#sessions",
    "https://user@api.fabric.microsoft.com/livy"
  )) {
    expect_error(
      fabric_livy_endpoint(url, "sessions"),
      "must not contain",
      fixed = TRUE
    )
  }
  expect_error(
    fabric_livy_endpoint(
      "https://api.fabric.microsoft.com:8443/livy",
      "sessions"
    ),
    "default port",
    fixed = TRUE
  )
  expect_equal(
    fabric_livy_endpoint(
      "https://example.test:8443/livy",
      "sessions"
    ),
    "https://example.test:8443/livy/sessions"
  )
  expect_error(
    fabric_livy_session(
      "https://example.test/base",
      session_tag = "not-hc",
      token = "token"
    ),
    "only available"
  )
  expect_error(
    fabric_livy_session(
      "https://example.test/base",
      tags = list("missing name"),
      token = "token"
    ),
    "uniquely named list"
  )
  expect_error(
    fabric_livy_session(
      "https://example.test/base",
      conf = list("spark.setting" = 1),
      token = "token"
    ),
    "single, non-missing strings"
  )
  expect_error(
    fabric_livy_session(
      "https://example.test/base",
      archives = list("archive.zip"),
      token = "token"
    ),
    "character vector"
  )
  expect_error(
    fabric_livy_session(
      "https://example.test/base",
      driver_cores = 1.5,
      token = "token"
    ),
    "whole number"
  )
  expect_error(
    fabric_livy_batch_submit(
      "https://example.test/base",
      file = livy_test_application_uri,
      tags = list(run = NA_character_),
      token = "token"
    ),
    "non-missing strings"
  )
  expect_error(
    fabric_livy_batch_submit(
      "https://example.test/base",
      file = "",
      token = "token"
    ),
    "file must"
  )
  auth_calls <- 0L
  token <- function(...) {
    auth_calls <<- auth_calls + 1L
    stop("must not authenticate")
  }
  expect_error(
    fabric_livy_session(
      "https://api.fabric.microsoft.com/livy",
      environment_id = "not-an-environment-guid",
      token = token
    ),
    "environment_id must be a GUID",
    fixed = TRUE
  )
  expect_error(
    fabric_livy_batch_submit(
      "https://api.fabric.microsoft.com/livy",
      file = livy_test_application_uri,
      target_lakehouse_id = "../not-a-lakehouse-guid",
      token = token
    ),
    "target_lakehouse_id must be a GUID",
    fixed = TRUE
  )
  expect_equal(auth_calls, 0L)
})

test_that("Livy batch application paths are safe absolute ABFS URIs", {
  expect_invisible(
    fabric_livy_validate_abfs_uri(livy_test_application_uri, "file")
  )
  expect_invisible(
    fabric_livy_validate_abfs_uri(
      "ABFS://container@account.dfs.core.windows.net/jobs/main.py",
      "file"
    )
  )

  invalid_uris <- c(
    "job.py",
    "https://account.example/jobs/main.py",
    "abfss://account.example/jobs/main.py",
    "abfss://@account.example/jobs/main.py",
    "abfss://container@/jobs/main.py",
    "abfss://container:secret@account.example/jobs/main.py",
    "abfss://container:@account.example/jobs/main.py",
    "abfss://container@account.example:443/jobs/main.py",
    "abfss://container@account.example/",
    "abfss://container@account.example/jobs/main.py?sig=secret",
    "abfss://container@account.example/jobs/main.py#fragment",
    "abfss://container@account.example/jobs/../main.py",
    "abfss://container@account.example/jobs/%2e%2e/main.py",
    "abfss://container@account.example/jobs\\main.py",
    "abfss://container@account.example/jobs/main file.py",
    "abfss://container@account.example/jobs/main%20file.py",
    "abfss://container@account.example/jobs/main%00.py",
    "abfss://container@account.example/jobs/main%ZZ.py"
  )
  resolve_calls <- 0L
  local_mocked_bindings(
    fabric_livy_resolve_url = function(...) {
      resolve_calls <<- resolve_calls + 1L
      stop("must not resolve an endpoint")
    }
  )

  for (uri in invalid_uris) {
    error <- rlang::catch_cnd(
      fabric_livy_batch_submit(
        "https://api.fabric.microsoft.com/livy",
        file = uri,
        token = "token",
        verbose = FALSE
      ),
      classes = "error"
    )
    expect_true(inherits(error, "fabric_livy_abfs_uri_error"), info = uri)
    expect_false(grepl(uri, conditionMessage(error), fixed = TRUE), info = uri)
  }
  expect_identical(resolve_calls, 0L)
})

test_that("Livy wait arguments are validated before remote side effects", {
  calls <- 0L
  local_mocked_bindings(
    fabric_livy_json = function(...) {
      calls <<- calls + 1L
      list(id = "created", state = "idle")
    }
  )

  expect_error(
    fabric_livy_query(
      "https://example.test/livy/sessions",
      code = "print(1)",
      timeout = NA_real_,
      token = "token"
    ),
    "timeout"
  )
  expect_identical(calls, 0L)

  expect_error(
    fabric_livy_batch_submit(
      "https://example.test/livy/batches",
      file = livy_test_application_uri,
      wait = TRUE,
      poll_interval = -1,
      token = "token"
    ),
    "poll_interval"
  )
  expect_identical(calls, 0L)
})

test_that("session run validates polling before submitting a statement", {
  posts <- 0L
  local_mocked_bindings(
    fabric_livy_json = function(method, ...) {
      if (method == "POST") {
        posts <<- posts + 1L
      }
      list(id = "session", state = "idle")
    },
    fabric_livy_ok = function(...) TRUE
  )
  session <- fabric_livy_session(
    "https://example.test/livy/sessions",
    token = "token",
    verbose = FALSE
  )
  expect_identical(posts, 1L)

  expect_error(
    session$run("print(1)", timeout = Inf),
    "timeout"
  )
  expect_identical(posts, 1L)
  session$close()
})

test_that("batch result validates error_on_failure before refresh", {
  gets <- 0L
  local_mocked_bindings(
    fabric_livy_json = function(method, ...) {
      if (method == "POST") {
        list(id = "batch", state = "success")
      } else {
        gets <<- gets + 1L
        list(id = "batch", state = "success")
      }
    }
  )
  batch <- fabric_livy_batch_submit(
    "https://example.test/livy/batches",
    file = livy_test_application_uri,
    token = "token",
    verbose = FALSE
  )

  expect_error(batch$result(error_on_failure = NA), "must be TRUE or FALSE")
  expect_identical(gets, 0L)
})

test_that("Livy result methods latch terminal completion times", {
  credential <- livy_test_credential()
  batch <- FabricLivyBatch$new(
    response = list(id = "batch", state = "success", result = "Succeeded"),
    url = "https://example.test/batches",
    credential = credential,
    verbose = FALSE
  )
  first_batch <- batch$result(refresh = FALSE)
  second_batch <- batch$result(refresh = FALSE)
  expect_s3_class(batch$completed_local, "POSIXct")
  expect_identical(first_batch$completed_local, second_batch$completed_local)

  statement <- FabricLivyStatement$new(
    session = new.env(parent = emptyenv()),
    response = list(
      id = 1L,
      state = "available",
      output = list(status = "ok", data = list())
    ),
    url = "https://example.test/statements/1",
    credential = credential,
    verbose = FALSE
  )
  first_statement <- statement$result(refresh = FALSE)
  second_statement <- statement$result(refresh = FALSE)
  expect_s3_class(statement$completed_local, "POSIXct")
  expect_identical(
    first_statement$completed_local,
    second_statement$completed_local
  )
  expect_identical(
    first_statement$duration_sec,
    second_statement$duration_sec
  )
})

test_that("Livy handles print concise summaries without credentials", {
  local_mocked_bindings(
    fabric_livy_json = function(...) list(id = "session", state = "idle")
  )
  credential <- fabric_credential(token = "never-print-this-token")
  session <- fabric_livy_session(
    "https://example.test/livy/sessions",
    token = "never-print-this-token",
    verbose = FALSE
  )
  statement <- FabricLivyStatement$new(
    session = session,
    response = list(id = 7L, state = "waiting"),
    url = "https://example.test/livy/sessions/session/statements/7",
    credential = credential,
    verbose = FALSE
  )
  batch <- FabricLivyBatch$new(
    response = list(id = "batch", state = "running"),
    url = "https://example.test/livy/batches",
    credential = credential,
    verbose = FALSE
  )

  capture_invisible_print <- function(value) {
    capture.output(expect_invisible(print(value)))
  }
  session_text <- capture_invisible_print(session)
  statement_text <- capture_invisible_print(statement)
  batch_text <- capture_invisible_print(batch)
  expect_match(paste(session_text, collapse = "\n"), "state: idle")
  expect_match(paste(statement_text, collapse = "\n"), "id: 7")
  expect_match(paste(batch_text, collapse = "\n"), "cancel requested: FALSE")
  expect_false(any(grepl(
    "never-print-this-token",
    c(session_text, statement_text, batch_text),
    fixed = TRUE
  )))
})

test_that("session waits stop on all documented terminal states", {
  check_terminal <- function(
    initial_state,
    result = NULL,
    high_concurrency = FALSE
  ) {
    responses <- list(
      list(id = "terminal-session", state = "starting"),
      list(
        id = "terminal-session",
        state = initial_state,
        result = result
      )
    )
    local_mocked_bindings(
      fabric_livy_json = function(...) {
        response <- responses[[1L]]
        responses <<- responses[-1L]
        response
      },
      fabric_livy_ok = function(...) TRUE
    )
    session <- fabric_livy_session(
      "https://example.test/livy",
      high_concurrency = high_concurrency,
      token = "token",
      verbose = FALSE
    )
    expect_error(
      session$wait(timeout = 1, poll_interval = 0),
      class = "fabric_livy_session_error"
    )
    session$close()
  }

  check_terminal("success")
  check_terminal("Deleting", high_concurrency = TRUE)
  check_terminal("starting", result = "Failed")
  check_terminal("unrecognized", result = "Cancelled")
})

test_that("session wait continues through an Uncertain intermediate result", {
  responses <- list(
    list(id = "uncertain-session", state = "starting"),
    list(id = "uncertain-session", state = "running", result = "Uncertain"),
    list(id = "uncertain-session", state = "idle", result = "Uncertain")
  )
  local_mocked_bindings(
    fabric_livy_json = function(...) {
      response <- responses[[1L]]
      responses <<- responses[-1L]
      response
    },
    fabric_livy_ok = function(...) TRUE
  )
  session <- fabric_livy_session(
    "https://example.test/livy",
    token = "token",
    verbose = FALSE
  )

  expect_invisible(session$wait(timeout = 1, poll_interval = 0))
  expect_identical(session$state, "idle")
  expect_length(responses, 0L)
  session$close()
})
