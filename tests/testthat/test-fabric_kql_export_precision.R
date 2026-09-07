test_that("Parquet export refuses decimal schemas before any write", {
  calls <- list()
  response <- kql_export_schema_response("decimal")
  local_mocked_bindings(.httr2_perform = function(req, ...) {
    calls[[length(calls) + 1L]] <<- req
    response
  })
  query <- "let source=datatable(value:decimal)[]; source; // trailing comment"
  export <- function() {
    fabric_kql_export(
      "https://cluster.z1.kusto.fabric.microsoft.com",
      query,
      kusto_export_test_target(),
      database = "Telemetry",
      path = "Files/decimal",
      token = "token"
    )
  }
  error <- rlang::catch_cnd(export())
  expect_s3_class(error, "fabric_kql_export_decimal_error")
  expect_identical(error$columns, "value1")
  expect_identical(length(calls), 1L)
  expect_identical(calls[[1L]]$body$data$csl, query)
  properties <- jsonlite::fromJSON(calls[[1L]]$body$data$properties)
  expect_identical(properties$Options$query_results_apply_getschema, TRUE)
  expect_snapshot(export(), error = TRUE)

  response <- kql_export_schema_response("decimal", second = "string")
  expect_s3_class(rlang::catch_cnd(export()), "fabric_kql_export_decimal_error")
  response <- kql_export_schema_response("decimal", alter = function(frames) {
    frames[[1L]]$IsProgressive <- TRUE
    rows <- frames[[2L]]$Rows
    frames[[2L]]$Rows <- NULL
    frames[[2L]]$FrameType <- "TableHeader"
    append(
      frames,
      list(
        list(
          FrameType = "TableFragment",
          TableId = 0L,
          FieldCount = 4L,
          TableFragmentType = "DataAppend",
          Rows = rows
        ),
        list(FrameType = "TableCompletion", TableId = 0L, RowCount = 1L)
      ),
      after = 2L
    )
  })
  expect_s3_class(rlang::catch_cnd(export()), "fabric_kql_export_decimal_error")
})

test_that("Parquet export fails closed on missing or unrecognized output schemas", {
  responses <- list(
    kql_export_schema_response(character()),
    kql_export_schema_response("new_decimal_type"),
    kusto_export_test_response(kusto_export_test_operation()),
    kql_export_schema_response("string", alter = function(frames) {
      frames[[2L]]$Rows[[1L]][[2L]] <- 1L
      frames
    }),
    kql_export_schema_response("string", alter = function(frames) {
      frames[[2L]]$Rows[[1L]][[3L]] <- "System.Int64"
      frames
    }),
    kql_export_schema_response("string", alter = function(frames) {
      frames[[2L]]$Columns <- c(
        frames[[2L]]$Columns,
        list(list(
          ColumnName = "ColumnType",
          ColumnType = "string"
        ))
      )
      frames[[2L]]$Rows[[1L]] <- c(frames[[2L]]$Rows[[1L]], list("decimal"))
      frames
    }),
    kql_export_schema_response("string", alter = function(frames) {
      frames[[length(frames)]]$HasErrors <- TRUE
      frames
    }),
    kql_export_schema_response("string", alter = function(frames) {
      frames[[length(frames)]]$Cancelled <- TRUE
      frames
    }),
    kql_export_schema_response("string", alter = function(frames) {
      frames[[2L]]$Columns <- c(
        frames[[2L]]$Columns,
        list(list(
          ColumnName = "Unexpected",
          ColumnType = "string"
        ))
      )
      frames[[2L]]$Rows[[1L]] <- c(frames[[2L]]$Rows[[1L]], list("decimal"))
      frames
    }),
    errorCondition("Schema transport failed")
  )
  calls <- 0L
  response <- NULL
  local_mocked_bindings(.httr2_perform = function(req, ...) {
    calls <<- calls + 1L
    if (inherits(response, "error")) {
      stop(response)
    }
    response
  })
  for (value in responses) {
    response <- value
    error <- rlang::catch_cnd(fabric_kql_export(
      "https://cluster.z1.kusto.fabric.microsoft.com",
      "Events",
      kusto_export_test_target(),
      database = "Telemetry",
      path = "Files/schema",
      token = "token"
    ))
    expect_s3_class(error, "fabric_kql_export_schema_error")
  }
  expect_identical(calls, length(responses))
})

test_that("Parquet schema preflight shares the complete export deadline", {
  now <- as.POSIXct("2026-09-06 12:00:00", tz = "UTC")
  started <- now
  calls <- list()
  local_mocked_bindings(.httr2_perform = function(req, deadline, ...) {
    calls[[length(calls) + 1L]] <<- list(req = req, deadline = deadline)
    if (endsWith(req$url, "/v2/rest/query")) {
      now <<- now + 2
      return(kql_export_schema_response(
        c(
          "bool",
          "datetime",
          "dynamic",
          "guid",
          "int",
          "long",
          "real",
          "string",
          "timespan"
        ),
        second = "decimal"
      ))
    }
    if (startsWith(req$body$data$csl, ".export")) {
      return(kusto_export_test_response(kusto_export_test_operation()))
    }
    if (grepl(" details$", req$body$data$csl)) {
      return(kusto_export_test_response(kusto_export_test_table(
        c(Path = "String", NumRecords = "Long"),
        list(list(
          "https://storageacct.blob.core.windows.net/container/part.parquet",
          "1"
        ))
      )))
    }
    kusto_export_test_response(kusto_export_test_status("Completed"))
  })
  result <- fabric_kql_export(
    "https://cluster.z1.kusto.fabric.microsoft.com",
    "print value='text'",
    kusto_export_test_target(),
    database = "Telemetry",
    path = "Files/schema",
    timeout = 10,
    token = "token",
    .now = function() now
  )
  expect_identical(result$numeric_policy, "exact")
  expect_identical(length(calls), 4L)
  expect_identical(
    vapply(calls, function(call) as.numeric(call$deadline), numeric(1)),
    rep(as.numeric(started + 10), 4L)
  )
  expect_identical(calls[[1L]]$req$options$timeout_ms, 10000)
  expect_match(
    calls[[2L]]$req$body$data$csl,
    "print value='text'",
    fixed = TRUE
  )

  calls <- list()
  error <- rlang::catch_cnd(fabric_kql_export(
    "https://cluster.z1.kusto.fabric.microsoft.com",
    "Events",
    kusto_export_test_target(),
    database = "Telemetry",
    path = "Files/expired",
    timeout = 1,
    token = "token",
    .now = function() now
  ))
  expect_s3_class(error, "fabric_kql_export_schema_error")
  expect_identical(length(calls), 1L)
})

test_that("item export forwards explicit policy and text formats retain service behavior", {
  commands <- character()
  local_mocked_bindings(.httr2_perform = function(req, ...) {
    commands <<- c(commands, req$body$data$csl)
    if (endsWith(req$url, "/v2/rest/query")) {
      return(kql_export_schema_response("decimal"))
    }
    if (startsWith(req$body$data$csl, ".export")) {
      return(kusto_export_test_response(kusto_export_test_operation()))
    }
    if (grepl(" details$", req$body$data$csl)) {
      return(kusto_export_test_response(kusto_export_test_table(
        c(Path = "String", NumRecords = "Long"),
        list(list(
          "https://storageacct.blob.core.windows.net/container/part",
          "1"
        ))
      )))
    }
    kusto_export_test_response(kusto_export_test_status("Completed"))
  })
  for (type in c("Eventhouse", "KQLDatabase")) {
    item <- fabric_r6_record(
      list(
        id = "33333333-3333-4333-8333-333333333333",
        workspaceId = "22222222-2222-4222-8222-222222222222",
        displayName = "Telemetry",
        type = type,
        query_service_uri = "https://cluster.z1.kusto.fabric.microsoft.com"
      ),
      legacy_class = c("fabric_item", "list"),
      credential = fabric_credential(token = "token")
    )
    error <- rlang::catch_cnd(item$export(
      "print value=decimal(1.25)",
      kusto_export_test_target(),
      database = "Telemetry",
      path = "Files/decimal"
    ))
    expect_s3_class(error, "fabric_kql_export_decimal_error")
    for (format in c("parquet", "csv", "tsv", "json")) {
      commands <- character()
      policy <- if (format == "parquet") "service" else "exact"
      result <- item$export(
        "print value=decimal(1.25)",
        kusto_export_test_target(),
        database = "Telemetry",
        path = "Files/decimal",
        format = format,
        numeric_policy = policy
      )
      expect_identical(result$numeric_policy, policy)
      expect_identical(result$state, "Completed")
      expect_identical(length(commands), 3L)
      expect_identical(sum(startsWith(commands, ".export async")), 1L)
    }
  }
})
