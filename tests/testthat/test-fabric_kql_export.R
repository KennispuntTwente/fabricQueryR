test_that("KQL export submits once, polls, and returns authoritative details", {
  calls <- list()
  responses <- list(
    kusto_export_test_response(
      kusto_export_test_operation(),
      request_id = "submit-request"
    ),
    kusto_export_test_response(kusto_export_test_status("InProgress")),
    kusto_export_test_response(kusto_export_test_status("Completed", "Done")),
    kusto_export_test_response(kusto_export_test_table(
      c(Path = "String", NumRecords = "Long"),
      list(
        list(
          paste0(
            "https://onelake.dfs.fabric.microsoft.com/bbbbbbbb-bbbb-",
            "4bbb-8bbb-bbbbbbbbbbbb/aaaaaaaa-aaaa-4aaa-8aaa-",
            "aaaaaaaaaaaa/Files/exports/part-1.parquet"
          ),
          "2"
        ),
        list(
          paste0(
            "https://onelake.dfs.fabric.microsoft.com/bbbbbbbb-bbbb-",
            "4bbb-8bbb-bbbbbbbbbbbb/aaaaaaaa-aaaa-4aaa-8aaa-",
            "aaaaaaaaaaaa/Files/exports/part-2.parquet"
          ),
          "1"
        )
      )
    ))
  )
  local_mocked_bindings(
    .httr2_perform = function(
      req,
      credential,
      audience,
      idempotent,
      deadline,
      ...
    ) {
      index <- length(calls) + 1L
      calls[[index]] <<- list(
        request = req,
        credential = credential,
        audience = audience,
        idempotent = idempotent,
        deadline = deadline
      )
      responses[[index]]
    }
  )
  now <- as.POSIXct("2026-08-14 10:00:00", tz = "UTC")
  result <- fabric_kql_export(
    list(
      id = "database-id",
      type = "KQLDatabase",
      displayName = "Telemetry",
      query_service_uri = paste0(
        "https://cluster.z1.kusto.fabric.microsoft.com/V2/REST/QUERY"
      )
    ),
    query = "Events | project id, observed_at",
    destination = kusto_export_test_target(),
    path = "Files/exports",
    name_prefix = 'events"daily',
    compression_type = "snappy",
    parquet_row_group_size = 50000,
    parquet_datetime_precision = "microsecond",
    token = "kusto-token",
    poll_interval = 0.1,
    .now = function() now,
    .sleep = function(seconds) {
      now <<- now + seconds
    }
  )

  expect_s3_class(result, "fabric_kql_export_result")
  expect_equal(result$operation_id, kusto_export_test_id)
  expect_equal(result$state, "Completed")
  expect_equal(result$file_count, 2L)
  expect_equal(as.numeric(result$artifacts$num_records), c(2, 1))
  expect_equal(as.numeric(result$records), 3)
  expect_equal(result$request_id, "submit-request")
  expect_equal(length(calls), 4L)
  expect_identical(
    vapply(calls, `[[`, logical(1), "idempotent"),
    c(FALSE, TRUE, TRUE, TRUE)
  )
  expect_true(all(vapply(
    calls,
    function(call) {
      identical(
        call$audience,
        "https://api.kusto.windows.net/.default"
      )
    },
    logical(1)
  )))
  expect_true(all(vapply(
    calls,
    function(call) {
      identical(
        call$request$url,
        "https://cluster.z1.kusto.fabric.microsoft.com/v1/rest/mgmt"
      )
    },
    logical(1)
  )))
  command <- calls[[1L]]$request$body$data$csl
  expect_match(command, "^.export async compressed to parquet", perl = TRUE)
  expect_match(command, "h@\"https://onelake", fixed = TRUE)
  expect_match(command, "/Files/exports;impersonate", fixed = TRUE)
  expect_match(command, 'namePrefix=@"events""daily"', fixed = TRUE)
  expect_match(command, "compressionType=@\"snappy\"", fixed = TRUE)
  expect_match(command, "persistDetails=true", fixed = TRUE)
  expect_match(command, "parquetRowGroupSize=50000", fixed = TRUE)
  expect_match(
    command,
    "parquetDatetimePrecision=@\"microsecond\"",
    fixed = TRUE
  )
  expect_match(command, "<| Events | project id, observed_at", fixed = TRUE)
  expect_null(calls[[1L]]$request$headers[["x-ms-readonly"]])
  expect_match(
    calls[[2L]]$request$body$data$csl,
    paste(".show operations", kusto_export_test_id),
    fixed = TRUE
  )
  expect_match(
    calls[[4L]]$request$body$data$csl,
    paste(".show operation", kusto_export_test_id, "details"),
    fixed = TRUE
  )
})

test_that("KQL export fails safely without accepting partial artifacts", {
  calls <- list()
  secret <- "storage-export-secret"
  responses <- list(
    kusto_export_test_response(kusto_export_test_operation()),
    kusto_export_test_response(kusto_export_test_status(
      "PartiallySucceeded",
      paste0(
        "write failed at https://storage.test/container?sig=",
        secret
      )
    ))
  )
  local_mocked_bindings(
    .httr2_perform = function(req, idempotent, ...) {
      index <- length(calls) + 1L
      calls[[index]] <<- list(request = req, idempotent = idempotent)
      responses[[index]]
    }
  )
  error <- tryCatch(
    fabric_kql_export(
      "https://cluster.z1.kusto.fabric.microsoft.com",
      query = "Events",
      database = "Telemetry",
      destination = paste0(
        "https://storage.test/container/export?sig=",
        secret
      ),
      token = "token"
    ),
    error = identity
  )

  expect_s3_class(error, "fabric_kql_export_failure")
  expect_equal(error$operation_id, kusto_export_test_id)
  expect_equal(error$last_status$state, "PartiallySucceeded")
  expect_false(grepl(secret, error$destination, fixed = TRUE))
  expect_false(grepl(secret, error$last_status$status, fixed = TRUE))
  expect_match(conditionMessage(error), "may be incomplete", fixed = TRUE)
  expect_length(calls, 2L)
  expect_identical(
    vapply(calls, `[[`, logical(1), "idempotent"),
    c(FALSE, TRUE)
  )
})

test_that("KQL export timeout retains its operation ID without replay", {
  calls <- 0L
  commands <- character()
  local_mocked_bindings(
    .httr2_perform = function(req, ...) {
      calls <<- calls + 1L
      commands[[calls]] <<- req$body$data$csl
      if (calls == 1L) {
        kusto_export_test_response(kusto_export_test_operation())
      } else {
        kusto_export_test_response(kusto_export_test_status("InProgress"))
      }
    }
  )
  now <- as.POSIXct("2026-08-14 10:00:00", tz = "UTC")
  error <- tryCatch(
    fabric_kql_export(
      "https://cluster.z1.kusto.fabric.microsoft.com",
      query = "Events",
      database = "Telemetry",
      destination = kusto_export_test_target(),
      path = "Files/timeout",
      timeout = 0.2,
      poll_interval = 0.1,
      token = "token",
      .now = function() now,
      .sleep = function(seconds) {
        now <<- now + seconds
      }
    ),
    error = identity
  )

  expect_s3_class(error, "fabric_kql_export_timeout")
  expect_equal(error$operation_id, kusto_export_test_id)
  expect_equal(error$last_status$state, "InProgress")
  expect_gte(calls, 3L)
  expect_equal(sum(startsWith(commands, ".export async")), 1L)
  expect_match(conditionMessage(error), "may still be running", fixed = TRUE)
})

test_that("KQL export distinguishes a completed artifact-details timeout", {
  calls <- 0L
  commands <- character()
  local_mocked_bindings(
    .httr2_perform = function(req, ...) {
      calls <<- calls + 1L
      commands[[calls]] <<- req$body$data$csl
      if (calls == 1L) {
        return(kusto_export_test_response(kusto_export_test_operation()))
      }
      if (calls == 2L) {
        return(kusto_export_test_response(
          kusto_export_test_status("Completed", "Done")
        ))
      }
      rlang::abort(
        "request deadline exhausted",
        class = "fabric_http_deadline_error"
      )
    }
  )

  error <- rlang::catch_cnd(
    fabric_kql_export(
      "https://cluster.z1.kusto.fabric.microsoft.com",
      query = "Events",
      database = "Telemetry",
      destination = kusto_export_test_target(),
      path = "Files/details-timeout",
      token = "token"
    ),
    classes = "error"
  )

  expect_s3_class(error, "fabric_kql_export_details_timeout")
  expect_s3_class(error, "fabric_kql_export_details_error")
  expect_false(inherits(error, "fabric_kql_export_timeout"))
  expect_true(error$operation_completed)
  expect_identical(error$operation_id, kusto_export_test_id)
  expect_identical(error$last_status$state, "Completed")
  expect_identical(error$format, "parquet")
  expect_identical(calls, 3L)
  expect_identical(sum(startsWith(commands, ".export async")), 1L)
  expect_match(conditionMessage(error), "completed, but its", fixed = TRUE)
})

test_that("KQL export treats a missing tracking ID as ambiguous", {
  calls <- 0L
  local_mocked_bindings(
    .httr2_perform = function(req, ...) {
      calls <<- calls + 1L
      kusto_export_test_response(kusto_export_test_table(
        c(Unexpected = "String"),
        list(list("value"))
      ))
    }
  )
  error <- tryCatch(
    fabric_kql_export(
      "https://cluster.z1.kusto.fabric.microsoft.com",
      query = "Events",
      database = "Telemetry",
      destination = kusto_export_test_target(),
      path = "Files/ambiguous",
      token = "token"
    ),
    error = identity
  )

  expect_s3_class(error, "fabric_kql_export_submission_error")
  expect_equal(calls, 1L)
  expect_match(conditionMessage(error), "not replayed", fixed = TRUE)
})

test_that("KQL export submission errors redact storage credentials", {
  secrets <- c("BARE_ACCOUNT_KEY_123", "SAS_SIGNATURE_456")
  locations <- c(
    paste0("https://storage.test/container/export;", secrets[[1L]]),
    paste0("https://storage.test/container/export?sv=1&sig=", secrets[[2L]])
  )
  httr2::local_mocked_responses(function(req) {
    kusto_ingestion_test_response(
      list(error = list(code = "BadRequest", message = paste(locations))),
      status = 400L,
      url = req$url
    )
  })

  error <- tryCatch(
    fabric_kql_export(
      "https://cluster.z1.kusto.fabric.microsoft.com",
      query = "Events",
      database = "Telemetry",
      destination = locations[[1L]],
      token = "token"
    ),
    error = identity
  )

  exposed <- c(
    conditionMessage(error),
    conditionMessage(error$parent),
    jsonlite::toJSON(error$response_metadata, auto_unbox = TRUE),
    rawToChar(serialize(error, NULL, ascii = TRUE))
  )
  expect_s3_class(error, "fabric_kql_export_submission_error")
  exposed_secret <- vapply(
    secrets,
    function(secret) any(grepl(secret, exposed, fixed = TRUE)),
    logical(1)
  )
  expect_false(any(exposed_secret))
  expect_match(paste(exposed, collapse = " "), "<redacted>", fixed = TRUE)
})

test_that("KQL export validates destinations and format-specific properties", {
  onelake_base <- paste0(
    "https://onelake.dfs.fabric.microsoft.com/",
    "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb/",
    "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa/"
  )
  expect_error(
    kusto_export_destination(
      kusto_export_test_target(),
      path = "Tables/not-safe"
    ),
    "below Tables",
    fixed = TRUE
  )
  unsafe_onelake <- c(
    paste0(
      onelake_base,
      "Tables/dbo/not-safe;impersonate"
    ),
    paste0(
      onelake_base,
      "Tables/dbo/not-safe;managed_identity=client-id"
    ),
    paste0(
      onelake_base,
      "Tables/dbo/not-safe?sv=1&sig=secret"
    ),
    paste0(
      "abfss://bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb@",
      "onelake.dfs.fabric.microsoft.com/",
      "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa/",
      "Tables/dbo/not-safe;impersonate"
    ),
    paste0(
      "https://westeurope-onelake.dfs.fabric.microsoft.com/",
      "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb/",
      "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa/",
      "Tables/dbo/not-safe"
    ),
    paste0(
      "https://westeurope-api.onelake.fabric.microsoft.com/",
      "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb/",
      "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa/",
      "Tables/dbo/not-safe"
    ),
    paste0(
      "https://bbbbbbbbbbbb4bbb8bbbbbbbbbbbbbbb.z12.",
      "blob.fabric.microsoft.com/",
      "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa/Tables/dbo/not-safe"
    )
  )
  for (destination in unsafe_onelake) {
    expect_error(
      kusto_export_destination(destination),
      "below Tables",
      fixed = TRUE
    )
  }
  expect_error(
    kusto_export_destination(c(
      paste0(
        onelake_base,
        "Files/safe;impersonate"
      ),
      unsafe_onelake[[1L]]
    )),
    "below Tables",
    fixed = TRUE
  )
  safe_onelake <- paste0(
    onelake_base,
    "Files/export;managed_identity=client-id"
  )
  safe <- kusto_export_destination(safe_onelake)
  expect_match(
    safe$connection,
    "Files/export;managed_identity=client-id",
    fixed = TRUE
  )
  regional_safe <- kusto_export_destination(paste0(
    "https://westeurope-onelake.dfs.fabric.microsoft.com/",
    "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb/",
    "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa/Files/export"
  ))
  expect_match(regional_safe$connection, ";impersonate$", perl = TRUE)
  workspace_safe <- kusto_export_destination(paste0(
    "https://bbbbbbbbbbbb4bbb8bbbbbbbbbbbbbbb.z12.",
    "blob.fabric.microsoft.com/",
    "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa/Files/export"
  ))
  expect_match(workspace_safe$connection, ";impersonate$", perl = TRUE)
  expect_match(
    workspace_safe$connection,
    ".z12.dfs.fabric.microsoft.com/",
    fixed = TRUE
  )
  expect_error(
    kusto_export_destination(
      "https://onelake.dfs.fabric.microsoft.com/workspace/item/Files/export",
      path = "Files/other"
    ),
    "must be omitted",
    fixed = TRUE
  )
  expect_error(
    kusto_export_properties(
      "parquet",
      TRUE,
      "all",
      NULL,
      NULL,
      NULL,
      NULL,
      "per_shard",
      100e6,
      NULL,
      NULL
    ),
    "only for csv or tsv",
    fixed = TRUE
  )
  expect_error(
    kusto_export_properties(
      "csv",
      FALSE,
      NULL,
      NULL,
      NULL,
      NULL,
      "gzip",
      "single",
      100e6,
      NULL,
      NULL
    ),
    "requires compressed",
    fixed = TRUE
  )
  expect_error(
    kusto_export_properties(
      "parquet",
      TRUE,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      "per_shard",
      100e6 - 1,
      NULL,
      NULL
    ),
    "size_limit",
    fixed = TRUE
  )
  expect_equal(
    kusto_export_literal('a"b', hidden = TRUE),
    'h@"a""b"'
  )
  unnamed_secret <- "an-unnamed-storage-access-key"
  destination <- kusto_export_destination(paste0(
    "https://storage.test/container/export;",
    unnamed_secret
  ))
  expect_false(grepl(unnamed_secret, destination$display, fixed = TRUE))
  expect_equal(destination$display, "https://storage.test/container/export")
  multiple <- kusto_export_destination(c(
    "https://storage-one.test/container/export;impersonate",
    "https://storage-two.test/container/export;impersonate"
  ))
  expect_length(multiple$connection, 2L)
  command <- kusto_export_command(
    "Events | take 1",
    multiple$connection,
    kusto_export_properties(
      "parquet",
      TRUE,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      "per_shard",
      100 * 1024^2,
      NULL,
      NULL
    )
  )
  expect_match(
    command,
    paste0(
      'h@"https://storage-one.test/container/export;impersonate", ',
      'h@"https://storage-two.test/container/export;impersonate"'
    ),
    fixed = TRUE
  )
  expect_error(
    kusto_export_destination(
      c("https://storage.test/one", "relative/two")
    ),
    "must all be complete",
    fixed = TRUE
  )
  status <- kusto_export_status_text(paste0(
    "failed at https://storage.test/container;",
    unnamed_secret
  ))
  expect_false(grepl(unnamed_secret, status, fixed = TRUE))
  expect_match(status, "<redacted>", fixed = TRUE)
})

test_that("KQL export uses Fabric's decimal file-size boundaries", {
  properties <- function(size_limit) {
    kusto_export_properties(
      format = "parquet",
      compressed = TRUE,
      include_headers = NULL,
      name_prefix = NULL,
      file_extension = NULL,
      encoding = NULL,
      compression_type = NULL,
      distribution = "per_shard",
      size_limit = size_limit,
      parquet_row_group_size = NULL,
      parquet_datetime_precision = NULL
    )
  }

  expect_identical(properties(100e6)$sizeLimit, 100e6)
  expect_identical(properties(4e9)$sizeLimit, 4e9)
  expect_error(properties(100e6 - 1), "size_limit")
  expect_error(properties(4e9 + 1), "4 GB", fixed = TRUE)
  expect_error(properties(4 * 1024^3), "4 GB", fixed = TRUE)
})
