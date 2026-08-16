test_that("queued-ingestion targets use discovered ingestion coordinates", {
  target <- kusto_resolve_ingestion_target(
    "https://ingest-cluster.kusto.fabric.microsoft.com/",
    "Telemetry DB",
    "Raw Events"
  )
  expect_equal(
    target$url,
    "https://ingest-cluster.kusto.fabric.microsoft.com"
  )
  expect_equal(
    kusto_ingestion_url(target),
    paste0(
      "https://ingest-cluster.kusto.fabric.microsoft.com/",
      "v1/rest/ingestion/queued/Telemetry%20DB/Raw%20Events"
    )
  )
  expect_equal(
    kusto_ingestion_url(target, "operation;with/slash"),
    paste0(
      "https://ingest-cluster.kusto.fabric.microsoft.com/",
      "v1/rest/ingestion/queued/Telemetry%20DB/Raw%20Events/",
      "operation%3Bwith%2Fslash"
    )
  )

  discovered <- kusto_resolve_ingestion_target(
    list(
      id = "database-id",
      type = "KQLDatabase",
      displayName = "Events",
      ingestion_service_uri = paste0(
        "https://ingest-cluster.kusto.fabric.microsoft.com"
      )
    ),
    database = NULL,
    table = "Raw"
  )
  expect_equal(discovered$database, "Events")

  expect_error(
    kusto_resolve_ingestion_target(
      list(type = "KQLDatabase", displayName = "Events"),
      NULL,
      "Raw"
    ),
    "cluster must be",
    fixed = TRUE
  )
  expect_error(
    kusto_resolve_ingestion_target(
      "https://attacker.example",
      "Events",
      "Raw"
    ),
    "Microsoft Kusto ingestion endpoint",
    fixed = TRUE
  )
  expect_equal(
    kusto_resolve_ingestion_target(
      "https://trusted.example",
      "Events",
      "Raw",
      allow_custom_endpoint = TRUE
    )$url,
    "https://trusted.example"
  )
  expect_error(
    kusto_resolve_ingestion_target(
      "https://ingest-cluster.kusto.fabric.microsoft.com/v1/rest/query",
      "Events",
      "Raw"
    ),
    "ingestion-service origin",
    fixed = TRUE
  )
})

test_that("source metadata validates URLs, GUIDs, sizes, and batch limits", {
  ids <- c(
    "11111111-1111-4111-8111-111111111111",
    "22222222-2222-4222-8222-222222222222"
  )
  records <- kusto_ingestion_sources(
    c(
      paste0(
        "https://onelake.dfs.fabric.microsoft.com/workspace/item/",
        "Files/a.csv;impersonate"
      ),
      paste0(
        "abfss://workspace@onelake.dfs.fabric.microsoft.com/",
        "item/Files/b.csv;impersonate"
      )
    ),
    source_ids = ids,
    raw_sizes = c(100, NA)
  )
  expect_equal(vapply(records, `[[`, character(1), "source_id"), ids)
  expect_equal(records[[1L]]$raw_size, 100)
  expect_null(records[[2L]]$raw_size)

  generated <- kusto_ingestion_sources(
    data.frame(
      url = "https://example.test/a.csv",
      rawSize = 12,
      stringsAsFactors = FALSE
    ),
    source_ids = NULL,
    raw_sizes = NULL
  )
  expect_true(fabric_is_guid(generated[[1L]]$source_id))

  expect_error(
    kusto_ingestion_sources(
      rep("https://example.test/a.csv", 21L),
      NULL,
      NULL
    ),
    "limit of 20 blobs",
    fixed = TRUE
  )
  expect_error(
    kusto_ingestion_sources(
      c("https://example.test/a.csv", "https://example.test/b.csv"),
      rep(ids[[1L]], 2L),
      NULL
    ),
    "must be unique",
    fixed = TRUE
  )
  expect_error(
    kusto_ingestion_sources(
      "file:///tmp/a.csv",
      NULL,
      NULL
    ),
    "https:// or abfss://",
    fixed = TRUE
  )
  expect_error(
    kusto_ingestion_sources(
      "https://example.test/a.csv",
      NULL,
      .kusto_ingestion_max_size + 1
    ),
    "exceeds the 6 GB",
    fixed = TRUE
  )
  expect_error(
    kusto_ingestion_sources(
      c("https://example.test/a.csv", "https://example.test/b.csv"),
      NULL,
      c(.kusto_ingestion_max_size / 2 + 1, .kusto_ingestion_max_size / 2)
    ),
    "known raw_sizes exceed",
    fixed = TRUE
  )
  expect_error(
    kusto_ingestion_sources(
      list(list(
        url = "https://example.test/a.csv",
        source_id = ids[[1L]],
        sourceId = ids[[2L]]
      )),
      NULL,
      NULL
    ),
    "conflicting aliases",
    fixed = TRUE
  )
})

test_that("fabric_kql_ingest sends the documented tracked payload", {
  captured <- NULL
  httr2::local_mocked_responses(function(req) {
    captured <<- req
    kusto_ingestion_test_response(
      list(ingestionOperationId = "ingest_op_12345"),
      url = req$url,
      headers = list("x-ms-request-id" = "service-request-id")
    )
  })

  source_id <- "11111111-1111-4111-8111-111111111111"
  handle <- fabric_kql_ingest(
    list(
      id = "database-id",
      type = "KQLDatabase",
      database_name = "Telemetry",
      ingestion_service_uri = paste0(
        "https://ingest-cluster.kusto.fabric.microsoft.com"
      )
    ),
    table = "Raw Events",
    sources = paste0(
      "https://account.blob.core.windows.net/container/events.csv?",
      "sv=1&sig=source-secret"
    ),
    format = "CSV",
    source_ids = source_id,
    raw_sizes = 1024,
    mapping = "EventsCsv",
    tags = "environment:test",
    ingest_if_not_exists = "events-2026-08-14",
    ignore_first_record = TRUE,
    skip_batching = FALSE,
    delete_after_download = FALSE,
    creation_time = as.Date("2026-08-14"),
    timestamp = as.POSIXct("2026-08-14 10:00:00", tz = "UTC"),
    validation_policy = list(
      ValidationOptions = 1L,
      ValidationImplications = 1L
    ),
    timeout = 17,
    token = "kusto-token"
  )

  expect_s3_class(handle, "fabric_kql_ingestion")
  expect_equal(handle$id, "ingest_op_12345")
  expect_equal(handle$request_id, "service-request-id")
  expect_equal(handle$sources$source_id, source_id)
  expect_false(grepl("source-secret", handle$sources$url, fixed = TRUE))
  expect_match(handle$sources$url, "<redacted>", fixed = TRUE)
  expect_equal(captured$options$timeout_ms, 17000)
  expect_equal(
    captured$url,
    paste0(
      "https://ingest-cluster.kusto.fabric.microsoft.com/",
      "v1/rest/ingestion/queued/Telemetry/Raw%20Events"
    )
  )
  expect_null(captured$headers[["x-ms-readonly"]])

  encoded <- jsonlite::toJSON(
    captured$body$data,
    auto_unbox = TRUE,
    digits = 22,
    null = "null"
  )
  payload <- jsonlite::fromJSON(encoded, simplifyVector = FALSE)
  expect_length(payload$blobs, 1L)
  expect_equal(payload$blobs[[1L]]$sourceId, source_id)
  expect_equal(payload$blobs[[1L]]$rawSize, 1024L)
  expect_true(payload$properties$enableTracking)
  expect_equal(payload$properties$format, "csv")
  expect_equal(payload$properties$ingestionMappingReference, "EventsCsv")
  expect_equal(
    unlist(payload$properties$tags),
    c("environment:test", "ingest-by:events-2026-08-14")
  )
  expect_equal(
    unlist(payload$properties$ingestIfNotExists),
    "events-2026-08-14"
  )
  expect_true(payload$properties$ignoreFirstRecord)
  expect_match(payload$properties$validationPolicy, "ValidationOptions")
  expect_equal(payload$timestamp, "2026-08-14T10:00:00.000000Z")
})

test_that("submission failures are not replayed after throttling", {
  calls <- 0L
  httr2::local_mocked_responses(function(req) {
    calls <<- calls + 1L
    kusto_ingestion_test_response(
      list(error = list(code = "TooManyRequests", message = "slow down")),
      status = 429L,
      url = req$url,
      headers = list("retry-after" = "30")
    )
  })

  error <- tryCatch(
    fabric_kql_ingest(
      "https://ingest-cluster.kusto.fabric.microsoft.com",
      table = "Raw",
      sources = "https://example.test/a.csv",
      database = "Telemetry",
      format = "csv",
      token = "token"
    ),
    error = identity
  )
  expect_s3_class(error, "fabric_kql_ingestion_submission_error")
  expect_equal(error$status, 429L)
  expect_equal(calls, 1L)
  expect_match(conditionMessage(error), "not replayed", fixed = TRUE)
})

test_that("submission permission failures retain HTTP diagnostics", {
  httr2::local_mocked_responses(function(req) {
    kusto_ingestion_test_response(
      list(
        error = list(
          code = "Forbidden",
          message = "Principal needs Table Ingestor permission"
        )
      ),
      status = 403L,
      url = req$url,
      headers = list("x-ms-request-id" = "permission-request-id")
    )
  })
  error <- tryCatch(
    fabric_kql_ingest(
      "https://ingest-cluster.kusto.fabric.microsoft.com",
      table = "Raw",
      sources = "https://example.test/a.csv",
      database = "Telemetry",
      format = "csv",
      token = "underprivileged-token"
    ),
    error = identity
  )
  expect_s3_class(error, "fabric_kql_ingestion_submission_error")
  expect_equal(error$status, 403L)
  expect_equal(
    error$response_metadata$body$error$code,
    "Forbidden"
  )
})

test_that("ingestion status normalizes details and redacts source secrets", {
  source_id <- "11111111-1111-4111-8111-111111111111"
  response <- kusto_ingestion_test_status(
    succeeded = 1L,
    details = list(list(
      sourceId = source_id,
      url = paste0(
        "https://account.blob.core.windows.net/c/a.csv?",
        "sv=1&sig=status-secret"
      ),
      status = "Succeeded",
      startTime = "2026-08-14T10:00:00Z",
      lastUpdated = "2026-08-14T10:01:00Z"
    ))
  )
  captured <- NULL
  audiences <- character()
  httr2::local_mocked_responses(function(req) {
    captured <<- req
    kusto_ingestion_test_response(
      response,
      url = req$url,
      headers = list("x-ms-request-id" = "status-request-id")
    )
  })

  status <- fabric_kql_ingestion_status(
    "operation;123",
    cluster = "https://ingest-cluster.kusto.fabric.microsoft.com",
    database = "Telemetry",
    table = "Raw",
    token = function(audience, force_refresh = FALSE) {
      audiences <<- c(audiences, audience)
      "token"
    }
  )

  expect_s3_class(status, "fabric_kql_ingestion_status")
  expect_equal(status$state, "Succeeded")
  expect_true(status$complete)
  expect_equal(status$succeeded, 1)
  expect_s3_class(status$start_time, "POSIXct")
  expect_equal(status$details$source_id, source_id)
  expect_false(grepl("status-secret", status$details$url, fixed = TRUE))
  expect_false(grepl(
    "status-secret",
    jsonlite::toJSON(status$raw, auto_unbox = TRUE),
    fixed = TRUE
  ))
  expect_equal(status$request_id, "status-request-id")
  expect_equal(audiences, "https://api.kusto.windows.net/.default")
  expect_match(captured$url, "operation%3B123", fixed = TRUE)
  expect_match(captured$url, "details=true", fixed = TRUE)
})

test_that("ingestion status permits a missing last-updated timestamp", {
  response <- kusto_ingestion_test_status(in_progress = 1L)
  response$lastUpdated <- NULL
  httr2::local_mocked_responses(list(
    kusto_ingestion_test_response(response)
  ))

  status <- fabric_kql_ingestion_status(
    "operation;pending",
    cluster = "https://ingest-cluster.kusto.fabric.microsoft.com",
    database = "Telemetry",
    table = "Raw",
    details = FALSE,
    token = "test-token"
  )

  expect_equal(status$state, "InProgress")
  expect_false(status$complete)
  expect_true(is.na(status$last_updated))
  expect_s3_class(status$last_updated, "POSIXct")
})

test_that("waiting exposes partial batch and mapping failures", {
  active <- kusto_ingestion_test_status(
    succeeded = 1L,
    in_progress = 1L,
    details = list(
      list(
        sourceId = "11111111-1111-4111-8111-111111111111",
        url = "https://example.test/a.csv",
        status = "Succeeded",
        startTime = "2026-08-14T10:00:00Z",
        lastUpdated = "2026-08-14T10:00:30Z"
      ),
      list(
        sourceId = "22222222-2222-4222-8222-222222222222",
        url = "https://example.test/b.csv",
        status = "InProgress",
        startTime = "2026-08-14T10:00:00Z",
        lastUpdated = "2026-08-14T10:00:30Z"
      )
    )
  )
  partial <- kusto_ingestion_test_status(
    succeeded = 1L,
    failed = 1L,
    details = list(
      active$details[[1L]],
      list(
        sourceId = "22222222-2222-4222-8222-222222222222",
        url = "https://example.test/b.csv",
        status = "Failed",
        startTime = "2026-08-14T10:00:00Z",
        lastUpdated = "2026-08-14T10:01:00Z",
        errorCode = "BadRequest_MissingMappingFailure",
        failureStatus = "Permanent",
        details = "Mapping not found"
      )
    )
  )
  calls <- 0L
  httr2::local_mocked_responses(function(req) {
    calls <<- calls + 1L
    kusto_ingestion_test_response(
      if (calls == 1L) active else partial,
      url = req$url
    )
  })
  result <- fabric_kql_ingestion_status(
    "ingest_op_partial",
    cluster = "https://ingest-cluster.kusto.fabric.microsoft.com",
    database = "Telemetry",
    table = "Raw",
    wait = TRUE,
    poll_interval = 0.1,
    error_on_failure = FALSE,
    token = "token",
    .sleep = function(seconds) NULL
  )
  expect_equal(calls, 2L)
  expect_equal(result$state, "PartiallySucceeded")
  expect_true(result$complete)
  expect_equal(result$failed, 1)
  expect_equal(
    result$details$error_code[[2L]],
    "BadRequest_MissingMappingFailure"
  )
  expect_equal(result$details$failure_status[[2L]], "Permanent")

  httr2::local_mocked_responses(list(
    kusto_ingestion_test_response(partial)
  ))
  error <- tryCatch(
    fabric_kql_ingestion_status(
      "ingest_op_partial",
      cluster = "https://ingest-cluster.kusto.fabric.microsoft.com",
      database = "Telemetry",
      table = "Raw",
      token = "token"
    ),
    error = identity
  )
  expect_s3_class(error, "fabric_kql_ingestion_partial_failure")
  expect_equal(error$last_status$state, "PartiallySucceeded")
  expect_match(conditionMessage(error), "Mapping not found", fixed = TRUE)
})

test_that("duplicate prevention failures remain actionable", {
  duplicate <- kusto_ingestion_test_status(
    failed = 1L,
    details = list(list(
      sourceId = "11111111-1111-4111-8111-111111111111",
      url = "https://example.test/a.csv",
      status = "Failed",
      startTime = "2026-08-14T10:00:00Z",
      lastUpdated = "2026-08-14T10:01:00Z",
      errorCode = "DataAlreadyExists",
      failureStatus = "Permanent",
      details = "An extent with ingest-by:batch-1 already exists"
    ))
  )
  httr2::local_mocked_responses(list(
    kusto_ingestion_test_response(duplicate)
  ))
  result <- fabric_kql_ingestion_status(
    "ingest_op_duplicate",
    cluster = "https://ingest-cluster.kusto.fabric.microsoft.com",
    database = "Telemetry",
    table = "Raw",
    error_on_failure = FALSE,
    token = "token"
  )
  expect_equal(result$state, "Failed")
  expect_equal(result$details$error_code, "DataAlreadyExists")
  expect_match(result$details$message, "ingest-by:batch-1", fixed = TRUE)
})

test_that("status GET retries throttling and respects retry hints", {
  calls <- 0L
  httr2::local_mocked_responses(function(req) {
    calls <<- calls + 1L
    if (calls == 1L) {
      return(kusto_ingestion_test_response(
        list(error = list(code = "TooManyRequests")),
        status = 429L,
        url = req$url,
        headers = list("retry-after" = "0")
      ))
    }
    kusto_ingestion_test_response(
      kusto_ingestion_test_status(succeeded = 1L),
      url = req$url
    )
  })
  result <- fabric_kql_ingestion_status(
    "ingest_op_throttled",
    cluster = "https://ingest-cluster.kusto.fabric.microsoft.com",
    database = "Telemetry",
    table = "Raw",
    details = FALSE,
    token = "token"
  )
  expect_equal(result$state, "Succeeded")
  expect_equal(calls, 2L)
})

test_that("wait timeout is distinct from the running service operation", {
  httr2::local_mocked_responses(function(req) {
    kusto_ingestion_test_response(
      kusto_ingestion_test_status(in_progress = 1L),
      url = req$url
    )
  })
  started <- Sys.time()
  clock_calls <- 0L
  clock <- function() {
    clock_calls <<- clock_calls + 1L
    started + if (clock_calls >= 2L) 1 else 0
  }
  error <- tryCatch(
    fabric_kql_ingestion_status(
      "ingest_op_running",
      cluster = "https://ingest-cluster.kusto.fabric.microsoft.com",
      database = "Telemetry",
      table = "Raw",
      wait = TRUE,
      timeout = 0.5,
      poll_interval = 0.1,
      token = "token",
      .sleep = function(seconds) NULL,
      .now = clock
    ),
    error = identity
  )
  expect_s3_class(error, "fabric_kql_ingestion_timeout")
  expect_equal(error$last_status$state, "InProgress")
  expect_match(conditionMessage(error), "may still be running", fixed = TRUE)
})

test_that("malformed preview status responses raise protocol errors", {
  target <- list(
    url = "https://ingest-cluster.kusto.fabric.microsoft.com",
    database = "Telemetry",
    table = "Raw"
  )
  context <- list(
    id = "operation",
    target = target,
    expected_count = NA_integer_,
    ingestion = NULL
  )
  expect_error(
    kusto_ingestion_status_record(
      list(startTime = "2026-08-14T10:00:00Z"),
      context
    ),
    class = "fabric_kql_ingestion_protocol_error"
  )
  malformed <- kusto_ingestion_test_status(succeeded = 1L)
  malformed$status$Succeeded <- -1L
  expect_error(
    kusto_ingestion_status_record(malformed, context),
    "invalid count",
    fixed = TRUE
  )
  missing_details <- kusto_ingestion_test_status(succeeded = 1L)
  missing_details$details <- NULL
  expect_error(
    kusto_ingestion_status_record(
      missing_details,
      context,
      details_requested = TRUE
    ),
    "omitted ingestion details",
    fixed = TRUE
  )
})

test_that("ingestion property validation fails before authentication", {
  expect_error(
    fabric_kql_ingest(
      "https://ingest-cluster.kusto.fabric.microsoft.com",
      "Raw",
      "https://example.test/a.csv",
      database = "Telemetry",
      format = "delta",
      token = function(...) stop("must not authenticate")
    ),
    "format must be one of",
    fixed = TRUE
  )
  expect_error(
    fabric_kql_ingest(
      "https://ingest-cluster.kusto.fabric.microsoft.com",
      "Raw",
      "https://example.test/a.parquet",
      database = "Telemetry",
      format = "parquet",
      validation_policy = list(ValidationOptions = 1L),
      token = function(...) stop("must not authenticate")
    ),
    "only for delimited text",
    fixed = TRUE
  )
  expect_error(
    fabric_kql_ingest(
      "https://ingest-cluster.kusto.fabric.microsoft.com",
      "Raw",
      "https://example.test/a.csv",
      database = "Telemetry",
      format = "csv",
      ingest_if_not_exists = "ingest-by:batch-1",
      token = function(...) stop("must not authenticate")
    ),
    "omit the 'ingest-by:' prefix",
    fixed = TRUE
  )
})
