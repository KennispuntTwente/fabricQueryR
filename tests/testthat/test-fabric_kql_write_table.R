kql_write_test_folder <- paste0(
  "https://onelake.dfs.fabric.microsoft.com/",
  "11111111-1111-4111-8111-111111111111/",
  "22222222-2222-4222-8222-222222222222/Files/ingestion"
)

kql_write_test_configuration <- function(
  max_data_size = 6442450944,
  max_blobs = 20
) {
  list(
    lake_folders = kql_write_test_folder,
    max_data_size = max_data_size,
    max_blobs = max_blobs,
    preferred_upload_method = "Lake",
    preferred_ingestion_method = "REST"
  )
}

kql_write_test_ingestion <- function() {
  structure(
    list(
      id = "ingest_op_r_data",
      operation_id = "ingest_op_r_data"
    ),
    class = "fabric_kql_ingestion"
  )
}

kql_write_test_status <- function(state = "Succeeded") {
  structure(
    list(
      operation_id = "ingest_op_r_data",
      state = state,
      complete = TRUE,
      succeeded = if (identical(state, "Succeeded")) 1 else 0,
      failed = if (identical(state, "Failed")) 1 else 0,
      in_progress = 0,
      canceled = 0,
      details = tibble::tibble(
        source_id = "11111111-1111-4111-8111-111111111111",
        url = "[REDACTED]",
        status = state,
        start_time = as.POSIXct("2026-08-14", tz = "UTC"),
        last_updated = as.POSIXct("2026-08-14", tz = "UTC"),
        error_code = if (identical(state, "Failed")) "BadRequest" else NA,
        failure_status = if (identical(state, "Failed")) "Permanent" else NA,
        message = if (identical(state, "Failed")) "schema mismatch" else NA
      )
    ),
    class = "fabric_kql_ingestion_status"
  )
}

test_that("ingestion configuration uses the documented Kusto contract", {
  captured <- NULL
  body <- list(
    containerSettings = list(
      containers = list(list(
        path = "https://storage.test/container?sig=do-not-retain"
      )),
      lakeFolders = list(list(path = kql_write_test_folder)),
      preferredUploadMethod = "Lake"
    ),
    ingestionSettings = list(
      maxBlobsPerBatch = 20L,
      maxDataSize = "6442450944",
      preferredIngestionMethod = "REST"
    )
  )
  local_mocked_bindings(
    .httr2_perform = function(req, credential, audience, ...) {
      captured <<- list(
        request = req,
        credential = credential,
        audience = audience
      )
      httr2::response(
        status_code = 200L,
        url = req$url,
        headers = list("content-type" = "application/json"),
        body = charToRaw(jsonlite::toJSON(body, auto_unbox = TRUE))
      )
    }
  )
  target <- kusto_resolve_ingestion_target(
    "https://ingest-cluster.kusto.fabric.microsoft.com",
    "Telemetry",
    "Raw"
  )
  result <- kusto_ingestion_configuration(
    target,
    fabric_credential(token = "test-token")
  )

  expect_equal(result$lake_folders, kql_write_test_folder)
  expect_equal(result$max_data_size, 6442450944)
  expect_equal(result$max_blobs, 20)
  expect_equal(result$preferred_upload_method, "Lake")
  expect_match(
    captured$request$url,
    "/v1/rest/ingestion/configuration",
    fixed = TRUE
  )
  expect_s3_class(captured$credential, "fabric_credential")
  expect_equal(captured$audience, "https://api.kusto.windows.net/.default")
  expect_false(grepl(
    "do-not-retain",
    jsonlite::toJSON(result$raw, auto_unbox = TRUE),
    fixed = TRUE
  ))
})

test_that("Eventhouse writer stages a data frame, waits, and cleans safely", {
  skip_if_not_installed("arrow")
  uploaded <- NULL
  submitted <- NULL
  status_args <- NULL
  cleanup_calls <- 0L
  local_mocked_bindings(
    .fabric_lakehouse_staging_id = function() "write-fixed",
    kusto_ingestion_configuration = function(...) {
      kql_write_test_configuration()
    },
    onelake_upload_target = function(target, credential, source, ...) {
      uploaded <<- list(
        target = target,
        data = as.data.frame(arrow::read_parquet(source)),
        bytes = file.info(source)$size
      )
      tibble::tibble(path = target$path)
    },
    fabric_kql_ingest = function(...) {
      submitted <<- list(...)
      kql_write_test_ingestion()
    },
    fabric_kql_ingestion_status = function(...) {
      status_args <<- list(...)
      kql_write_test_status()
    },
    .fabric_onelake_remove_staging = function(target, credential) {
      cleanup_calls <<- cleanup_calls + 1L
      TRUE
    }
  )
  value <- data.frame(
    id = 1:2,
    label = factor(c("a", "b")),
    observed_on = as.Date(c("2026-08-13", "2026-08-14"))
  )
  result <- fabric_kql_write_table(
    "https://ingest-cluster.kusto.fabric.microsoft.com",
    table = "Raw",
    data = value,
    database = "Telemetry",
    mapping = "RawParquet",
    tags = "r-object",
    ingest_if_not_exists = "batch-1",
    skip_batching = TRUE,
    token = "test-token"
  )

  expect_s3_class(result, "fabric_kql_write_result")
  expect_equal(result$rows, 2)
  expect_equal(result$bytes, uploaded$bytes)
  expect_equal(result$file_count, 1L)
  expect_equal(result$columns, names(value))
  expect_false(result$staging_retained)
  expect_equal(cleanup_calls, 1L)
  expect_equal(uploaded$data$label, c("a", "b"))
  expect_equal(
    uploaded$target$path,
    "Files/ingestion/fabricqueryr-staging/write-fixed/part-00001.parquet"
  )
  expect_match(submitted$sources, ";impersonate$", perl = TRUE)
  expect_equal(submitted$format, "parquet")
  expect_equal(submitted$raw_sizes, uploaded$bytes)
  expect_equal(submitted$mapping, "RawParquet")
  expect_equal(submitted$tags, "r-object")
  expect_equal(submitted$ingest_if_not_exists, "batch-1")
  expect_true(submitted$skip_batching)
  expect_s3_class(submitted$token, "fabric_credential")
  expect_true(status_args$wait)
  expect_false(status_args$error_on_failure)
})

test_that("Eventhouse writer submits bounded Parquet batches", {
  skip_if_not_installed("arrow")
  uploads <- list()
  submitted <- NULL
  local_mocked_bindings(
    .fabric_lakehouse_staging_id = function() "multi-file",
    kusto_ingestion_configuration = function(...) {
      kql_write_test_configuration(max_blobs = 3)
    },
    onelake_upload_target = function(target, source, ...) {
      uploads[[length(uploads) + 1L]] <<- list(
        path = onelake_path_url(target),
        bytes = file.info(source)$size,
        data = as.data.frame(arrow::read_parquet(source))
      )
      tibble::tibble(path = target$path)
    },
    fabric_kql_ingest = function(...) {
      submitted <<- list(...)
      kql_write_test_ingestion()
    },
    fabric_kql_ingestion_status = function(...) kql_write_test_status(),
    .fabric_onelake_remove_staging = function(...) TRUE
  )

  result <- fabric_kql_write_table(
    "https://ingest-cluster.kusto.fabric.microsoft.com",
    "Raw",
    data.frame(id = 1:5),
    database = "Telemetry",
    max_rows_per_file = 2,
    token = "test-token"
  )

  expect_equal(result$file_count, 3L)
  expect_equal(length(uploads), 3L)
  expect_equal(
    vapply(uploads, function(x) nrow(x$data), integer(1)),
    c(2L, 2L, 1L)
  )
  expect_equal(unlist(lapply(uploads, function(x) x$data$id)), 1:5)
  expect_equal(submitted$raw_sizes, vapply(uploads, `[[`, numeric(1), "bytes"))
  expect_equal(submitted$sources, paste0(result$staging_paths, ";impersonate"))
  expect_length(submitted$source_ids, 3L)
  expect_length(unique(submitted$source_ids), 3L)
  expect_equal(result$bytes, sum(result$part_bytes))
})

test_that("Eventhouse writer consumes an Arrow reader batch by batch", {
  skip_if_not_installed("arrow")
  emitted <- 0L
  batches <- list(
    arrow::record_batch(id = 1:2, label = c("a", "b")),
    arrow::record_batch(id = 3:5, label = c("c", "d", "e"))
  )
  reader <- arrow::as_record_batch_reader(
    function() {
      emitted <<- emitted + 1L
      if (emitted <= length(batches)) batches[[emitted]] else NULL
    },
    schema = batches[[1L]]$schema
  )
  stream <- nanoarrow::as_nanoarrow_array_stream(reader)
  staged <- NULL
  local_mocked_bindings(
    .fabric_lakehouse_staging_id = function() "arrow-reader",
    kusto_ingestion_configuration = function(...) {
      kql_write_test_configuration()
    },
    onelake_upload_target = function(target, credential, source, ...) {
      staged <<- as.data.frame(arrow::read_parquet(source))
      tibble::tibble(path = target$path)
    },
    fabric_kql_ingest = function(...) kql_write_test_ingestion(),
    fabric_kql_ingestion_status = function(...) kql_write_test_status()
  )
  result <- fabric_kql_write_table(
    "https://ingest-cluster.kusto.fabric.microsoft.com",
    "Raw",
    stream,
    database = "Telemetry",
    cleanup = FALSE,
    token = "test-token"
  )

  expect_equal(result$rows, 5)
  expect_true(result$staging_retained)
  expect_equal(staged$id, 1:5)
  expect_equal(emitted, 3L)
  expect_null(reader$read_next_batch())
})

test_that("Eventhouse writer enforces configured size before upload", {
  skip_if_not_installed("arrow")
  upload_calls <- 0L
  local_mocked_bindings(
    kusto_ingestion_configuration = function(...) {
      kql_write_test_configuration(max_data_size = 1)
    },
    onelake_upload_target = function(...) {
      upload_calls <<- upload_calls + 1L
    }
  )
  expect_error(
    fabric_kql_write_table(
      "https://ingest-cluster.kusto.fabric.microsoft.com",
      "Raw",
      data.frame(id = 1L),
      database = "Telemetry",
      token = "test-token"
    ),
    class = "fabric_kql_size_error"
  )
  expect_equal(upload_calls, 0L)
})

test_that("Eventhouse writer enforces the advertised blob count", {
  skip_if_not_installed("arrow")
  upload_calls <- 0L
  local_mocked_bindings(
    kusto_ingestion_configuration = function(...) {
      kql_write_test_configuration(max_blobs = 2)
    },
    onelake_upload_target = function(...) {
      upload_calls <<- upload_calls + 1L
    }
  )
  expect_error(
    fabric_kql_write_table(
      "https://ingest-cluster.kusto.fabric.microsoft.com",
      "Raw",
      data.frame(id = 1:3),
      database = "Telemetry",
      max_rows_per_file = 1,
      token = "test-token"
    ),
    "allowed 2 files",
    class = "fabric_kql_arrow_error"
  )
  expect_equal(upload_calls, 0L)
})

test_that("an ambiguous OneLake upload reports its inspectable path", {
  skip_if_not_installed("arrow")
  local_mocked_bindings(
    .fabric_lakehouse_staging_id = function() "upload-ambiguous",
    kusto_ingestion_configuration = function(...) {
      kql_write_test_configuration()
    },
    onelake_upload_target = function(...) {
      rlang::abort("connection closed during rename")
    }
  )
  error <- expect_error(
    fabric_kql_write_table(
      "https://ingest-cluster.kusto.fabric.microsoft.com",
      "Raw",
      data.frame(id = 1L),
      database = "Telemetry",
      token = "test-token"
    ),
    class = "fabric_kql_upload_error"
  )
  expect_true(is.na(error$staging_retained))
  expect_match(error$staging_path, "upload-ambiguous", fixed = TRUE)
  expect_match(conditionMessage(error), "may exist", fixed = TRUE)
})

test_that("ambiguous submission retains the complete staging file", {
  skip_if_not_installed("arrow")
  cleanup_calls <- 0L
  local_mocked_bindings(
    kusto_ingestion_configuration = function(...) {
      kql_write_test_configuration()
    },
    onelake_upload_target = function(...) tibble::tibble(),
    fabric_kql_ingest = function(...) {
      rlang::abort(
        "connection reset",
        class = "fabric_kql_ingestion_submission_error"
      )
    },
    .fabric_onelake_remove_staging = function(...) {
      cleanup_calls <<- cleanup_calls + 1L
      TRUE
    }
  )
  error <- expect_error(
    fabric_kql_write_table(
      "https://ingest-cluster.kusto.fabric.microsoft.com",
      "Raw",
      data.frame(id = 1L),
      database = "Telemetry",
      token = "test-token"
    ),
    class = "fabric_kql_write_ambiguous"
  )
  expect_true(error$staging_retained)
  expect_match(error$staging_path, "/Files/ingestion/", fixed = TRUE)
  expect_equal(cleanup_calls, 0L)
})

test_that("confirmed failure follows the staging retention policy", {
  skip_if_not_installed("arrow")
  cleanup_calls <- 0L
  local_mocked_bindings(
    kusto_ingestion_configuration = function(...) {
      kql_write_test_configuration()
    },
    onelake_upload_target = function(...) tibble::tibble(),
    fabric_kql_ingest = function(...) kql_write_test_ingestion(),
    fabric_kql_ingestion_status = function(...) {
      kql_write_test_status("Failed")
    },
    .fabric_onelake_remove_staging = function(...) {
      cleanup_calls <<- cleanup_calls + 1L
      TRUE
    }
  )
  result <- fabric_kql_write_table(
    "https://ingest-cluster.kusto.fabric.microsoft.com",
    "Raw",
    data.frame(id = 1L),
    database = "Telemetry",
    keep_staging_on_failure = FALSE,
    error_on_failure = FALSE,
    token = "test-token"
  )
  expect_equal(result$status$state, "Failed")
  expect_false(result$staging_retained)
  expect_equal(cleanup_calls, 1L)

  error <- expect_error(
    fabric_kql_write_table(
      "https://ingest-cluster.kusto.fabric.microsoft.com",
      "Raw",
      data.frame(id = 1L),
      database = "Telemetry",
      error_on_failure = TRUE,
      token = "test-token"
    ),
    class = "fabric_kql_write_failure"
  )
  expect_true(error$staging_retained)
  expect_equal(error$last_status$state, "Failed")
})

test_that("staging folders must stay inside trusted OneLake Files paths", {
  configuration <- kql_write_test_configuration()
  expect_error(
    kusto_ingestion_staging_folder(
      configuration,
      "https://attacker.example/workspace/item/Files"
    ),
    class = "fabric_kql_staging_configuration_error"
  )
  expect_error(
    kusto_ingestion_staging_folder(
      configuration,
      sub("/Files/.*$", "/Tables", kql_write_test_folder)
    ),
    class = "fabric_kql_staging_configuration_error"
  )
})

test_that("R and Arrow validation happens before authentication", {
  expect_error(
    fabric_kql_write_table(
      "https://ingest-cluster.kusto.fabric.microsoft.com",
      "Raw",
      list(not = "tabular"),
      database = "Telemetry",
      token = function(...) stop("must not authenticate")
    ),
    class = "fabric_kql_arrow_error"
  )
  duplicated <- data.frame(a = 1L, b = 2L)
  names(duplicated) <- c("id", "id")
  expect_error(
    fabric_kql_write_table(
      "https://ingest-cluster.kusto.fabric.microsoft.com",
      "Raw",
      duplicated,
      database = "Telemetry",
      token = function(...) stop("must not authenticate")
    ),
    class = "fabric_kql_write_error"
  )
})
