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
  expect_equal(
    result$storage_containers,
    "https://storage.test/container?sig=do-not-retain"
  )
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

test_that("Eventhouse writer honors preferred Storage container staging", {
  skip_if_not_installed("arrow")
  container <- paste0(
    "https://account.blob.core.windows.net/ingest?",
    "sv=2023-11-03&sp=rwd&sig=storage-secret"
  )
  uploads <- list()
  submitted <- NULL
  cleanup_calls <- 0L
  local_mocked_bindings(
    .fabric_lakehouse_staging_id = function() "storage-fixed",
    kusto_ingestion_configuration = function(...) {
      kql_write_test_configuration(
        storage_containers = container,
        preferred_upload_method = "Storage"
      )
    },
    kusto_storage_upload = function(url, source) {
      uploads[[length(uploads) + 1L]] <<- list(
        url = url,
        rows = nrow(as.data.frame(arrow::read_parquet(source)))
      )
      invisible(TRUE)
    },
    onelake_upload_target = function(...) {
      stop("OneLake must not be selected")
    },
    fabric_kql_ingest = function(...) {
      submitted <<- list(...)
      kql_write_test_ingestion()
    },
    fabric_kql_ingestion_status = function(...) kql_write_test_status(),
    kusto_remove_staging = function(
      method,
      storage_targets,
      source_paths,
      storage_credential
    ) {
      cleanup_calls <<- cleanup_calls + 1L
      TRUE
    }
  )

  result <- fabric_kql_write_table(
    "https://ingest-cluster.kusto.fabric.microsoft.com",
    "Raw",
    data.frame(id = 1:2),
    database = "Telemetry",
    token = "kusto-token"
  )

  expect_length(uploads, 1L)
  expect_equal(uploads[[1L]]$rows, 2L)
  expect_match(
    uploads[[1L]]$url,
    "/ingest/fabricqueryr-staging/storage-fixed/part-00001.parquet?",
    fixed = TRUE
  )
  expect_match(uploads[[1L]]$url, "sig=storage-secret", fixed = TRUE)
  expect_identical(submitted$sources, uploads[[1L]]$url)
  expect_identical(submitted$delete_after_download, TRUE)
  expect_identical(cleanup_calls, 0L)
  expect_false(result$staging_retained)
  expect_false(any(grepl("storage-secret", result$staging_paths, fixed = TRUE)))
  expect_false(grepl("storage-secret", result$staging_path, fixed = TRUE))
})

test_that("Storage staging constructs authenticated blob requests safely", {
  request <- NULL
  local_mocked_bindings(
    .httr2_perform = function(req, ...) {
      request <<- req
      httr2::response(status_code = 201L)
    }
  )
  source <- tempfile(fileext = ".parquet")
  writeBin(charToRaw("parquet"), source)
  withr::defer(unlink(source))
  container <- paste0(
    "https://account.blob.core.windows.net/ingest?",
    "sv=2023-11-03&sp=rwd&sig=secret"
  )
  url <- kusto_storage_blob_url(container, "safe path/part.parquet")

  expect_match(url, "/safe%20path/part.parquet?", fixed = TRUE)
  expect_equal(
    kusto_storage_blob_url(
      container,
      "safe path/part.parquet",
      include_credentials = FALSE
    ),
    "https://account.blob.core.windows.net/ingest/safe%20path/part.parquet"
  )
  kusto_storage_upload(url, source)
  expect_equal(request$method, "PUT")
  expect_equal(request$headers[["x-ms-blob-type"]], "BlockBlob")
  expect_equal(request$headers[["x-ms-version"]], "2023-11-03")
  expect_equal(request$body$content_type, "application/vnd.apache.parquet")

  expect_error(
    kusto_storage_validate_container("https://storage.example/no-sas"),
    class = "fabric_kql_staging_configuration_error"
  )
})

test_that("Eventhouse writer stages a data frame, waits, and cleans safely", {
  skip_if_not_installed("arrow")
  regional_folder <- sub(
    "onelake.dfs.fabric.microsoft.com",
    "switzerlandnorth-api.onelake.fabric.microsoft.com",
    kql_write_test_folder,
    fixed = TRUE
  )
  uploaded <- NULL
  submitted <- NULL
  status_args <- NULL
  cleanup_calls <- 0L
  local_mocked_bindings(
    .fabric_lakehouse_staging_id = function() "write-fixed",
    kusto_ingestion_configuration = function(...) {
      kql_write_test_configuration(lake_folder = regional_folder)
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
  token <- function(audience) {
    if (identical(audience, .fabric_audience$storage)) {
      "storage-token"
    } else {
      "kusto-token"
    }
  }
  result <- fabric_kql_write_table(
    "https://ingest-cluster.kusto.fabric.microsoft.com",
    table = "Raw",
    data = value,
    database = "Telemetry",
    mapping = "RawParquet",
    tags = "r-object",
    ingest_if_not_exists = "batch-1",
    skip_batching = TRUE,
    token = token
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
  expect_equal(
    uploaded$target$dfs_base,
    "https://switzerlandnorth-api.onelake.fabric.microsoft.com"
  )
  expect_equal(
    submitted$sources,
    paste0(
      kql_write_test_folder,
      "/fabricqueryr-staging/write-fixed/part-00001.parquet",
      ";token=storage-token"
    )
  )
  expect_equal(submitted$format, "parquet")
  expect_null(submitted$raw_sizes)
  expect_equal(submitted$mapping, "RawParquet")
  expect_equal(submitted$tags, "r-object")
  expect_equal(submitted$ingest_if_not_exists, "batch-1")
  expect_true(submitted$skip_batching)
  expect_s3_class(submitted$token, "fabric_credential")
  expect_true(status_args$wait)
  expect_false(status_args$error_on_failure)
})

test_that("Eventhouse writer requires a Storage credential for fixed tokens", {
  skip_if_not_installed("arrow")
  local_mocked_bindings(
    kusto_ingestion_configuration = function(...) {
      kql_write_test_configuration()
    }
  )

  expect_snapshot(
    error = TRUE,
    fabric_kql_write_table(
      "https://ingest-cluster.kusto.fabric.microsoft.com",
      "Raw",
      data.frame(id = 1L),
      database = "Telemetry",
      token = "kusto-token"
    )
  )
})

test_that("Eventhouse writer creates a missing table before staging", {
  skip_if_not_installed("arrow")
  calls <- character()
  management <- NULL
  local_mocked_bindings(
    .fabric_lakehouse_staging_id = function() "create-table",
    kusto_export_management = function(
      target,
      command,
      credential,
      deadline,
      idempotent,
      operation
    ) {
      calls <<- c(calls, "management")
      management <<- list(
        target = target,
        command = command,
        credential = credential,
        deadline = deadline,
        idempotent = idempotent,
        operation = operation
      )
      list(tables = list(), request_id = "create-request")
    },
    kusto_ingestion_configuration = function(...) {
      calls <<- c(calls, "configuration")
      kql_write_test_configuration()
    },
    onelake_upload_target = function(...) {
      calls <<- c(calls, "upload")
      tibble::tibble()
    },
    fabric_kql_ingest = function(...) kql_write_test_ingestion(),
    fabric_kql_ingestion_status = function(...) kql_write_test_status()
  )
  now <- as.POSIXct("2026-08-14 12:00:00", tz = "UTC")
  value <- data.frame(
    id = 1:2,
    amount = c(1.5, 2.5),
    active = c(TRUE, FALSE),
    observed_on = as.Date(c("2026-08-13", "2026-08-14")),
    label = c("a", "b")
  )

  result <- fabric_kql_write_table(
    "https://ingest-cluster.kusto.fabric.microsoft.com",
    "Raw new",
    value,
    database = "Telemetry",
    create_if_missing = TRUE,
    cleanup = FALSE,
    token = "test-token",
    storage_token = "storage-token",
    .now = function() now
  )

  expect_equal(calls[1:3], c("management", "configuration", "upload"))
  expect_equal(
    management$target$url,
    "https://cluster.kusto.fabric.microsoft.com/v2/rest/query"
  )
  expect_equal(management$target$database, "Telemetry")
  expect_equal(
    management$command,
    paste0(
      ".create table ['Raw new'] (['id']:int, ['amount']:real, ",
      "['active']:bool, ['observed_on']:datetime, ['label']:string)"
    )
  )
  expect_true(management$idempotent)
  expect_equal(management$operation, "CreateTable")
  expect_s3_class(management$credential, "fabric_credential")
  expect_equal(management$deadline, now + 60)
  expect_true(result$table_creation_requested)
})

test_that("KQL table creation accepts exact type overrides", {
  skip_if_not_installed("arrow")
  schema <- arrow::schema(id = arrow::int32(), payload = arrow::binary())

  command <- kusto_write_create_table_command(
    "Events-v2",
    schema,
    c("id", "payload"),
    c(payload = "dynamic", id = "long")
  )

  expect_equal(
    command,
    ".create table ['Events-v2'] (['id']:long, ['payload']:dynamic)"
  )
  expect_error(
    kusto_write_create_table_command(
      "Events",
      schema,
      c("id", "payload"),
      c(id = "long")
    ),
    class = "fabric_kql_schema_error"
  )
  expect_error(
    kusto_write_create_table_command(
      "Events",
      schema,
      c("id", "payload")
    ),
    "Cannot infer",
    class = "fabric_kql_schema_error"
  )
})

test_that("KQL table creation fails before staging or upload", {
  skip_if_not_installed("arrow")
  configuration_calls <- 0L
  upload_calls <- 0L
  local_mocked_bindings(
    kusto_export_management = function(...) rlang::abort("not authorized"),
    kusto_ingestion_configuration = function(...) {
      configuration_calls <<- configuration_calls + 1L
      kql_write_test_configuration()
    },
    onelake_upload_target = function(...) {
      upload_calls <<- upload_calls + 1L
      tibble::tibble()
    }
  )

  expect_error(
    fabric_kql_write_table(
      "https://ingest-cluster.kusto.fabric.microsoft.com",
      "Raw",
      data.frame(id = 1L),
      database = "Telemetry",
      create_if_missing = TRUE,
      token = "test-token",
      storage_token = "storage-token"
    ),
    class = "fabric_kql_table_create_error"
  )
  expect_equal(configuration_calls, 0L)
  expect_equal(upload_calls, 0L)
})

test_that("KQL table creation protects management endpoint and identifiers", {
  target <- kusto_resolve_ingestion_target(
    "https://ingest.example.test",
    "Telemetry",
    "Raw"
  )
  expect_error(
    kusto_write_management_target(
      "https://ingest.example.test",
      target,
      query_cluster = NULL
    ),
    "query_cluster is required",
    class = "fabric_kql_table_create_error"
  )
  expect_error(
    kusto_write_identifier("Raw; .drop table Other", "table"),
    class = "fabric_kql_schema_error"
  )
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
    token = "test-token",
    storage_token = "storage-token"
  )

  expect_equal(result$file_count, 3L)
  expect_equal(length(uploads), 3L)
  expect_equal(
    vapply(uploads, function(x) nrow(x$data), integer(1)),
    c(2L, 2L, 1L)
  )
  expect_equal(unlist(lapply(uploads, function(x) x$data$id)), 1:5)
  expect_null(submitted$raw_sizes)
  expect_equal(
    submitted$sources,
    paste0(result$staging_paths, ";token=storage-token")
  )
  expect_length(submitted$source_ids, 3L)
  expect_length(unique(submitted$source_ids), 3L)
  expect_equal(result$bytes, sum(result$part_bytes))
})

test_that("Eventhouse writer rejects unsafe multi-file idempotency", {
  skip_if_not_installed("arrow")
  upload_calls <- 0L
  local_mocked_bindings(
    kusto_ingestion_configuration = function(...) {
      kql_write_test_configuration(max_blobs = 3)
    },
    onelake_upload_target = function(...) {
      upload_calls <<- upload_calls + 1L
    }
  )

  expect_snapshot(
    error = TRUE,
    fabric_kql_write_table(
      "https://ingest-cluster.kusto.fabric.microsoft.com",
      "Raw",
      data.frame(id = 1:3),
      database = "Telemetry",
      ingest_if_not_exists = "batch-1",
      skip_batching = TRUE,
      max_rows_per_file = 1,
      token = "test-token",
      storage_token = "storage-token"
    )
  )
  expect_equal(upload_calls, 0L)
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
    token = "test-token",
    storage_token = "storage-token"
  )

  expect_equal(result$rows, 5)
  expect_true(result$staging_retained)
  expect_equal(staged$id, 1:5)
  expect_equal(emitted, 3L)
  expect_null(reader$read_next_batch())
})

test_that("Eventhouse writer enforces the advertised data-size limit", {
  skip_if_not_installed("arrow")
  upload_calls <- 0L
  submitted <- 0L
  local_mocked_bindings(
    kusto_ingestion_configuration = function(...) {
      kql_write_test_configuration(max_data_size = 1)
    },
    onelake_upload_target = function(...) {
      upload_calls <<- upload_calls + 1L
      tibble::tibble()
    },
    fabric_kql_ingest = function(...) {
      submitted <<- submitted + 1L
      kql_write_test_ingestion()
    },
    fabric_kql_ingestion_status = function(...) kql_write_test_status(),
    .fabric_onelake_remove_staging = function(...) TRUE
  )
  error <- tryCatch(
    fabric_kql_write_table(
      "https://ingest-cluster.kusto.fabric.microsoft.com",
      "Raw",
      data.frame(value = rep("compressible", 1000)),
      database = "Telemetry",
      token = "test-token",
      storage_token = "storage-token"
    ),
    error = identity
  )

  expect_s3_class(error, "fabric_kql_size_error")
  expect_gt(error$bytes, error$max_data_size)
  expect_identical(error$max_data_size, 1)
  expect_identical(upload_calls, 0L)
  expect_identical(submitted, 0L)
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
      token = "test-token",
      storage_token = "storage-token"
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
      token = "test-token",
      storage_token = "storage-token"
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
      token = "test-token",
      storage_token = "storage-token"
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
    token = "test-token",
    storage_token = "storage-token"
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
      token = "test-token",
      storage_token = "storage-token"
    ),
    class = "fabric_kql_write_failure"
  )
  expect_true(error$staging_retained)
  expect_equal(error$last_status$state, "Failed")
})

test_that("staging folders honor service paths and constrain overrides", {
  configuration <- kql_write_test_configuration()
  expect_error(
    kusto_ingestion_staging_folder(
      configuration,
      "https://attacker.example/workspace/item/Files"
    ),
    class = "fabric_kql_staging_configuration_error"
  )

  workspace_id <- "11111111-1111-4111-8111-111111111111"
  item_id <- "22222222-2222-4222-8222-222222222222"
  host <- paste0(
    gsub("-", "", workspace_id, fixed = TRUE),
    ".z12.dfs.fabric.microsoft.com"
  )
  service_folder <- paste0(
    "https://",
    host,
    "/",
    item_id,
    "/Ingestion/Queue"
  )
  configuration$lake_folders <- service_folder
  target <- kusto_ingestion_staging_folder(configuration)
  expect_equal(target$workspace, workspace_id)
  expect_equal(target$item, item_id)
  expect_equal(target$path, "Ingestion/Queue")

  expect_error(
    kusto_ingestion_staging_folder(
      configuration,
      sub("/Files/.*$", "/Tables", kql_write_test_folder)
    ),
    class = "fabric_kql_staging_configuration_error"
  )

  configuration$lake_folders <- sub(
    "/Ingestion/Queue$",
    "/Tables",
    service_folder
  )
  expect_error(
    kusto_ingestion_staging_folder(configuration),
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
