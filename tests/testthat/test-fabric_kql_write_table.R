test_that("ingestion configuration uses the documented Kusto contract", {
  captured <- NULL
  body <- list(
    containerSettings = list(
      containers = list(list(
        path = "https://storage.test/container?sig=do-not-retain"
      )),
      lakeFolders = list(list(path = kql_write_test_folder)),
      refreshInterval = "01:00:00",
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
    fabric_credential(token = "test-token"),
    .now = function() as.POSIXct("2026-08-30 12:00:00", tz = "UTC")
  )

  expect_equal(result$lake_folders, kql_write_test_folder)
  expect_equal(
    result$storage_containers,
    "https://storage.test/container?sig=do-not-retain"
  )
  expect_equal(result$max_data_size, 6442450944)
  expect_equal(result$max_blobs, 20)
  expect_equal(result$preferred_upload_method, "Lake")
  expect_equal(result$refresh_interval, 3600)
  expect_equal(
    result$retrieved_at,
    as.POSIXct("2026-08-30 12:00:00", tz = "UTC")
  )
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
  fresh_container <- sub(
    "storage-secret",
    "fresh-secret",
    container,
    fixed = TRUE
  )
  uploads <- list()
  submitted <- NULL
  cleanup_calls <- 0L
  configuration_calls <- 0L
  local_mocked_bindings(
    .fabric_lakehouse_staging_id = function() "storage-fixed",
    kusto_write_table_schema = function(...) character(),
    kusto_write_assert_identity_schema = function(...) invisible(NULL),
    kusto_ingestion_configuration = function(...) {
      configuration_calls <<- configuration_calls + 1L
      kql_write_test_configuration(
        storage_containers = if (configuration_calls == 1L) {
          container
        } else {
          fresh_container
        },
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
  expect_identical(configuration_calls, 2L)
  expect_equal(uploads[[1L]]$rows, 2L)
  expect_match(
    uploads[[1L]]$url,
    "/ingest/fabricqueryr-staging/storage-fixed/part-00001.parquet?",
    fixed = TRUE
  )
  expect_match(uploads[[1L]]$url, "sig=fresh-secret", fixed = TRUE)
  expect_false(grepl("storage-secret", uploads[[1L]]$url, fixed = TRUE))
  expect_identical(submitted$sources, uploads[[1L]]$url)
  expect_identical(submitted$delete_after_download, TRUE)
  expect_identical(cleanup_calls, 0L)
  expect_false(result$staging_retained)
  expect_false(any(grepl("storage-secret", result$staging_paths, fixed = TRUE)))
  expect_false(grepl("storage-secret", result$staging_path, fixed = TRUE))
})

test_that("KQL ingestion configuration validates refresh intervals", {
  expect_equal(kusto_ingestion_refresh_interval("01:02:03.5"), 3723.5)
  expect_equal(kusto_ingestion_refresh_interval(NULL), Inf)

  for (value in c("one hour", "00:60:00", "00:00:00", "-01:00:00")) {
    error <- rlang::catch_cnd(kusto_ingestion_refresh_interval(value))
    expect_s3_class(error, "fabric_kql_ingestion_protocol_error")
    expect_match(conditionMessage(error), "refreshInterval", fixed = TRUE)
  }
})

test_that("Eventhouse writer refreshes an expired Storage SAS and retries", {
  skip_if_not_installed("arrow")
  now <- as.POSIXct("2026-08-30 12:00:00", tz = "UTC")
  configuration_calls <- 0L
  upload_urls <- character()
  local_mocked_bindings(
    .fabric_lakehouse_staging_id = function() "sas-refresh",
    kusto_write_table_schema = function(...) character(),
    kusto_write_assert_identity_schema = function(...) invisible(NULL),
    kusto_ingestion_configuration = function(...) {
      configuration_calls <<- configuration_calls + 1L
      secret <- if (configuration_calls < 3L) "expired" else "fresh"
      configuration <- kql_write_test_configuration(
        storage_containers = paste0(
          "https://account.blob.core.windows.net/ingest?sp=rwd&sig=",
          secret
        ),
        preferred_upload_method = "Storage"
      )
      configuration$refresh_interval <- 3600
      configuration$retrieved_at <- now
      configuration
    },
    kusto_storage_upload = function(url, source) {
      upload_urls <<- c(upload_urls, url)
      if (length(upload_urls) == 1L) {
        rlang::abort(
          "SAS expired",
          class = "fabric_http_error",
          status = 403L
        )
      }
      invisible(TRUE)
    },
    fabric_kql_ingest = function(...) kql_write_test_ingestion(),
    fabric_kql_ingestion_status = function(...) kql_write_test_status()
  )

  result <- fabric_kql_write_table(
    "https://ingest-cluster.kusto.fabric.microsoft.com",
    "Raw",
    data.frame(id = 1L),
    database = "Telemetry",
    token = "test-token",
    .now = function() now
  )

  expect_identical(configuration_calls, 3L)
  expect_length(upload_urls, 2L)
  expect_match(upload_urls[[1L]], "sig=expired", fixed = TRUE)
  expect_match(upload_urls[[2L]], "sig=fresh", fixed = TRUE)
  expect_false(grepl("sig=", result$staging_path, fixed = TRUE))
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

test_that("Storage staging block-uploads files above the Put Blob limit", {
  requests <- list()
  local_mocked_bindings(
    .httr2_perform = function(req, ...) {
      requests[[length(requests) + 1L]] <<- req
      httr2::response(status_code = 201L)
    }
  )
  source <- tempfile(fileext = ".parquet")
  writeBin(charToRaw("parquet"), source)
  withr::defer(unlink(source))
  url <- paste0(
    "https://account.blob.core.windows.net/ingest/part.parquet?",
    "sv=2023-11-03&sp=rwd&sig=secret"
  )

  kusto_storage_upload(
    url,
    source,
    single_put_limit = 4,
    block_size = 3
  )

  expect_length(requests, 4L)
  expect_true(all(vapply(
    requests[1:3],
    function(request) grepl("&comp=block&blockid=", request$url, fixed = TRUE),
    logical(1)
  )))
  expect_equal(
    vapply(
      requests[1:3],
      function(request) length(request$body$data),
      integer(1)
    ),
    c(3L, 3L, 1L)
  )
  commit <- requests[[4L]]
  expect_match(commit$url, "&comp=blocklist", fixed = TRUE)
  expect_equal(
    commit$headers[["x-ms-blob-content-type"]],
    "application/vnd.apache.parquet"
  )
  expect_match(rawToChar(commit$body$data), "<BlockList>", fixed = TRUE)
  expect_equal(
    lengths(regmatches(
      rawToChar(commit$body$data),
      gregexpr("<Latest>", rawToChar(commit$body$data), fixed = TRUE)
    )),
    3L
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
    kusto_write_table_schema = function(...) character(),
    kusto_write_assert_identity_schema = function(...) invisible(NULL),
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
  expect_gt(result$buffer_bytes, 0)
  expect_equal(result$buffer_bytes, sum(result$part_buffer_bytes))
  expect_equal(submitted$mapping, "RawParquet")
  expect_equal(submitted$tags, "r-object")
  expect_equal(submitted$ingest_if_not_exists, "batch-1")
  expect_true(submitted$skip_batching)
  expect_s3_class(submitted$token, "fabric_credential")
  expect_true(status_args$wait)
  expect_false(status_args$error_on_failure)
})

test_that("Kusto ingestion handles recover only live credentials", {
  credential <- structure(
    list(provider = function(...) "token"),
    class = "fabric_credential"
  )

  expect_identical(
    kusto_ingestion_credential(list(credential = credential)),
    credential
  )

  protected <- kusto_ingestion_credential_reference(credential)
  weak_reference <- protected$reference
  expect_identical(
    kusto_ingestion_credential(list(credential = weak_reference)),
    credential
  )

  protected$key <- NULL
  gc()
  error <- rlang::catch_cnd(
    kusto_ingestion_credential(list(credential = weak_reference)),
    classes = "error"
  )
  expect_s3_class(error, "fabric_kql_ingestion_credential_error")
  expect_match(conditionMessage(error), "supply token")

  expect_error(
    kusto_ingestion_credential(list()),
    class = "fabric_kql_ingestion_credential_error"
  )
})

test_that("staging cleanup adapters delete only their parent directory", {
  target <- list(
    workspace = "workspace-id",
    item = "item-id",
    path = "Files/.fabricQueryR/write-id/part.parquet"
  )
  credential <- structure(list(id = "credential"), class = "fabric_credential")
  calls <- list()
  fail <- FALSE
  local_mocked_bindings(
    onelake_delete_target = function(
      target,
      credential,
      recursive,
      is_directory
    ) {
      if (fail) {
        rlang::abort("cleanup failed")
      }
      calls[[length(calls) + 1L]] <<- list(
        target = target,
        credential = credential,
        recursive = recursive,
        is_directory = is_directory
      )
      invisible(TRUE)
    }
  )

  cleanup <- list(
    lakehouse = .fabric_lakehouse_remove_staging,
    warehouse = .fabric_warehouse_remove_staging,
    onelake = .fabric_onelake_remove_staging
  )
  expect_true(all(vapply(
    cleanup,
    function(adapter) adapter(target, credential),
    logical(1)
  )))
  expect_length(calls, 3L)
  for (call in calls) {
    expect_identical(call$target$path, "Files/.fabricQueryR/write-id")
    expect_identical(call$target$workspace, target$workspace)
    expect_identical(call$target$item, target$item)
    expect_identical(call$credential, credential)
    expect_true(call$recursive)
    expect_true(call$is_directory)
  }
  expect_identical(
    target$path,
    "Files/.fabricQueryR/write-id/part.parquet"
  )

  fail <- TRUE
  expect_false(any(vapply(
    cleanup,
    function(adapter) adapter(target, credential),
    logical(1)
  )))
})

test_that("Eventhouse writer requires a Storage credential for fixed tokens", {
  skip_if_not_installed("arrow")
  local_mocked_bindings(
    kusto_write_table_schema = function(...) character(),
    kusto_write_assert_identity_schema = function(...) invisible(NULL),
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
      if (identical(operation, "CreateTable")) {
        management <<- list(
          target = target,
          command = command,
          credential = credential,
          deadline = deadline,
          idempotent = idempotent,
          operation = operation
        )
      }
      if (identical(operation, "GetTableSchema")) {
        schema <- jsonlite::toJSON(
          list(
            OrderedColumns = list(
              list(Name = "id", CslType = "int"),
              list(Name = "amount", CslType = "real"),
              list(Name = "active", CslType = "bool"),
              list(Name = "observed_on", CslType = "datetime"),
              list(Name = "label", CslType = "string")
            )
          ),
          auto_unbox = TRUE
        )
        return(list(
          tables = list(tibble::tibble(TableName = "Raw new", Schema = schema)),
          request_id = "schema-request"
        ))
      }
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

  expect_equal(
    calls[1:5],
    c(
      "management",
      "management",
      "configuration",
      "configuration",
      "upload"
    )
  )
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

test_that("KQL identity writes validate the authoritative table schema", {
  skip_if_not_installed("arrow")
  command <- NULL
  local_mocked_bindings(
    kusto_export_management = function(
      target,
      command,
      credential,
      deadline,
      idempotent,
      operation
    ) {
      command <<- command
      schema <- jsonlite::toJSON(
        list(
          OrderedColumns = list(
            list(Name = "id", Type = "System.Int32"),
            list(Name = "label", CslType = "string")
          )
        ),
        auto_unbox = TRUE
      )
      list(
        tables = list(tibble::tibble(TableName = "Raw", Schema = schema)),
        request_id = "schema-request"
      )
    }
  )
  target <- kusto_resolve_target(
    "https://cluster.kusto.fabric.microsoft.com",
    "Telemetry"
  )
  actual <- kusto_write_table_schema(
    target,
    "Raw",
    fabric_credential(token = "test-token"),
    Sys.time() + 60
  )

  expect_equal(command, ".show table ['Raw'] schema as json")
  expect_identical(actual, c(id = "int", label = "string"))
  expect_no_error(kusto_write_assert_identity_schema(
    actual,
    arrow::schema(id = arrow::int32(), label = arrow::utf8()),
    c("id", "label")
  ))
})

test_that("KQL identity writes reject schema mismatches before staging", {
  skip_if_not_installed("arrow")
  configuration_calls <- 0L
  upload_calls <- 0L
  local_mocked_bindings(
    kusto_write_table_schema = function(...) c(ID = "int"),
    kusto_ingestion_configuration = function(...) {
      configuration_calls <<- configuration_calls + 1L
      kql_write_test_configuration()
    },
    onelake_upload_target = function(...) {
      upload_calls <<- upload_calls + 1L
      tibble::tibble()
    }
  )
  error <- rlang::catch_cnd(fabric_kql_write_table(
    "https://ingest-cluster.kusto.fabric.microsoft.com",
    "Raw",
    data.frame(id = 1L),
    database = "Telemetry",
    token = "test-token",
    storage_token = "storage-token"
  ))

  expect_s3_class(error, "fabric_kql_schema_error")
  expect_match(conditionMessage(error), "Data: id:int; target: ID:int")
  expect_identical(error$data_schema, c(id = "int"))
  expect_identical(error$table_schema, c(ID = "int"))
  expect_identical(configuration_calls, 0L)
  expect_identical(upload_calls, 0L)
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
  expect_error(
    kusto_write_create_table_command(
      "Events",
      arrow::schema(elapsed = arrow::duration("ms")),
      "elapsed",
      c(elapsed = "timespan")
    ),
    "unsupported Kusto types",
    class = "fabric_kql_schema_error"
  )
})

test_that("KQL table creation rejects unsupported Parquet temporal mappings", {
  skip_if_not_installed("arrow")
  unsupported <- list(
    arrow::time32("ms"),
    arrow::time64("us"),
    arrow::duration("ms")
  )

  for (type in unsupported) {
    error <- tryCatch(
      kusto_write_arrow_type(type, "elapsed"),
      error = identity
    )
    expect_s3_class(error, "fabric_kql_schema_error")
    expect_match(conditionMessage(error), "Kusto Parquet mapping", fixed = TRUE)
    expect_match(conditionMessage(error), "convert it", fixed = TRUE)
  }
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
    kusto_write_table_schema = function(...) character(),
    kusto_write_assert_identity_schema = function(...) invisible(NULL),
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
  expect_equal(result$buffer_bytes, sum(result$part_buffer_bytes))
})

test_that("Eventhouse writer rejects unsafe multi-file idempotency", {
  skip_if_not_installed("arrow")
  upload_calls <- 0L
  local_mocked_bindings(
    kusto_write_table_schema = function(...) character(),
    kusto_write_assert_identity_schema = function(...) invisible(NULL),
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
    kusto_write_table_schema = function(...) character(),
    kusto_write_assert_identity_schema = function(...) invisible(NULL),
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

test_that("Eventhouse writer does not submit Arrow buffers as rawSize", {
  skip_if_not_installed("arrow")
  upload_calls <- 0L
  submitted <- NULL
  local_mocked_bindings(
    kusto_write_table_schema = function(...) character(),
    kusto_write_assert_identity_schema = function(...) invisible(NULL),
    kusto_ingestion_configuration = function(...) {
      kql_write_test_configuration(max_data_size = 10000)
    },
    onelake_upload_target = function(...) {
      upload_calls <<- upload_calls + 1L
      tibble::tibble()
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
    data.frame(value = rep(strrep("compressible", 100), 1000)),
    database = "Telemetry",
    token = "test-token",
    storage_token = "storage-token"
  )

  expect_s3_class(result, "fabric_kql_write_result")
  expect_gt(result$buffer_bytes, 10000)
  expect_lte(result$bytes, 10000)
  expect_identical(upload_calls, 1L)
  expect_null(submitted$raw_sizes)
})

test_that("Eventhouse writer enforces the advertised total data size", {
  skip_if_not_installed("arrow")
  upload_calls <- 0L
  local_mocked_bindings(
    kusto_write_table_schema = function(...) character(),
    kusto_write_assert_identity_schema = function(...) invisible(NULL),
    kusto_ingestion_configuration = function(...) {
      kql_write_test_configuration(max_data_size = 1)
    },
    onelake_upload_target = function(...) {
      upload_calls <<- upload_calls + 1L
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
    "maxDataSize of 1 bytes",
    class = "fabric_kql_size_error"
  )
  expect_gt(error$bytes, error$max_data_size)
  expect_identical(error$max_data_size, 1)
  expect_equal(upload_calls, 0L)
})

test_that("KQL data-size validation accepts the exact service limit", {
  configuration <- kql_write_test_configuration(max_data_size = 100)
  serialized <- list(total_bytes = 100, file_count = 1L)

  expect_invisible(kusto_write_validate_configuration(
    serialized,
    configuration
  ))
})

test_that("Eventhouse writer enforces the advertised blob count", {
  skip_if_not_installed("arrow")
  upload_calls <- 0L
  local_mocked_bindings(
    kusto_write_table_schema = function(...) character(),
    kusto_write_assert_identity_schema = function(...) invisible(NULL),
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
    kusto_write_table_schema = function(...) character(),
    kusto_write_assert_identity_schema = function(...) invisible(NULL),
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
    kusto_write_table_schema = function(...) character(),
    kusto_write_assert_identity_schema = function(...) invisible(NULL),
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

test_that("Eventhouse writer shares one post-upload deadline", {
  skip_if_not_installed("arrow")
  now <- as.POSIXct("2026-08-24 12:00:00", tz = "UTC")
  expected_deadline <- now + 10
  clock <- function() now
  submission_timeout <- NULL
  submission_deadline <- NULL
  submission_clock <- NULL
  status_timeout <- NULL
  status_deadline <- NULL
  status_clock <- NULL
  submission_calls <- 0L
  status_calls <- 0L
  local_mocked_bindings(
    kusto_write_table_schema = function(...) character(),
    kusto_write_assert_identity_schema = function(...) invisible(NULL),
    kusto_ingestion_configuration = function(...) {
      kql_write_test_configuration()
    },
    onelake_upload_target = function(...) tibble::tibble(),
    fabric_kql_ingest = function(..., timeout, .deadline, .now) {
      submission_calls <<- submission_calls + 1L
      submission_timeout <<- timeout
      submission_deadline <<- .deadline
      submission_clock <<- .now
      now <<- now + 3
      kql_write_test_ingestion()
    },
    fabric_kql_ingestion_status = function(..., timeout, .deadline, .now) {
      status_calls <<- status_calls + 1L
      status_timeout <<- timeout
      status_deadline <<- .deadline
      status_clock <<- .now
      kql_write_test_status()
    }
  )

  result <- fabric_kql_write_table(
    "https://ingest-cluster.kusto.fabric.microsoft.com",
    "Raw",
    data.frame(id = 1L),
    database = "Telemetry",
    cleanup = FALSE,
    timeout = 10,
    token = "test-token",
    storage_token = "storage-token",
    .now = clock
  )

  expect_s3_class(result, "fabric_kql_write_result")
  expect_identical(submission_calls, 1L)
  expect_identical(status_calls, 1L)
  expect_equal(submission_timeout, 10)
  expect_equal(status_timeout, 7)
  expect_identical(submission_deadline, expected_deadline)
  expect_identical(status_deadline, expected_deadline)
  expect_identical(submission_clock, clock)
  expect_identical(status_clock, clock)
})

test_that("Eventhouse writer retains its handle when the deadline is spent", {
  skip_if_not_installed("arrow")
  now <- as.POSIXct("2026-08-24 12:00:00", tz = "UTC")
  ingestion <- kql_write_test_ingestion()
  submission_calls <- 0L
  status_calls <- 0L
  local_mocked_bindings(
    kusto_write_table_schema = function(...) character(),
    kusto_write_assert_identity_schema = function(...) invisible(NULL),
    kusto_ingestion_configuration = function(...) {
      kql_write_test_configuration()
    },
    onelake_upload_target = function(...) tibble::tibble(),
    fabric_kql_ingest = function(...) {
      submission_calls <<- submission_calls + 1L
      now <<- now + 10
      ingestion
    },
    fabric_kql_ingestion_status = function(...) {
      status_calls <<- status_calls + 1L
      kql_write_test_status()
    }
  )

  error <- rlang::catch_cnd(fabric_kql_write_table(
    "https://ingest-cluster.kusto.fabric.microsoft.com",
    "Raw",
    data.frame(id = 1L),
    database = "Telemetry",
    timeout = 10,
    token = "test-token",
    storage_token = "storage-token",
    .now = function() now
  ))

  expect_s3_class(error, "fabric_kql_write_timeout")
  expect_s3_class(error, "fabric_kql_write_ambiguous")
  expect_identical(error$ingestion, ingestion)
  expect_identical(error$operation_id, ingestion$id)
  expect_true(error$staging_retained)
  expect_identical(submission_calls, 1L)
  expect_identical(status_calls, 0L)
})

test_that("Eventhouse writer does not submit after its deadline", {
  skip_if_not_installed("arrow")
  started <- as.POSIXct("2026-08-24 12:00:00", tz = "UTC")
  clock_calls <- 0L
  submission_calls <- 0L
  status_calls <- 0L
  local_mocked_bindings(
    kusto_write_table_schema = function(...) character(),
    kusto_write_assert_identity_schema = function(...) invisible(NULL),
    kusto_ingestion_configuration = function(...) {
      kql_write_test_configuration()
    },
    onelake_upload_target = function(...) tibble::tibble(),
    fabric_kql_ingest = function(...) {
      submission_calls <<- submission_calls + 1L
      kql_write_test_ingestion()
    },
    fabric_kql_ingestion_status = function(...) {
      status_calls <<- status_calls + 1L
      kql_write_test_status()
    }
  )
  clock <- function() {
    clock_calls <<- clock_calls + 1L
    if (clock_calls == 1L) started else started + 10
  }

  error <- rlang::catch_cnd(fabric_kql_write_table(
    "https://ingest-cluster.kusto.fabric.microsoft.com",
    "Raw",
    data.frame(id = 1L),
    database = "Telemetry",
    timeout = 10,
    token = "test-token",
    storage_token = "storage-token",
    .now = clock
  ))

  expect_s3_class(error, "fabric_kql_write_timeout")
  expect_false(inherits(error, "fabric_kql_write_ambiguous"))
  expect_null(error$ingestion)
  expect_true(error$staging_retained)
  expect_identical(submission_calls, 0L)
  expect_identical(status_calls, 0L)
})

test_that("Eventhouse writer validates polling hooks before authentication", {
  error <- rlang::catch_cnd(fabric_kql_write_table(
    "https://ingest-cluster.kusto.fabric.microsoft.com",
    "Raw",
    data.frame(id = 1L),
    database = "Telemetry",
    token = function(...) stop("must not authenticate"),
    .now = 1
  ))

  expect_match(conditionMessage(error), ".now must be functions", fixed = TRUE)
})

test_that("confirmed failure follows the staging retention policy", {
  skip_if_not_installed("arrow")
  cleanup_calls <- 0L
  local_mocked_bindings(
    kusto_write_table_schema = function(...) character(),
    kusto_write_assert_identity_schema = function(...) invisible(NULL),
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
