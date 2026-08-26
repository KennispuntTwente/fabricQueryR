.kusto_ingestion_max_blobs <- 20L
.kusto_ingestion_max_size <- 6 * 1024^3
.kusto_ingestion_poll_floor <- 0.1
.kusto_ingestion_formats <- c(
  "avro",
  "csv",
  "json",
  "multijson",
  "orc",
  "parquet",
  "psv",
  "raw",
  "scsv",
  "sohsv",
  "tsv",
  "tsve",
  "txt",
  "w3clogfile"
)

#' Submit and monitor tracked Eventhouse ingestion
#'
#' Queue existing blob or OneLake files for ingestion into an existing KQL
#' table, then inspect or wait for the tracked per-file result. These functions
#' use Kusto's queued-ingestion REST API, which is currently in preview
#'
#' @section Sources and storage access:
#' `fabric_kql_ingest()` never uploads local data or serializes an R object.
#' Every `sources` value must already identify a file in blob storage or
#' OneLake, and `table` must already exist. Use [fabric_kql_write_table()] when
#' the data is a data frame, tibble, or Arrow object; that function performs
#' staging and can create the target with `create_if_missing = TRUE`.
#'
#' `sources` can be a character vector of storage connection strings, a data
#' frame with `url`, `source_id`, and optional `raw_size` columns, or a list of
#' records with those fields. The camel-case service names `sourceId` and
#' `rawSize` are also accepted. Character inputs use the parallel `source_ids`
#' and `raw_sizes` arguments
#'
#' Only existing `https://` or `abfss://` storage sources are accepted.
#' Nonpublic sources must include a Kusto-supported authentication suffix or
#' credential in the storage connection string. For example, append
#' `;impersonate` to a OneLake URL when the caller has permission to read it
#'
#' Source IDs are generated when omitted and are returned in the ingestion
#' handle. They identify blobs in status details, but they are not by themselves
#' an exactly-once guarantee
#'
#' @section Delivery and idempotency:
#' Queued ingestion has at-least-once delivery semantics. Submission is
#' therefore not automatically replayed after throttling, network failure, or
#' an ambiguous response. Retain the returned operation ID before starting
#' unrelated work
#'
#' For idempotent batch designs, set `ingest_if_not_exists` to one or more
#' stable keys. The function also attaches the corresponding `ingest-by:` tags
#' unless they are already present. A later submission with a matching key is
#' observable in detailed status instead of silently duplicating committed
#' extents. Idempotency checks can race when the same key is queued concurrently,
#' so serialize submissions that share a key
#'
#' @section Tracking and failures:
#' `fabric_kql_ingestion_status()` accepts the handle returned by
#' `fabric_kql_ingest()` or a raw operation ID plus the ingestion target. With
#' `wait = FALSE`, it returns one snapshot. With `wait = TRUE`, it polls until
#' every expected source is terminal or `timeout` is reached
#'
#' The returned status distinguishes `Succeeded`, `PartiallySucceeded`,
#' `Failed`, `Canceled`, `PartiallyCanceled`, and `InProgress`. Detailed blob
#' failures retain `error_code`, `failure_status`, and `message`. Source URLs
#' and raw service data are redacted so SAS tokens and embedded credentials are
#' not retained in the result. Set `error_on_failure = FALSE` to inspect a
#' terminal failure instead of receiving a typed condition carrying the same
#' status in `last_status`
#'
#' @section Limits and permissions:
#' The preview REST API accepts at most 20 blobs per request and a maximum of
#' 6 GB of uncompressed data. `raw_sizes` are validated and summed when all are
#' known. Supplying sizes also avoids a metadata read by the ingestion service
#'
#' The caller needs Kusto Table Ingestor permission on the target table and
#' Database User access. Reading nonpublic source files additionally requires
#' storage access through the authentication method in each storage connection
#' string. `delete_after_download = TRUE` also requires delete permission and
#' permanently removes successfully downloaded source blobs
#'
#' @param cluster Ingestion URI, or one Eventhouse or KQLDatabase record from
#'   [fabric_eventhouses()], [fabric_kql_databases()], or [fabric_item()]. A
#'   KQLDatabase record also supplies `database`. Use the **Ingestion URI**, not
#'   the Query URI, for direct character input
#' @param table One existing target KQL table name
#' @param sources Existing blob or OneLake storage connection strings, a data
#'   frame of source metadata, or a list of source records. See Sources and
#'   storage access
#' @param database Target KQL database display name. Omit it when `cluster` is a
#'   discovered KQLDatabase record
#' @param format Kusto ingestion format. Supported file formats include `csv`,
#'   `json`, `multijson`, `parquet`, `avro`, `orc`, and the documented delimited
#'   text formats
#' @param source_ids Optional GUID per character `sources` entry. Missing IDs
#'   are generated. Do not combine with structured source records
#' @param raw_sizes Optional uncompressed byte size per character `sources`
#'   entry. Use `NA` for an unknown size. Do not combine with structured source
#'   records
#' @param mapping Optional name of a predefined ingestion mapping whose kind
#'   matches `format`
#' @param tags Character vector of extent tags to attach
#' @param ingest_if_not_exists Stable keys used for idempotent ingestion. The
#'   service checks existing `ingest-by:` tags for these values
#' @param ignore_first_record Whether to skip the first record in every source,
#'   commonly used for CSV headers
#' @param skip_batching Whether to bypass normal Kusto ingestion batching. This
#'   can reduce latency but should be reserved for latency-critical workloads
#' @param delete_after_download Whether Kusto may delete a source after it has
#'   downloaded it. The default preserves source data
#' @param creation_time Optional ISO 8601 extent creation time, `Date`, or
#'   `POSIXt`. Align historical values with the target merge policy lookback
#' @param validation_policy Optional JSON string or named list describing CSV
#'   validation behavior
#' @param zip_pattern Optional regular expression selecting files inside ZIP
#'   sources
#' @param timestamp Optional ISO 8601 request timestamp, `Date`, or `POSIXt`
#' @param ingestion A `fabric_kql_ingestion` handle or a non-empty operation ID
#' @param details Whether status should include per-source detail records
#' @param wait Whether to poll until all expected sources are terminal
#' @param timeout Positive client-side limit in seconds. For a wait, this bounds
#'   the complete polling operation; otherwise it bounds the status request
#' @param poll_interval Minimum seconds between status requests while waiting
#' @param error_on_failure Whether a terminal failed or canceled ingestion
#'   raises a typed error. Use `FALSE` to inspect the returned status
#' @param tenant_id Microsoft Entra tenant ID. Defaults to
#'   `FABRICQUERYR_TENANT_ID`
#' @param client_id Microsoft Entra application/client ID. Defaults to
#'   `FABRICQUERYR_CLIENT_ID`, then the Azure CLI application ID
#' @param token Optional access token or token-provider function. Status calls
#'   reuse an in-process handle credential unless authentication is overridden
#' @param auth_args Additional sign-in options passed to
#'   [AzureAuth::get_azure_token()]
#' @param .deadline Internal absolute POSIX date-time used when a higher-level
#'   operation composes submission and status polling under one deadline
#' @param .sleep,.now Internal hooks for deterministic deadline and polling tests
#'
#' @return `fabric_kql_ingest()` returns a `fabric_kql_ingestion` handle with
#'   the operation ID and source IDs. `fabric_kql_ingestion_status()` returns a
#'   `fabric_kql_ingestion_status` record with normalized counts, state, UTC
#'   times, and an optional details tibble
#' @references
#' [Queued ingestion REST API (preview)](https://learn.microsoft.com/en-us/kusto/management/data-ingestion/queued-ingest-use-http?view=microsoft-fabric)
#'
#' [Queued ingestion status REST API (preview)](https://learn.microsoft.com/en-us/kusto/management/data-ingestion/queued-ingest-status-http?view=microsoft-fabric)
#'
#' [Supported ingestion formats](https://learn.microsoft.com/en-us/kusto/ingestion-supported-formats?view=microsoft-fabric)
#'
#' [Storage connection strings](https://learn.microsoft.com/en-us/kusto/api/connection-strings/storage-connection-strings?view=microsoft-fabric)
#'
#' [Data ingestion properties](https://learn.microsoft.com/en-us/kusto/ingestion-properties?view=microsoft-fabric)
#' @export
#'
#' @examples
#' \dontrun{
#' # Discover the KQL database and a Lakehouse containing staged CSV files
#' workspace <- fabric_workspaces()[[1L]]
#' database <- fabric_kql_databases(workspace)[[1L]]
#' lakehouse <- fabric_lakehouses(workspace)[[1L]]
#' files <- fabric_onelake_list(
#'   workspace,
#'   lakehouse,
#'   path = "Files/events"
#' )
#' csv_file <- files[grepl("[.]csv$", files$path), ][1L, ]
#'
#' # Build the source URI from discovered IDs and the listed file path
#' source <- paste0(
#'   "https://onelake.dfs.fabric.microsoft.com/",
#'   workspace$id, "/", lakehouse$id, "/", csv_file$path[[1L]],
#'   ";impersonate"
#' )
#'
#' # Choose an existing target and CSV mapping from the KQL database explorer
#' table <- Sys.getenv("FABRIC_KQL_TABLE")
#' mapping <- Sys.getenv("FABRIC_KQL_CSV_MAPPING")
#'
#' # Queue the file once using a stable ingest-if-not-exists key
#' ingestion <- fabric_kql_ingest(
#'   database,
#'   table = table,
#'   sources = source,
#'   format = "csv",
#'   mapping = mapping,
#'   ignore_first_record = TRUE,
#'   ingest_if_not_exists = paste0("file:", csv_file$path[[1L]])
#' )
#'
#' # Wait for every submitted file to reach a terminal ingestion state
#' result <- fabric_kql_ingestion_status(
#'   ingestion,
#'   wait = TRUE,
#'   timeout = 900
#' )
#' result$state
#' result$details
#' }
fabric_kql_ingest <- function(
  cluster,
  table,
  sources,
  database = NULL,
  format,
  source_ids = NULL,
  raw_sizes = NULL,
  mapping = NULL,
  tags = character(),
  ingest_if_not_exists = character(),
  ignore_first_record = FALSE,
  skip_batching = FALSE,
  delete_after_download = FALSE,
  creation_time = NULL,
  validation_policy = NULL,
  zip_pattern = NULL,
  timestamp = NULL,
  timeout = 60,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  token = NULL,
  auth_args = list(),
  .deadline = NULL,
  .now = Sys.time
) {
  # 1 Validate the target and request metadata -----------------------------------------------------

  # Complete validation before authentication so local mistakes cannot prompt for sign-in

  if (missing(table)) {
    .fabric_abort("table is required")
  }
  if (missing(sources)) {
    .fabric_abort("sources is required")
  }
  if (missing(format)) {
    .fabric_abort("format is required")
  }
  target <- kusto_resolve_ingestion_target(
    cluster,
    database,
    table
  )
  format <- kusto_ingestion_format(format)
  blobs <- kusto_ingestion_sources(sources, source_ids, raw_sizes)
  mapping <- kusto_ingestion_optional_text(mapping, "mapping")
  tags <- kusto_ingestion_text_vector(tags, "tags")
  ingest_if_not_exists <- kusto_ingestion_text_vector(
    ingest_if_not_exists,
    "ingest_if_not_exists"
  )
  if (any(startsWith(tolower(ingest_if_not_exists), "ingest-by:"))) {
    .fabric_abort(
      "ingest_if_not_exists values must omit the 'ingest-by:' prefix"
    )
  }
  idempotency_tags <- paste0("ingest-by:", ingest_if_not_exists)
  tags <- unique(c(tags, idempotency_tags))
  kusto_ingestion_flag(ignore_first_record, "ignore_first_record")
  kusto_ingestion_flag(skip_batching, "skip_batching")
  kusto_ingestion_flag(delete_after_download, "delete_after_download")
  creation_time <- kusto_ingestion_datetime(
    creation_time,
    "creation_time",
    allow_date = TRUE
  )
  timestamp <- kusto_ingestion_datetime(
    timestamp,
    "timestamp",
    allow_date = FALSE
  )
  validation_policy <- kusto_ingestion_validation_policy(
    validation_policy,
    format
  )
  zip_pattern <- kusto_ingestion_optional_text(zip_pattern, "zip_pattern")
  kusto_ingestion_number(timeout, "timeout", minimum = 0, strict = TRUE)
  if (!is.function(.now)) {
    .fabric_abort(".now must be a function")
  }

  # 2 Build the documented preview payload --------------------------------------------------------

  # I() preserves one-element arrays when jsonlite auto-unboxes scalar properties

  properties <- list(
    format = format,
    enableTracking = TRUE,
    skipBatching = skip_batching,
    deleteAfterDownload = delete_after_download,
    ignoreFirstRecord = ignore_first_record
  )
  if (length(tags)) {
    properties$tags <- I(tags)
  }
  if (length(ingest_if_not_exists)) {
    properties$ingestIfNotExists <- I(ingest_if_not_exists)
  }
  properties$ingestionMappingReference <- mapping
  properties$creationTime <- creation_time
  properties$validationPolicy <- validation_policy
  properties$zipPattern <- zip_pattern
  properties <- Filter(Negate(is.null), properties)

  body <- list(
    blobs = I(lapply(blobs, function(blob) {
      Filter(
        Negate(is.null),
        list(
          url = blob$url,
          sourceId = blob$source_id,
          rawSize = blob$raw_size
        )
      )
    })),
    properties = properties
  )
  if (!is.null(timestamp)) {
    body$timestamp <- timestamp
  }

  # 3 Submit once and retain the tracking context --------------------------------------------------

  # Queued ingestion is at-least-once, so replaying POST after an ambiguous failure is unsafe

  credential <- fabric_credential(
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args
  )
  response <- kusto_ingestion_submit(
    target,
    body,
    credential,
    timeout = timeout,
    deadline = .deadline,
    .now = .now
  )
  kusto_ingestion_handle(
    operation_id = response$operation_id,
    target = target,
    blobs = blobs,
    format = format,
    mapping = mapping,
    tags = tags,
    ingest_if_not_exists = ingest_if_not_exists,
    credential = credential,
    request_id = response$request_id
  )
}

#' @rdname fabric_kql_ingest
#' @export
fabric_kql_ingestion_status <- function(
  ingestion,
  cluster = NULL,
  database = NULL,
  table = NULL,
  details = TRUE,
  wait = FALSE,
  timeout = 900,
  poll_interval = 2,
  error_on_failure = TRUE,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  token = NULL,
  auth_args = list(),
  .sleep = Sys.sleep,
  .now = Sys.time,
  .deadline = NULL
) {
  # 1 Validate polling behavior and recover the operation context ---------------------------------

  kusto_ingestion_flag(details, "details")
  kusto_ingestion_flag(wait, "wait")
  kusto_ingestion_flag(error_on_failure, "error_on_failure")
  kusto_ingestion_number(timeout, "timeout", minimum = 0, strict = TRUE)
  kusto_ingestion_number(
    poll_interval,
    "poll_interval",
    minimum = .kusto_ingestion_poll_floor
  )
  if (!is.function(.sleep) || !is.function(.now)) {
    .fabric_abort(".sleep and .now must be functions")
  }
  override_auth <- !missing(tenant_id) ||
    !missing(client_id) ||
    !is.null(token) ||
    length(auth_args) > 0L
  context <- kusto_ingestion_context(
    ingestion,
    cluster = cluster,
    database = database,
    table = table,
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args,
    override_auth = override_auth
  )
  deadline <- .deadline %||% (.now() + timeout)

  # 2 Read one snapshot or poll until terminal -----------------------------------------------------

  # Status GETs are idempotent and can safely retry transient failures and throttling

  last <- NULL
  progress <- if (isTRUE(wait)) {
    .fabric_poll_progress("Kusto ingestion", context$id)
  } else {
    NULL
  }
  repeat {
    last <- tryCatch(
      kusto_ingestion_get_status(
        context,
        details = details,
        deadline = deadline,
        .now = .now
      ),
      fabric_http_deadline_error = function(error) {
        kusto_ingestion_timeout_error(context, timeout, last, error)
      }
    )
    .fabric_poll_progress_update(progress, last$state)
    if (!isTRUE(wait) || isTRUE(last$complete)) {
      break
    }

    remaining <- as.numeric(difftime(deadline, .now(), units = "secs"))
    if (!is.finite(remaining) || remaining <= 0) {
      kusto_ingestion_timeout_error(context, timeout, last)
    }
    delay <- max(
      .kusto_ingestion_poll_floor,
      poll_interval,
      last$retry_after %||% 0
    )
    .sleep(min(delay, remaining))
    remaining <- as.numeric(difftime(deadline, .now(), units = "secs"))
    if (!is.finite(remaining) || remaining <= 0) {
      kusto_ingestion_timeout_error(context, timeout, last)
    }
  }

  # 3 Return or signal the terminal outcome --------------------------------------------------------

  # Conditions carry the complete safe result so batch callers can diagnose partial failures

  if (
    isTRUE(error_on_failure) &&
      isTRUE(last$complete) &&
      last$state %in%
        c(
          "Failed",
          "PartiallySucceeded",
          "Canceled",
          "PartiallyCanceled"
        )
  ) {
    kusto_ingestion_failure_error(last)
  }
  .fabric_poll_progress_done(progress)
  last
}

#' Print a tracked Kusto ingestion handle
#'
#' @param x A `fabric_kql_ingestion` handle
#' @param ... Unused
#' @return `x`, invisibly
#' @export
print.fabric_kql_ingestion <- function(x, ...) {
  .fabric_print(
    "fabric_kql_ingestion",
    list(
      operation = x$id,
      target = paste0(x$database, ".", x$table),
      sources = x$source_count,
      format = x$format
    )
  )
  invisible(x)
}

#' Print tracked Kusto ingestion status
#'
#' @param x A `fabric_kql_ingestion_status` record
#' @param ... Unused
#' @return `x`, invisibly
#' @export
print.fabric_kql_ingestion_status <- function(x, ...) {
  .fabric_print(
    "fabric_kql_ingestion_status",
    list(
      operation = x$operation_id,
      state = x$state,
      blobs = paste0(
        x$succeeded,
        " succeeded, ",
        x$failed,
        " failed, ",
        x$in_progress,
        " in progress, ",
        x$canceled,
        " canceled"
      )
    )
  )
  invisible(x)
}

# Resolve an ingestion URI or discovery record. Returns a trusted service root
# plus validated database and table names used by submission and status calls
kusto_resolve_ingestion_target <- function(
  cluster,
  database,
  table
) {
  record <- fabric_as_record(cluster)
  if (!is.null(record)) {
    type <- tolower(fabric_record_value(record, "type") %||% "")
    if (!type %in% c("eventhouse", "kqldatabase")) {
      .fabric_abort(
        "cluster discovery record must be an Eventhouse or KQLDatabase item"
      )
    }
    cluster <- fabric_record_value(
      record,
      "ingestion_service_uri",
      "ingestionServiceUri"
    )
    if (is.null(database) && identical(type, "kqldatabase")) {
      database <- fabric_record_value(
        record,
        "database_name",
        "displayName",
        "display_name"
      )
    }
  }

  endpoint <- kusto_ingestion_optional_text(cluster, "cluster", required = TRUE)
  database <- kusto_ingestion_target_name(database, "database")
  table <- kusto_ingestion_target_name(table, "table")
  endpoint <- sub("/+$", "", trimws(endpoint))
  parsed <- try(httr2::url_parse(endpoint), silent = TRUE)
  if (
    inherits(parsed, "try-error") ||
      !identical(tolower(parsed$scheme %||% ""), "https") ||
      is.null(parsed$hostname) ||
      !nzchar(parsed$hostname) ||
      nzchar(parsed$username %||% "") ||
      nzchar(parsed$password %||% "") ||
      !(parsed$port %||% "") %in% c("", "443") ||
      !(parsed$path %||% "") %in% c("", "/") ||
      length(parsed$query %||% list()) > 0L ||
      nzchar(parsed$fragment %||% "")
  ) {
    .fabric_abort(paste0(
      "cluster must be a valid HTTPS Kusto ingestion-service origin using ",
      "the default port (443)"
    ))
  }
  list(
    url = endpoint,
    database = database,
    table = table
  )
}

# Normalize all accepted source shapes. Returns service-ready records with a
# unique GUID and optional uncompressed size for every source URL
kusto_ingestion_sources <- function(sources, source_ids, raw_sizes) {
  structured <- is.data.frame(sources) ||
    is.list(sources) && !is.character(sources)
  if (structured && (!is.null(source_ids) || !is.null(raw_sizes))) {
    .fabric_abort(
      "source_ids and raw_sizes cannot be combined with structured sources"
    )
  }

  if (is.character(sources)) {
    records <- lapply(sources, function(url) list(url = url))
    count <- length(records)
    if (!is.null(source_ids)) {
      if (!is.character(source_ids) || length(source_ids) != count) {
        .fabric_abort("source_ids must contain one character value per source")
      }
      for (index in seq_len(count)) {
        records[[index]]$source_id <- source_ids[[index]]
      }
    }
    if (!is.null(raw_sizes)) {
      if (!is.numeric(raw_sizes) || length(raw_sizes) != count) {
        .fabric_abort("raw_sizes must contain one numeric value per source")
      }
      for (index in seq_len(count)) {
        records[[index]]$raw_size <- raw_sizes[[index]]
      }
    }
  } else if (is.data.frame(sources)) {
    if (!"url" %in% names(sources)) {
      .fabric_abort("structured sources must include a url field")
    }
    records <- lapply(seq_len(nrow(sources)), function(index) {
      as.list(sources[index, , drop = FALSE])
    })
  } else if (is.list(sources)) {
    if (!is.null(names(sources)) && "url" %in% names(sources)) {
      sources <- list(sources)
    }
    if (!all(vapply(sources, is.list, logical(1)))) {
      .fabric_abort("structured sources must be a list of source records")
    }
    records <- sources
  } else {
    .fabric_abort(
      "sources must be character storage URLs or structured source records"
    )
  }

  if (!length(records)) {
    .fabric_abort("sources must contain at least one storage source")
  }
  if (length(records) > .kusto_ingestion_max_blobs) {
    .fabric_abort(sprintf(
      "sources exceeds the queued-ingestion limit of %d blobs per request",
      .kusto_ingestion_max_blobs
    ))
  }

  normalized <- lapply(records, function(record) {
    if (is.data.frame(record)) {
      record <- as.list(record[1L, , drop = FALSE])
    }
    list(
      url = kusto_ingestion_source_url(
        kusto_ingestion_record_value(record, "url")
      ),
      source_id = kusto_ingestion_record_value(
        record,
        "source_id",
        "sourceId"
      ),
      raw_size = kusto_ingestion_record_value(
        record,
        "raw_size",
        "rawSize"
      )
    )
  })

  existing_ids <- character()
  for (index in seq_along(normalized)) {
    source_id <- normalized[[index]]$source_id
    if (is.factor(source_id)) {
      source_id <- as.character(source_id)
    }
    missing_id <- is.null(source_id) ||
      length(source_id) == 1L && is.na(source_id)
    if (missing_id) {
      source_id <- kusto_ingestion_source_id(existing_ids)
    }
    if (
      !is.character(source_id) ||
        length(source_id) != 1L ||
        is.na(source_id) ||
        !fabric_is_guid(source_id)
    ) {
      .fabric_abort("every source_id must be a GUID or missing")
    }
    normalized[[index]]$source_id <- tolower(source_id)
    existing_ids <- c(existing_ids, tolower(source_id))
  }
  if (anyDuplicated(existing_ids)) {
    .fabric_abort("source_ids must be unique within an ingestion request")
  }

  for (index in seq_along(normalized)) {
    size <- normalized[[index]]$raw_size
    if (is.null(size) || length(size) == 1L && is.na(size)) {
      normalized[[index]]$raw_size <- NULL
      next
    }
    kusto_ingestion_number(
      size,
      sprintf("raw_size for source %d", index),
      minimum = 0,
      whole = TRUE
    )
    if (size > .kusto_ingestion_max_size) {
      .fabric_abort(sprintf(
        "raw_size for source %d exceeds the 6 GB ingestion limit",
        index
      ))
    }
    normalized[[index]]$raw_size <- as.numeric(size)
  }
  sizes <- vapply(
    normalized,
    function(record) record$raw_size %||% NA_real_,
    numeric(1)
  )
  if (!anyNA(sizes) && sum(sizes) > .kusto_ingestion_max_size) {
    .fabric_abort("the known raw_sizes exceed the 6 GB ingestion limit")
  }
  normalized
}

# Read one field and reject conflicting aliases. Returns NULL for absent fields
kusto_ingestion_record_value <- function(record, ...) {
  fields <- c(...)
  present <- fields[fields %in% names(record)]
  if (!length(present)) {
    return(NULL)
  }
  values <- lapply(present, function(field) record[[field]])
  if (
    length(values) > 1L &&
      !all(vapply(
        values[-1L],
        identical,
        logical(1),
        values[[1L]]
      ))
  ) {
    .fabric_abort(paste0(
      "source record contains conflicting aliases: ",
      paste(present, collapse = ", ")
    ))
  }
  values[[1L]]
}

# Validate a Kusto storage connection string without reconstructing it. Returns
# the original value so SAS query order and authentication suffixes are intact
kusto_ingestion_source_url <- function(value) {
  value <- kusto_ingestion_optional_text(value, "source url", required = TRUE)
  if (nchar(value, type = "bytes") > 32768L) {
    .fabric_abort("source url must not exceed 32,768 bytes")
  }
  parsed <- try(httr2::url_parse(value), silent = TRUE)
  scheme <- if (inherits(parsed, "try-error")) {
    ""
  } else {
    tolower(parsed$scheme %||% "")
  }
  valid_credentials <- if (identical(scheme, "abfss")) {
    nzchar(parsed$username %||% "") && !nzchar(parsed$password %||% "")
  } else {
    !nzchar(parsed$username %||% "") && !nzchar(parsed$password %||% "")
  }
  if (
    inherits(parsed, "try-error") ||
      !scheme %in% c("https", "abfss") ||
      is.null(parsed$hostname) ||
      !nzchar(parsed$hostname) ||
      !valid_credentials ||
      !nzchar(parsed$path %||% "") ||
      identical(parsed$path, "/") ||
      nzchar(parsed$fragment %||% "")
  ) {
    .fabric_abort(
      "each source url must be a valid https:// or abfss:// storage source"
    )
  }
  value
}

# Generate a version-4-shaped GUID not already present in `exclude`. Returns a
# lowercase identifier used only for source correlation, not cryptography
kusto_ingestion_source_id <- function(exclude = character()) {
  hex <- c(as.character(0:9), letters[1:6])
  repeat {
    value <- sample(hex, 32L, replace = TRUE)
    value[[13L]] <- "4"
    value[[17L]] <- sample(c("8", "9", "a", "b"), 1L)
    id <- paste0(
      paste0(value[1:8], collapse = ""),
      "-",
      paste0(value[9:12], collapse = ""),
      "-",
      paste0(value[13:16], collapse = ""),
      "-",
      paste0(value[17:20], collapse = ""),
      "-",
      paste0(value[21:32], collapse = "")
    )
    if (!tolower(id) %in% tolower(exclude)) {
      return(id)
    }
  }
}

# Send the non-retriable submission request. Returns the validated operation ID
# plus a service request ID used to construct the public handle
kusto_ingestion_submit <- function(
  target,
  body,
  credential,
  timeout,
  deadline = NULL,
  .now = Sys.time
) {
  client_request_id <- .kusto_next_ingestion_request_id("Submit")
  request <- httr2::request(kusto_ingestion_url(target)) |>
    httr2::req_headers(
      Accept = "application/json",
      `x-ms-app` = "fabricQueryR",
      `x-ms-client-version` = as.character(
        utils::packageVersion("fabricQueryR")
      ),
      `x-ms-client-request-id` = client_request_id
    ) |>
    httr2::req_body_json(
      body,
      auto_unbox = TRUE,
      digits = 22,
      null = "null"
    ) |>
    httr2::req_timeout(timeout)
  response <- tryCatch(
    .httr2_perform(
      request,
      credential = credential,
      audience = .fabric_audience$kusto,
      idempotent = FALSE,
      deadline = deadline,
      .now = .now
    ),
    error = function(error) {
      safe_error <- kusto_storage_redact_condition(error)
      .fabric_abort(
        paste0(
          "Queued ingestion submission failed before a tracking ID was ",
          "received. The request was not replayed because the service may ",
          "already have accepted it; verify the target before resubmitting."
        ),
        class = c(
          "fabric_kql_ingestion_submission_error",
          "fabric_kql_ingestion_error"
        ),
        status = safe_error$status %||% NULL,
        response_metadata = safe_error$response_metadata %||% NULL,
        parent = safe_error,
        call = NULL,
        .trace = FALSE
      )
    }
  )
  payload <- tryCatch(
    httr2::resp_body_json(
      response,
      simplifyVector = FALSE,
      bigint_as_char = TRUE
    ),
    error = function(error) {
      kusto_ingestion_protocol_error(
        "Kusto accepted ingestion but returned invalid JSON",
        parent = error
      )
    }
  )
  operation_id <- payload$ingestionOperationId
  kusto_ingestion_operation_id(
    operation_id,
    message = paste0(
      "Kusto accepted ingestion without a non-empty ingestionOperationId; ",
      "tracked submission requires enableTracking = true"
    )
  )
  list(
    operation_id = operation_id,
    request_id = httr2::resp_header(response, "x-ms-request-id") %||%
      client_request_id
  )
}

# Return a unique Kusto client request ID for submission and status traffic
.kusto_next_ingestion_request_id <- local({
  counter <- 0L
  function(operation) {
    counter <<- if (counter == .Machine$integer.max) 1L else counter + 1L
    paste0(
      "fabricQueryR.Ingestion.",
      operation,
      ";",
      format(Sys.time(), "%Y%m%d%H%M%OS6", tz = "UTC"),
      "-",
      Sys.getpid(),
      "-",
      counter
    )
  }
})

# Create a reusable tracking handle with a weak credential reference. Source
# URLs are redacted before retention so serialized handles cannot expose SAS
kusto_ingestion_handle <- function(
  operation_id,
  target,
  blobs,
  format,
  mapping,
  tags,
  ingest_if_not_exists,
  credential,
  request_id
) {
  reference <- kusto_ingestion_credential_reference(credential)
  sources <- tibble::tibble(
    source_id = vapply(blobs, `[[`, character(1), "source_id"),
    url = vapply(
      blobs,
      function(blob) kusto_storage_redact_text(blob$url),
      character(1)
    ),
    raw_size = vapply(
      blobs,
      function(blob) blob$raw_size %||% NA_real_,
      numeric(1)
    )
  )
  structure(
    list(
      id = operation_id,
      operation_id = operation_id,
      endpoint = target$url,
      database = target$database,
      table = target$table,
      source_count = length(blobs),
      sources = sources,
      format = format,
      mapping = mapping,
      tags = tags,
      ingest_if_not_exists = ingest_if_not_exists,
      request_id = request_id,
      submitted_at = Sys.time(),
      credential = reference$reference,
      .credential_key = reference$key
    ),
    class = "fabric_kql_ingestion"
  )
}

# Reconstruct target and authentication from a handle or raw operation ID
kusto_ingestion_context <- function(
  ingestion,
  cluster,
  database,
  table,
  tenant_id,
  client_id,
  token,
  auth_args,
  override_auth
) {
  if (inherits(ingestion, "fabric_kql_ingestion_status")) {
    ingestion <- ingestion$ingestion
  }
  if (inherits(ingestion, "fabric_kql_ingestion")) {
    if (!is.null(cluster) || !is.null(database) || !is.null(table)) {
      .fabric_abort(
        "cluster, database, and table cannot be combined with an ingestion handle"
      )
    }
    credential <- if (isTRUE(override_auth)) {
      fabric_credential(
        tenant_id = tenant_id,
        client_id = client_id,
        token = token,
        auth_args = auth_args
      )
    } else {
      kusto_ingestion_credential(ingestion)
    }
    target <- kusto_resolve_ingestion_target(
      ingestion$endpoint,
      ingestion$database,
      ingestion$table
    )
    return(list(
      id = ingestion$id,
      target = target,
      expected_count = ingestion$source_count,
      credential = credential,
      ingestion = ingestion
    ))
  }

  kusto_ingestion_operation_id(ingestion)
  if (is.null(cluster)) {
    .fabric_abort("cluster is required when ingestion is a raw operation ID")
  }
  target <- kusto_resolve_ingestion_target(
    cluster,
    database,
    table
  )
  credential <- fabric_credential(
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args
  )
  list(
    id = ingestion,
    target = target,
    expected_count = NA_integer_,
    credential = credential,
    ingestion = NULL
  )
}

# Retrieve one status response and normalize it into the public status record
kusto_ingestion_get_status <- function(
  context,
  details,
  deadline,
  .now = Sys.time
) {
  client_request_id <- .kusto_next_ingestion_request_id("Status")
  request <- httr2::request(
    kusto_ingestion_url(context$target, context$id)
  ) |>
    httr2::req_url_query(details = if (details) "true" else "false") |>
    httr2::req_headers(
      Accept = "application/json",
      `x-ms-app` = "fabricQueryR",
      `x-ms-client-version` = as.character(
        utils::packageVersion("fabricQueryR")
      ),
      `x-ms-client-request-id` = client_request_id
    )
  response <- .httr2_perform(
    request,
    credential = context$credential,
    audience = .fabric_audience$kusto,
    idempotent = TRUE,
    deadline = deadline,
    .now = .now
  )
  payload <- tryCatch(
    httr2::resp_body_json(
      response,
      simplifyVector = FALSE,
      bigint_as_char = TRUE
    ),
    error = function(error) {
      kusto_ingestion_protocol_error(
        "Kusto returned invalid ingestion status JSON",
        parent = error
      )
    }
  )
  kusto_ingestion_status_record(
    payload,
    context,
    retry_after = .httr2_retry_after(response),
    request_id = httr2::resp_header(response, "x-ms-request-id") %||%
      client_request_id,
    details_requested = details
  )
}

# Normalize service status counts and per-blob details. Returns a safe public
# record that makes partial failure and completion explicit
kusto_ingestion_status_record <- function(
  payload,
  context,
  retry_after = NULL,
  request_id = NULL,
  details_requested = TRUE
) {
  if (!is.list(payload) || !is.list(payload$status)) {
    kusto_ingestion_protocol_error(
      "Kusto ingestion status response has no status object"
    )
  }
  status_names <- names(payload$status)
  if (is.null(status_names) || anyNA(status_names)) {
    kusto_ingestion_protocol_error(
      "Kusto ingestion status counts must be named"
    )
  }
  counts <- vapply(
    payload$status,
    function(value) {
      parsed <- suppressWarnings(as.numeric(value))
      if (
        length(parsed) != 1L ||
          is.na(parsed) ||
          !is.finite(parsed) ||
          parsed < 0 ||
          parsed != floor(parsed)
      ) {
        kusto_ingestion_protocol_error(
          "Kusto ingestion status contains an invalid count"
        )
      }
      parsed
    },
    numeric(1)
  )
  names(counts) <- status_names
  count <- function(name) {
    index <- match(tolower(name), tolower(names(counts)))
    if (is.na(index)) 0 else unname(counts[[index]])
  }
  succeeded <- count("Succeeded")
  failed <- count("Failed")
  in_progress <- count("InProgress")
  canceled <- count("Canceled")
  terminal <- succeeded + failed + canceled
  expected <- context$expected_count
  complete <- in_progress == 0 &&
    terminal > 0 &&
    (is.na(expected) || terminal >= expected)
  state <- kusto_ingestion_state(
    succeeded,
    failed,
    in_progress,
    canceled,
    complete
  )
  detail_rows <- kusto_ingestion_details(
    payload$details,
    requested = details_requested
  )
  safe_payload <- kusto_storage_redact_object(payload)
  structure(
    list(
      operation_id = context$id,
      database = context$target$database,
      table = context$target$table,
      state = state,
      complete = complete,
      succeeded = succeeded,
      failed = failed,
      in_progress = in_progress,
      canceled = canceled,
      total = terminal + in_progress,
      expected = expected,
      counts = counts,
      start_time = kusto_ingestion_response_time(
        payload$startTime,
        "startTime"
      ),
      last_updated = kusto_ingestion_response_time(
        payload$lastUpdated,
        "lastUpdated",
        optional = TRUE
      ),
      details = detail_rows,
      retry_after = retry_after,
      request_id = request_id,
      ingestion = context$ingestion,
      raw = safe_payload
    ),
    class = "fabric_kql_ingestion_status"
  )
}

# Convert detailed service records into a fixed tibble. Returned URLs and error
# messages are redacted before they can be printed or serialized
kusto_ingestion_details <- function(value, requested) {
  empty <- function() {
    tibble::tibble(
      source_id = character(),
      url = character(),
      status = character(),
      start_time = as.POSIXct(character(), tz = "UTC"),
      last_updated = as.POSIXct(character(), tz = "UTC"),
      error_code = character(),
      failure_status = character(),
      message = character()
    )
  }
  if (is.null(value)) {
    if (isTRUE(requested)) {
      kusto_ingestion_protocol_error(
        "Kusto omitted ingestion details when details = true"
      )
    }
    return(empty())
  }
  if (!is.list(value)) {
    kusto_ingestion_protocol_error("Kusto ingestion details must be an array")
  }
  if (!length(value)) {
    return(empty())
  }
  if (!all(vapply(value, is.list, logical(1)))) {
    kusto_ingestion_protocol_error(
      "Kusto ingestion details contain a malformed record"
    )
  }
  text <- function(record, field) {
    item <- record[[field]]
    if (is.null(item)) {
      return(NA_character_)
    }
    if (!is.character(item) || length(item) != 1L || is.na(item)) {
      kusto_ingestion_protocol_error(sprintf(
        "Kusto ingestion detail field %s must be text",
        field
      ))
    }
    kusto_storage_redact_text(item)
  }
  tibble::tibble(
    source_id = vapply(value, text, character(1), "sourceId"),
    url = vapply(value, text, character(1), "url"),
    status = vapply(value, text, character(1), "status"),
    start_time = kusto_ingestion_time_vector(value, "startTime"),
    last_updated = kusto_ingestion_time_vector(value, "lastUpdated"),
    error_code = vapply(value, text, character(1), "errorCode"),
    failure_status = vapply(value, text, character(1), "failureStatus"),
    message = vapply(value, text, character(1), "details")
  )
}

# Infer the normalized aggregate state from documented count categories
kusto_ingestion_state <- function(
  succeeded,
  failed,
  in_progress,
  canceled,
  complete
) {
  if (!isTRUE(complete) || in_progress > 0) {
    return("InProgress")
  }
  if (failed > 0 && succeeded > 0) {
    return("PartiallySucceeded")
  }
  if (failed > 0) {
    return("Failed")
  }
  if (canceled > 0 && succeeded > 0) {
    return("PartiallyCanceled")
  }
  if (canceled > 0) {
    return("Canceled")
  }
  "Succeeded"
}

# Build the documented queued-ingestion route using encoded target segments
kusto_ingestion_url <- function(target, operation_id = NULL) {
  segments <- c(target$database, target$table)
  if (!is.null(operation_id)) {
    segments <- c(segments, operation_id)
  }
  encoded <- vapply(
    segments,
    utils::URLencode,
    character(1),
    reserved = TRUE,
    USE.NAMES = FALSE
  )
  paste0(
    target$url,
    "/v1/rest/ingestion/queued/",
    paste(encoded, collapse = "/")
  )
}

# Hold a credential behind a weak reference so serialized handles contain no
# bearer tokens or authentication callbacks
kusto_ingestion_credential_reference <- function(credential) {
  key <- new.env(parent = emptyenv())
  list(
    reference = rlang::new_weakref(key, credential),
    key = key
  )
}

# Recover the in-process credential from an ingestion handle
kusto_ingestion_credential <- function(ingestion) {
  stored <- ingestion$credential
  if (inherits(stored, "fabric_credential")) {
    return(stored)
  }
  credential <- if (rlang::is_weakref(stored)) {
    rlang::wref_value(stored)
  } else {
    NULL
  }
  if (is.null(credential)) {
    .fabric_abort(
      paste0(
        "This Kusto ingestion handle no longer has an in-process credential; ",
        "supply token, tenant_id, or other authentication arguments"
      ),
      class = c(
        "fabric_kql_ingestion_credential_error",
        "fabric_kql_ingestion_error"
      )
    )
  }
  credential
}

# Raise a typed terminal failure that retains the complete safe status record
kusto_ingestion_failure_error <- function(status) {
  failures <- status$details[
    status$details$status %in% c("Failed", "Canceled"),
    ,
    drop = FALSE
  ]
  summaries <- if (nrow(failures)) {
    utils::head(
      vapply(
        seq_len(nrow(failures)),
        function(index) {
          paste0(
            failures$source_id[[index]] %||% "unknown source",
            ": ",
            failures$error_code[[index]] %||% failures$status[[index]],
            if (
              !is.na(failures$message[[index]]) &&
                nzchar(failures$message[[index]])
            ) {
              paste0(" (", failures$message[[index]], ")")
            } else {
              ""
            }
          )
        },
        character(1)
      ),
      3L
    )
  } else {
    character()
  }
  message <- paste0(
    "Kusto ingestion ",
    status$operation_id,
    " completed with state ",
    status$state,
    " (",
    status$succeeded,
    " succeeded, ",
    status$failed,
    " failed, ",
    status$canceled,
    " canceled)",
    if (length(summaries)) {
      paste0(": ", paste(summaries, collapse = "; "))
    } else {
      ""
    }
  )
  class <- if (identical(status$state, "PartiallySucceeded")) {
    "fabric_kql_ingestion_partial_failure"
  } else {
    "fabric_kql_ingestion_failure"
  }
  .fabric_abort(
    message,
    class = c(class, "fabric_kql_ingestion_error"),
    operation_id = status$operation_id,
    last_status = status,
    failures = failures
  )
}

# Raise a typed client timeout without implying that the service operation was
# canceled or failed
kusto_ingestion_timeout_error <- function(
  context,
  timeout,
  last,
  parent = NULL
) {
  .fabric_abort(
    sprintf(
      "Timed out after %s seconds waiting for Kusto ingestion %s; the service operation may still be running",
      format(timeout, trim = TRUE),
      context$id
    ),
    class = c(
      "fabric_kql_ingestion_timeout",
      "fabric_kql_ingestion_error"
    ),
    operation_id = context$id,
    ingestion = context$ingestion,
    last_status = last,
    parent = parent
  )
}

# Raise a typed preview-protocol error
kusto_ingestion_protocol_error <- function(message, parent = NULL) {
  .fabric_abort(
    message,
    class = c(
      "fabric_kql_ingestion_protocol_error",
      "fabric_kql_ingestion_error"
    ),
    parent = parent
  )
}

# Validate the operation ID returned by the preview API
kusto_ingestion_operation_id <- function(
  value,
  message = "ingestion must be one non-empty operation ID"
) {
  if (
    !is.character(value) ||
      length(value) != 1L ||
      is.na(value) ||
      !nzchar(trimws(value)) ||
      nchar(value, type = "bytes") > 2048L ||
      grepl("[\r\n]", value)
  ) {
    kusto_ingestion_protocol_error(message)
  }
  invisible(value)
}

# Validate and normalize one supported format name
kusto_ingestion_format <- function(value) {
  if (!is.character(value) || length(value) != 1L || is.na(value)) {
    .fabric_abort("format must be one supported Kusto ingestion format")
  }
  value <- tolower(trimws(value))
  if (!value %in% .kusto_ingestion_formats) {
    .fabric_abort(paste0(
      "format must be one of: ",
      paste(.kusto_ingestion_formats, collapse = ", ")
    ))
  }
  value
}

# Validate target names separately from general text properties
kusto_ingestion_target_name <- function(value, name) {
  value <- kusto_ingestion_optional_text(value, name, required = TRUE)
  if (nchar(value, type = "bytes") > 1024L) {
    .fabric_abort(sprintf("%s must not exceed 1,024 bytes", name))
  }
  trimws(value)
}

# Validate optional scalar text and reject control characters
kusto_ingestion_optional_text <- function(
  value,
  name,
  required = FALSE
) {
  if (is.null(value) && !required) {
    return(NULL)
  }
  if (
    !is.character(value) ||
      length(value) != 1L ||
      is.na(value) ||
      !nzchar(trimws(value)) ||
      grepl("[[:cntrl:]]", value)
  ) {
    .fabric_abort(sprintf("%s must be one non-empty character value", name))
  }
  trimws(value)
}

# Validate a tag or idempotency-key vector
kusto_ingestion_text_vector <- function(value, name) {
  if (is.null(value)) {
    return(character())
  }
  if (
    !is.character(value) ||
      anyNA(value) ||
      !all(nzchar(trimws(value))) ||
      any(grepl("[[:cntrl:]]", value)) ||
      any(nchar(value, type = "bytes") > 1024L)
  ) {
    .fabric_abort(sprintf(
      "%s must contain non-empty text values of at most 1,024 bytes",
      name
    ))
  }
  value <- trimws(value)
  if (anyDuplicated(value)) {
    .fabric_abort(sprintf("%s values must be unique", name))
  }
  value
}

# Validate a strict scalar flag
kusto_ingestion_flag <- function(value, name) {
  if (!is.logical(value) || length(value) != 1L || is.na(value)) {
    .fabric_abort(sprintf("%s must be TRUE or FALSE", name))
  }
  invisible(value)
}

# Validate one numeric control
kusto_ingestion_number <- function(
  value,
  name,
  minimum,
  strict = FALSE,
  whole = FALSE
) {
  valid <- is.numeric(value) &&
    length(value) == 1L &&
    !is.na(value) &&
    is.finite(value) &&
    if (strict) value > minimum else value >= minimum
  if (whole) {
    valid <- valid && value == floor(value)
  }
  if (!valid) {
    qualifier <- if (strict) "greater than" else "at least"
    suffix <- if (whole) " whole number" else " number"
    .fabric_abort(sprintf(
      "%s must be one%s %s %s",
      name,
      suffix,
      qualifier,
      format(minimum, trim = TRUE)
    ))
  }
  invisible(value)
}

# Normalize an input date-time to the service's ISO 8601 representation
kusto_ingestion_datetime <- function(value, name, allow_date) {
  if (is.null(value)) {
    return(NULL)
  }
  if (inherits(value, "POSIXt") && length(value) == 1L && !is.na(value)) {
    return(format(
      as.POSIXct(value, tz = "UTC"),
      "%Y-%m-%dT%H:%M:%OS6Z",
      tz = "UTC"
    ))
  }
  if (inherits(value, "Date") && length(value) == 1L && !is.na(value)) {
    if (!allow_date) {
      return(paste0(format(value, "%Y-%m-%d"), "T00:00:00Z"))
    }
    return(format(value, "%Y-%m-%d"))
  }
  if (!is.character(value) || length(value) != 1L || is.na(value)) {
    .fabric_abort(sprintf("%s must be one ISO 8601 date-time", name))
  }
  date <- "^[0-9]{4}-[0-9]{2}-[0-9]{2}$"
  datetime <- paste0(
    "^[0-9]{4}-[0-9]{2}-[0-9]{2}T",
    "[0-9]{2}:[0-9]{2}:[0-9]{2}(\\.[0-9]+)?",
    "(Z|[+-][0-9]{2}:[0-9]{2})$"
  )
  valid <- grepl(datetime, value) || allow_date && grepl(date, value)
  if (!valid) {
    .fabric_abort(sprintf("%s must use ISO 8601 format", name))
  }
  value
}

# Validate a CSV validation policy and return its JSON string
kusto_ingestion_validation_policy <- function(value, format) {
  if (is.null(value)) {
    return(NULL)
  }
  delimited <- c("csv", "tsv", "tsve", "psv", "scsv", "sohsv")
  if (!format %in% delimited) {
    .fabric_abort(
      "validation_policy is supported only for delimited text formats"
    )
  }
  if (is.list(value)) {
    if (
      is.null(names(value)) || anyNA(names(value)) || !all(nzchar(names(value)))
    ) {
      .fabric_abort("validation_policy must be a named list or JSON object")
    }
    return(as.character(jsonlite::toJSON(
      value,
      auto_unbox = TRUE,
      null = "null",
      digits = 22
    )))
  }
  if (!is.character(value) || length(value) != 1L || is.na(value)) {
    .fabric_abort("validation_policy must be a named list or JSON object")
  }
  decoded <- try(
    jsonlite::fromJSON(value, simplifyVector = FALSE),
    silent = TRUE
  )
  if (
    inherits(decoded, "try-error") ||
      !is.list(decoded) ||
      is.null(names(decoded))
  ) {
    .fabric_abort("validation_policy must contain one valid JSON object")
  }
  value
}

# Parse one documented response time as UTC, retaining NA for absent detail
# fields and rejecting malformed top-level timestamps
kusto_ingestion_response_time <- function(value, name, optional = FALSE) {
  if (is.null(value)) {
    if (optional) {
      return(as.POSIXct(NA_real_, origin = "1970-01-01", tz = "UTC"))
    }
    kusto_ingestion_protocol_error(sprintf(
      "Kusto ingestion status omitted %s",
      name
    ))
  }
  if (!is.character(value) || length(value) != 1L || is.na(value)) {
    kusto_ingestion_protocol_error(sprintf(
      "Kusto ingestion status field %s is not a timestamp",
      name
    ))
  }
  parsed <- suppressWarnings(as.POSIXct(
    value,
    format = "%Y-%m-%dT%H:%M:%OSZ",
    tz = "UTC"
  ))
  if (is.na(parsed)) {
    offset <- sub(
      "([+-][0-9]{2}):([0-9]{2})$",
      "\\1\\2",
      value
    )
    parsed <- suppressWarnings(as.POSIXct(
      offset,
      format = "%Y-%m-%dT%H:%M:%OS%z",
      tz = "UTC"
    ))
  }
  if (is.na(parsed)) {
    kusto_ingestion_protocol_error(sprintf(
      "Kusto ingestion status field %s is not a valid timestamp",
      name
    ))
  }
  parsed
}

# Parse an optional timestamp field from every detail record
kusto_ingestion_time_vector <- function(records, field) {
  values <- lapply(records, function(record) {
    kusto_ingestion_response_time(record[[field]], field, optional = TRUE)
  })
  as.POSIXct(
    vapply(values, as.numeric, numeric(1)),
    origin = "1970-01-01",
    tz = "UTC"
  )
}

#' Write an R or Arrow object to an Eventhouse table
#'
#' Serializes an R or Arrow object to Parquet, uploads it using the storage
#' container or OneLake folder preferred by the Kusto ingestion service,
#' submits tracked queued ingestion, waits for the terminal per-file result,
#' and removes staging only after a confirmed success.
#'
#' @section One-call staging workflow:
#' The queued-ingestion REST API accepts storage blobs rather than inline R
#' values. This function provides the higher-level one-call workflow: it reads
#' the ingestion service's preview configuration, honors its preferred upload
#' method, creates a unique `fabricqueryr-staging` path, and uploads bounded
#' Parquet parts. Service-provided Storage containers use their short-lived SAS
#' credentials. OneLake staging uses a Storage-audience access token, so an
#' audience-aware credential obtains both required tokens. When `token` is a
#' fixed bearer token or `AzureToken` and OneLake is selected, supply the
#' separate `storage_token`. `staging_folder` explicitly selects OneLake and
#' overrides the advertised upload preference with a trusted `Files/` URI.
#'
#' The caller therefore needs Kusto Table Ingestor and Database User access,
#' plus write/delete access when OneLake is selected. Advertised Storage
#' containers carry the service-managed SAS access needed for staging.
#'
#' @section R and Arrow inputs:
#' Data frames and tibbles are converted through Arrow. Factors become strings;
#' complex and `difftime` columns require an explicit conversion. Arrow Tables,
#' RecordBatches, Datasets, Scanners, `arrow_dplyr_query` objects, and
#' RecordBatchReaders are accepted, as are Arrow-compatible
#' `nanoarrow_array_stream` objects returned by package query helpers. Lazy
#' inputs are read one record batch at a time and written directly to a
#' temporary Parquet parts, so the complete data set is never collected into R
#' memory. A supplied reader or stream is single-use and is consumed.
#'
#' Parquet identity mapping matches source fields to existing KQL columns by
#' case-sensitive name. Supply `mapping` when the Parquet schema and table need
#' an explicit predefined mapping.
#'
#' `skip_batching = TRUE` cannot be combined with `ingest_if_not_exists` when
#' staging produces multiple Parquet files. Kusto then ingests each file
#' independently, so the shared idempotency tag can suppress later files in the
#' same logical write. Use normal batching, stage one file, or omit the
#' idempotency key.
#'
#' The service's advertised `maxDataSize` applies to uncompressed data. The
#' writer measures Arrow buffer bytes for every staged part, validates their
#' total before upload, and submits each value as the source `rawSize`. The
#' compressed Parquet file sizes remain available separately in the result.
#'
#' Set `create_if_missing = TRUE` to issue Kusto's idempotent `.create table`
#' command before staging. A missing table is created from the Arrow schema; an
#' existing table is returned unchanged, so this option never alters an
#' existing schema. Common Arrow scalar and nested types are inferred as Kusto
#' types. Supply a named `column_types` vector to override every column type.
#'
#' @section Failure and cleanup safety:
#' A successful tracked ingestion is cleaned up by default. Kusto removes blobs
#' uploaded to its service-owned Storage container; OneLake staging is removed
#' after the tracked success is confirmed. A submission error, polling timeout,
#' or other ambiguous result always retains staging because Kusto may still be
#' reading it. A confirmed terminal ingestion failure retains staging by default
#' and can remove it with
#' `keep_staging_on_failure = FALSE`. The retained full OneLake path is carried
#' by `fabric_kql_write_error` conditions.
#' A transport failure during OneLake's final atomic rename can also leave the
#' unique destination present; upload errors report `staging_retained = NA` and
#' the path to inspect.
#'
#' @param cluster Ingestion URI or Eventhouse/KQLDatabase discovery record; see
#'   [fabric_kql_ingest()].
#' @param table Target KQL table name.
#' @param data Data frame, tibble, Arrow Table/RecordBatch, lazy Arrow
#'   Dataset/Scanner/query, Arrow RecordBatchReader, or compatible array stream.
#' @param database Target KQL database name. Omit for a discovered KQLDatabase.
#' @param mapping Optional predefined Parquet ingestion mapping name.
#' @param staging_folder Optional trusted OneLake folder URI beginning below an
#'   item's `Files/` area. The ingestion configuration's lake folder is used by
#'   default.
#' @param staging_root Relative directory created below the selected lake
#'   folder for package staging.
#' @param cleanup Remove the unique staging directory after confirmed success.
#' @param keep_staging_on_failure Retain staging after a confirmed terminal
#'   Kusto failure. Ambiguous failures are always retained.
#' @param compression Parquet compression supported by [arrow::write_parquet()].
#' @param target_file_size Soft maximum bytes per staged Parquet file. The
#'   service's advertised total-size and blob-count limits are still enforced.
#' @param max_rows_per_file Optional exact maximum rows per staged file.
#' @param tags Extent tags passed to [fabric_kql_ingest()].
#' @param ingest_if_not_exists Stable idempotency keys passed to
#'   [fabric_kql_ingest()]. Cannot be combined with `skip_batching = TRUE` when
#'   staging produces multiple files.
#' @param skip_batching Whether Kusto should ingest each staged file
#'   independently. Cannot be combined with `ingest_if_not_exists` for a
#'   multi-file write.
#' @param creation_time Optional extent creation time passed to
#'   [fabric_kql_ingest()].
#' @param timeout Positive number of seconds shared by submission and tracked
#'   status waiting after upload. Time spent submitting reduces the time
#'   available for polling.
#' @param poll_interval Minimum seconds between ingestion status requests.
#' @param error_on_failure Raise a typed error for a confirmed failed or
#'   canceled ingestion. Set `FALSE` to return the failed result and its staging
#'   disposition.
#' @param create_if_missing Whether to create a missing KQL table from the
#'   Arrow schema before staging. Existing tables are left unchanged.
#' @param column_types Optional named character vector giving one Kusto scalar
#'   type for every data column when `create_if_missing = TRUE`. Supported
#'   canonical types are `bool`, `datetime`, `decimal`, `dynamic`, `guid`,
#'   `int`, `long`, `real`, and `string`. `NULL` infers them. Arrow time and
#'   duration columns must be converted because Kusto's Parquet mapping cannot
#'   ingest them as `timespan`.
#' @param query_cluster Optional Kusto query-service URI or discovery record
#'   used for table creation. A discovered `cluster` already carries this URI;
#'   a standard Microsoft ingestion URI is converted to its paired query URI.
#'   Supply this explicitly for a trusted custom ingestion endpoint.
#' @param tenant_id Microsoft Entra tenant ID.
#' @param client_id Microsoft Entra application/client ID.
#' @param token Optional access token or audience-aware token-provider function.
#'   A fixed token must target Kusto and be paired with `storage_token`.
#' @param storage_token Optional separate Azure Storage access token or token
#'   provider. Required when `token` cannot acquire a different audience.
#' @param auth_args Additional options passed to [AzureAuth::get_azure_token()].
#' @param .sleep,.now Internal deterministic polling hooks.
#'
#' @return A `fabric_kql_write_result` containing row/file counts, compressed
#'   Parquet `bytes`/`part_bytes`, uncompressed `raw_bytes`/`part_raw_bytes`,
#'   normalized ingestion status, tracking handle, source IDs, and staging
#'   disposition.
#' @references
#' [Queued ingestion configuration REST API (preview)](https://learn.microsoft.com/en-us/kusto/management/data-ingestion/queued-ingest-configuration-http?view=microsoft-fabric)
#'
#' [Queued ingestion REST API (preview)](https://learn.microsoft.com/en-us/kusto/management/data-ingestion/queued-ingest-use-http?view=microsoft-fabric)
#'
#' [Create a Kusto table](https://learn.microsoft.com/en-us/kusto/management/create-table-command?view=microsoft-fabric)
#'
#' [Kusto scalar data types](https://learn.microsoft.com/en-us/kusto/query/scalar-data-types/?view=microsoft-fabric)
#'
#' [Kusto Parquet mappings](https://learn.microsoft.com/en-us/kusto/management/parquet-mapping?view=microsoft-fabric)
#'
#' [OneLake ADLS-compatible access](https://learn.microsoft.com/en-us/fabric/onelake/onelake-access-api)
#'
#' [Arrow RecordBatchReader](https://arrow.apache.org/docs/r/reference/as_record_batch_reader.html)
#'
#' [Arrow Parquet writer](https://arrow.apache.org/docs/r/reference/ParquetFileWriter.html)
#' @export
#'
#' @examples
#' \dontrun{
#' # Discover the KQL database that will receive the R data
#' workspace <- fabric_workspaces()[[1L]]
#' database <- fabric_kql_databases(workspace)[[1L]]
#'
#' # Create a new table when needed, stage the data, and wait for ingestion
#' result <- fabric_kql_write_table(
#'   database,
#'   table = "EventsFromR",
#'   data = data.frame(id = 1:3, value = c("a", "b", "c")),
#'   create_if_missing = TRUE,
#'   ingest_if_not_exists = "r-batch-2026-08-14"
#' )
#' result$status$state
#'
#' # A local Arrow Dataset is scanned batch by batch rather than collected
#' dataset <- arrow::open_dataset(Sys.getenv("ARROW_DATASET_PATH"))
#' fabric_kql_write_table(database, "EventsFromArrow", dataset)
#' }
fabric_kql_write_table <- function(
  cluster,
  table,
  data,
  database = NULL,
  mapping = NULL,
  staging_folder = NULL,
  staging_root = "fabricqueryr-staging",
  cleanup = TRUE,
  keep_staging_on_failure = TRUE,
  compression = "snappy",
  target_file_size = 512 * 1024^2,
  max_rows_per_file = NULL,
  tags = character(),
  ingest_if_not_exists = character(),
  skip_batching = FALSE,
  creation_time = NULL,
  timeout = 900,
  poll_interval = 2,
  error_on_failure = TRUE,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  token = NULL,
  storage_token = NULL,
  auth_args = list(),
  create_if_missing = FALSE,
  column_types = NULL,
  query_cluster = NULL,
  .sleep = Sys.sleep,
  .now = Sys.time
) {
  # 1 Validate local arguments and adapt data to one lazy Arrow reader ----------------------------

  if (missing(data)) {
    .fabric_abort("data is required")
  }
  target <- kusto_resolve_ingestion_target(
    cluster,
    database,
    table
  )
  mapping <- kusto_ingestion_optional_text(mapping, "mapping")
  tags <- kusto_ingestion_text_vector(tags, "tags")
  ingest_if_not_exists <- kusto_ingestion_text_vector(
    ingest_if_not_exists,
    "ingest_if_not_exists"
  )
  if (any(startsWith(tolower(ingest_if_not_exists), "ingest-by:"))) {
    .fabric_abort(
      "ingest_if_not_exists values must omit the 'ingest-by:' prefix"
    )
  }
  kusto_ingestion_flag(skip_batching, "skip_batching")
  creation_time <- kusto_ingestion_datetime(
    creation_time,
    "creation_time",
    allow_date = TRUE
  )
  kusto_ingestion_flag(cleanup, "cleanup")
  kusto_ingestion_flag(
    keep_staging_on_failure,
    "keep_staging_on_failure"
  )
  kusto_ingestion_flag(error_on_failure, "error_on_failure")
  kusto_ingestion_flag(create_if_missing, "create_if_missing")
  if (!isTRUE(create_if_missing) && !is.null(column_types)) {
    .fabric_abort("column_types requires create_if_missing = TRUE")
  }
  if (!isTRUE(create_if_missing) && !is.null(query_cluster)) {
    .fabric_abort("query_cluster requires create_if_missing = TRUE")
  }
  if (!is.function(.sleep) || !is.function(.now)) {
    .fabric_abort(".sleep and .now must be functions")
  }
  kusto_ingestion_number(timeout, "timeout", minimum = 0, strict = TRUE)
  kusto_ingestion_number(
    poll_interval,
    "poll_interval",
    minimum = .kusto_ingestion_poll_floor
  )
  compression <- kusto_ingestion_optional_text(
    compression,
    "compression",
    required = TRUE
  )
  staging_root <- onelake_normalize_path(staging_root)
  if (!is.null(staging_folder)) {
    staging_folder <- kusto_ingestion_optional_text(
      staging_folder,
      "staging_folder",
      required = TRUE
    )
  }
  prepared <- tryCatch(
    {
      value <- .fabric_parquet_prepare_data(
        data,
        "fabric_kql_write_table()"
      )
      .fabric_parquet_column_names(value$names)
      value
    },
    error = function(error) {
      .fabric_abort(
        conditionMessage(error),
        class = c("fabric_kql_arrow_error", "fabric_kql_write_error"),
        parent = error
      )
    }
  )

  # 2 Discover the service-owned OneLake staging root ---------------------------------------------

  credential <- fabric_credential(
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args
  )
  if (isTRUE(create_if_missing)) {
    management_target <- kusto_write_management_target(
      cluster,
      target,
      query_cluster
    )
    command <- kusto_write_create_table_command(
      table,
      prepared$schema,
      prepared$names,
      column_types
    )
    tryCatch(
      kusto_export_management(
        management_target,
        command,
        credential,
        deadline = .now() + min(timeout, 60),
        idempotent = TRUE,
        operation = "CreateTable"
      ),
      error = function(error) {
        .fabric_abort(
          paste0(
            "Kusto could not create or confirm target table '",
            table,
            "' before staging"
          ),
          class = c(
            "fabric_kql_table_create_error",
            "fabric_kql_write_error"
          ),
          parent = error
        )
      }
    )
  }
  configuration <- kusto_ingestion_configuration(
    target,
    credential,
    timeout = min(timeout, 60)
  )
  staging <- kusto_ingestion_staging_destination(
    configuration,
    override = staging_folder
  )
  staging_id <- .fabric_lakehouse_staging_id()
  storage_credential <- NULL
  if (identical(staging$method, "Lake")) {
    storage_credential <- if (!is.null(storage_token)) {
      fabric_credential(token = storage_token)
    } else {
      if (!credential$type %in% c("AzureAuth", "callback")) {
        .fabric_abort(
          paste0(
            "OneLake staging requires an audience-aware token provider or ",
            "a separate storage_token"
          ),
          class = c("fabric_kql_auth_error", "fabric_kql_write_error")
        )
      }
      credential
    }
    storage_directory <- staging$target
    storage_directory$path <- paste(
      staging$target$path,
      staging_root,
      staging_id,
      sep = "/"
    )
    staging_path <- onelake_path_url(storage_directory)
  } else {
    storage_directory <- paste(staging_root, staging_id, sep = "/")
    staging_path <- kusto_storage_blob_url(
      staging$container,
      storage_directory,
      include_credentials = FALSE
    )
  }

  # 3 Stream to bounded local Parquet parts -------------------------------------------------------

  parquet_directory <- tempfile("fabricqueryr-kql-")
  dir.create(parquet_directory)
  on.exit(
    unlink(parquet_directory, recursive = TRUE, force = TRUE),
    add = TRUE
  )
  serialized <- .fabric_parquet_write_dataset(
    prepared,
    directory = parquet_directory,
    compression = compression,
    target_file_size = target_file_size,
    max_rows_per_file = max_rows_per_file,
    max_files = configuration$max_blobs,
    caller = "fabric_kql_write_table()",
    error_class = c("fabric_kql_arrow_error", "fabric_kql_write_error")
  )
  if (serialized$total_raw_bytes > configuration$max_data_size) {
    .fabric_abort(
      paste0(
        "The staged data is ",
        format(serialized$total_raw_bytes, scientific = FALSE, trim = TRUE),
        " uncompressed bytes, exceeding Kusto's advertised maxDataSize of ",
        format(configuration$max_data_size, scientific = FALSE, trim = TRUE),
        " bytes"
      ),
      class = c("fabric_kql_size_error", "fabric_kql_write_error"),
      bytes = serialized$total_raw_bytes,
      raw_bytes = serialized$total_raw_bytes,
      staged_bytes = serialized$total_bytes,
      max_data_size = configuration$max_data_size
    )
  }
  if (
    serialized$file_count > 1L &&
      isTRUE(skip_batching) &&
      length(ingest_if_not_exists)
  ) {
    .fabric_abort(
      c(
        "Cannot safely apply one idempotency key to independently ingested files",
        "x" = paste0(
          "{.arg skip_batching} is {.code TRUE} and staging produced ",
          "{serialized$file_count} Parquet files"
        ),
        "i" = paste0(
          "Disable {.arg skip_batching}, stage one file, or omit ",
          "{.arg ingest_if_not_exists}"
        )
      ),
      class = c("fabric_kql_idempotency_error", "fabric_kql_write_error"),
      .format = TRUE
    )
  }
  if (identical(staging$method, "Lake")) {
    storage_targets <- lapply(basename(serialized$paths), function(name) {
      target <- storage_directory
      target$path <- paste(storage_directory$path, name, sep = "/")
      target
    })
    source_paths <- vapply(storage_targets, onelake_path_url, character(1))
    staging_paths <- source_paths
  } else {
    storage_targets <- NULL
    relative_paths <- paste(
      storage_directory,
      basename(serialized$paths),
      sep = "/"
    )
    source_paths <- vapply(
      relative_paths,
      function(path) kusto_storage_blob_url(staging$container, path),
      character(1),
      USE.NAMES = FALSE
    )
    staging_paths <- vapply(
      relative_paths,
      function(path) {
        kusto_storage_blob_url(
          staging$container,
          path,
          include_credentials = FALSE
        )
      },
      character(1),
      USE.NAMES = FALSE
    )
  }

  # 4 Upload every complete file atomically -------------------------------------------------------

  tryCatch(
    for (index in seq_along(serialized$paths)) {
      if (identical(staging$method, "Lake")) {
        onelake_upload_target(
          storage_targets[[index]],
          storage_credential,
          source = serialized$paths[[index]],
          overwrite = FALSE,
          if_match = NULL,
          chunk_size = getOption(
            "fabricqueryr.onelake.chunk_size",
            8 * 1024^2
          ),
          content_type = "application/vnd.apache.parquet",
          create_parents = TRUE
        )
      } else {
        kusto_storage_upload(source_paths[[index]], serialized$paths[[index]])
      }
    },
    error = function(error) {
      .fabric_abort(
        paste0(
          "Could not confirm the Parquet upload to staging; the unique ",
          "staging path may exist and should be inspected before cleanup"
        ),
        class = c("fabric_kql_upload_error", "fabric_kql_write_error"),
        staging_path = staging_path,
        staging_retained = NA,
        parent = error
      )
    }
  )

  # 5 Submit once, then wait using retry-safe status requests ------------------------------------

  source_ids <- character()
  for (index in seq_along(staging_paths)) {
    source_ids[[index]] <- kusto_ingestion_source_id(source_ids)
  }
  write_started <- .now()
  write_deadline <- write_started + timeout
  submission_timeout <- kusto_write_remaining_timeout(
    write_deadline,
    timeout,
    .now,
    staging_path
  )
  ingestion <- tryCatch(
    fabric_kql_ingest(
      cluster,
      table = table,
      sources = if (identical(staging$method, "Lake")) {
        kusto_write_storage_sources(source_paths, storage_credential)
      } else {
        source_paths
      },
      database = database,
      format = "parquet",
      source_ids = source_ids,
      raw_sizes = serialized$raw_bytes,
      mapping = mapping,
      tags = tags,
      ingest_if_not_exists = ingest_if_not_exists,
      skip_batching = skip_batching,
      creation_time = creation_time,
      delete_after_download = identical(staging$method, "Storage") &&
        isTRUE(cleanup),
      timeout = submission_timeout,
      token = credential,
      .deadline = write_deadline,
      .now = .now
    ),
    error = function(error) {
      if (kusto_condition_inherits(error, "fabric_http_deadline_error")) {
        kusto_write_timeout_error(
          staging_path,
          parent = error,
          ambiguous = TRUE,
          message = paste0(
            "Kusto submission exceeded the write deadline without returning ",
            "a tracking handle; staging was retained because ingestion may ",
            "still have been accepted"
          )
        )
      }
      kusto_write_ambiguous_error(
        error,
        staging_path,
        message = paste0(
          "Kusto submission did not return a tracking handle; staging was ",
          "retained because ingestion may still have been accepted"
        )
      )
    }
  )
  status_timeout <- kusto_write_remaining_timeout(
    write_deadline,
    timeout,
    .now,
    staging_path,
    ingestion = ingestion
  )
  status <- tryCatch(
    fabric_kql_ingestion_status(
      ingestion,
      wait = TRUE,
      timeout = status_timeout,
      poll_interval = poll_interval,
      error_on_failure = FALSE,
      .sleep = .sleep,
      .now = .now,
      .deadline = write_deadline
    ),
    error = function(error) {
      if (
        inherits(error, "fabric_kql_ingestion_timeout") ||
          kusto_condition_inherits(error, "fabric_http_deadline_error")
      ) {
        kusto_write_timeout_error(
          staging_path,
          ingestion = ingestion,
          parent = error,
          ambiguous = TRUE,
          message = paste0(
            "The KQL write deadline expired before a terminal ingestion ",
            "result was confirmed; staging was retained because the ",
            "operation may still be running"
          )
        )
      }
      kusto_write_ambiguous_error(
        error,
        staging_path,
        ingestion = ingestion,
        message = paste0(
          "Could not confirm the terminal Kusto ingestion result; staging ",
          "was retained because the operation may still be running"
        )
      )
    }
  )

  # 6 Apply cleanup only after the tracked outcome is unambiguous --------------------------------

  succeeded <- identical(status$state, "Succeeded") && isTRUE(status$complete)
  staging_retained <- TRUE
  if (succeeded && isTRUE(cleanup)) {
    staging_retained <- if (identical(staging$method, "Storage")) {
      FALSE
    } else {
      !kusto_remove_staging(
        staging$method,
        storage_targets,
        source_paths,
        storage_credential
      )
    }
    if (staging_retained) {
      .fabric_warn(
        c(
          "Staging cleanup failed after the Kusto ingestion succeeded",
          "i" = "Staged files remain at {.path {staging_path}}"
        ),
        .format = TRUE
      )
    }
  } else if (!succeeded && !isTRUE(keep_staging_on_failure)) {
    staging_retained <- !kusto_remove_staging(
      staging$method,
      storage_targets,
      source_paths,
      storage_credential
    )
  }
  result <- kusto_write_result(
    target,
    serialized,
    ingestion,
    status,
    source_ids,
    staging_path,
    staging_paths,
    staging_retained,
    create_if_missing
  )
  if (!succeeded && isTRUE(error_on_failure)) {
    parent <- tryCatch(
      kusto_ingestion_failure_error(status),
      error = identity
    )
    .fabric_abort(
      paste0(
        "Kusto could not ingest the staged R/Arrow data. ",
        if (staging_retained) {
          paste0("Staging was retained at '", staging_path, "'.")
        } else {
          "The staging directory was removed."
        }
      ),
      class = c("fabric_kql_write_failure", "fabric_kql_write_error"),
      staging_path = staging_path,
      staging_retained = staging_retained,
      ingestion = ingestion,
      last_status = status,
      result = result,
      parent = parent
    )
  }
  result
}

#' Print an Eventhouse R/Arrow write result
#'
#' @param x A `fabric_kql_write_result`.
#' @param ... Unused.
#' @return `x`, invisibly.
#' @export
print.fabric_kql_write_result <- function(x, ...) {
  .fabric_print(
    "fabric_kql_write_result",
    list(
      operation = x$operation_id,
      target = paste0(x$database, ".", x$table),
      state = x$status$state,
      rows = x$rows,
      files = x$file_count,
      staging = if (x$staging_retained) "retained" else "removed"
    )
  )
  invisible(x)
}

# Resolve the query endpoint needed by `.create table`. Discovery records carry
# both service URIs; standard Microsoft ingestion origins use the documented
# `ingest-` hostname prefix. Custom origins require an explicit paired endpoint
# so credentials are never redirected by an inferred host rewrite
kusto_write_management_target <- function(
  cluster,
  ingestion_target,
  query_cluster
) {
  if (!is.null(query_cluster)) {
    return(kusto_resolve_target(
      query_cluster,
      ingestion_target$database
    ))
  }
  record <- fabric_as_record(cluster)
  if (!is.null(record)) {
    return(kusto_resolve_target(
      record,
      ingestion_target$database
    ))
  }
  parsed <- httr2::url_parse(ingestion_target$url)
  trusted <- any(vapply(
    c(
      "kusto.fabric.microsoft.com",
      "kusto.windows.net",
      "kusto.data.microsoft.com"
    ),
    function(suffix) fabric_host_matches(parsed$hostname, suffix),
    logical(1)
  ))
  if (!trusted) {
    .fabric_abort(
      "query_cluster is required to create a table through a custom endpoint",
      class = c("fabric_kql_table_create_error", "fabric_kql_write_error")
    )
  }
  query_uri <- sub(
    "^https://ingest-",
    "https://",
    ingestion_target$url,
    ignore.case = TRUE
  )
  kusto_resolve_target(query_uri, ingestion_target$database)
}

# Build an idempotent Kusto creation command. `.create table` returns an
# existing same-named table unchanged, which is exactly the public
# create-if-missing contract
kusto_write_create_table_command <- function(
  table,
  schema,
  columns,
  column_types = NULL
) {
  table <- kusto_write_identifier(table, "table")
  quoted_columns <- vapply(
    columns,
    kusto_write_identifier,
    character(1),
    name = "data column"
  )
  types <- kusto_write_column_types(schema, columns, column_types)
  paste0(
    ".create table ",
    table,
    " (",
    paste0(quoted_columns, ":", types, collapse = ", "),
    ")"
  )
}

kusto_write_identifier <- function(value, name) {
  kusto_entity_identifier(
    value,
    name,
    error_class = c("fabric_kql_schema_error", "fabric_kql_write_error")
  )
}

# Validate and quote one Kusto table or column entity name. Returns safe KQL
# source text shared by table creation and table-oriented reads
kusto_entity_identifier <- function(value, name, error_class = NULL) {
  value <- kusto_ingestion_target_name(value, name)
  valid <- grepl("^[\\p{L}\\p{N}_. -]+$", value, perl = TRUE) &&
    !startsWith(value, "__") &&
    !endsWith(value, "__")
  if (!valid) {
    .fabric_abort(
      paste0(
        name,
        " must use Kusto identifier characters (letters, numbers, spaces, ",
        "underscores, dots, or dashes) and cannot start or end with '__'"
      ),
      class = error_class
    )
  }
  paste0("['", value, "']")
}

kusto_write_column_types <- function(schema, columns, column_types = NULL) {
  allowed <- c(
    "bool",
    "datetime",
    "decimal",
    "dynamic",
    "guid",
    "int",
    "long",
    "real",
    "string"
  )
  if (is.null(column_types)) {
    return(vapply(
      seq_along(columns),
      function(index) {
        field <- schema$field(index - 1L)
        kusto_write_arrow_type(field$type, columns[[index]])
      },
      character(1)
    ))
  }
  if (
    !is.character(column_types) ||
      length(column_types) != length(columns) ||
      is.null(names(column_types)) ||
      anyNA(names(column_types)) ||
      !all(nzchar(names(column_types))) ||
      anyDuplicated(names(column_types)) ||
      !setequal(names(column_types), columns)
  ) {
    .fabric_abort(
      "column_types must be a named character vector with one entry for every data column",
      class = c("fabric_kql_schema_error", "fabric_kql_write_error")
    )
  }
  column_types <- tolower(trimws(column_types[match(
    columns,
    names(column_types)
  )]))
  invalid <- is.na(column_types) |
    !nzchar(column_types) |
    !column_types %in% allowed
  if (any(invalid)) {
    .fabric_abort(
      paste0(
        "column_types contains unsupported Kusto types for: ",
        paste(columns[invalid], collapse = ", "),
        ". Use one of: ",
        paste(allowed, collapse = ", ")
      ),
      class = c("fabric_kql_schema_error", "fabric_kql_write_error")
    )
  }
  unname(column_types)
}

# Infer only conversions supported by Kusto's documented Parquet mapping
# Ambiguous binary/interval types fail early and can be handled explicitly with
# column_types or a source conversion
kusto_write_arrow_type <- function(type, column) {
  if (inherits(type, "DictionaryType")) {
    return(kusto_write_arrow_type(type$value_type, column))
  }
  arrow_type <- tolower(type$ToString())
  if (grepl("^(time(32|64)|duration)", arrow_type)) {
    .fabric_abort(
      paste0(
        "Cannot ingest data column '",
        column,
        "' with Arrow type '",
        arrow_type,
        "' through Kusto Parquet mapping; convert it to a supported string, ",
        "integer, or datetime representation before writing"
      ),
      class = c("fabric_kql_schema_error", "fabric_kql_write_error")
    )
  }
  inferred <- if (grepl("^bool$", arrow_type)) {
    "bool"
  } else if (grepl("^(u?int(8|16)|int32)$", arrow_type)) {
    "int"
  } else if (grepl("^(uint32|int64)$", arrow_type)) {
    "long"
  } else if (grepl("^uint64$", arrow_type)) {
    "decimal"
  } else if (grepl("^(half_?float|float|double)$", arrow_type)) {
    "real"
  } else if (grepl("^decimal(128)?\\(", arrow_type)) {
    "decimal"
  } else if (grepl("^(timestamp|date(32|64))", arrow_type)) {
    "datetime"
  } else if (grepl("^(string|large_string|string_view)$", arrow_type)) {
    "string"
  } else if (
    grepl("^(list|large_list|fixed_size_list|struct|map)", arrow_type)
  ) {
    "dynamic"
  } else if (grepl("^extension<.*uuid", arrow_type)) {
    "guid"
  } else if (grepl("^null$", arrow_type)) {
    "string"
  } else {
    NA_character_
  }
  if (is.na(inferred)) {
    .fabric_abort(
      paste0(
        "Cannot infer a Kusto type for data column '",
        column,
        "' with Arrow type '",
        arrow_type,
        "'; convert the column or supply column_types"
      ),
      class = c("fabric_kql_schema_error", "fabric_kql_write_error")
    )
  }
  inferred
}

# Read and strictly normalize the preview ingestion configuration
kusto_ingestion_configuration <- function(target, credential, timeout = 60) {
  request <- httr2::request(paste0(
    target$url,
    "/v1/rest/ingestion/configuration"
  )) |>
    httr2::req_headers(
      Accept = "application/json",
      `x-ms-app` = "fabricQueryR",
      `x-ms-client-version` = as.character(utils::packageVersion(
        "fabricQueryR"
      )),
      `x-ms-client-request-id` = .kusto_next_ingestion_request_id(
        "Configuration"
      )
    ) |>
    httr2::req_timeout(timeout)
  response <- .httr2_perform(
    request,
    credential = credential,
    audience = .fabric_audience$kusto,
    idempotent = TRUE
  )
  payload <- tryCatch(
    httr2::resp_body_json(
      response,
      simplifyVector = FALSE,
      bigint_as_char = TRUE
    ),
    error = function(error) {
      kusto_ingestion_protocol_error(
        "Kusto returned invalid ingestion configuration JSON",
        parent = error
      )
    }
  )
  if (
    !is.list(payload) ||
      !is.list(payload$containerSettings) ||
      !is.list(payload$ingestionSettings)
  ) {
    kusto_ingestion_protocol_error(
      "Kusto ingestion configuration is missing required settings"
    )
  }
  max_data_size <- suppressWarnings(as.numeric(
    payload$ingestionSettings$maxDataSize
  ))
  max_blobs <- suppressWarnings(as.numeric(
    payload$ingestionSettings$maxBlobsPerBatch
  ))
  if (
    length(max_data_size) != 1L ||
      is.na(max_data_size) ||
      !is.finite(max_data_size) ||
      max_data_size <= 0 ||
      max_data_size != floor(max_data_size) ||
      length(max_blobs) != 1L ||
      is.na(max_blobs) ||
      !is.finite(max_blobs) ||
      max_blobs < 1 ||
      max_blobs != floor(max_blobs)
  ) {
    kusto_ingestion_protocol_error(
      "Kusto ingestion configuration contains invalid service limits"
    )
  }
  preferred_upload_method <- payload$containerSettings$preferredUploadMethod %||%
    "Default"
  if (
    !is.character(preferred_upload_method) ||
      length(preferred_upload_method) != 1L ||
      is.na(preferred_upload_method) ||
      !tolower(preferred_upload_method) %in% c("storage", "lake", "default")
  ) {
    kusto_ingestion_protocol_error(
      "Kusto ingestion configuration contains an invalid preferredUploadMethod"
    )
  }
  preferred_upload_method <- c(
    storage = "Storage",
    lake = "Lake",
    default = "Default"
  )[[tolower(preferred_upload_method)]]
  list(
    lake_folders = kusto_ingestion_configuration_paths(
      payload$containerSettings$lakeFolders,
      "lakeFolders"
    ),
    storage_containers = kusto_ingestion_configuration_paths(
      payload$containerSettings$containers,
      "containers"
    ),
    max_data_size = max_data_size,
    max_blobs = max_blobs,
    preferred_upload_method = preferred_upload_method,
    preferred_ingestion_method = payload$ingestionSettings$preferredIngestionMethod %||%
      NA_character_,
    raw = kusto_storage_redact_object(payload)
  )
}

# Extract and validate optional path records from ingestion configuration
kusto_ingestion_configuration_paths <- function(
  value,
  setting = "lakeFolders"
) {
  if (is.null(value)) {
    return(character())
  }
  if (!is.list(value)) {
    kusto_ingestion_protocol_error(
      paste0("Kusto ingestion configuration ", setting, " must be an array")
    )
  }
  paths <- vapply(
    value,
    function(record) {
      path <- if (is.list(record)) record$path else NULL
      if (
        !is.character(path) ||
          length(path) != 1L ||
          is.na(path) ||
          !nzchar(path)
      ) {
        kusto_ingestion_protocol_error(
          paste0(
            "Kusto ingestion configuration contains an invalid ",
            setting,
            " path"
          )
        )
      }
      path
    },
    character(1)
  )
  unique(paths)
}

# Honor the service's upload preference while allowing an explicit OneLake
# override. Returns either a validated OneLake target or a SAS container URL
kusto_ingestion_staging_destination <- function(
  configuration,
  override = NULL
) {
  if (!is.null(override)) {
    return(list(
      method = "Lake",
      target = kusto_ingestion_staging_folder(configuration, override)
    ))
  }

  preferred <- configuration$preferred_upload_method %||% "Default"
  methods <- switch(
    preferred,
    Storage = c("Storage", "Lake"),
    Lake = c("Lake", "Storage"),
    Default = c("Lake", "Storage"),
    c("Lake", "Storage")
  )
  for (method in methods) {
    if (identical(method, "Lake") && length(configuration$lake_folders)) {
      target <- try(
        kusto_ingestion_staging_folder(configuration),
        silent = TRUE
      )
      if (!inherits(target, "try-error")) {
        return(list(method = "Lake", target = target))
      }
    }
    if (
      identical(method, "Storage") && length(configuration$storage_containers)
    ) {
      for (container in configuration$storage_containers) {
        valid <- try(kusto_storage_validate_container(container), silent = TRUE)
        if (!inherits(valid, "try-error")) {
          return(list(method = "Storage", container = valid))
        }
      }
    }
  }
  .fabric_abort(
    paste0(
      "Kusto returned no usable staging destination; expected a valid ",
      "preferred Storage container or OneLake lake folder"
    ),
    class = c(
      "fabric_kql_staging_configuration_error",
      "fabric_kql_write_error"
    )
  )
}

# Validate a service-provided SAS container without exposing it in errors
kusto_storage_validate_container <- function(container) {
  parsed <- try(httr2::url_parse(container), silent = TRUE)
  invalid <- inherits(parsed, "try-error") ||
    !identical(tolower(parsed$scheme %||% ""), "https") ||
    !nzchar(parsed$hostname %||% "") ||
    !nzchar(parsed$path %||% "") ||
    nzchar(parsed$username %||% "") ||
    nzchar(parsed$password %||% "") ||
    nzchar(parsed$fragment %||% "") ||
    !length(parsed$query)
  if (invalid) {
    .fabric_abort(
      "Kusto returned an invalid credentialed Storage container",
      class = c(
        "fabric_kql_staging_configuration_error",
        "fabric_kql_write_error"
      )
    )
  }
  container
}

# Add a safe relative blob name before the container's SAS query string
kusto_storage_blob_url <- function(
  container,
  path,
  include_credentials = TRUE
) {
  container <- kusto_storage_validate_container(container)
  path <- onelake_normalize_path(path)
  encoded_path <- paste(
    vapply(
      strsplit(path, "/", fixed = TRUE)[[1L]],
      utils::URLencode,
      character(1),
      reserved = TRUE
    ),
    collapse = "/"
  )
  marker <- regexpr("?", container, fixed = TRUE)[[1L]]
  base <- substr(container, 1L, marker - 1L)
  query <- substr(container, marker + 1L, nchar(container))
  url <- paste0(sub("/+$", "", base), "/", encoded_path)
  if (isTRUE(include_credentials)) paste0(url, "?", query) else url
}

# Upload one complete Parquet file through a service-owned SAS container
kusto_storage_upload <- function(url, source) {
  request <- httr2::request(url) |>
    httr2::req_method("PUT") |>
    httr2::req_headers(
      `x-ms-version` = "2023-11-03",
      `x-ms-blob-type` = "BlockBlob"
    ) |>
    httr2::req_body_file(
      source,
      type = "application/vnd.apache.parquet"
    )
  .httr2_perform(request, idempotent = TRUE)
  invisible(TRUE)
}

# Best-effort removal for either selected staging backend
kusto_remove_staging <- function(
  method,
  storage_targets,
  source_paths,
  storage_credential
) {
  if (identical(method, "Lake")) {
    return(.fabric_onelake_remove_staging(
      storage_targets[[1L]],
      storage_credential
    ))
  }
  isTRUE(tryCatch(
    {
      for (url in source_paths) {
        request <- httr2::request(url) |>
          httr2::req_method("DELETE") |>
          httr2::req_headers(`x-ms-version` = "2023-11-03")
        .httr2_perform(
          request,
          idempotent = TRUE,
          accepted_status = 404L
        )
      }
      TRUE
    },
    error = function(error) FALSE
  ))
}

# Choose a trusted OneLake folder from configuration or an explicit Files URI
kusto_ingestion_staging_folder <- function(configuration, override = NULL) {
  configured <- is.null(override)
  candidates <- if (configured) configuration$lake_folders else override
  if (!length(candidates)) {
    .fabric_abort(
      paste0(
        "Kusto returned no OneLake lake folder for R/Arrow staging; supply ",
        "staging_folder with a writable OneLake Files URI"
      ),
      class = c(
        "fabric_kql_staging_configuration_error",
        "fabric_kql_write_error"
      )
    )
  }
  for (candidate in candidates) {
    target <- try(onelake_resolve_target(candidate), silent = TRUE)
    if (inherits(target, "try-error")) {
      next
    }
    pieces <- strsplit(target$path, "/", fixed = TRUE)[[1L]]
    if (!length(pieces) || !nzchar(target$path)) {
      next
    }
    root <- tolower(pieces[[1L]])
    if (
      (configured && !identical(root, "tables")) ||
        (!configured && identical(root, "files"))
    ) {
      return(target)
    }
  }
  .fabric_abort(
    if (configured) {
      "Kusto returned no writable OneLake staging folder outside Tables/"
    } else {
      "staging_folder must be a trusted OneLake Files URI"
    },
    class = c(
      "fabric_kql_staging_configuration_error",
      "fabric_kql_write_error"
    )
  )
}

# Canonicalize and authenticate staged sources for Eventhouse retrieval
kusto_write_storage_sources <- function(paths, storage_credential) {
  paths <- vapply(
    paths,
    function(path) {
      target <- onelake_resolve_target(path)
      target$dfs_base <- "https://onelake.dfs.fabric.microsoft.com"
      onelake_path_url(target)
    },
    character(1)
  )
  suffix <- paste0(
    ";token=",
    fabric_get_token(storage_credential, .fabric_audience$storage)
  )
  paste0(paths, suffix)
}

# Best-effort removal of the unique directory containing staged files
.fabric_onelake_remove_staging <- function(target, credential) {
  isTRUE(tryCatch(
    {
      directory <- target
      directory$path <- dirname(target$path)
      onelake_delete_target(
        directory,
        credential,
        recursive = TRUE,
        is_directory = TRUE
      )
      TRUE
    },
    error = function(error) FALSE
  ))
}

# Raise one safe error while retaining staging after an ambiguous Kusto result
kusto_write_ambiguous_error <- function(
  error,
  staging_path,
  ingestion = NULL,
  message
) {
  .fabric_abort(
    message,
    class = c("fabric_kql_write_ambiguous", "fabric_kql_write_error"),
    staging_path = staging_path,
    staging_retained = TRUE,
    ingestion = ingestion,
    parent = error
  )
}

# Return the positive portion of one shared post-upload write deadline
kusto_write_remaining_timeout <- function(
  deadline,
  timeout,
  .now,
  staging_path,
  ingestion = NULL
) {
  remaining <- as.numeric(difftime(deadline, .now(), units = "secs"))
  if (is.finite(remaining) && remaining > 0) {
    return(min(timeout, remaining))
  }
  if (is.null(ingestion)) {
    kusto_write_timeout_error(
      staging_path,
      message = paste0(
        "The KQL write deadline expired before ingestion submission started; ",
        "staging was retained"
      )
    )
  }
  kusto_write_timeout_error(
    staging_path,
    ingestion = ingestion,
    ambiguous = TRUE,
    message = paste0(
      "The KQL write deadline expired after ingestion was submitted; staging ",
      "was retained because the operation may still be running"
    )
  )
}

# Raise a timeout that distinguishes a local pre-submit expiry from ambiguity
kusto_write_timeout_error <- function(
  staging_path,
  ingestion = NULL,
  parent = NULL,
  ambiguous = FALSE,
  message
) {
  classes <- c("fabric_kql_write_timeout", "fabric_kql_write_error")
  if (isTRUE(ambiguous)) {
    classes <- c(
      "fabric_kql_write_timeout",
      "fabric_kql_write_ambiguous",
      "fabric_kql_write_error"
    )
  }
  .fabric_abort(
    message,
    class = classes,
    staging_path = staging_path,
    staging_retained = TRUE,
    operation_id = ingestion$id %||% ingestion$operation_id %||% NULL,
    ingestion = ingestion,
    parent = parent
  )
}

# Inspect wrapped conditions without exposing or copying their diagnostic data
kusto_condition_inherits <- function(error, class) {
  seen <- list()
  while (inherits(error, "condition")) {
    if (inherits(error, class)) {
      return(TRUE)
    }
    if (any(vapply(seen, identical, logical(1), y = error))) {
      return(FALSE)
    }
    seen[[length(seen) + 1L]] <- error
    error <- error$parent
  }
  FALSE
}

# Build the stable high-level result after status and cleanup are known
kusto_write_result <- function(
  target,
  serialized,
  ingestion,
  status,
  source_ids,
  staging_path,
  staging_paths,
  staging_retained,
  table_creation_requested
) {
  structure(
    list(
      operation_id = ingestion$id,
      database = target$database,
      table = target$table,
      rows = serialized$rows,
      bytes = serialized$total_bytes,
      part_bytes = serialized$bytes,
      raw_bytes = serialized$total_raw_bytes,
      part_raw_bytes = serialized$raw_bytes,
      file_count = serialized$file_count,
      columns = serialized$names,
      source_id = source_ids[[1L]],
      source_ids = source_ids,
      staging_path = staging_path,
      staging_paths = staging_paths,
      staging_retained = staging_retained,
      table_creation_requested = isTRUE(table_creation_requested),
      status = status,
      ingestion = ingestion
    ),
    class = "fabric_kql_write_result"
  )
}

#' Export a KQL query directly to external storage
#'
#' Runs Kusto's server-side `.export to storage` command and waits for its
#' asynchronous operation to finish. This avoids returning a large query result
#' through R and the Kusto client-result channel. A discovered Fabric item plus
#' a `Files/` directory is converted to a OneLake connection string using
#' caller impersonation; a complete documented Kusto storage connection string
#' can also be supplied.
#'
#' @section Tracking and failure safety:
#' The export submission is sent once and is never automatically replayed. The
#' function polls `.show operations` until Kusto reports a terminal state, then
#' calls `.show operation ... details` for the authoritative artifact paths and
#' record counts. Kusto does not remove files written before a failed export, so
#' a failure or timeout identifies the destination and operation ID but never
#' reports partial files as a successful result.
#' If Kusto has already reported `Completed` but the artifact-details request
#' exhausts the client deadline, the resulting details-timeout condition records
#' `operation_completed = TRUE`; it does not imply that the export is still
#' running or failed.
#'
#' Storage connection strings are emitted as obfuscated Kusto string literals
#' and are redacted from returned objects and conditions. If a submission fails
#' before its operation ID is received, inspect the destination and Kusto
#' operation history before trying again.
#'
#' @section Output properties:
#' `format` supports Kusto's `parquet`, `csv`, `tsv`, and `json` exporters.
#' `compressed = TRUE` enables the selected `compression_type`, or Kusto's
#' default codec when it is omitted. `size_limit` is the uncompressed target
#' size of each artifact and must be from 100 MB through 4 GB. Text header and
#' encoding options, and Parquet row-group and datetime-precision options, are
#' accepted only for their applicable formats.
#'
#' @section Permissions:
#' The caller needs at least Kusto Database Viewer permission. OneLake caller
#' impersonation additionally needs write access equivalent to Storage Blob
#' Data Contributor on the destination.
#'
#' @param cluster Query URI, or one Eventhouse or KQLDatabase discovery record.
#'   A KQLDatabase record also supplies `database`.
#' @param query One non-empty KQL query. The first result set is exported.
#' @param destination A discovered Fabric item, item name or ID, complete
#'   OneLake path, or complete HTTPS/ABFSS Kusto storage connection string. A
#'   character vector of complete paths distributes export work across multiple
#'   destinations. For an item, also supply `path` and optionally `workspace`.
#' @param database KQL database display name. Omit for a discovered KQLDatabase.
#' @param workspace Workspace containing an item supplied as `destination`.
#'   Omit when the discovered item contains its workspace ID.
#' @param path Destination directory relative to the OneLake item. It must be
#'   below `Files/`. Omit when `destination` is a complete storage path.
#' @param item_type Optional Fabric item type used to resolve a named item.
#' @param format Storage artifact format.
#' @param compressed Whether the artifacts use compression.
#' @param include_headers For CSV/TSV, one of `"none"`, `"all"`, or
#'   `"firstFile"`. `NULL` uses Kusto's default.
#' @param name_prefix Optional prefix for generated artifact names.
#' @param file_extension Optional artifact extension beginning with a dot.
#' @param encoding For CSV/TSV/JSON text, `"UTF8NoBOM"` or `"UTF8BOM"`.
#' @param compression_type Optional compression codec. Non-Parquet exports use
#'   `"gzip"`; Parquet also supports `"snappy"`, `"lz4_raw"`, `"brotli"`,
#'   and `"zstd"`.
#' @param distribution Kusto export distribution hint.
#' @param size_limit Maximum uncompressed bytes per artifact, from 100 MB to
#'   4 GB (100,000,000 to 4,000,000,000 bytes).
#' @param parquet_row_group_size Optional positive Parquet row-group row count.
#' @param parquet_datetime_precision Optional `"millisecond"` or
#'   `"microsecond"` precision for Parquet datetime values.
#' @param timeout Positive total client-side limit in seconds, shared by
#'   submission, status polling, and retrieval of artifact details.
#' @param poll_interval Positive seconds between operation status requests.
#' @inheritParams fabric_kql_query
#' @param .sleep,.now Internal hooks for deterministic polling tests.
#'
#' @return A `fabric_kql_export_result` containing the operation state,
#'   redacted destination, artifact paths, per-artifact record counts, and
#'   aggregate record count.
#' @references
#' [Kusto export to storage](https://learn.microsoft.com/en-us/kusto/management/data-export/export-data-to-storage?view=microsoft-fabric)
#'
#' [Kusto storage connection strings](https://learn.microsoft.com/en-us/kusto/api/connection-strings/storage-connection-strings?view=microsoft-fabric)
#'
#' [Kusto management HTTP request](https://learn.microsoft.com/en-us/kusto/api/rest/request?view=microsoft-fabric)
#'
#' [Show Kusto operations](https://learn.microsoft.com/en-us/kusto/management/show-operations?view=microsoft-fabric)
#' @export
#'
#' @examples
#' \dontrun{
#' # Discover both the source KQL database and destination Lakehouse
#' workspace <- fabric_workspaces()[[1L]]
#' database <- fabric_kql_databases(workspace)[[1L]]
#' lakehouse <- fabric_lakehouses(workspace)[[1L]]
#' table <- Sys.getenv("FABRIC_KQL_TABLE")
#' table_literal <- jsonlite::toJSON(table, auto_unbox = TRUE)
#'
#' # Export a bounded query to a new folder in the discovered Lakehouse
#' exported <- fabric_kql_export(
#'   database,
#'   query = paste0("table(", table_literal, ") | take 10000"),
#'   destination = lakehouse,
#'   path = "Files/exports/events-weekly",
#'   format = "parquet",
#'   name_prefix = "events"
#' )
#' exported$artifacts
#' }
fabric_kql_export <- function(
  cluster,
  query,
  destination,
  database = NULL,
  workspace = NULL,
  path = NULL,
  item_type = NULL,
  format = c("parquet", "csv", "tsv", "json"),
  compressed = TRUE,
  include_headers = NULL,
  name_prefix = NULL,
  file_extension = NULL,
  encoding = NULL,
  compression_type = NULL,
  distribution = c("per_shard", "per_node", "single"),
  size_limit = 100e6,
  parquet_row_group_size = NULL,
  parquet_datetime_precision = NULL,
  timeout = 900,
  poll_interval = 2,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  token = NULL,
  auth_args = list(),
  .sleep = Sys.sleep,
  .now = Sys.time
) {
  # 1 Resolve and validate the complete export contract -------------------------------------------

  if (
    !is.character(query) ||
      length(query) != 1L ||
      is.na(query) ||
      !nzchar(trimws(query))
  ) {
    .fabric_abort("query must be one non-empty character value")
  }
  target <- kusto_resolve_target(cluster, database)
  destination <- kusto_export_destination(
    destination,
    workspace = workspace,
    path = path,
    item_type = item_type
  )
  format <- match.arg(format)
  distribution <- match.arg(distribution)
  properties <- kusto_export_properties(
    format = format,
    compressed = compressed,
    include_headers = include_headers,
    name_prefix = name_prefix,
    file_extension = file_extension,
    encoding = encoding,
    compression_type = compression_type,
    distribution = distribution,
    size_limit = size_limit,
    parquet_row_group_size = parquet_row_group_size,
    parquet_datetime_precision = parquet_datetime_precision
  )
  kusto_ingestion_number(timeout, "timeout", minimum = 0, strict = TRUE)
  kusto_ingestion_number(
    poll_interval,
    "poll_interval",
    minimum = .kusto_ingestion_poll_floor
  )
  if (!is.function(.sleep) || !is.function(.now)) {
    .fabric_abort(".sleep and .now must be functions")
  }
  credential <- fabric_credential(
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args
  )
  command <- kusto_export_command(
    query,
    destination$connection,
    properties
  )
  started <- .now()
  deadline <- started + timeout

  # 2 Submit exactly once and recover the asynchronous operation ID -------------------------------

  submission <- tryCatch(
    {
      response <- kusto_export_management(
        target,
        command,
        credential,
        deadline = deadline,
        idempotent = FALSE,
        operation = "Submit"
      )
      list(
        response = response,
        operation_id = kusto_export_operation_id(response$tables)
      )
    },
    error = function(error) {
      safe_error <- kusto_storage_redact_condition(error)
      .fabric_abort(
        paste0(
          "KQL export submission did not return a tracking operation ID. ",
          "The request was not replayed because Kusto may already have ",
          "accepted it; inspect the destination and operation history before ",
          "resubmitting."
        ),
        class = c(
          "fabric_kql_export_submission_error",
          "fabric_kql_export_error"
        ),
        destination = destination$display,
        status = safe_error$status %||% NULL,
        response_metadata = safe_error$response_metadata %||% NULL,
        parent = safe_error,
        call = NULL,
        .trace = FALSE
      )
    }
  )
  submitted <- submission$response
  operation_id <- submission$operation_id

  # 3 Poll the retry-safe management status until one documented terminal state ------------------

  last <- NULL
  progress <- .fabric_poll_progress("KQL export", operation_id)
  on.exit(.fabric_poll_progress_done(progress), add = TRUE)
  repeat {
    elapsed <- as.numeric(difftime(.now(), started, units = "secs"))
    if (!is.finite(elapsed) || elapsed >= timeout) {
      kusto_export_timeout_error(
        operation_id,
        target,
        destination,
        properties$format,
        timeout,
        last
      )
    }
    status_response <- tryCatch(
      kusto_export_management(
        target,
        paste(".show operations", operation_id),
        credential,
        deadline = deadline,
        idempotent = TRUE,
        operation = "Status"
      ),
      fabric_http_deadline_error = function(error) {
        kusto_export_timeout_error(
          operation_id,
          target,
          destination,
          properties$format,
          timeout,
          last,
          parent = error
        )
      },
      error = function(error) {
        kusto_export_tracking_error(
          operation_id,
          target,
          destination,
          properties$format,
          last,
          parent = error
        )
      }
    )
    last <- kusto_export_status(status_response$tables, operation_id)
    .fabric_poll_progress_update(
      progress,
      if (is.null(last)) "Pending" else last$state
    )
    if (!is.null(last) && !last$state %in% c("InProgress", "Scheduled")) {
      break
    }
    elapsed <- as.numeric(difftime(.now(), started, units = "secs"))
    if (!is.finite(elapsed) || elapsed >= timeout) {
      kusto_export_timeout_error(
        operation_id,
        target,
        destination,
        properties$format,
        timeout,
        last
      )
    }
    .sleep(min(poll_interval, timeout - elapsed))
  }

  # 4 Retrieve authoritative artifacts only for a successful operation ----------------------------

  if (!identical(last$state, "Completed")) {
    kusto_export_failure_error(
      operation_id,
      target,
      destination,
      properties$format,
      last
    )
  }
  detail_response <- tryCatch(
    kusto_export_management(
      target,
      paste(".show operation", operation_id, "details"),
      credential,
      deadline = deadline,
      idempotent = TRUE,
      operation = "Details"
    ),
    fabric_http_deadline_error = function(error) {
      kusto_export_details_error(
        operation_id,
        target,
        destination,
        properties$format,
        last,
        timeout = TRUE,
        parent = error
      )
    },
    error = function(error) {
      kusto_export_details_error(
        operation_id,
        target,
        destination,
        properties$format,
        last,
        timeout = kusto_condition_inherits(
          error,
          "fabric_http_deadline_error"
        ),
        parent = error
      )
    }
  )
  artifacts <- kusto_export_artifacts(detail_response$tables)
  .fabric_poll_progress_done(progress)
  progress <- NULL
  structure(
    list(
      operation_id = operation_id,
      database = target$database,
      state = last$state,
      status = last$status,
      destination = destination$display,
      format = properties$format,
      artifacts = artifacts,
      file_count = nrow(artifacts),
      records = sum(artifacts$num_records),
      started_on = last$started_on,
      last_updated_on = last$last_updated_on,
      duration = last$duration,
      submitted_at = started,
      completed_at = .now(),
      request_id = submitted$request_id
    ),
    class = "fabric_kql_export_result"
  )
}

#' Print a KQL storage export result
#'
#' @param x A `fabric_kql_export_result`.
#' @param ... Unused.
#' @return `x`, invisibly.
#' @export
print.fabric_kql_export_result <- function(x, ...) {
  .fabric_print(
    "fabric_kql_export_result",
    list(
      operation = x$operation_id,
      database = x$database,
      state = x$state,
      files = x$file_count,
      records = format(x$records, scientific = FALSE)
    )
  )
  invisible(x)
}

# Normalize a discovered OneLake directory or complete storage connection
kusto_export_destination <- function(
  destination,
  workspace = NULL,
  path = NULL,
  item_type = NULL
) {
  multiple <- is.character(destination) && length(destination) > 1L
  if (multiple) {
    if (
      anyNA(destination) ||
        !all(grepl("^(?:https|abfss)://", destination, ignore.case = TRUE))
    ) {
      .fabric_abort(
        "Multiple export destinations must all be complete HTTPS/ABFSS paths"
      )
    }
    if (!is.null(workspace) || !is.null(path) || !is.null(item_type)) {
      .fabric_abort(
        paste0(
          "workspace, path, and item_type must be omitted when destination ",
          "contains complete storage paths"
        )
      )
    }
    resolved <- lapply(destination, kusto_export_destination)
    return(list(
      connection = vapply(resolved, `[[`, character(1), "connection"),
      display = vapply(resolved, `[[`, character(1), "display")
    ))
  }
  complete <- is.character(destination) &&
    length(destination) == 1L &&
    !is.na(destination) &&
    grepl("^(?:https|abfss)://", destination, ignore.case = TRUE)
  if (complete) {
    if (!is.null(workspace) || !is.null(path) || !is.null(item_type)) {
      .fabric_abort(
        paste0(
          "workspace, path, and item_type must be omitted when destination ",
          "is a complete storage path"
        )
      )
    }
    parts <- kusto_storage_connection_parts(destination)
    parsed <- try(httr2::url_parse(parts$resource), silent = TRUE)
    onelake <- !inherits(parsed, "try-error") &&
      fabric_host_matches(
        parsed$hostname %||% "",
        "onelake.dfs.fabric.microsoft.com"
      )
    if (onelake) {
      target <- onelake_resolve_target(parts$resource)
      kusto_export_onelake_target(target)
      suffix <- if (nzchar(parts$suffix)) parts$suffix else ";impersonate"
      connection <- paste0(onelake_path_url(target), suffix)
    } else {
      connection <- kusto_ingestion_source_url(destination)
    }
    return(list(
      connection = connection,
      display = kusto_export_storage_display(connection)
    ))
  }
  if (
    is.null(path) ||
      !is.character(path) ||
      length(path) != 1L ||
      is.na(path) ||
      !nzchar(trimws(path))
  ) {
    .fabric_abort(
      "path is required when destination is a Fabric item"
    )
  }
  target <- onelake_resolve_target(
    workspace,
    destination,
    path = path,
    item_type = item_type
  )
  kusto_export_onelake_target(target)
  connection <- paste0(onelake_path_url(target), ";impersonate")
  list(
    connection = connection,
    display = kusto_export_storage_display(connection)
  )
}

# Separate the storage resource from a Kusto authentication suffix without
# decoding or rebuilding the caller's credential text.
kusto_storage_connection_parts <- function(value) {
  delimiter <- regexpr("[;?]", value, perl = TRUE)[[1L]]
  if (delimiter < 0L) {
    return(list(resource = value, suffix = ""))
  }
  list(
    resource = substr(value, 1L, delimiter - 1L),
    suffix = substr(value, delimiter, nchar(value))
  )
}

# Retain the resource location while dropping every query/authentication suffix
kusto_export_storage_display <- function(value) {
  value <- sub(";.*$", "", value)
  value <- sub("[?].*$", "", value)
  .httr2_redact(value)
}

# Guard direct export against item roots and managed Delta table storage
kusto_export_onelake_target <- function(target) {
  onelake_require_mutable_path(target, "KQL export")
  root <- strsplit(target$path, "/", fixed = TRUE)[[1L]][[1L]]
  if (!identical(tolower(root), "files")) {
    .fabric_abort("A OneLake KQL export destination must be below Files/")
  }
  invisible(target)
}

# Validate documented export properties and return their normalized values
kusto_export_properties <- function(
  format,
  compressed,
  include_headers,
  name_prefix,
  file_extension,
  encoding,
  compression_type,
  distribution,
  size_limit,
  parquet_row_group_size,
  parquet_datetime_precision
) {
  format <- match.arg(tolower(format), c("parquet", "csv", "tsv", "json"))
  distribution <- match.arg(
    tolower(distribution),
    c("per_shard", "per_node", "single")
  )
  kusto_ingestion_flag(compressed, "compressed")
  if (!is.null(include_headers)) {
    include_headers <- match.arg(
      include_headers,
      c("none", "all", "firstFile")
    )
    if (!format %in% c("csv", "tsv")) {
      .fabric_abort("include_headers is available only for csv or tsv exports")
    }
  }
  name_prefix <- kusto_export_optional_text(name_prefix, "name_prefix")
  file_extension <- kusto_export_optional_text(
    file_extension,
    "file_extension"
  )
  if (
    !is.null(file_extension) &&
      (!startsWith(file_extension, ".") || grepl("[/\\\\]", file_extension))
  ) {
    .fabric_abort("file_extension must begin with a dot and contain no slash")
  }
  if (!is.null(encoding)) {
    encoding <- match.arg(encoding, c("UTF8NoBOM", "UTF8BOM"))
    if (identical(format, "parquet")) {
      .fabric_abort("encoding is not available for parquet exports")
    }
  }
  if (!is.null(compression_type)) {
    compression_type <- match.arg(
      tolower(compression_type),
      c("gzip", "snappy", "lz4_raw", "brotli", "zstd")
    )
    if (!isTRUE(compressed)) {
      .fabric_abort("compression_type requires compressed = TRUE")
    }
    if (!identical(format, "parquet") && !identical(compression_type, "gzip")) {
      .fabric_abort("Non-Parquet exports support only gzip compression")
    }
  }
  kusto_ingestion_number(
    size_limit,
    "size_limit",
    minimum = 100e6,
    whole = TRUE
  )
  if (size_limit > 4e9) {
    .fabric_abort("size_limit must not exceed 4 GB (4,000,000,000 bytes)")
  }
  if (!is.null(parquet_row_group_size)) {
    kusto_ingestion_number(
      parquet_row_group_size,
      "parquet_row_group_size",
      minimum = 1,
      whole = TRUE
    )
    if (!identical(format, "parquet")) {
      .fabric_abort(
        "parquet_row_group_size is available only for parquet exports"
      )
    }
  }
  if (!is.null(parquet_datetime_precision)) {
    parquet_datetime_precision <- match.arg(
      tolower(parquet_datetime_precision),
      c("millisecond", "microsecond")
    )
    if (!identical(format, "parquet")) {
      .fabric_abort(
        "parquet_datetime_precision is available only for parquet exports"
      )
    }
  }
  list(
    format = format,
    compressed = isTRUE(compressed),
    includeHeaders = include_headers,
    namePrefix = name_prefix,
    fileExtension = file_extension,
    encoding = encoding,
    compressionType = compression_type,
    distribution = distribution,
    persistDetails = TRUE,
    sizeLimit = as.numeric(size_limit),
    parquetRowGroupSize = parquet_row_group_size,
    parquetDatetimePrecision = parquet_datetime_precision
  )
}

# Accept optional scalar text without interpolating it unsafely into KQL
kusto_export_optional_text <- function(value, name) {
  if (is.null(value)) {
    return(NULL)
  }
  if (
    !is.character(value) ||
      length(value) != 1L ||
      is.na(value) ||
      !nzchar(value) ||
      nchar(value, type = "bytes") > 1024L
  ) {
    .fabric_abort(paste0(name, " must be one non-empty text value"))
  }
  value
}

# Quote a Kusto verbatim string, optionally as an obfuscated literal
kusto_export_literal <- function(value, hidden = FALSE) {
  escaped <- gsub('"', '""', value, fixed = TRUE)
  paste0(if (hidden) "h" else "", '@"', escaped, '"')
}

# Build the documented async command without retaining it in public results
kusto_export_command <- function(query, destination, properties) {
  compressed <- if (isTRUE(properties$compressed)) " compressed" else ""
  values <- properties[setdiff(names(properties), c("format", "compressed"))]
  values <- values[!vapply(values, is.null, logical(1))]
  encoded <- vapply(
    names(values),
    function(name) {
      value <- values[[name]]
      if (is.character(value)) {
        value <- kusto_export_literal(value)
      } else if (is.logical(value)) {
        value <- if (value) "true" else "false"
      } else {
        value <- format(value, scientific = FALSE, trim = TRUE)
      }
      paste0(name, "=", value)
    },
    character(1)
  )
  paste0(
    ".export async",
    compressed,
    " to ",
    properties$format,
    " (",
    paste(
      vapply(
        destination,
        kusto_export_literal,
        character(1),
        hidden = TRUE
      ),
      collapse = ", "
    ),
    ") with (",
    paste(encoded, collapse = ", "),
    ") <| ",
    query
  )
}

# Send one management command and parse the v1 table envelope
kusto_export_management <- function(
  target,
  command,
  credential,
  deadline,
  idempotent,
  operation
) {
  kusto_execute_management(
    target = target,
    command = command,
    credential = credential,
    deadline = deadline,
    idempotent = idempotent,
    operation = operation,
    request_prefix = "Export",
    error_class = c(
      "fabric_kql_export_protocol_error",
      "fabric_kql_export_error"
    )
  )
}

kusto_execute_management <- function(
  target,
  command,
  credential,
  deadline,
  idempotent,
  operation,
  request_prefix,
  error_class
) {
  client_request_id <- .kusto_next_ingestion_request_id(
    paste0(request_prefix, operation)
  )
  url <- sub(
    "/v2/rest/query$",
    "/v1/rest/mgmt",
    target$url,
    ignore.case = TRUE
  )
  properties <- as.character(jsonlite::toJSON(
    list(ClientRequestId = client_request_id),
    auto_unbox = TRUE
  ))
  request <- httr2::request(url) |>
    httr2::req_headers(
      Accept = "application/json",
      `x-ms-app` = "fabricQueryR",
      `x-ms-client-version` = as.character(
        utils::packageVersion("fabricQueryR")
      ),
      `x-ms-client-request-id` = client_request_id
    ) |>
    httr2::req_body_json(
      list(db = target$database, csl = command, properties = properties),
      auto_unbox = TRUE,
      digits = 22,
      null = "null"
    )
  response <- .httr2_perform(
    request,
    credential = credential,
    audience = .fabric_audience$kusto,
    idempotent = idempotent,
    deadline = deadline
  )
  payload <- tryCatch(
    httr2::resp_body_json(
      response,
      simplifyVector = FALSE,
      bigint_as_char = TRUE
    ),
    error = function(error) {
      .fabric_abort(
        "Kusto returned invalid management response JSON",
        class = error_class,
        parent = error
      )
    }
  )
  list(
    tables = kusto_management_tables(payload, error_class),
    request_id = httr2::resp_header(response, "x-ms-request-id") %||%
      client_request_id
  )
}

kusto_management_tables <- function(payload, error_class) {
  if (!is.list(payload) || !is.list(payload$Tables)) {
    .fabric_abort(
      "Kusto management response has no Tables array",
      class = error_class
    )
  }
  tables <- payload$Tables
  lapply(tables, function(table) {
    if (
      !is.list(table) ||
        !is.list(table$Columns) ||
        !is.list(table$Rows)
    ) {
      .fabric_abort(
        "Kusto management response contains a malformed table",
        class = error_class
      )
    }
    kusto_parse_table(table)
  })
}

# Find the first management table containing all requested fields
kusto_export_table <- function(tables, fields) {
  kusto_management_table(
    tables,
    fields,
    error_class = c(
      "fabric_kql_export_protocol_error",
      "fabric_kql_export_error"
    )
  )
}

kusto_management_table <- function(tables, fields, error_class) {
  for (table in tables) {
    if (all(tolower(fields) %in% tolower(names(table)))) {
      return(table)
    }
  }
  .fabric_abort(
    paste0(
      "Kusto management response is missing columns: ",
      paste(fields, collapse = ", ")
    ),
    class = error_class
  )
}

# Read a management table column case-insensitively
kusto_export_column <- function(table, name) {
  index <- match(tolower(name), tolower(names(table)))
  table[[index]]
}

# Extract and validate the async export operation GUID
kusto_export_operation_id <- function(tables) {
  table <- kusto_export_table(tables, "OperationId")
  if (nrow(table) != 1L) {
    .fabric_abort(
      "Kusto async export did not return exactly one operation ID",
      class = c(
        "fabric_kql_export_protocol_error",
        "fabric_kql_export_error"
      )
    )
  }
  id <- as.character(kusto_export_column(table, "OperationId")[[1L]])
  if (!fabric_is_guid(id)) {
    .fabric_abort(
      "Kusto async export returned an invalid operation ID",
      class = c(
        "fabric_kql_export_protocol_error",
        "fabric_kql_export_error"
      )
    )
  }
  tolower(id)
}

# Normalize the latest documented operation status; no row means not visible yet
kusto_export_status <- function(tables, operation_id) {
  table <- kusto_export_table(
    tables,
    c(
      "OperationId",
      "StartedOn",
      "LastUpdatedOn",
      "Duration",
      "State",
      "Status"
    )
  )
  if (!nrow(table)) {
    return(NULL)
  }
  ids <- tolower(as.character(kusto_export_column(table, "OperationId")))
  index <- which(ids == tolower(operation_id))
  if (length(index) != 1L) {
    .fabric_abort(
      "Kusto operation status did not identify the requested export once",
      class = c(
        "fabric_kql_export_protocol_error",
        "fabric_kql_export_error"
      )
    )
  }
  state <- as.character(kusto_export_column(table, "State")[[index]])
  documented <- c(
    "InProgress",
    "Completed",
    "Failed",
    "PartiallySucceeded",
    "Abandoned",
    "BadInput",
    "Scheduled",
    "Throttled",
    "Canceled",
    "Skipped"
  )
  if (!state %in% documented) {
    .fabric_abort(
      paste0("Kusto returned an unknown export operation state: ", state),
      class = c(
        "fabric_kql_export_protocol_error",
        "fabric_kql_export_error"
      )
    )
  }
  list(
    state = state,
    status = kusto_export_status_text(as.character(
      kusto_export_column(table, "Status")[[index]]
    )),
    started_on = kusto_export_column(table, "StartedOn")[[index]],
    last_updated_on = kusto_export_column(table, "LastUpdatedOn")[[index]],
    duration = kusto_export_column(table, "Duration")[[index]]
  )
}

# Redact named secrets and every documented storage credential suffix. Kusto
# accepts unnamed account keys after a semicolon, which the generic redactor
# cannot distinguish from ordinary text without the preceding storage URL.
kusto_storage_redact_text <- function(value) {
  value <- .httr2_redact(value)
  gsub(
    paste0(
      "(?i)((?:https|abfss)://[^[:space:]?;\"']+)",
      "(?:[?;][^[:space:]\"']+)"
    ),
    "\\1;<redacted>",
    value,
    perl = TRUE
  )
}

# Apply storage-specific redaction recursively while retaining payload shape.
kusto_storage_redact_object <- function(value) {
  value <- .httr2_redact_object(value)
  if (is.character(value)) {
    return(kusto_storage_redact_text(value))
  }
  if (!is.list(value)) {
    return(value)
  }
  for (index in seq_along(value)) {
    value[index] <- list(kusto_storage_redact_object(value[[index]]))
  }
  value
}

# Build a serializable copy of an error with Kusto storage credentials removed.
# Traces and calls are omitted because they can retain the authenticated request.
kusto_storage_redact_condition <- function(error) {
  fields <- unclass(error)
  fields[c("message", "call", "trace", "parent", "rlang")] <- NULL
  fields <- kusto_storage_redact_object(fields)
  fields$message <- kusto_storage_redact_text(conditionMessage(error))
  fields$call <- NULL
  if (inherits(error$parent, "condition")) {
    fields$parent <- kusto_storage_redact_condition(error$parent)
  } else if (!is.null(error$parent)) {
    fields$parent <- kusto_storage_redact_object(error$parent)
  }
  structure(fields, class = class(error))
}

# Redact credentials that may be embedded in export status text.
kusto_export_status_text <- function(value) {
  kusto_storage_redact_text(value)
}

# Normalize successful detail output while preserving exact Kusto long counts
kusto_export_artifacts <- function(tables) {
  table <- kusto_export_table(tables, c("Path", "NumRecords"))
  paths <- vapply(
    as.character(kusto_export_column(table, "Path")),
    kusto_export_storage_display,
    character(1)
  )
  counts <- as.character(kusto_export_column(table, "NumRecords"))
  valid <- grepl("^[0-9]+$", counts)
  if (anyNA(counts) || !all(valid)) {
    .fabric_abort(
      "Kusto export details contain an invalid NumRecords value",
      class = c(
        "fabric_kql_export_protocol_error",
        "fabric_kql_export_error"
      )
    )
  }
  tibble::tibble(
    path = paths,
    num_records = bit64::as.integer64(counts)
  )
}

# Preserve the operation context when status polling itself becomes ambiguous
kusto_export_tracking_error <- function(
  operation_id,
  target,
  destination,
  format,
  status,
  parent = NULL
) {
  .fabric_abort(
    paste0(
      "Could not confirm the state of KQL export ",
      operation_id,
      ". It may still be running; inspect the operation and destination ",
      "before resubmitting."
    ),
    class = c(
      "fabric_kql_export_tracking_error",
      "fabric_kql_export_error"
    ),
    operation_id = operation_id,
    database = target$database,
    destination = destination$display,
    format = format,
    last_status = status,
    parent = parent
  )
}

# Signal terminal failure without claiming that partial storage files are valid
kusto_export_failure_error <- function(
  operation_id,
  target,
  destination,
  format,
  status
) {
  .fabric_abort(
    paste0(
      "KQL export ",
      operation_id,
      " finished with state ",
      status$state,
      ". Files already written may be incomplete and were not returned as a ",
      "successful export."
    ),
    class = c("fabric_kql_export_failure", "fabric_kql_export_error"),
    operation_id = operation_id,
    database = target$database,
    destination = destination$display,
    format = format,
    last_status = status
  )
}

# Signal an exhausted polling deadline with safe resumability context
kusto_export_timeout_error <- function(
  operation_id,
  target,
  destination,
  format,
  timeout,
  status,
  parent = NULL
) {
  .fabric_abort(
    paste0(
      "KQL export ",
      operation_id,
      " did not reach a terminal state within ",
      base::format(timeout, scientific = FALSE, trim = TRUE),
      " seconds. It may still be running; inspect the operation and ",
      "destination before resubmitting."
    ),
    class = c("fabric_kql_export_timeout", "fabric_kql_export_error"),
    operation_id = operation_id,
    database = target$database,
    destination = destination$display,
    format = format,
    last_status = status,
    parent = parent
  )
}

# Distinguish a completed export whose artifact details missed the deadline
kusto_export_details_error <- function(
  operation_id,
  target,
  destination,
  format,
  status,
  timeout = FALSE,
  parent = NULL
) {
  if (isTRUE(timeout)) {
    message <- paste0(
      "KQL export ",
      operation_id,
      " completed, but its artifact details were not retrieved before the ",
      "client deadline; use .show operation ",
      operation_id,
      " details before resubmitting."
    )
    classes <- c(
      "fabric_kql_export_details_timeout",
      "fabric_kql_export_details_error",
      "fabric_kql_export_error"
    )
  } else {
    message <- paste0(
      "KQL export ",
      operation_id,
      " completed, but its artifact details could not be retrieved; use ",
      ".show operation ",
      operation_id,
      " details before resubmitting."
    )
    classes <- c(
      "fabric_kql_export_details_error",
      "fabric_kql_export_error"
    )
  }
  .fabric_abort(
    message,
    class = classes,
    operation_completed = TRUE,
    operation_id = operation_id,
    database = target$database,
    destination = destination$display,
    format = format,
    last_status = status,
    parent = parent
  )
}
