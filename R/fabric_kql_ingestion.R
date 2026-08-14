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
#' `sources` can be a character vector of storage connection strings, a data
#' frame with `url`, `source_id`, and optional `raw_size` columns, or a list of
#' records with those fields. The camel-case service names `sourceId` and
#' `rawSize` are also accepted. Character inputs use the parallel `source_ids`
#' and `raw_sizes` arguments
#'
#' Only existing `https://` or `abfss://` storage sources are accepted. Local
#' files and data frames containing the data itself are not staged implicitly.
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
#' @param allow_custom_endpoint Logical. Permit a trusted non-Microsoft Kusto
#'   HTTPS origin to receive credentials
#' @param .sleep,.now Internal hooks for deterministic polling tests
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
#' database <- fabric_kql_databases("Telemetry workspace")[[1]]
#' source <- paste0(
#'   "https://onelake.dfs.fabric.microsoft.com/workspace-id/",
#'   "lakehouse-id/Files/events/2026-08-14.csv;impersonate"
#' )
#'
#' ingestion <- fabric_kql_ingest(
#'   database,
#'   table = "Events",
#'   sources = source,
#'   format = "csv",
#'   mapping = "EventsCsv",
#'   ignore_first_record = TRUE,
#'   ingest_if_not_exists = "events-2026-08-14"
#' )
#'
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
  allow_custom_endpoint = FALSE
) {
  # 1 Validate the target and request metadata -----------------------------------------------------

  # Complete validation before authentication so local mistakes cannot prompt for sign-in

  if (missing(table)) {
    rlang::abort("table is required")
  }
  if (missing(sources)) {
    rlang::abort("sources is required")
  }
  if (missing(format)) {
    rlang::abort("format is required")
  }
  target <- kusto_resolve_ingestion_target(
    cluster,
    database,
    table,
    allow_custom_endpoint = allow_custom_endpoint
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
    rlang::abort(
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
    timeout = timeout
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
  allow_custom_endpoint = FALSE,
  .sleep = Sys.sleep,
  .now = Sys.time
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
    rlang::abort(".sleep and .now must be functions")
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
    allow_custom_endpoint = allow_custom_endpoint,
    override_auth = override_auth
  )
  started <- .now()
  deadline <- started + timeout

  # 2 Read one snapshot or poll until terminal -----------------------------------------------------

  # Status GETs are idempotent and can safely retry transient failures and throttling

  last <- NULL
  repeat {
    last <- tryCatch(
      kusto_ingestion_get_status(
        context,
        details = details,
        deadline = deadline
      ),
      fabric_http_deadline_error = function(error) {
        kusto_ingestion_timeout_error(context, timeout, last, error)
      }
    )
    if (!isTRUE(wait) || isTRUE(last$complete)) {
      break
    }

    elapsed <- as.numeric(difftime(.now(), started, units = "secs"))
    if (!is.finite(elapsed) || elapsed >= timeout) {
      kusto_ingestion_timeout_error(context, timeout, last)
    }
    delay <- max(
      .kusto_ingestion_poll_floor,
      poll_interval,
      last$retry_after %||% 0
    )
    .sleep(min(delay, timeout - elapsed))
    if (as.numeric(difftime(.now(), started, units = "secs")) >= timeout) {
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
  last
}

#' Print a tracked Kusto ingestion handle
#'
#' @param x A `fabric_kql_ingestion` handle
#' @param ... Unused
#' @return `x`, invisibly
#' @export
print.fabric_kql_ingestion <- function(x, ...) {
  cat("<fabric_kql_ingestion>\n")
  cat("  operation:", x$id, "\n")
  cat("  target:   ", paste0(x$database, ".", x$table), "\n")
  cat("  sources:  ", x$source_count, "\n")
  cat("  format:   ", x$format, "\n")
  invisible(x)
}

#' Print tracked Kusto ingestion status
#'
#' @param x A `fabric_kql_ingestion_status` record
#' @param ... Unused
#' @return `x`, invisibly
#' @export
print.fabric_kql_ingestion_status <- function(x, ...) {
  cat("<fabric_kql_ingestion_status>\n")
  cat("  operation:", x$operation_id, "\n")
  cat("  state:    ", x$state, "\n")
  cat(
    "  blobs:     ",
    paste0(
      x$succeeded,
      " succeeded, ",
      x$failed,
      " failed, ",
      x$in_progress,
      " in progress, ",
      x$canceled,
      " canceled"
    ),
    "\n"
  )
  invisible(x)
}

# Resolve an ingestion URI or discovery record. Returns a trusted service root
# plus validated database and table names used by submission and status calls
kusto_resolve_ingestion_target <- function(
  cluster,
  database,
  table,
  allow_custom_endpoint = FALSE
) {
  kusto_ingestion_flag(allow_custom_endpoint, "allow_custom_endpoint")
  record <- fabric_as_record(cluster)
  if (!is.null(record)) {
    type <- tolower(fabric_record_value(record, "type") %||% "")
    if (!type %in% c("eventhouse", "kqldatabase")) {
      rlang::abort(
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
    rlang::abort(paste0(
      "cluster must be a valid HTTPS Kusto ingestion-service origin using ",
      "the default port (443)"
    ))
  }
  trusted <- any(vapply(
    c(
      "kusto.fabric.microsoft.com",
      "kusto.windows.net",
      "kusto.data.microsoft.com"
    ),
    function(suffix) fabric_host_matches(parsed$hostname, suffix),
    logical(1)
  ))
  if (!trusted && !allow_custom_endpoint) {
    rlang::abort(paste0(
      "cluster must use a Microsoft Kusto ingestion endpoint; set ",
      "allow_custom_endpoint = TRUE only for a trusted custom origin"
    ))
  }
  list(
    url = endpoint,
    database = database,
    table = table,
    allow_custom_endpoint = isTRUE(allow_custom_endpoint)
  )
}

# Normalize all accepted source shapes. Returns service-ready records with a
# unique GUID and optional uncompressed size for every source URL
kusto_ingestion_sources <- function(sources, source_ids, raw_sizes) {
  structured <- is.data.frame(sources) ||
    is.list(sources) && !is.character(sources)
  if (structured && (!is.null(source_ids) || !is.null(raw_sizes))) {
    rlang::abort(
      "source_ids and raw_sizes cannot be combined with structured sources"
    )
  }

  if (is.character(sources)) {
    records <- lapply(sources, function(url) list(url = url))
    count <- length(records)
    if (!is.null(source_ids)) {
      if (!is.character(source_ids) || length(source_ids) != count) {
        rlang::abort("source_ids must contain one character value per source")
      }
      for (index in seq_len(count)) {
        records[[index]]$source_id <- source_ids[[index]]
      }
    }
    if (!is.null(raw_sizes)) {
      if (!is.numeric(raw_sizes) || length(raw_sizes) != count) {
        rlang::abort("raw_sizes must contain one numeric value per source")
      }
      for (index in seq_len(count)) {
        records[[index]]$raw_size <- raw_sizes[[index]]
      }
    }
  } else if (is.data.frame(sources)) {
    if (!"url" %in% names(sources)) {
      rlang::abort("structured sources must include a url field")
    }
    records <- lapply(seq_len(nrow(sources)), function(index) {
      as.list(sources[index, , drop = FALSE])
    })
  } else if (is.list(sources)) {
    if (!is.null(names(sources)) && "url" %in% names(sources)) {
      sources <- list(sources)
    }
    if (!all(vapply(sources, is.list, logical(1)))) {
      rlang::abort("structured sources must be a list of source records")
    }
    records <- sources
  } else {
    rlang::abort(
      "sources must be character storage URLs or structured source records"
    )
  }

  if (!length(records)) {
    rlang::abort("sources must contain at least one storage source")
  }
  if (length(records) > .kusto_ingestion_max_blobs) {
    rlang::abort(sprintf(
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
      rlang::abort("every source_id must be a GUID or missing")
    }
    normalized[[index]]$source_id <- tolower(source_id)
    existing_ids <- c(existing_ids, tolower(source_id))
  }
  if (anyDuplicated(existing_ids)) {
    rlang::abort("source_ids must be unique within an ingestion request")
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
      rlang::abort(sprintf(
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
    rlang::abort("the known raw_sizes exceed the 6 GB ingestion limit")
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
    rlang::abort(paste0(
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
    rlang::abort("source url must not exceed 32,768 bytes")
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
    rlang::abort(
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
kusto_ingestion_submit <- function(target, body, credential, timeout) {
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
      idempotent = FALSE
    ),
    error = function(error) {
      rlang::abort(
        paste0(
          "Queued ingestion submission failed before a tracking ID was ",
          "received. The request was not replayed because the service may ",
          "already have accepted it; verify the target before resubmitting."
        ),
        class = c(
          "fabric_kql_ingestion_submission_error",
          "fabric_kql_ingestion_error"
        ),
        status = error$status %||% NULL,
        response_metadata = error$response_metadata %||% NULL,
        parent = error
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
      function(blob) .httr2_redact(blob$url),
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
      allow_custom_endpoint = target$allow_custom_endpoint,
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
  allow_custom_endpoint,
  override_auth
) {
  if (inherits(ingestion, "fabric_kql_ingestion_status")) {
    ingestion <- ingestion$ingestion
  }
  if (inherits(ingestion, "fabric_kql_ingestion")) {
    if (!is.null(cluster) || !is.null(database) || !is.null(table)) {
      rlang::abort(
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
    custom <- ingestion$allow_custom_endpoint %||% allow_custom_endpoint
    target <- kusto_resolve_ingestion_target(
      ingestion$endpoint,
      ingestion$database,
      ingestion$table,
      allow_custom_endpoint = custom
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
    rlang::abort("cluster is required when ingestion is a raw operation ID")
  }
  target <- kusto_resolve_ingestion_target(
    cluster,
    database,
    table,
    allow_custom_endpoint = allow_custom_endpoint
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
kusto_ingestion_get_status <- function(context, details, deadline) {
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
    deadline = deadline
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
  safe_payload <- .httr2_redact_object(payload)
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
        "lastUpdated"
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
    .httr2_redact(item)
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
    rlang::abort(
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
  rlang::abort(
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
  rlang::abort(
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
  rlang::abort(
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
    rlang::abort("format must be one supported Kusto ingestion format")
  }
  value <- tolower(trimws(value))
  if (!value %in% .kusto_ingestion_formats) {
    rlang::abort(paste0(
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
    rlang::abort(sprintf("%s must not exceed 1,024 bytes", name))
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
    rlang::abort(sprintf("%s must be one non-empty character value", name))
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
    rlang::abort(sprintf(
      "%s must contain non-empty text values of at most 1,024 bytes",
      name
    ))
  }
  value <- trimws(value)
  if (anyDuplicated(value)) {
    rlang::abort(sprintf("%s values must be unique", name))
  }
  value
}

# Validate a strict scalar flag
kusto_ingestion_flag <- function(value, name) {
  if (!is.logical(value) || length(value) != 1L || is.na(value)) {
    rlang::abort(sprintf("%s must be TRUE or FALSE", name))
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
    rlang::abort(sprintf(
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
    rlang::abort(sprintf("%s must be one ISO 8601 date-time", name))
  }
  date <- "^[0-9]{4}-[0-9]{2}-[0-9]{2}$"
  datetime <- paste0(
    "^[0-9]{4}-[0-9]{2}-[0-9]{2}T",
    "[0-9]{2}:[0-9]{2}:[0-9]{2}(\\.[0-9]+)?",
    "(Z|[+-][0-9]{2}:[0-9]{2})$"
  )
  valid <- grepl(datetime, value) || allow_date && grepl(date, value)
  if (!valid) {
    rlang::abort(sprintf("%s must use ISO 8601 format", name))
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
    rlang::abort(
      "validation_policy is supported only for delimited text formats"
    )
  }
  if (is.list(value)) {
    if (
      is.null(names(value)) || anyNA(names(value)) || !all(nzchar(names(value)))
    ) {
      rlang::abort("validation_policy must be a named list or JSON object")
    }
    return(as.character(jsonlite::toJSON(
      value,
      auto_unbox = TRUE,
      null = "null",
      digits = 22
    )))
  }
  if (!is.character(value) || length(value) != 1L || is.na(value)) {
    rlang::abort("validation_policy must be a named list or JSON object")
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
    rlang::abort("validation_policy must contain one valid JSON object")
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
