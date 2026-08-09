#' Query a Microsoft Fabric Eventhouse with KQL
#'
#' Runs a read-only Kusto Query Language (KQL) query against a KQL database and
#' converts the result to R objects. In Fabric, an Eventhouse is a container for
#' one or more KQL databases designed for fast analysis of event, log,
#' telemetry, and time-series data.
#'
#' @details
#' The easiest input is an item from [fabric_kql_databases()], because it supplies
#' both the database name and its **Query URI**. If copying a URI from Fabric,
#' use **Query URI**, not **Ingestion URI**; this function queries existing data
#' and does not load new data. The caller needs database access through a Fabric
#' workspace role, Eventhouse sharing, or KQL database sharing.
#'
#' This function uses the Kusto v2 REST query endpoint and requests a token for
#' `https://api.kusto.windows.net/.default`.
#'
#' Query parameters are sent through Kusto client request properties, never
#' interpolated into `query`. Declare them in KQL with
#' `declare query_parameters(...)`. Scalar R values are encoded as Kusto
#' parameter values; vectors and lists are encoded as `dynamic(...)` literals.
#' Zero-length vectors and unnamed lists encode `dynamic([])`. A zero-length
#' list with non-`NULL` names encodes `dynamic({})`; `NULL` remains invalid so it
#' cannot be confused with an empty collection or a typed Kusto null.
#' Microsoft Fabric does not support the `queryconsistency` or
#' `query_weakconsistency_session_id` request properties. Do not include either
#' name in `request_properties`, even though Azure Data Explorer supports them.
#'
#' KQL `bool`, `datetime`, `int`, `long`, `real`, and `timespan` columns normally
#' become logical, UTC `POSIXct`, integer, `bit64::integer64`, double, and
#' `difftime` vectors. Base R and `bit64` reserve the minimum signed `int` and
#' `long` values for missing data; a column containing either boundary is
#' returned as character with a warning so the value remains exact. `dynamic`
#' columns are list-columns, GUIDs and strings are character vectors, and
#' decimals are doubles. Decimal values outside R's double precision should be
#' converted to strings in KQL when exact digits are needed.
#'
#' A query with one primary result table returns a tibble. A query with multiple
#' primary result tables returns a named list of tibbles with class
#' `fabric_kql_tables`. Auxiliary protocol tables are validated but not returned.
#' A query with no primary table returns an empty tibble.
#' Management commands and ingestion endpoints are intentionally not supported.
#'
#' @param cluster Query-service URI, or one Eventhouse or
#'   KQLDatabase record returned by [fabric_eventhouses()],
#'   [fabric_kql_databases()], or [fabric_item()]. A KQLDatabase record also
#'   supplies `database`. Despite the argument name, use Fabric's **Query URI**
#'   here.
#' @param query One non-empty, read-only KQL query, for example
#'   `"Events | where Severity == 'Error' | take 100"`.
#' @param database KQL database display name. Supply it with a copied Query URI
#'   or an Eventhouse record; omit it when `cluster` is a KQLDatabase record.
#' @param parameters Named list of values for parameters declared by
#'   `declare query_parameters(...)` in `query`. Scalar R values become Kusto
#'   scalar values; vectors and lists become `dynamic` values. Binding is safer
#'   and easier to quote correctly than building KQL with `paste()`.
#' @param request_properties Named list of Kusto client request options, such as
#'   `servertimeout = "2m"` or `notruncation = TRUE`. Most users can leave this
#'   empty; these are server-side Kusto controls, not query parameters. Fabric
#'   does not support `queryconsistency` or
#'   `query_weakconsistency_session_id`.
#' @param timeout Positive client-side HTTP timeout in seconds. This is separate
#'   from the Kusto `servertimeout` request property.
#' @param tenant_id Microsoft Entra tenant ID. Defaults to
#'   `FABRICQUERYR_TENANT_ID`.
#' @param client_id Microsoft Entra application/client ID. Defaults to
#'   `FABRICQUERYR_CLIENT_ID`, with the Azure CLI application ID as fallback.
#' @param token Optional `AzureAuth::AzureToken`, bearer-token string, or
#'   token-provider function. With `NULL`, `AzureAuth` reuses a matching cached
#'   token or starts its normal interactive login flow.
#' @param auth_args Named list of additional arguments passed to
#'   [AzureAuth::get_azure_token()] when no token source is supplied.
#' @param allow_custom_endpoint Logical. Permit a non-Microsoft Kusto HTTPS
#'   origin. Keep `FALSE` unless the endpoint is trusted; credentials are sent
#'   to the supplied origin.
#'
#' @return A typed tibble for one primary result, a `fabric_kql_tables` list for
#'   multiple primary results (one named element per table), or an empty tibble
#'   when there is no primary result. See Details for the KQL-to-R type mapping.
#' @references
#' [Access a KQL database and copy its Query URI](https://learn.microsoft.com/en-us/fabric/real-time-intelligence/access-database-copy-uri)
#'
#' [Kusto query HTTP request and parameters](https://learn.microsoft.com/en-us/kusto/api/rest/request?view=microsoft-fabric)
#'
#' [Kusto request properties](https://learn.microsoft.com/en-us/kusto/api/rest/request-properties?view=microsoft-fabric)
#'
#' [Kusto role-based access control](https://learn.microsoft.com/en-us/kusto/access-control/role-based-access-control?view=microsoft-fabric)
#' @export
#'
#' @examples
#' \dontrun{
#' database <- fabric_kql_databases("Telemetry workspace")[[1]]
#'
#' events <- fabric_kql_query(
#'   database,
#'   query = paste(
#'     "declare query_parameters(selected_type:string);",
#'     "Events | where EventType == selected_type | take 100"
#'   ),
#'   parameters = list(selected_type = "Warning")
#' )
#' }
fabric_kql_query <- function(
  cluster,
  query,
  database = NULL,
  parameters = list(),
  request_properties = list(),
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
  if (
    !is.character(query) ||
      length(query) != 1L ||
      is.na(query) ||
      !nzchar(trimws(query))
  ) {
    rlang::abort("query must be one non-empty character value")
  }
  target <- kusto_resolve_target(
    cluster,
    database,
    allow_custom_endpoint = allow_custom_endpoint
  )
  parameters <- kusto_encode_parameters(parameters)
  request_properties <- kusto_named_list(
    request_properties,
    "request_properties"
  )
  kusto_validate_request_properties(request_properties)
  if (
    !is.numeric(timeout) ||
      length(timeout) != 1L ||
      is.na(timeout) ||
      !is.finite(timeout) ||
      timeout <= 0
  ) {
    rlang::abort("timeout must be one positive number of seconds")
  }

  credential <- fabric_credential(
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args
  )
  kusto_execute_query(
    target$url,
    database = target$database,
    query = query,
    parameters = parameters,
    request_properties = request_properties,
    timeout = timeout,
    credential = credential
  )
}

kusto_resolve_target <- function(
  cluster,
  database = NULL,
  allow_custom_endpoint = FALSE
) {
  if (
    !is.logical(allow_custom_endpoint) ||
      length(allow_custom_endpoint) != 1L ||
      is.na(allow_custom_endpoint)
  ) {
    rlang::abort("allow_custom_endpoint must be TRUE or FALSE")
  }
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
      "query_service_uri",
      "queryServiceUri"
    )
    if (is.null(database) && identical(type, "kqldatabase")) {
      database <- fabric_record_value(record, "displayName", "display_name")
    }
  }
  if (
    !is.character(cluster) ||
      length(cluster) != 1L ||
      is.na(cluster) ||
      !nzchar(trimws(cluster))
  ) {
    rlang::abort(
      "cluster must supply one non-empty Kusto query-service URI"
    )
  }
  if (
    !is.character(database) ||
      length(database) != 1L ||
      is.na(database) ||
      !nzchar(trimws(database))
  ) {
    rlang::abort(
      "database is required unless cluster is a discovered KQLDatabase item"
    )
  }

  cluster <- sub("/+$", "", trimws(cluster))
  parsed <- try(httr2::url_parse(cluster), silent = TRUE)
  if (
    inherits(parsed, "try-error") ||
      !identical(parsed$scheme, "https") ||
      is.null(parsed$hostname) ||
      !nzchar(parsed$hostname) ||
      nzchar(parsed$username %||% "") ||
      nzchar(parsed$password %||% "") ||
      nzchar(parsed$port %||% "") ||
      length(parsed$query %||% list()) > 0L ||
      nzchar(parsed$fragment %||% "")
  ) {
    rlang::abort("cluster must be a valid HTTPS query-service URI")
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
      "cluster must use a Microsoft Kusto endpoint; set ",
      "allow_custom_endpoint = TRUE only for a trusted custom origin"
    ))
  }
  path <- parsed$path %||% ""
  if (
    !path %in% c("", "/") &&
      !grepl("/v[12]/rest/query/?$", path, ignore.case = TRUE)
  ) {
    rlang::abort(
      "cluster URI must be a service root or a Kusto REST query endpoint"
    )
  }
  url <- if (grepl("/v2/rest/query$", cluster, ignore.case = TRUE)) {
    cluster
  } else if (grepl("/v1/rest/query$", cluster, ignore.case = TRUE)) {
    sub("/v1/rest/query$", "/v2/rest/query", cluster, ignore.case = TRUE)
  } else {
    paste0(cluster, "/v2/rest/query")
  }
  list(url = url, database = trimws(database))
}

kusto_named_list <- function(value, name) {
  if (is.null(value)) {
    return(list())
  }
  if (!is.list(value)) {
    rlang::abort(cli::format_inline("{name} must be a named list"))
  }
  if (
    length(value) &&
      (is.null(names(value)) ||
        !all(nzchar(names(value))) ||
        anyDuplicated(names(value)))
  ) {
    rlang::abort(
      cli::format_inline("{name} must have unique, non-empty names")
    )
  }
  value
}

kusto_validate_request_properties <- function(request_properties) {
  unsupported <- c(
    "queryconsistency",
    "query_weakconsistency_session_id"
  )
  supplied <- names(request_properties)
  matches <- supplied[tolower(supplied) %in% unsupported]
  if (length(matches)) {
    rlang::abort(paste0(
      "Microsoft Fabric does not support request_properties: ",
      paste(matches, collapse = ", ")
    ))
  }
  invisible(request_properties)
}

kusto_encode_parameters <- function(parameters) {
  parameters <- kusto_named_list(parameters, "parameters")
  if (
    length(parameters) &&
      !all(grepl("^[A-Za-z_][A-Za-z0-9_]*$", names(parameters)))
  ) {
    rlang::abort("parameters names must be valid KQL identifiers")
  }
  lapply(parameters, kusto_encode_parameter)
}

kusto_encode_parameter <- function(value) {
  if (is.numeric(value) && length(value) == 1L && is.nan(value)) {
    return("real(nan)")
  }
  if (!is.null(value) && !length(value)) {
    empty <- if (is.list(value) && !is.null(names(value))) "{}" else "[]"
    return(paste0("dynamic(", empty, ")"))
  }
  if (is.null(value) || anyNA(value)) {
    rlang::abort(
      paste0(
        "KQL parameter values cannot be NULL or NA. ",
        "Use an explicit typed KQL null literal such as 'long(null)'"
      )
    )
  }
  if (
    inherits(value, c("POSIXt", "Date", "difftime", "integer64")) &&
      length(value) != 1L
  ) {
    rlang::abort(
      "Date, time, difftime, and integer64 KQL parameters must be scalar"
    )
  }
  if (inherits(value, "POSIXt")) {
    return(paste0(
      "datetime(",
      format(as.POSIXct(value, tz = "UTC"), "%Y-%m-%dT%H:%M:%OS6Z", tz = "UTC"),
      ")"
    ))
  }
  if (inherits(value, "Date")) {
    return(paste0("datetime(", format(value, "%Y-%m-%d"), ")"))
  }
  if (inherits(value, "difftime")) {
    seconds <- as.numeric(value, units = "secs")
    if (!is.finite(seconds)) {
      rlang::abort("KQL difftime parameter values must be finite")
    }
    return(paste0(
      "timespan(",
      format(
        seconds,
        scientific = FALSE,
        trim = TRUE
      ),
      "s)"
    ))
  }
  if (inherits(value, "integer64")) {
    return(as.character(value))
  }
  if (is.atomic(value) && length(value) == 1L) {
    if (is.logical(value)) {
      return(if (value) "true" else "false")
    }
    if (is.numeric(value)) {
      if (is.infinite(value)) {
        return(if (value > 0) "real(+inf)" else "real(-inf)")
      }
      return(format(value, digits = 17L, scientific = FALSE, trim = TRUE))
    }
    return(as.character(value))
  }
  json <- jsonlite::toJSON(
    value,
    auto_unbox = TRUE,
    null = "null",
    digits = NA
  )
  paste0("dynamic(", json, ")")
}

.kusto_next_request_id <- local({
  counter <- 0L
  function() {
    counter <<- if (counter == .Machine$integer.max) 1L else counter + 1L
    paste0(
      "fabricQueryR.Query;",
      format(Sys.time(), "%Y%m%d%H%M%OS6", tz = "UTC"),
      "-",
      Sys.getpid(),
      "-",
      counter
    )
  }
})

kusto_execute_query <- function(
  url,
  database,
  query,
  parameters,
  request_properties,
  timeout,
  credential
) {
  client_request_id <- .kusto_next_request_id()
  properties <- list(ClientRequestId = client_request_id)
  if (length(request_properties)) {
    properties$Options <- request_properties
  }
  if (length(parameters)) {
    properties$Parameters <- parameters
  }
  properties_json <- as.character(jsonlite::toJSON(
    properties,
    auto_unbox = TRUE,
    digits = 22,
    null = "null"
  ))
  req <- httr2::request(url) |>
    httr2::req_headers(
      Accept = "application/json",
      `x-ms-app` = "fabricQueryR",
      `x-ms-client-version` = as.character(
        utils::packageVersion("fabricQueryR")
      ),
      `x-ms-client-request-id` = client_request_id,
      `x-ms-readonly` = "true"
    ) |>
    httr2::req_body_json(
      list(db = database, csl = query, properties = properties_json),
      auto_unbox = TRUE,
      digits = 22,
      null = "null"
    ) |>
    httr2::req_timeout(timeout)
  resp <- .httr2_perform(
    req,
    credential = credential,
    audience = .fabric_audience$kusto,
    idempotent = TRUE
  )
  frames <- httr2::resp_body_json(
    resp,
    simplifyVector = FALSE,
    bigint_as_char = TRUE
  )
  kusto_parse_response(frames)
}

kusto_frame_type <- function(frame) {
  type <- frame$FrameType
  if (!is.null(type)) {
    return(type)
  }
  if (!is.null(frame$Version) && !is.null(frame$IsProgressive)) {
    return("DataSetHeader")
  }
  if (
    !is.null(frame$HasErrors) ||
      !is.null(frame$Cancelled) ||
      !is.null(frame$OneApiErrors)
  ) {
    return("DataSetCompletion")
  }
  if (
    !is.null(frame$TableKind) && !is.null(frame$Columns) && !is.null(frame$Rows)
  ) {
    return("DataTable")
  }
  if (!is.null(frame$TableKind) && !is.null(frame$Columns)) {
    return("TableHeader")
  }
  if (!is.null(frame$TableFragmentType) && !is.null(frame$Rows)) {
    return("TableFragment")
  }
  if (!is.null(frame$RowCount) && !is.null(frame$TableId)) {
    return("TableCompletion")
  }
  if (!is.null(frame$TableProgress)) {
    return("TableProgress")
  }
  "Unknown"
}

kusto_parse_response <- function(frames) {
  if (!is.list(frames) || !length(frames)) {
    rlang::abort("Kusto returned a malformed v2 response")
  }
  if (!all(vapply(frames, is.list, logical(1)))) {
    kusto_abort_malformed("every response frame must be an object")
  }
  frame_types <- vapply(frames, kusto_frame_type, character(1))
  if (!identical(frame_types[[1L]], "DataSetHeader")) {
    kusto_abort_malformed("DataSetHeader must be the first frame")
  }
  if (!identical(frame_types[[length(frame_types)]], "DataSetCompletion")) {
    kusto_abort_malformed("DataSetCompletion must be the final frame")
  }
  if (sum(frame_types == "DataSetHeader") != 1L) {
    kusto_abort_malformed("DataSetHeader must appear exactly once")
  }
  if (sum(frame_types == "DataSetCompletion") != 1L) {
    kusto_abort_malformed("DataSetCompletion must appear exactly once")
  }
  if (any(frame_types == "Unknown")) {
    kusto_abort_malformed("the response contains an unknown frame type")
  }

  header <- frames[[1L]]
  kusto_require_string(header, "Version", "DataSetHeader")
  if (!identical(header$Version, "v2.0")) {
    kusto_abort_malformed("DataSetHeader Version must be 'v2.0'")
  }
  kusto_require_flag(header, "IsProgressive", "DataSetHeader")

  tables <- list()
  table_order <- character()
  progressive_tables <- character()
  completed <- character()
  table_indexes <- if (length(frames) > 2L) {
    seq.int(2L, length(frames) - 1L)
  } else {
    integer()
  }
  for (index in table_indexes) {
    frame <- frames[[index]]
    type <- frame_types[[index]]
    if (identical(type, "DataTable")) {
      kusto_validate_table_frame(frame, "DataTable", require_rows = TRUE)
      key <- as.character(frame$TableId)
      if (key %in% table_order) {
        kusto_abort_malformed(paste0("duplicate table ID ", key))
      }
      tables[[key]] <- frame
      table_order <- c(table_order, key)
    } else if (identical(type, "TableHeader")) {
      if (!isTRUE(header$IsProgressive)) {
        kusto_abort_malformed(
          "progressive table frames require IsProgressive = true"
        )
      }
      kusto_validate_table_frame(frame, "TableHeader", require_rows = FALSE)
      key <- as.character(frame$TableId)
      if (key %in% table_order) {
        kusto_abort_malformed(paste0("duplicate table ID ", key))
      }
      frame$Rows <- list()
      tables[[key]] <- frame
      table_order <- c(table_order, key)
      progressive_tables <- c(progressive_tables, key)
    } else if (identical(type, "TableFragment")) {
      if (!isTRUE(header$IsProgressive)) {
        kusto_abort_malformed(
          "progressive table frames require IsProgressive = true"
        )
      }
      key <- kusto_progressive_table_key(frame, type, tables, completed)
      field_count <- kusto_require_count(frame, "FieldCount", type)
      if (field_count != length(tables[[key]]$Columns)) {
        kusto_abort_malformed(paste0(
          "TableFragment FieldCount does not match table ",
          key,
          " schema"
        ))
      }
      operation <- kusto_require_string(frame, "TableFragmentType", type)
      if (!operation %in% c("DataAppend", "DataReplace")) {
        kusto_abort_malformed(
          "TableFragmentType must be DataAppend or DataReplace"
        )
      }
      if (!"Rows" %in% names(frame) || !is.list(frame$Rows)) {
        kusto_abort_malformed("TableFragment Rows must be an array")
      }
      kusto_validate_rows(frame$Rows, field_count, "TableFragment")
      if (is.null(tables[[key]])) {
        kusto_abort_malformed("table fragment without a table header")
      }
      if (identical(operation, "DataReplace")) {
        tables[[key]]$Rows <- frame$Rows
      } else {
        tables[[key]]$Rows <- c(tables[[key]]$Rows, frame$Rows)
      }
    } else if (identical(type, "TableProgress")) {
      if (!isTRUE(header$IsProgressive)) {
        kusto_abort_malformed(
          "progressive table frames require IsProgressive = true"
        )
      }
      kusto_progressive_table_key(frame, type, tables, completed)
      progress <- frame$TableProgress
      if (
        !is.numeric(progress) ||
          length(progress) != 1L ||
          is.na(progress) ||
          !is.finite(progress) ||
          progress < 0 ||
          progress > 100
      ) {
        kusto_abort_malformed("TableProgress must be between 0 and 100")
      }
    } else if (identical(type, "TableCompletion")) {
      if (!isTRUE(header$IsProgressive)) {
        kusto_abort_malformed(
          "progressive table frames require IsProgressive = true"
        )
      }
      key <- kusto_progressive_table_key(frame, type, tables, completed)
      row_count <- kusto_require_count(frame, "RowCount", type)
      actual <- length(tables[[key]]$Rows)
      if (row_count != actual) {
        kusto_abort_malformed(sprintf(
          "TableCompletion RowCount is %d but table %s contains %d rows",
          row_count,
          key,
          actual
        ))
      }
      completed <- c(completed, key)
    }
  }
  incomplete <- setdiff(progressive_tables, completed)
  if (length(incomplete)) {
    kusto_abort_malformed(paste0(
      "missing TableCompletion for table ID(s): ",
      paste(incomplete, collapse = ", ")
    ))
  }
  completion <- frames[[length(frames)]]
  kusto_check_completion(completion)

  table_order <- unique(table_order)
  primary <- tables[table_order]
  primary <- primary[vapply(
    primary,
    function(table) {
      identical(
        tolower(table$TableKind %||% ""),
        "primaryresult"
      )
    },
    logical(1)
  )]
  if (!length(primary)) {
    return(tibble::tibble())
  }
  results <- lapply(primary, kusto_parse_table)
  result_names <- vapply(
    primary,
    function(table) table$TableName %||% "PrimaryResult",
    character(1)
  )
  names(results) <- make.unique(result_names, sep = "_")
  if (length(results) == 1L) {
    return(results[[1L]])
  }
  structure(results, class = c("fabric_kql_tables", "list"))
}

kusto_abort_malformed <- function(detail) {
  rlang::abort(
    paste0("Kusto returned a malformed v2 response: ", detail),
    class = "fabric_kql_protocol_error"
  )
}

kusto_require_string <- function(frame, field, type) {
  value <- frame[[field]]
  if (
    !is.character(value) ||
      length(value) != 1L ||
      is.na(value) ||
      !nzchar(value)
  ) {
    kusto_abort_malformed(paste(type, field, "must be one non-empty string"))
  }
  value
}

kusto_require_flag <- function(frame, field, type) {
  value <- frame[[field]]
  if (!is.logical(value) || length(value) != 1L || is.na(value)) {
    kusto_abort_malformed(paste(type, field, "must be true or false"))
  }
  value
}

kusto_require_count <- function(frame, field, type) {
  value <- frame[[field]]
  if (
    !is.numeric(value) ||
      length(value) != 1L ||
      is.na(value) ||
      !is.finite(value) ||
      value < 0 ||
      value != floor(value) ||
      value > .Machine$integer.max
  ) {
    kusto_abort_malformed(paste(type, field, "must be a non-negative integer"))
  }
  as.integer(value)
}

kusto_validate_table_frame <- function(frame, type, require_rows) {
  kusto_require_count(frame, "TableId", type)
  kusto_require_string(frame, "TableKind", type)
  kusto_require_string(frame, "TableName", type)
  columns <- frame$Columns
  if (!is.list(columns)) {
    kusto_abort_malformed(paste(type, "Columns must be an array"))
  }
  for (column in columns) {
    if (!is.list(column)) {
      kusto_abort_malformed(paste(type, "column definitions must be objects"))
    }
    kusto_require_string(column, "ColumnName", paste(type, "column"))
    kusto_require_string(column, "ColumnType", paste(type, "column"))
  }
  if (isTRUE(require_rows)) {
    if (!"Rows" %in% names(frame) || !is.list(frame$Rows)) {
      kusto_abort_malformed(paste(type, "Rows must be an array"))
    }
    kusto_validate_rows(frame$Rows, length(columns), type)
  }
  invisible(TRUE)
}

kusto_validate_rows <- function(rows, field_count, type) {
  for (row in rows) {
    if (!is.list(row) || length(row) != field_count) {
      kusto_abort_malformed(sprintf(
        "%s row width must equal the declared %d columns",
        type,
        field_count
      ))
    }
  }
  invisible(TRUE)
}

kusto_progressive_table_key <- function(frame, type, tables, completed) {
  table_id <- kusto_require_count(frame, "TableId", type)
  key <- as.character(table_id)
  if (is.null(tables[[key]])) {
    kusto_abort_malformed(paste(type, "was received without a TableHeader"))
  }
  if (key %in% completed) {
    kusto_abort_malformed(paste(type, "was received after TableCompletion"))
  }
  key
}

kusto_check_completion <- function(completion) {
  kusto_require_flag(completion, "HasErrors", "DataSetCompletion")
  kusto_require_flag(completion, "Cancelled", "DataSetCompletion")
  if (isTRUE(completion$Cancelled)) {
    rlang::abort("Kusto query was cancelled before completion")
  }
  if (!isTRUE(completion$HasErrors)) {
    return(invisible())
  }
  errors <- completion$OneApiErrors %||% list()
  detail <- unlist(errors, recursive = TRUE, use.names = TRUE)
  detail <- unique(as.character(detail[!is.na(detail) & nzchar(detail)]))
  if (!length(detail)) {
    detail <- "The service reported an unspecified partial query failure"
  }
  rlang::abort(
    paste0(
      "Kusto query failed after HTTP success: ",
      paste(detail, collapse = ": ")
    )
  )
}

kusto_parse_table <- function(table) {
  columns <- table$Columns %||% list()
  rows <- table$Rows %||% list()
  names <- vapply(
    columns,
    function(column) column$ColumnName %||% "",
    character(1)
  )
  types <- vapply(
    columns,
    function(column) tolower(column$ColumnType %||% "string"),
    character(1)
  )
  values <- lapply(seq_along(columns), function(index) {
    column <- lapply(rows, function(row) {
      if (length(row) < index) NULL else row[[index]]
    })
    kusto_convert_column(column, types[[index]])
  })
  names(values) <- names
  out <- tibble::as_tibble(values, .name_repair = "minimal")
  attr(out, "kusto_schema") <- tibble::tibble(name = names, type = types)
  attr(out, "kusto_table_name") <- table$TableName %||% NA_character_
  attr(out, "kusto_table_id") <- table$TableId %||% NA
  out
}

kusto_character_column <- function(values) {
  vapply(
    values,
    function(value) {
      if (is.null(value)) NA_character_ else as.character(value)
    },
    character(1)
  )
}

kusto_numeric_column <- function(values, integer = FALSE) {
  text <- kusto_character_column(values)
  special <- c(
    "NaN" = NaN,
    "Infinity" = Inf,
    "+Infinity" = Inf,
    "-Infinity" = -Inf,
    "inf" = Inf,
    "+inf" = Inf,
    "-inf" = -Inf
  )
  out <- suppressWarnings(as.numeric(text))
  matched <- match(text, names(special))
  out[!is.na(matched)] <- unname(special[matched[!is.na(matched)]])
  if (integer) {
    out <- suppressWarnings(as.integer(text))
  }
  invalid <- !is.na(text) & is.na(out)
  if (any(invalid)) {
    rlang::abort(
      "Kusto returned an invalid numeric value for its declared type"
    )
  }
  out
}

kusto_integer_column <- function(values, type) {
  text <- kusto_character_column(values)
  minimum <- if (identical(type, "int")) {
    "-2147483648"
  } else {
    "-9223372036854775808"
  }
  contains_minimum <- any(text == minimum, na.rm = TRUE)
  if (identical(type, "int")) {
    parsed <- suppressWarnings(as.integer(text))
  } else {
    parsed <- suppressWarnings(bit64::as.integer64(text))
  }
  invalid <- !is.na(text) & is.na(parsed) & text != minimum
  if (any(invalid)) {
    rlang::abort(
      "Kusto returned an invalid integer value for its declared type"
    )
  }
  if (contains_minimum) {
    rlang::warn(
      paste0(
        "KQL `",
        type,
        "` column contains ",
        minimum,
        ", which R reserves for missing data; returning it as character"
      ),
      class = "fabric_kql_integer_boundary_warning"
    )
    return(text)
  }
  parsed
}

kusto_datetime_column <- function(values) {
  text <- kusto_character_column(values)
  clean <- sub("Z$", "", text, ignore.case = TRUE)
  out <- as.POSIXct(
    strptime(clean, format = "%Y-%m-%dT%H:%M:%OS", tz = "UTC"),
    tz = "UTC"
  )
  invalid <- !is.na(text) & is.na(out)
  if (any(invalid)) {
    rlang::abort("Kusto returned an invalid datetime value")
  }
  out
}

kusto_timespan_seconds <- function(value) {
  if (is.na(value)) {
    return(NA_real_)
  }
  match <- regexec(
    "^(-)?(?:(\\d+)\\.)?(\\d{1,2}):(\\d{2}):(\\d{2}(?:\\.\\d+)?)$",
    value
  )
  parts <- regmatches(value, match)[[1L]]
  if (!length(parts)) {
    rlang::abort("Kusto returned an invalid timespan value")
  }
  sign <- if (identical(parts[[2L]], "-")) -1 else 1
  days <- if (nzchar(parts[[3L]])) as.numeric(parts[[3L]]) else 0
  sign *
    (days *
      86400 +
      as.numeric(parts[[4L]]) * 3600 +
      as.numeric(parts[[5L]]) * 60 +
      as.numeric(parts[[6L]]))
}

kusto_dynamic_value <- function(value) {
  if (is.null(value)) {
    return(NULL)
  }
  if (!is.character(value) || length(value) != 1L) {
    return(value)
  }
  parsed <- try(
    jsonlite::fromJSON(value, simplifyVector = FALSE, bigint_as_char = TRUE),
    silent = TRUE
  )
  if (inherits(parsed, "try-error")) value else parsed
}

kusto_convert_column <- function(values, type) {
  if (type %in% c("string", "guid", "uuid", "uniqueid")) {
    return(kusto_character_column(values))
  }
  if (type %in% c("bool", "boolean")) {
    return(vapply(
      values,
      function(value) {
        if (is.null(value)) NA else isTRUE(value)
      },
      logical(1)
    ))
  }
  if (type == "int") {
    return(kusto_integer_column(values, type))
  }
  if (type == "long") {
    return(kusto_integer_column(values, type))
  }
  if (type %in% c("real", "double", "decimal")) {
    return(kusto_numeric_column(values))
  }
  if (type %in% c("datetime", "date")) {
    return(kusto_datetime_column(values))
  }
  if (type %in% c("timespan", "time")) {
    seconds <- vapply(
      kusto_character_column(values),
      kusto_timespan_seconds,
      numeric(1),
      USE.NAMES = FALSE
    )
    return(as.difftime(seconds, units = "secs"))
  }
  if (type == "dynamic") {
    return(lapply(values, kusto_dynamic_value))
  }
  kusto_character_column(values)
}
