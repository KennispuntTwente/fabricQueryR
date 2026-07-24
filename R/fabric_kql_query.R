#' Query a Microsoft Fabric Eventhouse with KQL
#'
#' Executes a read-only Kusto Query Language (KQL) query against a Fabric
#' Eventhouse query service and converts primary result tables to typed tibbles.
#'
#' @details
#' This function uses the Kusto v2 REST query endpoint and requests a token for
#' `https://api.kusto.windows.net/.default`. The caller needs access to the KQL
#' database, normally through a Fabric workspace role or KQL database sharing.
#'
#' Query parameters are sent through Kusto client request properties, never
#' interpolated into `query`. Declare them in KQL with
#' `declare query_parameters(...)`. Scalar R values are encoded as Kusto
#' parameter values; vectors and lists are encoded as `dynamic(...)` literals.
#'
#' KQL `bool`, `datetime`, `int`, `long`, `real`, and `timespan` columns become
#' logical, UTC `POSIXct`, integer, `bit64::integer64`, double, and `difftime`
#' vectors. `dynamic` columns are list-columns, GUIDs and strings are character
#' vectors, and decimals are doubles. Decimal values outside R's double
#' precision should be converted to strings in KQL when exact digits are needed.
#'
#' A query with one primary result table returns a tibble. A query with multiple
#' primary result tables returns a named list of tibbles with class
#' `fabric_kql_tables`. Auxiliary protocol tables are validated but not returned.
#' A query with no primary table returns an empty tibble.
#' Management commands and ingestion endpoints are intentionally not supported.
#'
#' @param cluster Character query-service/cluster URI, or one Eventhouse or
#'   KQLDatabase record returned by [fabric_eventhouses()],
#'   [fabric_kql_databases()], or [fabric_item()]. A KQLDatabase record also
#'   supplies `database`.
#' @param query A non-empty KQL query.
#' @param database KQL database display name. Required for a direct URI or an
#'   Eventhouse record; inferred from a KQLDatabase record.
#' @param parameters Named list of values for parameters declared by
#'   `declare query_parameters(...)` in `query`.
#' @param request_properties Named list of Kusto client request options, such as
#'   `servertimeout = "2m"` or `notruncation = TRUE`.
#' @param timeout Positive request timeout in seconds.
#' @param tenant_id Microsoft Entra tenant ID. Defaults to
#'   `FABRICQUERYR_TENANT_ID`.
#' @param client_id Microsoft Entra application/client ID. Defaults to
#'   `FABRICQUERYR_CLIENT_ID`, with the Azure CLI application ID as fallback.
#' @param access_token Optional Kusto bearer token. Supply only one of
#'   `access_token` and `token_provider`.
#' @param token_provider Optional callback returning a Kusto bearer token. It
#'   may accept `audience` and `force_refresh` arguments.
#'
#' @return A typed tibble for one primary result, a `fabric_kql_tables` list for
#'   multiple primary results, or an empty tibble when there is no primary
#'   result.
#' @export
#'
#' @examples
#' \dontrun{
#' database <- fabric_kql_databases("Telemetry workspace")[1, ]
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
  access_token = NULL,
  token_provider = NULL
) {
  if (
    !is.character(query) ||
      length(query) != 1L ||
      is.na(query) ||
      !nzchar(trimws(query))
  ) {
    stop("query must be one non-empty character value.", call. = FALSE)
  }
  target <- kusto_resolve_target(cluster, database)
  parameters <- kusto_encode_parameters(parameters)
  request_properties <- kusto_named_list(
    request_properties,
    "request_properties"
  )
  if (
    !is.numeric(timeout) ||
      length(timeout) != 1L ||
      is.na(timeout) ||
      !is.finite(timeout) ||
      timeout <= 0
  ) {
    stop("timeout must be one positive number of seconds.", call. = FALSE)
  }

  credential <- fabric_credential(
    tenant_id = tenant_id,
    client_id = client_id,
    access_token = access_token,
    token_provider = token_provider
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

kusto_resolve_target <- function(cluster, database = NULL) {
  record <- fabric_as_record(cluster)
  if (!is.null(record)) {
    type <- tolower(fabric_record_value(record, "type") %||% "")
    if (!type %in% c("eventhouse", "kqldatabase")) {
      stop(
        "cluster discovery record must be an Eventhouse or KQLDatabase item.",
        call. = FALSE
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
    stop(
      "cluster must supply one non-empty Kusto query-service URI.",
      call. = FALSE
    )
  }
  if (
    !is.character(database) ||
      length(database) != 1L ||
      is.na(database) ||
      !nzchar(trimws(database))
  ) {
    stop(
      "database is required unless cluster is a discovered KQLDatabase item.",
      call. = FALSE
    )
  }

  cluster <- sub("/+$", "", trimws(cluster))
  parsed <- try(httr2::url_parse(cluster), silent = TRUE)
  if (
    inherits(parsed, "try-error") ||
      !identical(parsed$scheme, "https") ||
      is.null(parsed$hostname) ||
      !nzchar(parsed$hostname)
  ) {
    stop("cluster must be a valid HTTPS query-service URI.", call. = FALSE)
  }
  path <- parsed$path %||% ""
  if (
    !path %in% c("", "/") &&
      !grepl("/v[12]/rest/query/?$", path, ignore.case = TRUE)
  ) {
    stop(
      "cluster URI must be a service root or a Kusto REST query endpoint.",
      call. = FALSE
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
    stop(name, " must be a named list.", call. = FALSE)
  }
  if (
    length(value) &&
      (is.null(names(value)) ||
        !all(nzchar(names(value))) ||
        anyDuplicated(names(value)))
  ) {
    stop(name, " must have unique, non-empty names.", call. = FALSE)
  }
  value
}

kusto_encode_parameters <- function(parameters) {
  parameters <- kusto_named_list(parameters, "parameters")
  if (
    length(parameters) &&
      !all(grepl("^[A-Za-z_][A-Za-z0-9_]*$", names(parameters)))
  ) {
    stop("parameters names must be valid KQL identifiers.", call. = FALSE)
  }
  lapply(parameters, kusto_encode_parameter)
}

kusto_encode_parameter <- function(value) {
  if (is.null(value) || !length(value) || anyNA(value)) {
    stop(
      paste0(
        "KQL parameter values cannot be NULL, empty, or NA. ",
        "Use an explicit typed KQL null literal such as 'long(null)'."
      ),
      call. = FALSE
    )
  }
  if (
    inherits(value, c("POSIXt", "Date", "difftime", "integer64")) &&
      length(value) != 1L
  ) {
    stop(
      "Date, time, difftime, and integer64 KQL parameters must be scalar.",
      call. = FALSE
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
    return(paste0(
      "timespan(",
      format(
        as.numeric(value, units = "secs"),
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
  if (!is.list(frames)) {
    stop("Kusto returned a malformed v2 response.", call. = FALSE)
  }
  completion <- NULL
  tables <- list()
  table_order <- character()
  for (frame in frames) {
    type <- kusto_frame_type(frame)
    if (identical(type, "DataSetCompletion")) {
      completion <- frame
    } else if (identical(type, "DataTable")) {
      key <- as.character(frame$TableId %||% length(table_order))
      tables[[key]] <- frame
      table_order <- c(table_order, key)
    } else if (identical(type, "TableHeader")) {
      key <- as.character(frame$TableId)
      frame$Rows <- list()
      tables[[key]] <- frame
      table_order <- c(table_order, key)
    } else if (identical(type, "TableFragment")) {
      key <- as.character(frame$TableId)
      if (is.null(tables[[key]])) {
        stop(
          "Kusto returned a table fragment without a table header.",
          call. = FALSE
        )
      }
      if (identical(frame$TableFragmentType, "DataReplace")) {
        tables[[key]]$Rows <- frame$Rows %||% list()
      } else {
        tables[[key]]$Rows <- c(
          tables[[key]]$Rows %||% list(),
          frame$Rows %||% list()
        )
      }
    }
  }
  if (is.null(completion)) {
    stop(
      "Kusto v2 response did not include a DataSetCompletion frame.",
      call. = FALSE
    )
  }
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

kusto_check_completion <- function(completion) {
  if (isTRUE(completion$Cancelled)) {
    stop("Kusto query was cancelled before completion.", call. = FALSE)
  }
  if (!isTRUE(completion$HasErrors)) {
    return(invisible())
  }
  errors <- completion$OneApiErrors %||% list()
  detail <- unlist(errors, recursive = TRUE, use.names = TRUE)
  detail <- unique(as.character(detail[!is.na(detail) & nzchar(detail)]))
  if (!length(detail)) {
    detail <- "The service reported an unspecified partial query failure."
  }
  stop(
    paste0(
      "Kusto query failed after HTTP success: ",
      paste(detail, collapse = ": ")
    ),
    call. = FALSE
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
    stop(
      "Kusto returned an invalid numeric value for its declared type.",
      call. = FALSE
    )
  }
  out
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
    stop("Kusto returned an invalid datetime value.", call. = FALSE)
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
    stop("Kusto returned an invalid timespan value.", call. = FALSE)
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
    return(kusto_numeric_column(values, integer = TRUE))
  }
  if (type == "long") {
    return(bit64::as.integer64(kusto_character_column(values)))
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
      numeric(1)
    )
    return(as.difftime(seconds, units = "secs"))
  }
  if (type == "dynamic") {
    return(lapply(values, kusto_dynamic_value))
  }
  kusto_character_column(values)
}
