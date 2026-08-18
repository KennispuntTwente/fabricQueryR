#' Discover Microsoft Fabric KQL tables
#'
#' Lists tables in a Fabric KQL database through Kusto's management endpoint.
#' With `detail = TRUE`, retrieves each table's JSON schema and exposes its
#' ordered columns while retaining the complete metadata response.
#'
#' @inheritParams fabric_kql_query
#' @param detail Whether to retrieve the JSON schema for every table. Set to
#'   `FALSE` to issue only the database-level table-list command.
#'
#' @return A tibble with table `name`, `database`, `folder`, `description`,
#'   list-column `columns`, parsed `schema_metadata`, and the unmodified listing
#'   row in `raw`.
#'
#' @references
#' [Kusto `.show tables` command](https://learn.microsoft.com/en-us/kusto/management/show-tables-command?view=microsoft-fabric)
#'
#' [Kusto `.show table schema` command](https://learn.microsoft.com/en-us/kusto/management/show-table-schema-command?view=microsoft-fabric)
#' @export
#'
#' @examples
#' \dontrun{
#' workspace <- fabric_workspaces()[[1L]]
#' database <- fabric_kql_databases(workspace)[[1L]]
#'
#' tables <- fabric_kql_tables(database)
#' events <- fabric_kql_read_table(database, tables[1L, ], limit = 1000)
#' }
fabric_kql_tables <- function(
  cluster,
  database = NULL,
  detail = TRUE,
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
  .fabric_operation_logical(detail, "detail")
  if (
    !is.numeric(timeout) ||
      length(timeout) != 1L ||
      is.na(timeout) ||
      !is.finite(timeout) ||
      timeout <= 0
  ) {
    .fabric_abort(
      "timeout must be one positive number of seconds",
      class = "fabric_kql_tables_error"
    )
  }
  target <- kusto_resolve_target(
    cluster,
    database,
    allow_custom_endpoint = allow_custom_endpoint
  )
  credential <- fabric_credential(
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args
  )
  error_class <- c(
    "fabric_kql_tables_protocol_error",
    "fabric_kql_tables_error"
  )
  deadline <- Sys.time() + timeout
  response <- kusto_execute_management(
    target = target,
    command = ".show tables",
    credential = credential,
    deadline = deadline,
    idempotent = TRUE,
    operation = "ListTables",
    request_prefix = "Tables",
    error_class = error_class
  )
  table <- kusto_management_table(
    response$tables,
    c("TableName", "DatabaseName", "Folder", "DocString"),
    error_class
  )
  result <- kusto_table_inventory(table)

  if (isTRUE(detail) && nrow(result)) {
    for (index in seq_len(nrow(result))) {
      identifier <- kusto_entity_identifier(
        result$name[[index]],
        "table",
        error_class
      )
      schema_response <- kusto_execute_management(
        target = target,
        command = paste(
          ".show table",
          identifier,
          "schema as json"
        ),
        credential = credential,
        deadline = deadline,
        idempotent = TRUE,
        operation = "GetTable",
        request_prefix = "Tables",
        error_class = error_class
      )
      schema_table <- kusto_management_table(
        schema_response$tables,
        c("TableName", "Schema"),
        error_class
      )
      schema_text <- as.character(
        kusto_export_column(schema_table, "Schema")[[1L]]
      )
      metadata <- kusto_table_schema_metadata(schema_text, error_class)
      result$columns[[index]] <- metadata$OrderedColumns %||%
        metadata$orderedColumns %||%
        list()
      result$schema_metadata[[index]] <- metadata
    }
  }

  result
}

kusto_table_inventory <- function(table) {
  count <- nrow(table)
  if (!count) {
    return(tibble::tibble(
      name = character(),
      database = character(),
      folder = character(),
      description = character(),
      columns = list(),
      schema_metadata = list(),
      raw = list()
    ))
  }
  tibble::tibble(
    name = as.character(kusto_export_column(table, "TableName")),
    database = as.character(kusto_export_column(table, "DatabaseName")),
    folder = as.character(kusto_export_column(table, "Folder")),
    description = as.character(kusto_export_column(table, "DocString")),
    columns = rep(list(list()), count),
    schema_metadata = rep(list(list()), count),
    raw = lapply(seq_len(count), function(index) {
      lapply(table, `[[`, index)
    })
  )
}

kusto_table_schema_metadata <- function(value, error_class) {
  if (
    !is.character(value) ||
      length(value) != 1L ||
      is.na(value) ||
      !nzchar(value)
  ) {
    .fabric_abort("Kusto returned an empty table schema", class = error_class)
  }
  tryCatch(
    jsonlite::fromJSON(value, simplifyVector = FALSE),
    error = function(error) {
      .fabric_abort(
        "Kusto returned invalid table schema JSON",
        class = error_class,
        parent = error
      )
    }
  )
}

#' Read a Microsoft Fabric KQL table
#'
#' Provides the table-oriented read counterpart to [fabric_kql_write_table()].
#' It safely resolves the table through Kusto's `table()` function, optionally
#' projects columns and limits rows, then delegates typed result handling to
#' [fabric_kql_query()]. Use that lower-level function for filters, ordering,
#' joins, aggregations, or other KQL expressions.
#'
#' @inheritParams fabric_kql_query
#' @param table KQL table name, or a record containing a `name`, `table`, or
#'   `displayName` field.
#' @param columns Optional unique column names to project.
#' @param limit Optional non-negative whole-number maximum number of rows to
#'   return, no greater than Kusto's signed 32-bit `take` limit.
#'
#' @return A typed tibble containing the selected table rows. Kusto metadata is
#'   retained in the same attributes as [fabric_kql_query()].
#'
#' @section Large results:
#' The Kusto query HTTP response is collected and decoded in R. Use `columns`
#' and `limit` to bound an interactive read. For a result too large for client
#' memory, use [fabric_kql_export()] to export it server-side to OneLake or
#' another supported storage destination.
#'
#' @references
#' [Kusto `table()` function](https://learn.microsoft.com/en-us/kusto/query/table-function?view=microsoft-fabric)
#'
#' [Kusto `take` operator](https://learn.microsoft.com/en-us/kusto/query/take-operator?view=microsoft-fabric)
#'
#' [Kusto entity names](https://learn.microsoft.com/en-us/kusto/query/schema-entities/entity-names?view=microsoft-fabric)
#'
#' [Kusto query HTTP request](https://learn.microsoft.com/en-us/kusto/api/rest/request?view=microsoft-fabric)
#' @export
#'
#' @examples
#' \dontrun{
#' # Discover a KQL database instead of copying its Query URI and name
#' workspace <- fabric_workspaces()[[1L]]
#' database <- fabric_kql_databases(workspace)[[1L]]
#'
#' # Choose an existing table shown under Tables in the Fabric KQL explorer
#' table <- Sys.getenv("FABRIC_KQL_TABLE")
#'
#' # Read a bounded portion of that table into a tibble
#' events <- fabric_kql_read_table(
#'   database,
#'   table,
#'   limit = 1000
#' )
#' }
fabric_kql_read_table <- function(
  cluster,
  table,
  database = NULL,
  columns = NULL,
  limit = NULL,
  request_properties = list(),
  timeout = 60,
  retain_raw_frames = FALSE,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  token = NULL,
  auth_args = list(),
  allow_custom_endpoint = FALSE
) {
  table_record <- fabric_as_record(table)
  if (is.null(table_record) && is.list(table)) {
    table_record <- table
  }
  if (!is.null(table_record)) {
    table <- fabric_record_value(
      table_record,
      "name",
      "table",
      "displayName"
    )
  }
  table <- kusto_ingestion_target_name(table, "table")
  kusto_read_columns(columns)
  limit <- kusto_read_limit(limit)

  query <- paste(
    "declare query_parameters(_fabricqueryr_table:string);",
    "table(_fabricqueryr_table)"
  )
  if (!is.null(columns)) {
    projection <- paste(
      vapply(
        columns,
        kusto_read_identifier,
        character(1),
        USE.NAMES = FALSE
      ),
      collapse = ", "
    )
    query <- paste(query, "| project", projection)
  }
  if (!is.null(limit)) {
    query <- paste(query, "| take", as.character(limit))
  }

  fabric_kql_query(
    cluster = cluster,
    query = query,
    database = database,
    parameters = list(`_fabricqueryr_table` = table),
    request_properties = request_properties,
    timeout = timeout,
    retain_raw_frames = retain_raw_frames,
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args,
    allow_custom_endpoint = allow_custom_endpoint
  )
}

# Validate an optional KQL projection before authentication. Returns invisibly
# after checking unique, non-empty column names
kusto_read_columns <- function(columns) {
  if (
    !is.null(columns) &&
      (!is.character(columns) ||
        !length(columns) ||
        anyNA(columns) ||
        !all(nzchar(columns)) ||
        anyDuplicated(columns))
  ) {
    .fabric_abort(
      "columns must be NULL or one or more unique, non-empty strings",
      class = "fabric_kql_read_error"
    )
  }
  invisible(columns)
}

# Validate the row limit supported by Kusto's take operator. Returns an integer
# or NULL before the generated query is submitted
kusto_read_limit <- function(limit) {
  if (is.null(limit)) {
    return(NULL)
  }
  if (
    !is.numeric(limit) ||
      length(limit) != 1L ||
      is.na(limit) ||
      !is.finite(limit) ||
      limit < 0 ||
      limit != floor(limit) ||
      limit > .Machine$integer.max
  ) {
    .fabric_abort(
      paste0(
        "limit must be NULL or one non-negative whole number no greater than ",
        .Machine$integer.max
      ),
      class = "fabric_kql_read_error"
    )
  }
  as.integer(limit)
}

# Quote one projected Kusto column using the same documented entity-name subset
# accepted by the writer. Returns safe KQL source text
kusto_read_identifier <- function(value) {
  kusto_entity_identifier(
    value,
    "column",
    error_class = "fabric_kql_read_error"
  )
}

#' Run a KQL query in Microsoft Fabric
#'
#' Runs a read-only query against a KQL database and returns the result as a
#' tibble. KQL databases are commonly used for event, log, telemetry, and
#' time-series data in a Fabric Eventhouse
#'
#' @section Basic use:
#' The easiest input is an item from [fabric_kql_databases()], which already
#' contains the database name and its **Query URI**. If you copy a URI from
#' Fabric, choose **Query URI**, not **Ingestion URI**. This function reads
#' existing data; it does not load data or run management commands
#'
#' Put changing values in `parameters` and declare them in KQL with
#' `declare query_parameters(...)`. The values are sent separately from the
#' query text, which is safer and easier to quote correctly than using
#' `paste()`. Scalar R values become KQL scalar values; vectors and lists become
#' `dynamic` arrays or objects
#'
#' @section Advanced request options:
#' `request_properties` controls server behavior such as timeouts and result
#' truncation. Most users can leave it empty
#' Microsoft Fabric does not support the `queryconsistency` or
#' `query_weakconsistency_session_id` request properties. Do not include either
#' name in `request_properties`, even though Azure Data Explorer supports them
#'
#' @section Result types:
#' KQL `bool`, `datetime`, `int`, `long`, `real`, and `timespan` columns normally
#' become logical, UTC `POSIXct`, integer, `bit64::integer64`, double, and
#' `difftime` vectors. Base R and `bit64` reserve the minimum signed `int` and
#' `long` values for missing data; a column containing either boundary is
#' returned as character with a warning so the value remains exact. `dynamic`
#' columns are list-columns, and GUIDs, strings, and `decimal` values are
#' character vectors. Keeping decimal values in their original lexical form
#' avoids the silent precision loss that conversion to an R double can cause
#'
#' A query with several result tables returns a named `fabric_kql_tables` list;
#' a query with no result table returns an empty tibble. Service metadata is
#' retained in `kusto_*` attributes for troubleshooting
#'
#' @section Permissions:
#' The caller needs database access through a Fabric workspace role, Eventhouse
#' sharing, or KQL database sharing. Authentication uses the Kusto query service
#'
#' @param cluster Query URI, or one Eventhouse or
#'   KQLDatabase record returned by [fabric_eventhouses()],
#'   [fabric_kql_databases()], or [fabric_item()]. A KQLDatabase record also
#'   supplies `database`. Despite the argument name, use Fabric's **Query URI**
#'   here
#' @param query One non-empty, read-only KQL query, for example
#'   `"Events | where Severity == 'Error' | take 100"`
#' @param database KQL database display name. Supply it with a copied Query URI
#'   or an Eventhouse record; omit it when `cluster` is a KQLDatabase record
#' @param parameters Named list of values declared with
#'   `declare query_parameters(...)` in `query`
#' @param request_properties Named list of Kusto client request options, such as
#'   `servertimeout = "2m"` or `notruncation = TRUE`. Most users can leave this
#'   empty; these are server-side Kusto controls, not query parameters. Fabric
#'   does not support `queryconsistency` or
#'   `query_weakconsistency_session_id`
#' @param timeout Positive client-side HTTP timeout in seconds. This is separate
#'   from the Kusto `servertimeout` request property
#' @param retain_raw_frames Logical. Attach the complete decoded Kusto frame
#'   response as `kusto_raw_frames`. Keep `FALSE` for normal queries to avoid
#'   retaining a second copy of large result data
#' @param tenant_id Microsoft Entra tenant ID. Defaults to
#'   `FABRICQUERYR_TENANT_ID`
#' @param client_id Microsoft Entra application/client ID. Defaults to
#'   `FABRICQUERYR_CLIENT_ID`, with the Azure CLI application ID as fallback
#' @param token Optional access token or token-provider function. Leave `NULL`
#'   to let fabricQueryR use its normal sign-in flow
#' @param auth_args Additional sign-in options passed to
#'   [AzureAuth::get_azure_token()]
#' @param allow_custom_endpoint Logical. Permit a non-Microsoft Kusto HTTPS
#'   origin. Keep `FALSE` unless the endpoint is trusted; credentials are sent
#'   to the supplied origin
#'
#' @return A typed tibble for one primary result, a `fabric_kql_tables` list for
#'   multiple primary results (one named element per table), or an empty tibble
#'   when there is no primary result. See Details for the KQL-to-R type mapping
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
#' # Discover the KQL database and choose one of its existing tables
#' workspace <- fabric_workspaces()[[1L]]
#' database <- fabric_kql_databases(workspace)[[1L]]
#' table <- Sys.getenv("FABRIC_KQL_TABLE")
#'
#' # Keep the changing table name out of the KQL text by using a parameter
#' events <- fabric_kql_query(
#'   database,
#'   query = paste(
#'     "declare query_parameters(selected_table:string);",
#'     "table(selected_table) | take 100"
#'   ),
#'   parameters = list(selected_table = table)
#' )
#' }
fabric_kql_query <- function(
  cluster,
  query,
  database = NULL,
  parameters = list(),
  request_properties = list(),
  timeout = 60,
  retain_raw_frames = FALSE,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  token = NULL,
  auth_args = list(),
  allow_custom_endpoint = FALSE
) {
  # 1 Validate and normalize inputs ----------------------------------------------------------------

  # Resolve the endpoint and encode R parameter values before authenticating

  if (
    !is.character(query) ||
      length(query) != 1L ||
      is.na(query) ||
      !nzchar(trimws(query))
  ) {
    .fabric_abort("query must be one non-empty character value")
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
    .fabric_abort("timeout must be one positive number of seconds")
  }
  if (
    !is.logical(retain_raw_frames) ||
      length(retain_raw_frames) != 1L ||
      is.na(retain_raw_frames)
  ) {
    .fabric_abort("retain_raw_frames must be TRUE or FALSE")
  }

  # 2 Execute and return the query -----------------------------------------------------------------

  # Execute and return the query only after the request inputs are ready

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
    credential = credential,
    retain_raw_frames = retain_raw_frames
  )
}

# Resolve a discovery record or service URL plus `database`. Returns a trusted
# v2 query endpoint and database name for `fabric_kql_query()`
kusto_resolve_target <- function(
  cluster,
  database = NULL,
  allow_custom_endpoint = FALSE
) {
  # 1 Read and validate target inputs --------------------------------------------------------------

  # A discovered KQL database supplies both its query URL and display name

  if (
    !is.logical(allow_custom_endpoint) ||
      length(allow_custom_endpoint) != 1L ||
      is.na(allow_custom_endpoint)
  ) {
    .fabric_abort("allow_custom_endpoint must be TRUE or FALSE")
  }
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
    .fabric_abort(
      "cluster must supply one non-empty Kusto query-service URI"
    )
  }

  if (
    !is.character(database) ||
      length(database) != 1L ||
      is.na(database) ||
      !nzchar(trimws(database))
  ) {
    .fabric_abort(
      "database is required unless cluster is a discovered KQLDatabase item"
    )
  }

  # 2 Validate the endpoint trust boundary ---------------------------------------------------------

  # Check the endpoint trust boundary now so later code can rely on safe input

  # Parse the endpoint once and reject URL parts that could redirect a token
  cluster <- sub("/+$", "", trimws(cluster))
  parsed <- try(httr2::url_parse(cluster), silent = TRUE)
  if (
    inherits(parsed, "try-error") ||
      !identical(parsed$scheme, "https") ||
      is.null(parsed$hostname) ||
      !nzchar(parsed$hostname) ||
      nzchar(parsed$username %||% "") ||
      nzchar(parsed$password %||% "") ||
      !(parsed$port %||% "") %in% c("", "443") ||
      length(parsed$query %||% list()) > 0L ||
      nzchar(parsed$fragment %||% "")
  ) {
    .fabric_abort(paste0(
      "cluster must be a valid HTTPS query-service URI using the ",
      "default port (443)"
    ))
  }

  # Known Microsoft suffixes are trusted without an explicit override
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
    .fabric_abort(paste0(
      "cluster must use a Microsoft Kusto endpoint; set ",
      "allow_custom_endpoint = TRUE only for a trusted custom origin"
    ))
  }

  # Accept either the service root or a complete REST query route
  path <- parsed$path %||% ""
  if (
    !path %in% c("", "/") &&
      !grepl("/v[12]/rest/query/?$", path, ignore.case = TRUE)
  ) {
    .fabric_abort(
      "cluster URI must be a service root or a Kusto REST query endpoint"
    )
  }

  # 3 Normalize the REST route ---------------------------------------------------------------------

  # Normalize the REST route so later branches do not repeat the same conversion

  url <- if (grepl("/v2/rest/query$", cluster, ignore.case = TRUE)) {
    cluster
  } else if (grepl("/v1/rest/query$", cluster, ignore.case = TRUE)) {
    sub("/v1/rest/query$", "/v2/rest/query", cluster, ignore.case = TRUE)
  } else {
    paste0(cluster, "/v2/rest/query")
  }
  list(url = url, database = trimws(database))
}

# Validate `value` as a uniquely named list identified by `name`. Returns a list
# used for KQL parameters and request properties
kusto_named_list <- function(value, name) {
  if (is.null(value)) {
    return(list())
  }

  if (!is.list(value)) {
    .fabric_abort(cli::format_inline("{name} must be a named list"))
  }

  if (
    length(value) &&
      (is.null(names(value)) ||
        anyNA(names(value)) ||
        !all(nzchar(names(value))) ||
        anyDuplicated(names(value)))
  ) {
    .fabric_abort(
      cli::format_inline("{name} must have unique, non-empty names")
    )
  }
  value
}

# Reject Fabric-incompatible `request_properties`. Returns invisibly before the
# remaining properties are encoded into the Kusto request
kusto_validate_request_properties <- function(request_properties) {
  unsupported <- c(
    "queryconsistency",
    "query_weakconsistency_session_id"
  )
  supplied <- names(request_properties)
  matches <- supplied[tolower(supplied) %in% unsupported]
  if (length(matches)) {
    .fabric_abort(paste0(
      "Microsoft Fabric does not support request_properties: ",
      paste(matches, collapse = ", ")
    ))
  }
  invisible(request_properties)
}

# Validate parameter names and encode their values. Returns a named list of KQL
# literal text placed in Kusto request properties
kusto_encode_parameters <- function(parameters) {
  parameters <- kusto_named_list(parameters, "parameters")
  if (
    length(parameters) &&
      !all(grepl("^[A-Za-z_][A-Za-z0-9_]*$", names(parameters)))
  ) {
    .fabric_abort("parameters names must be valid KQL identifiers")
  }
  lapply(parameters, kusto_encode_parameter)
}

# Convert one supported R `value` into a KQL literal. Returns text used in the
# request's parameter map without interpolating values into query source
kusto_encode_parameter <- function(value) {
  # 1 Handle special and missing values ------------------------------------------------------------

  # Handle special and missing values separately so the common path stays simple

  if (is.numeric(value) && length(value) == 1L && is.nan(value)) {
    return("real(nan)")
  }

  if (!is.null(value) && !length(value)) {
    empty <- if (is.list(value) && !is.null(names(value))) "{}" else "[]"
    return(paste0("dynamic(", empty, ")"))
  }

  if (is.null(value) || anyNA(value)) {
    .fabric_abort(
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
    .fabric_abort(
      "Date, time, difftime, and integer64 KQL parameters must be scalar"
    )
  }

  # 2 Encode typed scalar values -------------------------------------------------------------------

  # Encode dates, times, and other supported scalars using Kusto literal syntax

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
      .fabric_abort("KQL difftime parameter values must be finite")
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

  # 3 Encode vector and object values --------------------------------------------------------------

  # Serialize vectors and objects as JSON-backed dynamic Kusto values

  json <- jsonlite::toJSON(
    value,
    auto_unbox = TRUE,
    null = "null",
    digits = NA
  )
  paste0("dynamic(", json, ")")
}

# Return a unique Kusto client request ID without inputs. The request sender uses
# the closure's counter to distinguish queries created in the same process
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

# Build and send one Kusto request, then parse its frames. Returns a tibble or
# table list used directly by `fabric_kql_query()`
kusto_execute_query <- function(
  url,
  database,
  query,
  parameters,
  request_properties,
  timeout,
  credential,
  retain_raw_frames = FALSE
) {
  # 1 Build request metadata -----------------------------------------------------------------------

  # A unique client request ID makes service-side troubleshooting possible

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

  # 2 Send the query -------------------------------------------------------------------------------

  # Send the query only after the request inputs are ready

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

  # 3 Parse and return response frames -------------------------------------------------------------

  # Decode the response into frames before validating their order and content

  frames <- httr2::resp_body_json(
    resp,
    simplifyVector = FALSE,
    bigint_as_char = TRUE
  )
  response_headers <- httr2::resp_headers(resp)
  kusto_parse_response(
    frames,
    retain_raw_frames = retain_raw_frames,
    metadata = list(
      client_request_id = client_request_id,
      request_id = httr2::resp_header(resp, "x-ms-request-id"),
      activity_id = httr2::resp_header(resp, "x-ms-activity-id") %||%
        httr2::resp_header(resp, "x-ms-root-activity-id"),
      response_headers = response_headers
    )
  )
}

# Identify one Kusto response `frame`. Returns its explicit or inferred type so
# the response parser can validate frame order and contents
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

# Validate and combine Kusto v2 `frames`. Returns primary result data with raw
# frames, auxiliary tables, completion state, and request metadata attached
kusto_parse_response <- function(
  frames,
  metadata = list(),
  retain_raw_frames = FALSE
) {
  # 1 Validate the response envelope ---------------------------------------------------------------

  # A v2 response must start with one header and end with one completion frame

  if (!is.list(frames) || !length(frames)) {
    .fabric_abort("Kusto returned a malformed v2 response")
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

  # 2 Combine table frames -------------------------------------------------------------------------

  # Progressive responses split schema, rows, progress, and completion across
  # several frames; rebuild each table in arrival order

  tables <- list()
  table_order <- character()
  progressive_tables <- character()
  completed <- character()

  table_indexes <- if (length(frames) > 2L) {
    seq.int(2L, length(frames) - 1L)
  } else {
    integer()
  }

  # Each frame either creates, changes, reports on, or completes one table
  for (index in table_indexes) {
    frame <- frames[[index]]
    type <- frame_types[[index]]

    if (identical(type, "DataTable")) {
      # Complete tables carry their schema and rows in one frame
      kusto_validate_table_frame(frame, "DataTable", require_rows = TRUE)
      key <- as.character(frame$TableId)

      if (key %in% table_order) {
        kusto_abort_malformed(paste0("duplicate table ID ", key))
      }

      tables[[key]] <- frame
      table_order <- c(table_order, key)
    } else if (identical(type, "TableHeader")) {
      # Progressive headers create an empty table that later fragments fill
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
      # Fragments append new rows or replace the rows collected so far
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
      # Progress frames do not add data but must still reference a valid table
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
      # Completion confirms that every announced row reached the table
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

  # Every progressive header must eventually receive a completion frame
  incomplete <- setdiff(progressive_tables, completed)
  if (length(incomplete)) {
    kusto_abort_malformed(paste0(
      "missing TableCompletion for table ID(s): ",
      paste(incomplete, collapse = ", ")
    ))
  }
  completion <- frames[[length(frames)]]

  # 3 Convert result tables ------------------------------------------------------------------------

  # Keep primary results as return data and auxiliary tables as metadata

  table_order <- unique(table_order)
  ordered_tables <- tables[table_order]
  is_primary <- vapply(
    ordered_tables,
    function(table) {
      identical(
        tolower(table$TableKind %||% ""),
        "primaryresult"
      )
    },
    logical(1)
  )
  primary <- ordered_tables[is_primary]
  auxiliary <- ordered_tables[!is_primary]
  auxiliary_results <- lapply(auxiliary, kusto_parse_table)
  auxiliary_names <- vapply(
    auxiliary,
    function(table) table$TableName %||% table$TableKind %||% "Auxiliary",
    character(1)
  )
  names(auxiliary_results) <- make.unique(auxiliary_names, sep = "_")

  results <- lapply(primary, kusto_parse_table)
  result_names <- vapply(
    primary,
    function(table) table$TableName %||% "PrimaryResult",
    character(1)
  )
  names(results) <- make.unique(result_names, sep = "_")
  result <- if (!length(results)) {
    tibble::tibble()
  } else if (length(results) == 1L) {
    results[[1L]]
  } else {
    structure(results, class = c("fabric_kql_tables", "list"))
  }

  # 4 Attach metadata and check completion ---------------------------------------------------------

  # Attach metadata and check completion before returning the completed result

  result <- kusto_attach_metadata(
    result,
    if (isTRUE(retain_raw_frames)) frames else NULL,
    completion,
    auxiliary_results,
    metadata
  )
  kusto_check_completion(completion, result, frames, metadata)
  result
}

# Attach request, completion, raw-frame, and auxiliary-table metadata to
# `result`. Returns the same result shape for the public query function
kusto_attach_metadata <- function(
  result,
  frames,
  completion,
  auxiliary,
  metadata
) {
  attr(result, "kusto_auxiliary_tables") <- auxiliary
  attr(result, "kusto_completion") <- completion
  attr(result, "kusto_raw_frames") <- frames
  attr(result, "kusto_client_request_id") <- metadata$client_request_id %||%
    NULL
  attr(result, "kusto_request_id") <- metadata$request_id %||% NULL
  attr(result, "kusto_activity_id") <- metadata$activity_id %||% NULL
  attr(result, "kusto_response_headers") <- metadata$response_headers %||%
    list()
  result
}

# Raise a protocol error with beginner-readable `detail`. This function does
# not return and is shared by all Kusto frame validators
kusto_abort_malformed <- function(detail) {
  .fabric_abort(
    paste0("Kusto returned a malformed v2 response: ", detail),
    class = "fabric_kql_protocol_error"
  )
}

# Read one required string `field` from `frame`. Returns the string or reports a
# malformed frame with its `type`
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

# Read one required logical `field` from `frame`. Returns the flag or reports a
# malformed frame with its `type`
kusto_require_flag <- function(frame, field, type) {
  value <- frame[[field]]
  if (!is.logical(value) || length(value) != 1L || is.na(value)) {
    kusto_abort_malformed(paste(type, field, "must be true or false"))
  }
  value
}

# Read one non-negative integer `field` from `frame`. Returns an R integer used
# for table IDs, row counts, and field counts
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

# Validate the schema and optional rows in a table `frame`. Returns invisibly
# before the response parser stores that frame
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

# Check every row against `field_count`. Returns invisibly and prevents shifted
# or truncated Kusto columns from reaching conversion
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

# Resolve the target table for a progressive `frame`. Returns its key after
# checking that the table exists and is not already complete
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

# Check the final completion frame and raise typed partial-result errors
# Returns invisibly only when the query completed successfully
kusto_check_completion <- function(
  completion,
  partial_data = NULL,
  raw_frames = NULL,
  metadata = list()
) {
  kusto_require_flag(completion, "HasErrors", "DataSetCompletion")
  kusto_require_flag(completion, "Cancelled", "DataSetCompletion")
  if (isTRUE(completion$Cancelled)) {
    .fabric_abort(
      "Kusto query was cancelled before completion",
      class = "fabric_kql_partial_error",
      partial_data = partial_data,
      raw_frames = raw_frames,
      completion = completion,
      request_id = metadata$request_id %||% NULL,
      activity_id = metadata$activity_id %||% NULL
    )
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
  .fabric_abort(
    paste0(
      "Kusto query failed after HTTP success: ",
      paste(detail, collapse = ": ")
    ),
    class = "fabric_kql_partial_error",
    partial_data = partial_data,
    raw_frames = raw_frames,
    completion = completion,
    request_id = metadata$request_id %||% NULL,
    activity_id = metadata$activity_id %||% NULL
  )
}

# Convert one validated Kusto `table` into a tibble. Returns typed columns plus
# schema, table-name, and table-ID attributes
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

# Convert nullable `values` to character. Returns an atomic vector used directly
# or as safe input to stricter Kusto type parsers
kusto_character_column <- function(values) {
  vapply(
    values,
    function(value) {
      if (is.null(value)) NA_character_ else as.character(value)
    },
    character(1)
  )
}

# Convert Kusto numeric `values`, including infinity spellings. Returns a double
# vector or raises a protocol error for invalid declared values
kusto_numeric_column <- function(values) {
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
  invalid <- !is.na(text) & is.na(out) & is.na(matched)
  if (any(invalid)) {
    .fabric_abort(
      "Kusto returned an invalid numeric value for its declared type"
    )
  }
  out
}

# Convert Kusto integer `values` for `type`. Returns integer/integer64 values,
# or character when R's missing-value sentinel would hide a boundary value
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
    .fabric_abort(
      "Kusto returned an invalid integer value for its declared type"
    )
  }

  if (contains_minimum) {
    .fabric_warn(
      c(
        "KQL {.code {type}} column contains {.val {minimum}}",
        "i" = paste(
          "R reserves this value for missing data; returning it as",
          "character"
        )
      ),
      .format = TRUE,
      class = "fabric_kql_integer_boundary_warning"
    )

    return(text)
  }
  parsed
}

# Parse nullable Kusto datetime `values`. Returns a UTC POSIXct vector used in
# result tibbles
kusto_datetime_column <- function(values) {
  text <- kusto_character_column(values)
  clean <- sub("Z$", "", text, ignore.case = TRUE)
  out <- as.POSIXct(
    strptime(clean, format = "%Y-%m-%dT%H:%M:%OS", tz = "UTC"),
    tz = "UTC"
  )
  invalid <- !is.na(text) & is.na(out)
  if (any(invalid)) {
    .fabric_abort("Kusto returned an invalid datetime value")
  }
  out
}

# Parse one Kusto timespan `value`. Returns seconds for construction of an R
# `difftime` result column
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
    .fabric_abort("Kusto returned an invalid timespan value")
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

# Decode one dynamic JSON `value` when possible. Returns structured R data, the
# original scalar, or `NULL` for a Kusto null
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

# Dispatch nullable `values` to the parser for declared Kusto `type`. Returns
# one R vector or list-column for `kusto_parse_table()`
kusto_convert_column <- function(values, type) {
  # 1 Convert scalar service types -----------------------------------------------------------------

  # Convert each scalar Kusto type to its closest stable R representation

  if (type %in% c("string", "guid", "uuid", "uniqueid")) {
    return(kusto_character_column(values))
  }

  if (type %in% c("bool", "boolean")) {
    return(vapply(
      values,
      function(value) {
        if (is.null(value)) {
          return(NA)
        }

        if (
          !is.logical(value) ||
            length(value) != 1L ||
            is.na(value)
        ) {
          .fabric_abort(
            "Kusto returned an invalid Boolean value",
            class = "fabric_kql_protocol_error"
          )
        }
        value
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

  if (type == "decimal") {
    return(kusto_character_column(values))
  }

  if (type %in% c("real", "double")) {
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

  # 2 Convert structured values or fall back to text -----------------------------------------------

  # Keep dynamic values structured and use readable text for unknown types

  if (type == "dynamic") {
    return(lapply(values, kusto_dynamic_value))
  }
  kusto_character_column(values)
}
