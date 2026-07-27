#' @title
#' Query a Microsoft Fabric/Power BI semantic model with DAX
#'
#' @description
#' Runs a Data Analysis Expressions (DAX) query against a published semantic
#' model and returns its result as a tibble. A semantic model is the
#' report-ready layer behind Power BI reports: it contains tables,
#' relationships, measures, and business calculations. Use DAX here, rather
#' than SQL intended for the underlying Lakehouse or Warehouse.
#'
#' @details
#' - The easiest input is an item from [fabric_semantic_models()]. You can instead
#'   supply `workspace_id` and `dataset_id` (both GUIDs), or a Power BI
#'   connection string containing the workspace and semantic-model names. IDs
#'   avoid name lookup and are best for scheduled code.
#' - In Fabric/Power BI, open the semantic model's settings to find its server
#'   or XMLA connection information. The signed-in identity needs Read and Build
#'   permission on the semantic model, either through its workspace role or
#'   through **Manage permissions** on the model.
#' - \pkg{AzureAuth} is used to acquire the token. Be wary of
#'  caching behavior; you may want to call [AzureAuth::clean_token_directory()]
#'  to clear cached tokens if the wrong account or tenant is being reused.
#' - Requests use the Power BI audience
#'  `https://analysis.windows.net/powerbi/api/.default` and require
#'  `Dataset.Read.All` (or `Dataset.ReadWrite.All`) plus dataset Read and Build
#'  permissions. Name lookup also requires `Workspace.Read.All` or equivalent.
#' - The Power BI tenant setting **Dataset Execute Queries REST API** must be
#'  enabled. Service-principal authentication also requires **Allow service
#'  principals to use Power BI APIs**. Service principals are not supported for
#'  semantic models with row-level security or single sign-on enabled.
#' - Set `api = "json"` (the default) for the established `executeQueries`
#'  endpoint. It accepts one DAX query and one result table per request.
#'  Results are limited to 100,000 rows or 1,000,000 values (whichever is
#'  reached first), 15 MB, and 120 requests per minute per user. Partial
#'  results reported by Power BI are treated as errors by this function.
#' - Set `api = "arrow"` for the newer `executeDaxQueries` endpoint. It
#'  preserves Arrow column types, raises errors carried in HTTP 200 Arrow error
#'  rowsets, and supports the additional documented request properties through
#'  `arrow_options`. The optional \pkg{arrow} package is required because Power
#'  BI compresses record batches with LZ4. This API requires a Premium, Fabric,
#'  or Embedded capacity and does not support deprecated Push semantic models.
#'  The Power BI tenant settings **Dataset Execute Queries REST API** and
#'  **Allow XMLA endpoints and Analyze in Excel with on-premises semantic
#'  models** must both be enabled.
#' - Arrow queries may contain multiple `EVALUATE` statements, but this helper
#'  retains its single-table return contract and errors when Power BI returns
#'  multiple data rowsets.
#'
#' @param connstr Optional Power BI connection string or one
#'   SemanticModel record returned by [fabric_semantic_models()] or
#'   [fabric_item()]. For a discovered record, workspace and dataset IDs are
#'   used directly. A character connection string can be, for example,
#'   `"Data Source=powerbi://api.powerbi.com/v1.0/myorg/Workspace;Initial Catalog=Dataset;"`.
#'   It may contain `Data Source=` and `Initial Catalog=` parts, or a bare
#'   `powerbi://...` source plus a `Dataset=`, `Catalog=`, or
#'   `Initial Catalog=` key. Omit it when `dataset_id` is supplied.
#' @param workspace_id Optional workspace GUID. Use with `dataset_id` to avoid
#'   name-based discovery. If omitted with `dataset_id`, the unscoped dataset
#'   endpoint is used.
#' @param dataset_id Optional semantic model/dataset GUID. When supplied, no
#'   connection-string name lookup is performed.
#' @param dax One DAX query, normally beginning with `EVALUATE`. DAX table
#'   expressions determine which rows and columns are returned.
#' @param tenant_id Microsoft Entra tenant ID. Defaults to
#'   `FABRICQUERYR_TENANT_ID`.
#' @param client_id Microsoft Entra application/client ID. Defaults to
#'   `FABRICQUERYR_CLIENT_ID`, then the Azure CLI application ID.
#' @param include_nulls Logical. With `TRUE`, Power BI includes properties whose
#'   value is blank/null. With `FALSE`, those properties can be absent from a
#'   returned row; retaining `TRUE` usually gives a more consistent tibble.
#'   Used only by `api = "json"`; Arrow has a schema and always represents
#'   nulls explicitly.
#' @param api_base Power BI REST API base URL. The default
#'   `"https://api.powerbi.com/v1.0/myorg"` is correct for the commercial cloud;
#'   override it only for a test service that implements the same endpoint and
#'   authentication contract. Sovereign Microsoft clouds are not currently
#'   supported by this helper.
#' @param token Optional `AzureAuth::AzureToken`, bearer-token string, or
#'   token-provider function. With `NULL`, `AzureAuth` reuses a matching cached
#'   token or starts its normal interactive login flow.
#' @param auth_args Named list of additional arguments passed to
#'   [AzureAuth::get_azure_token()] when no token source is supplied.
#' @param impersonated_user Optional user principal name, such as
#'   `"analyst@example.com"`, sent as `impersonatedUserName` for supported
#'   JSON row-level-security scenarios or as `effectiveUsername` for Arrow.
#'   Leave `NULL` for the normal identity context.
#' @param api Response API. `"json"` uses `executeQueries`; `"arrow"` uses
#'   `executeDaxQueries`.
#' @param result Return format. `"tibble"` collects the result in R memory.
#'   With `api = "arrow"`, `"arrow_stream"` returns a
#'   `nanoarrow_array_stream` compatible with
#'   [arrow::as_record_batch_reader()] and other Arrow C stream consumers. The
#'   HTTP response is streamed to a temporary file before Arrow reads it, but
#'   the returned Arrow table is materialized in memory so that concatenated
#'   data, error, and metrics rowsets can be validated.
#' @param arrow_options Named list of optional `executeDaxQueries` request
#'   properties. Supported names are `applicationContext`, `culture`,
#'   `customData`, `effectiveUsername`, `executionMetrics`, `memoryLimit`,
#'   `queryTimeout`, `resultSetRowCountLimit`, `roles`, and `schemaOnly`. The
#'   required `query` property is supplied from `dax`. Used only by
#'   `api = "arrow"`.
#'
#' @return With `result = "tibble"`, a tibble containing the single result
#'   table. With `api = "arrow", result = "arrow_stream"`, a
#'   `nanoarrow_array_stream`. Power BI's column names are preserved. An empty
#'   result becomes a typed zero-row result; API errors, multiple rowsets, and
#'   partial/truncated JSON results raise an error rather than silently
#'   returning incomplete data. When `executionMetrics = TRUE`, the metrics
#'   rowset is attached to either result as an `execution_metrics` tibble
#'   attribute.
#' @references
#' [Power BI JSON Execute Queries REST API](https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/execute-queries-in-group)
#'
#' [Power BI Arrow Execute DAX Queries REST API](https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/execute-dax-queries-in-group)
#'
#' [Semantic model permissions](https://learn.microsoft.com/en-us/power-bi/connect-data/service-datasets-permissions)
#'
#' [Semantic Model Execute Queries tenant setting](https://learn.microsoft.com/en-us/fabric/admin/service-admin-portal-integration#semantic-model-execute-queries-rest-api)
#' @export
#'
#' @examples
#' # Example is not executed since it requires configured credentials for Fabric
#' \dontrun{
#' conn <- "Data Source=powerbi://api.powerbi.com/v1.0/myorg/My Workspace;Initial Catalog=SalesModel;"
#' df <- fabric_pbi_dax_query(
#'   connstr = conn,
#'   dax = "EVALUATE TOPN(1000, 'Customers')",
#'   tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
#'   client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID")
#' )
#' dplyr::glimpse(df)
#'
#' # Arrow IPC endpoint, returned through the Arrow C stream interface
#' stream <- fabric_pbi_dax_query(
#'   connstr = conn,
#'   dax = "EVALUATE TOPN(1000, 'Customers')",
#'   api = "arrow",
#'   result = "arrow_stream"
#' )
#' reader <- arrow::as_record_batch_reader(stream)
#' }
fabric_pbi_dax_query <- function(
  connstr = NULL,
  dax,
  workspace_id = NULL,
  dataset_id = NULL,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  token = NULL,
  auth_args = list(),
  include_nulls = TRUE,
  api_base = "https://api.powerbi.com/v1.0/myorg",
  impersonated_user = NULL,
  api = c("json", "arrow"),
  result = c("tibble", "arrow_stream"),
  arrow_options = list()
) {
  api <- match.arg(api)
  result <- match.arg(result)
  if (!is.character(dax) || length(dax) != 1L || is.na(dax) || !nzchar(dax)) {
    rlang::abort("dax must be one non-empty string")
  }
  if (
    !is.logical(include_nulls) ||
      length(include_nulls) != 1L ||
      is.na(include_nulls)
  ) {
    rlang::abort("include_nulls must be TRUE or FALSE")
  }
  discovered <- fabric_as_record(connstr)
  if (!is.null(discovered)) {
    if (
      !identical(
        tolower(fabric_record_value(discovered, "type") %||% ""),
        "semanticmodel"
      )
    ) {
      rlang::abort(
        "connstr discovery record must be a SemanticModel item"
      )
    }
    workspace_id <- workspace_id %||%
      fabric_record_value(
        discovered,
        "workspaceId"
      )
    dataset_id <- dataset_id %||% fabric_record_value(discovered, "id")
    connstr <- fabric_record_value(
      discovered,
      "dax_connection_string"
    )
  }
  if (
    !is.null(connstr) &&
      (!is.character(connstr) ||
        length(connstr) != 1L ||
        is.na(connstr) ||
        !nzchar(connstr))
  ) {
    rlang::abort("connstr must be one non-empty string")
  }
  if (
    !is.null(workspace_id) &&
      (!is.character(workspace_id) ||
        length(workspace_id) != 1L ||
        is.na(workspace_id) ||
        !nzchar(workspace_id))
  ) {
    rlang::abort("workspace_id must be one non-empty string")
  }
  if (
    !is.null(dataset_id) &&
      (!is.character(dataset_id) ||
        length(dataset_id) != 1L ||
        is.na(dataset_id) ||
        !nzchar(dataset_id))
  ) {
    rlang::abort("dataset_id must be one non-empty string")
  }
  if (is.null(dataset_id) && is.null(connstr)) {
    rlang::abort(
      "Supply either connstr or dataset_id"
    )
  }
  if (
    !is.null(impersonated_user) &&
      (!is.character(impersonated_user) ||
        length(impersonated_user) != 1L ||
        is.na(impersonated_user) ||
        !nzchar(impersonated_user))
  ) {
    rlang::abort("impersonated_user must be one non-empty string")
  }
  arrow_options <- pbi_validate_arrow_options(arrow_options)
  if (identical(api, "json") && length(arrow_options)) {
    rlang::abort("arrow_options can only be used with api = \"arrow\"")
  }
  if (identical(api, "json") && !identical(result, "tibble")) {
    rlang::abort("result = \"arrow_stream\" requires api = \"arrow\"")
  }
  if (
    identical(api, "arrow") &&
      !is.null(impersonated_user) &&
      "effectiveUsername" %in% names(arrow_options)
  ) {
    rlang::abort(
      "Supply effective identity through either impersonated_user or arrow_options, not both"
    )
  }
  if (identical(api, "arrow")) {
    packages <- "arrow"
    if (identical(result, "arrow_stream")) {
      packages <- c(packages, "nanoarrow")
    }
    rlang::check_installed(
      packages,
      reason = "to consume Power BI Arrow DAX responses"
    )
  }

  credential <- fabric_credential(
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args
  )

  if (is.null(dataset_id)) {
    ids <- pbi_resolve_ids_from_connstr(
      connstr = connstr,
      credential = credential,
      api_base = api_base
    )
    workspace_id <- ids$group_id
    dataset_id <- ids$dataset_id
  }

  if (identical(api, "json")) {
    return(pbi_execute_dax(
      credential = credential,
      dataset_id = dataset_id,
      dax = dax,
      group_id = workspace_id,
      include_nulls = include_nulls,
      api_base = api_base,
      impersonated_user = impersonated_user
    ))
  }
  if (!is.null(impersonated_user)) {
    arrow_options$effectiveUsername <- impersonated_user
  }
  pbi_execute_dax_arrow(
    credential = credential,
    dataset_id = dataset_id,
    dax = dax,
    group_id = workspace_id,
    api_base = api_base,
    options = arrow_options,
    result = result
  )
}

#' Parse a Power BI connection string (XMLA) into components
#'
#' @param conn Character; a Power BI connection string.
#' @return A list with elements `server`, `workspace`, and `dataset`.
#' @keywords internal
#' @noRd
pbi_parse_connstr <- function(conn) {
  if (!is.character(conn) || length(conn) != 1L || is.na(conn)) {
    rlang::abort("conn must be one string")
  }
  toks <- strsplit(conn, ";", fixed = TRUE)[[1]]
  toks <- trimws(toks)
  toks <- toks[nzchar(toks)]

  # Data Source can be present as key=value or as a bare powerbi:// URL token
  ds <- sub(
    "(?i)^Data Source=",
    "",
    toks[grepl("(?i)^Data Source=", toks)],
    perl = TRUE
  )
  if (length(ds) == 0) {
    ds <- toks[grepl("(?i)^powerbi://", toks)]
  }
  if (length(ds) != 1) {
    rlang::abort(
      "Could not find a unique Data Source in connection string"
    )
  }
  ds <- trimws(ds[[1]])
  if (
    !grepl(
      "^powerbi://api\\.powerbi\\.com/v1\\.0/[^/]+/[^/]+/?$",
      ds,
      ignore.case = TRUE
    )
  ) {
    rlang::abort(
      paste0(
        "Data Source must be a Power BI XMLA workspace URL like ",
        "'powerbi://api.powerbi.com/v1.0/myorg/Workspace'"
      )
    )
  }

  # Dataset name can be specified using several synonyms
  catv <- sub(
    "(?i)^(Initial Catalog|Catalog|Database|Dataset)=",
    "",
    toks[grepl("(?i)^(Initial Catalog|Catalog|Database|Dataset)=", toks)],
    perl = TRUE
  )
  catv <- trimws(catv)
  if (length(catv) != 1L || !nzchar(catv[[1L]])) {
    rlang::abort(
      paste0(
        "Connection string must contain exactly one non-empty ",
        "Initial Catalog, Catalog, Database, or Dataset value"
      )
    )
  }
  dataset_name <- catv[[1L]]

  # Workspace is the last segment of the Data Source URL
  ds_clean <- sub("/+$", "", sub("(?i)^powerbi://", "", ds))
  segs <- strsplit(ds_clean, "/", fixed = TRUE)[[1]]
  workspace_name <- utils::URLdecode(utils::tail(segs, 1))
  if (!nzchar(workspace_name)) {
    rlang::abort(
      "Power BI Data Source does not contain a workspace name"
    )
  }

  list(server = ds, workspace = workspace_name, dataset = dataset_name)
}

#' Resolve workspace & dataset GUIDs using the Power BI REST API
#'
#' @param connstr Connection string used to infer workspace & dataset names.
#' @param credential Internal audience-aware credential.
#' @param api_base API base URL. Defaults to "https://api.powerbi.com/v1.0/myorg".
#' @return A list with `group_id`, `dataset_id`, `workspace`, and `dataset`.
#' @keywords internal
#' @noRd
pbi_resolve_ids_from_connstr <- function(
  connstr,
  credential,
  api_base = "https://api.powerbi.com/v1.0/myorg"
) {
  p <- pbi_parse_connstr(connstr)

  group_id <- pbi_get_group_id_by_name(
    credential = credential,
    workspace_name = p$workspace,
    api_base = api_base
  )
  dataset_id <- pbi_get_dataset_id_by_name(
    credential = credential,
    group_id = group_id,
    dataset_name = p$dataset,
    api_base = api_base
  )

  list(
    group_id = group_id,
    dataset_id = dataset_id,
    workspace = p$workspace,
    dataset = p$dataset
  )
}

#' Get a Power BI access token using AzureAuth
#'
#' @param tenant_id Azure AD tenant GUID.
#' @param client_id Azure AD application (client) ID.
#' @return A bearer access token string suitable for `Authorization: Bearer ...`.
#' @keywords internal
#' @noRd
pbi_get_token <- function(tenant_id, client_id) {
  fabric_get_token(
    fabric_credential(tenant_id = tenant_id, client_id = client_id),
    .fabric_audience$power_bi
  )
}

#' Execute a DAX query against a dataset
#'
#' @param credential Internal audience-aware credential.
#' @param dataset_id Dataset GUID.
#' @param dax DAX query.
#' @param group_id Optional workspace (group) GUID. If supplied, the request is made to the group-scoped endpoint.
#' @param include_nulls Logical; whether to include NULLs in response serialization.
#' @param api_base API base URL.
#' @param impersonated_user Optional impersonated user principal name.
#' @return A tibble.
#' @keywords internal
#' @noRd
pbi_execute_dax <- function(
  credential,
  dataset_id,
  dax,
  group_id = NULL,
  include_nulls = TRUE,
  api_base = "https://api.powerbi.com/v1.0/myorg",
  impersonated_user = NULL
) {
  path <- if (is.null(group_id)) {
    sprintf("%s/datasets/%s/executeQueries", api_base, dataset_id)
  } else {
    sprintf(
      "%s/groups/%s/datasets/%s/executeQueries",
      api_base,
      group_id,
      dataset_id
    )
  }

  body <- list(
    queries = list(list(query = dax)),
    serializerSettings = list(includeNulls = include_nulls)
  )
  if (!is.null(impersonated_user)) {
    body$impersonatedUserName <- impersonated_user
  }

  req <- httr2::request(path) |>
    httr2::req_body_json(body)

  out <- .httr2_json(
    req,
    simplifyVector = FALSE,
    credential = credential,
    audience = .fabric_audience$power_bi,
    idempotent = TRUE
  )
  pbi_parse_dax_response(out)
}

#' Validate optional Execute DAX Queries Arrow request properties
#' @keywords internal
#' @noRd
pbi_validate_arrow_options <- function(options) {
  if (!is.list(options) || is.data.frame(options)) {
    rlang::abort("arrow_options must be a named list")
  }
  if (!length(options)) {
    return(list())
  }
  option_names <- names(options)
  if (
    is.null(option_names) ||
      anyNA(option_names) ||
      !all(nzchar(option_names)) ||
      anyDuplicated(option_names)
  ) {
    rlang::abort("arrow_options must have unique, non-empty names")
  }
  supported <- c(
    "applicationContext",
    "culture",
    "customData",
    "effectiveUsername",
    "executionMetrics",
    "memoryLimit",
    "queryTimeout",
    "resultSetRowCountLimit",
    "roles",
    "schemaOnly"
  )
  unknown <- setdiff(option_names, supported)
  if (length(unknown)) {
    rlang::abort(paste0(
      "Unsupported arrow_options name(s): ",
      paste(unknown, collapse = ", ")
    ))
  }
  options <- Filter(Negate(is.null), options)
  string_options <- intersect(
    names(options),
    c(
      "applicationContext",
      "culture",
      "customData",
      "effectiveUsername"
    )
  )
  for (name in string_options) {
    value <- options[[name]]
    if (
      !is.character(value) ||
        length(value) != 1L ||
        is.na(value) ||
        !nzchar(value)
    ) {
      rlang::abort(paste0(
        "arrow_options$",
        name,
        " must be one non-empty string"
      ))
    }
  }
  integer_options <- intersect(
    names(options),
    c("memoryLimit", "queryTimeout", "resultSetRowCountLimit")
  )
  for (name in integer_options) {
    value <- options[[name]]
    if (
      !is.numeric(value) ||
        length(value) != 1L ||
        is.na(value) ||
        !is.finite(value) ||
        value <= 0 ||
        value != floor(value)
    ) {
      rlang::abort(paste0(
        "arrow_options$",
        name,
        " must be one positive integer"
      ))
    }
  }
  if ("roles" %in% names(options)) {
    roles <- options$roles
    if (
      !is.character(roles) ||
        !length(roles) ||
        anyNA(roles) ||
        !all(nzchar(roles))
    ) {
      rlang::abort(
        "arrow_options$roles must be a non-empty character vector"
      )
    }
  }
  logical_options <- intersect(
    names(options),
    c("executionMetrics", "schemaOnly")
  )
  for (name in logical_options) {
    value <- options[[name]]
    if (
      !is.logical(value) ||
        length(value) != 1L ||
        is.na(value)
    ) {
      rlang::abort(paste0(
        "arrow_options$",
        name,
        " must be TRUE or FALSE"
      ))
    }
  }
  options
}

#' Execute a DAX query through the Arrow IPC endpoint
#' @keywords internal
#' @noRd
pbi_execute_dax_arrow <- function(
  credential,
  dataset_id,
  dax,
  group_id = NULL,
  api_base = "https://api.powerbi.com/v1.0/myorg",
  options = list(),
  result = c("tibble", "arrow_stream")
) {
  result <- match.arg(result)
  path <- if (is.null(group_id)) {
    sprintf("%s/datasets/%s/executeDaxQueries", api_base, dataset_id)
  } else {
    sprintf(
      "%s/groups/%s/datasets/%s/executeDaxQueries",
      api_base,
      group_id,
      dataset_id
    )
  }
  body <- c(list(query = dax), options)
  req <- httr2::request(path) |>
    httr2::req_headers(
      Accept = "application/vnd.apache.arrow.stream"
    ) |>
    httr2::req_body_json(body)
  payload <- tempfile("fabricqueryr-dax-", fileext = ".arrow")
  on.exit(unlink(payload, force = TRUE), add = TRUE)
  .httr2_perform(
    req,
    credential = credential,
    audience = .fabric_audience$power_bi,
    idempotent = TRUE,
    download_path = payload
  )
  pbi_parse_dax_arrow_response(payload, result = result)
}

#' Parse concatenated Execute DAX Queries Arrow IPC streams
#' @keywords internal
#' @noRd
pbi_parse_dax_arrow_response <- function(
  payload,
  result = c("tibble", "arrow_stream")
) {
  result <- match.arg(result)
  path_payload <- is.character(payload) &&
    length(payload) == 1L &&
    !is.na(payload) &&
    file.exists(payload)
  if ((!is.raw(payload) && !path_payload) || !length(payload)) {
    rlang::abort("Power BI returned an empty Arrow DAX response")
  }
  if (path_payload && file.info(payload)$size == 0) {
    rlang::abort("Power BI returned an empty Arrow DAX response")
  }
  buffer <- if (path_payload) {
    arrow::mmap_open(payload)
  } else {
    arrow::BufferReader$create(payload)
  }
  if (path_payload) {
    on.exit(buffer$close(), add = TRUE)
  }
  size <- if (path_payload) file.info(payload)$size else length(payload)
  data_tables <- list()
  metrics_tables <- list()
  while (buffer$tell() < size) {
    position <- buffer$tell()
    reader <- tryCatch(
      arrow::RecordBatchStreamReader$create(buffer),
      error = function(error) {
        rlang::abort(
          "Power BI returned an invalid Arrow DAX response",
          parent = error
        )
      }
    )
    schema <- reader$schema
    table <- tryCatch(
      reader$read_table(),
      error = function(error) {
        rlang::abort(
          "Could not decompress or read the Power BI Arrow DAX response",
          parent = error
        )
      }
    )
    table <- pbi_decode_dax_arrow_dictionaries(table)
    metadata <- schema$metadata %||% list()
    if (identical(tolower(metadata[["IsError"]] %||% "false"), "true")) {
      pbi_abort_dax_arrow_error(metadata, table)
    }
    if (
      identical(
        tolower(metadata[["IsExecMetrics"]] %||% "false"),
        "true"
      )
    ) {
      metrics_tables[[length(metrics_tables) + 1L]] <- table
    } else {
      data_tables[[length(data_tables) + 1L]] <- table
    }
    if (buffer$tell() <= position) {
      rlang::abort("Power BI returned a non-advancing Arrow DAX stream")
    }
  }
  if (!length(data_tables)) {
    rlang::abort("Power BI returned no Arrow DAX data rowset")
  }
  if (length(data_tables) != 1L) {
    rlang::abort(sprintf(
      "Power BI returned %d Arrow DAX data rowsets; exactly one is supported",
      length(data_tables)
    ))
  }
  if (length(metrics_tables) > 1L) {
    rlang::abort(sprintf(
      "Power BI returned %d Arrow DAX execution-metrics rowsets; at most one is supported",
      length(metrics_tables)
    ))
  }
  table <- data_tables[[1L]]
  metrics <- if (length(metrics_tables)) {
    tibble::as_tibble(as.data.frame(metrics_tables[[1L]]))
  } else {
    NULL
  }
  if (identical(result, "arrow_stream")) {
    value <- nanoarrow::as_nanoarrow_array_stream(table)
  } else {
    value <- tibble::as_tibble(as.data.frame(table))
  }
  if (!is.null(metrics)) {
    attr(value, "execution_metrics") <- metrics
  }
  value
}

#' Decode dictionary columns before exposing Arrow DAX results to R
#' @keywords internal
#' @noRd
pbi_decode_dax_arrow_dictionaries <- function(table) {
  fields <- table$schema$fields
  dictionary <- vapply(
    fields,
    function(field) inherits(field$type, "DictionaryType"),
    logical(1)
  )
  if (!any(dictionary)) {
    return(table)
  }
  fields[dictionary] <- lapply(fields[dictionary], function(field) {
    arrow::field(
      field$name,
      field$type$value_type,
      nullable = field$nullable,
      metadata = field$metadata
    )
  })
  target <- do.call(arrow::schema, fields)
  if (length(table$schema$metadata)) {
    target <- target$WithMetadata(table$schema$metadata)
  }
  table$cast(target)
}

#' Raise an actionable HTTP 200 Arrow error-rowset response
#' @keywords internal
#' @noRd
pbi_abort_dax_arrow_error <- function(metadata, table) {
  fields <- tryCatch(
    as.data.frame(table),
    error = function(error) data.frame()
  )
  row_detail <- if (NROW(fields)) {
    values <- unlist(fields[1L, , drop = FALSE], use.names = TRUE)
    values <- values[!is.na(values) & nzchar(as.character(values))]
    paste(
      paste0(names(values), "=", as.character(values)),
      collapse = "; "
    )
  } else {
    ""
  }
  fault_code <- as.character(metadata[["FaultCode"]] %||% "")
  fault_string <- as.character(metadata[["FaultString"]] %||% "")
  summary <- paste0(
    if (nzchar(fault_code)) paste0("[", fault_code, "] ") else "",
    fault_string
  )
  detail <- paste(
    c(summary, row_detail)[nzchar(c(summary, row_detail))],
    collapse = ": "
  )
  if (!nzchar(detail)) {
    detail <- "unknown Arrow error rowset"
  }
  rlang::abort(
    paste0("Power BI Arrow DAX query failed: ", detail),
    class = "fabric_pbi_dax_error"
  )
}

#' Validate and parse an Execute Queries response
#' @param out Parsed JSON response.
#' @return A tibble.
#' @keywords internal
#' @noRd
pbi_parse_dax_response <- function(out) {
  pbi_check_dax_error(out$error, "response")

  results <- out$results
  if (is.null(results) || length(results) == 0L) {
    return(tibble::tibble())
  }
  for (result in results) {
    pbi_check_dax_error(result$error, "query result")
    for (table in result$tables %||% list()) {
      pbi_check_dax_error(table$error, "table result")
    }
  }
  if (length(results) != 1L) {
    rlang::abort(
      sprintf(
        "Power BI returned %d query results; exactly one is supported",
        length(results)
      )
    )
  }

  tables <- results[[1]]$tables
  if (is.null(tables) || length(tables) == 0L) {
    return(tibble::tibble())
  }
  if (length(tables) != 1L) {
    rlang::abort(
      sprintf(
        "Power BI returned %d result tables; exactly one is supported",
        length(tables)
      )
    )
  }

  rows <- tables[[1]]$rows
  if (is.null(rows) || length(rows) == 0L) {
    return(tibble::tibble())
  }

  # bind_rows preserves qualified and bracketed Power BI column names
  dplyr::bind_rows(rows)
}

#' Raise an actionable embedded Execute Queries error
#' @keywords internal
#' @noRd
pbi_check_dax_error <- function(error, level) {
  if (is.null(error) || !length(error)) {
    return(invisible())
  }
  flattened <- unlist(error, recursive = TRUE, use.names = TRUE)
  flattened <- as.character(flattened[!is.na(flattened) & nzchar(flattened)])
  detail <- if (length(flattened)) {
    paste(unique(flattened), collapse = ": ")
  } else {
    jsonlite::toJSON(error, auto_unbox = TRUE)
  }
  is_partial <- grepl(
    paste(
      "more than",
      "limit",
      "exceed",
      "truncat",
      "partial",
      "100[ ,]?000",
      "1[ ,]?000[ ,]?000",
      "15\\s*MB",
      sep = "|"
    ),
    detail,
    ignore.case = TRUE
  )
  if (is_partial) {
    rlang::abort(
      paste0(
        "Power BI returned an incomplete DAX ",
        level,
        ": ",
        detail,
        ". Reduce the selected rows/columns or page the query in DAX"
      )
    )
  }
  rlang::abort(
    paste0("Power BI DAX ", level, " failed: ", detail)
  )
}

#' Get a workspace (group) GUID by its name
#'
#' @param credential Internal audience-aware credential.
#' @param workspace_name Character; workspace display name (case-insensitive).
#' @param api_base API base URL.
#' @return Group GUID as a string.
#' @keywords internal
#' @noRd
pbi_get_group_id_by_name <- function(
  credential,
  workspace_name,
  api_base = "https://api.powerbi.com/v1.0/myorg"
) {
  url <- sprintf("%s/groups", api_base)
  vals <- pbi_get_collection(
    url,
    credential,
    offset_pagination = TRUE
  )
  hits <- vals[vapply(
    vals,
    function(g) tolower(g$name) == tolower(workspace_name),
    logical(1)
  )]
  if (length(hits) == 0) {
    rlang::abort(sprintf("Workspace '%s' not found", workspace_name))
  }
  if (length(hits) > 1L) {
    rlang::abort(
      sprintf(
        "Workspace name '%s' is ambiguous (%d case-insensitive matches). Use workspace_id",
        workspace_name,
        length(hits)
      )
    )
  }
  hits[[1]]$id
}

#' Get a dataset GUID by its name in a workspace
#'
#' @param credential Internal audience-aware credential.
#' @param group_id Workspace (group) GUID.
#' @param dataset_name Dataset display name (case-insensitive).
#' @param api_base API base URL.
#' @return Dataset GUID as a string.
#' @keywords internal
#' @noRd
pbi_get_dataset_id_by_name <- function(
  credential,
  group_id,
  dataset_name,
  api_base = "https://api.powerbi.com/v1.0/myorg"
) {
  url <- sprintf("%s/groups/%s/datasets", api_base, group_id)
  vals <- pbi_get_collection(url, credential)
  hits <- vals[vapply(
    vals,
    function(d) tolower(d$name) == tolower(dataset_name),
    logical(1)
  )]
  if (length(hits) == 0) {
    rlang::abort(sprintf("Dataset '%s' not found in workspace", dataset_name))
  }
  if (length(hits) > 1L) {
    rlang::abort(
      sprintf(
        "Dataset name '%s' is ambiguous in the workspace (%d case-insensitive matches). Use dataset_id",
        dataset_name,
        length(hits)
      )
    )
  }
  hits[[1]]$id
}

#' Read a complete Power BI collection
#' @param url Initial collection URL.
#' @param credential Internal audience-aware credential.
#' @param offset_pagination Whether to use documented `$top`/`$skip` paging.
#' @param page_size Page size for offset pagination.
#' @return A list containing every returned value.
#' @keywords internal
#' @noRd
pbi_get_collection <- function(
  url,
  credential,
  offset_pagination = FALSE,
  page_size = 5000L
) {
  if (is.character(credential)) {
    credential <- fabric_credential(token = credential)
  }
  .httr2_collection(
    url,
    credential = credential,
    audience = .fabric_audience$power_bi,
    offset_pagination = offset_pagination,
    page_size = page_size
  )
}
