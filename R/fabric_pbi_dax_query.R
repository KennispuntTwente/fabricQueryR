#' @title
#' Query a Microsoft Fabric/Power BI semantic model with DAX
#'
#' @description
#' Runs a DAX query against a published semantic model and returns the result as
#' a tibble. A semantic model is the report-ready data behind Power BI reports,
#' including tables, relationships, measures, and business calculations
#'
#' @section Choosing a model:
#' The easiest input is an item from [fabric_semantic_models()]. You can instead
#' supply workspace and dataset IDs, or a Power BI connection string copied from
#' the semantic model settings. IDs are the most reliable choice for scheduled
#' code. For a model in My Workspace, supply `dataset_id` and set
#' `my_workspace = TRUE`
#'
#' @section Choosing a response format:
#' Keep `api = "json"` for ordinary queries and broad compatibility. It returns
#' one result table and is available to Pro, PPU, and capacity-backed models
#' Results are limited by Power BI; fabricQueryR raises an error instead of
#' silently returning a partial result. Very large whole numbers are returned as
#' character values so they are not rounded
#'
#' Use `api = "arrow"` when exact semantic-model types matter, when a query has
#' several `EVALUATE` statements, or when you want an Arrow stream. It requires
#' the optional \pkg{arrow} package and a model on Premium or Fabric capacity.
#' Decimal128 and Decimal256 columns are returned as exact character values in
#' a tibble; `result = "arrow_stream"` retains their native Arrow decimal types.
#' The Power BI administrator must enable both **Dataset Execute Queries REST
#' API** under Developer settings and **Allow XMLA endpoints and Analyze in
#' Excel with on-premises semantic models** under Integration settings.
#' Multiple result tables are returned in statement order as a
#' `fabric_pbi_dax_rowsets` list
#'
#' @section Permissions and tenant settings:
#' The signed-in identity needs Read and Build permission on the semantic model
#' Your Power BI administrator must enable **Dataset Execute Queries REST API**;
#' service principals also need the relevant service-principal tenant setting.
#' The Arrow endpoint has the additional XMLA tenant setting and capacity
#' prerequisites described above
#' The APIs use the Power BI scope and require `Dataset.Read.All` (or
#' `Dataset.ReadWrite.All`). Name lookup also requires workspace read access
#' Row-level security, SSO, user impersonation, and the Arrow endpoint have
#' additional Power BI restrictions; see the linked Microsoft documentation
#'
#' @param connstr Optional semantic model record from
#'   [fabric_semantic_models()] or [fabric_item()], or a Power BI connection
#'   string. A character connection string can be, for example,
#'   `"Data Source=powerbi://api.powerbi.com/v1.0/myorg/Workspace;Initial Catalog=Dataset;"`
#'   It may contain `Data Source=` and `Initial Catalog=` parts, or a bare
#'   `powerbi://...` source plus a `Dataset=`, `Catalog=`, or
#'   `Initial Catalog=` key. Omit it when `dataset_id` is supplied
#' @param workspace_id Optional shared-workspace GUID. Use with `dataset_id` to
#'   avoid name-based discovery. For a model in My Workspace, omit this and set
#'   `my_workspace = TRUE` explicitly
#' @param dataset_id Optional semantic model/dataset GUID. When supplied, no
#'   connection-string name lookup is performed
#' @param my_workspace Whether `dataset_id` belongs to the signed-in user's My
#'   Workspace. Leave `FALSE` for shared workspaces
#' @param dax One DAX query, normally beginning with `EVALUATE`. DAX table
#'   expressions determine which rows and columns are returned
#' @param tenant_id Microsoft Entra tenant ID. Defaults to
#'   `FABRICQUERYR_TENANT_ID`
#' @param client_id Microsoft Entra application/client ID. Defaults to
#'   `FABRICQUERYR_CLIENT_ID`, then the Azure CLI application ID
#' @param include_nulls Logical. With `TRUE`, Power BI includes properties whose
#'   value is blank/null. With `FALSE`, those properties can be absent from a
#'   returned row; retaining `TRUE` usually gives a more consistent tibble
#'   Used only by `api = "json"`; Arrow has a schema and always represents
#'   nulls explicitly
#' @param api_base Power BI REST API base URL. The default
#'   `"https://api.powerbi.com/v1.0/myorg"` is correct for the commercial cloud;
#'   override it only for a test service that implements the same endpoint and
#'   authentication contract. Sovereign Microsoft clouds are not currently
#'   supported by this helper
#' @param token Optional access token or token-provider function. Leave `NULL`
#'   to let fabricQueryR use its normal sign-in flow
#' @param auth_args Additional sign-in options passed to
#'   [AzureAuth::get_azure_token()]
#' @param impersonated_user Optional user principal name, such as
#'   `"analyst@example.com"`, sent as `impersonatedUserName` for supported
#'   JSON row-level-security scenarios or as `effectiveUsername` for Arrow
#'   Leave `NULL` for the normal identity context
#' @param api Response format provided by Power BI. Use `"json"` for ordinary
#'   queries or `"arrow"` for richer types and multiple result tables
#' @param result Return a `"tibble"`, or with `api = "arrow"`, a single-use
#'   `"arrow_stream"` for batch processing without first collecting all rows in
#'   R memory
#' @param arrow_options Named list of optional `executeDaxQueries` request
#'   properties. Supported names are `applicationContext`, `culture`,
#'   `customData`, `effectiveUsername`, `executionMetrics`, `memoryLimit`,
#'   `queryTimeout`, `resultSetRowCountLimit`, `roles`, and `schemaOnly`. The
#'   required `query` property is supplied from `dax`. Used only by
#'   `api = "arrow"`
#'
#' @return A tibble for one result table. Multiple Arrow result tables are
#'   returned as a `fabric_pbi_dax_rowsets` list of tibbles or Arrow streams
#'   Power BI column names are preserved. Empty JSON results have no column
#'   metadata and therefore return a zero-row, zero-column tibble. Partial or
#'   truncated results raise an error
#' @references
#' [Power BI JSON Execute Queries REST API](https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/execute-queries-in-group)
#'
#' [Power BI Arrow Execute DAX Queries REST API](https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/execute-dax-queries-in-group)
#'
#' [Power BI Arrow API overview and capacity requirements](https://learn.microsoft.com/en-us/power-bi/developer/execute-dax-queries-arrow/overview)
#'
#' [Semantic model permissions](https://learn.microsoft.com/en-us/power-bi/connect-data/service-datasets-permissions)
#'
#' [Semantic Model Execute Queries tenant setting](https://learn.microsoft.com/en-us/fabric/admin/service-admin-portal-integration#semantic-model-execute-queries-rest-api)
#' @export
#'
#' @examples
#' \dontrun{
#' # Discover the semantic model instead of copying workspace and model IDs
#' workspace <- fabric_workspaces()[[1L]]
#' model <- fabric_semantic_models(workspace)[[1L]]
#'
#' # Supply a query tested in the model's DAX query view
#' dax <- Sys.getenv("FABRIC_DAX_QUERY")
#'
#' # Evaluate the DAX query and collect the result as a tibble
#' df <- fabric_pbi_dax_query(
#'   model,
#'   dax = dax
#' )
#' dplyr::glimpse(df)
#'
#' # Keep a larger result out of R memory with an Arrow stream
#' stream <- fabric_pbi_dax_query(
#'   model,
#'   dax = dax,
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
  my_workspace = FALSE,
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
  # 1 Validate query and target inputs -------------------------------------------------------------

  # Normalize the requested transport first, then resolve IDs from discovery
  # records or a copied connection string without allowing conflicting targets

  api <- match.arg(api)
  result <- match.arg(result)
  api_base <- pbi_api_base(api_base)

  # Check the query and simple flags before resolving a semantic model
  if (!is.character(dax) || length(dax) != 1L || is.na(dax) || !nzchar(dax)) {
    .fabric_abort("dax must be one non-empty string")
  }

  if (
    !is.logical(include_nulls) ||
      length(include_nulls) != 1L ||
      is.na(include_nulls)
  ) {
    .fabric_abort("include_nulls must be TRUE or FALSE")
  }

  # Discovery records may safely supply IDs and a connection string together
  discovered <- fabric_as_record(connstr)
  if (!is.null(discovered)) {
    if (
      !identical(
        tolower(fabric_record_value(discovered, "type") %||% ""),
        "semanticmodel"
      )
    ) {
      .fabric_abort(
        "connstr discovery record must be a SemanticModel item"
      )
    }

    discovered_workspace_id <- fabric_record_value(discovered, "workspaceId")
    discovered_dataset_id <- fabric_record_value(discovered, "id")

    pbi_validate_optional_guid(
      discovered_workspace_id,
      "discovered workspaceId"
    )
    pbi_validate_optional_guid(discovered_dataset_id, "discovered dataset id")

    # Explicit IDs must agree with any values carried by discovery
    pbi_reject_conflicting_id(
      workspace_id,
      discovered_workspace_id,
      "workspace_id",
      "the discovered SemanticModel workspaceId"
    )
    pbi_reject_conflicting_id(
      dataset_id,
      discovered_dataset_id,
      "dataset_id",
      "the discovered SemanticModel id"
    )

    workspace_id <- workspace_id %||% discovered_workspace_id
    dataset_id <- dataset_id %||% discovered_dataset_id
    connstr <- if (is.null(dataset_id)) {
      fabric_record_value(discovered, "dax_connection_string")
    } else {
      NULL
    }
  }

  # Copied connection strings and explicit selectors are alternative inputs
  if (
    !is.null(connstr) &&
      (!is.character(connstr) ||
        length(connstr) != 1L ||
        is.na(connstr) ||
        !nzchar(connstr))
  ) {
    .fabric_abort("connstr must be one non-empty string")
  }

  pbi_validate_optional_guid(workspace_id, "workspace_id")
  pbi_validate_optional_guid(dataset_id, "dataset_id")

  if (
    !is.logical(my_workspace) ||
      length(my_workspace) != 1L ||
      is.na(my_workspace)
  ) {
    .fabric_abort("my_workspace must be TRUE or FALSE")
  }

  if (!is.null(workspace_id) && isTRUE(my_workspace)) {
    .fabric_abort("Supply workspace_id or set my_workspace = TRUE, not both")
  }

  if (
    is.null(discovered) &&
      !is.null(connstr) &&
      (!is.null(workspace_id) || !is.null(dataset_id) || isTRUE(my_workspace))
  ) {
    .fabric_abort(
      paste0(
        "Supply a connection string or explicit workspace/dataset selectors, ",
        "not both"
      ),
      class = "fabric_pbi_target_conflict"
    )
  }

  if (is.null(dataset_id) && is.null(connstr)) {
    .fabric_abort(
      "Supply either connstr or dataset_id"
    )
  }

  # Validate optional execution settings after the target is unambiguous
  if (
    !is.null(impersonated_user) &&
      (!is.character(impersonated_user) ||
        length(impersonated_user) != 1L ||
        is.na(impersonated_user) ||
        !nzchar(impersonated_user))
  ) {
    .fabric_abort("impersonated_user must be one non-empty string")
  }

  arrow_options <- pbi_validate_arrow_options(arrow_options)

  if (identical(api, "json") && length(arrow_options)) {
    .fabric_abort("arrow_options can only be used with api = \"arrow\"")
  }

  if (identical(api, "json") && !identical(result, "tibble")) {
    .fabric_abort("result = \"arrow_stream\" requires api = \"arrow\"")
  }

  if (!is.null(dataset_id) && is.null(workspace_id) && !isTRUE(my_workspace)) {
    .fabric_abort(
      "dataset_id requires workspace_id or explicit my_workspace = TRUE"
    )
  }

  if (
    identical(api, "arrow") &&
      !is.null(impersonated_user) &&
      "effectiveUsername" %in% names(arrow_options)
  ) {
    .fabric_abort(
      "Supply effective identity through either impersonated_user or arrow_options, not both"
    )
  }

  # Check optional packages only when the selected result path needs them
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

  # 2 Resolve authentication and IDs ---------------------------------------------------------------

  # Name-based connection strings need API lookups; explicit IDs skip them

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
    pbi_validate_optional_guid(workspace_id, "workspace_id")
    pbi_validate_optional_guid(dataset_id, "dataset_id")
  }

  # 3 Execute through the selected transport -------------------------------------------------------

  # JSON returns a tibble directly. Arrow can return either a tibble or a
  # single-use stream for large results

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

# Validate and normalize the Power BI `api_base`. Returns a myorg URL
# before any Power BI token is sent
pbi_api_base <- function(api_base) {
  # 1 Validate the endpoint ------------------------------------------------------------------------

  # Check the endpoint now so later code can rely on safe input

  if (
    !is.character(api_base) ||
      length(api_base) != 1L ||
      is.na(api_base) ||
      !nzchar(api_base)
  ) {
    .fabric_abort("api_base must be one non-empty string")
  }

  # Parse the URL once, then inspect every part of its origin
  endpoint <- sub("/+$", "", trimws(api_base))
  parsed <- try(httr2::url_parse(endpoint), silent = TRUE)
  path <- if (inherits(parsed, "try-error")) "" else parsed$path %||% ""
  path <- sub("/+$", "", path)
  host <- if (inherits(parsed, "try-error")) {
    ""
  } else {
    tolower(parsed$hostname %||% "")
  }
  clean_origin <- !inherits(parsed, "try-error") &&
    identical(tolower(parsed$scheme %||% ""), "https") &&
    nzchar(host) &&
    !nzchar(parsed$username %||% "") &&
    !nzchar(parsed$password %||% "") &&
    !nzchar(parsed$port %||% "") &&
    identical(tolower(path), "/v1.0/myorg") &&
    length(parsed$query %||% list()) == 0L &&
    !nzchar(parsed$fragment %||% "")

  if (!clean_origin) {
    .fabric_abort(
      "api_base must be an HTTPS origin ending in /v1.0/myorg",
      class = "fabric_pbi_endpoint_error"
    )
  }

  # Supplying a custom host is the caller's explicit endpoint choice
  # 2 Return the normalized API base ---------------------------------------------------------------

  # Return the normalized API base in the stable form expected by the caller

  endpoint
}

# Validate optional `value` as a GUID identified by `name`. Returns invisibly for
# semantic-model target resolution
pbi_validate_optional_guid <- function(value, name) {
  if (is.null(value)) {
    return(invisible(value))
  }

  if (
    !is.character(value) ||
      length(value) != 1L ||
      is.na(value) ||
      !fabric_is_guid(value)
  ) {
    .fabric_abort(paste0(name, " must be a GUID"))
  }
  invisible(value)
}

#' Parse a Power BI connection string (XMLA) into components
#'
#' @param conn Character; a Power BI connection string
#' @return A list with elements `server`, `workspace`, and `dataset`
#' @keywords internal
#' @noRd
# Uses `conn`, and returns parsed server/workspace/dataset fields for name lookup
pbi_parse_connstr <- function(conn) {
  # 1 Split and validate fields --------------------------------------------------------------------

  # Reuse the shared parser so quoted names may contain semicolons safely

  if (!is.character(conn) || length(conn) != 1L || is.na(conn)) {
    .fabric_abort("conn must be one string")
  }
  toks <- trimws(fabric_split_connection_string(conn))
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
    .fabric_abort(
      "Could not find a unique Data Source in connection string"
    )
  }
  ds <- fabric_unquote_connection_value(ds[[1L]])
  shared <- regexec(
    "^powerbi://api\\.powerbi\\.com/v1\\.0/([^/]+)/([^/]+)/?$",
    ds,
    ignore.case = TRUE
  )
  shared <- regmatches(ds, shared)[[1L]]
  personal <- regexec(
    paste0(
      "^powerbi://api\\.powerbi\\.com/v2\\.0/",
      "([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-",
      "[0-9a-f]{12})/home/myworkspace/([^/]+)/?$"
    ),
    ds,
    ignore.case = TRUE
  )
  personal <- regmatches(ds, personal)[[1L]]
  if (!length(shared) && !length(personal)) {
    .fabric_abort(
      paste0(
        "Data Source must be a shared-workspace v1 or personal-workspace ",
        "v2 Power BI XMLA URL"
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
  catv <- vapply(
    catv,
    fabric_unquote_connection_value,
    character(1)
  )

  if (length(catv) != 1L || !nzchar(catv[[1L]])) {
    .fabric_abort(
      paste0(
        "Connection string must contain exactly one non-empty ",
        "Initial Catalog, Catalog, Database, or Dataset value"
      )
    )
  }
  dataset_name <- catv[[1L]]

  is_personal <- length(personal) > 0L
  workspace_name <- if (is_personal) {
    "My Workspace"
  } else {
    utils::URLdecode(shared[[3L]])
  }

  if (!nzchar(workspace_name)) {
    .fabric_abort(
      "Power BI Data Source does not contain a workspace name"
    )
  }

  # 2 Return the resolved target names -------------------------------------------------------------

  # Return the resolved target names in the stable form expected by the caller

  list(
    server = ds,
    workspace = workspace_name,
    dataset = dataset_name,
    personal = is_personal,
    tenant_id = if (is_personal) personal[[2L]] else NULL,
    owner = if (is_personal) utils::URLdecode(personal[[3L]]) else NULL
  )
}

#' Resolve workspace & dataset GUIDs using the Power BI REST API
#'
#' @param connstr Connection string used to infer workspace & dataset names
#' @param credential Internal audience-aware credential
#' @param api_base API base URL. Defaults to "https://api.powerbi.com/v1.0/myorg"
#' @return A list with `group_id`, `dataset_id`, `workspace`, and `dataset`
#' @keywords internal
#' @noRd
# Uses connection-string names plus `credential`; returns resolved Power BI IDs
pbi_resolve_ids_from_connstr <- function(
  connstr,
  credential,
  api_base = "https://api.powerbi.com/v1.0/myorg"
) {
  p <- pbi_parse_connstr(connstr)

  if (isTRUE(p$personal)) {
    .fabric_abort(
      paste0(
        "A personal-workspace XMLA target cannot be resolved safely through ",
        "the unscoped Power BI REST dataset route. Supply dataset_id and set ",
        "my_workspace = TRUE instead"
      ),
      class = "fabric_pbi_personal_workspace_error"
    )
  }

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
    dataset = p$dataset,
    personal = p$personal
  )
}

#' Execute a DAX query against a dataset
#'
#' @param credential Internal audience-aware credential
#' @param dataset_id Dataset GUID
#' @param dax DAX query
#' @param group_id Optional workspace (group) GUID. If supplied, the request is made to the group-scoped endpoint
#' @param include_nulls Logical; whether to include NULLs in response serialization
#' @param api_base API base URL
#' @param impersonated_user Optional impersonated user principal name
#' @return A tibble
#' @keywords internal
#' @noRd
# Uses IDs, query text, and `credential`; returns the parsed JSON result tibble
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
    bigint_as_char = TRUE,
    credential = credential,
    audience = .fabric_audience$power_bi,
    idempotent = TRUE
  )
  pbi_parse_dax_response(out)
}

#' Validate optional Execute DAX Queries Arrow request properties
#' @keywords internal
#' @noRd
# Uses `options`; returns a validated list for the Arrow Execute Queries request
pbi_validate_arrow_options <- function(options) {
  # 1 Validate option names ------------------------------------------------------------------------

  # Check option names now so later code can rely on safe input

  if (!is.list(options) || is.data.frame(options)) {
    .fabric_abort("arrow_options must be a named list")
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
    .fabric_abort("arrow_options must have unique, non-empty names")
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
    .fabric_abort(paste0(
      "Unsupported arrow_options name(s): ",
      paste(unknown, collapse = ", ")
    ))
  }
  options <- Filter(Negate(is.null), options)

  # 2 Validate option values -----------------------------------------------------------------------

  # Check option values now so later code can rely on safe input

  string_options <- intersect(
    names(options),
    c(
      "applicationContext",
      "culture",
      "customData",
      "effectiveUsername"
    )
  )

  # Text options must contain one useful value
  for (name in string_options) {
    value <- options[[name]]
    if (
      !is.character(value) ||
        length(value) != 1L ||
        is.na(value) ||
        !nzchar(value)
    ) {
      .fabric_abort(paste0(
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

  # Limits and timeouts use positive whole numbers
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
      .fabric_abort(paste0(
        "arrow_options$",
        name,
        " must be one positive integer"
      ))
    }
  }

  # Roles are sent as a non-empty list of names
  if ("roles" %in% names(options)) {
    roles <- options$roles
    if (
      !is.character(roles) ||
        !length(roles) ||
        anyNA(roles) ||
        !all(nzchar(roles))
    ) {
      .fabric_abort(
        "arrow_options$roles must be a non-empty character vector"
      )
    }
  }

  logical_options <- intersect(
    names(options),
    c("executionMetrics", "schemaOnly")
  )

  # Feature switches must be explicit TRUE or FALSE values
  for (name in logical_options) {
    value <- options[[name]]
    if (
      !is.logical(value) ||
        length(value) != 1L ||
        is.na(value)
    ) {
      .fabric_abort(paste0(
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
# Uses IDs, query text, options, and `credential`; returns an Arrow result shape
pbi_execute_dax_arrow <- function(
  credential,
  dataset_id,
  dax,
  group_id = NULL,
  api_base = "https://api.powerbi.com/v1.0/myorg",
  options = list(),
  result = c("tibble", "arrow_stream")
) {
  # 1 Validate and open the payload ----------------------------------------------------------------

  # A large response may be staged in a file; smaller responses arrive as raw
  # bytes. Both are consumed through Arrow's seekable buffer interface

  result <- match.arg(result)

  # Workspace models and My Workspace models use different service routes
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

  # Preserve roles as a JSON array even when only one role is supplied
  body <- c(list(query = dax), options)
  if (!is.null(body$roles)) {
    body$roles <- I(body$roles)
  }
  req <- httr2::request(path) |>
    httr2::req_headers(
      Accept = "application/vnd.apache.arrow.stream"
    ) |>
    httr2::req_body_json(body)

  # Stage the response so large Arrow streams do not need a second raw copy
  payload <- tempfile("fabricqueryr-dax-", fileext = ".arrow")
  keep_payload <- FALSE
  on.exit(
    {
      if (!keep_payload) {
        unlink(payload, force = TRUE)
      }
    },
    add = TRUE
  )

  .httr2_perform(
    req,
    credential = credential,
    audience = .fabric_audience$power_bi,
    idempotent = TRUE,
    download_path = payload
  )

  # Lazy streams take ownership of the staged file through cleanup metadata
  value <- pbi_parse_dax_arrow_response(
    payload,
    result = result,
    cleanup_path = identical(result, "arrow_stream")
  )
  keep_payload <- identical(result, "arrow_stream")

  value
}

#' Parse concatenated Execute DAX Queries Arrow IPC streams
#' @keywords internal
#' @noRd
# Uses raw or staged `payload`; returns rowset tibbles or retained Arrow streams
pbi_parse_dax_arrow_response <- function(
  payload,
  result = c("tibble", "arrow_stream"),
  cleanup_path = FALSE
) {
  # 1 Validate and open the response ---------------------------------------------------------------

  # Accept either downloaded bytes or a staged file, then open one Arrow reader
  # that the remaining sections can consume safely

  result <- match.arg(result)
  path_payload <- is.character(payload) &&
    length(payload) == 1L &&
    !is.na(payload) &&
    file.exists(payload)
  if ((!is.raw(payload) && !path_payload) || !length(payload)) {
    .fabric_abort("Power BI returned an empty Arrow DAX response")
  }

  if (path_payload && file.info(payload)$size == 0) {
    .fabric_abort("Power BI returned an empty Arrow DAX response")
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

  # 2 Read concatenated Arrow streams --------------------------------------------------------------

  # Power BI may concatenate data, error, and execution-metrics streams

  data_tables <- list()
  data_positions <- numeric()
  metrics_tables <- list()

  while (buffer$tell() < size) {
    # Remember each stream boundary so lazy results can reopen it later
    position <- buffer$tell()
    reader <- tryCatch(
      arrow::RecordBatchStreamReader$create(buffer),
      error = function(error) {
        .fabric_abort(
          "Power BI returned an invalid Arrow DAX response",
          parent = error
        )
      }
    )

    # Schema metadata tells data, service errors, and metrics apart
    schema <- reader$schema
    metadata <- schema$metadata %||% list()
    is_error <- identical(
      tolower(metadata[["IsError"]] %||% "false"),
      "true"
    )
    is_metrics <- identical(
      tolower(metadata[["IsExecMetrics"]] %||% "false"),
      "true"
    )

    # Error and metrics streams must be read now even for lazy output
    materialize <- !identical(result, "arrow_stream") || is_error || is_metrics
    table <- if (materialize) {
      value <- tryCatch(
        reader$read_table(),
        error = function(error) {
          .fabric_abort(
            "Could not decompress or read the Power BI Arrow DAX response",
            parent = error
          )
        }
      )
      pbi_decode_dax_arrow_dictionaries(value)
    } else {
      tryCatch(
        while (!is.null(reader$read_next_batch())) {
          # Validate and advance one bounded record batch at a time
        },
        error = function(error) {
          .fabric_abort(
            "Could not decompress or read the Power BI Arrow DAX response",
            parent = error
          )
        }
      )
      NULL
    }

    # Service error streams become typed R errors immediately
    if (is_error) {
      pbi_abort_dax_arrow_error(metadata, table)
    }

    # Keep metrics separate from the caller's rowsets
    if (is_metrics) {
      metrics_tables[[length(metrics_tables) + 1L]] <- table
    } else {
      data_tables[length(data_tables) + 1L] <- list(table)
      data_positions <- c(data_positions, position)
    }

    # A reader that did not advance would otherwise create an endless loop
    if (buffer$tell() <= position) {
      .fabric_abort("Power BI returned a non-advancing Arrow DAX stream")
    }
  }

  # 3 Validate returned rowsets --------------------------------------------------------------------

  # Check returned rowsets now so later code can rely on safe input

  if (!length(data_tables)) {
    .fabric_abort("Power BI returned no Arrow DAX data rowset")
  }

  if (length(metrics_tables) > 1L) {
    .fabric_abort(sprintf(
      "Power BI returned %d Arrow DAX execution-metrics rowsets; at most one is supported",
      length(metrics_tables)
    ))
  }
  metrics <- if (length(metrics_tables)) {
    tibble::as_tibble(as.data.frame(metrics_tables[[1L]]))
  } else {
    NULL
  }

  # 4 Build the requested R result -----------------------------------------------------------------

  # Stream results retain their buffers through a finalizer; tibble results are
  # materialized immediately

  if (identical(result, "arrow_stream")) {
    resource <- new.env(parent = emptyenv())
    resource$buffers <- vector("list", length(data_positions))
    resource$readers <- vector("list", length(data_positions))
    resource$path <- if (path_payload && isTRUE(cleanup_path)) payload else NULL
    reg.finalizer(
      resource,
      function(environment) {
        for (stream_buffer in environment$buffers) {
          try(stream_buffer$close(), silent = TRUE)
        }

        if (!is.null(environment$path)) {
          unlink(environment$path, force = TRUE)
        }
      },
      onexit = TRUE
    )
    rowsets <- lapply(seq_along(data_positions), function(index) {
      stream_buffer <- if (path_payload) {
        arrow::mmap_open(payload)
      } else {
        arrow::BufferReader$create(payload)
      }
      stream_buffer$seek(data_positions[[index]])
      stream_reader <- arrow::RecordBatchStreamReader$create(stream_buffer)
      resource$buffers[[index]] <- stream_buffer
      resource$readers[[index]] <- stream_reader
      stream <- nanoarrow::as_nanoarrow_array_stream(stream_reader)
      attr(stream, "fabric_dax_resource") <- resource
      stream
    })
  } else {
    rowsets <- lapply(data_tables, function(table) {
      pbi_dax_arrow_tibble(table)
    })
  }
  value <- if (length(rowsets) == 1L) {
    rowsets[[1L]]
  } else {
    structure(rowsets, class = c("fabric_pbi_dax_rowsets", "list"))
  }

  if (!is.null(metrics)) {
    attr(value, "execution_metrics") <- metrics
  }
  value
}

# Convert one Arrow DAX table without rounding fixed-precision decimals
pbi_dax_arrow_tibble <- function(table) {
  fields <- table$schema$fields
  decimal <- vapply(
    fields,
    function(field) inherits(field$type, "DecimalType"),
    logical(1)
  )
  if (any(decimal)) {
    fields[decimal] <- lapply(fields[decimal], function(field) {
      arrow::field(
        field$name,
        arrow::utf8(),
        nullable = field$nullable,
        metadata = field$metadata
      )
    })
    target <- do.call(arrow::schema, fields)
    if (length(table$schema$metadata)) {
      target <- target$WithMetadata(table$schema$metadata)
    }
    table <- table$cast(target)
  }
  tibble::as_tibble(as.data.frame(table))
}

#' Decode dictionary columns before exposing Arrow DAX results to R
#' @keywords internal
#' @noRd
# Uses an Arrow `table`; returns the table with dictionary columns decoded
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
# Uses response `metadata` and error `table`; raises a typed service error
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
  .fabric_abort(
    paste0("Power BI Arrow DAX query failed: ", detail),
    class = "fabric_pbi_dax_error"
  )
}

#' Validate and parse an Execute Queries response
#' @param out Parsed JSON response
#' @return A tibble
#' @keywords internal
#' @noRd
# Uses decoded JSON `out`; returns a normalized DAX result tibble
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
    .fabric_abort(
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
    .fabric_abort(
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

  # bigint_as_char preserves large Whole Numbers as strings. Promote smaller
  # numeric values in the same column before binding so a valid mixed-size
  # integer column remains exact and does not fail dplyr's type negotiation
  rows <- pbi_normalize_dax_integer_columns(rows)
  dplyr::bind_rows(rows)
}

# Reject different explicit and discovered IDs for one target field. Returns
# invisibly when the values agree or either side is absent
pbi_reject_conflicting_id <- function(
  explicit,
  discovered,
  explicit_name,
  source
) {
  if (
    !is.null(explicit) &&
      !is.null(discovered) &&
      !identical(tolower(explicit), tolower(discovered))
  ) {
    .fabric_abort(
      paste0(explicit_name, " conflicts with ", source),
      class = "fabric_pbi_target_conflict"
    )
  }
  invisible()
}

# Normalize JSON integer columns in `rows` without losing values. Returns rows
# with safe base integer or bit64 representations before tibble binding
pbi_normalize_dax_integer_columns <- function(rows) {
  column_names <- unique(unlist(lapply(rows, names), use.names = FALSE))
  for (column_name in column_names) {
    values <- lapply(rows, function(row) row[[column_name]])
    present <- Filter(Negate(is.null), values)
    has_character <- any(vapply(present, is.character, logical(1)))
    has_numeric <- any(vapply(
      present,
      function(value) is.integer(value) || is.double(value),
      logical(1)
    ))
    if (!has_character || !has_numeric) {
      next
    }
    rows <- lapply(rows, function(row) {
      value <- row[[column_name]]
      if (!is.null(value) && (is.integer(value) || is.double(value))) {
        row[[column_name]] <- if (is.na(value)) {
          NA_character_
        } else {
          format(value, scientific = FALSE, trim = TRUE, digits = 22)
        }
      }
      row
    })
  }
  rows
}

#' Raise an actionable embedded Execute Queries error
#' @keywords internal
#' @noRd
# Uses one embedded `error` and nesting `level`; returns invisibly or raises
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
    .fabric_abort(
      paste0(
        "Power BI returned an incomplete DAX ",
        level,
        ": ",
        detail,
        ". Reduce the selected rows/columns or page the query in DAX"
      )
    )
  }
  .fabric_abort(
    paste0("Power BI DAX ", level, " failed: ", detail)
  )
}

#' Get a workspace (group) GUID by its name
#'
#' @param credential Internal audience-aware credential
#' @param workspace_name Character; workspace display name (case-insensitive)
#' @param api_base API base URL
#' @return Group GUID as a string
#' @keywords internal
#' @noRd
# Uses an exact workspace name and `credential`; returns its group GUID
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
    .fabric_abort(sprintf("Workspace '%s' not found", workspace_name))
  }

  if (length(hits) > 1L) {
    .fabric_abort(
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
#' @param credential Internal audience-aware credential
#' @param group_id Workspace (group) GUID
#' @param dataset_name Dataset display name (case-insensitive)
#' @param api_base API base URL
#' @return Dataset GUID as a string
#' @keywords internal
#' @noRd
# Uses an exact dataset name and `credential`; returns its dataset GUID
pbi_get_dataset_id_by_name <- function(
  credential,
  group_id,
  dataset_name,
  api_base = "https://api.powerbi.com/v1.0/myorg"
) {
  url <- if (is.null(group_id)) {
    sprintf("%s/datasets", api_base)
  } else {
    sprintf("%s/groups/%s/datasets", api_base, group_id)
  }
  vals <- pbi_get_collection(url, credential)
  hits <- vals[vapply(
    vals,
    function(d) tolower(d$name) == tolower(dataset_name),
    logical(1)
  )]
  if (length(hits) == 0) {
    .fabric_abort(sprintf("Dataset '%s' not found in workspace", dataset_name))
  }

  if (length(hits) > 1L) {
    .fabric_abort(
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
#' @param url Initial collection URL
#' @param credential Internal audience-aware credential
#' @param offset_pagination Whether to use documented `$top`/`$skip` paging
#' @param page_size Page size for offset pagination
#' @return A list containing every returned value
#' @keywords internal
#' @noRd
# Uses a paged collection URL and `credential`; returns every response value
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
