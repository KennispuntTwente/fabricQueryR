#' @title
#' Query a Microsoft Fabric/Power Bi semantic model with DAX
#'
#' @description
#' High-level helper that authenticates against Azure AD, resolves the
#' workspace & dataset from a Power BI (Microsoft Fabric) XMLA/connection string, executes a DAX
#' statement via the Power BI REST API, validates the complete response, and
#' returns a tibble with the resulting data.
#'
#' @details
#' - In Microsoft Fabric/Power BI, you can find and copy the connection string by going to
#'  a 'Semantic model' item, then go to 'File' -> 'Settings' -> 'Server settings'.
#'  Ensure that the account you use to authenticate has access to the workspace,
#'  or has been granted 'Build' permissions on the dataset (via sharing).
#' - \pkg{AzureAuth} is used to acquire the token. Be wary of
#'  caching behavior; you may want to call [AzureAuth::clean_token_directory()]
#'  to clear cached tokens if you run into issues
#' - Requests use the Power BI audience
#'  `https://analysis.windows.net/powerbi/api/.default` and require
#'  `Dataset.Read.All` (or `Dataset.ReadWrite.All`) plus dataset Read and Build
#'  permissions. Name lookup also requires `Workspace.Read.All` or equivalent.
#'
#' @param connstr Optional character Power BI connection string or one
#'   SemanticModel record returned by [fabric_semantic_models()] or
#'   [fabric_item()]. For a discovered record, workspace and dataset IDs are
#'   used directly.
#'   A character connection string can be, e.g.
#'   `"Data Source=powerbi://api.powerbi.com/v1.0/myorg/Workspace;Initial Catalog=Dataset;"`.
#'   The function accepts either `Data Source=` and `Initial Catalog=` parts, or a
#'   bare `powerbi://...` for the data source plus a `Dataset=`/`Catalog=`/`Initial Catalog=` key
#'   (see details). May be omitted when `dataset_id` is supplied.
#' @param workspace_id Optional workspace GUID. Use with `dataset_id` to avoid
#'   name-based discovery. If omitted with `dataset_id`, the unscoped dataset
#'   endpoint is used.
#' @param dataset_id Optional semantic model/dataset GUID. When supplied, no
#'   connection-string name lookup is performed.
#' @param dax Character scalar with a valid DAX query (see example).
#' @param tenant_id Microsoft Azure tenant ID. Defaults to `Sys.getenv("FABRICQUERYR_TENANT_ID")` if missing.
#' @param client_id Microsoft Azure application (client) ID used to authenticate. Defaults to
#'   `Sys.getenv("FABRICQUERYR_CLIENT_ID")`. You may be able to use the Azure CLI app id
#'   `"04b07795-8ddb-461a-bbee-02f9e1bf7b46"`, but may want to make your own
#'   app registration in your tenant for better control.
#' @param include_nulls Logical; pass-through to the REST serializer setting. Defaults to TRUE.
#' If TRUE, null values are included in the response; if FALSE, they are omitted.
#' @param api_base API base URL. Defaults to "https://api.powerbi.com/v1.0/myorg".
#' 'myorg' is appropriate for most use cases and does not necessarily need to be changed.
#' @param access_token Optional character. If supplied, use this bearer token
#' instead of acquiring a new one via `{AzureAuth}`.
#' @param token_provider Optional function that returns a Power BI bearer token.
#'   It may accept `audience` and `force_refresh` arguments and is called again
#'   after an HTTP 401. Supply only one of `access_token` and `token_provider`.
#' @param impersonated_user Optional user principal name sent as
#'   `impersonatedUserName` for supported row-level security scenarios.
#'
#' @return A tibble with the query result (0 rows if the DAX query returned no rows).
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
  include_nulls = TRUE,
  api_base = "https://api.powerbi.com/v1.0/myorg",
  access_token = NULL,
  token_provider = NULL,
  impersonated_user = NULL
) {
  stopifnot(is.character(dax), length(dax) == 1L, nzchar(dax))
  discovered <- fabric_as_record(connstr)
  if (!is.null(discovered)) {
    if (
      !identical(
        tolower(fabric_record_value(discovered, "type") %||% ""),
        "semanticmodel"
      )
    ) {
      stop(
        "connstr discovery record must be a SemanticModel item.",
        call. = FALSE
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
  if (!is.null(connstr)) {
    stopifnot(is.character(connstr), length(connstr) == 1L, nzchar(connstr))
  }
  if (!is.null(workspace_id)) {
    stopifnot(
      is.character(workspace_id),
      length(workspace_id) == 1L,
      nzchar(workspace_id)
    )
  }
  if (!is.null(dataset_id)) {
    stopifnot(
      is.character(dataset_id),
      length(dataset_id) == 1L,
      nzchar(dataset_id)
    )
  }
  if (is.null(dataset_id) && is.null(connstr)) {
    stop(
      "Supply either connstr or dataset_id.",
      call. = FALSE
    )
  }
  if (!is.null(impersonated_user)) {
    stopifnot(
      is.character(impersonated_user),
      length(impersonated_user) == 1L,
      nzchar(impersonated_user)
    )
  }

  credential <- fabric_credential(
    tenant_id = tenant_id,
    client_id = client_id,
    access_token = access_token,
    token_provider = token_provider
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

  pbi_execute_dax(
    credential = credential,
    dataset_id = dataset_id,
    dax = dax,
    group_id = workspace_id,
    include_nulls = include_nulls,
    api_base = api_base,
    impersonated_user = impersonated_user
  )
}

#' Parse a Power BI connection string (XMLA) into components
#'
#' @param conn Character; a Power BI connection string.
#' @return A list with elements `server`, `workspace`, and `dataset`.
#' @keywords internal
#' @noRd
pbi_parse_connstr <- function(conn) {
  stopifnot(is.character(conn), length(conn) == 1L)
  toks <- strsplit(conn, ";", fixed = TRUE)[[1]]
  toks <- trimws(toks)

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
    stop(
      "Could not find a unique Data Source in connection string.",
      call. = FALSE
    )
  }
  ds <- ds[[1]]

  # Dataset name can be specified using several synonyms
  catv <- sub(
    "(?i)^(Initial Catalog|Catalog|Database|Dataset)=",
    "",
    toks[grepl("(?i)^(Initial Catalog|Catalog|Database|Dataset)=", toks)],
    perl = TRUE
  )
  dataset_name <- if (length(catv)) catv[[1]] else NA_character_

  # Workspace is the last segment of the Data Source URL
  ds_clean <- sub("(?i)^powerbi://", "", ds)
  segs <- strsplit(ds_clean, "/", fixed = TRUE)[[1]]
  workspace_name <- utils::URLdecode(utils::tail(segs, 1))

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
#' @param access_token OAuth2 bearer token.
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
    serializerSettings = list(includeNulls = isTRUE(include_nulls))
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
    stop(
      sprintf(
        "Power BI returned %d query results; exactly one is supported.",
        length(results)
      ),
      call. = FALSE
    )
  }

  tables <- results[[1]]$tables
  if (is.null(tables) || length(tables) == 0L) {
    return(tibble::tibble())
  }
  if (length(tables) != 1L) {
    stop(
      sprintf(
        "Power BI returned %d result tables; exactly one is supported.",
        length(tables)
      ),
      call. = FALSE
    )
  }

  rows <- tables[[1]]$rows
  if (is.null(rows) || length(rows) == 0L) {
    return(tibble::tibble())
  }

  # bind_rows preserves qualified and bracketed Power BI column names.
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
    stop(
      paste0(
        "Power BI returned an incomplete DAX ",
        level,
        ": ",
        detail,
        ". Reduce the selected rows/columns or page the query in DAX."
      ),
      call. = FALSE
    )
  }
  stop(
    paste0("Power BI DAX ", level, " failed: ", detail),
    call. = FALSE
  )
}

#' Get a workspace (group) GUID by its name
#'
#' @param access_token OAuth2 bearer token.
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
    stop(sprintf("Workspace '%s' not found.", workspace_name), call. = FALSE)
  }
  if (length(hits) > 1L) {
    stop(
      sprintf(
        "Workspace name '%s' is ambiguous (%d case-insensitive matches). Use workspace_id.",
        workspace_name,
        length(hits)
      ),
      call. = FALSE
    )
  }
  hits[[1]]$id
}

#' Get a dataset GUID by its name in a workspace
#'
#' @param access_token OAuth2 bearer token.
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
    stop(
      sprintf("Dataset '%s' not found in workspace.", dataset_name),
      call. = FALSE
    )
  }
  if (length(hits) > 1L) {
    stop(
      sprintf(
        "Dataset name '%s' is ambiguous in the workspace (%d case-insensitive matches). Use dataset_id.",
        dataset_name,
        length(hits)
      ),
      call. = FALSE
    )
  }
  hits[[1]]$id
}

#' Read a complete Power BI collection
#' @param url Initial collection URL.
#' @param access_token OAuth2 bearer token.
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
    credential <- fabric_credential(access_token = credential)
  }
  .httr2_collection(
    url,
    credential = credential,
    audience = .fabric_audience$power_bi,
    offset_pagination = offset_pagination,
    page_size = page_size
  )
}
