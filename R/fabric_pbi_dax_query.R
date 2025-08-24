#' @title
#' Query a Microsoft Fabric/Power Bi semantic model with DAX
#'
#' @description
#' High-level helper that authenticates against Azure AD, resolves the
#' workspace & dataset from a Power BI (Microsoft Fabric) XMLA/connection string, executes a DAX
#' statement via the Power BI REST API, and returns a tibble with
#' the resulting data.
#'
#' @details
#' - In Microsoft Fabric/Power BI, you can find and copy the connection string by going to
#'  a 'Semantic model' item, then go to 'File' -> 'Settings' -> 'Server settings'.
#'  Ensure that the account you use to authenticate has access to the workspace,
#'  or has been granted 'Build' permissions on the dataset (via sharing).
#' - \pkg{AzureAuth} is used to acquire the token. Be wary of
#'  caching behavior; you may want to call [AzureAuth::clean_token_directory()]
#'  to clear cached tokens if you run into issues
#'
#' @param connstr Character. Power BI connection string, e.g.
#'   `"Data Source=powerbi://api.powerbi.com/v1.0/myorg/Workspace;Initial Catalog=Dataset;"`.
#'   The function accepts either `Data Source=` and `Initial Catalog=` parts, or a
#'   bare `powerbi://...` for the data source plus a `Dataset=`/`Catalog=`/`Initial Catalog=` key
#'   (see details).
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
#'
#' @return A tibble with the query result (0 rows if the DAX query returned no rows).
#' @export
#'
#' @examples
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
  connstr,
  dax,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  include_nulls = TRUE,
  api_base = "https://api.powerbi.com/v1.0/myorg"
) {
  stopifnot(is.character(connstr), length(connstr) == 1L)
  stopifnot(is.character(dax), length(dax) == 1L)

  if (!nzchar(tenant_id))
    stop(
      "tenant_id is required (or set FABRICQUERYR_TENANT_ID env var).",
      call. = FALSE
    )
  if (!nzchar(client_id))
    stop(
      "client_id is required (or set FABRICQUERYR_CLIENT_ID env var).",
      call. = FALSE
    )

  token <- pbi_get_token(tenant_id = tenant_id, client_id = client_id)
  ids <- pbi_resolve_ids_from_connstr(
    connstr = connstr,
    access_token = token,
    api_base = api_base
  )

  pbi_execute_dax(
    access_token = token,
    dataset_id = ids$dataset_id,
    dax = dax,
    group_id = ids$group_id,
    include_nulls = include_nulls,
    api_base = api_base
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
  if (length(ds) == 0) ds <- toks[grepl("(?i)^powerbi://", toks)]
  if (length(ds) != 1)
    stop(
      "Could not find a unique Data Source in connection string.",
      call. = FALSE
    )
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
#' @param access_token OAuth2 bearer token for the Power BI API.
#' @param api_base API base URL. Defaults to "https://api.powerbi.com/v1.0/myorg".
#' @return A list with `group_id`, `dataset_id`, `workspace`, and `dataset`.
#' @keywords internal
#' @noRd
pbi_resolve_ids_from_connstr <- function(
  connstr,
  access_token,
  api_base = "https://api.powerbi.com/v1.0/myorg"
) {
  p <- pbi_parse_connstr(connstr)

  group_id <- pbi_get_group_id_by_name(
    access_token = access_token,
    workspace_name = p$workspace,
    api_base = api_base
  )
  dataset_id <- pbi_get_dataset_id_by_name(
    access_token = access_token,
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
  tok <- AzureAuth::get_azure_token(
    tenant = tenant_id,
    app = client_id,
    version = 2,
    resource = c(
      "https://analysis.windows.net/powerbi/api/.default",
      "offline_access"
    )
  )
  tok$credentials$access_token
}

#' Execute a DAX query against a dataset
#'
#' @param access_token OAuth2 bearer token.
#' @param dataset_id Dataset GUID.
#' @param dax DAX query.
#' @param group_id Optional workspace (group) GUID. If supplied, the request is made to the group-scoped endpoint.
#' @param include_nulls Logical; whether to include NULLs in response serialization.
#' @param api_base API base URL.
#' @return A tibble.
#' @keywords internal
#' @noRd
pbi_execute_dax <- function(
  access_token,
  dataset_id,
  dax,
  group_id = NULL,
  include_nulls = TRUE,
  api_base = "https://api.powerbi.com/v1.0/myorg"
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

  req <- httr2::request(path) |>
    httr2::req_headers(Authorization = paste("Bearer", access_token)) |>
    httr2::req_body_json(body)

  resp <- httr2::req_perform(req)
  out <- httr2::resp_body_json(resp)

  tbls <- out$results[[1]]$tables
  if (is.null(tbls) || length(tbls) == 0L) return(tibble::tibble())

  rows <- tbls[[1]]$rows
  if (is.null(rows) || length(rows) == 0L) return(tibble::tibble())

  # rows: list of named lists. bind_rows keeps original column names, including [Table][Col] style.
  dplyr::bind_rows(rows)
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
  access_token,
  workspace_name,
  api_base = "https://api.powerbi.com/v1.0/myorg"
) {
  url <- sprintf("%s/groups", api_base)
  resp <- httr2::request(url) |>
    httr2::req_headers(Authorization = paste("Bearer", access_token)) |>
    httr2::req_perform() |>
    httr2::resp_body_json()

  vals <- resp$value
  hits <- vals[vapply(
    vals,
    function(g) tolower(g$name) == tolower(workspace_name),
    logical(1)
  )]
  if (length(hits) == 0)
    stop(sprintf("Workspace '%s' not found.", workspace_name), call. = FALSE)
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
  access_token,
  group_id,
  dataset_name,
  api_base = "https://api.powerbi.com/v1.0/myorg"
) {
  url <- sprintf("%s/groups/%s/datasets", api_base, group_id)
  resp <- httr2::request(url) |>
    httr2::req_headers(Authorization = paste("Bearer", access_token)) |>
    httr2::req_perform() |>
    httr2::resp_body_json()

  vals <- resp$value
  hits <- vals[vapply(
    vals,
    function(d) tolower(d$name) == tolower(dataset_name),
    logical(1)
  )]
  if (length(hits) == 0)
    stop(
      sprintf("Dataset '%s' not found in workspace.", dataset_name),
      call. = FALSE
    )
  hits[[1]]$id
}
