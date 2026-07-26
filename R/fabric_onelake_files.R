#' Work with files in Microsoft Fabric OneLake
#'
#' @description
#' These functions expose the ADLS Gen2-compatible OneLake filesystem without
#' applying Delta Lake transaction semantics:
#'
#' - `fabric_onelake_list()` lists paths and follows all continuation tokens.
#' - `fabric_onelake_metadata()` returns file or directory properties.
#' - `fabric_onelake_download()` reads a file into memory or streams it to disk.
#' - `fabric_onelake_upload()` creates or replaces a file.
#' - `fabric_onelake_delete()` explicitly deletes a file or directory.
#'
#' @details
#' `workspace` and `item` may be names, GUIDs, or one-row discovery results.
#' Name-based items must include their item-type suffix (for example,
#' `"Sales.Lakehouse"`) or supply `item_type`. Microsoft requires workspace and
#' item GUIDs to be used together. As a convenience, `workspace` may instead be
#' a complete `https://...dfs.fabric.microsoft.com/...` or `abfss://...`
#' OneLake path, in which case `item` must be `NULL`.
#'
#' OneLake uses the Storage token audience
#' `https://storage.azure.com/.default`. Fabric manages the item root and its
#' first-level folders (such as `Files` and `Tables`), so upload and delete
#' operations are limited to descendants of a managed folder. Deletion also
#' requires `confirm = TRUE`.
#'
#' Uploads are streamed in chunks to a temporary sibling file. The completed
#' file is atomically renamed to its destination with the requested overwrite
#' or ETag precondition, so failed transfers do not truncate an existing file.
#'
#' @param workspace Workspace name, GUID, discovered workspace, or complete
#'   OneLake HTTPS/ABFSS path.
#' @param item Item name, GUID, or discovered Fabric item. Use `NULL` when
#'   `workspace` is a complete OneLake path.
#' @param path Path relative to the item. Complete OneLake paths already contain
#'   this value.
#' @param recursive Logical. Recurse into subdirectories.
#' @param page_size Maximum paths requested per ADLS page, from 1 to 5000.
#' @param item_type Optional Fabric item type appended to an item name unless
#'   that name already ends in the same suffix, for example `"Lakehouse"`.
#' @param tenant_id Entra tenant ID. Defaults to
#'   `FABRICQUERYR_TENANT_ID`.
#' @param client_id Entra application ID. Defaults to
#'   `FABRICQUERYR_CLIENT_ID`, then the Azure CLI application ID.
#' @param token Optional `AzureAuth::AzureToken`, bearer-token string, or
#'   token-provider function. With `NULL`, `AzureAuth` reuses a matching cached
#'   token or starts its normal interactive login flow.
#' @param auth_args Named list of additional arguments passed to
#'   [AzureAuth::get_azure_token()].
#' @param dfs_base OneLake DFS endpoint. Regional and workspace-private DFS
#'   endpoints are supported.
#' @param range Optional inclusive zero-based byte range. Supply one value for
#'   `bytes=start-` or two values for `bytes=start-end`.
#' @param dest Optional local destination. When `NULL`, download returns a raw
#'   vector. A destination download is streamed and committed atomically.
#' @param overwrite Logical. Whether an existing destination may be replaced.
#'   Upload defaults to `FALSE` and uses `If-None-Match: *`.
#' @param if_match Optional ETag for a conditional operation. Values returned
#'   by `fabric_onelake_metadata()` can be passed back unchanged; unquoted
#'   values are quoted as required by ADLS.
#' @param source A local file path or raw vector to upload.
#' @param chunk_size Positive upload chunk size in bytes. Defaults to 8 MiB;
#'   local files are streamed without being loaded into memory.
#' @param content_type Optional MIME type stored with an uploaded file.
#' @param create_parents Logical. Create missing parent directories below the
#'   Fabric-managed first-level folder.
#' @param confirm Must be `TRUE` to enable deletion.
#'
#' @return `fabric_onelake_list()` and `fabric_onelake_metadata()` return
#'   tibbles. `fabric_onelake_download()` returns a raw vector, or invisibly
#'   returns the destination path. `fabric_onelake_upload()` returns a one-row
#'   metadata tibble. `fabric_onelake_delete()` invisibly returns `TRUE`.
#'
#' @examples
#' \dontrun{
#' files <- fabric_onelake_list(
#'   workspace = "Finance",
#'   item = "Curated.Lakehouse",
#'   path = "Files/incoming",
#'   recursive = TRUE
#' )
#'
#' bytes <- fabric_onelake_download(
#'   "Finance",
#'   "Curated.Lakehouse",
#'   "Files/incoming/data.csv",
#'   range = c(0, 99)
#' )
#'
#' fabric_onelake_upload(
#'   "Finance",
#'   "Curated.Lakehouse",
#'   "Files/incoming/data.csv",
#'   source = "data.csv"
#' )
#'
#' fabric_onelake_delete(
#'   "Finance",
#'   "Curated.Lakehouse",
#'   "Files/incoming/data.csv",
#'   confirm = TRUE
#' )
#' }
#' @name fabric_onelake_files
NULL

#' @rdname fabric_onelake_files
#' @export
fabric_onelake_list <- function(
  workspace,
  item = NULL,
  path = "",
  recursive = FALSE,
  page_size = 5000L,
  item_type = NULL,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  token = NULL,
  auth_args = list(),
  dfs_base = "https://onelake.dfs.fabric.microsoft.com"
) {
  target <- onelake_resolve_target(
    workspace,
    item,
    path,
    item_type,
    dfs_base
  )
  credential <- fabric_credential(
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args
  )
  onelake_list_target(
    target,
    credential,
    recursive = recursive,
    page_size = page_size
  )
}

#' @rdname fabric_onelake_files
#' @export
fabric_onelake_metadata <- function(
  workspace,
  item = NULL,
  path = "",
  item_type = NULL,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  token = NULL,
  auth_args = list(),
  dfs_base = "https://onelake.dfs.fabric.microsoft.com"
) {
  target <- onelake_resolve_target(
    workspace,
    item,
    path,
    item_type,
    dfs_base
  )
  credential <- fabric_credential(
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args
  )
  onelake_metadata_target(target, credential)
}

#' @rdname fabric_onelake_files
#' @export
fabric_onelake_download <- function(
  workspace,
  item = NULL,
  path = "",
  dest = NULL,
  range = NULL,
  overwrite = FALSE,
  if_match = NULL,
  item_type = NULL,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  token = NULL,
  auth_args = list(),
  dfs_base = "https://onelake.dfs.fabric.microsoft.com"
) {
  target <- onelake_resolve_target(
    workspace,
    item,
    path,
    item_type,
    dfs_base
  )
  onelake_require_file_path(target, "download")
  credential <- fabric_credential(
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args
  )
  onelake_download_target(
    target,
    credential,
    dest = dest,
    range = range,
    overwrite = overwrite,
    if_match = if_match
  )
}

#' @rdname fabric_onelake_files
#' @export
fabric_onelake_upload <- function(
  workspace,
  item = NULL,
  path = "",
  source,
  overwrite = FALSE,
  if_match = NULL,
  content_type = NULL,
  create_parents = TRUE,
  item_type = NULL,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  token = NULL,
  auth_args = list(),
  dfs_base = "https://onelake.dfs.fabric.microsoft.com",
  chunk_size = getOption(
    "fabricqueryr.onelake.chunk_size",
    8 * 1024^2
  )
) {
  target <- onelake_resolve_target(
    workspace,
    item,
    path,
    item_type,
    dfs_base
  )
  onelake_require_mutable_path(target, "upload")
  if (!is.logical(overwrite) || length(overwrite) != 1L || is.na(overwrite)) {
    rlang::abort("overwrite must be TRUE or FALSE")
  }
  if (!is.null(if_match) && !isTRUE(overwrite)) {
    rlang::abort("if_match requires overwrite = TRUE")
  }
  credential <- fabric_credential(
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args
  )
  onelake_upload_target(
    target,
    credential,
    source = source,
    overwrite = overwrite,
    if_match = if_match,
    chunk_size = chunk_size,
    content_type = content_type,
    create_parents = create_parents
  )
}

#' @rdname fabric_onelake_files
#' @export
fabric_onelake_delete <- function(
  workspace,
  item = NULL,
  path = "",
  recursive = FALSE,
  confirm = FALSE,
  if_match = NULL,
  item_type = NULL,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  token = NULL,
  auth_args = list(),
  dfs_base = "https://onelake.dfs.fabric.microsoft.com"
) {
  target <- onelake_resolve_target(
    workspace,
    item,
    path,
    item_type,
    dfs_base
  )
  onelake_require_mutable_path(target, "delete")
  if (!isTRUE(confirm)) {
    rlang::abort(
      "Deletion is disabled by default; set confirm = TRUE explicitly"
    )
  }
  credential <- fabric_credential(
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args
  )
  onelake_delete_target(
    target,
    credential,
    recursive = recursive,
    if_match = if_match
  )
}

onelake_resolve_target <- function(
  workspace,
  item = NULL,
  path = "",
  item_type = NULL,
  dfs_base = "https://onelake.dfs.fabric.microsoft.com"
) {
  if (
    is.character(workspace) &&
      length(workspace) == 1L &&
      grepl("^(?:https|abfss?)://", workspace, ignore.case = TRUE)
  ) {
    if (!is.null(item)) {
      rlang::abort(
        "item must be NULL when workspace is a complete OneLake path"
      )
    }
    if (!identical(path, "")) {
      rlang::abort(
        "path must be empty when workspace is a complete OneLake path"
      )
    }
    return(onelake_parse_uri(workspace))
  }

  workspace_record <- fabric_as_record(workspace)
  item_record <- fabric_as_record(item)
  workspace_value <- if (is.null(workspace_record)) {
    workspace
  } else {
    fabric_record_value(workspace_record, "id", "workspaceId")
  }
  item_value <- if (is.null(item_record)) {
    item
  } else {
    fabric_record_value(item_record, "id")
  }
  item_workspace <- if (is.null(item_record)) {
    NULL
  } else {
    fabric_record_value(item_record, "workspaceId")
  }
  if (!is.null(item_workspace)) {
    if (
      !is.null(workspace_value) &&
        fabric_is_guid(as.character(workspace_value)) &&
        !identical(tolower(workspace_value), tolower(item_workspace))
    ) {
      rlang::abort(
        "The discovered item belongs to a different workspace"
      )
    }
    workspace_value <- item_workspace
  }
  onelake_scalar(workspace_value, "workspace")
  onelake_scalar(item_value, "item")
  onelake_scalar(path, "path", allow_empty = TRUE)

  workspace_guid <- fabric_is_guid(workspace_value)
  item_guid <- fabric_is_guid(item_value)
  if (!identical(workspace_guid, item_guid)) {
    rlang::abort(
      "OneLake requires workspace and item GUIDs to be used together"
    )
  }
  if (!item_guid) {
    if (!is.null(item_type)) {
      onelake_scalar(item_type, "item_type")
      suffix <- paste0(".", item_type)
      if (!endsWith(tolower(item_value), tolower(suffix))) {
        item_value <- paste0(item_value, ".", item_type)
      }
    } else if (!grepl("\\.[^.]+$", item_value)) {
      rlang::abort(
        "A name-based item needs its type suffix or item_type"
      )
    }
  }

  onelake_validate_endpoint(dfs_base)
  structure(
    list(
      dfs_base = sub("/+$", "", dfs_base),
      workspace = workspace_value,
      item = item_value,
      path = onelake_normalize_path(path, allow_empty = TRUE)
    ),
    class = "fabric_onelake_target"
  )
}

onelake_parse_uri <- function(uri) {
  parsed <- httr2::url_parse(uri)
  scheme <- tolower(parsed$scheme %||% "")
  if (!scheme %in% c("https", "abfs", "abfss")) {
    rlang::abort("OneLake paths must use HTTPS, ABFS, or ABFSS")
  }
  onelake_validate_host(parsed$hostname)
  if (!is.null(parsed$query) || !is.null(parsed$fragment)) {
    rlang::abort(
      "A OneLake path must not contain a query string or fragment"
    )
  }

  if (scheme == "https") {
    pieces <- strsplit(sub("^/+", "", parsed$path), "/", fixed = TRUE)[[1L]]
    if (length(pieces) < 2L) {
      rlang::abort(
        "A OneLake HTTPS path must include workspace and item"
      )
    }
    workspace <- pieces[[1L]]
    item <- pieces[[2L]]
    path <- paste(utils::tail(pieces, -2L), collapse = "/")
  } else {
    workspace <- parsed$username
    pieces <- strsplit(sub("^/+", "", parsed$path), "/", fixed = TRUE)[[1L]]
    if (is.null(workspace) || !nzchar(workspace) || !length(pieces)) {
      rlang::abort("An ABFS path must include workspace and item")
    }
    item <- pieces[[1L]]
    path <- paste(utils::tail(pieces, -1L), collapse = "/")
  }
  onelake_scalar(workspace, "workspace")
  onelake_scalar(item, "item")
  workspace_guid <- fabric_is_guid(workspace)
  item_guid <- fabric_is_guid(item)
  if (!identical(workspace_guid, item_guid)) {
    rlang::abort(
      "OneLake requires workspace and item GUIDs to be used together"
    )
  }
  if (!item_guid && !grepl("\\.[^.]+$", item)) {
    rlang::abort(
      "A name-based OneLake item must include its type suffix"
    )
  }
  structure(
    list(
      dfs_base = paste0("https://", parsed$hostname),
      workspace = workspace,
      item = item,
      path = onelake_normalize_path(path, allow_empty = TRUE)
    ),
    class = "fabric_onelake_target"
  )
}

onelake_scalar <- function(value, name, allow_empty = FALSE) {
  if (
    !is.character(value) ||
      length(value) != 1L ||
      is.na(value) ||
      (!allow_empty && !nzchar(value))
  ) {
    rlang::abort(paste0(
      name,
      " must be one ",
      if (allow_empty) "" else "non-empty ",
      "character value"
    ))
  }
  invisible(value)
}

onelake_validate_endpoint <- function(endpoint) {
  onelake_scalar(endpoint, "dfs_base")
  parsed <- httr2::url_parse(endpoint)
  if (!identical(tolower(parsed$scheme %||% ""), "https")) {
    rlang::abort("dfs_base must use HTTPS")
  }
  onelake_validate_host(parsed$hostname)
  if (
    !identical(parsed$path %||% "", "") &&
      !identical(parsed$path %||% "", "/")
  ) {
    rlang::abort("dfs_base must not include a path")
  }
  invisible(endpoint)
}

onelake_validate_host <- function(host) {
  host <- tolower(host %||% "")
  valid <- grepl("(^|\\.)dfs\\.fabric\\.microsoft\\.com$", host) ||
    grepl("(^|[-.])api\\.onelake\\.fabric\\.microsoft\\.com$", host)
  if (!valid) {
    rlang::abort("The endpoint is not a Microsoft Fabric OneLake host")
  }
  invisible(host)
}

onelake_normalize_path <- function(path, allow_empty = FALSE) {
  onelake_scalar(path, "path", allow_empty = allow_empty)
  path <- gsub("\\\\", "/", path)
  path <- sub("^/+", "", sub("/+$", "", path))
  if (!nzchar(path)) {
    if (allow_empty) {
      return("")
    }
    rlang::abort("path must not be empty")
  }
  pieces <- strsplit(path, "/", fixed = TRUE)[[1L]]
  if (!all(nzchar(pieces)) || any(pieces %in% c(".", ".."))) {
    rlang::abort("path contains an empty or unsafe segment")
  }
  if (any(grepl("[?#\r\n]", pieces))) {
    rlang::abort("path contains a query, fragment, or line break")
  }
  paste(pieces, collapse = "/")
}

onelake_require_file_path <- function(target, operation) {
  if (!nzchar(target$path)) {
    rlang::abort(
      cli::format_inline("{operation} requires a path below the item")
    )
  }
  invisible(target)
}

onelake_require_mutable_path <- function(target, operation) {
  onelake_require_file_path(target, operation)
  pieces <- strsplit(target$path, "/", fixed = TRUE)[[1L]]
  if (length(pieces) < 2L) {
    rlang::abort(paste0(
      operation,
      " is not allowed on a Fabric-managed first-level folder"
    ))
  }
  invisible(target)
}

onelake_encode_path <- function(...) {
  values <- unlist(list(...), use.names = FALSE)
  pieces <- unlist(strsplit(values, "/", fixed = TRUE), use.names = FALSE)
  paste(
    vapply(
      pieces,
      utils::URLencode,
      character(1),
      reserved = TRUE,
      USE.NAMES = FALSE
    ),
    collapse = "/"
  )
}

onelake_path_url <- function(target) {
  values <- c(target$workspace, target$item)
  if (nzchar(target$path)) {
    values <- c(values, target$path)
  }
  paste0(target$dfs_base, "/", onelake_encode_path(values))
}

onelake_workspace_url <- function(target) {
  paste0(target$dfs_base, "/", onelake_encode_path(target$workspace))
}

onelake_request <- function(url, method = "GET", headers = list()) {
  req <- httr2::request(url) |>
    httr2::req_method(method) |>
    httr2::req_headers(`x-ms-version` = "2023-08-03")
  if (length(headers)) {
    req <- do.call(httr2::req_headers, c(list(req), headers))
  }
  req
}

onelake_if_match <- function(value) {
  if (is.null(value)) {
    return(NULL)
  }
  onelake_scalar(value, "if_match")
  if (
    identical(value, "*") ||
      grepl('^(?:W/)?"[^"]*"$', value)
  ) {
    value
  } else {
    paste0('"', value, '"')
  }
}

onelake_list_target <- function(
  target,
  credential,
  recursive = FALSE,
  page_size = 5000L
) {
  if (
    !is.logical(recursive) ||
      length(recursive) != 1L ||
      is.na(recursive)
  ) {
    rlang::abort("recursive must be TRUE or FALSE")
  }
  if (
    length(page_size) != 1L ||
      !is.numeric(page_size) ||
      is.na(page_size) ||
      !is.finite(page_size) ||
      page_size != floor(page_size)
  ) {
    rlang::abort("page_size must be one whole number between 1 and 5000")
  }
  page_size <- as.integer(page_size)
  if (page_size < 1L || page_size > 5000L) {
    rlang::abort("page_size must be one whole number between 1 and 5000")
  }

  directory <- paste(
    c(target$item, if (nzchar(target$path)) target$path),
    collapse = "/"
  )
  continuation <- NULL
  records <- list()
  repeat {
    req <- onelake_request(onelake_workspace_url(target))
    query <- list(
      req,
      resource = "filesystem",
      directory = directory,
      recursive = if (recursive) "true" else "false",
      maxResults = page_size
    )
    if (!is.null(continuation)) {
      query$continuation <- continuation
    }
    req <- do.call(httr2::req_url_query, query)
    response <- .httr2_perform(
      req,
      credential = credential,
      audience = .fabric_audience$storage
    )
    body <- httr2::resp_body_json(response, simplifyVector = FALSE)
    records <- c(records, body$paths %||% list())
    continuation <- httr2::resp_header(response, "x-ms-continuation")
    if (is.null(continuation) || !nzchar(continuation)) break
  }
  onelake_list_tibble(records, target)
}

onelake_list_tibble <- function(records, target) {
  empty <- tibble::tibble(
    path = character(),
    name = character(),
    is_directory = logical(),
    content_length = numeric(),
    etag = character(),
    last_modified = character(),
    owner = character(),
    group = character(),
    permissions = character()
  )
  if (!length(records)) {
    return(empty)
  }

  item_prefix <- paste0(target$item, "/")
  rows <- lapply(records, function(record) {
    full_path <- record$name %||% ""
    relative <- if (identical(full_path, target$item)) {
      ""
    } else if (startsWith(full_path, item_prefix)) {
      substring(full_path, nchar(item_prefix) + 1L)
    } else {
      rlang::abort("OneLake returned a path outside the requested item")
    }
    data.frame(
      path = relative,
      name = if (nzchar(relative)) basename(relative) else "",
      is_directory = onelake_directory_flag(record$isDirectory),
      content_length = suppressWarnings(as.numeric(
        record$contentLength %||% NA_real_
      )),
      etag = as.character(record$etag %||% NA_character_),
      last_modified = as.character(record$lastModified %||% NA_character_),
      owner = as.character(record$owner %||% NA_character_),
      group = as.character(record$group %||% NA_character_),
      permissions = as.character(record$permissions %||% NA_character_),
      stringsAsFactors = FALSE
    )
  })
  tibble::as_tibble(do.call(rbind, rows))
}

onelake_directory_flag <- function(value) {
  if (isTRUE(value)) {
    return(TRUE)
  }
  is.character(value) &&
    length(value) == 1L &&
    !is.na(value) &&
    identical(tolower(value), "true")
}

onelake_metadata_target <- function(
  target,
  credential,
  accepted_status = integer()
) {
  response <- .httr2_perform(
    onelake_request(onelake_path_url(target), "HEAD"),
    credential = credential,
    audience = .fabric_audience$storage,
    accepted_status = accepted_status
  )
  if (httr2::resp_status(response) >= 400L) {
    return(response)
  }
  onelake_response_metadata(response, target)
}

onelake_response_metadata <- function(
  response,
  target,
  content_length = NULL
) {
  header <- function(name) httr2::resp_header(response, name)
  length_value <- content_length %||% header("content-length")
  tibble::tibble(
    path = target$path,
    name = if (nzchar(target$path)) basename(target$path) else target$item,
    is_directory = identical(
      tolower(header("x-ms-resource-type") %||% ""),
      "directory"
    ),
    content_length = suppressWarnings(as.numeric(length_value %||% NA_real_)),
    content_type = header("content-type") %||% NA_character_,
    etag = header("etag") %||% NA_character_,
    last_modified = header("last-modified") %||% NA_character_,
    content_range = header("content-range") %||% NA_character_,
    request_id = header("x-ms-request-id") %||% NA_character_
  )
}

onelake_validate_range <- function(range) {
  if (is.null(range)) {
    return(NULL)
  }
  if (
    !is.numeric(range) ||
      !length(range) %in% c(1L, 2L) ||
      anyNA(range) ||
      !all(is.finite(range)) ||
      any(range < 0) ||
      any(range != floor(range)) ||
      (length(range) == 2L && range[[2L]] < range[[1L]])
  ) {
    rlang::abort(
      "range must contain one or two non-negative whole byte offsets"
    )
  }
  paste0(
    "bytes=",
    format(range[[1L]], scientific = FALSE, trim = TRUE),
    "-",
    if (length(range) == 2L) {
      format(range[[2L]], scientific = FALSE, trim = TRUE)
    } else {
      ""
    }
  )
}

onelake_download_target <- function(
  target,
  credential,
  dest = NULL,
  range = NULL,
  overwrite = FALSE,
  if_match = NULL
) {
  range_header <- onelake_validate_range(range)
  headers <- list()
  if (!is.null(range_header)) {
    headers$Range <- range_header
  }
  if (!is.null(if_match)) {
    headers[["If-Match"]] <- onelake_if_match(if_match)
  }
  req <- onelake_request(onelake_path_url(target), headers = headers)

  if (is.null(dest)) {
    response <- .httr2_perform(
      req,
      credential = credential,
      audience = .fabric_audience$storage
    )
    return(httr2::resp_body_raw(response))
  }
  onelake_scalar(dest, "dest")
  if (
    !is.logical(overwrite) ||
      length(overwrite) != 1L ||
      is.na(overwrite)
  ) {
    rlang::abort("overwrite must be TRUE or FALSE")
  }
  if (file.exists(dest) && !overwrite) {
    rlang::abort("Destination already exists; set overwrite = TRUE")
  }
  dir.create(dirname(dest), recursive = TRUE, showWarnings = FALSE)
  temporary <- tempfile(".fabricqueryr-download-", tmpdir = dirname(dest))
  on.exit(unlink(temporary, force = TRUE), add = TRUE)
  .httr2_perform(
    req,
    credential = credential,
    audience = .fabric_audience$storage,
    download_path = temporary
  )
  if (file.exists(dest) && !unlink(dest)) {
    rlang::abort("Could not replace the existing destination")
  }
  if (!file.rename(temporary, dest)) {
    rlang::abort("Could not commit the downloaded file")
  }
  invisible(normalizePath(dest, winslash = "/", mustWork = TRUE))
}

onelake_upload_source <- function(source) {
  if (is.raw(source)) {
    return(list(kind = "raw", value = source, size = length(source)))
  }
  onelake_scalar(source, "source")
  if (!file.exists(source) || dir.exists(source)) {
    rlang::abort(
      "source must be a raw vector or an existing local file"
    )
  }
  size <- file.info(source)$size
  if (is.na(size)) {
    rlang::abort("Could not determine source file size")
  }
  list(kind = "file", value = source, size = as.numeric(size))
}

onelake_create_parents <- function(target, credential) {
  pieces <- strsplit(target$path, "/", fixed = TRUE)[[1L]]
  if (length(pieces) <= 2L) {
    return(invisible(TRUE))
  }
  parent_paths <- vapply(
    seq.int(2L, length(pieces) - 1L),
    function(index) paste(pieces[seq_len(index)], collapse = "/"),
    character(1)
  )
  for (parent_path in parent_paths) {
    parent <- target
    parent$path <- parent_path
    status <- onelake_metadata_target(
      parent,
      credential,
      accepted_status = 404L
    )
    if (!inherits(status, "httr2_response")) {
      next
    }
    req <- onelake_request(onelake_path_url(parent), "PUT") |>
      httr2::req_url_query(resource = "directory") |>
      httr2::req_body_raw(raw())
    .httr2_perform(
      req,
      credential = credential,
      audience = .fabric_audience$storage
    )
  }
  invisible(TRUE)
}

onelake_upload_target <- function(
  target,
  credential,
  source,
  overwrite,
  if_match,
  chunk_size,
  content_type,
  create_parents
) {
  upload <- onelake_upload_source(source)
  chunk_size <- onelake_upload_chunk_size(chunk_size)
  if (!is.null(content_type)) {
    onelake_scalar(content_type, "content_type")
  }
  if (
    !is.logical(create_parents) ||
      length(create_parents) != 1L ||
      is.na(create_parents)
  ) {
    rlang::abort("create_parents must be TRUE or FALSE")
  }
  if (create_parents) {
    onelake_create_parents(target, credential)
  }

  temporary <- onelake_upload_temporary_target(target)
  committed <- FALSE
  temporary_created <- FALSE
  on.exit(
    if (temporary_created && !committed) {
      try(
        onelake_delete_target(temporary, credential),
        silent = TRUE
      )
    },
    add = TRUE
  )

  headers <- list(`If-None-Match` = "*")
  if (!is.null(content_type)) {
    headers[["x-ms-content-type"]] <- content_type
  }
  create <- onelake_request(
    onelake_path_url(temporary),
    "PUT",
    headers = headers
  ) |>
    httr2::req_url_query(resource = "file") |>
    httr2::req_body_raw(raw())
  .httr2_perform(
    create,
    credential = credential,
    audience = .fabric_audience$storage,
    idempotent = FALSE
  )
  temporary_created <- TRUE

  onelake_upload_chunks(upload, chunk_size, function(bytes, position) {
    append <- onelake_request(onelake_path_url(temporary), "PATCH") |>
      httr2::req_url_query(
        action = "append",
        position = format(position, scientific = FALSE, trim = TRUE)
      ) |>
      httr2::req_body_raw(bytes)
    .httr2_perform(
      append,
      credential = credential,
      audience = .fabric_audience$storage,
      idempotent = FALSE
    )
  })

  flush <- onelake_request(onelake_path_url(temporary), "PATCH") |>
    httr2::req_url_query(
      action = "flush",
      position = format(upload$size, scientific = FALSE, trim = TRUE),
      close = "true"
    ) |>
    httr2::req_body_raw(raw())
  .httr2_perform(
    flush,
    credential = credential,
    audience = .fabric_audience$storage,
    idempotent = FALSE
  )

  rename_headers <- list(
    `x-ms-rename-source` = onelake_rename_source(temporary)
  )
  if (!overwrite) {
    rename_headers[["If-None-Match"]] <- "*"
  }
  if (!is.null(if_match)) {
    rename_headers[["If-Match"]] <- onelake_if_match(if_match)
  }
  rename <- onelake_request(
    onelake_path_url(target),
    "PUT",
    headers = rename_headers
  ) |>
    httr2::req_url_query(mode = "posix") |>
    httr2::req_body_raw(raw())
  response <- .httr2_perform(
    rename,
    credential = credential,
    audience = .fabric_audience$storage,
    idempotent = FALSE
  )
  committed <- TRUE
  onelake_response_metadata(response, target, content_length = upload$size)
}

onelake_upload_chunk_size <- function(value) {
  if (
    length(value) != 1L ||
      is.na(value) ||
      !is.numeric(value) ||
      !is.finite(value) ||
      value < 1 ||
      value > 100 * 1024^2 ||
      value != floor(value)
  ) {
    rlang::abort(
      "chunk_size must be one whole number between 1 and 104857600 bytes"
    )
  }
  as.numeric(value)
}

onelake_upload_chunks <- function(upload, chunk_size, callback) {
  if (upload$size == 0) {
    return(invisible(TRUE))
  }
  if (identical(upload$kind, "raw")) {
    position <- 0
    while (position < upload$size) {
      end <- min(position + chunk_size, upload$size)
      callback(upload$value[seq.int(position + 1, end)], position)
      position <- end
    }
    return(invisible(TRUE))
  }

  connection <- file(upload$value, open = "rb")
  on.exit(close(connection), add = TRUE)
  position <- 0
  while (position < upload$size) {
    bytes <- readBin(
      connection,
      what = "raw",
      n = min(chunk_size, upload$size - position)
    )
    if (!length(bytes)) {
      rlang::abort("Local upload source ended before its reported size")
    }
    callback(bytes, position)
    position <- position + length(bytes)
  }
  invisible(TRUE)
}

onelake_upload_temporary_target <- function(target) {
  temporary <- target
  parent <- dirname(target$path)
  temporary_name <- basename(tempfile(".fabricqueryr-upload-"))
  temporary$path <- paste(
    c(if (!identical(parent, ".")) parent, temporary_name),
    collapse = "/"
  )
  temporary
}

onelake_rename_source <- function(target) {
  paste0(
    "/",
    onelake_encode_path(c(target$workspace, target$item, target$path))
  )
}

onelake_delete_target <- function(
  target,
  credential,
  recursive = FALSE,
  if_match = NULL
) {
  if (
    !is.logical(recursive) ||
      length(recursive) != 1L ||
      is.na(recursive)
  ) {
    rlang::abort("recursive must be TRUE or FALSE")
  }
  headers <- list()
  if (!is.null(if_match)) {
    headers[["If-Match"]] <- onelake_if_match(if_match)
  }
  continuation <- NULL
  repeat {
    req <- onelake_request(
      onelake_path_url(target),
      "DELETE",
      headers = headers
    )
    query <- list(req, recursive = if (recursive) "true" else "false")
    if (recursive) {
      query$paginated <- "true"
    }
    if (!is.null(continuation)) {
      query$continuation <- continuation
    }
    req <- do.call(httr2::req_url_query, query)
    response <- .httr2_perform(
      req,
      credential = credential,
      audience = .fabric_audience$storage
    )
    continuation <- httr2::resp_header(response, "x-ms-continuation")
    if (is.null(continuation) || !nzchar(continuation)) break
  }
  invisible(TRUE)
}
