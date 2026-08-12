#' Work with files in Microsoft Fabric OneLake
#'
#' @description
#' List, inspect, download, upload, and delete ordinary files stored in OneLake.
#' These helpers are intended for files such as CSV, JSON, images, and model
#' artifacts in a Fabric item's `Files/` area.
#'
#' - `fabric_onelake_list()` lists paths and follows all continuation tokens.
#' - `fabric_onelake_metadata()` returns file or directory properties.
#' - `fabric_onelake_download()` reads a file into memory or streams it to disk.
#' - `fabric_onelake_upload()` creates or replaces a file.
#' - `fabric_onelake_delete()` explicitly deletes a file or directory.
#'
#' @section Choosing a target:
#' The easiest inputs are a workspace plus an item returned by
#' [fabric_lakehouses()]. You can also use names, IDs, or a complete OneLake
#' HTTPS/ABFSS path. When using an item name, include its type suffix, such as
#' `"Sales.Lakehouse"`, or supply `item_type`.
#'
#' A Lakehouse's `Tables/` area is managed as Delta tables. Use
#' [fabric_onelake_read_delta_table()] to read those tables, and use SQL, Spark,
#' or another Delta-aware tool to change them. Uploading or deleting individual
#' files below `Tables/` can damage a table and is blocked by default.
#'
#' @section Permissions:
#' The signed-in user or application needs access through a workspace role or
#' the item's **Manage OneLake data access** roles. Uploading and deleting need
#' write permission. Your Fabric administrator must also allow external apps to
#' access OneLake. If a call returns HTTP 403 after sign-in succeeds, check both
#' that tenant setting and the item's data permissions.
#'
#' @section Safe file replacement:
#' Existing files are protected unless `overwrite = TRUE`. Uploads and downloads
#' are staged before replacing their destination, so an interrupted transfer
#' does not normally leave a partial file. Use `if_match` when a file should be
#' replaced only if it has not changed since you inspected it.
#'
#' @param workspace Workspace name, ID, record from [fabric_workspaces()], or a
#'   complete OneLake HTTPS/ABFSS path.
#' @param item Item name, GUID, or discovered Fabric item. Use `NULL` when
#'   `workspace` is a complete OneLake path. An item from [fabric_lakehouses()] is
#'   the least ambiguous input.
#' @param path Path relative to the item, usually beginning with `Files/` or
#'   `Tables/`, for example `"Files/incoming/data.csv"`. Use forward slashes.
#'   A complete OneLake path already contains this value.
#' @param recursive For listing, whether to include all descendants. For
#'   deletion, whether a non-empty directory may be removed.
#' @param page_size Maximum paths requested from OneLake per API call, from 1 to
#'   5000. Smaller values reduce each response size but require more requests.
#' @param begin_from Optional path at which to begin a listing. Use this to
#'   resume a long, alphabetically ordered scan.
#' @param item_type Optional Fabric item type appended to an item name unless
#'   that name already ends in the same suffix, for example `"Lakehouse"`.
#'   Usually unnecessary for a discovered item or a name such as
#'   `"Sales.Lakehouse"`.
#' @param tenant_id Entra tenant ID. Defaults to
#'   `FABRICQUERYR_TENANT_ID`.
#' @param client_id Entra application ID. Defaults to
#'   `FABRICQUERYR_CLIENT_ID`, then the Azure CLI application ID.
#' @param token Optional access token or token-provider function. Leave `NULL`
#'   to let fabricQueryR use its normal sign-in flow.
#' @param auth_args Additional sign-in options passed to
#'   [AzureAuth::get_azure_token()].
#' @param dfs_base OneLake service address. Most users should keep the default;
#'   a workspace-specific address discovered from Fabric is used when available.
#' @param range Optional inclusive zero-based byte range. Supply one value for
#'   all bytes from that offset onward, or two values for `start` through `end`.
#'   Leave `NULL` to download the entire file.
#' @param dest Optional local destination. When `NULL`, download returns a raw
#'   vector held in R memory. Supply a path to stream large files to disk. A
#'   destination download is staged before it replaces an existing file.
#' @param overwrite Whether an existing local or OneLake file may be replaced.
#'   Existing files are protected by default.
#' @param if_match Optional file version (`etag`) returned by
#'   [fabric_onelake_metadata()]. The operation proceeds only if the file still
#'   has that version.
#' @param source Local file path or raw vector to upload. A path is streamed;
#'   a raw vector is already held in memory.
#' @param chunk_size Upload chunk size in bytes. The default suits most files;
#'   larger values make fewer requests but use more memory.
#' @param content_type Optional MIME type stored with an uploaded file, for
#'   example `"text/csv"`.
#' @param create_parents Logical. Create missing parent directories below the
#'   Fabric-managed first-level folder. Keep `TRUE` for normal uploads.
#' @param allow_managed_tables Whether to allow direct changes below `Tables/`.
#'   Keep `FALSE` for normal use: changing Delta files directly can corrupt a
#'   managed table.
#' @param confirm Safety switch that must be explicitly set to `TRUE` before
#'   deletion is attempted.
#'
#' @return `fabric_onelake_list()` returns one row per path, including its
#'   item-relative `path`, file `name`, `is_directory`, `content_length`,
#'   `etag`, and modification/permission fields.
#'   `fabric_onelake_metadata()` and `fabric_onelake_upload()` return a one-row
#'   tibble with the resolved path and available HTTP metadata.
#'   `fabric_onelake_download()` returns a raw vector when `dest = NULL`, or
#'   invisibly returns the destination path after writing to disk.
#'   `fabric_onelake_delete()` invisibly returns `TRUE`.
#'
#' @references
#' [Connect to OneLake with ADLS APIs](https://learn.microsoft.com/en-us/fabric/onelake/onelake-access-api)
#'
#' [ADLS Gen2 List Paths](https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/list)
#'
#' [Create and manage OneLake security roles](https://learn.microsoft.com/en-us/fabric/onelake/security/create-manage-roles)
#'
#' [OneLake security best practices](https://learn.microsoft.com/en-us/fabric/onelake/security/best-practices-secure-data-in-onelake)
#'
#' [OneLake tenant settings](https://learn.microsoft.com/en-us/fabric/admin/service-admin-portal-onelake)
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
  begin_from = NULL,
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
  dfs_base_supplied <- !missing(dfs_base)
  target <- onelake_resolve_target(
    workspace,
    item,
    path,
    item_type,
    if (dfs_base_supplied) dfs_base else NULL
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
    page_size = page_size,
    begin_from = begin_from
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
  dfs_base_supplied <- !missing(dfs_base)
  target <- onelake_resolve_target(
    workspace,
    item,
    path,
    item_type,
    if (dfs_base_supplied) dfs_base else NULL
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
  dfs_base_supplied <- !missing(dfs_base)
  target <- onelake_resolve_target(
    workspace,
    item,
    path,
    item_type,
    if (dfs_base_supplied) dfs_base else NULL
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
  allow_managed_tables = FALSE,
  chunk_size = getOption(
    "fabricqueryr.onelake.chunk_size",
    8 * 1024^2
  )
) {
  # 1 Resolve and validate the target --------------------------------------------------------------

  # Managed Tables paths are protected by default because direct file changes
  # can bypass the Delta transaction protocol.
  dfs_base_supplied <- !missing(dfs_base)
  target <- onelake_resolve_target(
    workspace,
    item,
    path,
    item_type,
    if (dfs_base_supplied) dfs_base else NULL
  )
  onelake_require_mutable_path(
    target,
    "upload",
    allow_managed_tables = allow_managed_tables
  )
  if (!is.logical(overwrite) || length(overwrite) != 1L || is.na(overwrite)) {
    rlang::abort("overwrite must be TRUE or FALSE")
  }
  if (!is.null(if_match) && !isTRUE(overwrite)) {
    rlang::abort("if_match requires overwrite = TRUE")
  }

  # 2 Upload and return metadata -------------------------------------------------------------------

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
  dfs_base = "https://onelake.dfs.fabric.microsoft.com",
  allow_managed_tables = FALSE
) {
  # 1 Resolve and validate the target --------------------------------------------------------------

  dfs_base_supplied <- !missing(dfs_base)
  target <- onelake_resolve_target(
    workspace,
    item,
    path,
    item_type,
    if (dfs_base_supplied) dfs_base else NULL
  )
  onelake_require_mutable_path(
    target,
    "delete",
    allow_managed_tables = allow_managed_tables
  )

  # 2 Require explicit deletion confirmation -------------------------------------------------------

  if (!isTRUE(confirm)) {
    rlang::abort(
      "Deletion is disabled by default; set confirm = TRUE explicitly"
    )
  }

  # 3 Delete the target ----------------------------------------------------------------------------

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

# Resolve IDs, names, discovery records, or a full URI into one OneLake target.
# Returns normalized workspace, item, path, and trusted DFS base fields.
onelake_resolve_target <- function(
  workspace,
  item = NULL,
  path = "",
  item_type = NULL,
  dfs_base = NULL
) {
  # 1 Parse a complete OneLake URI -----------------------------------------------------------------

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

  # 2 Resolve workspace and item values ------------------------------------------------------------

  # Discovery records may carry IDs and a workspace-specific DFS endpoint.
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
  onelake_segment(workspace_value, "workspace")
  onelake_segment(item_value, "item")
  onelake_scalar(path, "path", allow_empty = TRUE)

  # 3 Validate the name or GUID form ---------------------------------------------------------------

  workspace_guid <- fabric_is_guid(workspace_value)
  item_guid <- fabric_is_guid(item_value)
  if (!identical(workspace_guid, item_guid)) {
    rlang::abort(
      "OneLake requires workspace and item GUIDs to be used together"
    )
  }
  if (!item_guid) {
    if (!is.null(item_type)) {
      onelake_segment(item_type, "item_type")
      suffix <- paste0(".", item_type)
      known_suffix <- if (
        grepl("\\.lakehouse$", item_value, ignore.case = TRUE)
      ) {
        "Lakehouse"
      } else if (grepl("\\.warehouse$", item_value, ignore.case = TRUE)) {
        "Warehouse"
      } else {
        NULL
      }
      if (
        !is.null(known_suffix) &&
          !identical(tolower(known_suffix), tolower(item_type))
      ) {
        rlang::abort("item_type conflicts with the item's existing type suffix")
      }
      if (!endsWith(tolower(item_value), tolower(suffix))) {
        item_value <- paste0(item_value, ".", item_type)
      }
    } else if (!grepl("\\.[^.]+$", item_value)) {
      rlang::abort(
        "A name-based item needs its type suffix or item_type"
      )
    }
  }

  # 4 Build the normalized target ------------------------------------------------------------------

  if (is.null(dfs_base)) {
    dfs_base <- onelake_record_dfs_endpoint(workspace_record) %||%
      onelake_record_dfs_endpoint(item_record) %||%
      "https://onelake.dfs.fabric.microsoft.com"
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

# Read a workspace-specific DFS endpoint from `record`. Returns URL text or
# `NULL` so target resolution can fall back to the public OneLake endpoint.
onelake_record_dfs_endpoint <- function(record) {
  if (is.null(record)) {
    return(NULL)
  }
  endpoint <- fabric_record_value(
    record,
    "workspaceOneLakeDfsEndpoint",
    "oneLakeDfsEndpoint",
    "dfsEndpoint",
    "dfs_endpoint"
  )
  if (!is.null(endpoint)) {
    return(endpoint)
  }
  endpoints <- record$oneLakeEndpoints %||%
    record$one_lake_endpoints %||%
    record$workspaceOneLakeEndpoints
  if (is.list(endpoints)) {
    endpoints$dfsEndpoint %||% endpoints$dfs_endpoint
  } else {
    NULL
  }
}

# Parse and validate a full HTTPS, ABFS, or ABFSS OneLake `uri`. Returns a
# normalized target used by every file operation.
onelake_parse_uri <- function(uri) {
  # 1 Validate URI structure -----------------------------------------------------------------------

  parsed <- httr2::url_parse(uri)
  scheme <- tolower(parsed$scheme %||% "")
  if (!scheme %in% c("https", "abfs", "abfss")) {
    rlang::abort("OneLake paths must use HTTPS, ABFS, or ABFSS")
  }
  onelake_validate_host(parsed$hostname)
  if (
    scheme == "https" &&
      (nzchar(parsed$username %||% "") || nzchar(parsed$password %||% ""))
  ) {
    rlang::abort("A OneLake HTTPS path must not include user information")
  }
  if (scheme != "https" && nzchar(parsed$password %||% "")) {
    rlang::abort("An ABFS path must not include a password")
  }
  default_port <- if (scheme == "abfs") "80" else "443"
  if (!is.null(parsed$port) && !identical(parsed$port, default_port)) {
    rlang::abort("A OneLake path must use its default port")
  }
  if (!is.null(parsed$query) || !is.null(parsed$fragment)) {
    rlang::abort(
      "A OneLake path must not contain a query string or fragment"
    )
  }

  # 2 Read workspace, item, and path ---------------------------------------------------------------

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

  # 3 Return the normalized target -----------------------------------------------------------------

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

# Check `value` as one string identified by `name`, optionally empty. Returns
# invisibly for shared OneLake argument validation.
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

# Check `value` as one safe URI path segment. Returns invisibly for OneLake
# workspace, item, and type inputs.
onelake_segment <- function(value, name) {
  onelake_scalar(value, name)
  if (grepl("[/\\\\]", value)) {
    rlang::abort(paste0(name, " must be exactly one URI path segment"))
  }
  invisible(value)
}

# Validate a OneLake DFS `endpoint` and its trust boundary. Returns invisibly
# before storage credentials can be sent there.
onelake_validate_endpoint <- function(endpoint) {
  onelake_scalar(endpoint, "dfs_base")
  parsed <- httr2::url_parse(endpoint)
  if (!identical(tolower(parsed$scheme %||% ""), "https")) {
    rlang::abort("dfs_base must use HTTPS")
  }
  if (
    nzchar(parsed$username %||% "") ||
      nzchar(parsed$password %||% "")
  ) {
    rlang::abort("dfs_base must not include user information")
  }
  onelake_validate_host(parsed$hostname)
  if (!is.null(parsed$port) && !identical(parsed$port, "443")) {
    rlang::abort("dfs_base must use the default HTTPS port (443)")
  }
  if (
    !identical(parsed$path %||% "", "") &&
      !identical(parsed$path %||% "", "/")
  ) {
    rlang::abort("dfs_base must not include a path")
  }
  if (!is.null(parsed$query) || !is.null(parsed$fragment)) {
    rlang::abort("dfs_base must not include a query string or fragment")
  }
  invisible(endpoint)
}

# Check `host` against Microsoft Fabric OneLake domains. Returns normalized host
# text or raises before any authenticated storage request.
onelake_validate_host <- function(host) {
  host <- tolower(host %||% "")
  valid <- grepl("(^|\\.)dfs\\.fabric\\.microsoft\\.com$", host) ||
    grepl("(^|[-.])api\\.onelake\\.fabric\\.microsoft\\.com$", host)
  if (!valid) {
    rlang::abort("The endpoint is not a Microsoft Fabric OneLake host")
  }
  invisible(host)
}

# Normalize separators in `path` and reject unsafe segments. Returns a relative
# OneLake path used to build request URLs.
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
  if (any(grepl("[\r\n]", pieces))) {
    rlang::abort("path contains a line break")
  }
  paste(pieces, collapse = "/")
}

# Require `target` to point below the item root for `operation`. Returns the
# target invisibly before file-specific work begins.
onelake_require_file_path <- function(target, operation) {
  if (!nzchar(target$path)) {
    rlang::abort(
      cli::format_inline("{operation} requires a path below the item")
    )
  }
  invisible(target)
}

# Check whether `target` is safe to change for `operation`. Returns invisibly and
# guards managed top-level folders unless explicitly allowed.
onelake_require_mutable_path <- function(
  target,
  operation,
  allow_managed_tables = FALSE
) {
  if (
    !is.logical(allow_managed_tables) ||
      length(allow_managed_tables) != 1L ||
      is.na(allow_managed_tables)
  ) {
    rlang::abort("allow_managed_tables must be TRUE or FALSE")
  }
  onelake_require_file_path(target, operation)
  pieces <- strsplit(target$path, "/", fixed = TRUE)[[1L]]
  if (length(pieces) < 2L) {
    rlang::abort(paste0(
      operation,
      " is not allowed on a Fabric-managed first-level folder"
    ))
  }
  if (
    identical(tolower(pieces[[1L]]), "tables") &&
      !isTRUE(allow_managed_tables)
  ) {
    rlang::abort(paste0(
      operation,
      " below Tables/ is blocked because direct changes can corrupt a ",
      "managed Delta table; use allow_managed_tables = TRUE only when ",
      "deliberately managing the Delta protocol"
    ))
  }
  invisible(target)
}

# URL-encode every segment supplied through `...`. Returns path text without
# allowing separators inside a segment to bypass validation.
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

# Build the full DFS URL for `target`. Returns encoded HTTPS text used by all
# OneLake requests.
onelake_path_url <- function(target) {
  values <- c(target$workspace, target$item)
  prefix <- paste0(target$dfs_base, "/", onelake_encode_path(values))
  if (!nzchar(target$path)) {
    return(prefix)
  }
  encoded_path <- target$.encoded_path %||% onelake_encode_path(target$path)
  paste0(prefix, "/", encoded_path)
}

# Build an Azure storage request from `url`, `method`, and `headers`. Returns an
# httr2 request with the shared API version applied.
onelake_request <- function(url, method = "GET", headers = list()) {
  req <- httr2::request(url) |>
    httr2::req_method(method) |>
    httr2::req_headers(`x-ms-version` = "2023-08-03")
  if (length(headers)) {
    req <- do.call(httr2::req_headers, c(list(req), headers))
  }
  req
}

# Normalize an ETag `value` for If-Match. Returns quoted header text or `*` for
# conditional download, upload, and delete operations.
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

# Read all directory entries under `target`. Returns a tibble after following
# OneLake continuation tokens with bounded pagination.
onelake_list_target <- function(
  target,
  credential,
  recursive = FALSE,
  page_size = 5000L,
  begin_from = NULL
) {
  # 1 Validate list options ------------------------------------------------------------------------

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
  if (!is.null(begin_from)) {
    begin_from <- onelake_normalize_path(begin_from)
  }

  directory <- paste(
    c(target$item, if (nzchar(target$path)) target$path),
    collapse = "/"
  )
  continuation <- NULL

  # 2 Read directory pages -------------------------------------------------------------------------

  # OneLake returns a continuation header for large listings; add the decoded
  # marker to each next request and guard against repeated URLs.
  records <- list()
  page_number <- 0L
  seen_urls <- character()
  repeat {
    workspace_url <- paste0(
      target$dfs_base,
      "/",
      onelake_encode_path(target$workspace)
    )
    req <- onelake_request(workspace_url)
    query <- list(
      req,
      resource = "filesystem",
      directory = directory,
      recursive = if (recursive) "true" else "false",
      maxResults = page_size
    )
    if (!is.null(begin_from)) {
      query$beginFrom <- begin_from
    }
    if (!is.null(continuation)) {
      query$continuation <- continuation
    }
    req <- do.call(httr2::req_url_query, query)
    page_number <- page_number + 1L
    seen_urls <- .httr2_pagination_guard(
      req$url,
      seen_urls,
      page_number
    )
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

  # 3 Return a relative-path tibble ----------------------------------------------------------------

  onelake_list_tibble(records, target)
}

# Convert OneLake list `records` into paths relative to `target`. Returns a
# stable tibble used by `fabric_onelake_list()`.
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

# Interpret a OneLake directory `value` supplied as logical or text. Returns one
# logical value for list and metadata results.
onelake_directory_flag <- function(value) {
  if (isTRUE(value)) {
    return(TRUE)
  }
  is.character(value) &&
    length(value) == 1L &&
    !is.na(value) &&
    identical(tolower(value), "true")
}

# Send a HEAD request for `target`. Returns a metadata tibble, or the accepted
# error response used when checking whether parent folders exist.
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

# Convert storage headers from `response` into a metadata tibble for `target`.
# Returns the stable shape shared by metadata, upload, and related operations.
onelake_response_metadata <- function(
  response,
  target,
  content_length = NULL
) {
  # Read header `name` from this response; returns text or `NULL` below.
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

# Validate optional byte `range` offsets. Returns a Range header string or
# `NULL` for a complete download.
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

# Download `target` into memory or atomically to `dest`. Returns raw bytes or the
# normalized destination path after validating range and overwrite behavior.
onelake_download_target <- function(
  target,
  credential,
  dest = NULL,
  range = NULL,
  overwrite = FALSE,
  if_match = NULL
) {
  # 1 Build the download request -------------------------------------------------------------------

  range_header <- onelake_validate_range(range)
  headers <- list()
  if (!is.null(range_header)) {
    headers$Range <- range_header
  }
  if (!is.null(if_match)) {
    headers[["If-Match"]] <- onelake_if_match(if_match)
  }
  req <- onelake_request(onelake_path_url(target), headers = headers)

  # 2 Return an in-memory download -----------------------------------------------------------------

  if (is.null(dest)) {
    response <- .httr2_perform(
      req,
      credential = credential,
      audience = .fabric_audience$storage
    )
    if (is.raw(response$body) && length(response$body) == 0L) {
      return(raw())
    }
    return(httr2::resp_body_raw(response))
  }

  # 3 Stage and commit a file download -------------------------------------------------------------

  # Download beside the destination so the final rename remains on one volume.
  onelake_scalar(dest, "dest")
  if (
    !is.logical(overwrite) ||
      length(overwrite) != 1L ||
      is.na(overwrite)
  ) {
    rlang::abort("overwrite must be TRUE or FALSE")
  }
  if (dir.exists(dest)) {
    rlang::abort("Destination is a directory; supply a file path")
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
  onelake_commit_download(temporary, dest, overwrite = overwrite)
  invisible(normalizePath(dest, winslash = "/", mustWork = TRUE))
}

# Commit staged `temporary` content to `dest`, preserving an existing file when
# replacement fails. Returns invisibly after a safe local handoff.
onelake_commit_download <- function(temporary, dest, overwrite) {
  # 1 Recheck the destination ----------------------------------------------------------------------

  # State can change while the remote file downloads, so validate it again.
  if (dir.exists(dest)) {
    rlang::abort("Destination is a directory; supply a file path")
  }
  if (file.exists(dest) && !overwrite) {
    rlang::abort("Destination already exists; set overwrite = TRUE")
  }
  if (!file.exists(dest)) {
    if (!overwrite) {
      return(onelake_commit_new_download(temporary, dest))
    }
    if (.onelake_file_rename(temporary, dest)) {
      return(invisible(TRUE))
    }
    if (dir.exists(dest)) {
      rlang::abort("Destination became a directory during download")
    }
    if (file.exists(dest)) {
      return(onelake_commit_download(temporary, dest, overwrite = TRUE))
    }
    rlang::abort("Could not commit the downloaded file")
  }

  # 2 Protect and replace an existing file ---------------------------------------------------------

  backup <- tempfile(".fabricqueryr-backup-", tmpdir = dirname(dest))
  if (!.onelake_file_rename(dest, backup)) {
    rlang::abort(
      "Could not protect the existing destination before replacing it"
    )
  }
  if (.onelake_file_rename(temporary, dest)) {
    if (unlink(backup, force = TRUE) != 0L) {
      rlang::warn(c(
        "Downloaded file replaced the destination, but its backup could not be removed.",
        "i" = cli::format_inline("The backup remains at {.path {backup}}.")
      ))
    }
    return(invisible(TRUE))
  }

  restored <- .onelake_file_rename(backup, dest)
  if (!restored) {
    rlang::abort(c(
      "Could not commit the downloaded file or restore the destination.",
      "i" = cli::format_inline(
        "The original file remains at {.path {backup}}."
      )
    ))
  }
  rlang::abort(
    "Could not commit the downloaded file; the original destination was restored"
  )
}

# Commit a staged download only when `dest` is still absent. Returns invisibly
# after linking or exclusive-copying the file without overwriting a race winner.
onelake_commit_new_download <- function(temporary, dest) {
  # 1 Try an atomic hard link ----------------------------------------------------------------------

  if (.onelake_file_link(temporary, dest)) {
    unlink(temporary, force = TRUE)
    return(invisible(TRUE))
  }
  if (dir.exists(dest)) {
    rlang::abort("Destination became a directory during download")
  }
  if (file.exists(dest)) {
    rlang::abort("Destination already exists; set overwrite = TRUE")
  }

  # 2 Fall back to exclusive copy ------------------------------------------------------------------

  # Exclusive creation keeps overwrite = FALSE safe even across filesystems.
  destination <- tryCatch(
    file(dest, open = "wxb"),
    error = identity
  )
  if (inherits(destination, "error")) {
    if (dir.exists(dest)) {
      rlang::abort("Destination became a directory during download")
    }
    if (file.exists(dest)) {
      rlang::abort("Destination already exists; set overwrite = TRUE")
    }
    rlang::abort("Could not commit the downloaded file", parent = destination)
  }
  source <- file(temporary, open = "rb")
  committed <- FALSE
  on.exit(
    {
      try(close(source), silent = TRUE)
      try(close(destination), silent = TRUE)
      if (!committed) {
        unlink(dest, force = TRUE)
      }
    },
    add = TRUE
  )
  repeat {
    chunk <- readBin(source, what = "raw", n = 8L * 1024L * 1024L)
    if (!length(chunk)) {
      break
    }
    writeBin(chunk, destination)
  }
  close(source)
  close(destination)
  committed <- TRUE
  unlink(temporary, force = TRUE)
  invisible(TRUE)
}

# Rename local `from` to `to`. Returns the base-R result and remains a small test
# seam for race and rollback behavior in atomic downloads.
.onelake_file_rename <- function(from, to) {
  file.rename(from, to)
}

# Hard-link local `from` to absent `to`. Returns the base-R result and remains a
# test seam for the no-overwrite download path.
.onelake_file_link <- function(from, to) {
  file.link(from, to)
}

# Normalize raw bytes or a local file `source`. Returns its kind, value, and size
# for chunked upload processing.
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

# Create missing parent directories for `target`. Returns invisibly and is used
# before uploads when `create_parents` is enabled.
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

# Stage, append, flush, and atomically rename an upload to `target`. Returns a
# metadata tibble only after the destination commit succeeds.
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
  # 1 Validate source and upload settings ----------------------------------------------------------

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

  # 2 Create a unique staging file -----------------------------------------------------------------

  # Upload away from the destination so partial content is never exposed there.
  temporary <- onelake_upload_temporary_target(target)
  committed <- FALSE
  temporary_may_exist <- FALSE
  on.exit(
    if (temporary_may_exist && !committed) {
      try(
        onelake_delete_target(
          temporary,
          credential,
          is_directory = FALSE
        ),
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
  # A transport failure can occur after OneLake commits the create. Mark the
  # unique staging path before transmission so cleanup covers that ambiguity.
  temporary_may_exist <- TRUE
  .httr2_perform(
    create,
    credential = credential,
    audience = .fabric_audience$storage,
    idempotent = FALSE
  )

  # 3 Append all source chunks ---------------------------------------------------------------------

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

  # 4 Flush the staging file -----------------------------------------------------------------------

  flush_headers <- list()
  if (!is.null(content_type)) {
    flush_headers[["x-ms-content-type"]] <- content_type
  }
  flush <- onelake_request(
    onelake_path_url(temporary),
    "PATCH",
    headers = flush_headers
  ) |>
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

  # 5 Commit the upload ----------------------------------------------------------------------------

  # Rename is the first operation that exposes the complete file at its target.
  rename_headers <- list(
    `x-ms-rename-source` = paste0(
      "/",
      onelake_encode_path(c(
        temporary$workspace,
        temporary$item,
        temporary$path
      ))
    )
  )
  if (!is.null(content_type)) {
    rename_headers[["x-ms-content-type"]] <- content_type
  }
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

# Validate upload chunk-size `value`. Returns bytes as a number within OneLake's
# supported client limit.
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

# Read `upload` in `chunk_size` pieces and call `callback` with bytes and offset.
# Returns invisibly after raw or file input is fully consumed.
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

# Create a unique staging target beside `target`. Returns a copied OneLake target
# used for failure-safe upload cleanup.
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

# Delete a file or directory `target`, following recursive continuation pages.
# Returns invisibly after all accepted delete requests complete.
onelake_delete_target <- function(
  target,
  credential,
  recursive = FALSE,
  if_match = NULL,
  is_directory = NULL
) {
  # 1 Validate the target type and options ---------------------------------------------------------

  if (
    !is.logical(recursive) ||
      length(recursive) != 1L ||
      is.na(recursive)
  ) {
    rlang::abort("recursive must be TRUE or FALSE")
  }
  if (is.null(is_directory)) {
    metadata <- onelake_metadata_target(target, credential)
    is_directory <- metadata$is_directory[[1L]]
  }
  if (
    !is.logical(is_directory) ||
      length(is_directory) != 1L ||
      is.na(is_directory)
  ) {
    rlang::abort("is_directory must be TRUE or FALSE")
  }
  headers <- list()
  if (!is.null(if_match)) {
    headers[["If-Match"]] <- onelake_if_match(if_match)
  }

  # 2 Delete every continuation page ---------------------------------------------------------------

  # Recursive OneLake deletion can require several requests; reject repeated
  # request URLs so a malformed continuation cannot loop forever.
  continuation <- NULL
  page_number <- 0L
  seen_urls <- character()
  repeat {
    req <- onelake_request(
      onelake_path_url(target),
      "DELETE",
      headers = headers
    )
    query <- list(req)
    if (is_directory) {
      query$recursive <- if (recursive) "true" else "false"
    }
    if (is_directory && recursive) {
      query$paginated <- "true"
    }
    if (!is.null(continuation)) {
      query$continuation <- continuation
    }
    req <- do.call(httr2::req_url_query, query)
    page_number <- page_number + 1L
    seen_urls <- .httr2_pagination_guard(
      req$url,
      seen_urls,
      page_number
    )
    response <- .httr2_perform(
      req,
      credential = credential,
      audience = .fabric_audience$storage,
      accepted_status = 404L
    )
    continuation <- httr2::resp_header(response, "x-ms-continuation")
    if (is.null(continuation) || !nzchar(continuation)) break
  }
  invisible(TRUE)
}
