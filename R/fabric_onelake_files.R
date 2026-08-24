#' Work with files in Microsoft Fabric OneLake
#'
#' @description
#' List, inspect, download, upload, and delete ordinary files stored in OneLake
#' These helpers are intended for files such as CSV, JSON, images, and model
#' artifacts in a Fabric item's `Files/` area
#'
#' - `fabric_onelake_list()` lists paths and follows all continuation tokens
#' - `fabric_onelake_metadata()` returns file or directory properties
#' - `fabric_onelake_download()` reads a file into memory or streams it to disk
#' - `fabric_onelake_upload()` creates or replaces a file
#' - `fabric_onelake_delete()` explicitly deletes a file or directory
#'
#' @section Choosing a target:
#' The easiest inputs are a workspace plus an item returned by
#' [fabric_lakehouses()]. You can also use names, IDs, or a complete OneLake
#' HTTPS/ABFSS path. When using an item name, include its type suffix, such as
#' `"Sales.Lakehouse"`, or supply `item_type`
#'
#' A Lakehouse's `Tables/` area is managed as Delta tables. Use
#' [fabric_onelake_read_delta_table()] to read those tables, and use SQL, Spark,
#' or another Delta-aware tool to change them. Uploading or deleting individual
#' files below `Tables/` can damage a table and is blocked by default
#'
#' @section Permissions:
#' The signed-in user or application needs access through a workspace role or
#' the item's **OneLake security roles**, configured under **Manage OneLake
#' security**. Uploading and deleting need
#' write permission. Your Fabric administrator must also allow external apps to
#' access OneLake. If a call returns HTTP 403 after sign-in succeeds, check both
#' that tenant setting and the item's data permissions
#'
#' @section Safe file replacement:
#' Existing files are protected unless `overwrite = TRUE`. Uploads and downloads
#' are staged before replacing their destination, so an interrupted transfer
#' does not normally leave a partial file. Local downloads are published with an
#' atomic same-directory rename or hard link and fail closed when the filesystem
#' cannot provide the required primitive. Use `if_match` when a OneLake file
#' should be replaced only if it has not changed since you inspected it
#'
#' @param workspace Workspace name, ID, record from [fabric_workspaces()], or a
#'   complete OneLake HTTPS/ABFSS path
#' @param item Item name, GUID, or discovered Fabric item. Use `NULL` when
#'   `workspace` is a complete OneLake path. An item from [fabric_lakehouses()] is
#'   the least ambiguous input
#' @param path Path relative to the item, usually beginning with `Files/` or
#'   `Tables/`, for example `"Files/incoming/data.csv"`. Use forward slashes
#'   A complete OneLake path already contains this value
#' @param recursive For listing, whether to include all descendants. For
#'   deletion, whether a non-empty directory may be removed
#' @param page_size Maximum paths requested from OneLake per API call, from 1 to
#'   5000. Smaller values reduce each response size but require more requests
#' @param begin_from Optional path at which to begin a listing. Use this to
#'   resume a long, alphabetically ordered scan. Non-recursive listings accept
#'   only a single path level
#' @param item_type Optional Fabric item type appended to an item name unless
#'   that name already ends in the same suffix, for example `"Lakehouse"`
#'   Usually unnecessary for a discovered item or a name such as
#'   `"Sales.Lakehouse"`
#' @param tenant_id Entra tenant ID. Defaults to
#'   `FABRICQUERYR_TENANT_ID`
#' @param client_id Entra application ID. Defaults to
#'   `FABRICQUERYR_CLIENT_ID`, then the Azure CLI application ID
#' @param token Optional access token or token-provider function. Leave `NULL`
#'   to let 'fabricQueryR' use its normal sign-in flow
#' @param auth_args Additional sign-in options passed to
#'   [AzureAuth::get_azure_token()]
#' @param dfs_base OneLake service address. Most users should keep the default;
#'   a workspace-specific address discovered from Fabric is used when available
#' @param range Optional inclusive zero-based byte range. Supply one value for
#'   all bytes from that offset onward, or two values for `start` through `end`
#'   Leave `NULL` to download the entire file. A ranged request must receive a
#'   matching HTTP 206 `Content-Range` response
#' @param dest Optional local destination. When `NULL`, download returns a raw
#'   vector held in R memory. Supply a path to stream large files to disk. A
#'   destination download is staged before it replaces an existing file
#' @param overwrite Whether an existing local or OneLake file may be replaced
#'   Existing files are protected by default
#' @param if_match Optional file version (`etag`) returned by
#'   [fabric_onelake_metadata()]. The operation proceeds only if the file still
#'   has that version
#' @param source Local file path or raw vector to upload. A path is streamed;
#'   a raw vector is already held in memory
#' @param chunk_size Upload chunk size in bytes. The default suits most files;
#'   larger values make fewer requests but use more memory
#' @param content_type Optional MIME type stored with an uploaded file, for
#'   example `"text/csv"`
#' @param create_parents Logical. Create missing parent directories below the
#'   Fabric-managed first-level folder. Keep `TRUE` for normal uploads
#' @param allow_managed_tables Whether to allow direct changes below `Tables/`
#'   Keep `FALSE` for normal use: changing Delta files directly can corrupt a
#'   managed table
#' @param confirm Safety switch that must be explicitly set to `TRUE` before
#'   deletion is attempted
#'
#' @return `fabric_onelake_list()` returns one row per path, including its
#'   item-relative `path`, file `name`, `is_directory`, `content_length`,
#'   `etag`, and modification/permission fields
#'   `fabric_onelake_metadata()` and `fabric_onelake_upload()` return a one-row
#'   tibble with the resolved path and available HTTP metadata
#'   `fabric_onelake_download()` returns a raw vector when `dest = NULL`, or
#'   invisibly returns the destination path after writing to disk
#'   `fabric_onelake_delete()` invisibly returns `TRUE`
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
#' # Discover the OneLake target instead of typing workspace and item names
#' workspace <- fabric_workspaces()[[1L]]
#' lakehouse <- fabric_lakehouses(workspace)[[1L]]
#'
#' # Create a small local CSV and upload it to the discovered Lakehouse
#' local_csv <- tempfile(fileext = ".csv")
#' write.csv(data.frame(id = 1:3), local_csv, row.names = FALSE)
#' fabric_onelake_upload(
#'   workspace,
#'   lakehouse,
#'   "Files/incoming/example.csv",
#'   source = local_csv
#' )
#'
#' # List the folder and inspect metadata for the uploaded file
#' files <- fabric_onelake_list(
#'   workspace = workspace,
#'   item = lakehouse,
#'   path = "Files/incoming",
#'   recursive = TRUE
#' )
#' metadata <- fabric_onelake_metadata(
#'   workspace,
#'   lakehouse,
#'   "Files/incoming/example.csv"
#' )
#'
#' # Download the first 100 bytes when only a file sample is needed
#' bytes <- fabric_onelake_download(
#'   workspace,
#'   lakehouse,
#'   "Files/incoming/example.csv",
#'   range = c(0, 99)
#' )
#'
#' # Deletion is explicit and requires confirm = TRUE
#' fabric_onelake_delete(
#'   workspace,
#'   lakehouse,
#'   "Files/incoming/example.csv",
#'   confirm = TRUE
#' )
#' }
#' @name fabric_onelake_files
NULL

#' Read and write R or Arrow objects in OneLake Files
#'
#' These object-aware helpers sit above [fabric_onelake_download()] and
#' [fabric_onelake_upload()]. They serialize data frames, tibbles, and lazy
#' Arrow inputs without collecting the complete object in R memory, and decode
#' supported OneLake files directly to a tibble or Arrow stream.
#'
#' @param workspace Workspace name, ID, record from [fabric_workspaces()], or a
#'   complete OneLake HTTPS/ABFSS path.
#' @param item Item name, GUID, or discovered Fabric item. Use `NULL` when
#'   `workspace` is a complete OneLake path.
#' @param path Item-relative path, normally below `Files/`.
#' @param data A data frame, tibble, Arrow Table/RecordBatch, lazy Arrow
#'   Dataset/Scanner/query, RecordBatchReader, or Arrow-compatible array stream.
#' @param format File format. `"auto"` infers `"parquet"`, `"csv"`, or
#'   `"arrow"` from the path extension.
#' @param result Return a `"tibble"` or a disk-backed, single-use
#'   `"arrow_stream"`.
#' @param overwrite Whether an existing OneLake file may be replaced.
#' @param if_match Optional destination ETag for conditional replacement.
#' @param compression Parquet compression codec passed to Arrow.
#' @param include_header Whether a written CSV includes column names.
#' @param na Text used for missing values in a written CSV.
#' @param create_parents Whether missing parent directories are created.
#' @param item_type Optional Fabric item type used to resolve a named item.
#' @param tenant_id Entra tenant ID. Defaults to
#'   `FABRICQUERYR_TENANT_ID`.
#' @param client_id Entra application ID. Defaults to
#'   `FABRICQUERYR_CLIENT_ID`, then the Azure CLI application ID.
#' @param token Optional access token or audience-aware token-provider function.
#' @param auth_args Additional sign-in options passed to
#'   [AzureAuth::get_azure_token()].
#' @param dfs_base OneLake DFS service address. A private or regional endpoint
#'   on a discovered record is preferred when this argument is omitted.
#' @param allow_managed_tables Whether direct writes below `Tables/` are
#'   permitted. Keep the safe default, `FALSE`, for managed Delta tables.
#' @param chunk_size Upload chunk size in bytes.
#'
#' @return `fabric_onelake_read_file()` returns a tibble or a disk-backed
#'   `nanoarrow_array_stream`. `fabric_onelake_write_file()` returns OneLake
#'   metadata with additional `format`, `rows`, and `columns` fields.
#' @references
#' [Connect to OneLake with ADLS APIs](https://learn.microsoft.com/en-us/fabric/onelake/onelake-access-api)
#'
#' [Get data into OneLake](https://learn.microsoft.com/en-us/fabric/onelake/quickstart-get-data)
#' @examples
#' \dontrun{
#' # Discover the Lakehouse that will store the Parquet file
#' workspace <- fabric_workspaces()[[1L]]
#' lakehouse <- fabric_lakehouses(workspace)[[1L]]
#'
#' # Serialize the R data frame directly to OneLake as Parquet
#' fabric_onelake_write_file(
#'   workspace,
#'   lakehouse,
#'   "Files/exports/orders.parquet",
#'   data.frame(id = 1:3, amount = c(10.5, NA, 30))
#' )
#'
#' # Read the same file back as a tibble
#' orders <- fabric_onelake_read_file(
#'   workspace,
#'   lakehouse,
#'   "Files/exports/orders.parquet"
#' )
#' }
#' @name fabric_onelake_object_files
NULL

#' @rdname fabric_onelake_object_files
#' @export
fabric_onelake_read_file <- function(
  workspace,
  item = NULL,
  path = "",
  format = c("auto", "parquet", "csv", "arrow"),
  result = c("tibble", "arrow_stream"),
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
  format_path <- if (
    is.character(path) && length(path) == 1L && !is.na(path) && nzchar(path)
  ) {
    path
  } else if (is.character(workspace) && length(workspace) == 1L) {
    workspace
  } else {
    path
  }
  format <- .fabric_onelake_object_format(format_path, format)
  result <- rlang::arg_match(result, c("tibble", "arrow_stream"))
  .fabric_onelake_require_arrow(result)

  local_path <- tempfile(
    "fabricqueryr-onelake-object-",
    fileext = paste0(".", format)
  )
  keep_local <- FALSE
  on.exit(
    if (!keep_local) {
      unlink(local_path, force = TRUE)
    },
    add = TRUE
  )
  fabric_onelake_download(
    workspace = workspace,
    item = item,
    path = path,
    dest = local_path,
    item_type = item_type,
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args,
    dfs_base = if (dfs_base_supplied) dfs_base else NULL
  )

  if (identical(result, "tibble")) {
    value <- tryCatch(
      switch(
        format,
        parquet = arrow::read_parquet(local_path, as_data_frame = TRUE),
        csv = arrow::read_csv_arrow(local_path, as_data_frame = TRUE),
        arrow = arrow::read_ipc_stream(local_path, as_data_frame = TRUE)
      ),
      error = function(error) {
        .fabric_abort(
          paste0("Could not decode the OneLake ", format, " file"),
          class = c("fabric_onelake_object_read_error", "fabric_error"),
          parent = error
        )
      }
    )
    return(tibble::as_tibble(value))
  }

  stream <- .fabric_onelake_object_stream(local_path, format)
  keep_local <- TRUE
  stream
}

#' @rdname fabric_onelake_object_files
#' @export
fabric_onelake_write_file <- function(
  workspace,
  item = NULL,
  path = "",
  data,
  format = c("auto", "parquet", "csv", "arrow"),
  overwrite = FALSE,
  if_match = NULL,
  compression = "snappy",
  include_header = TRUE,
  na = "",
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
  chunk_size = getOption("fabricqueryr.onelake.chunk_size", 8 * 1024^2)
) {
  dfs_base_supplied <- !missing(dfs_base)
  if (missing(data)) {
    .fabric_abort("data is required")
  }
  format_path <- if (
    is.character(path) && length(path) == 1L && !is.na(path) && nzchar(path)
  ) {
    path
  } else if (is.character(workspace) && length(workspace) == 1L) {
    workspace
  } else {
    path
  }
  format <- .fabric_onelake_object_format(format_path, format)
  .fabric_onelake_require_arrow()
  prepared <- .fabric_parquet_prepare_data(data, "fabric_onelake_write_file()")
  .fabric_parquet_column_names(prepared$names)
  .fabric_onelake_scalar_logical(include_header, "include_header")
  if (!is.character(na) || length(na) != 1L || is.na(na)) {
    .fabric_abort("na must be one character value")
  }

  local_path <- tempfile(
    "fabricqueryr-onelake-object-",
    fileext = paste0(".", format)
  )
  on.exit(unlink(local_path, force = TRUE), add = TRUE)
  serialized <- .fabric_onelake_serialize_object(
    prepared,
    local_path,
    format,
    compression,
    include_header,
    na
  )
  content_type <- switch(
    format,
    parquet = "application/vnd.apache.parquet",
    csv = "text/csv; charset=utf-8",
    arrow = "application/vnd.apache.arrow.stream"
  )
  metadata <- fabric_onelake_upload(
    workspace = workspace,
    item = item,
    path = path,
    source = local_path,
    overwrite = overwrite,
    if_match = if_match,
    content_type = content_type,
    create_parents = create_parents,
    item_type = item_type,
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args,
    dfs_base = if (dfs_base_supplied) dfs_base else NULL,
    allow_managed_tables = allow_managed_tables,
    chunk_size = chunk_size
  )
  metadata$format <- format
  metadata$rows <- serialized$rows
  metadata$columns <- list(serialized$names)
  class(metadata) <- unique(c(
    "fabric_onelake_file_write_result",
    class(metadata)
  ))
  metadata
}

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
  # Resolve discovery records, URIs, and separate path fields the same way
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

  # Downloads require a file target rather than a directory
  onelake_require_file_path(target, "download")

  # Build one storage credential for the download request
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
  # can bypass the Delta transaction protocol

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
    .fabric_abort("overwrite must be TRUE or FALSE")
  }

  if (!is.null(if_match) && !isTRUE(overwrite)) {
    .fabric_abort("if_match requires overwrite = TRUE")
  }

  # 2 Upload and return metadata -------------------------------------------------------------------

  # Upload and return metadata only after validating the source and destination

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

  # Resolve and validate the target once so later steps use one consistent value

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

  # Stop unless the caller explicitly confirms this destructive operation

  if (!isTRUE(confirm)) {
    .fabric_abort(
      "Deletion is disabled by default; set confirm = TRUE explicitly"
    )
  }

  # 3 Delete the target ----------------------------------------------------------------------------

  # Delete the target only after the target and confirmation are validated

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

# Resolve IDs, names, discovery records, or a full URI into one OneLake target
# Returns normalized workspace, item, path, and trusted DFS base fields
onelake_resolve_target <- function(
  workspace,
  item = NULL,
  path = "",
  item_type = NULL,
  dfs_base = NULL
) {
  # 1 Parse a complete OneLake URI -----------------------------------------------------------------

  # Parse the full URI before resolving separate workspace or item arguments

  if (
    is.character(workspace) &&
      length(workspace) == 1L &&
      grepl("^(?:https|abfss?)://", workspace, ignore.case = TRUE)
  ) {
    if (!is.null(item)) {
      .fabric_abort(
        "item must be NULL when workspace is a complete OneLake path"
      )
    }

    if (!identical(path, "")) {
      .fabric_abort(
        "path must be empty when workspace is a complete OneLake path"
      )
    }

    return(onelake_parse_uri(workspace))
  }

  # 2 Resolve workspace and item values ------------------------------------------------------------

  # Discovery records may carry IDs and a workspace-specific DFS endpoint

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
      .fabric_abort(
        "The discovered item belongs to a different workspace"
      )
    }
    workspace_value <- item_workspace
  }
  onelake_segment(workspace_value, "workspace")
  onelake_segment(item_value, "item")
  onelake_scalar(path, "path", allow_empty = TRUE)

  # 3 Validate the name or GUID form ---------------------------------------------------------------

  # Check the name or GUID form now so later code can rely on safe input

  workspace_guid <- fabric_is_guid(workspace_value)
  item_guid <- fabric_is_guid(item_value)
  if (!identical(workspace_guid, item_guid)) {
    .fabric_abort(
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
      } else if (
        grepl("\\.mirroreddatabase$", item_value, ignore.case = TRUE)
      ) {
        "MirroredDatabase"
      } else {
        NULL
      }

      if (
        !is.null(known_suffix) &&
          !identical(tolower(known_suffix), tolower(item_type))
      ) {
        .fabric_abort(
          "item_type conflicts with the item's existing type suffix"
        )
      }

      if (!endsWith(tolower(item_value), tolower(suffix))) {
        item_value <- paste0(item_value, ".", item_type)
      }
    } else if (!grepl("\\.[^.]+$", item_value)) {
      .fabric_abort(
        "A name-based item needs its type suffix or item_type"
      )
    }
  }

  # 4 Build the normalized target ------------------------------------------------------------------

  # Build the normalized target from the validated values required by the next step

  if (is.null(dfs_base)) {
    dfs_base <- onelake_record_dfs_endpoint(workspace_record) %||%
      onelake_record_dfs_endpoint(item_record) %||%
      "https://onelake.dfs.fabric.microsoft.com"
  }
  dfs_base <- onelake_validate_endpoint(dfs_base)
  structure(
    list(
      dfs_base = dfs_base,
      workspace = workspace_value,
      item = item_value,
      path = onelake_normalize_path(path, allow_empty = TRUE)
    ),
    class = "fabric_onelake_target"
  )
}

# Read a workspace-specific DFS endpoint from `record`. Returns URL text or
# `NULL` so target resolution can fall back to the public OneLake endpoint
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
# normalized target used by every file operation
onelake_parse_uri <- function(uri) {
  # 1 Validate URI structure -----------------------------------------------------------------------

  # Check URI structure now so later code can rely on safe input

  parsed <- httr2::url_parse(uri)
  scheme <- tolower(parsed$scheme %||% "")
  if (!scheme %in% c("https", "abfs", "abfss")) {
    .fabric_abort("OneLake paths must use HTTPS, ABFS, or ABFSS")
  }
  onelake_validate_host(parsed$hostname)
  if (
    scheme == "https" &&
      (nzchar(parsed$username %||% "") || nzchar(parsed$password %||% ""))
  ) {
    .fabric_abort("A OneLake HTTPS path must not include user information")
  }

  if (scheme != "https" && nzchar(parsed$password %||% "")) {
    .fabric_abort("An ABFS path must not include a password")
  }
  default_port <- if (scheme == "abfs") "80" else "443"
  if (!is.null(parsed$port) && !identical(parsed$port, default_port)) {
    .fabric_abort("A OneLake path must use its default port")
  }

  if (!is.null(parsed$query) || !is.null(parsed$fragment)) {
    .fabric_abort(
      "A OneLake path must not contain a query string or fragment"
    )
  }

  # 2 Read workspace, item, and path ---------------------------------------------------------------

  # Read workspace, item, and path once so later checks use a consistent view

  if (scheme == "https") {
    pieces <- strsplit(sub("^/+", "", parsed$path), "/", fixed = TRUE)[[1L]]
    host_workspace <- onelake_workspace_host_guid(parsed$hostname)
    if (!is.null(host_workspace)) {
      if (!length(pieces) || !nzchar(pieces[[1L]])) {
        .fabric_abort(
          "A workspace-specific OneLake path must include an item"
        )
      }
      first_is_workspace <- identical(
        gsub("-", "", tolower(pieces[[1L]]), fixed = TRUE),
        gsub("-", "", host_workspace, fixed = TRUE)
      )
      if (first_is_workspace) {
        if (length(pieces) < 2L) {
          .fabric_abort(
            "A workspace-specific OneLake path must include an item"
          )
        }
        item <- pieces[[2L]]
        path <- paste(utils::tail(pieces, -2L), collapse = "/")
      } else {
        item <- pieces[[1L]]
        path <- paste(utils::tail(pieces, -1L), collapse = "/")
      }
      workspace <- host_workspace
    } else {
      if (length(pieces) < 2L) {
        .fabric_abort(
          "A OneLake HTTPS path must include workspace and item"
        )
      }
      workspace <- pieces[[1L]]
      item <- pieces[[2L]]
      path <- paste(utils::tail(pieces, -2L), collapse = "/")
    }
  } else {
    workspace <- parsed$username
    pieces <- strsplit(sub("^/+", "", parsed$path), "/", fixed = TRUE)[[1L]]
    if (is.null(workspace) || !nzchar(workspace) || !length(pieces)) {
      .fabric_abort("An ABFS path must include workspace and item")
    }
    item <- pieces[[1L]]
    path <- paste(utils::tail(pieces, -1L), collapse = "/")
  }
  onelake_scalar(workspace, "workspace")
  onelake_scalar(item, "item")
  workspace_guid <- fabric_is_guid(workspace)
  item_guid <- fabric_is_guid(item)
  if (!identical(workspace_guid, item_guid)) {
    .fabric_abort(
      "OneLake requires workspace and item GUIDs to be used together"
    )
  }

  if (!item_guid && !grepl("\\.[^.]+$", item)) {
    .fabric_abort(
      "A name-based OneLake item must include its type suffix"
    )
  }

  # 3 Return the normalized target -----------------------------------------------------------------

  # Return the normalized target in the stable form expected by the caller

  structure(
    list(
      dfs_base = paste0(
        "https://",
        sub(
          "\\.blob\\.fabric\\.microsoft\\.com$",
          ".dfs.fabric.microsoft.com",
          tolower(parsed$hostname)
        )
      ),
      workspace = workspace,
      item = item,
      path = onelake_normalize_path(path, allow_empty = TRUE)
    ),
    class = "fabric_onelake_target"
  )
}

# Extract and canonicalize the workspace GUID embedded in Fabric's documented
# workspace-specific OneLake FQDN. These endpoints omit the workspace segment
# from item-scoped paths and use the GUID without dashes in the hostname
onelake_workspace_host_guid <- function(host) {
  match <- regexec(
    paste0(
      "^([0-9a-f]{32})\\.z[0-9a-f]{2}\\.",
      "(?:dfs|blob)\\.fabric\\.microsoft\\.com$"
    ),
    tolower(host %||% ""),
    perl = TRUE
  )
  pieces <- regmatches(tolower(host %||% ""), match)[[1L]]
  if (length(pieces) != 2L) {
    return(NULL)
  }
  value <- pieces[[2L]]
  paste0(
    substr(value, 1L, 8L),
    "-",
    substr(value, 9L, 12L),
    "-",
    substr(value, 13L, 16L),
    "-",
    substr(value, 17L, 20L),
    "-",
    substr(value, 21L, 32L)
  )
}

# Check `value` as one string identified by `name`, optionally empty. Returns
# invisibly for shared OneLake argument validation
onelake_scalar <- function(value, name, allow_empty = FALSE) {
  if (
    !is.character(value) ||
      length(value) != 1L ||
      is.na(value) ||
      (!allow_empty && !nzchar(value))
  ) {
    .fabric_abort(paste0(
      name,
      " must be one ",
      if (allow_empty) "" else "non-empty ",
      "character value"
    ))
  }
  invisible(value)
}

# Check `value` as one safe URI path segment. Returns invisibly for OneLake
# workspace, item, and type inputs
onelake_segment <- function(value, name) {
  onelake_scalar(value, name)
  decoded <- value
  unsafe <- FALSE
  # Check both supplied and decoded forms. Repeating the decode catches common
  # double-encoded separator/dot-segment forms before an HTTP stack or proxy can
  # normalize them into a different resource path
  for (index in seq_len(3L)) {
    unsafe <- unsafe ||
      decoded %in% c(".", "..") ||
      grepl("[/\\\\[:cntrl:]]", decoded)
    next_decoded <- utils::URLdecode(decoded)
    if (identical(next_decoded, decoded)) {
      break
    }
    decoded <- next_decoded
  }
  unsafe <- unsafe ||
    decoded %in% c(".", "..") ||
    grepl("[/\\\\[:cntrl:]]", decoded)
  if (unsafe) {
    .fabric_abort(paste0(name, " must be exactly one URI path segment"))
  }
  invisible(value)
}

# Validate and normalize a OneLake DFS `endpoint` and its trust boundary.
# Returns its canonical HTTPS origin before credentials can be sent there.
onelake_validate_endpoint <- function(endpoint) {
  onelake_scalar(endpoint, "dfs_base")
  parsed <- httr2::url_parse(endpoint)
  if (!identical(tolower(parsed$scheme %||% ""), "https")) {
    .fabric_abort("dfs_base must use HTTPS")
  }

  if (
    nzchar(parsed$username %||% "") ||
      nzchar(parsed$password %||% "")
  ) {
    .fabric_abort("dfs_base must not include user information")
  }
  onelake_validate_host(parsed$hostname)
  if (!is.null(parsed$port) && !identical(parsed$port, "443")) {
    .fabric_abort("dfs_base must use the default HTTPS port (443)")
  }

  if (
    !identical(parsed$path %||% "", "") &&
      !identical(parsed$path %||% "", "/")
  ) {
    .fabric_abort("dfs_base must not include a path")
  }

  if (!is.null(parsed$query) || !is.null(parsed$fragment)) {
    .fabric_abort("dfs_base must not include a query string or fragment")
  }
  host <- sub(
    "\\.blob\\.fabric\\.microsoft\\.com$",
    ".dfs.fabric.microsoft.com",
    tolower(parsed$hostname)
  )
  paste0(
    "https://",
    host,
    if (is.null(parsed$port)) "" else paste0(":", parsed$port)
  )
}

# Check `host` against Microsoft Fabric OneLake domains. Returns normalized host
# text or raises before any authenticated storage request
onelake_validate_host <- function(host) {
  host <- tolower(host %||% "")
  valid <- grepl("(^|\\.)dfs\\.fabric\\.microsoft\\.com$", host) ||
    grepl("(^|\\.)blob\\.fabric\\.microsoft\\.com$", host) ||
    grepl("(^|[-.])api\\.onelake\\.fabric\\.microsoft\\.com$", host)
  if (!valid) {
    .fabric_abort("The endpoint is not a Microsoft Fabric OneLake host")
  }
  invisible(host)
}

# Normalize separators in `path` and reject unsafe segments. Returns a relative
# OneLake path used to build request URLs
onelake_normalize_path <- function(path, allow_empty = FALSE) {
  onelake_scalar(path, "path", allow_empty = allow_empty)
  path <- gsub("\\\\", "/", path)
  path <- sub("^/+", "", sub("/+$", "", path))
  if (!nzchar(path)) {
    if (allow_empty) {
      return("")
    }
    .fabric_abort("path must not be empty")
  }
  pieces <- strsplit(path, "/", fixed = TRUE)[[1L]]
  if (!all(nzchar(pieces)) || any(pieces %in% c(".", ".."))) {
    .fabric_abort("path contains an empty or unsafe segment")
  }

  if (any(grepl("[\r\n]", pieces))) {
    .fabric_abort("path contains a line break")
  }
  paste(pieces, collapse = "/")
}

# Require `target` to point below the item root for `operation`. Returns the
# target invisibly before file-specific work begins
onelake_require_file_path <- function(target, operation) {
  if (!nzchar(target$path)) {
    .fabric_abort(
      cli::format_inline("{operation} requires a path below the item")
    )
  }
  invisible(target)
}

# Check whether `target` is safe to change for `operation`. Returns invisibly and
# guards managed top-level folders unless explicitly allowed
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
    .fabric_abort("allow_managed_tables must be TRUE or FALSE")
  }
  onelake_require_file_path(target, operation)
  pieces <- strsplit(target$path, "/", fixed = TRUE)[[1L]]
  if (length(pieces) < 2L) {
    .fabric_abort(paste0(
      operation,
      " is not allowed on a Fabric-managed first-level folder"
    ))
  }

  if (
    identical(tolower(pieces[[1L]]), "tables") &&
      !isTRUE(allow_managed_tables)
  ) {
    .fabric_abort(paste0(
      operation,
      " below Tables/ is blocked because direct changes can corrupt a ",
      "managed Delta table; use allow_managed_tables = TRUE only when ",
      "deliberately managing the Delta protocol"
    ))
  }
  invisible(target)
}

# URL-encode every segment supplied through `...`. Returns path text without
# allowing separators inside a segment to bypass validation
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
# OneLake requests
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
# httr2 request with the shared API version applied
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
# conditional download, upload, and delete operations
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
# OneLake continuation tokens with bounded pagination
onelake_list_target <- function(
  target,
  credential,
  recursive = FALSE,
  page_size = 5000L,
  begin_from = NULL
) {
  # 1 Validate list options ------------------------------------------------------------------------

  # Check list options now so later code can rely on safe input

  if (
    !is.logical(recursive) ||
      length(recursive) != 1L ||
      is.na(recursive)
  ) {
    .fabric_abort("recursive must be TRUE or FALSE")
  }

  if (
    length(page_size) != 1L ||
      !is.numeric(page_size) ||
      is.na(page_size) ||
      !is.finite(page_size) ||
      page_size != floor(page_size)
  ) {
    .fabric_abort("page_size must be one whole number between 1 and 5000")
  }
  page_size <- as.integer(page_size)
  if (page_size < 1L || page_size > 5000L) {
    .fabric_abort("page_size must be one whole number between 1 and 5000")
  }

  if (!is.null(begin_from)) {
    begin_from <- onelake_normalize_path(begin_from)
    if (!isTRUE(recursive) && grepl("/", begin_from, fixed = TRUE)) {
      .fabric_abort(
        "begin_from must contain one path level when recursive = FALSE"
      )
    }
  }

  directory <- paste(
    c(target$item, if (nzchar(target$path)) target$path),
    collapse = "/"
  )
  continuation <- NULL

  # 2 Read directory pages -------------------------------------------------------------------------

  # OneLake returns a continuation header for large listings; add the decoded
  # marker to each next request and guard against repeated URLs

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

  # Return a relative-path tibble in the stable form expected by the caller

  onelake_list_tibble(records, target)
}

# Convert OneLake list `records` into paths relative to `target`. Returns a
# stable tibble used by `fabric_onelake_list()`
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
      .fabric_abort("OneLake returned a path outside the requested item")
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
# logical value for list and metadata results
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
# error response used when checking whether parent folders exist
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

# Convert storage headers from `response` into a metadata tibble for `target`
# Returns the stable shape shared by metadata, upload, and related operations
onelake_response_metadata <- function(
  response,
  target,
  content_length = NULL
) {
  # Read header `name` from this response; returns text or `NULL` below
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
# `NULL` for a complete download
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
    .fabric_abort(
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

# Validate that a ranged download returned exactly the requested byte interval.
# Returns invisibly after checking the status and Content-Range response header.
onelake_validate_range_response <- function(response, range) {
  if (is.null(range)) {
    return(invisible(response))
  }

  status <- httr2::resp_status(response)
  content_range <- httr2::resp_header(response, "content-range")
  match <- if (
    is.character(content_range) &&
      length(content_range) == 1L &&
      !is.na(content_range)
  ) {
    regexec(
      "^bytes[[:space:]]+([0-9]+)-([0-9]+)/([0-9]+)$",
      trimws(content_range),
      ignore.case = TRUE
    )
  } else {
    list(-1L)
  }
  pieces <- if (identical(match[[1L]], -1L)) {
    character()
  } else {
    regmatches(trimws(content_range), match)[[1L]]
  }
  values <- if (length(pieces) == 4L) {
    suppressWarnings(as.numeric(pieces[2:4]))
  } else {
    rep(NA_real_, 3L)
  }
  response_start <- values[[1L]]
  response_end <- values[[2L]]
  response_total <- values[[3L]]
  expected_end <- if (is.finite(response_total) && response_total > 0) {
    if (length(range) == 2L) {
      min(range[[2L]], response_total - 1)
    } else {
      response_total - 1
    }
  } else {
    NA_real_
  }
  valid <- identical(status, 206L) &&
    all(is.finite(values)) &&
    response_start == range[[1L]] &&
    response_end >= response_start &&
    response_total > response_end &&
    response_end == expected_end

  if (!isTRUE(valid)) {
    .fabric_abort(
      paste0(
        "OneLake returned an invalid response for the requested byte range"
      ),
      class = "fabric_onelake_range_response_error",
      status_code = status,
      requested_start = range[[1L]],
      requested_end = if (length(range) == 2L) range[[2L]] else NULL,
      response_start = if (is.finite(response_start)) response_start else NULL,
      response_end = if (is.finite(response_end)) response_end else NULL,
      response_total = if (is.finite(response_total)) response_total else NULL
    )
  }

  invisible(response)
}

# Download `target` into memory or atomically to `dest`. Returns raw bytes or the
# normalized destination path after validating range and overwrite behavior
onelake_download_target <- function(
  target,
  credential,
  dest = NULL,
  range = NULL,
  overwrite = FALSE,
  if_match = NULL
) {
  # 1 Build the download request -------------------------------------------------------------------

  # Build the download request from the validated values required by the next step

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

  # Return an in-memory download in the stable form expected by the caller

  if (is.null(dest)) {
    response <- .httr2_perform(
      req,
      credential = credential,
      audience = .fabric_audience$storage
    )
    onelake_validate_range_response(response, range)

    if (is.raw(response$body) && length(response$body) == 0L) {
      return(raw())
    }

    return(httr2::resp_body_raw(response))
  }

  # 3 Stage and commit a file download -------------------------------------------------------------

  # Download beside the destination so the final rename remains on one volume

  onelake_scalar(dest, "dest")
  if (
    !is.logical(overwrite) ||
      length(overwrite) != 1L ||
      is.na(overwrite)
  ) {
    .fabric_abort("overwrite must be TRUE or FALSE")
  }

  if (dir.exists(dest)) {
    .fabric_abort("Destination is a directory; supply a file path")
  }

  if (file.exists(dest) && !overwrite) {
    .fabric_abort("Destination already exists; set overwrite = TRUE")
  }
  dir.create(dirname(dest), recursive = TRUE, showWarnings = FALSE)
  temporary <- tempfile(".fabricqueryr-download-", tmpdir = dirname(dest))
  on.exit(unlink(temporary, force = TRUE), add = TRUE)
  response <- .httr2_perform(
    req,
    credential = credential,
    audience = .fabric_audience$storage,
    download_path = temporary
  )
  onelake_validate_range_response(response, range)
  onelake_commit_download(temporary, dest, overwrite = overwrite)
  invisible(normalizePath(dest, winslash = "/", mustWork = TRUE))
}

# Commit staged `temporary` content to `dest`, preserving an existing file when
# replacement fails. Returns invisibly after a safe local handoff
onelake_commit_download <- function(temporary, dest, overwrite) {
  # 1 Recheck the destination ----------------------------------------------------------------------

  # State can change while the remote file downloads, so validate it again

  if (dir.exists(dest)) {
    .fabric_abort("Destination is a directory; supply a file path")
  }

  if (file.exists(dest) && !overwrite) {
    .fabric_abort("Destination already exists; set overwrite = TRUE")
  }

  if (!overwrite) {
    return(onelake_commit_new_download(temporary, dest))
  }

  # 2 Atomically publish or replace the destination ------------------------------------------------

  # A same-directory rename is the only operation that exposes the staged bytes

  if (.onelake_file_rename(temporary, dest)) {
    return(invisible(TRUE))
  }

  if (dir.exists(dest)) {
    .fabric_abort("Destination became a directory during download")
  }
  .fabric_abort(
    paste0(
      "Could not atomically commit the downloaded file; the staged file and ",
      "any existing destination were left unchanged"
    ),
    class = "fabric_onelake_atomic_commit_unavailable"
  )
}

# Commit a staged download only when `dest` is still absent. Returns invisibly
# after linking or exclusive-copying the file without overwriting a race winner
onelake_commit_new_download <- function(temporary, dest) {
  # 1 Try an atomic hard link ----------------------------------------------------------------------

  # Try an atomic hard link first to preserve the safest available behavior

  if (.onelake_file_link(temporary, dest)) {
    if (.onelake_file_unlink(temporary) != 0L) {
      .fabric_warn(
        c(
          "The download was committed, but its staging link could not be removed.",
          "i" = "The staging link remains at {.path {temporary}}."
        ),
        .format = TRUE
      )
    }
    return(invisible(TRUE))
  }

  if (dir.exists(dest)) {
    .fabric_abort("Destination became a directory during download")
  }

  if (file.exists(dest)) {
    .fabric_abort("Destination already exists; set overwrite = TRUE")
  }

  .fabric_abort(
    paste0(
      "The filesystem could not atomically publish the download without ",
      "overwriting an existing path"
    ),
    class = "fabric_onelake_atomic_commit_unavailable",
    staging_path = temporary,
    destination_path = dest
  )
}

# Rename local `from` to `to`. Returns the base-R result and remains a small test
# seam for race and rollback behavior in atomic downloads
.onelake_file_rename <- function(from, to) {
  file.rename(from, to)
}

# Hard-link local `from` to absent `to`. Returns the base-R result and remains a
# test seam for the no-overwrite download path
.onelake_file_link <- function(from, to) {
  file.link(from, to)
}

# Remove one local staging path. Returns the base-R status and remains a small
# test seam for post-commit cleanup failures
.onelake_file_unlink <- function(path) {
  unlink(path, force = TRUE)
}

# Resolve an explicit or extension-derived object file format
.fabric_onelake_object_format <- function(path, format) {
  format <- match.arg(
    tolower(format),
    c("auto", "parquet", "csv", "arrow")
  )
  if (!identical(format, "auto")) {
    return(format)
  }
  onelake_scalar(path, "path")
  extension <- tolower(tools::file_ext(path))
  inferred <- switch(
    extension,
    parquet = "parquet",
    pq = "parquet",
    csv = "csv",
    arrow = "arrow",
    arrows = "arrow",
    ipc = "arrow",
    NULL
  )
  if (is.null(inferred)) {
    .fabric_abort(
      paste0(
        "Could not infer format from path; use a .parquet, .csv, .arrow, ",
        ".arrows, or .ipc extension, or supply format"
      ),
      class = c("fabric_onelake_object_format_error", "fabric_error")
    )
  }
  inferred
}

# Require Arrow for object serialization and nanoarrow for streaming results
.fabric_onelake_require_arrow <- function(result = "tibble") {
  if (!requireNamespace("arrow", quietly = TRUE)) {
    .fabric_abort(
      "OneLake object file I/O requires the optional arrow package",
      class = c("fabric_onelake_object_error", "fabric_error")
    )
  }
  if (
    identical(result, "arrow_stream") &&
      !requireNamespace("nanoarrow", quietly = TRUE)
  ) {
    .fabric_abort(
      "OneLake Arrow streams require the nanoarrow package",
      class = c("fabric_onelake_object_error", "fabric_error")
    )
  }
  invisible(TRUE)
}

.fabric_onelake_scalar_logical <- function(value, name) {
  if (!is.logical(value) || length(value) != 1L || is.na(value)) {
    .fabric_abort(paste0(name, " must be TRUE or FALSE"))
  }
  invisible(value)
}

# Serialize one prepared Arrow reader to a supported local staging format
.fabric_onelake_serialize_object <- function(
  prepared,
  path,
  format,
  compression,
  include_header,
  na
) {
  if (identical(format, "parquet")) {
    return(.fabric_parquet_write_stream(
      prepared,
      path = path,
      compression = compression,
      caller = "fabric_onelake_write_file()",
      error_class = c("fabric_onelake_object_write_error", "fabric_error")
    ))
  }
  tryCatch(
    {
      if (identical(format, "csv")) {
        arrow::write_csv_arrow(
          prepared$reader,
          path,
          include_header = include_header,
          na = na
        )
      } else {
        arrow::write_ipc_stream(prepared$reader, path)
      }
      bytes <- file.info(path)$size
      if (
        length(bytes) != 1L ||
          is.na(bytes) ||
          !is.finite(bytes) ||
          bytes < 0
      ) {
        .fabric_abort("Arrow did not create a readable object file")
      }
      list(
        path = path,
        rows = NA_real_,
        bytes = as.numeric(bytes),
        names = prepared$names
      )
    },
    error = function(error) {
      .fabric_abort(
        paste0("Could not serialize data as ", format, " for OneLake"),
        class = c("fabric_onelake_object_write_error", "fabric_error"),
        parent = error
      )
    }
  )
}

# Open a downloaded object file as a lazy Arrow stream and own its local file
.fabric_onelake_object_stream <- function(path, format) {
  owner <- NULL
  reader <- NULL
  input <- NULL
  complete <- FALSE
  on.exit(
    if (!complete) {
      .fabric_onelake_release_object_file(reader, input, path)
    },
    add = TRUE
  )
  tryCatch(
    {
      if (identical(format, "parquet")) {
        owner <- arrow::open_dataset(path, format = "parquet")
        reader <- arrow::as_record_batch_reader(owner)
      } else if (identical(format, "csv")) {
        owner <- arrow::open_csv_dataset(path)
        reader <- arrow::as_record_batch_reader(owner)
      } else {
        input <- arrow::mmap_open(path)
        reader <- arrow::RecordBatchStreamReader$create(input)
        owner <- list(input = input, reader = reader)
      }
      stream <- nanoarrow::as_nanoarrow_array_stream(reader)
      cleanup <- local({
        local_reader <- reader
        local_input <- input
        local_path <- path
        function() {
          .fabric_onelake_release_object_file(
            local_reader,
            local_input,
            local_path
          )
        }
      })
      stream <- nanoarrow::array_stream_set_finalizer(stream, cleanup)
      attr(stream, "fabric_onelake_file_owner") <- owner
      attr(stream, "fabric_onelake_file_path") <- path
      complete <- TRUE
      stream
    },
    error = function(error) {
      .fabric_abort(
        paste0("Could not stream the OneLake ", format, " file"),
        class = c("fabric_onelake_object_read_error", "fabric_error"),
        parent = error
      )
    }
  )
}

# Close Arrow owners before removing a downloaded object file. Returns whether
# the file is absent after a small bounded retry for Windows handle release
.fabric_onelake_release_object_file <- function(reader, input, path) {
  if (!is.null(reader)) {
    try(reader$Close(), silent = TRUE)
  }
  if (!is.null(input)) {
    try(input$close(), silent = TRUE)
  }

  for (attempt in seq_len(3L)) {
    if (!file.exists(path) || unlink(path, force = TRUE) == 0L) {
      return(invisible(TRUE))
    }
    if (attempt < 3L) {
      Sys.sleep(0.01)
    }
  }
  invisible(!file.exists(path))
}

# Normalize raw bytes or a local file `source`. Returns its kind, value, and size
# for chunked upload processing
onelake_upload_source <- function(source) {
  if (is.raw(source)) {
    return(list(kind = "raw", value = source, size = length(source)))
  }
  onelake_scalar(source, "source")
  if (!file.exists(source) || dir.exists(source)) {
    .fabric_abort(
      "source must be a raw vector or an existing local file"
    )
  }
  size <- file.info(source)$size
  if (is.na(size)) {
    .fabric_abort("Could not determine source file size")
  }
  list(kind = "file", value = source, size = as.numeric(size))
}

# Create missing parent directories for `target`. Returns invisibly and is used
# before uploads when `create_parents` is enabled
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
# metadata tibble only after the destination commit succeeds
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

  # Check source and upload settings now so later code can rely on safe input

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
    .fabric_abort("create_parents must be TRUE or FALSE")
  }

  if (create_parents) {
    onelake_create_parents(target, credential)
  }

  # 2 Create a unique staging file -----------------------------------------------------------------

  # Upload away from the destination so partial content is never exposed there

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
  # unique staging path before transmission so cleanup covers that ambiguity
  temporary_may_exist <- TRUE
  .httr2_perform(
    create,
    credential = credential,
    audience = .fabric_audience$storage,
    idempotent = FALSE
  )

  # 3 Append all source chunks ---------------------------------------------------------------------

  # Append each source chunk in order while tracking its exact byte position

  progress <- if (upload$size > 0) {
    filename <- basename(target$path)
    cli::cli_progress_bar(
      "Uploading {.file {filename}}",
      total = upload$size,
      type = "download",
      auto_terminate = TRUE
    )
  } else {
    NULL
  }
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
    if (!is.null(progress)) {
      cli::cli_progress_update(id = progress, inc = length(bytes))
    }
  })

  # 4 Flush the staging file -----------------------------------------------------------------------

  # Flush the staging file only after every preceding write has completed

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

  # Rename is the first operation that exposes the complete file at its target

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
  if (!is.null(progress)) {
    cli::cli_progress_done(id = progress)
  }
  onelake_response_metadata(response, target, content_length = upload$size)
}

# Validate upload chunk-size `value`. Returns bytes as a number within OneLake's
# supported client limit
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
    .fabric_abort(
      "chunk_size must be one whole number between 1 and 104857600 bytes"
    )
  }
  as.numeric(value)
}

# Read `upload` in `chunk_size` pieces and call `callback` with bytes and offset
# Returns invisibly after raw or file input is fully consumed
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
      .fabric_abort("Local upload source ended before its reported size")
    }
    callback(bytes, position)
    position <- position + length(bytes)
  }
  invisible(TRUE)
}

# Create a unique staging target beside `target`. Returns a copied OneLake target
# used for failure-safe upload cleanup
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

# Delete a file or directory `target`, following recursive continuation pages
# Returns invisibly after all accepted delete requests complete
onelake_delete_target <- function(
  target,
  credential,
  recursive = FALSE,
  if_match = NULL,
  is_directory = NULL
) {
  # 1 Validate the target type and options ---------------------------------------------------------

  # Check the target type and options now so later code can rely on safe input

  if (
    !is.logical(recursive) ||
      length(recursive) != 1L ||
      is.na(recursive)
  ) {
    .fabric_abort("recursive must be TRUE or FALSE")
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
    .fabric_abort("is_directory must be TRUE or FALSE")
  }
  headers <- list()
  if (!is.null(if_match)) {
    headers[["If-Match"]] <- onelake_if_match(if_match)
  }

  # 2 Delete every continuation page ---------------------------------------------------------------

  # Recursive OneLake deletion can require several requests; reject repeated
  # request URLs so a malformed continuation cannot loop forever

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
