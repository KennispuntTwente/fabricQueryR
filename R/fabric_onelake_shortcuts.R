#' Manage OneLake shortcuts
#'
#' Lists, inspects, creates or updates, and deletes shortcuts on a Fabric item.
#' Discovered Fabric items can be used directly as OneLake targets. A validated
#' raw target list supports connection-backed shortcut types already configured
#' in Fabric without copying data into R.
#'
#' @param item Destination Fabric item name, GUID, or record returned by a
#'   discovery function.
#' @param workspace Workspace name, GUID, or discovery record containing
#'   `item`. May be omitted when `item` contains `workspaceId`.
#' @param item_type Optional item type used to disambiguate a destination item
#'   supplied by name.
#' @param parent_path Optional `Files` or `Tables` path from which listing
#'   starts. Fabric still returns shortcuts below that path exhaustively.
#' @param path Parent `Files` or `Tables` path where the shortcut exists or will
#'   be created. Local checks validate path syntax, while Fabric applies
#'   item- and workload-specific placement rules. In a Lakehouse `Tables`
#'   section, use `Tables` because Fabric permits shortcuts only at that top
#'   level.
#' @param name Shortcut name.
#' @param target A discovered Fabric item, its name or GUID, or a raw named
#'   shortcut target list. A raw target must contain exactly one documented
#'   key such as `oneLake`, `adlsGen2`, `amazonS3`, `azureBlobStorage`,
#'   `googleCloudStorage`, `oneDriveSharePoint`, `s3Compatible`, or `dataverse`.
#'   Connection-backed targets must include the documented required fields, use
#'   a Fabric connection ID, and must not embed credentials. Local checks cover
#'   structure, required fields, identifier shape, and generic URL safety; they
#'   do not verify source-specific host/path rules or that a connection refers
#'   to the supplied location. A raw `oneLake` target may also include its
#'   documented optional `connectionId`. Fabric service validation is
#'   authoritative.
#' @param target_workspace Workspace containing a OneLake `target`. May be
#'   omitted when a discovered target contains `workspaceId`.
#' @param target_path Item-relative `Files` or `Tables` path for a OneLake
#'   target. Required when `target` is an item and unused for a raw target list.
#' @param target_item_type Optional Fabric item type used to disambiguate a
#'   OneLake target supplied by name.
#' @param conflict_policy `"Abort"` preserves an existing shortcut with the
#'   same path and name. `"GenerateUniqueName"` creates a uniquely named
#'   shortcut, `"CreateOrOverwrite"` creates or updates it, and
#'   `"OverwriteOnly"` updates an existing shortcut without creating one.
#' @param shortcuts For bulk creation, a non-empty list of shortcut request
#'   lists. Each request requires `path`, `name`, and `target`, accepts the same
#'   target companion fields as `fabric_onelake_shortcut_create()`, and may
#'   include a `transform` list. The only currently documented transform is
#'   `csvToDelta`; see Details.
#' @param confirm Logical. Deletion is disabled unless explicitly set to
#'   `TRUE`. Deleting a shortcut does not delete its destination data.
#' @inheritParams fabric_workspaces
#'
#' @details
#' Shortcut names, parent paths, and OneLake target paths follow Fabric's
#' current shortcut limits: `%`, `+`, and non-ASCII characters are rejected.
#' Other source- and destination-specific restrictions are intentionally left
#' to Fabric so that newly supported connection types and rules remain usable.
#'
#' @return `fabric_onelake_shortcuts()` returns a tibble with one row per
#'   shortcut. `fabric_onelake_shortcut_get()` and
#'   `fabric_onelake_shortcut_create()` return the same one-row shape.
#'   `fabric_onelake_shortcut_delete()` returns `TRUE` invisibly after success.
#'   `fabric_onelake_shortcut_cache_reset()` returns a `fabric_operation`
#'   handle for either immediate or asynchronous completion.
#' @details
#' Listing follows Fabric continuation links and tokens until every shortcut
#' below `parent_path` is returned. Unknown target details and transform fields
#' are preserved in list columns for forward compatibility.
#'
#' Create is deliberately not replayed automatically because its POST outcome
#' can be ambiguous after a transport failure. The default conflict policy is
#' Fabric's non-destructive `Abort`; overwrite must be requested explicitly.
#'
#' Bulk creation is a preview Fabric API. Its optional `csvToDelta` transform
#' accepts `includeSubfolders` and a `properties` list containing `delimiter`,
#' `skipFilesWithErrors`, and `useFirstRowAsHeader`. Supported delimiters are
#' comma, space, tab, `|`, `&`, and `;`. A bulk request returns a
#' `fabric_operation`; use [fabric_operation_result()] to retrieve the
#' per-request statuses, created shortcuts, and errors after completion.
#'
#' These Core REST APIs require `OneLake.Read.All` or
#' `OneLake.ReadWrite.All` for reads, and `OneLake.ReadWrite.All` for create and
#' delete. Fabric documents support for users, service principals, and managed
#' identities. API scope is not sufficient by itself: listing or reading also
#' requires item Read permission or OneLake Read permission on the destination
#' path. Creating requires item Write or OneLake ReadWrite on the destination,
#' plus Read access to the target path. Updating or deleting likewise requires
#' item Write or destination-path OneLake ReadWrite permission.
#' @references
#' [OneLake shortcuts REST API](https://learn.microsoft.com/en-us/rest/api/fabric/core/onelake-shortcuts/)
#'
#' [OneLake shortcut placement and limitations](https://learn.microsoft.com/en-us/fabric/onelake/onelake-shortcuts)
#'
#' [OneLake shortcut security and path permissions](https://learn.microsoft.com/en-us/fabric/onelake/onelake-shortcut-security)
#'
#' [Create shortcuts in bulk](https://learn.microsoft.com/en-us/rest/api/fabric/core/onelake-shortcuts/creates-shortcuts-in-bulk)
#'
#' [Reset shortcut cache](https://learn.microsoft.com/en-us/rest/api/fabric/core/onelake-shortcuts/reset-shortcut-cache)
#' @examples
#' \dontrun{
#' # Discover two Lakehouses in the same workspace
#' workspace <- fabric_workspaces()[[1L]]
#' lakehouses <- fabric_lakehouses(workspace)
#' destination <- lakehouses[[1L]]
#' source <- lakehouses[[2L]]
#' source_paths <- fabric_onelake_list(workspace, source, path = "Tables")
#' source_table <- source_paths[source_paths$is_directory, ][1L, ]
#'
#' # Create a shortcut whose target came from the source Lakehouse listing
#' created <- fabric_onelake_shortcut_create(
#'   destination,
#'   path = "Files",
#'   name = "shared-orders",
#'   target = source,
#'   target_path = source_table$path[[1L]]
#' )
#'
#' # List the folder, then fetch the created shortcut by its returned identity
#' fabric_onelake_shortcuts(destination, parent_path = "Files")
#' shortcut <- fabric_onelake_shortcut_get(
#'   destination,
#'   path = created$path[[1L]],
#'   name = created$name[[1L]]
#' )
#'
#' # Delete that same discovered shortcut explicitly
#' fabric_onelake_shortcut_delete(
#'   destination,
#'   path = created$path[[1L]],
#'   name = created$name[[1L]],
#'   confirm = TRUE
#' )
#' }
#' @export
fabric_onelake_shortcuts <- function(
  item,
  workspace = NULL,
  item_type = NULL,
  parent_path = NULL,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  token = NULL,
  auth_args = list(),
  api_base = .fabric_api_base
) {
  if (!is.null(parent_path)) {
    parent_path <- .fabric_shortcut_path(parent_path, "parent_path")
  }
  context <- .fabric_shortcut_context(
    item,
    workspace,
    item_type,
    tenant_id,
    client_id,
    token,
    auth_args,
    api_base,
    use_workspace_endpoint = missing(api_base)
  )
  request <- httr2::request(.fabric_shortcut_collection_url(context))
  if (!is.null(parent_path)) {
    request <- httr2::req_url_query(request, parentPath = parent_path)
  }
  records <- .httr2_collection(
    request$url,
    credential = context$credential,
    audience = .fabric_audience$fabric
  )
  .fabric_shortcut_tibble(records)
}

#' @rdname fabric_onelake_shortcuts
#' @export
fabric_onelake_shortcut_get <- function(
  item,
  path,
  name,
  workspace = NULL,
  item_type = NULL,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  token = NULL,
  auth_args = list(),
  api_base = .fabric_api_base
) {
  path <- .fabric_shortcut_path(path, "path")
  .fabric_shortcut_segment(name, "name")
  context <- .fabric_shortcut_context(
    item,
    workspace,
    item_type,
    tenant_id,
    client_id,
    token,
    auth_args,
    api_base,
    use_workspace_endpoint = missing(api_base)
  )
  body <- .httr2_json(
    httr2::request(.fabric_shortcut_resource_url(context, path, name)),
    simplifyVector = FALSE,
    credential = context$credential,
    audience = .fabric_audience$fabric
  )
  .fabric_shortcut_tibble(list(body))
}

#' @rdname fabric_onelake_shortcuts
#' @export
fabric_onelake_shortcut_create <- function(
  item,
  path,
  name,
  target,
  workspace = NULL,
  item_type = NULL,
  target_workspace = NULL,
  target_path = NULL,
  target_item_type = NULL,
  conflict_policy = c(
    "Abort",
    "GenerateUniqueName",
    "CreateOrOverwrite",
    "OverwriteOnly"
  ),
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  token = NULL,
  auth_args = list(),
  api_base = .fabric_api_base
) {
  path <- .fabric_shortcut_path(path, "path")
  .fabric_shortcut_segment(name, "name")
  conflict_policy <- .fabric_shortcut_choice(
    conflict_policy,
    c(
      "Abort",
      "GenerateUniqueName",
      "CreateOrOverwrite",
      "OverwriteOnly"
    ),
    "conflict_policy"
  )
  context <- .fabric_shortcut_context(
    item,
    workspace,
    item_type,
    tenant_id,
    client_id,
    token,
    auth_args,
    api_base,
    use_workspace_endpoint = missing(api_base)
  )
  shortcut_target <- .fabric_shortcut_create_target(
    target,
    target_workspace,
    target_path,
    target_item_type,
    context,
    use_workspace_endpoint = missing(api_base)
  )
  request <- httr2::request(.fabric_shortcut_collection_url(context)) |>
    httr2::req_method("POST") |>
    httr2::req_body_json(list(
      path = path,
      name = name,
      target = shortcut_target
    ))
  if (!identical(conflict_policy, "Abort")) {
    request <- httr2::req_url_query(
      request,
      shortcutConflictPolicy = conflict_policy
    )
  }
  body <- .httr2_json(
    request,
    simplifyVector = FALSE,
    credential = context$credential,
    audience = .fabric_audience$fabric,
    idempotent = FALSE
  )
  .fabric_shortcut_tibble(list(body))
}

#' @rdname fabric_onelake_shortcuts
#' @export
fabric_onelake_shortcuts_bulk_create <- function(
  item,
  shortcuts,
  workspace = NULL,
  item_type = NULL,
  conflict_policy = c(
    "Abort",
    "GenerateUniqueName",
    "CreateOrOverwrite",
    "OverwriteOnly"
  ),
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  token = NULL,
  auth_args = list(),
  api_base = .fabric_api_base
) {
  conflict_policy <- .fabric_shortcut_choice(
    conflict_policy,
    c(
      "Abort",
      "GenerateUniqueName",
      "CreateOrOverwrite",
      "OverwriteOnly"
    ),
    "conflict_policy"
  )
  if (!is.list(shortcuts) || !length(shortcuts)) {
    .fabric_abort(
      "`shortcuts` must be a non-empty list of shortcut request lists",
      class = c("fabric_shortcut_bulk_error", "fabric_shortcut_error")
    )
  }
  context <- .fabric_shortcut_context(
    item,
    workspace,
    item_type,
    tenant_id,
    client_id,
    token,
    auth_args,
    api_base,
    use_workspace_endpoint = missing(api_base)
  )
  use_workspace_endpoint <- missing(api_base)
  requests <- lapply(
    seq_along(shortcuts),
    function(index) {
      .fabric_shortcut_bulk_request(
        shortcuts[[index]],
        index,
        context,
        use_workspace_endpoint = use_workspace_endpoint
      )
    }
  )
  identities <- tolower(vapply(
    requests,
    function(request) paste(request$path, request$name, sep = "/"),
    character(1)
  ))
  if (anyDuplicated(identities)) {
    .fabric_abort(
      "`shortcuts` contains duplicate path and name combinations",
      class = c("fabric_shortcut_bulk_error", "fabric_shortcut_error")
    )
  }

  request <- httr2::request(
    paste0(.fabric_shortcut_collection_url(context), "/bulkCreate")
  ) |>
    httr2::req_method("POST") |>
    httr2::req_body_json(list(createShortcutRequests = requests))
  if (!identical(conflict_policy, "Abort")) {
    request <- httr2::req_url_query(
      request,
      shortcutConflictPolicy = conflict_policy
    )
  }
  .fabric_operation_submit(
    request,
    credential = context$credential,
    api_base = context$api_base,
    idempotent = FALSE,
    result_expected = TRUE
  )
}

#' @rdname fabric_onelake_shortcuts
#' @export
fabric_onelake_shortcut_cache_reset <- function(
  workspace,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  token = NULL,
  auth_args = list(),
  api_base = .fabric_api_base
) {
  credential <- fabric_credential(
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args
  )
  base <- fabric_api_base(api_base)
  workspace <- fabric_resolve_workspace(
    workspace,
    credential,
    base,
    use_workspace_endpoint = missing(api_base)
  )
  request <- httr2::request(paste0(
    workspace$api_base,
    "/workspaces/",
    workspace$id,
    "/onelake/resetShortcutCache"
  )) |>
    httr2::req_method("POST")
  .fabric_operation_submit(
    request,
    credential = credential,
    api_base = workspace$api_base,
    idempotent = FALSE,
    result_expected = FALSE
  )
}

#' @rdname fabric_onelake_shortcuts
#' @export
fabric_onelake_shortcut_delete <- function(
  item,
  path,
  name,
  workspace = NULL,
  item_type = NULL,
  confirm = FALSE,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  token = NULL,
  auth_args = list(),
  api_base = .fabric_api_base
) {
  path <- .fabric_shortcut_path(path, "path")
  .fabric_shortcut_segment(name, "name")
  if (!isTRUE(confirm)) {
    .fabric_abort(
      "Shortcut deletion is disabled by default; set confirm = TRUE explicitly"
    )
  }
  context <- .fabric_shortcut_context(
    item,
    workspace,
    item_type,
    tenant_id,
    client_id,
    token,
    auth_args,
    api_base,
    use_workspace_endpoint = missing(api_base)
  )
  request <- httr2::request(
    .fabric_shortcut_resource_url(context, path, name)
  ) |>
    httr2::req_method("DELETE")
  .httr2_perform(
    request,
    credential = context$credential,
    audience = .fabric_audience$fabric,
    idempotent = TRUE
  )
  invisible(TRUE)
}

# Resolve the destination once for every shortcut operation
.fabric_shortcut_context <- function(
  item,
  workspace,
  item_type,
  tenant_id,
  client_id,
  token,
  auth_args,
  api_base,
  use_workspace_endpoint
) {
  credential <- fabric_credential(
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args
  )
  base <- fabric_api_base(api_base)
  destination <- .fabric_job_target(
    item,
    workspace,
    item_type,
    credential,
    base,
    use_workspace_endpoint = use_workspace_endpoint
  )
  list(
    workspace_id = destination$workspace_id,
    item_id = destination$item_id,
    item_type = destination$item_type,
    api_base = destination$api_base,
    discovery_api_base = base,
    credential = credential
  )
}

.fabric_shortcut_collection_url <- function(context) {
  paste0(
    context$api_base,
    "/workspaces/",
    context$workspace_id,
    "/items/",
    context$item_id,
    "/shortcuts"
  )
}

.fabric_shortcut_resource_url <- function(context, path, name) {
  paste0(
    .fabric_shortcut_collection_url(context),
    "/",
    onelake_encode_path(path, name)
  )
}

# Convert a discovered target into OneLake JSON or validate a raw external
# target object without adding response-only `type` fields
.fabric_shortcut_create_target <- function(
  target,
  target_workspace,
  target_path,
  target_item_type,
  context,
  use_workspace_endpoint
) {
  target_record <- fabric_as_record(target)
  if (!is.null(target_record) || is.character(target)) {
    if (is.null(target_path)) {
      .fabric_abort(
        "`target_path` is required when `target` is a Fabric item",
        class = c("fabric_shortcut_target_error", "fabric_shortcut_error")
      )
    }
    target_path <- .fabric_shortcut_path(target_path, "target_path")
    resolved <- .fabric_job_target(
      target,
      target_workspace,
      target_item_type,
      context$credential,
      context$discovery_api_base,
      use_workspace_endpoint = use_workspace_endpoint
    )
    return(list(
      oneLake = list(
        workspaceId = resolved$workspace_id,
        itemId = resolved$item_id,
        path = target_path
      )
    ))
  }
  if (!is.list(target) || is.null(names(target))) {
    .fabric_abort(
      paste0(
        "`target` must be a Fabric item or a named raw shortcut target list"
      ),
      class = c("fabric_shortcut_target_error", "fabric_shortcut_error")
    )
  }
  if (
    !is.null(target_workspace) ||
      !is.null(target_path) ||
      !is.null(target_item_type)
  ) {
    .fabric_abort(
      paste0(
        "`target_workspace`, `target_path`, and `target_item_type` are only ",
        "used when `target` is a Fabric item"
      ),
      class = c("fabric_shortcut_target_error", "fabric_shortcut_error")
    )
  }
  .fabric_shortcut_raw_target(target)
}

.fabric_shortcut_bulk_request <- function(
  request,
  index,
  context,
  use_workspace_endpoint
) {
  label <- paste0("`shortcuts[[", index, "]]`")
  allowed <- c(
    "path",
    "name",
    "target",
    "target_workspace",
    "target_path",
    "target_item_type",
    "transform"
  )
  if (
    !is.list(request) ||
      is.null(names(request)) ||
      anyNA(names(request)) ||
      any(!nzchar(names(request))) ||
      anyDuplicated(names(request))
  ) {
    .fabric_abort(
      paste0(label, " must be a uniquely named shortcut request list"),
      class = c("fabric_shortcut_bulk_error", "fabric_shortcut_error")
    )
  }
  unsupported <- setdiff(names(request), allowed)
  if (length(unsupported)) {
    .fabric_abort(
      paste0(
        label,
        " contains unsupported field",
        if (length(unsupported) == 1L) " " else "s ",
        paste(unsupported, collapse = ", ")
      ),
      class = c("fabric_shortcut_bulk_error", "fabric_shortcut_error")
    )
  }
  missing <- setdiff(c("path", "name", "target"), names(request))
  if (length(missing)) {
    .fabric_abort(
      paste0(label, " requires path, name, and target"),
      class = c("fabric_shortcut_bulk_error", "fabric_shortcut_error")
    )
  }

  path <- .fabric_shortcut_path(request$path, paste0(label, "$path"))
  .fabric_shortcut_segment(request$name, paste0(label, "$name"))
  result <- list(
    path = path,
    name = request$name,
    target = .fabric_shortcut_create_target(
      request$target,
      request$target_workspace,
      request$target_path,
      request$target_item_type,
      context,
      use_workspace_endpoint = use_workspace_endpoint
    )
  )
  if (!is.null(request$transform)) {
    result$transform <- .fabric_shortcut_transform(request$transform, label)
  }
  result
}

.fabric_shortcut_transform <- function(transform, label) {
  if (
    !is.list(transform) ||
      is.null(names(transform)) ||
      anyNA(names(transform)) ||
      any(!nzchar(names(transform))) ||
      anyDuplicated(names(transform))
  ) {
    .fabric_abort(
      paste0(label, "$transform must be a uniquely named list"),
      class = c("fabric_shortcut_transform_error", "fabric_shortcut_error")
    )
  }
  allowed <- c("type", "includeSubfolders", "properties")
  unsupported <- setdiff(names(transform), allowed)
  if (length(unsupported)) {
    .fabric_abort(
      paste0(
        label,
        "$transform contains unsupported field",
        if (length(unsupported) == 1L) " " else "s ",
        paste(unsupported, collapse = ", ")
      ),
      class = c("fabric_shortcut_transform_error", "fabric_shortcut_error")
    )
  }
  if (!"type" %in% names(transform)) {
    .fabric_abort(
      paste0(label, "$transform requires type = \"csvToDelta\""),
      class = c("fabric_shortcut_transform_error", "fabric_shortcut_error")
    )
  }
  transform$type <- .fabric_shortcut_transform_choice(
    transform$type,
    "csvToDelta",
    paste0(label, "$transform$type")
  )
  if (!is.null(transform$includeSubfolders)) {
    .fabric_shortcut_boolean(
      transform$includeSubfolders,
      paste0(label, "$transform$includeSubfolders")
    )
  }
  if (!is.null(transform$properties)) {
    transform$properties <- .fabric_shortcut_transform_properties(
      transform$properties,
      label
    )
  }
  transform
}

.fabric_shortcut_transform_properties <- function(properties, label) {
  if (
    !is.list(properties) ||
      (length(properties) && is.null(names(properties))) ||
      anyNA(names(properties) %||% character()) ||
      any(!nzchar(names(properties) %||% character())) ||
      anyDuplicated(names(properties) %||% character())
  ) {
    .fabric_abort(
      paste0(label, "$transform$properties must be a uniquely named list"),
      class = c("fabric_shortcut_transform_error", "fabric_shortcut_error")
    )
  }
  allowed <- c(
    "delimiter",
    "skipFilesWithErrors",
    "useFirstRowAsHeader"
  )
  unsupported <- setdiff(names(properties) %||% character(), allowed)
  if (length(unsupported)) {
    .fabric_abort(
      paste0(
        label,
        "$transform$properties contains unsupported field",
        if (length(unsupported) == 1L) " " else "s ",
        paste(unsupported, collapse = ", ")
      ),
      class = c("fabric_shortcut_transform_error", "fabric_shortcut_error")
    )
  }
  if (!is.null(properties$delimiter)) {
    properties$delimiter <- .fabric_shortcut_transform_choice(
      properties$delimiter,
      c(",", " ", "\t", "|", "&", ";"),
      paste0(label, "$transform$properties$delimiter")
    )
  }
  for (field in c("skipFilesWithErrors", "useFirstRowAsHeader")) {
    if (!is.null(properties[[field]])) {
      .fabric_shortcut_boolean(
        properties[[field]],
        paste0(label, "$transform$properties$", field)
      )
    }
  }
  properties
}

.fabric_shortcut_transform_choice <- function(value, choices, name) {
  if (
    !is.character(value) ||
      length(value) != 1L ||
      is.na(value) ||
      !nzchar(value)
  ) {
    .fabric_abort(
      paste0(name, " must be one non-empty string"),
      class = c("fabric_shortcut_transform_error", "fabric_shortcut_error")
    )
  }
  index <- match(tolower(value), tolower(choices))
  if (is.na(index)) {
    .fabric_abort(
      paste0(name, " must be one of ", paste(choices, collapse = ", ")),
      class = c("fabric_shortcut_transform_error", "fabric_shortcut_error")
    )
  }
  choices[[index]]
}

.fabric_shortcut_boolean <- function(value, name) {
  if (!is.logical(value) || length(value) != 1L || is.na(value)) {
    .fabric_abort(
      paste0(name, " must be TRUE or FALSE"),
      class = c("fabric_shortcut_transform_error", "fabric_shortcut_error")
    )
  }
  invisible(value)
}

.fabric_shortcut_raw_target <- function(target) {
  allowed <- c(
    "oneLake",
    "adlsGen2",
    "amazonS3",
    "azureBlobStorage",
    "googleCloudStorage",
    "oneDriveSharePoint",
    "s3Compatible",
    "dataverse"
  )
  if ("type" %in% names(target)) {
    target$type <- NULL
  }
  supplied <- names(target)
  supplied <- supplied[!is.na(supplied) & nzchar(supplied)]
  if (length(supplied) != 1L || length(target) != 1L) {
    .fabric_abort(
      "A raw shortcut target must contain exactly one target type",
      class = c("fabric_shortcut_target_error", "fabric_shortcut_error")
    )
  }
  index <- match(tolower(supplied), tolower(allowed))
  if (is.na(index)) {
    .fabric_abort(
      paste0(
        "Unsupported raw shortcut target type '",
        supplied,
        "'"
      ),
      class = c("fabric_shortcut_target_error", "fabric_shortcut_error")
    )
  }
  name <- allowed[[index]]
  details <- target[[1L]]
  if (!is.list(details) || (length(details) && is.null(names(details)))) {
    .fabric_abort(
      "Raw shortcut target details must be a named list",
      class = c("fabric_shortcut_target_error", "fabric_shortcut_error")
    )
  }
  if ("type" %in% names(details)) {
    details$type <- NULL
  }
  if (.fabric_shortcut_has_secret_field(details)) {
    .fabric_abort(
      paste0(
        "Raw shortcut targets must reference a Fabric connection ID and must ",
        "not contain credential fields"
      ),
      class = c("fabric_shortcut_target_error", "fabric_shortcut_error")
    )
  }
  allowed_fields <- .fabric_shortcut_target_fields(name)
  unsupported <- setdiff(names(details) %||% character(), allowed_fields)
  if (length(unsupported)) {
    .fabric_abort(
      paste0(
        "A raw ",
        name,
        " target contains unsupported field",
        if (length(unsupported) == 1L) " " else "s ",
        paste(unsupported, collapse = ", ")
      ),
      class = c("fabric_shortcut_target_error", "fabric_shortcut_error")
    )
  }
  if (identical(name, "oneLake")) {
    required <- c("workspaceId", "itemId", "path")
    if (!all(required %in% names(details))) {
      .fabric_abort(
        "A raw oneLake target requires workspaceId, itemId, and path",
        class = c("fabric_shortcut_target_error", "fabric_shortcut_error")
      )
    }
    .fabric_job_guid(details$workspaceId, "OneLake target workspace ID")
    .fabric_job_guid(details$itemId, "OneLake target item ID")
    if (!is.null(details$connectionId)) {
      .fabric_job_guid(details$connectionId, "OneLake target connection ID")
    }
    details$path <- .fabric_shortcut_path(details$path, "target path")
  } else {
    details <- .fabric_shortcut_external_target(details, name)
  }
  stats::setNames(list(details), name)
}

.fabric_shortcut_target_fields <- function(type) {
  switch(
    type,
    oneLake = c("workspaceId", "itemId", "path", "connectionId"),
    adlsGen2 = c("connectionId", "location", "subpath"),
    amazonS3 = c("connectionId", "location", "subpath"),
    azureBlobStorage = c("connectionId", "location", "subpath"),
    googleCloudStorage = c("connectionId", "location", "subpath"),
    oneDriveSharePoint = c(
      "connectionId",
      "location",
      "subpath",
      "updateFabricItemSensitivity"
    ),
    s3Compatible = c("connectionId", "location", "bucket", "subpath"),
    dataverse = c(
      "connectionId",
      "deltaLakeFolder",
      "environmentDomain",
      "tableName"
    )
  )
}

.fabric_shortcut_external_target <- function(details, type) {
  required <- switch(
    type,
    adlsGen2 = c("connectionId", "location"),
    amazonS3 = c("connectionId", "location"),
    azureBlobStorage = c("connectionId", "location"),
    googleCloudStorage = c("connectionId", "location"),
    oneDriveSharePoint = c("connectionId", "location"),
    s3Compatible = c("connectionId", "location", "bucket"),
    dataverse = c(
      "connectionId",
      "deltaLakeFolder",
      "environmentDomain",
      "tableName"
    )
  )
  missing <- setdiff(required, names(details))
  if (length(missing)) {
    .fabric_abort(
      paste0(
        "A raw ",
        type,
        " target requires ",
        paste(required, collapse = ", ")
      ),
      class = c("fabric_shortcut_target_error", "fabric_shortcut_error")
    )
  }

  .fabric_job_guid(details$connectionId, "shortcut connection ID")
  if (identical(type, "dataverse")) {
    details$deltaLakeFolder <- .fabric_shortcut_target_string(
      details$deltaLakeFolder,
      "Dataverse deltaLakeFolder"
    )
    details$environmentDomain <- .fabric_shortcut_target_url(
      details$environmentDomain,
      "Dataverse environmentDomain"
    )
    details$tableName <- .fabric_shortcut_target_string(
      details$tableName,
      "Dataverse tableName"
    )
    return(details)
  }

  details$location <- .fabric_shortcut_target_url(
    details$location,
    paste0(type, " location")
  )
  if (!is.null(details$subpath)) {
    details$subpath <- .fabric_shortcut_target_string(
      details$subpath,
      paste0(type, " subpath"),
      allow_empty = TRUE
    )
  }
  if (identical(type, "s3Compatible")) {
    details$bucket <- .fabric_shortcut_target_string(
      details$bucket,
      "s3Compatible bucket"
    )
  }
  if (
    identical(type, "oneDriveSharePoint") &&
      !is.null(details$updateFabricItemSensitivity) &&
      (!is.logical(details$updateFabricItemSensitivity) ||
        length(details$updateFabricItemSensitivity) != 1L ||
        is.na(details$updateFabricItemSensitivity))
  ) {
    .fabric_abort(
      "oneDriveSharePoint updateFabricItemSensitivity must be TRUE or FALSE",
      class = c("fabric_shortcut_target_error", "fabric_shortcut_error")
    )
  }
  details
}

.fabric_shortcut_target_string <- function(value, label, allow_empty = FALSE) {
  if (
    !is.character(value) ||
      length(value) != 1L ||
      is.na(value) ||
      (!allow_empty && !nzchar(value))
  ) {
    .fabric_abort(
      paste0(label, " must be a single non-missing character value"),
      class = c("fabric_shortcut_target_error", "fabric_shortcut_error")
    )
  }
  value
}

.fabric_shortcut_target_url <- function(value, label) {
  value <- .fabric_shortcut_target_string(value, label)
  parsed <- try(httr2::url_parse(value), silent = TRUE)
  invalid <- inherits(parsed, "try-error") ||
    !identical(tolower(parsed$scheme %||% ""), "https") ||
    !nzchar(parsed$hostname %||% "") ||
    nzchar(parsed$username %||% "") ||
    nzchar(parsed$password %||% "") ||
    length(parsed$query %||% list()) > 0L ||
    nzchar(parsed$fragment %||% "")
  if (invalid) {
    .fabric_abort(
      paste0(label, " must be an HTTPS URL without credentials or query data"),
      class = c("fabric_shortcut_target_error", "fabric_shortcut_error")
    )
  }
  value
}

# Normalize service records into a stable tibble while preserving raw fields
.fabric_shortcut_tibble <- function(records) {
  empty <- tibble::tibble(
    path = character(),
    name = character(),
    target_type = character(),
    one_lake_workspace_id = character(),
    one_lake_item_id = character(),
    one_lake_path = character(),
    is_transform = logical(),
    target = list(),
    transform = list(),
    raw = list()
  )
  if (!length(records)) {
    return(empty)
  }
  rows <- lapply(records, .fabric_shortcut_record)
  tibble::tibble(
    path = vapply(rows, `[[`, character(1), "path"),
    name = vapply(rows, `[[`, character(1), "name"),
    target_type = vapply(rows, `[[`, character(1), "target_type"),
    one_lake_workspace_id = vapply(
      rows,
      `[[`,
      character(1),
      "one_lake_workspace_id"
    ),
    one_lake_item_id = vapply(
      rows,
      `[[`,
      character(1),
      "one_lake_item_id"
    ),
    one_lake_path = vapply(rows, `[[`, character(1), "one_lake_path"),
    is_transform = vapply(rows, `[[`, logical(1), "is_transform"),
    target = lapply(rows, `[[`, "target"),
    transform = lapply(rows, `[[`, "transform"),
    raw = lapply(rows, `[[`, "raw")
  )
}

.fabric_shortcut_record <- function(record) {
  if (!is.list(record)) {
    .fabric_abort(
      "Fabric returned an invalid shortcut record",
      class = c("fabric_shortcut_protocol_error", "fabric_shortcut_error")
    )
  }
  path <- record$path
  name <- record$name
  if (
    !is.character(path) ||
      length(path) != 1L ||
      is.na(path) ||
      !is.character(name) ||
      length(name) != 1L ||
      is.na(name)
  ) {
    .fabric_abort(
      "Fabric returned a shortcut without a valid path and name",
      class = c("fabric_shortcut_protocol_error", "fabric_shortcut_error")
    )
  }
  target <- .httr2_redact_object(record$target %||% list())
  if (!is.list(target)) {
    .fabric_abort(
      "Fabric returned invalid shortcut target details",
      class = c("fabric_shortcut_protocol_error", "fabric_shortcut_error")
    )
  }
  target_keys <- setdiff(names(target) %||% character(), "type")
  target_type <- target$type %||%
    if (length(target_keys)) target_keys[[1L]] else NA_character_
  if (
    !is.character(target_type) ||
      length(target_type) != 1L ||
      is.na(target_type) ||
      !nzchar(target_type)
  ) {
    .fabric_abort(
      "Fabric returned an invalid shortcut target type",
      class = c("fabric_shortcut_protocol_error", "fabric_shortcut_error")
    )
  }
  one_lake <- target$oneLake %||% list()
  list(
    path = path,
    name = name,
    target_type = as.character(target_type),
    one_lake_workspace_id = .fabric_shortcut_optional_string(
      one_lake$workspaceId
    ),
    one_lake_item_id = .fabric_shortcut_optional_string(one_lake$itemId),
    one_lake_path = .fabric_shortcut_optional_string(one_lake$path),
    is_transform = isTRUE(record$isShortcutTransform),
    target = target,
    transform = .httr2_redact_object(record$transform %||% list()),
    raw = .httr2_redact_object(record)
  )
}

.fabric_shortcut_has_secret_field <- function(value) {
  if (!is.list(value)) {
    return(FALSE)
  }
  fields <- names(value) %||% character()
  any(vapply(fields, .httr2_is_secret_field, logical(1))) ||
    any(vapply(value, .fabric_shortcut_has_secret_field, logical(1)))
}

.fabric_shortcut_optional_string <- function(value) {
  if (
    !is.character(value) ||
      length(value) != 1L ||
      is.na(value)
  ) {
    NA_character_
  } else {
    value
  }
}

# Validate a full shortcut parent/target path using the service's linked file
# naming limits, while requiring Fabric's Files or Tables root
.fabric_shortcut_path <- function(value, name) {
  if (
    !is.character(value) ||
      length(value) != 1L ||
      is.na(value) ||
      !nzchar(value)
  ) {
    .fabric_abort(paste0("`", name, "` must be one non-empty path"))
  }
  value <- gsub("\\\\", "/", value)
  value <- sub("^/+", "", sub("/+$", "", value))
  pieces <- strsplit(value, "/", fixed = TRUE)[[1L]]
  if (!length(pieces) || !tolower(pieces[[1L]]) %in% c("files", "tables")) {
    .fabric_abort(paste0("`", name, "` must begin with Files or Tables"))
  }
  if (length(pieces) > 251L || nchar(value, type = "bytes") > 2048L) {
    .fabric_abort(paste0("`", name, "` exceeds Fabric path limits"))
  }
  for (piece in pieces) {
    .fabric_shortcut_segment(piece, name)
  }
  paste(pieces, collapse = "/")
}

.fabric_shortcut_segment <- function(value, name) {
  invalid_device <- grepl(
    "^(?:LPT[1-9]|COM[1-9]|PRN|AUX|NUL|CON|CLOCK\\$)(?:\\..*)?$",
    value,
    ignore.case = TRUE
  )
  if (
    !is.character(value) ||
      length(value) != 1L ||
      is.na(value) ||
      !nzchar(value) ||
      value %in% c(".", "..") ||
      nchar(value) > 255L ||
      grepl('["\\\\/:|<>*?[:cntrl:]]', value) ||
      grepl("[%+]", value) ||
      grepl("[^\\x20-\\x7E]", value, perl = TRUE) ||
      grepl("[. ]$", value) ||
      invalid_device
  ) {
    .fabric_abort(paste0("`", name, "` contains an invalid path component"))
  }
  invisible(value)
}

.fabric_shortcut_choice <- function(value, choices, name) {
  if (length(value) > 1L) {
    value <- value[[1L]]
  }
  if (
    !is.character(value) ||
      length(value) != 1L ||
      is.na(value) ||
      !nzchar(value)
  ) {
    .fabric_abort(paste0("`", name, "` must be one non-empty string"))
  }
  index <- match(tolower(value), tolower(choices))
  if (is.na(index)) {
    .fabric_abort(paste0(
      "`",
      name,
      "` must be one of ",
      paste(choices, collapse = ", ")
    ))
  }
  choices[[index]]
}
