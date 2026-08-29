#' Search the OneLake catalog
#'
#' Searches Fabric item metadata across every workspace visible to the caller.
#' Results are lightweight R6 discovery objects that contain the item and
#' workspace identity. Type-specific results expose the methods that can run
#' from that metadata.
#'
#' @param search Optional non-empty text query. Fabric searches display names,
#'   workspace display names, and descriptions. Leave `NULL` to browse visible
#'   catalog entries without a text query.
#' @param types Optional unique vector of Fabric item types. This is converted
#'   to the catalog API's documented `Type eq ... or Type eq ...` filter.
#' @param filter Optional raw catalog filter string. Fabric currently supports
#'   `Type`, `eq`, `ne`, `or`, and parentheses. Supply either `types` or
#'   `filter`, not both.
#' @param page_size Optional number of results requested per page, from 1 to
#'   1000. Leave `NULL` to use Fabric's service default.
#' @inheritParams fabric_workspaces
#'
#' @return With `output = "r6"`, a list of [FabricItem] objects or
#'   type-specific subclasses. With `output = "list"`, a list of
#'   `fabric_catalog_entry` records that also inherit from `fabric_item`. Both
#'   representations preserve the fields returned by Fabric and add the item
#'   workspace identity from the catalog hierarchy.
#' @details
#' Catalog search is a preview Fabric API. It is for metadata discovery only
#' and does not grant access to item contents. The caller needs
#' `Catalog.Read.All`; Fabric returns only entries that the calling user,
#' service principal, or managed identity is authorized to see.
#'
#' Pagination uses a new POST body for each continuation token. A repeated or
#' malformed token raises a `fabric_catalog_protocol_error` instead of silently
#' returning partial results or looping indefinitely.
#' @references
#' [Catalog search REST API](https://learn.microsoft.com/en-us/rest/api/fabric/core/catalog/search)
#'
#' [OneLake Catalog REST API overview](https://learn.microsoft.com/en-us/rest/api/fabric/articles/onelakecatalog/overview)
#' @examples
#' \dontrun{
#' # Search all visible workspaces for Lakehouses related to sales
#' entries <- fabric_catalog_search(
#'   search = "sales",
#'   types = "Lakehouse",
#'   page_size = 100
#' )
#'
#' # `$onelake_list()` calls fabric_onelake_list()
#' lakehouse <- entries[[1L]]
#' lakehouse$onelake_list(path = "Tables")
#' }
#' @export
fabric_catalog_search <- function(
  search = NULL,
  types = NULL,
  filter = NULL,
  page_size = NULL,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  token = NULL,
  auth_args = list(),
  api_base = .fabric_api_base,
  output = c("r6", "list")
) {
  output <- .fabric_r6_output(output)
  search <- .fabric_catalog_optional_string(search, "search")
  filter <- .fabric_catalog_optional_string(filter, "filter")
  if (!is.null(types) && !is.null(filter)) {
    .fabric_abort("Supply either `types` or `filter`, not both")
  }
  if (!is.null(types)) {
    valid_types <- is.character(types) &&
      length(types) > 0L &&
      !anyNA(types) &&
      all(grepl("^[A-Za-z][A-Za-z0-9]*$", types)) &&
      !anyDuplicated(tolower(types))
    if (!valid_types) {
      .fabric_abort(
        "`types` must be a unique vector of Fabric item type names"
      )
    }
    filter <- paste0("Type eq '", types, "'", collapse = " or ")
  }
  if (!is.null(page_size)) {
    valid_page_size <- is.numeric(page_size) &&
      length(page_size) == 1L &&
      !is.na(page_size) &&
      is.finite(page_size) &&
      page_size == floor(page_size) &&
      page_size >= 1L &&
      page_size <= 1000L
    if (!valid_page_size) {
      .fabric_abort("`page_size` must be NULL or an integer from 1 to 1000")
    }
    page_size <- as.integer(page_size)
  }

  base <- fabric_api_base(api_base)
  credential <- fabric_credential(
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args
  )
  records <- .fabric_catalog_pages(
    paste0(base, "/catalog/search"),
    search = search,
    filter = filter,
    page_size = page_size,
    credential = credential
  )
  lapply(records, function(entry) {
    .fabric_catalog_entry(entry, output, credential)
  })
}

.fabric_catalog_optional_string <- function(value, name) {
  if (is.null(value)) {
    return(NULL)
  }
  if (
    !is.character(value) ||
      length(value) != 1L ||
      is.na(value) ||
      !nzchar(trimws(value))
  ) {
    .fabric_abort(paste0("`", name, "` must be NULL or one non-empty string"))
  }
  value
}

.fabric_catalog_pages <- function(
  url,
  search,
  filter,
  page_size,
  credential,
  max_pages = 10000L
) {
  records <- list()
  continuation_token <- NULL
  seen_tokens <- character()
  page_number <- 0L
  repeat {
    page_number <- page_number + 1L
    if (page_number > max_pages) {
      .fabric_catalog_abort_protocol(
        paste0("Catalog pagination exceeded ", max_pages, " pages"),
        page_number
      )
    }
    body <- Filter(
      Negate(is.null),
      list(search = search, pageSize = page_size, filter = filter)
    )
    if (!is.null(continuation_token)) {
      body$continuationToken <- continuation_token
    }
    page <- .httr2_json(
      httr2::request(url) |>
        httr2::req_method("POST") |>
        httr2::req_body_json(body),
      simplifyVector = FALSE,
      credential = credential,
      audience = .fabric_audience$fabric,
      idempotent = TRUE
    )
    page <- .fabric_catalog_page(page, page_number)
    records <- c(records, page$value)
    continuation_token <- page$continuation_token
    if (is.null(continuation_token)) {
      break
    }
    if (continuation_token %in% seen_tokens) {
      .fabric_catalog_abort_protocol(
        "Fabric repeated a catalog continuation token",
        page_number
      )
    }
    seen_tokens <- c(seen_tokens, continuation_token)
  }
  records
}

.fabric_catalog_page <- function(page, page_number) {
  if (!is.list(page) || is.null(names(page))) {
    .fabric_catalog_abort_protocol(
      "Fabric returned a catalog page that was not a JSON object",
      page_number
    )
  }
  value_positions <- which(names(page) == "value")
  if (length(value_positions) != 1L) {
    .fabric_catalog_abort_protocol(
      "A catalog page must contain exactly one value field",
      page_number
    )
  }
  value <- page[[value_positions]]
  if (!is.list(value) || !is.null(names(value))) {
    .fabric_catalog_abort_protocol(
      "A catalog page value must be a JSON array",
      page_number
    )
  }
  if (
    !all(vapply(
      value,
      function(entry) is.list(entry) && !is.null(names(entry)),
      logical(1)
    ))
  ) {
    .fabric_catalog_abort_protocol(
      "A catalog page value must contain only JSON objects",
      page_number
    )
  }
  token_positions <- which(names(page) == "continuationToken")
  if (length(token_positions) > 1L) {
    .fabric_catalog_abort_protocol(
      "A catalog page contains duplicate continuationToken fields",
      page_number
    )
  }
  continuation_token <- if (!length(token_positions)) {
    NULL
  } else {
    page[[token_positions]]
  }
  if (!is.null(continuation_token)) {
    if (
      !is.character(continuation_token) ||
        length(continuation_token) != 1L ||
        is.na(continuation_token)
    ) {
      .fabric_catalog_abort_protocol(
        "A catalog continuation token must be one string or null",
        page_number
      )
    }
    if (!nzchar(continuation_token)) {
      continuation_token <- NULL
    }
  }
  list(value = value, continuation_token = continuation_token)
}

.fabric_catalog_entry <- function(
  entry,
  output = c("r6", "list"),
  credential = NULL
) {
  output <- .fabric_r6_output(output)
  hierarchy <- if (is.list(entry$hierarchy)) entry$hierarchy else NULL
  workspace <- if (is.list(hierarchy)) hierarchy$workspace else NULL
  valid <- is.character(entry$id) &&
    length(entry$id) == 1L &&
    !is.na(entry$id) &&
    fabric_is_guid(entry$id) &&
    is.character(entry$type) &&
    length(entry$type) == 1L &&
    !is.na(entry$type) &&
    nzchar(entry$type) &&
    is.character(entry$displayName) &&
    length(entry$displayName) == 1L &&
    !is.na(entry$displayName) &&
    nzchar(entry$displayName) &&
    is.list(workspace) &&
    is.character(workspace$id) &&
    length(workspace$id) == 1L &&
    !is.na(workspace$id) &&
    fabric_is_guid(workspace$id) &&
    is.character(workspace$displayName) &&
    length(workspace$displayName) == 1L &&
    !is.na(workspace$displayName) &&
    nzchar(workspace$displayName)
  if (!valid) {
    .fabric_catalog_abort_protocol(
      "Fabric returned a catalog entry without valid item and workspace identity"
    )
  }
  entry$workspaceId <- workspace$id
  entry$workspaceDisplayName <- workspace$displayName
  record <- structure(
    entry,
    class = c("fabric_catalog_entry", "fabric_item", "list")
  )
  if (identical(output, "r6")) {
    fabric_r6_record(record, class(record), credential)
  } else {
    record
  }
}

.fabric_catalog_abort_protocol <- function(message, page_number = NULL) {
  .fabric_abort(
    message,
    class = c("fabric_catalog_protocol_error", "fabric_catalog_error"),
    page_number = page_number
  )
}
