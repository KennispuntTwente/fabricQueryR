#' Query a Microsoft Fabric API for GraphQL
#'
#' Sends a GraphQL query or mutation to an API for GraphQL item configured in
#' Fabric. Fabric generates the API schema from selected Lakehouse, Warehouse,
#' or SQL Database objects; this function calls that API from R and returns its
#' nested response.
#'
#' @details
#' Before using this function, create an **API for GraphQL** item in a Fabric
#' workspace, connect its data source, and choose which tables, fields, queries,
#' and mutations the API exposes. Fabric's built-in GraphQL editor and schema
#' explorer are the easiest places to design and test a document before copying
#' it to R.
#'
#' Mutation availability depends on the configured source. Fabric Warehouse and
#' SQL Database sources can expose supported mutations, while Lakehouse and
#' mirrored SQL analytics endpoint sources are read-only and expose queries only.
#'
#' A direct endpoint has the form
#' `https://api.fabric.microsoft.com/v1/workspaces/{workspace-id}/graphqlapis/{api-id}/graphql`.
#' You can instead pass a GraphQL API GUID with `workspace_id`, or one item from
#' [fabric_graphql_apis()] or [fabric_item()].
#'
#' Interactive/delegated authentication requires the Power BI delegated scope
#' `GraphQLApi.Execute.All`, plus **Run Queries and Mutations** permission on
#' the API. Service principals are also supported by Fabric: request a Fabric
#' API token with `auth_args` or pass one through `token`, enable service
#' principals for Fabric APIs in the tenant, and grant the principal API
#' Execute access or a suitable workspace role. With SSO connectivity, the
#' caller also needs the required access to the underlying data source.
#' Saved-credential APIs use the configured connection instead.
#'
#' When `audience = NULL`, the package selects the Fabric API scope for an
#' AzureAuth client-credentials flow and the delegated GraphQL scope otherwise.
#' Set `audience` explicitly when a custom token provider uses a
#' service-principal flow. The value is ignored for a static token string.
#'
#' GraphQL POST requests are not retried by default because a document can
#' contain mutations. Set `idempotent = TRUE` only when the operation is safe
#' to repeat.
#'
#' Fabric returns at most 100 items by default and permits at most 100,000 items
#' across pagination. Each response is limited to 64 MB, each request to 100
#' seconds, and query nesting to 10 levels. Use smaller pages and filtered query
#' partitions when a result could approach these service limits.
#'
#' JSON integers outside R's exact double-precision range are returned as
#' character values so identifiers and other large integer fields are not
#' rounded.
#'
#' @param api GraphQL HTTPS endpoint, GraphQL API GUID, or one discovered
#'   GraphQLApi record. An item from [fabric_graphql_apis()] is usually easiest
#'   because it supplies the endpoint and workspace ID.
#' @param query One GraphQL document containing a query or mutation. Use
#'   variables for changing values instead of pasting values into this string.
#' @param variables Named list of GraphQL variables. Values must be representable
#'   as JSON; names must match variables declared in `query`.
#' @param operation_name Optional operation name. Supply it when the document
#'   contains more than one named operation; otherwise leave `NULL`.
#' @param workspace_id Workspace GUID. Required when `api` is a GraphQL API
#'   GUID, and otherwise inferred from a discovered record.
#' @param error_policy How GraphQL-level errors are handled. `"return"`
#'   lets the caller inspect partial data and errors; `"warn"` also makes errors
#'   visible immediately; `"error"` stops and attaches the result to a
#'   `fabric_graphql_error`. HTTP/authentication failures always stop.
#' @param timeout Positive wall-clock HTTP timeout in seconds. The 110-second
#'   default leaves transfer overhead beyond Fabric's 100-second server
#'   execution limit, allowing the service's own timeout response to arrive.
#' @param idempotent Logical. Permit retries after transient HTTP failures.
#'   `TRUE` is normally suitable for a read-only query, but not for a mutation
#'   that could be applied twice.
#' @param tenant_id Microsoft Entra tenant ID. Defaults to
#'   `FABRICQUERYR_TENANT_ID`.
#' @param client_id Microsoft Entra application/client ID. Defaults to
#'   `FABRICQUERYR_CLIENT_ID`, with the Azure CLI application ID as fallback.
#' @param token Optional `AzureAuth::AzureToken`, bearer-token string, or
#'   token-provider function. With `NULL`, `AzureAuth` reuses a matching cached
#'   token or starts its normal interactive login flow.
#' @param auth_args Named list of additional arguments passed to
#'   [AzureAuth::get_azure_token()] when no token source is supplied.
#' @param audience OAuth audience/scope passed to the credential. `NULL`
#'   selects the documented scope from the authentication flow. Set this only
#'   for a custom token provider or unusual identity flow.
#' @param api_base Fabric REST API base URL used to derive endpoints from IDs.
#'   Most users should keep the default.
#' @param allow_custom_endpoint Logical. Permit a GraphQL endpoint outside the
#'   Microsoft Fabric API origin. Keep `FALSE` unless the origin is trusted;
#'   credentials are sent to the supplied endpoint.
#'
#' @return A `fabric_graphql_result` list with `data`, `errors`, `extensions`,
#'   and `response` (the complete parsed response). `data` follows the nested
#'   shape requested in the GraphQL document and is usually a combination of
#'   named lists and vectors, not a tibble. Because GraphQL can return partial
#'   data, inspect `errors` even when `data` is present.
#' @references
#' [Fabric API for GraphQL editor](https://learn.microsoft.com/en-us/fabric/data-engineering/api-graphql-editor)
#'
#' [Fabric GraphQL schema explorer](https://learn.microsoft.com/en-us/fabric/data-engineering/graphql-schema-view)
#'
#' [Use service principals with Fabric API for GraphQL](https://learn.microsoft.com/en-us/fabric/data-engineering/api-graphql-service-principal)
#'
#' [Fabric API for GraphQL limits](https://learn.microsoft.com/en-us/fabric/data-engineering/api-graphql-limits)
#' @export
#'
#' @examples
#' \dontrun{
#' api <- fabric_graphql_apis("Analytics workspace")[[1]]
#'
#' result <- fabric_graphql_query(
#'   api,
#'   query = paste(
#'     "query Products($category: String!) {",
#'     "  products(filter: {category: {eq: $category}}) {",
#'     "    items { id name category }",
#'     "  }",
#'     "}"
#'   ),
#'   variables = list(category = "A"),
#'   operation_name = "Products"
#' )
#' result$data$products$items
#' result$errors
#' }
fabric_graphql_query <- function(
  api,
  query,
  variables = list(),
  operation_name = NULL,
  workspace_id = NULL,
  error_policy = c("return", "warn", "error"),
  timeout = 110,
  idempotent = FALSE,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  token = NULL,
  auth_args = list(),
  audience = NULL,
  api_base = .fabric_api_base,
  allow_custom_endpoint = FALSE
) {
  variables <- graphql_validate_variables(variables)
  context <- graphql_request_context(
    api = api,
    query = query,
    operation_name = operation_name,
    workspace_id = workspace_id,
    error_policy = error_policy,
    timeout = timeout,
    idempotent = idempotent,
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args,
    audience = audience,
    api_base = api_base,
    allow_custom_endpoint = allow_custom_endpoint
  )

  graphql_execute_context(context, variables)
}

graphql_request_context <- function(
  api,
  query,
  operation_name,
  workspace_id,
  error_policy,
  timeout,
  idempotent,
  tenant_id,
  client_id,
  token,
  auth_args,
  audience,
  api_base,
  allow_custom_endpoint
) {
  graphql_validate_query(query)
  operation_name <- graphql_optional_string(
    operation_name,
    "operation_name"
  )
  error_policy <- match.arg(error_policy, c("return", "warn", "error"))
  graphql_validate_scalar(
    timeout,
    is.numeric,
    "timeout must be one positive number of seconds",
    function(value) is.finite(value) && value > 0
  )
  graphql_validate_scalar(
    idempotent,
    is.logical,
    "idempotent must be TRUE or FALSE"
  )
  endpoint <- graphql_resolve_endpoint(
    api,
    workspace_id = workspace_id,
    api_base = api_base,
    allow_custom_endpoint = allow_custom_endpoint
  )
  credential <- fabric_credential(
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args
  )
  audience <- graphql_resolve_audience(
    audience,
    token = token,
    auth_args = auth_args
  )

  list(
    endpoint = endpoint,
    query = query,
    operation_name = operation_name,
    error_policy = error_policy,
    timeout = timeout,
    idempotent = idempotent,
    credential = credential,
    audience = audience
  )
}

graphql_execute_context <- function(context, variables) {
  graphql_execute(
    context$endpoint,
    query = context$query,
    variables = variables,
    operation_name = context$operation_name,
    error_policy = context$error_policy,
    timeout = context$timeout,
    idempotent = context$idempotent,
    credential = context$credential,
    audience = context$audience
  )
}

#' Paginate a Microsoft Fabric GraphQL operation
#'
#' Repeats [fabric_graphql_query()] while `next_cursor` returns a cursor. The
#' callback tells the function where the current page stores its next cursor,
#' because that location depends on the API schema.
#'
#' @param next_cursor Function accepting a `fabric_graphql_result` and returning
#'   the next opaque cursor, or `NULL` when pagination is complete. Use
#'   [fabric_graphql_cursor()] for Fabric's normal connection fields.
#' @param cursor_variable Name of the GraphQL variable that receives the next
#'   cursor, commonly `"after"`. It must match the variable declared in `query`.
#' @param max_pages Positive maximum number of requests. This guards against a
#'   faulty or unexpectedly large pagination loop.
#' @inheritParams fabric_graphql_query
#'
#' @return A `fabric_graphql_pages` list with `pages`, combined `errors`, and
#'   the final `variables`. `pages` contains one `fabric_graphql_result` per
#'   request and `complete` is `TRUE` when the callback reported no next page.
#'   Results are kept page-by-page because the requested schema shape can vary.
#' @export
#'
#' @examples
#' \dontrun{
#' api <- fabric_graphql_apis("Analytics workspace")[[1]]
#'
#' pages <- fabric_graphql_paginate(
#'   api,
#'   query = paste(
#'     "query Products($first: Int!, $after: String) {",
#'     "  products(first: $first, after: $after) {",
#'     "    items { id name } hasNextPage endCursor",
#'     "  }",
#'     "}"
#'   ),
#'   variables = list(first = 100L, after = NULL),
#'   next_cursor = fabric_graphql_cursor("products")
#' )
#' }
fabric_graphql_paginate <- function(
  api,
  query,
  next_cursor,
  variables = list(),
  cursor_variable = "after",
  operation_name = NULL,
  workspace_id = NULL,
  error_policy = c("return", "warn", "error"),
  max_pages = 100L,
  timeout = 110,
  idempotent = FALSE,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  token = NULL,
  auth_args = list(),
  audience = NULL,
  api_base = .fabric_api_base,
  allow_custom_endpoint = FALSE
) {
  if (!is.function(next_cursor)) {
    rlang::abort("next_cursor must be a function")
  }
  cursor_variable <- graphql_required_string(
    cursor_variable,
    "cursor_variable"
  )
  if (
    !is.numeric(max_pages) ||
      length(max_pages) != 1L ||
      is.na(max_pages) ||
      !is.finite(max_pages) ||
      max_pages < 1 ||
      max_pages > .Machine$integer.max ||
      max_pages != floor(max_pages)
  ) {
    rlang::abort("max_pages must be one positive integer")
  }
  variables <- graphql_validate_variables(variables)
  error_policy <- match.arg(error_policy, c("return", "warn", "error"))
  context <- graphql_request_context(
    api = api,
    query = query,
    operation_name = operation_name,
    workspace_id = workspace_id,
    error_policy = error_policy,
    timeout = timeout,
    idempotent = idempotent,
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args,
    audience = audience,
    api_base = api_base,
    allow_custom_endpoint = allow_custom_endpoint
  )
  pages <- list()
  seen <- character()

  for (page_number in seq_len(as.integer(max_pages))) {
    result <- graphql_execute_context(context, variables)
    pages[[page_number]] <- result
    cursor <- next_cursor(result)
    if (is.null(cursor)) {
      return(graphql_pages_result(pages, variables, complete = TRUE))
    }
    cursor <- graphql_required_string(cursor, "next_cursor result")
    if (cursor %in% seen) {
      rlang::abort(
        "next_cursor returned a cursor that was already used"
      )
    }
    seen <- c(seen, cursor)
    variables[[cursor_variable]] <- cursor
  }

  structure(
    list(
      message = sprintf(
        "GraphQL pagination exceeded max_pages (%d)",
        as.integer(max_pages)
      ),
      call = NULL,
      pages = graphql_pages_result(pages, variables, complete = FALSE)
    ),
    class = c("fabric_graphql_pagination_error", "error", "condition")
  ) |>
    rlang::cnd_signal()
}

graphql_resolve_audience <- function(audience, token, auth_args) {
  if (!is.null(audience)) {
    return(graphql_required_string(audience, "audience"))
  }
  if (is.null(token) && fabric_uses_client_credentials(auth_args)) {
    .fabric_audience$fabric
  } else {
    .fabric_audience$graphql
  }
}

#' Build a Fabric GraphQL cursor extractor
#'
#' @param path Character path from the result's `data` field to a connection
#'   object, for example `"products"` or `c("viewer", "products")`. This is the
#'   parent object that contains the pagination fields, not the `items` field.
#' @param has_next Name of the logical connection field indicating another
#'   page. Fabric commonly uses `"hasNextPage"`.
#' @param end_cursor Name of the connection field containing the opaque cursor.
#'   Fabric commonly uses `"endCursor"`.
#'
#' @return A function suitable for `next_cursor` in
#'   [fabric_graphql_paginate()]. For each page it returns the cursor when
#'   `has_next` is true, otherwise `NULL`.
#' @export
fabric_graphql_cursor <- function(
  path,
  has_next = "hasNextPage",
  end_cursor = "endCursor"
) {
  if (
    !is.character(path) || !length(path) || anyNA(path) || !all(nzchar(path))
  ) {
    rlang::abort("path must contain one or more non-empty field names")
  }
  has_next <- graphql_required_string(has_next, "has_next")
  end_cursor <- graphql_required_string(end_cursor, "end_cursor")

  function(result) {
    if (!inherits(result, "fabric_graphql_result")) {
      rlang::abort(
        "The cursor extractor requires a fabric_graphql_result"
      )
    }
    connection <- graphql_at_path(result$data, path)
    if (is.null(connection)) {
      rlang::abort(
        sprintf(
          "GraphQL pagination path '%s' was not found",
          paste(path, collapse = ".")
        )
      )
    }
    more <- connection[[has_next]]
    if (!is.logical(more) || length(more) != 1L || is.na(more)) {
      rlang::abort(
        sprintf(
          "GraphQL pagination field '%s' must be TRUE or FALSE",
          has_next
        )
      )
    }
    if (!more) {
      return(NULL)
    }
    graphql_required_string(
      connection[[end_cursor]],
      sprintf("GraphQL pagination field '%s'", end_cursor)
    )
  }
}

graphql_execute <- function(
  endpoint,
  query,
  variables,
  operation_name,
  error_policy,
  timeout,
  idempotent,
  credential,
  audience
) {
  body <- list(query = query)
  if (length(variables)) {
    body$variables <- variables
  }
  if (!is.null(operation_name)) {
    body$operationName <- operation_name
  }
  req <- httr2::request(endpoint)
  req <- httr2::req_headers(req, Accept = "application/graphql-response+json")
  req <- httr2::req_body_json(req, body, auto_unbox = TRUE, null = "null")
  req <- httr2::req_timeout(req, timeout)
  response <- .httr2_json(
    req,
    simplifyVector = FALSE,
    bigint_as_char = TRUE,
    credential = credential,
    audience = audience,
    idempotent = idempotent
  )
  graphql_parse_response(response, error_policy = error_policy)
}

graphql_parse_response <- function(
  response,
  error_policy = c("return", "warn", "error")
) {
  error_policy <- match.arg(error_policy)
  if (!is.list(response) || is.null(names(response))) {
    rlang::abort(
      "The GraphQL endpoint returned a malformed response object"
    )
  }
  has_data <- "data" %in% names(response)
  has_errors <- "errors" %in% names(response)
  if (!has_data && !has_errors) {
    rlang::abort(
      "The GraphQL response contains neither data nor errors"
    )
  }
  errors <- response$errors
  if (is.null(errors)) {
    errors <- list()
  } else if (!is.list(errors)) {
    rlang::abort("GraphQL response errors must be a list")
  } else if (
    length(errors) &&
      !is.null(names(errors)) &&
      any(names(errors) %in% c("message", "path", "locations", "extensions"))
  ) {
    errors <- list(errors)
  }
  result <- structure(
    list(
      data = if (has_data) response$data else NULL,
      errors = errors,
      extensions = response$extensions %||% NULL,
      response = response
    ),
    class = c("fabric_graphql_result", "list")
  )

  if (length(errors)) {
    message <- graphql_error_message(errors)
    if (identical(error_policy, "warn")) {
      rlang::warn(message)
    } else if (identical(error_policy, "error")) {
      condition <- structure(
        list(message = message, call = NULL, result = result, errors = errors),
        class = c("fabric_graphql_error", "error", "condition")
      )
      rlang::cnd_signal(condition)
    }
  }
  result
}

graphql_error_message <- function(errors) {
  messages <- vapply(
    errors,
    function(error) {
      if (!is.list(error)) {
        return(as.character(error))
      }
      message <- error$message %||% "Unknown GraphQL error"
      path <- error$path
      if (!is.null(path) && length(path)) {
        message <- paste0(message, " [path: ", paste(path, collapse = "."), "]")
      }
      code <- error$extensions$code %||% NULL
      if (!is.null(code)) {
        message <- paste0(message, " [code: ", code, "]")
      }
      message
    },
    character(1)
  )
  paste0("GraphQL response contains errors: ", paste(messages, collapse = "; "))
}

graphql_pages_result <- function(pages, variables, complete) {
  errors <- unlist(
    lapply(pages, function(page) page$errors),
    recursive = FALSE
  )
  structure(
    list(
      pages = pages,
      errors = errors,
      variables = variables,
      complete = complete
    ),
    class = c("fabric_graphql_pages", "list")
  )
}

graphql_resolve_endpoint <- function(
  api,
  workspace_id = NULL,
  api_base = .fabric_api_base,
  allow_custom_endpoint = FALSE
) {
  graphql_validate_logical(allow_custom_endpoint, "allow_custom_endpoint")
  record <- fabric_as_record(api)
  if (!is.null(record)) {
    type <- tolower(fabric_record_value(record, "type") %||% "")
    if (!identical(type, "graphqlapi")) {
      rlang::abort(
        "api discovery record must be a GraphQLApi item"
      )
    }
    endpoint <- fabric_record_value(
      record,
      "graphql_endpoint",
      "graphQLEndpoint"
    )
    if (!is.null(endpoint)) {
      return(graphql_validate_endpoint(endpoint, allow_custom_endpoint))
    }
    workspace_id <- workspace_id %||%
      fabric_record_value(record, "workspaceId", "workspace_id")
    api <- fabric_record_value(record, "id")
  }

  if (
    is.character(api) &&
      length(api) == 1L &&
      !is.na(api) &&
      fabric_is_guid(api)
  ) {
    if (
      !is.character(workspace_id) ||
        length(workspace_id) != 1L ||
        is.na(workspace_id) ||
        !fabric_is_guid(workspace_id)
    ) {
      rlang::abort(
        "workspace_id must be a GUID when api is a GraphQL API GUID"
      )
    }
    endpoint <- paste0(
      fabric_api_base(api_base, allow_custom_endpoint),
      "/workspaces/",
      workspace_id,
      "/graphqlapis/",
      api,
      "/graphql"
    )
    return(graphql_validate_endpoint(endpoint, allow_custom_endpoint))
  }
  graphql_validate_endpoint(api, allow_custom_endpoint)
}

graphql_validate_endpoint <- function(endpoint, allow_custom_endpoint = FALSE) {
  graphql_validate_logical(allow_custom_endpoint, "allow_custom_endpoint")
  endpoint <- graphql_required_string(endpoint, "api")
  endpoint <- sub("/+$", "", trimws(endpoint))
  parsed <- try(httr2::url_parse(endpoint), silent = TRUE)
  if (
    inherits(parsed, "try-error") ||
      !identical(parsed$scheme, "https") ||
      is.null(parsed$hostname) ||
      !nzchar(parsed$hostname) ||
      nzchar(parsed$username %||% "") ||
      nzchar(parsed$password %||% "") ||
      nzchar(parsed$port %||% "") ||
      length(parsed$query %||% list()) > 0L ||
      nzchar(parsed$fragment %||% "")
  ) {
    rlang::abort("api must be a valid HTTPS GraphQL endpoint")
  }
  if (
    !fabric_host_matches(parsed$hostname, "api.fabric.microsoft.com") &&
      !allow_custom_endpoint
  ) {
    rlang::abort(paste0(
      "api must use a Microsoft Fabric endpoint; set ",
      "allow_custom_endpoint = TRUE only for a trusted custom origin"
    ))
  }
  endpoint
}

graphql_validate_logical <- function(value, name) {
  if (
    !is.logical(value) ||
      length(value) != 1L ||
      is.na(value)
  ) {
    rlang::abort(paste0(name, " must be TRUE or FALSE"))
  }
  invisible(TRUE)
}

graphql_validate_query <- function(query) {
  graphql_required_string(query, "query")
  invisible(TRUE)
}

graphql_validate_variables <- function(variables) {
  if (!is.list(variables)) {
    rlang::abort("variables must be a list")
  }
  if (
    length(variables) &&
      (is.null(names(variables)) ||
        anyNA(names(variables)) ||
        !all(nzchar(names(variables))) ||
        anyDuplicated(names(variables)))
  ) {
    rlang::abort(
      "variables must have unique, non-empty names"
    )
  }
  variables
}

graphql_required_string <- function(value, name) {
  if (
    !is.character(value) ||
      length(value) != 1L ||
      is.na(value) ||
      !nzchar(trimws(value))
  ) {
    rlang::abort(
      sprintf("%s must be one non-empty character value", name)
    )
  }
  value
}

graphql_optional_string <- function(value, name) {
  if (is.null(value)) {
    return(NULL)
  }
  graphql_required_string(value, name)
}

graphql_validate_scalar <- function(
  value,
  type_predicate,
  message,
  value_predicate = function(value) TRUE
) {
  if (
    !type_predicate(value) ||
      length(value) != 1L ||
      is.na(value) ||
      !value_predicate(value)
  ) {
    rlang::abort(message)
  }
  invisible(TRUE)
}

graphql_at_path <- function(value, path) {
  for (field in path) {
    if (!is.list(value) || is.null(value[[field]])) {
      return(NULL)
    }
    value <- value[[field]]
  }
  value
}
