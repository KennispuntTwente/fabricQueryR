#' Query a Microsoft Fabric API for GraphQL
#'
#' Executes a GraphQL document against a Fabric GraphQL endpoint and preserves
#' `data`, `errors`, and `extensions` independently. This is important because
#' a successful HTTP response can contain both partial data and GraphQL errors.
#'
#' @details
#' A direct endpoint has the form
#' `https://api.fabric.microsoft.com/v1/workspaces/{workspace-id}/graphqlapis/{api-id}/graphql`.
#' You can instead pass a GraphQL API GUID with `workspace_id`, or one row from
#' [fabric_graphql_apis()] or [fabric_item()].
#'
#' Interactive/delegated authentication requires the Power BI delegated scope
#' `GraphQLApi.Execute.All`, plus **Run Queries and Mutations** permission on
#' the API. Service principals are also supported by Fabric: use a Fabric API
#' token from a `token_provider` or `access_token`, enable service principals
#' for Fabric APIs in the tenant, and grant the principal API Execute access or
#' a suitable workspace role. With SSO connectivity, the caller also needs the
#' required access to the underlying data source. Saved-credential APIs use the
#' configured connection instead.
#'
#' The default `audience` is the delegated GraphQL scope. Set it to
#' `https://api.fabric.microsoft.com/.default` when a custom provider obtains
#' service-principal tokens. The value is ignored for a static `access_token`.
#'
#' GraphQL POST requests are not retried by default because a document can
#' contain mutations. Set `idempotent = TRUE` only when the operation is safe
#' to repeat.
#'
#' @param api GraphQL HTTPS endpoint, GraphQL API GUID, or one discovered
#'   GraphQLApi record.
#' @param query One non-empty GraphQL document.
#' @param variables Named list of GraphQL variables.
#' @param operation_name Optional operation name for a multi-operation document.
#' @param workspace_id Workspace GUID. Required when `api` is a GraphQL API
#'   GUID, and otherwise inferred from a discovered record.
#' @param error_policy How GraphQL-level errors are handled. `"return"`
#'   preserves them in the result, `"warn"` also emits a warning, and
#'   `"error"` raises a `fabric_graphql_error` carrying the result.
#' @param timeout Positive request timeout in seconds.
#' @param idempotent Logical. Allow shared transient HTTP retries for this POST.
#' @param tenant_id Microsoft Entra tenant ID. Defaults to
#'   `FABRICQUERYR_TENANT_ID`.
#' @param client_id Microsoft Entra application/client ID. Defaults to
#'   `FABRICQUERYR_CLIENT_ID`, with the Azure CLI application ID as fallback.
#' @param access_token Optional bearer token. Supply only one of
#'   `access_token` and `token_provider`.
#' @param token_provider Optional callback returning a bearer token. It may
#'   accept `audience` and `force_refresh` arguments.
#' @param audience OAuth audience/scope passed to the credential.
#' @param api_base Fabric REST API base URL, used to derive endpoints from IDs.
#'
#' @return A `fabric_graphql_result` list with `data`, `errors`, `extensions`,
#'   and `response` (the complete parsed response).
#' @export
#'
#' @examples
#' \dontrun{
#' api <- fabric_graphql_apis("Analytics workspace")[1, ]
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
  timeout = 100,
  idempotent = FALSE,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  access_token = NULL,
  token_provider = NULL,
  audience = .fabric_audience$graphql,
  api_base = .fabric_api_base
) {
  graphql_validate_query(query)
  variables <- graphql_validate_variables(variables)
  operation_name <- graphql_optional_string(
    operation_name,
    "operation_name"
  )
  error_policy <- match.arg(error_policy)
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
    api_base = api_base
  )
  credential <- fabric_credential(
    tenant_id = tenant_id,
    client_id = client_id,
    access_token = access_token,
    token_provider = token_provider
  )

  graphql_execute(
    endpoint,
    query = query,
    variables = variables,
    operation_name = operation_name,
    error_policy = error_policy,
    timeout = timeout,
    idempotent = idempotent,
    credential = credential,
    audience = audience
  )
}

#' Paginate a Microsoft Fabric GraphQL operation
#'
#' Repeats [fabric_graphql_query()] while `next_cursor` returns a cursor. The
#' callback makes pagination independent of the user's schema shape.
#'
#' @param next_cursor Function accepting a `fabric_graphql_result` and returning
#'   the next opaque cursor, or `NULL` when pagination is complete. Use
#'   [fabric_graphql_cursor()] for Fabric's normal connection fields.
#' @param cursor_variable Name of the GraphQL variable that receives the cursor.
#' @param max_pages Positive maximum number of requests.
#' @inheritParams fabric_graphql_query
#'
#' @return A `fabric_graphql_pages` list with `pages`, combined `errors`, and
#'   the final `variables`.
#' @export
#'
#' @examples
#' \dontrun{
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
  timeout = 100,
  idempotent = FALSE,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  access_token = NULL,
  token_provider = NULL,
  audience = .fabric_audience$graphql,
  api_base = .fabric_api_base
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
      max_pages != as.integer(max_pages)
  ) {
    rlang::abort("max_pages must be one positive integer")
  }
  variables <- graphql_validate_variables(variables)
  error_policy <- match.arg(error_policy)
  pages <- list()
  seen <- character()

  for (page_number in seq_len(as.integer(max_pages))) {
    result <- fabric_graphql_query(
      api = api,
      query = query,
      variables = variables,
      operation_name = operation_name,
      workspace_id = workspace_id,
      error_policy = error_policy,
      timeout = timeout,
      idempotent = idempotent,
      tenant_id = tenant_id,
      client_id = client_id,
      access_token = access_token,
      token_provider = token_provider,
      audience = audience,
      api_base = api_base
    )
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

#' Build a Fabric GraphQL cursor extractor
#'
#' @param path Character path from the result's `data` field to a connection
#'   object, for example `"products"` or `c("viewer", "products")`.
#' @param has_next Name of the connection field indicating another page.
#' @param end_cursor Name of the connection field containing the opaque cursor.
#'
#' @return A function suitable for `next_cursor` in
#'   [fabric_graphql_paginate()].
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
  api_base = .fabric_api_base
) {
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
      return(graphql_validate_endpoint(endpoint))
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
    return(paste0(
      fabric_api_base(api_base),
      "/workspaces/",
      workspace_id,
      "/graphqlapis/",
      api,
      "/graphql"
    ))
  }
  graphql_validate_endpoint(api)
}

graphql_validate_endpoint <- function(endpoint) {
  endpoint <- graphql_required_string(endpoint, "api")
  endpoint <- sub("/+$", "", trimws(endpoint))
  parsed <- try(httr2::url_parse(endpoint), silent = TRUE)
  if (
    inherits(parsed, "try-error") ||
      !identical(parsed$scheme, "https") ||
      is.null(parsed$hostname) ||
      !nzchar(parsed$hostname)
  ) {
    rlang::abort("api must be a valid HTTPS GraphQL endpoint")
  }
  endpoint
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
