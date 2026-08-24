.graphql_introspection_query <- paste(
  c(
    "query IntrospectionQuery {",
    "  __schema {",
    "    queryType { name }",
    "    mutationType { name }",
    "    subscriptionType { name }",
    "    types { ...FullType }",
    "    directives {",
    "      name",
    "      description",
    "      locations",
    "      args { ...InputValue }",
    "    }",
    "  }",
    "}",
    "fragment FullType on __Type {",
    "  kind",
    "  name",
    "  description",
    "  fields(includeDeprecated: true) {",
    "    name",
    "    description",
    "    args { ...InputValue }",
    "    type { ...TypeRef }",
    "    isDeprecated",
    "    deprecationReason",
    "  }",
    "  inputFields { ...InputValue }",
    "  interfaces { ...TypeRef }",
    "  enumValues(includeDeprecated: true) {",
    "    name",
    "    description",
    "    isDeprecated",
    "    deprecationReason",
    "  }",
    "  possibleTypes { ...TypeRef }",
    "}",
    "fragment InputValue on __InputValue {",
    "  name",
    "  description",
    "  type { ...TypeRef }",
    "  defaultValue",
    "}",
    "fragment TypeRef on __Type {",
    "  kind",
    "  name",
    "  ofType {",
    "    kind",
    "    name",
    "    ofType {",
    "      kind",
    "      name",
    "      ofType {",
    "        kind",
    "        name",
    "        ofType {",
    "          kind",
    "          name",
    "          ofType {",
    "            kind",
    "            name",
    "            ofType {",
    "              kind",
    "              name",
    "              ofType { kind name }",
    "            }",
    "          }",
    "        }",
    "      }",
    "    }",
    "  }",
    "}"
  ),
  collapse = "\n"
)

#' Run a query against a Fabric GraphQL API
#'
#' Sends a GraphQL query or mutation to an **API for GraphQL** item and returns
#' the result as a nested R list. Use this when a Fabric API already exposes the
#' Lakehouse, Warehouse, or SQL Database data you need
#'
#' @section Before you query:
#' Before using this function, create an **API for GraphQL** item in a Fabric
#' workspace, connect its data source, and choose which tables, fields, queries,
#' and mutations the API exposes. Fabric's built-in GraphQL editor and schema
#' explorer are the easiest places to design and test a document before copying
#' it to R
#'
#' Mutation availability depends on the configured source. Fabric Warehouse and
#' SQL Database sources can expose supported mutations, while Lakehouse and
#' mirrored SQL analytics endpoint sources are read-only and expose queries only
#'
#' The easiest input is an item from [fabric_graphql_apis()]. You can instead
#' supply the API's endpoint, or its ID together with `workspace_id`
#'
#' @section Permissions and authentication:
#' Interactive authentication requires the Power BI delegated scope
#' `GraphQLApi.Execute.All`, plus **Run Queries and Mutations** permission on
#' the API. Service principals are also supported by Fabric: request a Fabric
#' API token with `auth_args` or pass one through `token`, enable service
#' principals for Fabric APIs in the tenant, and grant the principal API
#' Execute access or a suitable workspace role. With SSO connectivity, the
#' caller also needs the required access to the underlying data source
#' Saved-credential APIs use the configured connection instead
#'
#' Most users can leave `audience = NULL`; 'fabricQueryR' chooses the
#' documented scope for the sign-in flow. Set it only for a custom identity
#' provider
#'
#' @section Retries and service limits:
#' GraphQL POST requests are not retried by default because a document can
#' contain mutations. Set `idempotent = TRUE` only when the operation is safe
#' to repeat
#'
#' Fabric returns at most 100 items by default and permits at most 100,000 items
#' across pagination. Each response is limited to 64 MB, each request to 100
#' seconds, and query nesting to 10 levels. Use smaller pages and filtered query
#' partitions when a result could approach these service limits
#'
#' Large integers outside R's exact numeric range are returned as
#' character values so identifiers and other large integer fields are not
#' rounded
#'
#' @param api GraphQL endpoint, API ID, or one discovered GraphQLApi record. An
#'   item from [fabric_graphql_apis()] is usually easiest
#'   because it supplies the endpoint and workspace ID
#' @param query One GraphQL document containing a query or mutation. Use
#'   variables for changing values instead of pasting values into this string
#' @param variables Named list of values for variables declared in `query`
#'   One-element values are normally sent as scalars. Wrap a one-element list
#'   variable in [I()], for example `list(ids = I("x"))`, to send it as an array
#' @param operation_name Optional operation name. Supply it when the document
#'   contains more than one named operation; otherwise leave `NULL`
#' @param workspace_id Workspace GUID. Required when `api` is a GraphQL API
#'   GUID, and otherwise inferred from a discovered record
#' @param error_policy How GraphQL-level errors are handled. `"return"`
#'   lets the caller inspect partial data and errors; `"warn"` also makes errors
#'   visible immediately; `"error"` stops and attaches the result to a
#'   `fabric_graphql_error`. HTTP/authentication failures always stop
#' @param timeout Maximum time in seconds for the request. The default allows
#'   Fabric's own 100-second query timeout response to arrive
#' @param idempotent Logical. Permit retries after transient HTTP failures
#'   `TRUE` is normally suitable for a read-only query, but not for a mutation
#'   that could be applied twice
#' @param tenant_id Microsoft Entra tenant ID. Defaults to
#'   `FABRICQUERYR_TENANT_ID`
#' @param client_id Microsoft Entra application/client ID. Defaults to
#'   `FABRICQUERYR_CLIENT_ID`, with the Azure CLI application ID as fallback
#' @param token Optional access token or token-provider function. Leave `NULL`
#'   to let 'fabricQueryR' use its normal sign-in flow for a Microsoft Fabric
#'   host. A custom API endpoint, including an API Management gateway, requires
#'   an explicitly supplied token or provider so an automatically acquired
#'   Fabric credential is not forwarded to another host
#' @param auth_args Additional sign-in options passed to
#'   [AzureAuth::get_azure_token()]
#' @param audience OAuth audience/scope passed to the credential. `NULL`
#'   selects the documented scope from the authentication flow. Set this only
#'   for a custom token provider or unusual identity flow
#' @param api_base Fabric REST API base URL used to derive endpoints from IDs
#'   Most users should keep the default
#'
#' @return A `fabric_graphql_result` list with `data`, `errors`, `extensions`,
#'   and `response` (the complete parsed response). `data` follows the nested
#'   shape requested in the GraphQL document and is usually a combination of
#'   named lists and vectors, not a tibble. Because GraphQL can return partial
#'   data, inspect `errors` even when `data` is present
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
#' # Discover an API for GraphQL item instead of copying its endpoint or ID
#' workspace <- fabric_workspaces()[[1L]]
#' api <- fabric_graphql_apis(workspace)[[1L]]
#'
#' # Keep the filter value in variables rather than inserting it into the query
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
#'
#' # GraphQL can return data and errors in the same response; inspect both
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
  api_base = .fabric_api_base
) {
  # 1 Prepare the request --------------------------------------------------------------------------

  # Validate caller-controlled values once and keep the resolved endpoint and
  # credential together for the request

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
    api_base = api_base
  )

  # 2 Execute and return the query -----------------------------------------------------------------

  # Execute and return the query only after the request inputs are ready

  graphql_execute_context(context, variables)
}

#' Inspect a Fabric GraphQL schema
#'
#' Runs the standard GraphQL introspection query against an **API for GraphQL**
#' item. The returned schema retains the service's nested type references,
#' fields, input values, enum values, and directives so callers can explore the
#' API without assuming how Fabric named its generated objects
#'
#' Microsoft Fabric disables runtime introspection by default. A workspace
#' administrator must enable it under **API Settings > Introspection**. When it
#' must remain disabled, use **Export schema** in the Fabric portal instead;
#' schema export remains available independently of the runtime setting
#'
#' @inheritParams fabric_graphql_query
#'
#' @return A `fabric_graphql_schema` list containing the standard `__schema`
#'   fields. The original GraphQL response and its (normally empty) errors are
#'   available in the `response` and `errors` attributes
#' @references
#' [Fabric API for GraphQL introspection and schema export](https://learn.microsoft.com/en-us/fabric/data-engineering/api-graphql-introspection-schema-export)
#' @export
#'
#' @examples
#' \dontrun{
#' # Discover the GraphQL API whose schema you want to inspect
#' workspace <- fabric_workspaces()[[1L]]
#' api <- fabric_graphql_apis(workspace)[[1L]]
#'
#' # Request the standard GraphQL introspection schema
#' schema <- fabric_graphql_schema(api)
#'
#' # List its named types to learn what can be queried
#' vapply(schema$types, `[[`, character(1), "name")
#' }
fabric_graphql_schema <- function(
  api,
  workspace_id = NULL,
  timeout = 110,
  idempotent = TRUE,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  token = NULL,
  auth_args = list(),
  audience = NULL,
  api_base = .fabric_api_base
) {
  # 1 Request the standard schema -----------------------------------------------------------------

  # Introspection is read-only, so transient transport retries are safe when
  # the caller leaves the idempotent default enabled

  result <- fabric_graphql_query(
    api = api,
    query = .graphql_introspection_query,
    operation_name = "IntrospectionQuery",
    workspace_id = workspace_id,
    error_policy = "return",
    timeout = timeout,
    idempotent = idempotent,
    tenant_id = tenant_id,
    client_id = client_id,
    token = token,
    auth_args = auth_args,
    audience = audience,
    api_base = api_base
  )

  # 2 Reject unavailable or partial introspection --------------------------------------------------

  # A partial type graph is unsafe for discovery and code generation. Fabric's
  # default-disabled setting is the common cause, so make its remedy explicit

  schema <- if (is.list(result$data)) {
    result$data[["__schema"]] %||% NULL
  } else {
    NULL
  }
  if (length(result$errors) || !is.list(schema)) {
    graphql_schema_abort(result)
  }

  # 3 Return the service schema unchanged ----------------------------------------------------------

  structure(
    schema,
    errors = result$errors,
    response = result$response,
    class = c("fabric_graphql_schema", "list")
  )
}

#' Read all pages from a Fabric GraphQL query
#'
#' Repeats [fabric_graphql_query()] until the API reports that no more pages are
#' available. Because every GraphQL schema can store pagination information in
#' a different place, `next_cursor` tells the function where to find it
#'
#' @param next_cursor Function accepting a `fabric_graphql_result` and returning
#'   the next opaque cursor, or `NULL` when pagination is complete. Use
#'   [fabric_graphql_cursor()] for Fabric's normal connection fields
#' @param cursor_variable Name of the GraphQL variable that receives the next
#'   cursor, commonly `"after"`. It must match the variable declared in `query`
#' @param max_pages Positive maximum number of requests. This guards against a
#'   faulty or unexpectedly large pagination loop
#' @inheritParams fabric_graphql_query
#'
#' @return A `fabric_graphql_pages` list with `pages`, combined `errors`, and
#'   the final `variables`. `pages` contains one `fabric_graphql_result` per
#'   request and `complete` is `TRUE` when the callback reported no next page
#'   Results are kept page-by-page because the requested schema shape can vary
#' @export
#'
#' @examples
#' \dontrun{
#' # Discover the GraphQL API that exposes the Products query
#' workspace <- fabric_workspaces()[[1L]]
#' api <- fabric_graphql_apis(workspace)[[1L]]
#'
#' # Fetch pages until the helper sees no next cursor
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
#' pages$complete
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
  api_base = .fabric_api_base
) {
  # 1 Validate pagination inputs -------------------------------------------------------------------

  # The callback controls when paging ends, while `max_pages` is a safety cap

  if (!is.function(next_cursor)) {
    .fabric_abort("next_cursor must be a function")
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
    .fabric_abort("max_pages must be one positive integer")
  }
  variables <- graphql_validate_variables(variables)
  error_policy <- match.arg(error_policy, c("return", "warn", "error"))

  # 2 Prepare shared request state -----------------------------------------------------------------

  # Resolve authentication and the endpoint once, then reuse them for pages

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
    api_base = api_base
  )
  pages <- list()
  seen <- character()

  # 3 Read pages -----------------------------------------------------------------------------------

  # Feed each opaque cursor back through the named GraphQL variable and reject
  # repeated cursors before they can create an endless loop

  for (page_number in seq_len(as.integer(max_pages))) {
    result <- graphql_execute_context(context, variables)
    pages[[page_number]] <- result
    cursor <- next_cursor(result)
    if (is.null(cursor)) {
      return(graphql_pages_result(pages, variables, complete = TRUE))
    }
    cursor <- graphql_required_string(cursor, "next_cursor result")
    if (cursor %in% seen) {
      .fabric_abort(
        "next_cursor returned a cursor that was already used"
      )
    }
    seen <- c(seen, cursor)
    variables[[cursor_variable]] <- cursor
  }

  # 4 Report an incomplete result ------------------------------------------------------------------

  # Turn the final state into clear output for the caller

  .fabric_abort(
    "GraphQL pagination exceeded {.arg max_pages} ({as.integer(max_pages)})",
    .format = TRUE,
    class = "fabric_graphql_pagination_error",
    pages = graphql_pages_result(pages, variables, complete = FALSE),
    call = NULL
  )
}

#' Locate pagination information in a GraphQL result
#'
#' Creates the `next_cursor` function used by [fabric_graphql_paginate()] for the
#' common `hasNextPage` and `endCursor` pagination fields
#'
#' @param path Character path from the result's `data` field to a connection
#'   object, for example `"products"` or `c("viewer", "products")`. This is the
#'   parent object that contains the pagination fields, not the `items` field
#' @param has_next Name of the logical connection field indicating another
#'   page. Fabric commonly uses `"hasNextPage"`
#' @param end_cursor Name of the connection field containing the opaque cursor
#'   Fabric commonly uses `"endCursor"`
#'
#' @return A function suitable for `next_cursor` in
#'   [fabric_graphql_paginate()]. For each page it returns the cursor when
#'   `has_next` is true, otherwise `NULL`
#' @examples
#' # Build a reusable extractor for a GraphQL connection named "products"
#' next_cursor <- fabric_graphql_cursor("products")
#'
#' # This small local result shows the response shape expected by the extractor
#' page <- structure(
#'   list(data = list(products = list(
#'     hasNextPage = TRUE,
#'     endCursor = "opaque-cursor"
#'   ))),
#'   class = c("fabric_graphql_result", "list")
#' )
#'
#' # TRUE plus a cursor tells the paginator to request another page
#' next_cursor(page)
#' @export
fabric_graphql_cursor <- function(
  path,
  has_next = "hasNextPage",
  end_cursor = "endCursor"
) {
  # 1 Validate field names -------------------------------------------------------------------------

  # Capture valid field names now so the returned callback stays simple

  path <- graphql_validate_path(path)
  has_next <- graphql_required_string(has_next, "has_next")
  end_cursor <- graphql_required_string(end_cursor, "end_cursor")

  # 2 Build the cursor callback --------------------------------------------------------------------

  # Build the cursor callback from the validated values required by the next step

  function(result) {
    # Cursor extraction only works on results produced by this package
    if (!inherits(result, "fabric_graphql_result")) {
      .fabric_abort(
        "The cursor extractor requires a fabric_graphql_result"
      )
    }

    # Follow the configured field path to the connection object
    connection <- graphql_at_path(result$data, path)
    if (is.null(connection)) {
      .fabric_abort(
        sprintf(
          "GraphQL pagination path '%s' was not found",
          paste(path, collapse = ".")
        )
      )
    }

    # The service must return one unambiguous continuation flag
    more <- connection[[has_next]]
    if (!is.logical(more) || length(more) != 1L || is.na(more)) {
      .fabric_abort(
        sprintf(
          "GraphQL pagination field '%s' must be TRUE or FALSE",
          has_next
        )
      )
    }

    # A missing next page needs no cursor value
    if (!more) {
      return(NULL)
    }
    graphql_required_string(
      connection[[end_cursor]],
      sprintf("GraphQL pagination field '%s'", end_cursor)
    )
  }
}

#' Collect paged GraphQL row objects into a tibble
#'
#' Combines row objects from a caller-selected field in every result returned
#' by [fabric_graphql_paginate()]. The explicit `path` is relative to each
#' page's `data` field because GraphQL response shapes are schema-defined and
#' cannot be inferred safely
#'
#' Scalar fields become ordinary tibble columns. Nested objects and arrays stay
#' as list-columns and are never flattened. Fields introduced on later pages
#' are added in first-seen order, with missing or GraphQL `null` scalar values
#' represented by typed `NA` values when their type can be inferred. Exact
#' integer strings returned by [fabric_graphql_query()] remain character data;
#' integer-valued numeric entries in the same field are promoted to character
#' rather than coercing a large integer to an inexact double
#'
#' A successful result has class `fabric_graphql_rows` and reports completion,
#' page count, path, and GraphQL errors in its printed header and attributes.
#' Use `attr(rows, "errors")` to inspect partial GraphQL errors. If pagination
#' stopped before `next_cursor` reported completion, including at `max_pages`,
#' the function raises `fabric_graphql_collection_error`; its `partial_data`
#' field contains the rows collected so far and is explicitly marked
#' incomplete
#'
#' @param pages A `fabric_graphql_pages` result from
#'   [fabric_graphql_paginate()]. Requiring that result, rather than an
#'   unverified single response, lets the function enforce completion
#' @param path Character path from each page's `data` field to the list of row
#'   objects, for example `c("viewer", "products", "items")`
#'
#' @return A `fabric_graphql_rows` tibble. Attributes `complete`, `errors`,
#'   `page_count`, and `path` retain collection metadata
#' @references
#' [Fabric API for GraphQL limits](https://learn.microsoft.com/en-us/fabric/data-engineering/api-graphql-limits)
#'
#' [Fabric GraphQL aggregation and pagination shape](https://learn.microsoft.com/en-us/fabric/data-engineering/api-graphql-aggregations)
#' @export
#'
#' @examples
#' \dontrun{
#' # Discover the GraphQL API, then fetch every Products page
#' workspace <- fabric_workspaces()[[1L]]
#' api <- fabric_graphql_apis(workspace)[[1L]]
#' pages <- fabric_graphql_paginate(
#'   api,
#'   query = paste(
#'     "query Products($first: Int!, $after: String) {",
#'     "  products(first: $first, after: $after) {",
#'     "    items { id name details { category } }",
#'     "    hasNextPage endCursor",
#'     "  }",
#'     "}"
#'   ),
#'   variables = list(first = 100L, after = NULL),
#'   next_cursor = fabric_graphql_cursor("products")
#' )
#'
#' # Combine nested item rows from every page into one tibble
#' rows <- fabric_graphql_collect(pages, c("products", "items"))
#' attr(rows, "complete")
#' attr(rows, "errors")
#' }
fabric_graphql_collect <- function(pages, path) {
  # 1 Validate the collection contract -------------------------------------------------------------

  if (!inherits(pages, "fabric_graphql_pages")) {
    .fabric_abort(
      paste(
        "pages must be a fabric_graphql_pages result from",
        "fabric_graphql_paginate()"
      )
    )
  }
  path <- graphql_validate_path(path)
  if (!is.list(pages$pages)) {
    .fabric_abort("pages$pages must be a list of GraphQL results")
  }
  complete <- pages$complete
  graphql_validate_scalar(
    complete,
    is.logical,
    "pages$complete must be TRUE or FALSE"
  )

  # 2 Locate and bind the selected row objects -----------------------------------------------------

  rows <- graphql_collect_page_rows(pages$pages, path)
  result <- graphql_rows_result(
    rows,
    complete = complete,
    errors = pages$errors %||% list(),
    page_count = length(pages$pages),
    path = path
  )

  # 3 Refuse to present an incomplete collection as a normal tibble -------------------------------

  if (!complete) {
    .fabric_abort(
      c(
        "GraphQL row collection is incomplete",
        "x" = paste(
          "Pagination stopped before the API reported completion, either at",
          "{.arg max_pages} or Fabric's 100,000-item pagination limit"
        ),
        "i" = "Partial rows are available in {.field partial_data}"
      ),
      .format = TRUE,
      class = "fabric_graphql_collection_error",
      partial_data = result,
      pages = pages,
      errors = attr(result, "errors"),
      call = NULL
    )
  }

  result
}

#' Print collected GraphQL rows
#'
#' @param x A `fabric_graphql_rows` tibble returned by
#'   [fabric_graphql_collect()]
#' @param ... Additional arguments passed to the tibble print method
#' @return `x`, invisibly
#' @export
print.fabric_graphql_rows <- function(x, ...) {
  complete <- isTRUE(attr(x, "complete"))
  errors <- attr(x, "errors") %||% list()
  page_count <- attr(x, "page_count") %||% NA_integer_
  pagination <- if (complete) "complete" else "incomplete"
  pages <- graphql_count_label(page_count, "page")
  errors <- graphql_count_label(length(errors), "GraphQL error")
  app <- cli::start_app(output = "stdout", .auto_close = FALSE)
  on.exit(cli::stop_app(app), add = TRUE)
  cli::cli_text(
    "# Fabric GraphQL: pagination {pagination}; {pages}; {errors}"
  )
  NextMethod()
}

# Validate shared GraphQL request inputs and resolve service state. Returns a
# reusable context used by single-page and paged public entry points
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
  api_base
) {
  # 1 Validate query settings ----------------------------------------------------------------------

  # Report input problems before constructing credentials or endpoints

  graphql_required_string(query, "query")
  if (!is.null(operation_name)) {
    operation_name <- graphql_required_string(operation_name, "operation_name")
  }
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

  # 2 Resolve endpoint and authentication ----------------------------------------------------------

  # Resolve endpoint and authentication once so later steps use one consistent value

  endpoint <- graphql_resolve_endpoint(
    api,
    workspace_id = workspace_id,
    api_base = api_base
  )
  fabric_require_explicit_custom_token(endpoint, token, "api")
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

  # 3 Return reusable request state ----------------------------------------------------------------

  # Return reusable request state in the stable form expected by the caller

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

# Execute one page from `context` with `variables`. Returns a parsed GraphQL
# result and keeps pagination from duplicating request argument plumbing
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

# Choose the explicit or flow-appropriate token `audience`. Returns one scope
# string used while the GraphQL request context is built
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

# Send one GraphQL request and parse its response. Returns a
# `fabric_graphql_result` for the public query and pagination functions
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

# Validate a decoded GraphQL `response` and apply `error_policy`. Returns a
# stable result object used by both public query entry points
graphql_parse_response <- function(
  response,
  error_policy = c("return", "warn", "error")
) {
  # 1 Validate the response shape ------------------------------------------------------------------

  # GraphQL may return data, errors, or both, but neither means the response is
  # not a valid GraphQL result

  error_policy <- match.arg(error_policy)
  if (!is.list(response) || is.null(names(response))) {
    .fabric_abort(
      "The GraphQL endpoint returned a malformed response object"
    )
  }
  has_data <- "data" %in% names(response)
  has_errors <- "errors" %in% names(response)
  if (!has_data && !has_errors) {
    .fabric_abort(
      "The GraphQL response contains neither data nor errors"
    )
  }
  errors <- response$errors
  if (is.null(errors)) {
    errors <- list()
  } else if (!is.list(errors)) {
    .fabric_abort("GraphQL response errors must be a list")
  } else if (
    length(errors) &&
      !is.null(names(errors)) &&
      any(names(errors) %in% c("message", "path", "locations", "extensions"))
  ) {
    errors <- list(errors)
  }

  # 2 Build the result -----------------------------------------------------------------------------

  # Build the result from the validated values required by the next step

  result <- structure(
    list(
      data = if (has_data) response$data else NULL,
      errors = errors,
      extensions = response$extensions %||% NULL,
      response = response
    ),
    class = c("fabric_graphql_result", "list")
  )

  # 3 Apply the requested error policy -------------------------------------------------------------

  # Apply the requested error policy before the result is returned

  if (length(errors)) {
    message <- graphql_error_message(errors)
    if (identical(error_policy, "warn")) {
      .fabric_warn(message)
    } else if (identical(error_policy, "error")) {
      .fabric_abort(
        message,
        class = "fabric_graphql_error",
        result = result,
        errors = errors,
        call = NULL
      )
    }
  }
  result
}

# Turn GraphQL `errors` into one readable message. Returns text used for warning
# and error policies without discarding path or service-code context
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

# Stop after unsuccessful introspection. The typed condition retains the
# GraphQL result while directing Fabric users to the documented setting
graphql_schema_abort <- function(result) {
  service_message <- if (length(result$errors)) {
    paste0(" ", graphql_error_message(result$errors))
  } else {
    " The response did not contain a __schema object."
  }
  .fabric_abort(
    c(
      paste0("GraphQL schema introspection failed.", service_message),
      "i" = paste(
        "Microsoft Fabric disables introspection by default; ask a workspace",
        "admin to enable API Settings > Introspection, then retry"
      ),
      "i" = paste(
        "If runtime introspection must remain disabled, use Export schema in",
        "the Fabric portal"
      )
    ),
    class = "fabric_graphql_introspection_error",
    result = result,
    errors = result$errors,
    call = NULL
  )
}

# Validate a GraphQL field path. Returns the unchanged path for cursor and row
# extraction helpers
graphql_validate_path <- function(path) {
  if (
    !is.character(path) ||
      !length(path) ||
      anyNA(path) ||
      !all(nzchar(path))
  ) {
    .fabric_abort("path must contain one or more non-empty field names")
  }
  path
}

# Locate row arrays in each GraphQL page. Returns one flat list of named row
# objects, tolerating a missing/null path only on a page with retained errors
graphql_collect_page_rows <- function(pages, path) {
  rows <- list()
  for (page_number in seq_along(pages)) {
    page <- pages[[page_number]]
    if (!inherits(page, "fabric_graphql_result")) {
      .fabric_abort(sprintf(
        "GraphQL page %d is not a fabric_graphql_result",
        page_number
      ))
    }
    located <- graphql_find_path(page$data, path)
    if (!located$found || is.null(located$value)) {
      if (graphql_errors_overlap_path(page$errors, path)) {
        next
      }
      .fabric_abort(sprintf(
        "GraphQL row path '%s' was not found on page %d",
        paste(path, collapse = "."),
        page_number
      ))
    }
    page_rows <- located$value
    if (!is.list(page_rows)) {
      .fabric_abort(sprintf(
        "GraphQL row path '%s' must contain a list on page %d",
        paste(path, collapse = "."),
        page_number
      ))
    }
    for (row_number in seq_along(page_rows)) {
      row <- page_rows[[row_number]]
      if (
        !is.list(row) ||
          is.null(names(row)) ||
          anyNA(names(row)) ||
          !all(nzchar(names(row))) ||
          anyDuplicated(names(row))
      ) {
        .fabric_abort(sprintf(
          paste0(
            "GraphQL row %d on page %d must be an object with unique, ",
            "non-empty field names"
          ),
          row_number,
          page_number
        ))
      }
      rows[[length(rows) + 1L]] <- row
    }
  }
  rows
}

# Bind named GraphQL row objects by first-seen column name. Returns a tibble
# whose nested fields remain list-columns
graphql_rows_result <- function(
  rows,
  complete,
  errors,
  page_count,
  path
) {
  column_names <- unique(unlist(lapply(rows, names), use.names = FALSE))
  columns <- stats::setNames(
    lapply(
      column_names,
      function(name) {
        values <- lapply(
          rows,
          function(row) {
            if (name %in% names(row)) row[[name]] else NULL
          }
        )
        graphql_rows_column(values, name)
      }
    ),
    column_names
  )
  result <- tibble::new_tibble(columns, nrow = length(rows))
  class(result) <- c("fabric_graphql_rows", class(result))
  attr(result, "complete") <- complete
  attr(result, "errors") <- errors
  attr(result, "page_count") <- as.integer(page_count)
  attr(result, "path") <- path
  result
}

# Combine one GraphQL field across rows. Returns an atomic vector when a common
# scalar type exists, otherwise a list-column for nested/array/unknown values
graphql_rows_column <- function(values, name) {
  non_null <- Filter(Negate(is.null), values)
  if (!length(non_null)) {
    return(rep(list(NULL), length(values)))
  }
  complex <- vapply(
    non_null,
    function(value) is.list(value) || length(value) != 1L,
    logical(1)
  )
  if (any(complex)) {
    if (!all(complex)) {
      .fabric_abort(sprintf(
        paste0(
          "GraphQL field '%s' changes between nested and scalar values ",
          "across rows; transform the page values explicitly before collecting"
        ),
        name
      ))
    }
    return(values)
  }
  if (graphql_rows_mixed_integer_strings(non_null)) {
    return(vapply(
      values,
      graphql_rows_integer_character,
      character(1)
    ))
  }

  prototype <- tryCatch(
    do.call(vctrs::vec_ptype_common, unname(non_null)),
    error = function(error) {
      .fabric_abort(
        sprintf(
          paste0(
            "GraphQL field '%s' has incompatible scalar types across rows; ",
            "transform the page values explicitly before collecting"
          ),
          name
        ),
        parent = error
      )
    }
  )
  pieces <- lapply(
    values,
    function(value) {
      if (is.null(value)) {
        vctrs::vec_init(prototype, 1L)
      } else {
        vctrs::vec_cast(value, prototype)
      }
    }
  )
  do.call(vctrs::vec_c, unname(pieces))
}

# Detect safe character promotion for a column containing JSON integers on
# both sides of jsonlite's exact-large-integer boundary
graphql_rows_mixed_integer_strings <- function(values) {
  kinds <- vapply(
    values,
    function(value) {
      if (is.character(value)) {
        "character"
      } else if (is.numeric(value)) {
        "numeric"
      } else {
        "other"
      }
    },
    character(1)
  )
  if (
    !all(kinds %in% c("character", "numeric")) ||
      !all(c("character", "numeric") %in% kinds)
  ) {
    return(FALSE)
  }
  all(vapply(
    values,
    function(value) {
      if (is.character(value)) {
        grepl("^[+-]?[0-9]+$", value)
      } else {
        is.finite(value) && value == floor(value)
      }
    },
    logical(1)
  ))
}

# Render one integer-valued scalar as exact character data. Returns NA for a
# missing/null field
graphql_rows_integer_character <- function(value) {
  if (is.null(value)) {
    return(NA_character_)
  }
  if (is.character(value)) {
    return(value)
  }
  format(value, scientific = FALSE, trim = TRUE, digits = 22L)
}

# Format one count for the GraphQL rows print header
graphql_count_label <- function(value, label) {
  suffix <- if (identical(as.integer(value), 1L)) "" else "s"
  paste(value, paste0(label, suffix))
}

# Check whether a page error can explain a null or absent selected row path
# Returns one logical so unrelated errors cannot conceal a misspelled path
graphql_errors_overlap_path <- function(errors, path) {
  if (!length(errors)) {
    return(FALSE)
  }
  any(vapply(
    errors,
    function(error) {
      error_path <- if (is.list(error)) error$path %||% NULL else NULL
      if (!length(error_path)) {
        return(FALSE)
      }
      error_path <- as.character(unlist(error_path, use.names = FALSE))
      shared <- min(length(error_path), length(path))
      identical(error_path[seq_len(shared)], path[seq_len(shared)])
    },
    logical(1)
  ))
}

# Combine page results, final `variables`, and completion state. Returns the
# stable pagination object exposed by `fabric_graphql_paginate()`
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

# Resolve a discovery record, GUID pair, or URL into a GraphQL endpoint
# Returns a validated HTTPS URL for the request context
graphql_resolve_endpoint <- function(
  api,
  workspace_id = NULL,
  api_base = .fabric_api_base
) {
  # 1 Read a discovery record ----------------------------------------------------------------------

  # Prefer the ready-to-use endpoint supplied by item discovery

  record <- fabric_as_record(api)
  if (!is.null(record)) {
    type <- tolower(fabric_record_value(record, "type") %||% "")
    if (!identical(type, "graphqlapi")) {
      .fabric_abort(
        "api discovery record must be a GraphQLApi item"
      )
    }
    endpoint <- fabric_record_value(
      record,
      "graphql_endpoint",
      "graphQLEndpoint"
    )
    record_workspace_id <- fabric_record_value(
      record,
      "workspaceId",
      "workspace_id"
    )

    if (!is.null(workspace_id) && !is.null(record_workspace_id)) {
      if (
        !is.character(workspace_id) ||
          length(workspace_id) != 1L ||
          is.na(workspace_id) ||
          !fabric_is_guid(workspace_id)
      ) {
        .fabric_abort("workspace_id must be a GUID")
      }
      if (
        !is.character(record_workspace_id) ||
          length(record_workspace_id) != 1L ||
          is.na(record_workspace_id) ||
          !fabric_is_guid(record_workspace_id)
      ) {
        .fabric_abort("api discovery record workspaceId must be a GUID")
      }
      if (!identical(tolower(workspace_id), tolower(record_workspace_id))) {
        .fabric_abort(
          "workspace_id conflicts with the api discovery record workspaceId"
        )
      }
    }

    if (!is.null(endpoint)) {
      return(graphql_validate_endpoint(endpoint))
    }
    workspace_id <- workspace_id %||%
      record_workspace_id
    api <- fabric_record_value(record, "id")
  }

  # 2 Build an endpoint from IDs -------------------------------------------------------------------

  # Build an endpoint from IDs from the validated values required by the next step

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
      .fabric_abort(
        "workspace_id must be a GUID when api is a GraphQL API GUID"
      )
    }
    endpoint <- paste0(
      fabric_api_base(api_base),
      "/workspaces/",
      workspace_id,
      "/graphqlapis/",
      api,
      "/graphql"
    )

    return(graphql_validate_endpoint(endpoint))
  }

  # 3 Validate a direct endpoint -------------------------------------------------------------------

  # Check a direct endpoint now so later code can rely on safe input

  graphql_validate_endpoint(api)
}

# Validate `endpoint`. Returns a normalized HTTPS URL.
graphql_validate_endpoint <- function(endpoint) {
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
      (nzchar(parsed$port %||% "") && !identical(parsed$port, "443")) ||
      length(parsed$query %||% list()) > 0L ||
      nzchar(parsed$fragment %||% "")
  ) {
    .fabric_abort("api must be a valid HTTPS GraphQL endpoint")
  }

  endpoint
}

# Check GraphQL `variables` as a uniquely named list. Returns the same list for
# single-page and pagination requests
graphql_validate_variables <- function(variables) {
  if (!is.list(variables)) {
    .fabric_abort("variables must be a list")
  }

  if (
    length(variables) &&
      (is.null(names(variables)) ||
        anyNA(names(variables)) ||
        !all(nzchar(names(variables))) ||
        anyDuplicated(names(variables)))
  ) {
    .fabric_abort(
      "variables must have unique, non-empty names"
    )
  }
  variables
}

# Check `value` as one non-empty string named by `name`. Returns the original
# string for GraphQL validators and resolvers
graphql_required_string <- function(value, name) {
  if (
    !is.character(value) ||
      length(value) != 1L ||
      is.na(value) ||
      !nzchar(trimws(value))
  ) {
    .fabric_abort(
      sprintf("%s must be one non-empty character value", name)
    )
  }
  value
}

# Apply type and value predicates to one scalar `value`. Returns invisibly and
# centralizes simple GraphQL option checks with a caller-friendly `message`
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
    .fabric_abort(message)
  }
  invisible(TRUE)
}

# Follow field names in `path` through nested `value`. Returns the located value
# or `NULL` for the cursor callback to report clearly
graphql_at_path <- function(value, path) {
  located <- graphql_find_path(value, path)
  if (located$found) located$value else NULL
}

# Follow field names while distinguishing a present GraphQL null from a missing
# path. Returns both the location status and value for tidy row collection
graphql_find_path <- function(value, path) {
  for (field in path) {
    if (
      !is.list(value) ||
        is.null(names(value)) ||
        !field %in% names(value)
    ) {
      return(list(found = FALSE, value = NULL))
    }
    value <- value[[field]]
  }
  list(found = TRUE, value = value)
}
