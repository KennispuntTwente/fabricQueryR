# Fabric integration coverage: GraphQL date-time variable serialization
test_that("GraphQL filters accept explicit ISO date-time variables", {
  manifest <- fabric_test_manifest()
  api <- fabric_test_manifest_item(manifest, "TestGraphQL")
  token <- fabric_test_token_provider()
  root <- api$root_field
  query <- paste0(
    "query Rows($after: String, $at: DateTime!) { ",
    root,
    "(first: 1, after: $after, filter: {loaded_at: {eq: $at}}, orderBy: {id: ASC})",
    " { items { id loaded_at } hasNextPage endCursor } }"
  )
  first <- fabric_graphql_query(
    api$endpoint,
    paste0(
      "{ ",
      root,
      "(first: 1, orderBy: {id: ASC}) { items { id loaded_at } } }"
    ),
    error_policy = "error",
    token = token,
    audience = "https://api.fabric.microsoft.com/.default"
  )$data[[root]]$items[[1L]]
  expect_type(first$loaded_at, "character")
  expect_match(first$loaded_at, "(Z|[+-][0-9]{2}:[0-9]{2})$")
  pages <- fabric_graphql_paginate(
    api$endpoint,
    query,
    variables = list(at = first$loaded_at, after = NULL),
    next_cursor = fabric_graphql_cursor(root),
    error_policy = "error",
    token = token,
    audience = "https://api.fabric.microsoft.com/.default"
  )
  rows <- fabric_graphql_collect(pages, c(root, "items"))
  expect_true(first$id %in% rows$id)
  expect_true(all(rows$loaded_at == first$loaded_at))
})
