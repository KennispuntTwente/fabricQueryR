# Collect paged GraphQL row objects into a tibble

Combines row objects from a caller-selected field in every result
returned by
[`fabric_graphql_paginate()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_graphql_paginate.md).
The explicit `path` is relative to each page's `data` field because
GraphQL response shapes are schema-defined and cannot be inferred safely

## Usage

``` r
fabric_graphql_collect(pages, path)
```

## Arguments

- pages:

  A `fabric_graphql_pages` result from
  [`fabric_graphql_paginate()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_graphql_paginate.md).
  Requiring that result, rather than an unverified single response, lets
  the function enforce completion

- path:

  Character path from each page's `data` field to the list of row
  objects, for example `c("viewer", "products", "items")`

## Value

A `fabric_graphql_rows` tibble. Attributes `complete`, `errors`,
`page_count`, and `path` retain collection metadata

## Details

Scalar fields become ordinary tibble columns. Nested objects and arrays
stay as list-columns and are never flattened. Fields introduced on later
pages are added in first-seen order, with missing or GraphQL `null`
scalar values represented by typed `NA` values when their type can be
inferred. Exact integer strings returned by
[`fabric_graphql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_graphql_query.md)
remain character data; integer-valued numeric entries in the same field
are promoted to character rather than coercing a large integer to an
inexact double

A successful result has class `fabric_graphql_rows` and reports
completion, page count, path, and GraphQL errors in its printed header
and attributes. Use `attr(rows, "errors")` to inspect partial GraphQL
errors. If pagination stopped before `next_cursor` reported completion,
including at `max_pages`, the function raises
`fabric_graphql_collection_error`; its `partial_data` field contains the
rows collected so far and is explicitly marked incomplete

## References

[Fabric API for GraphQL
limits](https://learn.microsoft.com/en-us/fabric/data-engineering/api-graphql-limits)

[Fabric GraphQL aggregation and pagination
shape](https://learn.microsoft.com/en-us/fabric/data-engineering/api-graphql-aggregations)

## Examples

``` r
if (FALSE) { # \dontrun{
api <- fabric_graphql_apis("Analytics workspace")[[1]]
pages <- fabric_graphql_paginate(
  api,
  query = paste(
    "query Products($first: Int!, $after: String) {",
    "  products(first: $first, after: $after) {",
    "    items { id name details { category } }",
    "    hasNextPage endCursor",
    "  }",
    "}"
  ),
  variables = list(first = 100L, after = NULL),
  next_cursor = fabric_graphql_cursor("products")
)

rows <- fabric_graphql_collect(pages, c("products", "items"))
attr(rows, "complete")
attr(rows, "errors")
} # }
```
