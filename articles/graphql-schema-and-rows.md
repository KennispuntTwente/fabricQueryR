# Working with GraphQL

A Fabric API for GraphQL provides a structured view of data selected by
the API’s owner. A query names the fields you want, and the response
follows the same nested shape. This is useful when you should use an
approved API instead of connecting directly to the underlying data
source.

This guide starts with one small query. It then introduces schema
inspection, pagination, and collection into a tibble. You only need the
later sections when the result spans several pages or contains nested
fields.

Start with a discovered API so its endpoint and workspace travel
together:

``` r

library(fabricQueryR)

api <- fabric_graphql_apis("Analytics workspace")[[1]]
```

The caller needs *Run Queries and Mutations* permission on the API. With
SSO connectivity, the caller also needs access to the underlying data
source.

## Run a first query

A GraphQL document is one character string. The field names depend on
how the Fabric API was configured, so replace `products`, `id`, and
`name` with fields from your API:

``` r

response <- fabric_graphql_query(
  api,
  query = "{ products { items { id name } } }"
)

response$data$products$items
response$errors
```

GraphQL can return useful data and service errors together.
‘fabricQueryR’ keeps them separate so you can inspect both. When this
small request works, use the next sections to discover fields and
retrieve more than one page.

## Inspect the available fields

Microsoft Fabric disables runtime introspection by default. Only a
workspace admin can enable it under *API Settings \> Introspection*.
Once enabled, the standard introspection response is available as a
nested R list:

``` r

schema <- fabric_graphql_schema(api)

schema$queryType$name
vapply(schema$types, `[[`, character(1), "name")
```

[`fabric_graphql_schema()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_graphql_schema.md)
stops with a `fabric_graphql_introspection_error` when the service does
not return a complete schema. Its message points to the administrator
setting and the portal’s *Export schema* alternative. Export remains
available when runtime introspection must stay disabled.

## Read more than one page

Fabric normally represents a generated collection with `items`,
`hasNextPage`, and `endCursor`. Request all three pieces needed by the
workflow and use a stable explicit ordering when pages must be
repeatable:

``` r

pages <- fabric_graphql_paginate(
  api,
  query = paste(
    "query Products($first: Int!, $after: String) {",
    "  products(first: $first, after: $after, orderBy: {id: ASC}) {",
    "    items {",
    "      id",
    "      name",
    "      category { id name }",
    "      tags",
    "    }",
    "    hasNextPage",
    "    endCursor",
    "  }",
    "}"
  ),
  variables = list(first = 100L, after = NULL),
  operation_name = "Products",
  next_cursor = fabric_graphql_cursor("products"),
  idempotent = TRUE
)
```

The cursor is opaque.
[`fabric_graphql_cursor()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_graphql_cursor.md)
only reads it from the configured connection path and feeds it back
through the `after` variable; it does not interpret the value.

## Build an analysis-ready tibble

Select the row array explicitly relative to each page’s `data` field:

``` r

products <- fabric_graphql_collect(pages, c("products", "items"))

products
attr(products, "complete")
attr(products, "page_count")
attr(products, "errors")
```

Scalar fields become ordinary columns. A field that first appears on a
later page is added to the union of columns, with typed `NA` values in
earlier rows. Nested objects and arrays remain list-columns rather than
being flattened:

``` r

products$category[[1]]
products$tags[[1]]
```

The collector preserves large whole numbers as character values so they
are not silently rounded. GraphQL can also return data and errors
together: usable rows remain available, while combined service errors
stay in `attr(products, "errors")`.

## Treat incomplete pagination as partial data

[`fabric_graphql_paginate()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_graphql_paginate.md)
marks a result complete only after the API reports that no next page
exists. If a page limit is reached first, collection raises an error
rather than returning an apparently complete tibble. The partial rows
remain available for explicit recovery:

``` r

tryCatch(
  fabric_graphql_collect(incomplete_pages, c("products", "items")),
  fabric_graphql_collection_error = function(error) {
    partial <- error$partial_data
    attr(partial, "complete")
    partial
  }
)
```

For large results, use smaller pages and stable filtered partitions. See
the Fabric limits documentation before designing a high-volume API
workflow.

See Microsoft’s documentation for [introspection and schema
export](https://learn.microsoft.com/en-us/fabric/data-engineering/api-graphql-introspection-schema-export),
[GraphQL
limits](https://learn.microsoft.com/en-us/fabric/data-engineering/api-graphql-limits),
and the generated [pagination
shape](https://learn.microsoft.com/en-us/fabric/data-engineering/api-graphql-aggregations).
