# Build a Fabric GraphQL cursor extractor

Build a Fabric GraphQL cursor extractor

## Usage

``` r
fabric_graphql_cursor(path, has_next = "hasNextPage", end_cursor = "endCursor")
```

## Arguments

- path:

  Character path from the result's `data` field to a connection object,
  for example `"products"` or `c("viewer", "products")`.

- has_next:

  Name of the connection field indicating another page.

- end_cursor:

  Name of the connection field containing the opaque cursor.

## Value

A function suitable for `next_cursor` in
[`fabric_graphql_paginate()`](https://lukakoning.github.io/fabricQueryR/reference/fabric_graphql_paginate.md).
