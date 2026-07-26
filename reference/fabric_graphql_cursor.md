# Build a Fabric GraphQL cursor extractor

Build a Fabric GraphQL cursor extractor

## Usage

``` r
fabric_graphql_cursor(path, has_next = "hasNextPage", end_cursor = "endCursor")
```

## Arguments

- path:

  Character path from the result's `data` field to a connection object,
  for example `"products"` or `c("viewer", "products")`. This is the
  parent object that contains the pagination fields, not the `items`
  field.

- has_next:

  Name of the logical connection field indicating another page. Fabric
  commonly uses `"hasNextPage"`.

- end_cursor:

  Name of the connection field containing the opaque cursor. Fabric
  commonly uses `"endCursor"`.

## Value

A function suitable for `next_cursor` in
[`fabric_graphql_paginate()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_graphql_paginate.md).
For each page it returns the cursor when `has_next` is true, otherwise
`NULL`.
