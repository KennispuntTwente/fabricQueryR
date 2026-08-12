# Locate pagination information in a GraphQL result

Creates the `next_cursor` function used by
[`fabric_graphql_paginate()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_graphql_paginate.md)
for the common `hasNextPage` and `endCursor` pagination fields

## Usage

``` r
fabric_graphql_cursor(path, has_next = "hasNextPage", end_cursor = "endCursor")
```

## Arguments

- path:

  Character path from the result's `data` field to a connection object,
  for example `"products"` or `c("viewer", "products")`. This is the
  parent object that contains the pagination fields, not the `items`
  field

- has_next:

  Name of the logical connection field indicating another page. Fabric
  commonly uses `"hasNextPage"`

- end_cursor:

  Name of the connection field containing the opaque cursor Fabric
  commonly uses `"endCursor"`

## Value

A function suitable for `next_cursor` in
[`fabric_graphql_paginate()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_graphql_paginate.md).
For each page it returns the cursor when `has_next` is true, otherwise
`NULL`

## Examples

``` r
next_cursor <- fabric_graphql_cursor("products")
page <- structure(
  list(data = list(products = list(
    hasNextPage = TRUE,
    endCursor = "opaque-cursor"
  ))),
  class = c("fabric_graphql_result", "list")
)
next_cursor(page)
#> [1] "opaque-cursor"
```
