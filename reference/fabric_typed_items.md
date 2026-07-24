# Typed Microsoft Fabric item discovery

These helpers are equivalent to
[`fabric_items()`](https://lukakoning.github.io/fabricQueryR/reference/fabric_items.md)
with an item type and `detail = TRUE`. They return workload-specific
properties and derived connection targets.

## Usage

``` r
fabric_lakehouses(workspace, ...)

fabric_warehouses(workspace, ...)

fabric_sql_databases(workspace, ...)

fabric_semantic_models(workspace, ...)

fabric_eventhouses(workspace, ...)

fabric_kql_databases(workspace, ...)

fabric_notebooks(workspace, ...)

fabric_graphql_apis(workspace, ...)
```

## Arguments

- workspace:

  Workspace GUID, exact display name, or a workspace record returned by
  [`fabric_workspaces()`](https://lukakoning.github.io/fabricQueryR/reference/fabric_workspaces.md).

- ...:

  Authentication and API arguments forwarded to
  [`fabric_items()`](https://lukakoning.github.io/fabricQueryR/reference/fabric_items.md).

## Value

A tibble of enriched Fabric items.
