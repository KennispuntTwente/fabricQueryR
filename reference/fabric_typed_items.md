# Typed Microsoft Fabric item discovery

These shortcuts list one kind of item and include the detailed
connection fields used by the matching query functions. They are
equivalent to
[`fabric_items()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_items.md)
with a fixed item type and `detail = TRUE`.

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
  [`fabric_workspaces()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_workspaces.md).
  A record or GUID avoids an extra lookup; a name is often easier for
  interactive use.

- ...:

  Authentication and API arguments forwarded to
  [`fabric_items()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_items.md).
  Do not supply `type` or `detail`; each helper sets those values.

## Value

A tibble with one row per matching item, common item metadata, and
applicable connection fields. See
[`fabric_items()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_items.md)
for the column groups.

## Choosing a helper

- `fabric_lakehouses()` and `fabric_warehouses()` find data stores that
  can be queried through
  [`fabric_sql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_query.md);
  Lakehouses can also be accessed through OneLake and Livy.

- `fabric_sql_databases()` finds transactional Fabric SQL databases.

- `fabric_semantic_models()` finds the business models queried with DAX
  via
  [`fabric_pbi_dax_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_pbi_dax_query.md).

- `fabric_eventhouses()` and `fabric_kql_databases()` find real-time
  data stores queried with
  [`fabric_kql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_kql_query.md).

- `fabric_notebooks()` finds notebooks that can be run with
  [`fabric_job_run()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_run.md).

- `fabric_graphql_apis()` finds APIs configured in Fabric for use with
  [`fabric_graphql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_graphql_query.md).
