# Typed Microsoft Fabric item discovery

These shortcuts list one kind of item and include the detailed
connection fields used by the matching query functions. They are
equivalent to
[`fabric_items()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_items.md)
with a fixed item type and `detail = TRUE`. Set `detail = FALSE` when
only names and identifiers are needed.

## Usage

``` r
fabric_lakehouses(workspace, detail = TRUE, ...)

fabric_warehouses(workspace, detail = TRUE, ...)

fabric_sql_databases(workspace, detail = TRUE, ...)

fabric_semantic_models(workspace, detail = TRUE, ...)

fabric_eventhouses(workspace, detail = TRUE, ...)

fabric_kql_databases(workspace, detail = TRUE, ...)

fabric_notebooks(workspace, detail = TRUE, ...)

fabric_graphql_apis(workspace, detail = TRUE, ...)
```

## Arguments

- workspace:

  Workspace GUID, exact display name, or a workspace record returned by
  [`fabric_workspaces()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_workspaces.md).
  A record avoids an extra lookup and, if it contains `apiEndpoint`,
  routes workspace calls through that endpoint. A name is often easier
  for interactive use.

- detail:

  Logical. `FALSE` makes the fewest API calls and is sufficient for
  names and IDs. `TRUE` also retrieves supported workload properties,
  such as SQL connection strings and Livy or KQL endpoints, but is
  slower and can require additional permissions. The typed helpers below
  use `TRUE`.

- ...:

  Authentication and API arguments forwarded to
  [`fabric_items()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_items.md).
  Do not supply `type`; each helper sets that value.

## Value

A list with one `fabric_item` object per matching item. Each object
contains common item metadata and applicable connection fields. See
[`fabric_items()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_items.md)
for details.

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
