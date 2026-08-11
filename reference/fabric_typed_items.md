# Typed Microsoft Fabric item discovery

These shortcuts find one kind of Fabric item. By default, they also
retrieve the connection details needed by the matching query functions,
so their results can usually be passed straight to the next fabricQueryR
call. Set `detail = FALSE` when you only need names and IDs.

## Usage

``` r
fabric_lakehouses(workspace, detail = TRUE, ...)

fabric_warehouses(workspace, detail = TRUE, ...)

fabric_warehouse_snapshots(workspace, detail = TRUE, ...)

fabric_sql_databases(workspace, detail = TRUE, ...)

fabric_semantic_models(workspace, detail = TRUE, ...)

fabric_eventhouses(workspace, detail = TRUE, ...)

fabric_kql_databases(workspace, detail = TRUE, ...)

fabric_notebooks(workspace, detail = TRUE, ...)

fabric_graphql_apis(workspace, detail = TRUE, ...)
```

## Arguments

- workspace:

  Workspace name, ID, or record returned by
  [`fabric_workspaces()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_workspaces.md).
  A name is convenient for interactive use; a record avoids an extra
  lookup.

- detail:

  Whether to retrieve connection details as well as names and IDs. This
  takes more requests and may require additional permissions. The typed
  discovery helpers use `TRUE` by default.

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

- `fabric_lakehouses()`, `fabric_warehouses()`, and
  `fabric_warehouse_snapshots()` find data stores that can be queried
  through
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
