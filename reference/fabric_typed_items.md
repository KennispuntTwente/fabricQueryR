# Typed Microsoft Fabric item discovery

These shortcuts find one kind of Fabric item. Most also retrieve
workload connection details by default, so their results can usually be
passed straight to the next 'fabricQueryR' call. Semantic Model and
GraphQL helpers default to lightweight discovery because their
executable targets are derived from list-level IDs and workspace fields;
set `detail = TRUE` when their workload-specific properties are needed

## Usage

``` r
fabric_lakehouses(workspace, detail = TRUE, ...)

fabric_warehouses(workspace, detail = TRUE, ...)

fabric_warehouse_snapshots(workspace, detail = TRUE, ...)

fabric_mirrored_databases(workspace, detail = TRUE, ...)

fabric_sql_databases(workspace, detail = TRUE, ...)

fabric_semantic_models(workspace, detail = FALSE, ...)

fabric_eventhouses(workspace, detail = TRUE, ...)

fabric_kql_databases(workspace, detail = TRUE, ...)

fabric_notebooks(workspace, detail = TRUE, ...)

fabric_data_pipelines(workspace, detail = TRUE, ...)

fabric_spark_job_definitions(workspace, detail = TRUE, ...)

fabric_environments(workspace, detail = TRUE, ...)

fabric_user_data_functions(workspace, detail = TRUE, ...)

fabric_graphql_apis(workspace, detail = FALSE, ...)
```

## Arguments

- workspace:

  Workspace name, ID, or record returned by
  [`fabric_workspaces()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_workspaces.md).
  A name is convenient for interactive use; a record avoids an extra
  lookup

- detail:

  Whether to retrieve connection details as well as names and IDs. This
  takes more requests and may require additional permissions. The typed
  discovery helpers generally use `TRUE`; Semantic Model and GraphQL
  helpers default to lightweight records because their query targets can
  be derived without workload detail requests

- ...:

  Authentication and API arguments forwarded to
  [`fabric_items()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_items.md)
  Do not supply `type`; each helper sets that value

## Value

A list with one `fabric_item` object per matching item. Each object
contains common item metadata and applicable connection fields. See
[`fabric_items()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_items.md)
for details

## Choosing a helper

- `fabric_lakehouses()`, `fabric_warehouses()`,
  `fabric_warehouse_snapshots()`, and `fabric_mirrored_databases()` find
  data stores that can be queried through
  [`fabric_sql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_query.md);
  Lakehouses and mirrored databases can also be accessed through OneLake

- `fabric_sql_databases()` finds transactional Fabric SQL databases

- `fabric_semantic_models()` finds the business models queried with DAX
  via
  [`fabric_pbi_dax_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_pbi_dax_query.md)

- `fabric_eventhouses()` and `fabric_kql_databases()` find real-time
  data stores queried with
  [`fabric_kql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_kql_query.md)

- `fabric_notebooks()` finds notebooks that can be run with
  [`fabric_job_run()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_run.md)

- `fabric_data_pipelines()` and `fabric_spark_job_definitions()` find
  the other executable items supported by
  [`fabric_job_run()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_run.md)

- `fabric_environments()` finds reusable Spark runtime configurations

- `fabric_user_data_functions()` finds serverless Python function items

- `fabric_graphql_apis()` finds APIs configured in Fabric for use with
  [`fabric_graphql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_graphql_query.md)

## Filtering and returned fields

Each helper requests its exact Fabric item type and verifies that every
returned record has that type. The records otherwise keep all fields
returned by Fabric, including fields added by the service in the future

Folder recursion, workspace-specific private-link routing,
authentication, and `detail_errors` have the same behavior as in
[`fabric_items()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_items.md).
The four new helpers do not currently make workload-specific detail
requests. Their core records contain the IDs and type needed by
[`fabric_job_run()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_run.md)
where applicable, and 'fabricQueryR' does not yet consume an additional
target from Environment or User Data Function details

## References

[List items REST
API](https://learn.microsoft.com/en-us/rest/api/fabric/core/items/list-items)

[List data
pipelines](https://learn.microsoft.com/en-us/rest/api/fabric/datapipeline/items/list-data-pipelines)

[List Spark job
definitions](https://learn.microsoft.com/en-us/rest/api/fabric/sparkjobdefinition/items/list-spark-job-definitions)

[List
environments](https://learn.microsoft.com/en-us/rest/api/fabric/environment/items/list-environments)

[List User Data
Functions](https://learn.microsoft.com/en-us/rest/api/fabric/userdatafunction/items/list-user-data-functions)

## Examples

``` r
if (FALSE) { # \dontrun{
# Discover a workspace once, then reuse its record for typed discovery
workspace <- fabric_workspaces()[[1]]

# Discover data items that feed the package's query and storage helpers
lakehouses <- fabric_lakehouses(workspace)
warehouses <- fabric_warehouses(workspace)
snapshots <- fabric_warehouse_snapshots(workspace)
mirrored_databases <- fabric_mirrored_databases(workspace)
sql_databases <- fabric_sql_databases(workspace)
semantic_models <- fabric_semantic_models(workspace)
eventhouses <- fabric_eventhouses(workspace)
kql_databases <- fabric_kql_databases(workspace)
graphql_apis <- fabric_graphql_apis(workspace)

# A discovered record can be passed directly to a matching helper
fabric_lakehouse_tables(lakehouses[[1L]])
fabric_sql_connection_info(warehouses[[1L]])
fabric_pbi_dax_query(
  semantic_models[[1L]],
  dax = Sys.getenv("FABRIC_DAX_QUERY")
)

# Discover executable items that can be passed to fabric_job_run()
notebook <- fabric_notebooks(workspace)[[1]]
pipeline <- fabric_data_pipelines(workspace)[[1]]
spark_job <- fabric_spark_job_definitions(workspace)[[1]]

fabric_job_wait(fabric_job_run(notebook), timeout = 900)
fabric_job_wait(fabric_job_run(pipeline), timeout = 900)
fabric_job_wait(fabric_job_run(spark_job), timeout = 900)

# Discover supporting Spark and serverless-function items as well
environments <- fabric_environments(workspace)
functions <- fabric_user_data_functions(workspace)
} # }
```
