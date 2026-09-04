# Typed Microsoft Fabric item discovery

These shortcuts cover an intentional subset of Microsoft Fabric item
types; they are not an exhaustive list of the items that
[`fabric_items()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_items.md)
can discover. Each helper requests one exact type and has a
corresponding
[FabricWorkspace](https://kennispunttwente.github.io/fabricQueryR/reference/FabricItem.md)
method. Most retrieve workload connection details by default. Semantic
Model and GraphQL helpers default to lightweight discovery because their
executable targets are derived from list-level IDs and workspace fields.
User Data Functions default to lightweight discovery because Microsoft
limits detail retrieval to delegated user identities. Set
`detail = TRUE` when the workload and identity support it

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

fabric_graphql_apis(workspace, detail = FALSE, ...)
```

## Arguments

- workspace:

  Workspace name, ID, or object returned by
  [`fabric_workspaces()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_workspaces.md).
  A name is convenient for interactive use; an object avoids an extra
  lookup

- detail:

  Whether to retrieve connection details as well as names and IDs. This
  takes more requests and may require additional permissions. For
  [`fabric_item()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_item.md),
  `NULL` enriches every supported type except User Data Functions, whose
  detail endpoint does not support application identities. The typed
  Semantic Model, GraphQL, and User Data Function helpers also default
  to lightweight records

- ...:

  Authentication and API arguments forwarded to
  [`fabric_items()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_items.md)
  Do not supply `type`; each helper sets that value

## Value

A list with one
[FabricItem](https://kennispunttwente.github.io/fabricQueryR/reference/FabricItem.md)
object or type-specific R6 subclass per matching item. Each object
contains common item metadata, applicable connection fields, and
workload methods. See
[`fabric_items()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_items.md)
for details

## Typed support matrix

`Default detail` is the value used when `detail` is omitted.
`FabricItem` in the final column means that the typed helper and
workload Get route are supported but no workload-specific R6 subclass is
currently provided.

|  |  |  |  |
|----|----|----|----|
| **Helper** | **Fabric type** | **Default detail** | **R6 class** |
| `fabric_lakehouses()` | `Lakehouse` | `TRUE` | `FabricLakehouse` |
| `fabric_warehouses()` | `Warehouse` | `TRUE` | `FabricWarehouse` |
| `fabric_warehouse_snapshots()` | `WarehouseSnapshot` | `TRUE` | `FabricWarehouseSnapshot` |
| `fabric_mirrored_databases()` | `MirroredDatabase` | `TRUE` | `FabricMirroredDatabase` |
| `fabric_sql_databases()` | `SQLDatabase` | `TRUE` | `FabricSqlDatabase` |
| `fabric_semantic_models()` | `SemanticModel` | `FALSE` | `FabricSemanticModel` |
| `fabric_eventhouses()` | `Eventhouse` | `TRUE` | `FabricEventhouse` |
| `fabric_kql_databases()` | `KQLDatabase` | `TRUE` | `FabricKqlDatabase` |
| `fabric_notebooks()` | `Notebook` | `TRUE` | `FabricJobItem` |
| `fabric_data_pipelines()` | `DataPipeline` | `TRUE` | `FabricJobItem` |
| `fabric_spark_job_definitions()` | `SparkJobDefinition` | `TRUE` | `FabricJobItem` |
| `fabric_environments()` | `Environment` | `TRUE` | `FabricItem` |
| [`fabric_user_data_functions()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_user_data_functions.md) | `UserDataFunction` | `FALSE` | `FabricItem` |
| `fabric_graphql_apis()` | `GraphQLApi` | `FALSE` | `FabricGraphQLApi` |

## Choosing a helper

- `fabric_lakehouses()`, `fabric_warehouses()`,
  `fabric_warehouse_snapshots()`, and `fabric_mirrored_databases()` find
  data stores with `$sql_query()`
  ([`fabric_sql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_query.md))
  and other SQL methods; Lakehouses and mirrored databases can also be
  accessed through OneLake

- `fabric_sql_databases()` finds transactional Fabric SQL databases

- `fabric_semantic_models()` finds business models with `$dax_query()`
  ([`fabric_pbi_dax_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_pbi_dax_query.md))
  and refresh lifecycle methods

- `fabric_eventhouses()` and `fabric_kql_databases()` find real-time
  data stores with `$query()`
  ([`fabric_kql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_kql_query.md))
  and `$read_table()`
  ([`fabric_kql_read_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_kql_read_table.md)),
  plus ingestion and export methods

- `fabric_notebooks()` finds notebooks with job lifecycle and schedule
  methods

- `fabric_data_pipelines()` and `fabric_spark_job_definitions()` find
  the other executable items with the same job methods

- `fabric_environments()` finds reusable Spark runtime configurations

- [`fabric_user_data_functions()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_user_data_functions.md)
  finds serverless Python function items

- `fabric_graphql_apis()` finds APIs configured in Fabric with
  `$query()`
  ([`fabric_graphql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_graphql_query.md)),
  `$schema()`
  ([`fabric_graphql_schema()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_graphql_schema.md)),
  and `$paginate()`
  ([`fabric_graphql_paginate()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_graphql_paginate.md))

## Filtering and returned fields

Each helper requests its exact Fabric item type and verifies that every
returned object has that type. The objects otherwise keep all fields
returned by Fabric, including fields added by the service in the future

Folder recursion, workspace-specific private-link routing,
authentication, and `detail_errors` have the same behavior as in
[`fabric_items()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_items.md).
With `detail = TRUE`, each helper calls its documented workload-specific
Get API and preserves fields such as Spark job and Environment
properties. The User Data Function detail endpoint supports delegated
users but not service principals or managed identities; those callers
can use `detail = FALSE`

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

[Get data
pipeline](https://learn.microsoft.com/en-us/rest/api/fabric/datapipeline/items/get-data-pipeline)

[Get Spark job
definition](https://learn.microsoft.com/en-us/rest/api/fabric/sparkjobdefinition/items/get-spark-job-definition)

[Get
environment](https://learn.microsoft.com/en-us/rest/api/fabric/environment/items/get-environment)

[Get User Data
Function](https://learn.microsoft.com/en-us/rest/api/fabric/userdatafunction/items/get-user-data-function)

## Examples

``` r
if (FALSE) { # \dontrun{
# Discover a workspace once, then reuse its object for typed discovery
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

# Each method calls the corresponding exported function
# fabric_lakehouse_tables()
lakehouses[[1L]]$tables()
# fabric_sql_connection_info()
warehouses[[1L]]$sql_connection_info()
# fabric_pbi_dax_query()
semantic_models[[1L]]$dax_query(
  dax = Sys.getenv("FABRIC_DAX_QUERY")
)

# Runnable methods call fabric_job_run() and fabric_job_wait()
notebook <- fabric_notebooks(workspace)[[1]]
pipeline <- fabric_data_pipelines(workspace)[[1]]
spark_job <- fabric_spark_job_definitions(workspace)[[1]]

notebook$wait(notebook$run(), timeout = 900)
pipeline$wait(pipeline$run(), timeout = 900)
spark_job$wait(spark_job$run(), timeout = 900)

# Discover supporting Spark and serverless-function items as well
environments <- fabric_environments(workspace)
functions <- fabric_user_data_functions(workspace)
} # }
```
