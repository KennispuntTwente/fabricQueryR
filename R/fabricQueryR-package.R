#' Work with Microsoft Fabric from R
#'
#' 'fabricQueryR' helps you find and work with Microsoft Fabric data from R.
#' Start by discovering the workspaces and items available to you. Discovery
#' returns read-only R6 objects that include the service fields and expose methods
#' matched to each actionable resource. In most workflows, you can continue with
#' `$` methods without copying IDs, endpoints, or credentials by hand. The
#' same operations can also be called through the corresponding `fabric_*()`
#' functions
#'
#' @section Where to start:
#' - Use [fabric_workspaces()] and the typed discovery helpers, such as
#'   [fabric_lakehouses()] or [fabric_semantic_models()], to find data. A
#'   workspace can also continue discovery with `$lakehouses()`
#'   ([fabric_lakehouses()]) and `$semantic_models()`
#'   ([fabric_semantic_models()]); see [FabricItem]
#' - Use [fabric_sql_tables()] to discover tables and views across Warehouse,
#'   SQL Database, Warehouse snapshot, or Lakehouse SQL endpoints, then
#'   use `$sql_read_table()` ([fabric_sql_read_table()]) or `$sql_query()`
#'   ([fabric_sql_query()]) to read them
#' - Use `$dax_query()` ([fabric_pbi_dax_query()]) on a semantic model
#' - Use `$livy_query()` ([fabric_livy_query()]), `$livy_session()`
#'   ([fabric_livy_session()]), or `$livy_batch_submit()`
#'   ([fabric_livy_batch_submit()]) on a Lakehouse for Spark processing
#' - Use [fabric_lakehouse_schemas()] and [fabric_lakehouse_tables()] to discover
#'   managed Delta tables; Lakehouse methods include `$tables()`
#'   ([fabric_lakehouse_tables()]), `$read_table()`
#'   ([fabric_lakehouse_read_table()]), `$write_table()`
#'   ([fabric_lakehouse_write_table()]), and `$load_table()`
#'   ([fabric_lakehouse_load_table()])
#' - Use [fabric_onelake_files] for ordinary files
#' - Use [fabric_warehouse_schemas()] and [fabric_warehouse_tables()] to discover
#'   Warehouse tables, then `$read_table()` ([fabric_warehouse_read_table()]) or
#'   `$write_table()` ([fabric_warehouse_write_table()]) on the Warehouse object
#' - Use [fabric_mirrored_databases()] and [fabric_mirrored_database_tables()]
#'   to discover and read continuously replicated Delta tables
#' - On an Eventhouse or KQL database, use `$tables()` ([fabric_kql_tables()]),
#'   `$query()` ([fabric_kql_query()]), `$read_table()`
#'   ([fabric_kql_read_table()]), `$ingest()` ([fabric_kql_ingest()]),
#'   `$write_table()` ([fabric_kql_write_table()]), and `$export()`
#'   ([fabric_kql_export()])
#' - On an API for GraphQL item, use `$query()` ([fabric_graphql_query()]),
#'   `$schema()` ([fabric_graphql_schema()]), and `$paginate()`
#'   ([fabric_graphql_paginate()])
#' - Use experimental [fabric_function_invoke()] to call published business
#'   logic through a User Data Function's explicit public URL
#' - On a runnable item, use `$run()` ([fabric_job_run()]), `$status()`
#'   ([fabric_job_status()]), `$wait()` ([fabric_job_wait()]), and `$cancel()`
#'   ([fabric_job_cancel()])
#' - See `vignette("authentication", package = "fabricQueryR")` for interactive
#'   and unattended authentication setup, required token audiences, and Fabric
#'   permissions
#'
#' @references
#' [What is Microsoft Fabric?](https://learn.microsoft.com/en-us/fabric/fundamentals/microsoft-fabric-overview)
#'
#' [Microsoft Fabric REST API documentation](https://learn.microsoft.com/en-us/rest/api/fabric/)
#'
#' @keywords internal
"_PACKAGE"

## usethis namespace: start
#' @importFrom rlang %||%
#' @importFrom R6 R6Class
## usethis namespace: end
NULL
