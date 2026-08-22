#' Work with Microsoft Fabric from R
#'
#' 'fabricQueryR' helps you find and work with Microsoft Fabric data from R.
#' Start by discovering the workspaces and items available to you, then pass those
#' results directly to the package's query, file, Spark, and job functions. In
#' most workflows, you do not need to copy IDs or connection details by hand
#'
#' @section Where to start:
#' - Use [fabric_workspaces()] and the typed discovery helpers, such as
#'   [fabric_lakehouses()] or [fabric_semantic_models()], to find data
#' - Use [fabric_sql_tables()] to discover tables and views across Warehouse,
#'   SQL Database, Warehouse snapshot, or Lakehouse SQL endpoints, then
#'   [fabric_sql_read_table()] or [fabric_sql_query()] to read them
#' - Use [fabric_pbi_dax_query()] for report-ready semantic models
#' - Use the Livy helpers when Spark processing is required
#' - Use [fabric_lakehouse_schemas()] and [fabric_lakehouse_tables()] to discover
#'   or load managed Delta tables, and [fabric_onelake_read_delta_table()] to
#'   read them
#' - Use [fabric_onelake_files] for ordinary files
#' - Use [fabric_warehouse_schemas()] and [fabric_warehouse_tables()] to discover
#'   Warehouse tables, then
#'   [fabric_warehouse_read_table()] or [fabric_warehouse_write_table()] for
#'   symmetric Warehouse table transfer
#' - Use [fabric_mirrored_databases()] and [fabric_mirrored_database_tables()]
#'   to discover and read continuously replicated Delta tables
#' - Use [fabric_kql_tables()] to discover Eventhouse tables,
#'   [fabric_kql_query()] for Eventhouse/KQL data,
#'   [fabric_kql_read_table()] for table-oriented Eventhouse reads,
#'   [fabric_kql_ingest()] for tracked storage ingestion,
#'   [fabric_kql_write_table()] for R/Arrow-to-Eventhouse writes, and
#'   [fabric_kql_export()] for tracked server-side exports to storage
#' - Use [fabric_graphql_query()] for an API for GraphQL item
#' - Use [fabric_function_invoke()] to call published business logic through a
#'   User Data Function's explicit public URL
#' - Use [fabric_job_run()], [fabric_job_status()], [fabric_job_wait()], and
#'   [fabric_job_cancel()] to control supported on-demand item jobs
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
