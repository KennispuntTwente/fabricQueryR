#' Work with Microsoft Fabric from R
#'
#' fabricQueryR provides R-friendly access to common Microsoft Fabric data
#' interfaces. It can discover the workspaces and items available to you, then
#' pass those discovered records directly to query and file functions so that
#' you rarely need to copy GUIDs or endpoints by hand.
#'
#' @section Where to start:
#' - Use [fabric_workspaces()] and the typed discovery helpers, such as
#'   [fabric_lakehouses()] or [fabric_semantic_models()], to find data.
#' - Use [fabric_sql_query()] for T-SQL against a Warehouse, SQL Database, or
#'   Lakehouse SQL analytics endpoint.
#' - Use [fabric_pbi_dax_query()] for report-ready semantic models,
#'   [fabric_kql_query()] for Eventhouse/KQL data, and
#'   [fabric_graphql_query()] for an API for GraphQL item.
#' - Use [fabric_onelake_files] for ordinary files,
#'   [fabric_onelake_read_delta_table()] for Delta tables, and the Livy helpers
#'   when Spark processing is required.
#' - Use [fabric_job_run()], [fabric_job_status()], [fabric_job_wait()], and
#'   [fabric_job_cancel()] to control supported on-demand item jobs.
#' - See `vignette("authentication", package = "fabricQueryR")` for interactive
#'   and unattended authentication setup, required token audiences, and Fabric
#'   permissions.
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
