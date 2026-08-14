# Work with Microsoft Fabric from R

fabricQueryR helps you find and work with Microsoft Fabric data from R.
Start by discovering the workspaces and items available to you, then
pass those results directly to the package's query, file, Spark, and job
functions. In most workflows, you do not need to copy IDs or connection
details by hand

## Where to start

- Use
  [`fabric_workspaces()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_workspaces.md)
  and the typed discovery helpers, such as
  [`fabric_lakehouses()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md)
  or
  [`fabric_semantic_models()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md),
  to find data

- Use
  [`fabric_sql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_query.md)
  for T-SQL against a Warehouse, SQL Database, or Lakehouse SQL
  analytics endpoint

- Use
  [`fabric_pbi_dax_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_pbi_dax_query.md)
  for report-ready semantic models,
  [`fabric_kql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_kql_query.md)
  for Eventhouse/KQL data,
  [`fabric_kql_ingest()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_kql_ingest.md)
  for tracked storage ingestion,
  [`fabric_kql_write_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_kql_write_table.md)
  for R/Arrow-to-Eventhouse writes,
  [`fabric_kql_export()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_kql_export.md)
  for tracked server-side exports to storage, and
  [`fabric_graphql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_graphql_query.md)
  for an API for GraphQL item

- Use
  [`fabric_function_invoke()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_function_invoke.md)
  to call published business logic through a User Data Function's
  explicit public URL

- Use
  [fabric_onelake_files](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_files.md)
  for ordinary files,
  [`fabric_lakehouse_tables()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_lakehouse_tables.md)
  to discover or load managed Delta tables,
  [`fabric_onelake_read_delta_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_read_delta_table.md)
  to read them, and the Livy helpers when Spark processing is required

- Use
  [`fabric_job_run()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_run.md),
  [`fabric_job_status()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_run.md),
  [`fabric_job_wait()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_run.md),
  and
  [`fabric_job_cancel()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_run.md)
  to control supported on-demand item jobs

- See
  [`vignette("authentication", package = "fabricQueryR")`](https://kennispunttwente.github.io/fabricQueryR/articles/authentication.md)
  for interactive and unattended authentication setup, required token
  audiences, and Fabric permissions

## References

[What is Microsoft
Fabric?](https://learn.microsoft.com/en-us/fabric/fundamentals/microsoft-fabric-overview)

[Microsoft Fabric REST API
documentation](https://learn.microsoft.com/en-us/rest/api/fabric/)

## See also

Useful links:

- <https://github.com/kennispunttwente/fabricQueryR>

- <https://kennispunttwente.github.io/fabricQueryR/>

- Report bugs at
  <https://github.com/kennispunttwente/fabricQueryR/issues>

## Author

**Maintainer**: Luka Koning <koningluka@gmail.com> \[copyright holder\]

Authors:

- Luka Koning <koningluka@gmail.com> \[copyright holder\]

Other contributors:

- Kennispunt Twente <info@kennispunttwente.nl> \[funder\]
