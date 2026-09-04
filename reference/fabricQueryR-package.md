# Work with Microsoft Fabric from R

'fabricQueryR' helps you find and work with Microsoft Fabric data from
R. Start by discovering the workspaces and items available to you.
Discovery returns read-only R6 objects that include the service fields
and expose methods matched to each actionable resource. In most
workflows, you can continue with `$` methods without copying IDs,
endpoints, or credentials by hand. The same operations can also be
called through the corresponding `fabric_*()` functions

## Where to start

- Use
  [`fabric_workspaces()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_workspaces.md)
  and the typed discovery helpers, such as
  [`fabric_lakehouses()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md)
  or
  [`fabric_semantic_models()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md),
  to find data. A workspace can also continue discovery with
  `$lakehouses()`
  ([`fabric_lakehouses()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md))
  and `$semantic_models()`
  ([`fabric_semantic_models()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md));
  see
  [FabricItem](https://kennispunttwente.github.io/fabricQueryR/reference/FabricItem.md)

- Use
  [`fabric_sql_tables()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_tables.md)
  to discover tables and views across Warehouse, SQL Database, Warehouse
  snapshot, or Lakehouse SQL endpoints, then use `$sql_read_table()`
  ([`fabric_sql_read_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_tables.md))
  or `$sql_query()`
  ([`fabric_sql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_query.md))
  to read them

- Use `$dax_query()`
  ([`fabric_pbi_dax_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_pbi_dax_query.md))
  on a semantic model

- Use `$livy_query()`
  ([`fabric_livy_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_livy_query.md)),
  `$livy_session()`
  ([`fabric_livy_session()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_livy_session.md)),
  or `$livy_batch_submit()`
  ([`fabric_livy_batch_submit()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_livy_batch_submit.md))
  on a Lakehouse for Spark processing

- Use
  [`fabric_lakehouse_schemas()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_catalog.md)
  and
  [`fabric_lakehouse_tables()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_lakehouse_tables.md)
  to discover managed Delta tables; Lakehouse methods include
  `$tables()`
  ([`fabric_lakehouse_tables()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_lakehouse_tables.md)),
  `$read_table()`
  ([`fabric_lakehouse_read_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_lakehouse_read_table.md)),
  `$write_table()`
  ([`fabric_lakehouse_write_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_lakehouse_tables.md)),
  and `$load_table()`
  ([`fabric_lakehouse_load_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_lakehouse_tables.md))

- Use
  [fabric_onelake_files](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_files.md)
  for ordinary files

- Use
  [`fabric_warehouse_schemas()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_catalog.md)
  and
  [`fabric_warehouse_tables()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_warehouse_tables.md)
  to discover Warehouse tables, then `$read_table()`
  ([`fabric_warehouse_read_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_warehouse_read_table.md))
  or `$write_table()`
  ([`fabric_warehouse_write_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_warehouse_write_table.md))
  on the Warehouse object

- Use
  [`fabric_mirrored_databases()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md)
  and
  [`fabric_mirrored_database_tables()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_mirrored_database_tables.md)
  to discover and read continuously replicated Delta tables

- On an Eventhouse or KQL database, use `$tables()`
  ([`fabric_kql_tables()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_kql_tables.md)),
  `$query()`
  ([`fabric_kql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_kql_query.md)),
  `$read_table()`
  ([`fabric_kql_read_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_kql_read_table.md)),
  `$ingest()`
  ([`fabric_kql_ingest()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_kql_ingest.md)),
  `$write_table()`
  ([`fabric_kql_write_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_kql_write_table.md)),
  and `$export()`
  ([`fabric_kql_export()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_kql_export.md))

- On an API for GraphQL item, use `$query()`
  ([`fabric_graphql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_graphql_query.md)),
  `$schema()`
  ([`fabric_graphql_schema()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_graphql_schema.md)),
  and `$paginate()`
  ([`fabric_graphql_paginate()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_graphql_paginate.md))

- Use experimental
  [`fabric_function_invoke()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_function_invoke.md)
  to call published business logic through a User Data Function's
  explicit public URL

- On a runnable item, use `$run()`
  ([`fabric_job_run()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_run.md)),
  `$status()`
  ([`fabric_job_status()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_run.md)),
  `$wait()`
  ([`fabric_job_wait()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_run.md)),
  and `$cancel()`
  ([`fabric_job_cancel()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_run.md))

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
