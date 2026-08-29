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
  to find data. A workspace can also continue discovery with methods
  such as `$lakehouses()` and `$semantic_models()`; see
  [FabricItem](https://kennispunttwente.github.io/fabricQueryR/reference/FabricItem.md)

- Use
  [`fabric_sql_tables()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_tables.md)
  to discover tables and views across Warehouse, SQL Database, Warehouse
  snapshot, or Lakehouse SQL endpoints, then use an item's
  `$sql_read_table()` or `$sql_query()` method to read them

- Use a semantic model's `$dax_query()` method for report-ready models

- Use a Lakehouse's `$livy_query()`, `$livy_session()`, or
  `$livy_batch_submit()` method when Spark processing is required

- Use
  [`fabric_lakehouse_schemas()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_catalog.md)
  and
  [`fabric_lakehouse_tables()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_lakehouse_tables.md)
  to discover managed Delta tables; discovered Lakehouses provide
  `$tables()`, `$read_table()`, `$write_table()`, and `$load_table()`
  methods

- Use
  [fabric_onelake_files](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_files.md)
  for ordinary files

- Use
  [`fabric_warehouse_schemas()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_catalog.md)
  and
  [`fabric_warehouse_tables()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_warehouse_tables.md)
  to discover Warehouse tables, then `$read_table()` or `$write_table()`
  on the Warehouse object for symmetric table transfer

- Use
  [`fabric_mirrored_databases()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md)
  and
  [`fabric_mirrored_database_tables()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_mirrored_database_tables.md)
  to discover and read continuously replicated Delta tables

- Use `$tables()`, `$query()`, `$read_table()`, `$ingest()`,
  `$write_table()`, and `$export()` on a discovered Eventhouse or KQL
  database

- Use `$query()`, `$schema()`, and `$paginate()` on an API for GraphQL
  item

- Use
  [`fabric_function_invoke()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_function_invoke.md)
  to call published business logic through a User Data Function's
  explicit public URL

- Use `$run()`, `$status()`, `$wait()`, and `$cancel()` on a discovered
  notebook, data pipeline, or Spark job definition

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
