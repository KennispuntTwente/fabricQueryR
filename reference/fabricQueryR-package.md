# Work with Microsoft Fabric from R

fabricQueryR provides R-friendly access to common Microsoft Fabric data
interfaces. It can discover the workspaces and items available to you,
then pass those discovered records directly to query and file functions
so that you rarely need to copy GUIDs or endpoints by hand.

## Where to start

- Use
  [`fabric_workspaces()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_workspaces.md)
  and the typed discovery helpers, such as
  [`fabric_lakehouses()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md)
  or
  [`fabric_semantic_models()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md),
  to find data.

- Use
  [`fabric_sql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_query.md)
  for T-SQL against a Warehouse, SQL Database, or Lakehouse SQL
  analytics endpoint.

- Use
  [`fabric_pbi_dax_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_pbi_dax_query.md)
  for report-ready semantic models,
  [`fabric_kql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_kql_query.md)
  for Eventhouse/KQL data, and
  [`fabric_graphql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_graphql_query.md)
  for an API for GraphQL item.

- Use
  [fabric_onelake_files](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_files.md)
  for ordinary files,
  [`fabric_onelake_read_delta_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_read_delta_table.md)
  for Delta tables, and the Livy helpers when Spark processing is
  required.

- See
  [`vignette("authentication", package = "fabricQueryR")`](https://kennispunttwente.github.io/fabricQueryR/articles/authentication.md)
  for interactive and unattended authentication setup, required token
  audiences, and Fabric permissions.

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
