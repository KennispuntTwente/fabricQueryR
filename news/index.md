# Changelog

## fabricQueryR (development version)

### New

- [`fabric_workspaces()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_workspaces.md),
  [`fabric_items()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_items.md),
  and type-specific functions such as
  [`fabric_lakehouses()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md)
  and
  [`fabric_semantic_models()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md)
  discover common Fabric resources. Their results can be passed directly
  to other fabricQueryR functions, avoiding copied IDs and endpoints in
  most workflows.

- [`fabric_livy_session()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_livy_session.md)
  and
  [`fabric_livy_batch_submit()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_livy_batch_submit.md)
  add reusable Spark sessions and standalone batch jobs.
  [`fabric_livy_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_livy_query.md)
  remains the simplest option for running one piece of Spark code.

- [`fabric_onelake_read_file()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_object_files.md),
  [`fabric_onelake_write_file()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_object_files.md),
  [`fabric_onelake_download()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_files.md),
  and
  [`fabric_onelake_upload()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_files.md)
  move files and Parquet, CSV, or Arrow data between R, local storage,
  and OneLake.
  [`fabric_onelake_list()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_files.md),
  [`fabric_onelake_metadata()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_files.md),
  and
  [`fabric_onelake_delete()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_files.md)
  list, inspect, and delete files.

- [`fabric_lakehouse_tables()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_lakehouse_tables.md),
  [`fabric_lakehouse_read_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_lakehouse_read_table.md),
  [`fabric_lakehouse_load_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_lakehouse_tables.md),
  and
  [`fabric_lakehouse_write_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_lakehouse_tables.md)
  discover and read Lakehouse tables, load CSV or Parquet files, and
  write data frames or Arrow data. Both ordinary and schema-enabled
  Lakehouses are supported.

- [`fabric_warehouse_read_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_warehouse_read_table.md)
  and
  [`fabric_warehouse_write_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_warehouse_write_table.md)
  read and bulk-write Warehouse tables using data frames or Arrow data.
  Tables can be created, appended to, overwritten, or recreated as
  requested.

- [`fabric_kql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_kql_query.md)
  and
  [`fabric_kql_read_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_kql_read_table.md)
  bring Eventhouse query and table results into R as typed R objects.

- [`fabric_kql_ingest()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_kql_ingest.md),
  [`fabric_kql_write_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_kql_write_table.md),
  and
  [`fabric_kql_export()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_kql_export.md)
  load existing files or R and Arrow data into Eventhouse, monitor the
  load, and export large query results to OneLake or other supported
  storage. A destination table can be created when needed.
  [`fabric_kql_write_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_kql_write_table.md)
  now rejects multi-file writes that combine per-blob ingestion with one
  shared idempotency key, preventing successful tracking results with
  omitted parts.

- `fabric_graphql_*()` functions query a Fabric API for GraphQL, inspect
  its schema, work through paginated results, and collect the result
  into tidy R objects.

- [`fabric_function_invoke()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_function_invoke.md)
  calls published Fabric User Data Functions from R.

- [`fabric_onelake_shortcuts()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_shortcuts.md)
  and `fabric_onelake_shortcut_*()` functions inspect, create, update,
  and delete OneLake shortcuts, which link Fabric items to data stored
  elsewhere.

- `fabric_pbi_refresh_*()` functions start, monitor, wait for, cancel,
  and inspect the history of semantic-model refreshes, including
  refreshes limited to selected tables or partitions.

- `fabric_job_*()` functions run, monitor, wait for, and cancel Fabric
  Notebooks, data pipelines, Spark job definitions, and other supported
  item jobs. They also inspect run history and manage recurring
  schedules.

- `fabric_operation_*()` functions resume, monitor, and retrieve the
  results of longer-running Fabric tasks such as Lakehouse loads.

### Changed

- Authenticated functions now consistently accept an AzureAuth token, a
  bearer token, or a function that supplies refreshed tokens through
  `token`; `auth_args` controls AzureAuth sign-in. The older
  `access_token` argument for SQL and Livy is deprecated. Requests also
  check service addresses more carefully and give clearer retry,
  timeout, and error messages.

- [`fabric_sql_connect()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_connect.md)
  and
  [`fabric_sql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_query.md)
  now accept discovered items and portal connection strings, and support
  Lakehouses, Warehouses, and Fabric SQL Databases. Queries can use
  parameters and return a tibble or Arrow stream. The default
  `database = NULL` infers the database when possible;
  [`fabric_sql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_query.md)
  is now limited to one read-only statement, so use
  [`fabric_sql_connect()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_connect.md)
  for other SQL work.

- [`fabric_pbi_dax_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_pbi_dax_query.md)
  now accepts discovered semantic models or direct IDs, can test results
  for a user under row-level security, and reports incomplete Power BI
  results instead of silently returning them. An optional Arrow mode
  provides typed tibbles or streams for models that support it.

- [`fabric_onelake_read_delta_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_read_delta_table.md)
  now reads current or historical Lakehouse and compatible Warehouse
  tables through an optional Python Delta reader. It supports selected
  columns, row limits, and Arrow streams for large or nested results.
  The `dest_dir` argument has been removed; tables using unsupported
  Delta features should be read through SQL or Spark instead.

- [`fabric_livy_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_livy_query.md)
  table results now follow the declared Spark schema and preserve large
  whole numbers and decimals exactly.

## fabricQueryR 0.2.1

CRAN release: 2026-04-03

- Update e-mail address of maintainer in DESCRIPTION file (change to a
  personal e-mail address due to leaving the organization).

- [`fabric_onelake_read_delta_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_read_delta_table.md):
  add experimental support for specifying a Lakehouse schema name to
  read from a specific schema within a Lakehouse which has Lakehouse
  schemas enabled.

## fabricQueryR 0.2.0

CRAN release: 2025-09-15

- Added
  [`fabric_livy_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_livy_query.md)
  to run queries against the ‘Fabric Livy API’, allowing remote
  execution of ‘Spark’ code.

## fabricQueryR 0.1.1

CRAN release: 2025-09-08

- Initial CRAN submission.
