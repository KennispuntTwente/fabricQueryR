# Changelog

## fabricQueryR 1.0.0

### Breaking changes

- Arrow-backed features now require ‘arrow’ 17.0.0 or later for CSV
  support and reliable file cleanup on Windows.

- [`fabric_onelake_read_delta_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_read_delta_table.md)
  now uses the optional Python ‘deltalake’ reader through ‘reticulate’.
  The `dest_dir` argument has been removed. Remove this argument from
  existing calls; use `columns` and `limit` to restrict a read, or
  `result = "arrow_stream"` to consume batches. Tables using unsupported
  Delta features should be read through SQL or Spark instead.

### New

- Discovery functions find the Fabric workspaces and items available to
  the signed-in user or application. Use
  [`fabric_workspaces()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_workspaces.md)
  and
  [`fabric_items()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_items.md)
  for general discovery, or typed helpers such as
  [`fabric_lakehouses()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md)
  and
  [`fabric_semantic_models()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md)
  to find a specific kind of item. Discovery results are read-only R6
  objects that include every service field, reuse the discovery
  credential, and provide type-specific methods for workspaces, SQL
  items, Lakehouses, Warehouses, mirrored databases, Eventhouses, KQL
  databases, GraphQL APIs, semantic models, and runnable jobs. Semantic
  models and runnable jobs expose status, wait, and cancellation
  methods; KQL items expose status and wait methods for asynchronous
  ingestion. Discovered resources can be passed directly to other
  ‘fabricQueryR’ functions, avoiding copied IDs and endpoints in most
  workflows. Use `$as_list()`,
  [`as.list()`](https://rdrr.io/r/base/list.html), or `output = "list"`
  when a plain record is specifically required.

- [`fabric_livy_session()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_livy_session.md)
  and
  [`fabric_livy_batch_submit()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_livy_batch_submit.md)
  add reusable Spark sessions and standalone batch jobs.
  [`fabric_livy_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_livy_query.md)
  is the simplest option for running one piece of Spark code.

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

- [`fabric_lakehouse_schemas()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_catalog.md),
  [`fabric_lakehouse_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_catalog.md),
  [`fabric_lakehouse_tables()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_lakehouse_tables.md),
  [`fabric_lakehouse_read_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_lakehouse_read_table.md),
  [`fabric_lakehouse_load_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_lakehouse_tables.md),
  and
  [`fabric_lakehouse_write_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_lakehouse_tables.md)
  discover Lakehouse schemas and tables, read them, load CSV or Parquet
  files, and write data frames or Arrow data. Both ordinary and
  schema-enabled Lakehouses are supported.

- [`fabric_warehouse_schemas()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_catalog.md),
  [`fabric_warehouse_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_catalog.md),
  [`fabric_warehouse_tables()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_warehouse_tables.md),
  [`fabric_warehouse_read_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_warehouse_read_table.md),
  and
  [`fabric_warehouse_write_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_warehouse_write_table.md)
  discover schemas and tables, read them, and bulk-write Warehouse
  tables using data frames or Arrow data. Tables can be created,
  appended to, overwritten, or recreated as requested.

- [`fabric_mirrored_databases()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md)
  and the `fabric_mirrored_database_*()` helpers discover mirrored
  databases and inspect or read their OneLake Delta tables. Discovered
  records also work with the generic SQL helpers through each mirrored
  database’s read-only SQL analytics endpoint.

- [`fabric_kql_tables()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_kql_tables.md),
  [`fabric_kql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_kql_query.md),
  and
  [`fabric_kql_read_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_kql_read_table.md)
  discover Eventhouse tables and bring query or table results into R as
  typed R objects.

- [`fabric_kql_ingest()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_kql_ingest.md),
  [`fabric_kql_write_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_kql_write_table.md),
  and
  [`fabric_kql_export()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_kql_export.md)
  load existing files or R and Arrow data into Eventhouse, monitor the
  load, and export large query results to OneLake or other supported
  storage. A destination table can be created when needed.

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
  and inspect the history of semantic-model refreshes.

- [`fabric_sql_connect()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_connect.md)
  and
  [`fabric_sql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_query.md)
  support ODBC and ADBC connections, discovered SQL items, bound query
  parameters, and Arrow streams for larger results. SQL queries and
  Warehouse reads default to the driver’s numeric conversion for ODBC,
  with a once-per-session precision warning, and exact conversion for
  ADBC. Set `numeric_policy = "driver"` to explicitly accept driver
  conversion without the warning, or `numeric_policy = "exact"` to
  reject potentially lossy ODBC results.

- `fabric_job_*()` functions run, monitor, wait for, and cancel Fabric
  Notebooks, data pipelines, Spark job definitions, and other supported
  item jobs. They also inspect run history and manage recurring
  schedules.

- `fabric_operation_*()` functions resume, monitor, and retrieve the
  results of longer-running Fabric tasks such as Lakehouse loads.

### Changed

- Arrow streams from
  [`fabric_onelake_read_file()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_object_files.md)
  and
  [`fabric_pbi_dax_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_pbi_dax_query.md)
  release their temporary IPC files on Windows even when returned Arrow
  tables remain in use.

- Authenticated functions now consistently accept an ‘AzureAuth’ token,
  a bearer token, or a function that supplies refreshed tokens through
  `token`; `auth_args` controls ‘AzureAuth’ sign-in. The older
  `access_token` argument for SQL and Livy is deprecated.

- [`fabric_pbi_dax_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_pbi_dax_query.md)
  now accepts discovered semantic models or direct IDs, can test results
  for a user under row-level security, and reports incomplete Power BI
  results instead of silently returning them. An optional Arrow mode
  provides typed tibbles or streams for models that support it. Both
  JSON and Arrow executions now expose a client-side HTTP `timeout`.
  Mixed JSON Whole Number columns preserve both signed 64-bit extrema
  exactly.

- [`fabric_onelake_read_delta_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_read_delta_table.md)
  now reads current or historical Lakehouse and compatible Warehouse
  tables through an optional Python Delta reader. It supports selected
  columns, row limits, and Arrow streams for large or nested results,
  including through discovered workspace-private OneLake endpoints.

- [`fabric_livy_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_livy_query.md)
  table results now follow the declared Spark schema and preserve large
  whole numbers and decimals exactly.

- [`fabric_livy_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_livy_query.md)
  now bounds temporary-session cleanup with a separate deadline and
  reports both errors when statement execution and session deletion fail
  together.
