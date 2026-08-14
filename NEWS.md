# fabricQueryR (development version)

## New

* `fabric_function_invoke()` calls published Fabric User Data Functions from R.

* `fabric_graphql_*()` functions query a Fabric API for GraphQL, inspect its
  schema, work through paginated results, and collect the result into tidy R
  objects.

* `fabric_job_*()` functions run, monitor, wait for, and cancel Fabric
  Notebooks, data pipelines, Spark job definitions, and other supported item
  jobs. They also inspect run history and manage recurring schedules.

* `fabric_kql_*()` functions query Eventhouse data, read tables, ingest existing
  files or R and Arrow data, monitor loads, and export large results to OneLake
  or other supported storage. A destination table can be created when needed.

* `fabric_lakehouse_tables()`, `fabric_lakehouse_read_table()`,
  `fabric_lakehouse_load_table()`, and `fabric_lakehouse_write_table()` discover
  and read Lakehouse tables, load CSV or Parquet files, and write data frames or
  Arrow data. Both ordinary and schema-enabled Lakehouses are supported.

* `fabric_livy_session()` and `fabric_livy_batch_submit()` add reusable Spark
  sessions and standalone batch jobs. `fabric_livy_query()` remains the simplest
  option for running one piece of Spark code.

* `fabric_onelake_*()` functions now manage files and shortcuts in OneLake. They
  can list, inspect, download, upload, and delete files; read and write Parquet,
  CSV, and Arrow data; and manage links to data stored elsewhere.

* `fabric_operation_*()` functions resume, monitor, and retrieve the results of
  longer-running Fabric tasks such as Lakehouse loads.

* `fabric_pbi_refresh_*()` functions start, monitor, wait for, cancel, and
  inspect the history of semantic-model refreshes, including refreshes limited
  to selected tables or partitions.

* `fabric_warehouse_read_table()` and `fabric_warehouse_write_table()` read and
  bulk-write Warehouse tables using data frames or Arrow data. Tables can be
  created, appended to, overwritten, or recreated as requested.

* `fabric_workspaces()`, `fabric_items()`, and type-specific functions such as
  `fabric_lakehouses()` and `fabric_semantic_models()` discover common Fabric
  resources. Their results can be passed directly to other fabricQueryR
  functions, avoiding copied IDs and endpoints in most workflows.

## Changed

* `fabric_sql_*()`, `fabric_livy_*()`, `fabric_onelake_*()`, and other
  authenticated functions now consistently accept an AzureAuth token, a bearer
  token, or a function that supplies refreshed tokens through `token`;
  `auth_args` controls AzureAuth sign-in. The older `access_token` argument for
  SQL and Livy is deprecated. Requests also check service addresses more
  carefully and give clearer retry, timeout, and error messages.

* `fabric_job_*()`, `fabric_pbi_refresh_*()`, and `fabric_kql_*()` functions now
  provide more consistent console messages, progress displays, and summaries
  for long-running tasks.

* `fabric_livy_query()` table results now follow the declared Spark schema and
  preserve large whole numbers and decimals exactly.

* `fabric_onelake_read_delta_table()` now reads current or historical Lakehouse
  and compatible Warehouse tables through an optional Python Delta reader. It
  supports selected columns, row limits, and Arrow streams for large or nested
  results. The `dest_dir` argument has been removed; tables using unsupported
  Delta features should be read through SQL or Spark instead.

* `fabric_pbi_dax_query()` now accepts discovered semantic models or direct IDs,
  can test results for a user under row-level security, and reports incomplete
  Power BI results instead of silently returning them. An optional Arrow mode
  provides typed tibbles or streams for models that support it.

* `fabric_sql_connect()` and `fabric_sql_query()` now accept discovered items
  and portal connection strings, and support Lakehouses, Warehouses, and Fabric
  SQL Databases. Queries can use parameters and return a tibble or Arrow stream.
  The default `database = NULL` infers the database when possible;
  `fabric_sql_query()` is now limited to one read-only statement, so use
  `fabric_sql_connect()` for other SQL work.

# fabricQueryR 0.2.1

* Update e-mail address of maintainer in DESCRIPTION file (change to a 
personal e-mail address due to leaving the organization).

* `fabric_onelake_read_delta_table()`: add experimental support for specifying
a Lakehouse schema name to read from a specific schema within a Lakehouse which
has Lakehouse schemas enabled.

# fabricQueryR 0.2.0

* Added `fabric_livy_query()` to run queries against the 'Fabric Livy API',
allowing remote execution of 'Spark' code.

# fabricQueryR 0.1.1

* Initial CRAN submission.
