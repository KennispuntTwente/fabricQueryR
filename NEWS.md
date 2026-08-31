# 'fabricQueryR' (development version)

## New

* Discovery functions find the Fabric workspaces and items available to the
signed-in user or application. Use `fabric_workspaces()` and `fabric_items()`
for general discovery, or typed helpers such as `fabric_lakehouses()` and
`fabric_semantic_models()` to find a specific kind of item. Discovery results
are read-only R6 objects that include every service field, reuse the discovery
credential, and provide type-specific methods for workspaces, SQL items,
Lakehouses, Warehouses, mirrored databases, Eventhouses, KQL databases, GraphQL
APIs, semantic models, and runnable jobs. Semantic models, runnable jobs, and
KQL items also expose status, wait, and cancellation methods for asynchronous
work. Discovered resources can be passed directly to other 'fabricQueryR'
functions, avoiding copied IDs and endpoints in most workflows. Use
`$as_list()`, `as.list()`, or `output = "list"` when a plain record is
specifically required.

* `fabric_livy_session()` and `fabric_livy_batch_submit()` add reusable Spark
sessions and standalone batch jobs. `fabric_livy_query()` is the simplest
option for running one piece of Spark code.

* `fabric_onelake_read_file()`, `fabric_onelake_write_file()`,
`fabric_onelake_download()`, and `fabric_onelake_upload()` move files and
Parquet, CSV, or Arrow data between R, local storage, and OneLake.
`fabric_onelake_list()`, `fabric_onelake_metadata()`, and
`fabric_onelake_delete()` list, inspect, and delete files.

* `fabric_lakehouse_schemas()`, `fabric_lakehouse_table()`,
`fabric_lakehouse_tables()`, `fabric_lakehouse_read_table()`,
`fabric_lakehouse_load_table()`, and `fabric_lakehouse_write_table()` discover
Lakehouse schemas and tables, read them, load CSV or Parquet files, and write
data frames or Arrow data. Both ordinary and schema-enabled Lakehouses are
supported.

* `fabric_warehouse_schemas()`, `fabric_warehouse_table()`,
`fabric_warehouse_tables()`, `fabric_warehouse_read_table()`, and
`fabric_warehouse_write_table()` discover schemas and tables, read them, and
bulk-write Warehouse tables using data frames or Arrow data. Tables can be
created, appended to, overwritten, or recreated as requested.

* `fabric_mirrored_databases()` and the `fabric_mirrored_database_*()` helpers
discover mirrored databases and inspect or read their OneLake Delta tables.
Discovered records also work with the generic SQL helpers through each mirrored
database's read-only SQL analytics endpoint.

* `fabric_kql_tables()`, `fabric_kql_query()`, and `fabric_kql_read_table()`
discover Eventhouse tables and bring query or table results into R as typed R
objects.

* `fabric_kql_ingest()`, `fabric_kql_write_table()`, and `fabric_kql_export()`
load existing files or R and Arrow data into Eventhouse, monitor the load, and
export large query results to OneLake or other supported storage. A
destination table can be created when needed. 

* `fabric_graphql_*()` functions query a Fabric API for GraphQL, inspect its
schema, work through paginated results, and collect the result into tidy R
objects.

* `fabric_function_invoke()` calls published Fabric User Data Functions from R.

* `fabric_onelake_shortcuts()` and `fabric_onelake_shortcut_*()` functions
inspect, create, update, and delete OneLake shortcuts, which link Fabric items
to data stored elsewhere.

* `fabric_pbi_refresh_*()` functions start, monitor, wait for, cancel, and
inspect the history of semantic-model refreshes.

* `fabric_job_*()` functions run, monitor, wait for, and cancel Fabric
Notebooks, data pipelines, Spark job definitions, and other supported item
jobs. They also inspect run history and manage recurring schedules.

* `fabric_operation_*()` functions resume, monitor, and retrieve the results of
longer-running Fabric tasks such as Lakehouse loads.

## Changed

* Authenticated functions now consistently accept an 'AzureAuth' token, a bearer
token, or a function that supplies refreshed tokens through `token`;
`auth_args` controls 'AzureAuth' sign-in. The older `access_token` argument for
SQL and Livy is deprecated.

* `fabric_catalog_search()` now follows Fabric's continuation-token contract by
sending search and filter parameters only on the first page.

* `fabric_onelake_schema_exists()` and `fabric_onelake_table_exists()` now use
paginated Delta metadata inventories, avoiding HTTP 400 responses from
OneLake's documented `HEAD` routes.

* `fabric_pbi_dax_query()` now accepts discovered semantic models or direct IDs,
can test results for a user under row-level security, and reports incomplete
Power BI results instead of silently returning them. An optional Arrow mode
provides typed tibbles or streams for models that support it. Both JSON and
Arrow executions now expose a client-side HTTP `timeout`.

* `fabric_onelake_read_delta_table()` now reads current or historical Lakehouse
and compatible Warehouse tables through an optional Python Delta reader. It
supports selected columns, row limits, and Arrow streams for large or nested
results, including through discovered workspace-private OneLake endpoints.
The `dest_dir` argument has been removed. Tables using unsupported
Delta features should be read through SQL or Spark instead.

* `fabric_livy_query()` table results now follow the declared Spark schema and
preserve large whole numbers and decimals exactly.

* `fabric_livy_query()` now bounds temporary-session cleanup with a separate
deadline and reports both errors when statement execution and session deletion
fail together.

# 'fabricQueryR' 0.2.1

* Update e-mail address of maintainer in DESCRIPTION file (change to a 
personal e-mail address due to leaving the organization).

* `fabric_onelake_read_delta_table()`: add experimental support for specifying
a Lakehouse schema name to read from a specific schema within a Lakehouse which
has Lakehouse schemas enabled.

# 'fabricQueryR' 0.2.0

* Added `fabric_livy_query()` to run queries against the 'Fabric Livy API',
allowing remote execution of 'Spark' code.

# 'fabricQueryR' 0.1.1

* Initial CRAN submission.
