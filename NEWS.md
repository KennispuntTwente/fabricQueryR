# fabricQueryR (development version)

## Find and connect to Fabric

* `fabric_workspaces()`, `fabric_items()`, and helpers such as
  `fabric_lakehouses()` and `fabric_semantic_models()` can now discover common
  Fabric resources. Their results can be passed directly to other fabricQueryR
  functions, so most workflows no longer require copied IDs or endpoints.

* `fabric_sql_connect()` and `fabric_sql_query()` now accept discovered items
  and portal connection strings, and support Lakehouses, Warehouses, and Fabric
  SQL Databases. Queries can use parameters and return either a tibble or an
  Arrow stream. The default `database = NULL` now infers the database when
  possible; `fabric_sql_query()` is limited to one read-only statement, so use
  `fabric_sql_connect()` for other SQL work.

* Authentication is now consistent across the package: `token` accepts an
  AzureAuth token, a bearer token, or a function that supplies refreshed tokens,
  while `auth_args` controls AzureAuth sign-in. The older `access_token`
  argument for SQL and Livy is deprecated. Authenticated requests also check
  service addresses more carefully and give clearer retry, timeout, and error
  messages.

## Read and write Fabric data

* New OneLake helpers list, inspect, download, upload, and delete files, or
  read and write Parquet, CSV, and Arrow files as R or Arrow objects. Shortcut
  helpers also manage links from a Lakehouse or other OneLake item to data
  stored elsewhere.

* New Lakehouse table helpers discover and read tables, load existing CSV or
  Parquet files, and write data frames or Arrow data. They support both ordinary
  and schema-enabled Lakehouses and can return large results as Arrow streams.

* `fabric_onelake_read_delta_table()` now reads current or historical Lakehouse
  and compatible Warehouse tables through an optional Python Delta reader. It
  supports selecting columns, limiting rows, and streaming large or nested
  results. The removed `dest_dir` argument is no longer needed; tables using
  unsupported Delta features should be read through SQL or Spark instead.

* `fabric_warehouse_read_table()` and `fabric_warehouse_write_table()` add
  direct Warehouse table reads and bulk writes for data frames and Arrow data,
  including creating tables and appending, overwriting, or recreating them when
  requested.

* New Eventhouse helpers query KQL databases and read tables used for event and
  real-time data. They can also ingest existing files or R and Arrow data,
  create a destination table when requested, monitor the load, and export large
  query results to OneLake or other supported storage.

* New GraphQL helpers run queries against a Fabric API for GraphQL, inspect its
  schema, work through paginated results, and collect the result into tidy R
  objects.

## Run and automate Fabric workloads

* `fabric_pbi_dax_query()` now accepts discovered semantic models or direct IDs,
  can test results for a user under row-level security, and reports incomplete
  Power BI results instead of silently returning them. An optional Arrow mode
  provides typed tibbles or streams for models that support it.

* New semantic-model refresh helpers start, monitor, wait for, cancel, and
  inspect the history of Power BI refreshes, including refreshes limited to
  selected tables or partitions.

* Livy support now includes reusable Spark sessions and standalone batch jobs,
  while `fabric_livy_query()` remains the simplest option for one piece of
  Spark code. Table results now preserve the declared Spark types, including
  large whole numbers and decimals.

* New job helpers run, monitor, wait for, and cancel Fabric Notebooks, data
  pipelines, Spark job definitions, and other supported item jobs. They also
  inspect run history and manage recurring schedules.

* `fabric_function_invoke()` can call published Fabric User Data Functions from
  R. Shared operation helpers can resume, monitor, and retrieve the results of
  longer-running Fabric tasks such as Lakehouse loads.

* Console messages and progress displays are now more consistent across the
  package, with clearer summaries for jobs, refreshes, KQL operations, and other
  long-running tasks.

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
