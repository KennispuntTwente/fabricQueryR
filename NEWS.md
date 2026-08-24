# 'fabricQueryR' (development version)

## New

* `fabric_workspaces()`, `fabric_items()`, and type-specific functions such as
`fabric_lakehouses()` and `fabric_semantic_models()` discover common Fabric
resources. Their results can be passed directly to other 'fabricQueryR'
functions, avoiding copied IDs and endpoints in most workflows.

* `fabric_livy_session()` and `fabric_livy_batch_submit()` add reusable Spark
sessions and standalone batch jobs. `fabric_livy_query()` remains the simplest
option for running one piece of Spark code. Batch application paths are now
validated as absolute ABFS/ABFSS URIs before authentication or submission.
Timeout conditions now retain the live session, statement, or batch handle for
in-process status checks and cancellation, while keeping serializable metadata.
Mixed, raw, and nested fallback output columns remain list-columns instead of
being silently coerced to a different scalar type. Livy documentation now
separates the four required delegated scopes from optional Azure-service scopes
and explains that an explicit `audience` replaces the defaults.

* `fabric_onelake_read_file()`, `fabric_onelake_write_file()`,
`fabric_onelake_download()`, and `fabric_onelake_upload()` move files and
Parquet, CSV, or Arrow data between R, local storage, and OneLake.
`fabric_onelake_list()`, `fabric_onelake_metadata()`, and
`fabric_onelake_delete()` list, inspect, and delete files. Ranged downloads now
require a matching partial-content response whose header interval and received
byte count agree, instead of accepting a complete or corrupt response. Disk
downloads now publish by atomic rename or hard link and fail closed instead of
exposing partial bytes or
temporarily removing an existing destination.
Concurrent OneLake uploads now create parents conditionally and verify that a
race winner is a directory. If the final remote rename has an unknown outcome,
the typed diagnostic reports absolute target and staging URLs and marks staging
presence as unknown. Automatic cleanup is skipped while a late commit may still
be completing.

* `fabric_lakehouse_schemas()`, `fabric_lakehouse_table()`,
`fabric_lakehouse_tables()`, `fabric_lakehouse_read_table()`,
`fabric_lakehouse_load_table()`, and `fabric_lakehouse_write_table()` discover
Lakehouse schemas and tables, read them, load CSV or Parquet files, and write
data frames or Arrow data. Both ordinary and schema-enabled Lakehouses are
supported. Singular table helpers now accept ordinary named-list records as
documented, in addition to one-row data frames and discovered records. Detail
enrichment now retains every table in the listing snapshot when a concurrent
deletion makes an individual detail request return `TableNotFound`.

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
objects. KQL partial-result errors now honor `retain_raw_frames = FALSE`, and
response-header metadata is redacted before it is attached to results or
errors.

* `fabric_kql_ingest()`, `fabric_kql_write_table()`, and `fabric_kql_export()`
load existing files or R and Arrow data into Eventhouse, monitor the load, and
export large query results to OneLake or other supported storage. A
destination table can be created when needed. `fabric_kql_write_table()` now
rejects multi-file writes that combine per-blob ingestion with one shared
idempotency key, preventing successful tracking results with omitted parts. Its
post-upload timeout is now one shared deadline: submission time reduces the
time available for status polling, and timeout conditions retain the tracking
handle whenever submission completed. Export artifact-detail timeouts now state
that the export itself completed instead of reporting it as still running.

* `fabric_graphql_*()` functions query a Fabric API for GraphQL, inspect its
schema, work through paginated results, and collect the result into tidy R
objects. Documentation now identifies Fabric's 1,000 attached-source-object
limit per GraphQL API item and its multi-item and abstraction workarounds.

* `fabric_function_invoke()` calls published Fabric User Data Functions from R.

* `fabric_onelake_shortcuts()` and `fabric_onelake_shortcut_*()` functions
inspect, create, update, and delete OneLake shortcuts, which link Fabric items
to data stored elsewhere. Raw connection-backed targets are checked locally for
structure, required fields, connection identifiers, embedded credentials, and
generic URL safety; Fabric remains authoritative for source- and
destination-specific rules.

* `fabric_onelake_shortcut_create()` now rejects fields outside Microsoft's
documented target schemas and recognizes common storage-key and connection-string
names as credentials. Shortcut responses also redact those fields recursively.

* `fabric_pbi_refresh_*()` functions start, monitor, wait for, cancel, and
inspect the history of semantic-model refreshes, including refreshes limited
to selected tables or partitions. Standard refresh status falls back to
refresh history when request-specific details are unavailable; cancellation
remains available only for enhanced refreshes. Standard refreshes now omit
`notifyOption` by default so opaque static or callback service-principal tokens
do not accidentally send an inapplicable mail-notification option; delegated
callers can request notifications explicitly. Status and cancellation calls
given only a raw refresh ID now resolve its standard or enhanced mode from
history before falling back or sending a cancellation request.

* `fabric_job_*()` functions run, monitor, wait for, and cancel Fabric
Notebooks, data pipelines, Spark job definitions, and other supported item
jobs. They also inspect run history and manage recurring schedules. Notebook
runs now use the stable Core Job Scheduler `RunNotebook` route. Status and wait
calls also default to the stable Core endpoint; beta Notebook details are an
explicit `notebook_details = TRUE` opt-in.

* `fabric_operation_*()` functions resume, monitor, and retrieve the results of
longer-running Fabric tasks such as Lakehouse loads. Non-waiting result lookup
now reads one state snapshot immediately instead of sleeping until a stored
future polling hint.

## Changed

* Authenticated functions now consistently accept an 'AzureAuth' token, a bearer
token, or a function that supplies refreshed tokens through `token`;
`auth_args` controls 'AzureAuth' sign-in. The older `access_token` argument for
SQL and Livy is deprecated. Requests also check service addresses more
carefully and give clearer retry, timeout, and error messages. Retry decisions
now use httr2's effective request method, so a body-implied POST is not replayed
unless its caller explicitly marks it idempotent. Automatic AzureAuth cache keys
now preserve punctuation and vector boundaries in custom audiences so distinct
scope sets cannot reuse the wrong cached token. User Data Function, GraphQL,
and Livy requests now require an explicitly supplied token or provider for a
custom host, preventing automatic Fabric sign-in credentials from crossing a
caller-selected endpoint boundary. User Data Function documentation now
identifies `UserDataFunction.Execute.All` as the least-privilege delegated
default and `Item.Execute.All` as a broader explicit alternative; both still
require item Execute permission. Endpoint documentation now explains that
HTTPS and URL-shape checks do not establish hostname ownership or token audience,
and limits custom gateways to organization-controlled hosts with credentials
issued for their intended audience. Supplied and acquired bearer tokens are
rejected before use if they contain whitespace or control characters that
could corrupt or inject an HTTP header. Paged collection calls now fail with a
typed, body-free protocol error when a service returns a malformed envelope,
record array, next link, or continuation token.

* `fabric_sql_tables()`, `fabric_sql_views()`, and `fabric_sql_read_table()`
provide symmetric discovery and reads across Lakehouses, Warehouses, Warehouse
snapshots, and Fabric SQL Databases. `fabric_sql_connect()` and
`fabric_sql_query()` accept the same discovered items and portal connection
strings. Queries can use parameters and return a tibble or Arrow stream. The
default `database = NULL` infers the database when possible;
`fabric_sql_query()` is limited to one read-only statement, so use
`fabric_sql_connect()` for other SQL work. View discovery reads definitions
from `sys.sql_modules` so definitions longer than 4,000 characters are not
silently truncated. Lazy Arrow query streams now retain their DBI result and
connection until the stream is released.

* `fabric_pbi_dax_query()` now accepts discovered semantic models or direct IDs,
can test results for a user under row-level security, and reports incomplete
Power BI results instead of silently returning them. An optional Arrow mode
provides typed tibbles or streams for models that support it. Both JSON and
Arrow executions now expose a client-side HTTP `timeout`, distinct from the
Arrow service's `queryTimeout` option.

* `fabric_onelake_read_delta_table()` now reads current or historical Lakehouse
and compatible Warehouse tables through an optional Python Delta reader. It
supports selected columns, row limits, and Arrow streams for large or nested
results, including through discovered workspace-private OneLake endpoints.
Unshredded Variant tables can now be queried when `columns` excludes all
Variant-containing fields; decoded Variant values and shredded Variant remain
unsupported. The `dest_dir` argument has been removed; tables using unsupported
Delta features should be read through SQL or Spark instead. Variant preflight
uses the current delta-rs schema serialization API.

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
