# R6 objects for discovered Microsoft Fabric resources

These generators back the default `output = "r6"` discovery interface.
Users normally receive objects from
[`fabric_workspaces()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_workspaces.md),
[`fabric_items()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_items.md),
[`fabric_item()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_item.md),
the typed discovery helpers, or
[`fabric_catalog_search()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_catalog_search.md)
rather than constructing them directly.

## Format

An [R6::R6Class](https://r6.r-lib.org/reference/R6Class.html) generator.

## Value

The corresponding R6 generator.

## Details

Every object includes the complete Fabric API record. Read
non-conflicting fields directly with `$`; use `$get()` for
collision-safe field access and `$as_list()` or
[`as.list()`](https://rdrr.io/r/base/list.html) for a plain record.
`$get()` is an object-only field helper. Record fields are read-only.

Methods delegate to the corresponding `fabric_*()` function. Their `...`
arguments are forwarded unchanged, and the credential used for discovery
is reused while the object is in the current R process. An explicitly
supplied `token`, `tenant_id`, `client_id`, `auth_args`, or `api_base`
takes precedence. The Fabric API base used for discovery is also reused,
so chained methods stay on the same public, sovereign-cloud, or
workspace endpoint.

SQL-capable resources inherit common `sql_*()` methods. Lakehouses,
Warehouses, mirrored databases, Eventhouses, KQL databases, GraphQL
APIs, semantic models, and runnable job items add workload-specific
methods. Other discovered types are returned as `FabricItem` objects
with `$details()`
([`fabric_item()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_item.md))
and record access. They do not expose methods that cannot operate from
discovery metadata. This generic fallback also applies to typed
Environment and User Data Function discovery; a typed helper and
workload detail route do not by themselves imply a specialized R6 class.

## Super class

`FabricRecord` -\> `FabricWorkspace`

## Methods

### Public methods

- [`FabricWorkspace$new()`](#method-FabricWorkspace-initialize)

- [`FabricWorkspace$items()`](#method-FabricWorkspace-items)

- [`FabricWorkspace$item()`](#method-FabricWorkspace-item)

- [`FabricWorkspace$lakehouses()`](#method-FabricWorkspace-lakehouses)

- [`FabricWorkspace$warehouses()`](#method-FabricWorkspace-warehouses)

- [`FabricWorkspace$warehouse_snapshots()`](#method-FabricWorkspace-warehouse_snapshots)

- [`FabricWorkspace$mirrored_databases()`](#method-FabricWorkspace-mirrored_databases)

- [`FabricWorkspace$sql_databases()`](#method-FabricWorkspace-sql_databases)

- [`FabricWorkspace$semantic_models()`](#method-FabricWorkspace-semantic_models)

- [`FabricWorkspace$eventhouses()`](#method-FabricWorkspace-eventhouses)

- [`FabricWorkspace$kql_databases()`](#method-FabricWorkspace-kql_databases)

- [`FabricWorkspace$notebooks()`](#method-FabricWorkspace-notebooks)

- [`FabricWorkspace$data_pipelines()`](#method-FabricWorkspace-data_pipelines)

- [`FabricWorkspace$spark_job_definitions()`](#method-FabricWorkspace-spark_job_definitions)

- [`FabricWorkspace$environments()`](#method-FabricWorkspace-environments)

- [`FabricWorkspace$user_data_functions()`](#method-FabricWorkspace-user_data_functions)

- [`FabricWorkspace$graphql_apis()`](#method-FabricWorkspace-graphql_apis)

- [`FabricWorkspace$shortcut_cache_reset()`](#method-FabricWorkspace-shortcut_cache_reset)

Inherited methods

- `FabricRecord$as_list()`
- `FabricRecord$field_names()`
- `FabricRecord$get()`
- `FabricRecord$print()`

------------------------------------------------------------------------

### `FabricWorkspace$new()`

Internal constructor used by discovery factories.

#### Usage

    FabricWorkspace$new(
      record,
      legacy_class = c("fabric_workspace", "list"),
      credential = NULL,
      api_base = NULL
    )

#### Arguments

- `record`:

  One named Fabric workspace record.

- `legacy_class`:

  Classes assigned by `$as_list()`.

- `credential`:

  Optional internal authentication credential.

- `api_base`:

  Optional Fabric REST API base inherited from discovery.

------------------------------------------------------------------------

### `FabricWorkspace$items()`

Discover items in this workspace.

#### Usage

    FabricWorkspace$items(...)

#### Arguments

- `...`:

  Arguments forwarded to
  [`fabric_items()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_items.md).

#### Returns

A list of `FabricItem` objects or type-specific subclasses.

------------------------------------------------------------------------

### `FabricWorkspace$item()`

Discover and enrich one item in this workspace.

#### Usage

    FabricWorkspace$item(item, ...)

#### Arguments

- `item`:

  Item GUID, display name, or discovered item.

- `...`:

  Arguments forwarded to
  [`fabric_item()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_item.md).

#### Returns

A `FabricItem` object or one of its subclasses.

------------------------------------------------------------------------

### `FabricWorkspace$lakehouses()`

Discover Lakehouses in this workspace.

#### Usage

    FabricWorkspace$lakehouses(detail = TRUE, ...)

#### Arguments

- `detail`:

  Whether to retrieve workload details.

- `...`:

  Additional discovery arguments.

#### Returns

A list of `FabricLakehouse` objects.

------------------------------------------------------------------------

### `FabricWorkspace$warehouses()`

Discover Warehouses in this workspace.

#### Usage

    FabricWorkspace$warehouses(detail = TRUE, ...)

#### Arguments

- `detail`:

  Whether to retrieve workload details.

- `...`:

  Additional discovery arguments.

#### Returns

A list of `FabricWarehouse` objects.

------------------------------------------------------------------------

### `FabricWorkspace$warehouse_snapshots()`

Discover Warehouse snapshots in this workspace.

#### Usage

    FabricWorkspace$warehouse_snapshots(detail = TRUE, ...)

#### Arguments

- `detail`:

  Whether to retrieve workload details.

- `...`:

  Additional discovery arguments.

#### Returns

A list of `FabricWarehouseSnapshot` objects.

------------------------------------------------------------------------

### `FabricWorkspace$mirrored_databases()`

Discover mirrored databases in this workspace.

#### Usage

    FabricWorkspace$mirrored_databases(detail = TRUE, ...)

#### Arguments

- `detail`:

  Whether to retrieve workload details.

- `...`:

  Additional discovery arguments.

#### Returns

A list of `FabricMirroredDatabase` objects.

------------------------------------------------------------------------

### `FabricWorkspace$sql_databases()`

Discover SQL databases in this workspace.

#### Usage

    FabricWorkspace$sql_databases(detail = TRUE, ...)

#### Arguments

- `detail`:

  Whether to retrieve workload details.

- `...`:

  Additional discovery arguments.

#### Returns

A list of `FabricSqlDatabase` objects.

------------------------------------------------------------------------

### `FabricWorkspace$semantic_models()`

Discover semantic models in this workspace.

#### Usage

    FabricWorkspace$semantic_models(detail = FALSE, ...)

#### Arguments

- `detail`:

  Whether to retrieve workload details.

- `...`:

  Additional discovery arguments.

#### Returns

A list of `FabricSemanticModel` objects.

------------------------------------------------------------------------

### `FabricWorkspace$eventhouses()`

Discover Eventhouses in this workspace.

#### Usage

    FabricWorkspace$eventhouses(detail = TRUE, ...)

#### Arguments

- `detail`:

  Whether to retrieve workload details.

- `...`:

  Additional discovery arguments.

#### Returns

A list of `FabricEventhouse` objects.

------------------------------------------------------------------------

### `FabricWorkspace$kql_databases()`

Discover KQL databases in this workspace.

#### Usage

    FabricWorkspace$kql_databases(detail = TRUE, ...)

#### Arguments

- `detail`:

  Whether to retrieve workload details.

- `...`:

  Additional discovery arguments.

#### Returns

A list of `FabricKqlDatabase` objects.

------------------------------------------------------------------------

### `FabricWorkspace$notebooks()`

Discover notebooks in this workspace.

#### Usage

    FabricWorkspace$notebooks(detail = TRUE, ...)

#### Arguments

- `detail`:

  Whether to retrieve workload details.

- `...`:

  Additional discovery arguments.

#### Returns

A list of `FabricJobItem` objects.

------------------------------------------------------------------------

### `FabricWorkspace$data_pipelines()`

Discover data pipelines in this workspace.

#### Usage

    FabricWorkspace$data_pipelines(detail = TRUE, ...)

#### Arguments

- `detail`:

  Whether to retrieve workload details.

- `...`:

  Additional discovery arguments.

#### Returns

A list of `FabricJobItem` objects.

------------------------------------------------------------------------

### `FabricWorkspace$spark_job_definitions()`

Discover Spark job definitions in this workspace.

#### Usage

    FabricWorkspace$spark_job_definitions(detail = TRUE, ...)

#### Arguments

- `detail`:

  Whether to retrieve workload details.

- `...`:

  Additional discovery arguments.

#### Returns

A list of `FabricJobItem` objects.

------------------------------------------------------------------------

### `FabricWorkspace$environments()`

Discover environments in this workspace.

#### Usage

    FabricWorkspace$environments(detail = TRUE, ...)

#### Arguments

- `detail`:

  Whether to retrieve workload details.

- `...`:

  Additional discovery arguments.

#### Returns

A list of `FabricItem` objects.

------------------------------------------------------------------------

### `FabricWorkspace$user_data_functions()`

Discover User Data Functions in this workspace.

#### Usage

    FabricWorkspace$user_data_functions(detail = FALSE, ...)

#### Arguments

- `detail`:

  Whether to retrieve workload details.

- `...`:

  Additional discovery arguments.

#### Returns

A list of `FabricItem` objects.

------------------------------------------------------------------------

### `FabricWorkspace$graphql_apis()`

Discover GraphQL APIs in this workspace.

#### Usage

    FabricWorkspace$graphql_apis(detail = FALSE, ...)

#### Arguments

- `detail`:

  Whether to retrieve workload details.

- `...`:

  Additional discovery arguments.

#### Returns

A list of `FabricGraphQLApi` objects.

------------------------------------------------------------------------

### `FabricWorkspace$shortcut_cache_reset()`

Reset this workspace's OneLake shortcut cache.

#### Usage

    FabricWorkspace$shortcut_cache_reset(...)

#### Arguments

- `...`:

  Arguments forwarded to
  [`fabric_onelake_shortcut_cache_reset()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_shortcuts.md).

#### Returns

A `fabric_operation` handle.

## Super class

`FabricRecord` -\> `FabricItem`

## Methods

### Public methods

- [`FabricItem$new()`](#method-FabricItem-initialize)

- [`FabricItem$details()`](#method-FabricItem-details)

Inherited methods

- `FabricRecord$as_list()`
- `FabricRecord$field_names()`
- `FabricRecord$get()`
- `FabricRecord$print()`

------------------------------------------------------------------------

### `FabricItem$new()`

Internal constructor used by discovery factories.

#### Usage

    FabricItem$new(
      record,
      legacy_class = c("fabric_item", "list"),
      credential = NULL,
      api_base = NULL
    )

#### Arguments

- `record`:

  One named Fabric item record.

- `legacy_class`:

  Classes assigned by `$as_list()`.

- `credential`:

  Optional internal authentication credential.

- `api_base`:

  Optional Fabric REST API base inherited from discovery.

------------------------------------------------------------------------

### `FabricItem$details()`

Retrieve a fresh item record and supported workload details. User Data
Function workload details require `detail = TRUE` and a delegated user
identity.

#### Usage

    FabricItem$details(...)

#### Arguments

- `...`:

  Arguments forwarded to
  [`fabric_item()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_item.md).

#### Returns

A new `FabricItem` object or one of its subclasses.

## Super classes

`FabricRecord` -\> `FabricItem` -\> `FabricSqlItem` -\>
`FabricLakehouse`

## Methods

### Public methods

- [`FabricLakehouse$schemas()`](#method-FabricLakehouse-schemas)

- [`FabricLakehouse$table()`](#method-FabricLakehouse-table)

- [`FabricLakehouse$tables()`](#method-FabricLakehouse-tables)

- [`FabricLakehouse$read_table()`](#method-FabricLakehouse-read_table)

- [`FabricLakehouse$read_delta_table()`](#method-FabricLakehouse-read_delta_table)

- [`FabricLakehouse$load_table()`](#method-FabricLakehouse-load_table)

- [`FabricLakehouse$write_table()`](#method-FabricLakehouse-write_table)

- [`FabricLakehouse$livy_query()`](#method-FabricLakehouse-livy_query)

- [`FabricLakehouse$livy_session()`](#method-FabricLakehouse-livy_session)

- [`FabricLakehouse$livy_batch_submit()`](#method-FabricLakehouse-livy_batch_submit)

- [`FabricLakehouse$onelake_list()`](#method-FabricLakehouse-onelake_list)

- [`FabricLakehouse$onelake_metadata()`](#method-FabricLakehouse-onelake_metadata)

- [`FabricLakehouse$onelake_read_file()`](#method-FabricLakehouse-onelake_read_file)

- [`FabricLakehouse$onelake_write_file()`](#method-FabricLakehouse-onelake_write_file)

- [`FabricLakehouse$onelake_download()`](#method-FabricLakehouse-onelake_download)

- [`FabricLakehouse$onelake_upload()`](#method-FabricLakehouse-onelake_upload)

- [`FabricLakehouse$onelake_delete()`](#method-FabricLakehouse-onelake_delete)

- [`FabricLakehouse$schema_exists()`](#method-FabricLakehouse-schema_exists)

- [`FabricLakehouse$table_exists()`](#method-FabricLakehouse-table_exists)

- [`FabricLakehouse$shortcuts()`](#method-FabricLakehouse-shortcuts)

- [`FabricLakehouse$shortcut()`](#method-FabricLakehouse-shortcut)

- [`FabricLakehouse$shortcut_create()`](#method-FabricLakehouse-shortcut_create)

- [`FabricLakehouse$shortcuts_bulk_create()`](#method-FabricLakehouse-shortcuts_bulk_create)

- [`FabricLakehouse$shortcut_delete()`](#method-FabricLakehouse-shortcut_delete)

Inherited methods

- `FabricRecord$as_list()`
- `FabricRecord$field_names()`
- `FabricRecord$get()`
- `FabricRecord$print()`
- [`FabricItem$details()`](https://kennispunttwente.github.io/fabricQueryR/reference/FabricItem.html#method-details)
- [`FabricItem$initialize()`](https://kennispunttwente.github.io/fabricQueryR/reference/FabricItem.html#method-initialize)
- `FabricSqlItem$sql_connect()`
- `FabricSqlItem$sql_connection_info()`
- `FabricSqlItem$sql_query()`
- `FabricSqlItem$sql_read_table()`
- `FabricSqlItem$sql_tables()`
- `FabricSqlItem$sql_views()`

------------------------------------------------------------------------

### `FabricLakehouse$schemas()`

List Lakehouse schemas.

#### Usage

    FabricLakehouse$schemas(...)

#### Arguments

- `...`:

  Arguments forwarded to
  [`fabric_lakehouse_schemas()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_catalog.md).

#### Returns

A schema inventory tibble.

------------------------------------------------------------------------

### `FabricLakehouse$table()`

Retrieve one Lakehouse table's metadata.

#### Usage

    FabricLakehouse$table(table, ...)

#### Arguments

- `table`:

  Table name or discovered table row.

- `...`:

  Arguments forwarded to
  [`fabric_lakehouse_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_catalog.md).

#### Returns

A one-row table metadata tibble.

------------------------------------------------------------------------

### `FabricLakehouse$tables()`

List Lakehouse tables.

#### Usage

    FabricLakehouse$tables(...)

#### Arguments

- `...`:

  Arguments forwarded to
  [`fabric_lakehouse_tables()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_lakehouse_tables.md).

#### Returns

A table inventory tibble.

------------------------------------------------------------------------

### `FabricLakehouse$read_table()`

Read one managed Delta table.

#### Usage

    FabricLakehouse$read_table(table, ...)

#### Arguments

- `table`:

  Table name or discovered table row.

- `...`:

  Arguments forwarded to
  [`fabric_lakehouse_read_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_lakehouse_read_table.md).

#### Returns

A tibble or Arrow stream.

------------------------------------------------------------------------

### `FabricLakehouse$read_delta_table()`

Read a Delta table directly from OneLake.

#### Usage

    FabricLakehouse$read_delta_table(table_path, ...)

#### Arguments

- `table_path`:

  Table path below the item.

- `...`:

  Arguments forwarded to
  [`fabric_onelake_read_delta_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_read_delta_table.md).

#### Returns

A tibble or Arrow stream.

------------------------------------------------------------------------

### `FabricLakehouse$load_table()`

Load an existing file into a managed table.

#### Usage

    FabricLakehouse$load_table(table, path, ...)

#### Arguments

- `table`:

  Destination table name.

- `path`:

  Source path under the Lakehouse.

- `...`:

  Arguments forwarded to
  [`fabric_lakehouse_load_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_lakehouse_tables.md).

#### Returns

A `fabric_operation` or completed operation result.

------------------------------------------------------------------------

### `FabricLakehouse$write_table()`

Write R or Arrow data to a managed table.

#### Usage

    FabricLakehouse$write_table(table, data, ...)

#### Arguments

- `table`:

  Destination table name.

- `data`:

  Data frame or Arrow-compatible source.

- `...`:

  Arguments forwarded to
  [`fabric_lakehouse_write_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_lakehouse_tables.md).

#### Returns

A completed write result.

------------------------------------------------------------------------

### `FabricLakehouse$livy_query()`

Run one Spark statement through Livy.

#### Usage

    FabricLakehouse$livy_query(code, ...)

#### Arguments

- `code`:

  Spark, PySpark, SparkR, or Spark SQL code.

- `...`:

  Arguments forwarded to
  [`fabric_livy_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_livy_query.md).

#### Returns

A Livy statement result.

------------------------------------------------------------------------

### `FabricLakehouse$livy_session()`

Create a reusable Livy session.

#### Usage

    FabricLakehouse$livy_session(...)

#### Arguments

- `...`:

  Arguments forwarded to
  [`fabric_livy_session()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_livy_session.md).

#### Returns

A
[FabricLivySession](https://kennispunttwente.github.io/fabricQueryR/reference/FabricLivySession.md).

------------------------------------------------------------------------

### `FabricLakehouse$livy_batch_submit()`

Submit a standalone Livy batch.

#### Usage

    FabricLakehouse$livy_batch_submit(file, ...)

#### Arguments

- `file`:

  ABFSS application-file URI.

- `...`:

  Arguments forwarded to
  [`fabric_livy_batch_submit()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_livy_batch_submit.md).

#### Returns

A
[FabricLivyBatch](https://kennispunttwente.github.io/fabricQueryR/reference/FabricLivyBatch.md)
or its result.

------------------------------------------------------------------------

### `FabricLakehouse$onelake_list()`

List OneLake files and directories.

#### Usage

    FabricLakehouse$onelake_list(path = "", ...)

#### Arguments

- `path`:

  Path within the Lakehouse.

- `...`:

  Arguments forwarded to
  [`fabric_onelake_list()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_files.md).

#### Returns

A file inventory tibble.

------------------------------------------------------------------------

### `FabricLakehouse$onelake_metadata()`

Retrieve OneLake path metadata.

#### Usage

    FabricLakehouse$onelake_metadata(path = "", ...)

#### Arguments

- `path`:

  Path within the Lakehouse.

- `...`:

  Arguments forwarded to
  [`fabric_onelake_metadata()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_files.md).

#### Returns

A one-row metadata tibble.

------------------------------------------------------------------------

### `FabricLakehouse$onelake_read_file()`

Read a supported file from OneLake.

#### Usage

    FabricLakehouse$onelake_read_file(path, ...)

#### Arguments

- `path`:

  Path within the Lakehouse.

- `...`:

  Arguments forwarded to
  [`fabric_onelake_read_file()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_object_files.md).

#### Returns

A tibble or Arrow stream.

------------------------------------------------------------------------

### `FabricLakehouse$onelake_write_file()`

Write data to a supported OneLake file.

#### Usage

    FabricLakehouse$onelake_write_file(path, data, ...)

#### Arguments

- `path`:

  Destination path within the Lakehouse.

- `data`:

  Data to write.

- `...`:

  Arguments forwarded to
  [`fabric_onelake_write_file()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_object_files.md).

#### Returns

A file-write result.

------------------------------------------------------------------------

### `FabricLakehouse$onelake_download()`

Download one OneLake file.

#### Usage

    FabricLakehouse$onelake_download(path, ...)

#### Arguments

- `path`:

  Source path within the Lakehouse.

- `...`:

  Arguments forwarded to
  [`fabric_onelake_download()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_files.md).

#### Returns

The local destination path.

------------------------------------------------------------------------

### `FabricLakehouse$onelake_upload()`

Upload a local file or raw vector to OneLake.

#### Usage

    FabricLakehouse$onelake_upload(path, source, ...)

#### Arguments

- `path`:

  Destination path within the Lakehouse.

- `source`:

  Local path or raw vector.

- `...`:

  Arguments forwarded to
  [`fabric_onelake_upload()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_files.md).

#### Returns

A file-write result.

------------------------------------------------------------------------

### `FabricLakehouse$onelake_delete()`

Delete a OneLake path.

#### Usage

    FabricLakehouse$onelake_delete(path, ...)

#### Arguments

- `path`:

  Path within the Lakehouse.

- `...`:

  Arguments forwarded to
  [`fabric_onelake_delete()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_files.md).

#### Returns

`TRUE`, invisibly, after deletion.

------------------------------------------------------------------------

### `FabricLakehouse$schema_exists()`

Check whether a schema exists.

#### Usage

    FabricLakehouse$schema_exists(schema, ...)

#### Arguments

- `schema`:

  Schema name.

- `...`:

  Arguments forwarded to
  [`fabric_onelake_schema_exists()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_schema_exists.md).

#### Returns

One logical value.

------------------------------------------------------------------------

### `FabricLakehouse$table_exists()`

Check whether a table exists.

#### Usage

    FabricLakehouse$table_exists(table, ...)

#### Arguments

- `table`:

  Table name or discovered table row.

- `...`:

  Arguments forwarded to
  [`fabric_onelake_table_exists()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_schema_exists.md).

#### Returns

One logical value.

------------------------------------------------------------------------

### `FabricLakehouse$shortcuts()`

List OneLake shortcuts.

#### Usage

    FabricLakehouse$shortcuts(...)

#### Arguments

- `...`:

  Arguments forwarded to
  [`fabric_onelake_shortcuts()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_shortcuts.md).

#### Returns

A shortcut inventory tibble.

------------------------------------------------------------------------

### `FabricLakehouse$shortcut()`

Retrieve one OneLake shortcut.

#### Usage

    FabricLakehouse$shortcut(path, name, ...)

#### Arguments

- `path`:

  Shortcut parent path.

- `name`:

  Shortcut name.

- `...`:

  Arguments forwarded to
  [`fabric_onelake_shortcut_get()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_shortcuts.md).

#### Returns

A one-row shortcut tibble.

------------------------------------------------------------------------

### `FabricLakehouse$shortcut_create()`

Create a OneLake shortcut.

#### Usage

    FabricLakehouse$shortcut_create(path, name, target, ...)

#### Arguments

- `path`:

  Shortcut parent path.

- `name`:

  Shortcut name.

- `target`:

  Shortcut target.

- `...`:

  Arguments forwarded to
  [`fabric_onelake_shortcut_create()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_shortcuts.md).

#### Returns

A one-row shortcut tibble.

------------------------------------------------------------------------

### `FabricLakehouse$shortcuts_bulk_create()`

Create multiple OneLake shortcuts.

#### Usage

    FabricLakehouse$shortcuts_bulk_create(shortcuts, ...)

#### Arguments

- `shortcuts`:

  Shortcut request lists.

- `...`:

  Arguments forwarded to
  [`fabric_onelake_shortcuts_bulk_create()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_shortcuts.md).

#### Returns

A `fabric_operation` handle.

------------------------------------------------------------------------

### `FabricLakehouse$shortcut_delete()`

Delete one OneLake shortcut.

#### Usage

    FabricLakehouse$shortcut_delete(path, name, ...)

#### Arguments

- `path`:

  Shortcut parent path.

- `name`:

  Shortcut name.

- `...`:

  Arguments forwarded to
  [`fabric_onelake_shortcut_delete()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_shortcuts.md).

#### Returns

`TRUE`, invisibly, after deletion.

## Super classes

`FabricRecord` -\> `FabricItem` -\> `FabricSqlItem` -\>
`FabricWarehouse`

## Methods

### Public methods

- [`FabricWarehouse$schemas()`](#method-FabricWarehouse-schemas)

- [`FabricWarehouse$table()`](#method-FabricWarehouse-table)

- [`FabricWarehouse$tables()`](#method-FabricWarehouse-tables)

- [`FabricWarehouse$read_table()`](#method-FabricWarehouse-read_table)

- [`FabricWarehouse$write_table()`](#method-FabricWarehouse-write_table)

- [`FabricWarehouse$read_delta_table()`](#method-FabricWarehouse-read_delta_table)

Inherited methods

- `FabricRecord$as_list()`
- `FabricRecord$field_names()`
- `FabricRecord$get()`
- `FabricRecord$print()`
- [`FabricItem$details()`](https://kennispunttwente.github.io/fabricQueryR/reference/FabricItem.html#method-details)
- [`FabricItem$initialize()`](https://kennispunttwente.github.io/fabricQueryR/reference/FabricItem.html#method-initialize)
- `FabricSqlItem$sql_connect()`
- `FabricSqlItem$sql_connection_info()`
- `FabricSqlItem$sql_query()`
- `FabricSqlItem$sql_read_table()`
- `FabricSqlItem$sql_tables()`
- `FabricSqlItem$sql_views()`

------------------------------------------------------------------------

### `FabricWarehouse$schemas()`

List Warehouse schemas.

#### Usage

    FabricWarehouse$schemas(...)

#### Arguments

- `...`:

  Arguments forwarded to
  [`fabric_warehouse_schemas()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_catalog.md).

#### Returns

A schema inventory tibble.

------------------------------------------------------------------------

### `FabricWarehouse$table()`

Retrieve one Warehouse table's metadata.

#### Usage

    FabricWarehouse$table(table, ...)

#### Arguments

- `table`:

  Table name or discovered table row.

- `...`:

  Arguments forwarded to
  [`fabric_warehouse_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_catalog.md).

#### Returns

A one-row table metadata tibble.

------------------------------------------------------------------------

### `FabricWarehouse$tables()`

List Warehouse tables.

#### Usage

    FabricWarehouse$tables(...)

#### Arguments

- `...`:

  Arguments forwarded to
  [`fabric_warehouse_tables()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_warehouse_tables.md).

#### Returns

A table inventory tibble.

------------------------------------------------------------------------

### `FabricWarehouse$read_table()`

Read one Warehouse table.

#### Usage

    FabricWarehouse$read_table(table, ...)

#### Arguments

- `table`:

  Table name or discovered table row.

- `...`:

  Arguments forwarded to
  [`fabric_warehouse_read_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_warehouse_read_table.md).

#### Returns

A tibble or Arrow stream.

------------------------------------------------------------------------

### `FabricWarehouse$write_table()`

Write R or Arrow data to a Warehouse table.

#### Usage

    FabricWarehouse$write_table(table, data, staging_lakehouse, ...)

#### Arguments

- `table`:

  Destination table name.

- `data`:

  Data frame or Arrow-compatible source.

- `staging_lakehouse`:

  Lakehouse used for staged Parquet data.

- `...`:

  Arguments forwarded to
  [`fabric_warehouse_write_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_warehouse_write_table.md).

#### Returns

A completed write result.

------------------------------------------------------------------------

### `FabricWarehouse$read_delta_table()`

Read a Delta table directly from OneLake.

#### Usage

    FabricWarehouse$read_delta_table(table_path, ...)

#### Arguments

- `table_path`:

  Table path below the item.

- `...`:

  Arguments forwarded to
  [`fabric_onelake_read_delta_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_read_delta_table.md).

#### Returns

A tibble or Arrow stream.

## Super classes

`FabricRecord` -\> `FabricItem` -\> `FabricSqlItem` -\>
`FabricWarehouseSnapshot`

## Super classes

`FabricRecord` -\> `FabricItem` -\> `FabricSqlItem` -\>
`FabricSqlDatabase`

## Super classes

`FabricRecord` -\> `FabricItem` -\> `FabricSqlItem` -\>
`FabricMirroredDatabase`

## Methods

### Public methods

- [`FabricMirroredDatabase$schemas()`](#method-FabricMirroredDatabase-schemas)

- [`FabricMirroredDatabase$table()`](#method-FabricMirroredDatabase-table)

- [`FabricMirroredDatabase$tables()`](#method-FabricMirroredDatabase-tables)

- [`FabricMirroredDatabase$read_table()`](#method-FabricMirroredDatabase-read_table)

- [`FabricMirroredDatabase$read_delta_table()`](#method-FabricMirroredDatabase-read_delta_table)

Inherited methods

- `FabricRecord$as_list()`
- `FabricRecord$field_names()`
- `FabricRecord$get()`
- `FabricRecord$print()`
- [`FabricItem$details()`](https://kennispunttwente.github.io/fabricQueryR/reference/FabricItem.html#method-details)
- [`FabricItem$initialize()`](https://kennispunttwente.github.io/fabricQueryR/reference/FabricItem.html#method-initialize)
- `FabricSqlItem$sql_connect()`
- `FabricSqlItem$sql_connection_info()`
- `FabricSqlItem$sql_query()`
- `FabricSqlItem$sql_read_table()`
- `FabricSqlItem$sql_tables()`
- `FabricSqlItem$sql_views()`

------------------------------------------------------------------------

### `FabricMirroredDatabase$schemas()`

List mirrored database schemas.

#### Usage

    FabricMirroredDatabase$schemas(...)

#### Arguments

- `...`:

  Arguments forwarded to
  [`fabric_mirrored_database_schemas()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_mirrored_database_tables.md).

#### Returns

A schema inventory tibble.

------------------------------------------------------------------------

### `FabricMirroredDatabase$table()`

Retrieve one mirrored table's metadata.

#### Usage

    FabricMirroredDatabase$table(table, ...)

#### Arguments

- `table`:

  Table name or discovered table row.

- `...`:

  Arguments forwarded to
  [`fabric_mirrored_database_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_mirrored_database_tables.md).

#### Returns

A one-row table metadata tibble.

------------------------------------------------------------------------

### `FabricMirroredDatabase$tables()`

List mirrored database tables.

#### Usage

    FabricMirroredDatabase$tables(...)

#### Arguments

- `...`:

  Arguments forwarded to
  [`fabric_mirrored_database_tables()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_mirrored_database_tables.md).

#### Returns

A table inventory tibble.

------------------------------------------------------------------------

### `FabricMirroredDatabase$read_table()`

Read one mirrored Delta table.

#### Usage

    FabricMirroredDatabase$read_table(table, ...)

#### Arguments

- `table`:

  Table name or discovered table row.

- `...`:

  Arguments forwarded to
  [`fabric_mirrored_database_read_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_mirrored_database_tables.md).

#### Returns

A tibble or Arrow stream.

------------------------------------------------------------------------

### `FabricMirroredDatabase$read_delta_table()`

Read a Delta table directly from OneLake.

#### Usage

    FabricMirroredDatabase$read_delta_table(table_path, ...)

#### Arguments

- `table_path`:

  Table path below the item.

- `...`:

  Arguments forwarded to
  [`fabric_onelake_read_delta_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_read_delta_table.md).

#### Returns

A tibble or Arrow stream.

## Super classes

`FabricRecord` -\> `FabricItem` -\> `FabricKqlItem` -\>
`FabricEventhouse`

## Super classes

`FabricRecord` -\> `FabricItem` -\> `FabricKqlItem` -\>
`FabricKqlDatabase`

## Super classes

`FabricRecord` -\> `FabricItem` -\> `FabricGraphQLApi`

## Methods

### Public methods

- [`FabricGraphQLApi$query()`](#method-FabricGraphQLApi-query)

- [`FabricGraphQLApi$schema()`](#method-FabricGraphQLApi-schema)

- [`FabricGraphQLApi$paginate()`](#method-FabricGraphQLApi-paginate)

Inherited methods

- `FabricRecord$as_list()`
- `FabricRecord$field_names()`
- `FabricRecord$get()`
- `FabricRecord$print()`
- [`FabricItem$details()`](https://kennispunttwente.github.io/fabricQueryR/reference/FabricItem.html#method-details)
- [`FabricItem$initialize()`](https://kennispunttwente.github.io/fabricQueryR/reference/FabricItem.html#method-initialize)

------------------------------------------------------------------------

### `FabricGraphQLApi$query()`

Run a GraphQL query.

#### Usage

    FabricGraphQLApi$query(query, ...)

#### Arguments

- `query`:

  One GraphQL query.

- `...`:

  Arguments forwarded to
  [`fabric_graphql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_graphql_query.md).

#### Returns

A `fabric_graphql_result`.

------------------------------------------------------------------------

### `FabricGraphQLApi$schema()`

Retrieve the GraphQL schema.

#### Usage

    FabricGraphQLApi$schema(...)

#### Arguments

- `...`:

  Arguments forwarded to
  [`fabric_graphql_schema()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_graphql_schema.md).

#### Returns

A `fabric_graphql_schema`.

------------------------------------------------------------------------

### `FabricGraphQLApi$paginate()`

Retrieve all pages of a GraphQL cursor query.

#### Usage

    FabricGraphQLApi$paginate(query, next_cursor, ...)

#### Arguments

- `query`:

  One GraphQL query.

- `next_cursor`:

  Function extracting the next cursor.

- `...`:

  Arguments forwarded to
  [`fabric_graphql_paginate()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_graphql_paginate.md).

#### Returns

A `fabric_graphql_pages` list.

## Super classes

`FabricRecord` -\> `FabricItem` -\> `FabricSemanticModel`

## Methods

### Public methods

- [`FabricSemanticModel$dax_query()`](#method-FabricSemanticModel-dax_query)

- [`FabricSemanticModel$refresh()`](#method-FabricSemanticModel-refresh)

- [`FabricSemanticModel$refresh_history()`](#method-FabricSemanticModel-refresh_history)

- [`FabricSemanticModel$refresh_status()`](#method-FabricSemanticModel-refresh_status)

- [`FabricSemanticModel$refresh_wait()`](#method-FabricSemanticModel-refresh_wait)

- [`FabricSemanticModel$refresh_cancel()`](#method-FabricSemanticModel-refresh_cancel)

Inherited methods

- `FabricRecord$as_list()`
- `FabricRecord$field_names()`
- `FabricRecord$get()`
- `FabricRecord$print()`
- [`FabricItem$details()`](https://kennispunttwente.github.io/fabricQueryR/reference/FabricItem.html#method-details)
- [`FabricItem$initialize()`](https://kennispunttwente.github.io/fabricQueryR/reference/FabricItem.html#method-initialize)

------------------------------------------------------------------------

### `FabricSemanticModel$dax_query()`

Run a DAX query against this semantic model.

#### Usage

    FabricSemanticModel$dax_query(dax, ...)

#### Arguments

- `dax`:

  One DAX query.

- `...`:

  Arguments forwarded to
  [`fabric_pbi_dax_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_pbi_dax_query.md).

#### Returns

A tibble or Arrow stream.

------------------------------------------------------------------------

### `FabricSemanticModel$refresh()`

Start a refresh of this semantic model.

#### Usage

    FabricSemanticModel$refresh(...)

#### Arguments

- `...`:

  Arguments forwarded to
  [`fabric_pbi_refresh()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_pbi_refresh.md).

#### Returns

A `fabric_pbi_refresh` handle.

------------------------------------------------------------------------

### `FabricSemanticModel$refresh_history()`

Retrieve this semantic model's refresh history.

#### Usage

    FabricSemanticModel$refresh_history(...)

#### Arguments

- `...`:

  Arguments forwarded to
  [`fabric_pbi_refresh_history()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_pbi_refresh.md).

#### Returns

A `fabric_pbi_refresh_history` list.

------------------------------------------------------------------------

### `FabricSemanticModel$refresh_status()`

Retrieve one refresh status snapshot.

#### Usage

    FabricSemanticModel$refresh_status(refresh, ...)

#### Arguments

- `refresh`:

  Refresh handle, detail record, or request ID.

- `...`:

  Arguments forwarded to
  [`fabric_pbi_refresh_status()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_pbi_refresh.md).

#### Returns

A `fabric_pbi_refresh_detail` record.

------------------------------------------------------------------------

### `FabricSemanticModel$refresh_wait()`

Wait for a submitted refresh to finish.

#### Usage

    FabricSemanticModel$refresh_wait(refresh, ...)

#### Arguments

- `refresh`:

  Refresh handle or detail record.

- `...`:

  Arguments forwarded to
  [`fabric_pbi_refresh_wait()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_pbi_refresh.md).

#### Returns

A terminal `fabric_pbi_refresh_detail` record.

------------------------------------------------------------------------

### `FabricSemanticModel$refresh_cancel()`

Cancel a submitted refresh.

#### Usage

    FabricSemanticModel$refresh_cancel(refresh, ...)

#### Arguments

- `refresh`:

  Refresh handle, detail record, or request ID.

- `...`:

  Arguments forwarded to
  [`fabric_pbi_refresh_cancel()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_pbi_refresh.md).

#### Returns

`TRUE`, invisibly, when cancellation is accepted.

## Super classes

`FabricRecord` -\> `FabricItem` -\> `FabricJobItem`

## Methods

### Public methods

- [`FabricJobItem$run()`](#method-FabricJobItem-run)

- [`FabricJobItem$status()`](#method-FabricJobItem-status)

- [`FabricJobItem$wait()`](#method-FabricJobItem-wait)

- [`FabricJobItem$cancel()`](#method-FabricJobItem-cancel)

- [`FabricJobItem$instances()`](#method-FabricJobItem-instances)

- [`FabricJobItem$schedules()`](#method-FabricJobItem-schedules)

- [`FabricJobItem$schedule_create()`](#method-FabricJobItem-schedule_create)

- [`FabricJobItem$schedule_update()`](#method-FabricJobItem-schedule_update)

- [`FabricJobItem$schedule_delete()`](#method-FabricJobItem-schedule_delete)

Inherited methods

- `FabricRecord$as_list()`
- `FabricRecord$field_names()`
- `FabricRecord$get()`
- `FabricRecord$print()`
- [`FabricItem$details()`](https://kennispunttwente.github.io/fabricQueryR/reference/FabricItem.html#method-details)
- [`FabricItem$initialize()`](https://kennispunttwente.github.io/fabricQueryR/reference/FabricItem.html#method-initialize)

------------------------------------------------------------------------

### `FabricJobItem$run()`

Start an on-demand item job.

#### Usage

    FabricJobItem$run(...)

#### Arguments

- `...`:

  Arguments forwarded to
  [`fabric_job_run()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_run.md).

#### Returns

A `fabric_job` handle.

------------------------------------------------------------------------

### `FabricJobItem$status()`

Retrieve one job status snapshot.

#### Usage

    FabricJobItem$status(job = NULL, ...)

#### Arguments

- `job`:

  Job handle, instance record, or job instance ID.

- `...`:

  Arguments forwarded to
  [`fabric_job_status()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_run.md).

#### Returns

A `fabric_job_instance` record.

------------------------------------------------------------------------

### `FabricJobItem$wait()`

Wait for a submitted job to finish.

#### Usage

    FabricJobItem$wait(job, ...)

#### Arguments

- `job`:

  Job handle or instance record.

- `...`:

  Arguments forwarded to
  [`fabric_job_wait()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_run.md).

#### Returns

A terminal `fabric_job_instance` record.

------------------------------------------------------------------------

### `FabricJobItem$cancel()`

Cancel a submitted job.

#### Usage

    FabricJobItem$cancel(job = NULL, ...)

#### Arguments

- `job`:

  Job handle, instance record, or job instance ID.

- `...`:

  Arguments forwarded to
  [`fabric_job_cancel()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_run.md).

#### Returns

`TRUE`, invisibly, when cancellation is accepted.

------------------------------------------------------------------------

### `FabricJobItem$instances()`

Retrieve this item's job history.

#### Usage

    FabricJobItem$instances(...)

#### Arguments

- `...`:

  Arguments forwarded to
  [`fabric_job_instances()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_instances.md).

#### Returns

A `fabric_job_instance_list`.

------------------------------------------------------------------------

### `FabricJobItem$schedules()`

List this item's schedules.

#### Usage

    FabricJobItem$schedules(...)

#### Arguments

- `...`:

  Arguments forwarded to
  [`fabric_job_schedules()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_schedules.md).

#### Returns

A `fabric_job_schedule_list`.

------------------------------------------------------------------------

### `FabricJobItem$schedule_create()`

Create an item schedule.

#### Usage

    FabricJobItem$schedule_create(configuration, ...)

#### Arguments

- `configuration`:

  A
  [`fabric_job_schedule_config()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_schedule_config.md)
  record.

- `...`:

  Arguments forwarded to
  [`fabric_job_schedule_create()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_schedules.md).

#### Returns

A `fabric_job_schedule` record.

------------------------------------------------------------------------

### `FabricJobItem$schedule_update()`

Update an item schedule.

#### Usage

    FabricJobItem$schedule_update(schedule_id, configuration = NULL, ...)

#### Arguments

- `schedule_id`:

  Schedule GUID or schedule record.

- `configuration`:

  Optional replacement schedule configuration.

- `...`:

  Arguments forwarded to
  [`fabric_job_schedule_update()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_schedules.md).

#### Returns

A `fabric_job_schedule` record.

------------------------------------------------------------------------

### `FabricJobItem$schedule_delete()`

Delete an item schedule.

#### Usage

    FabricJobItem$schedule_delete(schedule_id, ...)

#### Arguments

- `schedule_id`:

  Schedule GUID or schedule record.

- `...`:

  Arguments forwarded to
  [`fabric_job_schedule_delete()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_schedules.md).

#### Returns

`TRUE`, invisibly, after deletion.

## Examples

``` r
if (FALSE) { # \dontrun{
workspace <- fabric_workspaces()[[1L]]
workspace$displayName

# Equivalent function: fabric_lakehouses(workspace)
lakehouse <- workspace$lakehouses()[[1L]]
lakehouse$id
# Equivalent function: fabric_lakehouse_tables(lakehouse)
lakehouse$tables()
# Equivalent function: fabric_lakehouse_read_table(lakehouse, ...)
orders <- lakehouse$read_table("orders", limit = 100L)

# Workload-specific subclasses expose their own lifecycle methods
# Equivalent function: fabric_semantic_models(workspace)
model <- workspace$semantic_models()[[1L]]
# Equivalent function: fabric_pbi_refresh(model)
refresh <- model$refresh()
# Equivalent function: fabric_pbi_refresh_wait(refresh, ...)
model$refresh_wait(refresh, timeout = 1800)

# Equivalent generic: as.list(lakehouse)
lakehouse_record <- lakehouse$as_list()

# Fabric item types outside the typed-helper subset remain usable records
report <- workspace$items(type = "Report")[[1L]]
report$type
} # }
```
