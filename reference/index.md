# Package index

## Discovery

Find Fabric workspaces, items, and workload-specific connection details

- [`fabric_workspaces()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_workspaces.md)
  : Discover Microsoft Fabric workspaces
- [`fabric_items()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_items.md)
  : Discover Microsoft Fabric items
- [`fabric_item()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_item.md)
  : Discover one Microsoft Fabric item
- [`fabric_lakehouses()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md)
  [`fabric_warehouses()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md)
  [`fabric_warehouse_snapshots()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md)
  [`fabric_sql_databases()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md)
  [`fabric_semantic_models()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md)
  [`fabric_eventhouses()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md)
  [`fabric_kql_databases()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md)
  [`fabric_notebooks()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md)
  [`fabric_data_pipelines()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md)
  [`fabric_spark_job_definitions()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md)
  [`fabric_environments()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md)
  [`fabric_user_data_functions()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md)
  [`fabric_graphql_apis()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md)
  : Typed Microsoft Fabric item discovery

## SQL connectivity

Connect to and query Lakehouse, Warehouse, and SQL Database endpoints

- [`fabric_sql_connection_info()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_connection_info.md)
  : Get connection details for a Fabric SQL item
- [`fabric_sql_connect()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_connect.md)
  : Connect to a Microsoft Fabric SQL target
- [`fabric_sql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_query.md)
  : Run a parameterized query against Microsoft Fabric SQL

## Semantic models and DAX

Query and refresh Fabric and Power BI semantic models

- [`fabric_pbi_dax_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_pbi_dax_query.md)
  : Query a Microsoft Fabric/Power BI semantic model with DAX
- [`fabric_pbi_refresh()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_pbi_refresh.md)
  [`fabric_pbi_refresh_history()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_pbi_refresh.md)
  [`fabric_pbi_refresh_status()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_pbi_refresh.md)
  [`fabric_pbi_refresh_wait()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_pbi_refresh.md)
  [`fabric_pbi_refresh_cancel()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_pbi_refresh.md)
  : Refresh and monitor a Power BI semantic model
- [`print(`*`<fabric_pbi_refresh>`*`)`](https://kennispunttwente.github.io/fabricQueryR/reference/print.fabric_pbi_refresh.md)
  : Print a submitted Power BI refresh
- [`print(`*`<fabric_pbi_refresh_detail>`*`)`](https://kennispunttwente.github.io/fabricQueryR/reference/print.fabric_pbi_refresh_detail.md)
  : Print Power BI refresh details

## OneLake files

List, inspect, download, upload, and delete files in OneLake

- [`fabric_onelake_list()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_files.md)
  [`fabric_onelake_metadata()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_files.md)
  [`fabric_onelake_download()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_files.md)
  [`fabric_onelake_upload()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_files.md)
  [`fabric_onelake_delete()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_files.md)
  : Work with files in Microsoft Fabric OneLake

## Delta tables

Discover, load, and read Delta Lake tables in OneLake

- [`fabric_lakehouse_tables()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_lakehouse_tables.md)
  [`fabric_lakehouse_load_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_lakehouse_tables.md)
  [`fabric_lakehouse_write_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_lakehouse_tables.md)
  : Discover and load Microsoft Fabric Lakehouse tables
- [`fabric_lakehouse_read_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_lakehouse_read_table.md)
  : Read a Microsoft Fabric Lakehouse table
- [`fabric_onelake_read_delta_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_read_delta_table.md)
  : Read a Delta table from OneLake
- [`fabric_delta_config()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_delta_config.md)
  : Inspect the optional Python Delta runtime

## KQL

Query Eventhouse data and ingest storage, R, or Arrow sources

- [`fabric_kql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_kql_query.md)
  : Run a KQL query in Microsoft Fabric
- [`fabric_kql_ingest()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_kql_ingest.md)
  [`fabric_kql_ingestion_status()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_kql_ingest.md)
  : Submit and monitor tracked Eventhouse ingestion
- [`fabric_kql_write_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_kql_write_table.md)
  : Write an R or Arrow object to an Eventhouse table
- [`print(`*`<fabric_kql_ingestion>`*`)`](https://kennispunttwente.github.io/fabricQueryR/reference/print.fabric_kql_ingestion.md)
  : Print a tracked Kusto ingestion handle
- [`print(`*`<fabric_kql_ingestion_status>`*`)`](https://kennispunttwente.github.io/fabricQueryR/reference/print.fabric_kql_ingestion_status.md)
  : Print tracked Kusto ingestion status
- [`print(`*`<fabric_kql_write_result>`*`)`](https://kennispunttwente.github.io/fabricQueryR/reference/print.fabric_kql_write_result.md)
  : Print an Eventhouse R/Arrow write result

## GraphQL

Explore schemas, execute operations, and collect cursor-based pages

- [`fabric_graphql_schema()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_graphql_schema.md)
  : Inspect a Fabric GraphQL schema
- [`fabric_graphql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_graphql_query.md)
  : Run a query against a Fabric GraphQL API
- [`fabric_graphql_paginate()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_graphql_paginate.md)
  : Read all pages from a Fabric GraphQL query
- [`fabric_graphql_cursor()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_graphql_cursor.md)
  : Locate pagination information in a GraphQL result
- [`fabric_graphql_collect()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_graphql_collect.md)
  : Collect paged GraphQL row objects into a tibble
- [`print(`*`<fabric_graphql_rows>`*`)`](https://kennispunttwente.github.io/fabricQueryR/reference/print.fabric_graphql_rows.md)
  : Print collected GraphQL rows

## User Data Functions

Invoke published Fabric business logic through trusted public URLs

- [`fabric_function_invoke()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_function_invoke.md)
  : Invoke a published Fabric user data function

## Livy and Spark

Run Spark statements, reusable sessions, and batch jobs through Livy

- [`fabric_livy_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_livy_query.md)
  : Run Spark code in a temporary Microsoft Fabric Livy session
- [`fabric_livy_session()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_livy_session.md)
  : Create a Microsoft Fabric Livy session
- [`fabric_livy_batch_submit()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_livy_batch_submit.md)
  : Submit a Microsoft Fabric Livy batch job
- [`FabricLivySession`](https://kennispunttwente.github.io/fabricQueryR/reference/FabricLivySession.md)
  : A Microsoft Fabric Livy session
- [`FabricLivyStatement`](https://kennispunttwente.github.io/fabricQueryR/reference/FabricLivyStatement.md)
  : A statement submitted to a Fabric Livy session
- [`FabricLivyBatch`](https://kennispunttwente.github.io/fabricQueryR/reference/FabricLivyBatch.md)
  : A Microsoft Fabric Livy batch job

## Item jobs

Run, monitor, schedule, and inspect Fabric item jobs

- [`fabric_job_run()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_run.md)
  [`fabric_job_status()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_run.md)
  [`fabric_job_wait()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_run.md)
  [`fabric_job_cancel()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_run.md)
  : Run and monitor Microsoft Fabric item jobs
- [`fabric_job_instances()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_instances.md)
  : Inspect Microsoft Fabric job history
- [`fabric_job_schedule_config()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_schedule_config.md)
  : Build a Microsoft Fabric job schedule configuration
- [`fabric_job_schedules()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_schedules.md)
  [`fabric_job_schedule_create()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_schedules.md)
  [`fabric_job_schedule_update()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_schedules.md)
  [`fabric_job_schedule_delete()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_schedules.md)
  : Manage Microsoft Fabric item schedules
- [`print(`*`<fabric_job>`*`)`](https://kennispunttwente.github.io/fabricQueryR/reference/print.fabric_job.md)
  : Print a submitted Fabric job
- [`print(`*`<fabric_job_instance>`*`)`](https://kennispunttwente.github.io/fabricQueryR/reference/print.fabric_job_instance.md)
  : Print Fabric job status
- [`print(`*`<fabric_job_schedule>`*`)`](https://kennispunttwente.github.io/fabricQueryR/reference/print.fabric_job_schedule.md)
  : Print a Fabric job schedule

## Long-running operations

Resume, monitor, and retrieve Fabric Core asynchronous operations

- [`fabric_operation_status()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_operation_status.md)
  [`fabric_operation_wait()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_operation_status.md)
  [`fabric_operation_result()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_operation_status.md)
  : Monitor Microsoft Fabric long-running operations

## Package

Package overview

- [`fabricQueryR`](https://kennispunttwente.github.io/fabricQueryR/reference/fabricQueryR-package.md)
  [`fabricQueryR-package`](https://kennispunttwente.github.io/fabricQueryR/reference/fabricQueryR-package.md)
  : Work with Microsoft Fabric from R
