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
  [`fabric_sql_databases()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md)
  [`fabric_semantic_models()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md)
  [`fabric_eventhouses()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md)
  [`fabric_kql_databases()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md)
  [`fabric_notebooks()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md)
  [`fabric_graphql_apis()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_typed_items.md)
  : Typed Microsoft Fabric item discovery

## SQL connectivity

Connect to and query Lakehouse, Warehouse, and SQL Database endpoints

- [`fabric_sql_connection_info()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_connection_info.md)
  : Parse a Microsoft Fabric SQL target
- [`fabric_sql_connect()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_connect.md)
  : Connect to a Microsoft Fabric SQL target
- [`fabric_sql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_sql_query.md)
  : Run a parameterized query against Microsoft Fabric SQL

## Semantic models and DAX

Run DAX queries against Fabric and Power BI semantic models

- [`fabric_pbi_dax_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_pbi_dax_query.md)
  : Query a Microsoft Fabric/Power BI semantic model with DAX

## OneLake files

List, inspect, download, upload, and delete files in OneLake

- [`fabric_onelake_list()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_files.md)
  [`fabric_onelake_metadata()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_files.md)
  [`fabric_onelake_download()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_files.md)
  [`fabric_onelake_upload()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_files.md)
  [`fabric_onelake_delete()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_files.md)
  : Work with files in Microsoft Fabric OneLake

## Delta tables

Resolve and read supported Delta Lake snapshots from OneLake

- [`fabric_onelake_read_delta_table()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_read_delta_table.md)
  : Read a Delta table from Microsoft Fabric OneLake
- [`fabric_delta_config()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_delta_config.md)
  : Inspect the optional Python Delta runtime
- [`format(`*`<fabric_delta_timestamp_ntz>`*`)`](https://kennispunttwente.github.io/fabricQueryR/reference/format.fabric_delta_timestamp_ntz.md)
  : Format a Delta timestamp without time zone as wall-clock text
- [`as.POSIXct(`*`<fabric_delta_timestamp_ntz>`*`)`](https://kennispunttwente.github.io/fabricQueryR/reference/as.POSIXct.fabric_delta_timestamp_ntz.md)
  : Localize a Delta timestamp without time zone

## KQL

Query Eventhouse and KQL Database data

- [`fabric_kql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_kql_query.md)
  : Query a Microsoft Fabric Eventhouse with KQL

## GraphQL

Execute GraphQL operations and traverse cursor-based pages

- [`fabric_graphql_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_graphql_query.md)
  : Query a Microsoft Fabric API for GraphQL
- [`fabric_graphql_paginate()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_graphql_paginate.md)
  : Paginate a Microsoft Fabric GraphQL operation
- [`fabric_graphql_cursor()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_graphql_cursor.md)
  : Build a Fabric GraphQL cursor extractor

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

Run, monitor, wait for, and cancel Fabric item jobs

- [`fabric_job_run()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_run.md)
  [`fabric_job_status()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_run.md)
  [`fabric_job_wait()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_run.md)
  [`fabric_job_cancel()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_job_run.md)
  : Run and monitor Microsoft Fabric item jobs

## Package

Package overview

- [`fabricQueryR`](https://kennispunttwente.github.io/fabricQueryR/reference/fabricQueryR-package.md)
  [`fabricQueryR-package`](https://kennispunttwente.github.io/fabricQueryR/reference/fabricQueryR-package.md)
  : Work with Microsoft Fabric from R
