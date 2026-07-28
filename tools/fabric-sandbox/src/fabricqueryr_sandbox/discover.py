"""Discover deployed endpoints and write the R test manifest."""

from __future__ import annotations

import time
from typing import Any
from urllib.parse import quote

from .credentials import get_credential
from .fabric_api import FabricApi
from .graphql_api import (
    GRAPHQL_API_NAME,
    GRAPHQL_CREATE_FIELD,
    GRAPHQL_ROOT_FIELD,
)
from .manifest import SandboxManifest
from .power_bi_api import (
    ARROW_SEMANTIC_MODEL_NAME,
    PowerBiApi,
    SEMANTIC_MODEL_NAME,
)
from .settings import SandboxSettings
from .sql_api import SQL_FIXTURE_TABLE


def _wait_for_lakehouse_sql_endpoint(
    api: FabricApi,
    workspace_id: str,
    lakehouse_id: str,
    *,
    timeout: int = 600,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        lakehouse = api.get_lakehouse(workspace_id, lakehouse_id)
        endpoint = lakehouse.get("properties", {}).get("sqlEndpointProperties", {})
        if endpoint.get("provisioningStatus") == "Success":
            return lakehouse
        api.sleep(10)
    raise TimeoutError("lakehouse SQL analytics endpoint was not ready in time")


def _wait_for_sql_properties(
    api: FabricApi,
    workspace_id: str,
    item_id: str,
    *,
    item_type: str,
    timeout: int = 900,
) -> dict[str, Any]:
    getters = {
        "Warehouse": api.get_warehouse,
        "SQLDatabase": api.get_sql_database,
    }
    required_properties = {
        "Warehouse": ("connectionString",),
        "SQLDatabase": ("connectionString", "serverFqdn", "databaseName"),
    }
    getter = getters[item_type]
    required = required_properties[item_type]
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        item = getter(workspace_id, item_id)
        properties = item.get("properties", {})
        if all(properties.get(name) for name in required):
            return item
        api.sleep(10)
    raise TimeoutError(
        f"{item_type} SQL connection properties were not ready in time"
    )


def _wait_for_kql_properties(
    api: FabricApi,
    workspace_id: str,
    item_id: str,
    *,
    item_type: str,
    timeout: int = 900,
) -> dict[str, Any]:
    getters = {
        "Eventhouse": api.get_eventhouse,
        "KQLDatabase": api.get_kql_database,
    }
    getter = getters[item_type]
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        item = getter(workspace_id, item_id)
        properties = item.get("properties", {})
        if properties.get("queryServiceUri"):
            return item
        api.sleep(10)
    raise TimeoutError(f"{item_type} Kusto query service URI was not ready in time")


def discover(settings: SandboxSettings) -> SandboxManifest:
    workspace_id = settings.require_workspace()
    with FabricApi(get_credential()) as api:
        lakehouse_item = api.find_item(workspace_id, "TestLakehouse", "Lakehouse")
        notebook_item = api.find_item(workspace_id, "SeedFixtures", "Notebook")
        job_notebook_item = api.find_item(
            workspace_id, "JobFixtures", "Notebook"
        )
        pipeline_item = api.find_item(
            workspace_id, "TestPipeline", "DataPipeline"
        )
        spark_job_item = api.find_item(
            workspace_id, "TestSparkJob", "SparkJobDefinition"
        )
        warehouse_item = api.find_item(
            workspace_id, "TestWarehouse", "Warehouse"
        )
        sql_database_item = api.find_item(
            workspace_id, "TestSQLDatabase", "SQLDatabase"
        )
        eventhouse_item = api.find_item(
            workspace_id, "TestEventhouse", "Eventhouse"
        )
        kql_database_item = api.find_item(
            workspace_id, "TestKQLDatabase", "KQLDatabase"
        )
        graphql_api_item = api.find_item(
            workspace_id, GRAPHQL_API_NAME, "GraphQLApi"
        )
        lakehouse = _wait_for_lakehouse_sql_endpoint(
            api, workspace_id, lakehouse_item["id"]
        )
        warehouse = _wait_for_sql_properties(
            api,
            workspace_id,
            warehouse_item["id"],
            item_type="Warehouse",
        )
        sql_database = _wait_for_sql_properties(
            api,
            workspace_id,
            sql_database_item["id"],
            item_type="SQLDatabase",
        )
        eventhouse = _wait_for_kql_properties(
            api,
            workspace_id,
            eventhouse_item["id"],
            item_type="Eventhouse",
        )
        kql_database = _wait_for_kql_properties(
            api,
            workspace_id,
            kql_database_item["id"],
            item_type="KQLDatabase",
        )
        sql_endpoint_id = lakehouse["properties"]["sqlEndpointProperties"]["id"]
        api.refresh_sql_endpoint_metadata(workspace_id, sql_endpoint_id)

    properties = lakehouse["properties"]
    sql_endpoint = properties["sqlEndpointProperties"]
    warehouse_properties = warehouse["properties"]
    sql_database_properties = sql_database["properties"]
    eventhouse_properties = eventhouse["properties"]
    kql_database_properties = kql_database["properties"]
    with PowerBiApi(get_credential()) as power_bi:
        semantic_model = power_bi.find_dataset(
            workspace_id,
            SEMANTIC_MODEL_NAME,
        )
        arrow_semantic_model = power_bi.find_dataset(
            workspace_id,
            ARROW_SEMANTIC_MODEL_NAME,
        )
    manifest = SandboxManifest(
        workspace_id=workspace_id,
        workspace_name=settings.workspace_name,
        items={
            "TestLakehouse": {
                "id": lakehouse_item["id"],
                "type": "Lakehouse",
                "display_name": lakehouse_item["displayName"],
                "schema": "dbo",
                "one_lake_files_path": properties.get("oneLakeFilesPath"),
                "one_lake_tables_path": properties.get("oneLakeTablesPath"),
                "sql_endpoint": sql_endpoint.get("connectionString"),
                "sql_endpoint_id": sql_endpoint.get("id"),
                "livy_url": (
                    f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}"
                    f"/lakehouses/{lakehouse_item['id']}"
                    "/livyapi/versions/2023-12-01/sessions"
                ),
                "livy_batch_file": (
                    f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/"
                    f"{lakehouse_item['id']}/Files/fixtures/livy_batch.py"
                ),
                "tables": {
                    "basic": "fabricqueryr_basic",
                    "empty": "fabricqueryr_empty",
                    "void": "fabricqueryr_void",
                    "partitioned": "fabricqueryr_partitioned",
                    "typed_partitions": "fabricqueryr_typed_partitions",
                    "schema_evolved": "fabricqueryr_schema_evolved",
                    "column_mapped": "fabricqueryr_column_mapped",
                    "column_mapped_id": "fabricqueryr_column_mapped_id",
                    "deletion_vectors": "fabricqueryr_deletion_vectors",
                    "deletion_vectors_stress": (
                        "fabricqueryr_deletion_vectors_stress"
                    ),
                    "deletion_vectors_checkpoint": (
                        "fabricqueryr_deletion_vectors_checkpoint"
                    ),
                    "deletion_vectors_dense": (
                        "fabricqueryr_deletion_vectors_dense"
                    ),
                    "exact_types": "fabricqueryr_exact_types",
                    "complex_types": "fabricqueryr_complex_types",
                    "shallow_clone": "fabricqueryr_shallow_clone",
                    "type_widened": "fabricqueryr_type_widened",
                    "type_widened_exact": "fabricqueryr_type_widened_exact",
                    "type_widened_nested": (
                        "fabricqueryr_type_widened_nested"
                    ),
                    "v2_checkpoint": "fabricqueryr_v2_checkpoint",
                    "variant": "fabricqueryr_variant",
                    "livy_batch_result": "fabricqueryr_livy_batch_result",
                    "spark_job_result": "fabricqueryr_spark_job_result",
                },
            },
            "SeedFixtures": {
                "id": notebook_item["id"],
                "type": "Notebook",
                "display_name": notebook_item["displayName"],
            },
            "JobFixtures": {
                "id": job_notebook_item["id"],
                "type": "Notebook",
                "display_name": job_notebook_item["displayName"],
            },
            "TestPipeline": {
                "id": pipeline_item["id"],
                "type": "DataPipeline",
                "display_name": pipeline_item["displayName"],
            },
            "TestSparkJob": {
                "id": spark_job_item["id"],
                "type": "SparkJobDefinition",
                "display_name": spark_job_item["displayName"],
            },
            "TestWarehouse": {
                "id": warehouse_item["id"],
                "type": "Warehouse",
                "display_name": warehouse_item["displayName"],
                "connection_string": warehouse_properties["connectionString"],
                "database_name": warehouse_item["displayName"],
                "tables": {
                    "types": SQL_FIXTURE_TABLE,
                },
            },
            "TestSQLDatabase": {
                "id": sql_database_item["id"],
                "type": "SQLDatabase",
                "display_name": sql_database_item["displayName"],
                "connection_string": sql_database_properties["connectionString"],
                "server_fqdn": sql_database_properties["serverFqdn"],
                "database_name": sql_database_properties["databaseName"],
                "tables": {
                    "types": SQL_FIXTURE_TABLE,
                },
            },
            "TestEventhouse": {
                "id": eventhouse_item["id"],
                "type": "Eventhouse",
                "display_name": eventhouse_item["displayName"],
                "query_service_uri": eventhouse_properties["queryServiceUri"],
                "ingestion_service_uri": eventhouse_properties.get(
                    "ingestionServiceUri"
                ),
            },
            "TestKQLDatabase": {
                "id": kql_database_item["id"],
                "type": "KQLDatabase",
                "display_name": kql_database_item["displayName"],
                "database_name": kql_database_item["displayName"],
                "parent_eventhouse_id": kql_database_properties.get(
                    "parentEventhouseItemId"
                ),
                "query_service_uri": kql_database_properties["queryServiceUri"],
                "ingestion_service_uri": kql_database_properties.get(
                    "ingestionServiceUri"
                ),
                "tables": {
                    "events": "fabricqueryr_events",
                },
            },
            "TestSemanticModel": {
                "id": semantic_model["id"],
                "type": "SemanticModel",
                "display_name": SEMANTIC_MODEL_NAME,
                "connection_string": (
                    "Data Source=powerbi://api.powerbi.com/v1.0/myorg/"
                    f"{quote(settings.workspace_name, safe='')};"
                    f"Initial Catalog={SEMANTIC_MODEL_NAME};"
                ),
            },
            "TestArrowSemanticModel": {
                "id": arrow_semantic_model["id"],
                "type": "SemanticModel",
                "display_name": ARROW_SEMANTIC_MODEL_NAME,
                "connection_string": (
                    "Data Source=powerbi://api.powerbi.com/v1.0/myorg/"
                    f"{quote(settings.workspace_name, safe='')};"
                    f"Initial Catalog={ARROW_SEMANTIC_MODEL_NAME};"
                ),
            },
            "TestGraphQL": {
                "id": graphql_api_item["id"],
                "type": "GraphQLApi",
                "display_name": graphql_api_item["displayName"],
                "endpoint": (
                    f"https://api.fabric.microsoft.com/v1/workspaces/"
                    f"{workspace_id}/graphqlapis/{graphql_api_item['id']}/graphql"
                ),
                "root_field": GRAPHQL_ROOT_FIELD,
                "create_field": GRAPHQL_CREATE_FIELD,
            },
        },
    )
    manifest.write(settings.manifest_path)
    return manifest
