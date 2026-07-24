"""Discover deployed endpoints and write the R test manifest."""

from __future__ import annotations

import time
from typing import Any
from urllib.parse import quote

from .credentials import get_credential
from .fabric_api import FabricApi
from .manifest import SandboxManifest
from .power_bi_api import PowerBiApi, SEMANTIC_MODEL_NAME
from .settings import SandboxSettings


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
                "tables": {
                    "basic": "fabricqueryr_basic",
                    "partitioned": "fabricqueryr_partitioned",
                },
            },
            "SeedFixtures": {
                "id": notebook_item["id"],
                "type": "Notebook",
            },
            "TestWarehouse": {
                "id": warehouse_item["id"],
                "type": "Warehouse",
                "display_name": warehouse_item["displayName"],
                "connection_string": warehouse_properties["connectionString"],
                "database_name": warehouse_item["displayName"],
            },
            "TestSQLDatabase": {
                "id": sql_database_item["id"],
                "type": "SQLDatabase",
                "display_name": sql_database_item["displayName"],
                "connection_string": sql_database_properties["connectionString"],
                "server_fqdn": sql_database_properties["serverFqdn"],
                "database_name": sql_database_properties["databaseName"],
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
        },
    )
    manifest.write(settings.manifest_path)
    return manifest
