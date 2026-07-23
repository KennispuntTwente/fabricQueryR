"""Discover deployed endpoints and write the R test manifest."""

from __future__ import annotations

import time
from typing import Any

from .credentials import get_credential
from .fabric_api import FabricApi
from .manifest import SandboxManifest
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
        time.sleep(10)
    raise TimeoutError("lakehouse SQL analytics endpoint was not ready in time")


def discover(settings: SandboxSettings) -> SandboxManifest:
    workspace_id = settings.require_workspace()
    with FabricApi(get_credential()) as api:
        lakehouse_item = api.find_item(workspace_id, "TestLakehouse", "Lakehouse")
        notebook_item = api.find_item(workspace_id, "SeedFixtures", "Notebook")
        lakehouse = _wait_for_lakehouse_sql_endpoint(
            api, workspace_id, lakehouse_item["id"]
        )
        sql_endpoint_id = lakehouse["properties"]["sqlEndpointProperties"]["id"]
        api.refresh_sql_endpoint_metadata(workspace_id, sql_endpoint_id)

    properties = lakehouse["properties"]
    sql_endpoint = properties["sqlEndpointProperties"]
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
        },
    )
    manifest.write(settings.manifest_path)
    return manifest
