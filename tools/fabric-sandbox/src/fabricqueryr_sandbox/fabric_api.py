"""Small Fabric REST client for sandbox seeding and discovery."""

from __future__ import annotations

from collections.abc import Callable
import time
from typing import Any

from azure.core.credentials import TokenCredential
import httpx


FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
FABRIC_API = "https://api.fabric.microsoft.com/v1"
TERMINAL_JOB_STATES = {"Completed", "Failed", "Cancelled", "Deduped"}
TERMINAL_OPERATION_STATES = {"Succeeded", "Failed", "Cancelled"}
NOTEBOOK_ERROR_PREFIX = "fabricqueryr-seed-error:"
NOTEBOOK_SUCCESS_VALUE = "fabricqueryr-seed-success"


class FabricApi:
    def __init__(
        self,
        credential: TokenCredential,
        *,
        transport: httpx.BaseTransport | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.credential = credential
        self.client = httpx.Client(
            base_url=FABRIC_API,
            timeout=60,
            transport=transport,
        )
        self.sleep = sleep

    def close(self) -> None:
        self.client.close()

    def __enter__(self) -> "FabricApi":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        token = self.credential.get_token(FABRIC_SCOPE).token
        headers = {"Authorization": f"Bearer {token}"}
        headers.update(kwargs.pop("headers", {}))
        response = self.client.request(method, url, headers=headers, **kwargs)
        response.raise_for_status()
        return response

    def list_items(self, workspace_id: str) -> list[dict[str, Any]]:
        url: str | None = f"/workspaces/{workspace_id}/items"
        items: list[dict[str, Any]] = []
        while url:
            payload = self.request("GET", url).json()
            items.extend(payload.get("value", payload.get("data", [])))
            url = payload.get("continuationUri")
        return items

    def find_item(
        self, workspace_id: str, display_name: str, item_type: str
    ) -> dict[str, Any]:
        matches = [
            item
            for item in self.list_items(workspace_id)
            if item.get("displayName") == display_name
            and item.get("type") == item_type
        ]
        if len(matches) != 1:
            raise RuntimeError(
                f"expected one {item_type} named {display_name!r}, found {len(matches)}"
            )
        return matches[0]

    def get_lakehouse(self, workspace_id: str, lakehouse_id: str) -> dict[str, Any]:
        return self.request(
            "GET", f"/workspaces/{workspace_id}/lakehouses/{lakehouse_id}"
        ).json()

    def refresh_sql_endpoint_metadata(
        self,
        workspace_id: str,
        sql_endpoint_id: str,
        *,
        timeout: int = 900,
    ) -> dict[str, Any]:
        response = self.request(
            "POST",
            (
                f"/workspaces/{workspace_id}/sqlEndpoints/"
                f"{sql_endpoint_id}/refreshMetadata"
            ),
            json={"recreateTables": False},
        )
        if response.status_code == 200:
            return response.json()

        location = response.headers.get("Location")
        if not location:
            raise RuntimeError(
                "SQL endpoint metadata refresh did not include a Location header"
            )

        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            operation = self.request("GET", location).json()
            status = operation.get("status")
            if status in TERMINAL_OPERATION_STATES:
                if status != "Succeeded":
                    raise RuntimeError(
                        f"SQL endpoint metadata refresh ended in {status}: "
                        f"{operation.get('error')}"
                    )
                return operation
            self.sleep(10)
        raise TimeoutError("SQL endpoint metadata refresh did not finish in time")

    def run_notebook(
        self,
        workspace_id: str,
        notebook_id: str,
        *,
        lakehouse_id: str,
        timeout: int = 900,
    ) -> dict[str, Any]:
        response = self.request(
            "POST",
            (
                f"/workspaces/{workspace_id}/notebooks/{notebook_id}"
                "/jobs/execute/instances"
            ),
            params={"beta": "false"},
            json={
                "executionData": {
                    "compute": "Spark",
                    "computeConfiguration": {
                        "defaultLakehouse": {
                            "referenceType": "ById",
                            "itemId": lakehouse_id,
                            "workspaceId": workspace_id,
                        }
                    },
                }
            },
        )
        location = response.headers.get("Location")
        if not location:
            raise RuntimeError("notebook job response did not include a Location header")

        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            job = self.request("GET", location, params={"beta": "false"}).json()
            status = job.get("status")
            if status in TERMINAL_JOB_STATES:
                if status != "Completed":
                    raise RuntimeError(
                        f"notebook job {job.get('id')} ended in {status} "
                        f"(root activity {job.get('rootActivityId')}): "
                        f"{job.get('failureReason')}"
                    )
                exit_value = job.get("exitValue")
                if isinstance(exit_value, str) and exit_value.startswith(
                    NOTEBOOK_ERROR_PREFIX
                ):
                    raise RuntimeError(exit_value)
                if exit_value != NOTEBOOK_SUCCESS_VALUE:
                    raise RuntimeError(
                        "seed notebook completed without its success marker; "
                        f"exitValue={exit_value!r}"
                    )
                return job
            self.sleep(10)
        raise TimeoutError(f"notebook job did not finish within {timeout} seconds")
