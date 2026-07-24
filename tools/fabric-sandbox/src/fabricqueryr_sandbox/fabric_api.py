"""Small Fabric REST client for sandbox seeding and discovery."""

from __future__ import annotations

import base64
import json
import time
from collections.abc import Callable
from typing import Any
from urllib.parse import urlparse

from azure.core.credentials import TokenCredential
import httpx


FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
FABRIC_API = "https://api.fabric.microsoft.com/v1"
TERMINAL_JOB_STATES = {"Completed", "Failed", "Cancelled", "Deduped"}
TERMINAL_OPERATION_STATES = {"Succeeded", "Failed", "Cancelled"}
NOTEBOOK_ERROR_PREFIX = "fabricqueryr-seed-error:"
NOTEBOOK_SUCCESS_VALUE = "fabricqueryr-seed-success"
JOB_VISIBILITY_RETRIES = 12
JOB_VISIBILITY_RETRY_SECONDS = 5


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
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as error:
            request_id = (
                response.headers.get("x-ms-request-id")
                or response.headers.get("request-id")
            )
            body = response.text.strip() or "<empty body>"
            detail = (
                f"{error}\nFabric response: {body}"
                + (f"\nRequest ID: {request_id}" if request_id else "")
            )
            raise httpx.HTTPStatusError(
                detail,
                request=error.request,
                response=response,
            ) from error
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

    def get_warehouse(self, workspace_id: str, warehouse_id: str) -> dict[str, Any]:
        return self.request(
            "GET", f"/workspaces/{workspace_id}/warehouses/{warehouse_id}"
        ).json()

    def get_sql_database(
        self, workspace_id: str, sql_database_id: str
    ) -> dict[str, Any]:
        return self.request(
            "GET",
            f"/workspaces/{workspace_id}/sqlDatabases/{sql_database_id}",
        ).json()

    def get_eventhouse(
        self, workspace_id: str, eventhouse_id: str
    ) -> dict[str, Any]:
        return self.request(
            "GET",
            f"/workspaces/{workspace_id}/eventhouses/{eventhouse_id}",
        ).json()

    def get_kql_database(
        self, workspace_id: str, kql_database_id: str
    ) -> dict[str, Any]:
        return self.request(
            "GET",
            f"/workspaces/{workspace_id}/kqlDatabases/{kql_database_id}",
        ).json()

    def get_graphql_api(
        self, workspace_id: str, graphql_api_id: str
    ) -> dict[str, Any]:
        return self.request(
            "GET",
            f"/workspaces/{workspace_id}/graphQLApis/{graphql_api_id}",
        ).json()

    def update_graphql_definition(
        self,
        workspace_id: str,
        graphql_api_id: str,
        definition: dict[str, Any],
        *,
        timeout: int = 900,
    ) -> dict[str, Any]:
        payload = base64.b64encode(
            json.dumps(
                definition,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
        ).decode("ascii")
        response = self.request(
            "POST",
            (
                f"/workspaces/{workspace_id}/graphQLApis/"
                f"{graphql_api_id}/updateDefinition"
            ),
            json={
                "definition": {
                    "parts": [
                        {
                            "path": "graphql-definition.json",
                            "payload": payload,
                            "payloadType": "InlineBase64",
                        }
                    ],
                }
            },
        )
        if response.status_code == 200:
            return {"status": "Succeeded"}
        return self._wait_for_operation(
            response,
            operation_name="GraphQL definition update",
            timeout=timeout,
        )

    def wait_for_graphql_type(
        self,
        workspace_id: str,
        graphql_api_id: str,
        graphql_type: str,
        *,
        timeout: int = 600,
    ) -> dict[str, Any]:
        endpoint = (
            f"/workspaces/{workspace_id}/graphqlapis/"
            f"{graphql_api_id}/graphql"
        )
        deadline = time.monotonic() + timeout
        last_errors: Any = None
        while time.monotonic() < deadline:
            try:
                response = self.request(
                    "POST",
                    endpoint,
                    headers={"Accept": "application/graphql-response+json"},
                    json={
                        "query": (
                            "query SchemaReady($name: String!) { "
                            "__type(name: $name) { name fields { name } } }"
                        ),
                        "variables": {"name": graphql_type},
                        "operationName": "SchemaReady",
                    },
                ).json()
            except httpx.HTTPStatusError as error:
                if error.response.status_code not in {
                    404,
                    408,
                    409,
                    429,
                    500,
                    502,
                    503,
                    504,
                }:
                    raise
                last_errors = error.response.text
                self.sleep(10)
                continue
            found_type = response.get("data", {}).get("__type")
            if found_type and found_type.get("name") == graphql_type:
                return found_type
            last_errors = response.get("errors")
            self.sleep(10)
        raise TimeoutError(
            f"GraphQL type {graphql_type!r} was not ready in time; "
            f"last errors: {last_errors!r}"
        )

    def _wait_for_operation(
        self,
        response: httpx.Response,
        *,
        operation_name: str,
        timeout: int,
    ) -> dict[str, Any]:
        location = response.headers.get("Location")
        if not location:
            raise RuntimeError(
                f"{operation_name} did not include a Location header"
            )
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            operation = self.request("GET", location).json()
            status = operation.get("status")
            if status in TERMINAL_OPERATION_STATES:
                if status != "Succeeded":
                    raise RuntimeError(
                        f"{operation_name} ended in {status}: "
                        f"{operation.get('error')}"
                    )
                return operation
            self.sleep(10)
        raise TimeoutError(f"{operation_name} did not finish in time")

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

        return self._wait_for_operation(
            response,
            operation_name="SQL endpoint metadata refresh",
            timeout=timeout,
        )

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
        job_instance_id = urlparse(location).path.rstrip("/").rsplit("/", 1)[-1]
        job_url = (
            f"/workspaces/{workspace_id}/notebooks/{notebook_id}"
            f"/jobs/execute/instances/{job_instance_id}"
        )

        deadline = time.monotonic() + timeout
        not_found_retries = 0
        while time.monotonic() < deadline:
            try:
                job = self.request(
                    "GET", job_url, params={"beta": "true"}
                ).json()
            except httpx.HTTPStatusError as error:
                if (
                    error.response.status_code != 404
                    or not_found_retries >= JOB_VISIBILITY_RETRIES
                ):
                    raise
                not_found_retries += 1
                self.sleep(JOB_VISIBILITY_RETRY_SECONDS)
                continue
            status = job.get("status")
            if status in TERMINAL_JOB_STATES:
                if status != "Completed":
                    raise RuntimeError(
                        f"notebook job {job.get('id')} ended in {status} "
                        f"(root activity {job.get('rootActivityId')}): "
                        f"{job.get('failureReason')}"
                    )
                exit_value = job.get("exitValue")
                if exit_value is None:
                    exit_value = job.get("properties", {}).get("exitValue")
                if isinstance(exit_value, str) and exit_value.startswith(
                    NOTEBOOK_ERROR_PREFIX
                ):
                    raise RuntimeError(exit_value)
                if exit_value != NOTEBOOK_SUCCESS_VALUE:
                    raise RuntimeError(
                        "seed notebook completed without its success marker; "
                        f"exitValue={exit_value!r}"
                    )
                job["exitValue"] = exit_value
                return job
            self.sleep(10)
        raise TimeoutError(f"notebook job did not finish within {timeout} seconds")
