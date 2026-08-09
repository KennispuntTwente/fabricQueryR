"""Small Fabric REST client for sandbox seeding and discovery."""

from __future__ import annotations

import base64
import json
import re
import time
from collections.abc import Callable
from typing import Any
from urllib.parse import urlparse
from uuid import UUID

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
RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}
IDEMPOTENT_METHODS = {"GET", "HEAD", "OPTIONS", "PUT", "DELETE"}


class FabricApi:
    def __init__(
        self,
        credential: TokenCredential,
        *,
        transport: httpx.BaseTransport | None = None,
        sleep: Callable[[float], None] = time.sleep,
        max_attempts: int = 4,
    ) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        self.credential = credential
        self.client = httpx.Client(
            base_url=FABRIC_API,
            timeout=60,
            transport=transport,
        )
        self.sleep = sleep
        self.max_attempts = max_attempts

    def close(self) -> None:
        self.client.close()

    def __enter__(self) -> "FabricApi":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        target = self._same_origin_url(url)
        supplied_headers = kwargs.pop("headers", {})
        method = method.upper()
        for attempt in range(1, self.max_attempts + 1):
            token = self.credential.get_token(FABRIC_SCOPE).token
            headers = {"Authorization": f"Bearer {token}"}
            headers.update(supplied_headers)
            response = self.client.request(
                method,
                target,
                headers=headers,
                **kwargs,
            )
            retryable = response.status_code in RETRYABLE_STATUS_CODES and (
                method in IDEMPOTENT_METHODS or response.status_code == 429
            )
            if not retryable or attempt == self.max_attempts:
                break
            default = min(30.0, 0.5 * (2 ** (attempt - 1)))
            delay = self._retry_after(response, default)
            if delay > 0:
                self.sleep(delay)
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

    def _same_origin_url(self, url: str) -> httpx.URL:
        target = self.client.build_request("GET", url).url
        base = self.client.base_url
        target_origin = (target.scheme, target.host, target.port)
        base_origin = (base.scheme, base.host, base.port)
        if (
            target_origin != base_origin
            or bool(target.username)
            or bool(target.password)
        ):
            raise ValueError(
                "Fabric API URLs must remain on the configured HTTPS origin"
            )
        return target

    @staticmethod
    def _retry_after(response: httpx.Response, default: float) -> float:
        value = response.headers.get("Retry-After")
        if value is None:
            return default
        try:
            return max(0.0, float(value))
        except ValueError:
            return default

    def _sleep_for_poll(
        self,
        response: httpx.Response,
        deadline: float,
        *,
        default: float,
    ) -> None:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        delay = min(self._retry_after(response, default), remaining)
        if delay > 0:
            self.sleep(delay)

    def list_items(self, workspace_id: str) -> list[dict[str, Any]]:
        url: str | None = f"/workspaces/{workspace_id}/items"
        items: list[dict[str, Any]] = []
        while url:
            payload = self.request("GET", url).json()
            items.extend(payload.get("value", payload.get("data", [])))
            url = payload.get("continuationUri")
        return items

    def list_workspaces(self, *, roles: str = "Admin") -> list[dict[str, Any]]:
        url: str | None = "/workspaces"
        params: dict[str, str] | None = {"roles": roles}
        workspaces: list[dict[str, Any]] = []
        while url:
            payload = self.request("GET", url, params=params).json()
            workspaces.extend(payload.get("value", []))
            url = payload.get("continuationUri")
            params = None
        return workspaces

    def delete_workspace(self, workspace_id: str) -> None:
        self.request("DELETE", f"/workspaces/{workspace_id}")

    def configure_workspace_spark_runtime(
        self,
        workspace_id: str,
        runtime_version: str,
    ) -> dict[str, Any]:
        """Set the sandbox runtime without replacing a named environment."""
        url = f"/workspaces/{workspace_id}/spark/settings"
        settings = self.request("GET", url).json()
        environment = settings.get("environment", {})
        environment_name = environment.get("name") or ""
        if environment_name:
            raise RuntimeError(
                "the Fabric integration workspace uses the named default "
                f"environment {environment_name!r}; remove it or set that "
                f"environment to Spark Runtime {runtime_version}"
            )
        if environment.get("runtimeVersion") == runtime_version:
            return settings
        return self.request(
            "PATCH",
            url,
            json={
                "environment": {
                    "name": "",
                    "runtimeVersion": runtime_version,
                }
            },
        ).json()

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

    def get_warehouse_snapshot(
        self, workspace_id: str, warehouse_snapshot_id: str
    ) -> dict[str, Any]:
        return self.request(
            "GET",
            f"/workspaces/{workspace_id}/warehouseSnapshots/"
            f"{warehouse_snapshot_id}",
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

    def wait_for_graphql_root_field(
        self,
        workspace_id: str,
        graphql_api_id: str,
        root_field: str,
        *,
        timeout: int = 180,
    ) -> dict[str, Any]:
        if not re.fullmatch(r"[_A-Za-z][_0-9A-Za-z]*", root_field):
            raise ValueError("root_field must be a valid GraphQL name")
        endpoint = (
            f"/workspaces/{workspace_id}/graphqlapis/"
            f"{graphql_api_id}/graphql"
        )
        deadline = time.monotonic() + timeout
        last_errors: Any = None
        while time.monotonic() < deadline:
            try:
                # Fabric blocks introspection on this endpoint, so readiness
                # must be proved by executing the known seeded schema.
                response = self.request(
                    "POST",
                    endpoint,
                    headers={"Accept": "application/graphql-response+json"},
                    json={
                        "query": (
                            "query SchemaReady { "
                            f"{root_field}(first: 1) {{ items {{ id }} }} "
                            "}"
                        ),
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
            root_result = response.get("data", {}).get(root_field)
            if (
                isinstance(root_result, dict)
                and isinstance(root_result.get("items"), list)
            ):
                return {"name": root_field}
            last_errors = response.get("errors")
            self.sleep(10)
        raise TimeoutError(
            f"GraphQL root field {root_field!r} was not ready in time; "
            f"last errors: {last_errors!r}"
        )

    def _wait_for_operation(
        self,
        response: httpx.Response,
        *,
        operation_name: str,
        timeout: int,
        return_result: bool = False,
    ) -> dict[str, Any]:
        operation_id = response.headers.get("x-ms-operation-id")
        if operation_id:
            try:
                operation_id = str(UUID(operation_id))
            except ValueError as error:
                raise RuntimeError(
                    f"{operation_name} included an invalid x-ms-operation-id"
                ) from error
            location = f"/operations/{operation_id}"
        else:
            location = response.headers.get("Location")
        if not location:
            raise RuntimeError(
                f"{operation_name} did not include an operation identifier"
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
                if return_result:
                    return self.request(
                        "GET",
                        f"{location.rstrip('/')}/result",
                    ).json()
                return operation
            self.sleep(10)
        raise TimeoutError(f"{operation_name} did not finish in time")

    def refresh_sql_endpoint_metadata(
        self,
        workspace_id: str,
        sql_endpoint_id: str,
        *,
        timeout: int = 900,
        recreate_tables: bool = False,
    ) -> dict[str, Any]:
        response = self.request(
            "POST",
            (
                f"/workspaces/{workspace_id}/sqlEndpoints/"
                f"{sql_endpoint_id}/refreshMetadata"
            ),
            json={
                "recreateTables": recreate_tables,
                "timeout": {
                    "value": timeout,
                    "timeUnit": "Seconds",
                },
            },
        )
        if response.status_code == 200:
            return response.json()

        return self._wait_for_operation(
            response,
            operation_name="SQL endpoint metadata refresh",
            timeout=timeout,
            return_result=True,
        )

    def wait_for_sql_endpoint_table(
        self,
        workspace_id: str,
        sql_endpoint_id: str,
        table_name: str,
        *,
        timeout: int = 300,
    ) -> dict[str, Any]:
        def normalize_table_name(value: object) -> str:
            return str(value).replace("[", "").replace("]", "").casefold()

        normalized_table = normalize_table_name(table_name)
        expected_names = {
            normalized_table,
            normalized_table.rsplit(".", 1)[-1],
        }
        deadline = time.monotonic() + timeout
        last_statuses: Any = None
        attempt = 0
        while time.monotonic() < deadline:
            remaining = max(1, int(deadline - time.monotonic()))
            result = self.refresh_sql_endpoint_metadata(
                workspace_id,
                sql_endpoint_id,
                timeout=remaining,
                recreate_tables=attempt > 0,
            )
            statuses = result.get("value")
            if not isinstance(statuses, list):
                raise RuntimeError(
                    "SQL endpoint metadata refresh did not return table "
                    f"sync statuses: {result!r}"
                )
            matching = [
                status
                for status in statuses
                if normalize_table_name(status.get("tableName", ""))
                in expected_names
            ]
            for status in matching:
                sync_status = str(status.get("status", "")).casefold()
                if sync_status == "success":
                    return status
                if sync_status == "failure":
                    if attempt > 0:
                        raise RuntimeError(
                            "SQL endpoint failed to synchronize "
                            f"{table_name!r} after a clean rebuild: "
                            f"{status.get('error')!r}"
                        )
            last_statuses = matching or statuses
            if attempt >= 2:
                raise RuntimeError(
                    f"SQL endpoint table {table_name!r} was not synchronized "
                    f"after three refreshes; last statuses: {last_statuses!r}"
                )
            attempt += 1
            remaining = deadline - time.monotonic()
            if remaining > 0:
                self.sleep(min(15, remaining))
        raise TimeoutError(
            f"SQL endpoint table {table_name!r} was not synchronized in time; "
            f"last statuses: {last_statuses!r}"
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
            raise RuntimeError(
                "notebook job response did not include a Location header"
            )
        job_instance_id = urlparse(location).path.rstrip("/").rsplit("/", 1)[-1]
        job_url = (
            f"/workspaces/{workspace_id}/notebooks/{notebook_id}"
            f"/jobs/execute/instances/{job_instance_id}"
        )

        deadline = time.monotonic() + timeout
        self._sleep_for_poll(response, deadline, default=0)
        not_found_retries = 0
        while time.monotonic() < deadline:
            try:
                status_response = self.request(
                    "GET", job_url, params={"beta": "true"}
                )
                job = status_response.json()
            except httpx.HTTPStatusError as error:
                if (
                    error.response.status_code != 404
                    or not_found_retries >= JOB_VISIBILITY_RETRIES
                ):
                    raise
                not_found_retries += 1
                self._sleep_for_poll(
                    error.response,
                    deadline,
                    default=JOB_VISIBILITY_RETRY_SECONDS,
                )
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
                if not (
                    isinstance(exit_value, str)
                    and exit_value.startswith(f"{NOTEBOOK_SUCCESS_VALUE}:")
                ):
                    raise RuntimeError(
                        "seed notebook completed without its success marker; "
                        f"exitValue={exit_value!r}"
                    )
                job["exitValue"] = exit_value
                return job
            self._sleep_for_poll(status_response, deadline, default=10)
        raise TimeoutError(f"notebook job did not finish within {timeout} seconds")
