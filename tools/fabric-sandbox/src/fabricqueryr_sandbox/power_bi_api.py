"""Minimal Power BI client for the ephemeral DAX integration fixture."""

from __future__ import annotations

from collections.abc import Callable
import math
import time
from typing import Any

from azure.core.credentials import TokenCredential
import httpx


POWER_BI_SCOPE = "https://analysis.windows.net/powerbi/api/.default"
POWER_BI_API = "https://api.powerbi.com/v1.0/myorg"
SEMANTIC_MODEL_NAME = "FabricQueryRIntegrationModel"
ARROW_SEMANTIC_MODEL_NAME = "FabricQueryRArrowIntegrationModel"
SEMANTIC_MODEL_TABLE = "Facts"
TERMINAL_REFRESH_FAILURES = {"Failed", "Disabled", "Cancelled", "TimedOut"}
RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}
IDEMPOTENT_METHODS = {"GET", "HEAD", "OPTIONS", "PUT", "DELETE"}


class PowerBiApi:
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
            base_url=POWER_BI_API,
            timeout=60,
            transport=transport,
        )
        self.sleep = sleep
        self.max_attempts = max_attempts

    def close(self) -> None:
        self.client.close()

    def __enter__(self) -> "PowerBiApi":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        target = self._same_origin_url(url)
        supplied_headers = kwargs.pop("headers", {})
        method = method.upper()
        response: httpx.Response | None = None
        for attempt in range(1, self.max_attempts + 1):
            token = self.credential.get_token(POWER_BI_SCOPE).token
            headers = {"Authorization": f"Bearer {token}"}
            headers.update(supplied_headers)
            try:
                response = self.client.request(
                    method,
                    target,
                    headers=headers,
                    **kwargs,
                )
            except (httpx.ConnectError, httpx.ConnectTimeout):
                if attempt == self.max_attempts:
                    raise
                delay = min(30.0, 0.5 * (2 ** (attempt - 1)))
                if delay > 0:
                    self.sleep(delay)
                continue
            retryable = response.status_code in RETRYABLE_STATUS_CODES and (
                method in IDEMPOTENT_METHODS or response.status_code == 429
            )
            if not retryable or attempt == self.max_attempts:
                break
            default = min(30.0, 0.5 * (2 ** (attempt - 1)))
            delay = self._retry_after(response, default)
            if delay > 0:
                self.sleep(delay)
        if response is None:
            raise AssertionError("Power BI request loop ended without a response")
        response.raise_for_status()
        return response

    @staticmethod
    def _retry_after(response: httpx.Response, default: float) -> float:
        value = response.headers.get("Retry-After")
        if value is None:
            return default
        try:
            delay = max(0.0, float(value))
        except ValueError:
            return default
        return delay if math.isfinite(delay) else default

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
                "Power BI API URLs must remain on the configured HTTPS origin"
            )
        return target

    def create_test_semantic_model(self, workspace_id: str) -> dict[str, Any]:
        """Create the Push fixture used to exercise the JSON query API."""
        return self.request(
            "POST",
            f"/groups/{workspace_id}/datasets",
            json={
                "name": SEMANTIC_MODEL_NAME,
                "defaultMode": "Push",
                "tables": [
                    {
                        "name": SEMANTIC_MODEL_TABLE,
                        "columns": [
                            {"name": "id", "dataType": "Int64"},
                            {"name": "name", "dataType": "string"},
                            {"name": "category", "dataType": "string"},
                            {"name": "amount", "dataType": "Double"},
                        ],
                    }
                ],
            },
        ).json()

    def find_datasets(
        self, workspace_id: str, name: str
    ) -> list[dict[str, Any]]:
        datasets = self.request(
            "GET",
            f"/groups/{workspace_id}/datasets",
        ).json().get("value", [])
        return [
            dataset
            for dataset in datasets
            if dataset.get("name", "").casefold() == name.casefold()
        ]

    def find_dataset(self, workspace_id: str, name: str) -> dict[str, Any]:
        matches = self.find_datasets(workspace_id, name)
        if not matches:
            raise LookupError(f"Power BI dataset not found: {name}")
        if len(matches) > 1:
            raise LookupError(f"Power BI dataset name is ambiguous: {name}")
        return matches[0]

    def delete_dataset(self, workspace_id: str, dataset_id: str) -> None:
        self.request(
            "DELETE",
            f"/groups/{workspace_id}/datasets/{dataset_id}",
        )

    def reset_test_semantic_model(
        self, workspace_id: str
    ) -> dict[str, Any]:
        """Delete stale fixture copies and create exactly one fresh model."""
        for dataset in self.find_datasets(
            workspace_id, SEMANTIC_MODEL_NAME
        ):
            self.delete_dataset(workspace_id, dataset["id"])
        return self.create_test_semantic_model(workspace_id)

    def add_test_rows(self, workspace_id: str, dataset_id: str) -> None:
        self.request(
            "POST",
            (
                f"/groups/{workspace_id}/datasets/{dataset_id}/tables/"
                f"{SEMANTIC_MODEL_TABLE}/rows"
            ),
            json={
                "rows": [
                    {"id": 1, "name": "alpha", "category": "A", "amount": 10.5},
                    {"id": 2, "name": "beta", "category": "B", "amount": 20.0},
                    {"id": 3, "name": "gamma", "category": "A", "amount": None},
                ]
            },
        )

    def wait_for_test_rows(
        self,
        workspace_id: str,
        dataset_id: str,
        *,
        timeout: int = 180,
    ) -> None:
        deadline = time.monotonic() + timeout
        last_error: Exception | None = None
        while time.monotonic() < deadline:
            try:
                response = self.request(
                    "POST",
                    (
                        f"/groups/{workspace_id}/datasets/{dataset_id}"
                        "/executeQueries"
                    ),
                    json={
                        "queries": [
                            {
                                "query": (
                                    'EVALUATE ROW("row_count", '
                                    f"COUNTROWS('{SEMANTIC_MODEL_TABLE}'))"
                                )
                            }
                        ],
                        "serializerSettings": {"includeNulls": True},
                    },
                ).json()
                rows = response["results"][0]["tables"][0]["rows"]
                if rows and rows[0].get("[row_count]") == 3:
                    return
            except (httpx.HTTPError, KeyError, IndexError, TypeError) as error:
                last_error = error
            self.sleep(5)
        raise TimeoutError(
            "Power BI semantic model rows were not queryable in time"
        ) from last_error

    def refresh_import_model(
        self,
        workspace_id: str,
        dataset_id: str,
        *,
        timeout: int = 900,
    ) -> dict[str, Any]:
        """Refresh the source-controlled Arrow fixture and await completion."""
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        response = self.request(
            "POST",
            f"/groups/{workspace_id}/datasets/{dataset_id}/refreshes",
        )
        if response.status_code != 202:
            raise RuntimeError(
                "Power BI refresh trigger returned unexpected HTTP "
                f"{response.status_code}"
            )
        location = response.headers.get("Location")
        if not location:
            raise RuntimeError(
                "Power BI refresh response did not include a Location header"
            )
        location_url = self._same_origin_url(location)
        expected_prefix = self.client.build_request(
            "GET",
            f"/groups/{workspace_id}/datasets/{dataset_id}/refreshes/",
        ).url.path
        refresh_id = location_url.path.removeprefix(expected_prefix)
        if (
            not location_url.path.startswith(expected_prefix)
            or not refresh_id
            or "/" in refresh_id
            or location_url.query
            or location_url.fragment
        ):
            raise RuntimeError(
                "Power BI refresh Location did not match the expected "
                "dataset refresh route"
            )
        refresh_url = (
            f"/groups/{workspace_id}/datasets/{dataset_id}/refreshes/"
            f"{refresh_id}"
        )
        deadline = time.monotonic() + timeout
        last_refresh: dict[str, Any] | None = None
        while time.monotonic() < deadline:
            status_response = self.request("GET", refresh_url)
            if status_response.status_code not in {200, 202}:
                raise RuntimeError(
                    "Power BI refresh status returned unexpected HTTP "
                    f"{status_response.status_code}"
                )
            try:
                refresh = status_response.json()
            except ValueError as error:
                raise RuntimeError(
                    "Power BI refresh status did not return valid JSON"
                ) from error
            if not isinstance(refresh, dict):
                raise RuntimeError(
                    "Power BI refresh status did not return an object"
                )
            last_refresh = refresh
            status = refresh.get("status")
            if not isinstance(status, str) or not status:
                raise RuntimeError(
                    "Power BI refresh status did not include a status value"
                )
            extended_value = refresh.get("extendedStatus")
            extended_status = (
                extended_value if isinstance(extended_value, str) else ""
            )
            terminal_failure = next(
                (
                    value
                    for value in (status, extended_status)
                    if value in TERMINAL_REFRESH_FAILURES
                ),
                None,
            )
            if terminal_failure is not None:
                detail = (
                    refresh.get("messages")
                    or refresh.get("serviceExceptionJson")
                    or refresh.get("refreshAttempts")
                )
                raise RuntimeError(
                    f"Power BI semantic model refresh ended in "
                    f"{terminal_failure}: {detail!r}"
                )
            if status_response.status_code == 200:
                if status == "Completed":
                    return refresh
                raise RuntimeError(
                    "Power BI refresh returned a terminal response with "
                    f"unexpected status {status!r}: {refresh!r}"
                )
            if status != "Unknown":
                raise RuntimeError(
                    "Power BI in-progress refresh returned unexpected status "
                    f"{status!r}: {refresh!r}"
                )
            retry_after = status_response.headers.get("Retry-After")
            try:
                delay = max(0.0, float(retry_after or 5))
            except ValueError:
                delay = 5
            if not math.isfinite(delay):
                delay = 5
            remaining = max(0.0, deadline - time.monotonic())
            if delay > 0 and remaining > 0:
                self.sleep(min(delay, remaining))
        raise TimeoutError(
            "Power BI semantic model refresh did not finish in time; "
            f"last status: {last_refresh!r}"
        )


def seed_test_semantic_model(
    credential: TokenCredential,
    workspace_id: str,
) -> dict[str, Any]:
    with PowerBiApi(credential) as api:
        dataset = api.reset_test_semantic_model(workspace_id)
        api.add_test_rows(workspace_id, dataset["id"])
        api.wait_for_test_rows(workspace_id, dataset["id"])
        return dataset


def prepare_arrow_test_semantic_model(
    credential: TokenCredential,
    workspace_id: str,
) -> dict[str, Any]:
    """Refresh and verify the modern import model used by the Arrow API."""
    with PowerBiApi(credential) as api:
        dataset = api.find_dataset(workspace_id, ARROW_SEMANTIC_MODEL_NAME)
        api.refresh_import_model(workspace_id, dataset["id"])
        api.wait_for_test_rows(workspace_id, dataset["id"])
        return dataset
