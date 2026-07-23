"""Minimal Power BI client for the ephemeral DAX integration fixture."""

from __future__ import annotations

from collections.abc import Callable
import time
from typing import Any

from azure.core.credentials import TokenCredential
import httpx


POWER_BI_SCOPE = "https://analysis.windows.net/powerbi/api/.default"
POWER_BI_API = "https://api.powerbi.com/v1.0/myorg"
SEMANTIC_MODEL_NAME = "FabricQueryRIntegrationModel"
SEMANTIC_MODEL_TABLE = "Facts"


class PowerBiApi:
    def __init__(
        self,
        credential: TokenCredential,
        *,
        transport: httpx.BaseTransport | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.credential = credential
        self.client = httpx.Client(
            base_url=POWER_BI_API,
            timeout=60,
            transport=transport,
        )
        self.sleep = sleep

    def close(self) -> None:
        self.client.close()

    def __enter__(self) -> "PowerBiApi":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        token = self.credential.get_token(POWER_BI_SCOPE).token
        headers = {"Authorization": f"Bearer {token}"}
        headers.update(kwargs.pop("headers", {}))
        response = self.client.request(method, url, headers=headers, **kwargs)
        response.raise_for_status()
        return response

    def create_test_semantic_model(self, workspace_id: str) -> dict[str, Any]:
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


def seed_test_semantic_model(
    credential: TokenCredential,
    workspace_id: str,
) -> dict[str, Any]:
    with PowerBiApi(credential) as api:
        dataset = api.create_test_semantic_model(workspace_id)
        api.add_test_rows(workspace_id, dataset["id"])
        api.wait_for_test_rows(workspace_id, dataset["id"])
        return dataset
