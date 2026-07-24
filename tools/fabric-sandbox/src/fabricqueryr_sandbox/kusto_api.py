"""Narrow Kusto REST client used only to seed the integration fixture."""

from __future__ import annotations

from collections.abc import Callable
import time
from typing import Any
from uuid import uuid4

from azure.core.credentials import TokenCredential
import httpx


KUSTO_SCOPE = "https://api.kusto.windows.net/.default"
TRANSIENT_STATUS_CODES = {408, 429, 500, 502, 503, 504}
SEED_TABLE = "fabricqueryr_events"
SEED_COMMAND = f"""
.set-or-replace {SEED_TABLE} with (recreate_schema=true) <|
datatable(
    id:int,
    name:string,
    category:string,
    amount:real,
    observed_at:datetime,
    active:bool,
    correlation_id:guid,
    metadata:dynamic
)
[
    1, "alpha", "A", 10.5, datetime(2026-01-01T00:00:00Z), true,
        guid(11111111-1111-1111-1111-111111111111),
        dynamic({{"source":"sandbox","rank":1}}),
    2, "beta", "B", 20.0, datetime(2026-01-02T00:00:00Z), false,
        guid(22222222-2222-2222-2222-222222222222),
        dynamic({{"source":"sandbox","rank":2}}),
    3, "gamma", "A", real(null), datetime(2026-01-03T00:00:00Z), true,
        guid(33333333-3333-3333-3333-333333333333),
        dynamic({{"source":"sandbox","rank":3}})
]
""".strip()


class KustoApi:
    def __init__(
        self,
        credential: TokenCredential,
        *,
        transport: httpx.BaseTransport | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.credential = credential
        self.client = httpx.Client(timeout=90, transport=transport)
        self.sleep = sleep

    def close(self) -> None:
        self.client.close()

    def __enter__(self) -> "KustoApi":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def execute_management(
        self,
        query_service_uri: str,
        database: str,
        command: str,
        *,
        max_attempts: int = 12,
    ) -> dict[str, Any]:
        token = self.credential.get_token(KUSTO_SCOPE).token
        url = f"{query_service_uri.rstrip('/')}/v1/rest/mgmt"
        client_request_id = f"fabricqueryr-sandbox.Seed;{uuid4()}"
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=utf-8",
            "x-ms-app": "fabricqueryr-sandbox",
            "x-ms-client-request-id": client_request_id,
        }
        response: httpx.Response | None = None
        for attempt in range(1, max_attempts + 1):
            response = self.client.post(
                url,
                headers=headers,
                json={
                    "db": database,
                    "csl": command,
                    "properties": (
                        f'{{"ClientRequestId":"{client_request_id}"}}'
                    ),
                },
            )
            if (
                response.status_code not in TRANSIENT_STATUS_CODES
                or attempt == max_attempts
            ):
                break
            retry_after = response.headers.get("Retry-After")
            delay = float(retry_after) if retry_after else min(30, 2**attempt)
            self.sleep(delay)

        assert response is not None
        response.raise_for_status()
        payload = response.json()
        self._raise_embedded_failure(payload)
        return payload

    @staticmethod
    def _raise_embedded_failure(payload: dict[str, Any]) -> None:
        for table in payload.get("Tables", []):
            if table.get("TableName") != "QueryStatus":
                continue
            columns = [
                column.get("ColumnName") for column in table.get("Columns", [])
            ]
            try:
                severity_index = columns.index("Severity")
            except ValueError:
                continue
            description_index = (
                columns.index("StatusDescription")
                if "StatusDescription" in columns
                else None
            )
            for row in table.get("Rows", []):
                if row[severity_index] <= 2:
                    detail = (
                        row[description_index]
                        if description_index is not None
                        else "Kusto management command failed"
                    )
                    raise RuntimeError(str(detail))

    def seed_fixture(self, query_service_uri: str, database: str) -> dict[str, Any]:
        return self.execute_management(
            query_service_uri,
            database,
            SEED_COMMAND,
        )
