import json

from azure.core.credentials import AccessToken
import httpx
import pytest

from fabricqueryr_sandbox.kusto_api import (
    KUSTO_SCOPE,
    SEED_COMMAND,
    SEED_TABLE,
    KustoApi,
)


class RecordingCredential:
    def __init__(self):
        self.scopes = []

    def get_token(self, *scopes, **_kwargs):
        self.scopes.extend(scopes)
        return AccessToken("kusto-token", 4_102_444_800)


def test_seed_fixture_uses_kusto_scope_and_replace_command():
    credential = RecordingCredential()

    def handler(request):
        assert request.url == "https://cluster.kusto.test/v1/rest/mgmt"
        assert request.headers["Authorization"] == "Bearer kusto-token"
        assert request.headers["x-ms-app"] == "fabricqueryr-sandbox"
        body = json.loads(request.content)
        assert body["db"] == "TestKQLDatabase"
        assert body["csl"] == SEED_COMMAND
        assert body["csl"].startswith(f".set-or-replace {SEED_TABLE}")
        assert "datatable(" in body["csl"]
        properties = json.loads(body["properties"])
        assert properties["ClientRequestId"].startswith(
            "fabricqueryr-sandbox.Seed;"
        )
        return httpx.Response(200, json={"Tables": []})

    with KustoApi(
        credential,
        transport=httpx.MockTransport(handler),
    ) as api:
        result = api.seed_fixture(
            "https://cluster.kusto.test/",
            "TestKQLDatabase",
        )

    assert result == {"Tables": []}
    assert credential.scopes == [KUSTO_SCOPE]


def test_management_seed_retries_transient_readiness_failures():
    credential = RecordingCredential()
    attempts = 0
    sleeps = []

    def handler(_request):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return httpx.Response(503, headers={"Retry-After": "3"})
        return httpx.Response(200, json={"Tables": []})

    with KustoApi(
        credential,
        transport=httpx.MockTransport(handler),
        sleep=sleeps.append,
    ) as api:
        api.seed_fixture("https://cluster.kusto.test", "TestKQLDatabase")

    assert attempts == 2
    assert sleeps == [3.0]


def test_management_seed_surfaces_embedded_query_status_failure():
    credential = RecordingCredential()
    response = {
        "Tables": [
            {
                "TableName": "QueryStatus",
                "Columns": [
                    {"ColumnName": "Severity"},
                    {"ColumnName": "StatusDescription"},
                ],
                "Rows": [[2, "database principal cannot create table"]],
            }
        ]
    }

    with KustoApi(
        credential,
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(200, json=response)
        ),
    ) as api:
        with pytest.raises(RuntimeError, match="cannot create table"):
            api.seed_fixture(
                "https://cluster.kusto.test",
                "TestKQLDatabase",
            )
