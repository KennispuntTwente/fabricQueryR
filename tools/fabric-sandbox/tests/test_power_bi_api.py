import json

from azure.core.credentials import AccessToken
import httpx
import pytest

from fabricqueryr_sandbox.power_bi_api import (
    ARROW_SEMANTIC_MODEL_NAME,
    PowerBiApi,
    SEMANTIC_MODEL_NAME,
    SEMANTIC_MODEL_TABLE,
    prepare_arrow_test_semantic_model,
)


class StaticCredential:
    def get_token(self, *scopes, **_kwargs):
        assert scopes == ("https://analysis.windows.net/powerbi/api/.default",)
        return AccessToken("power-bi-token", 4_102_444_800)


def test_semantic_model_fixture_is_created_seeded_and_verified():
    requests = []

    def handler(request):
        requests.append(request)
        assert request.headers["Authorization"] == "Bearer power-bi-token"
        if request.url.path.endswith("/datasets"):
            return httpx.Response(
                201,
                json={"id": "dataset-id", "name": SEMANTIC_MODEL_NAME},
            )
        if request.url.path.endswith(f"/tables/{SEMANTIC_MODEL_TABLE}/rows"):
            return httpx.Response(200)
        if request.url.path.endswith("/executeQueries"):
            return httpx.Response(
                200,
                json={
                    "results": [
                        {
                            "tables": [
                                {"rows": [{"[row_count]": 3}]},
                            ]
                        }
                    ]
                },
            )
        raise AssertionError(f"unexpected request: {request.url}")

    with PowerBiApi(
        StaticCredential(),
        transport=httpx.MockTransport(handler),
        sleep=lambda _: None,
    ) as api:
        dataset = api.create_test_semantic_model("workspace-id")
        api.add_test_rows("workspace-id", dataset["id"])
        api.wait_for_test_rows("workspace-id", dataset["id"])

    create_payload = json.loads(requests[0].content)
    assert create_payload["name"] == SEMANTIC_MODEL_NAME
    assert create_payload["defaultMode"] == "Push"
    assert create_payload["tables"][0]["name"] == SEMANTIC_MODEL_TABLE

    row_payload = json.loads(requests[1].content)
    assert len(row_payload["rows"]) == 3
    assert row_payload["rows"][2]["amount"] is None


def test_semantic_model_fixture_can_be_found_by_unique_name():
    def handler(request):
        assert request.method == "GET"
        return httpx.Response(
            200,
            json={
                "value": [
                    {"id": "other-id", "name": "Other"},
                    {"id": "dataset-id", "name": SEMANTIC_MODEL_NAME.lower()},
                ]
            },
        )

    with PowerBiApi(
        StaticCredential(),
        transport=httpx.MockTransport(handler),
    ) as api:
        dataset = api.find_dataset("workspace-id", SEMANTIC_MODEL_NAME)

    assert dataset["id"] == "dataset-id"


def test_semantic_model_fixture_reset_removes_all_stale_copies():
    requests = []

    def handler(request):
        requests.append((request.method, request.url.path))
        if request.method == "GET":
            return httpx.Response(
                200,
                json={
                    "value": [
                        {"id": "stale-1", "name": SEMANTIC_MODEL_NAME},
                        {
                            "id": "stale-2",
                            "name": SEMANTIC_MODEL_NAME.lower(),
                        },
                        {"id": "other", "name": "Other"},
                    ]
                },
            )
        if request.method == "DELETE":
            return httpx.Response(200)
        if request.method == "POST" and request.url.path.endswith("/datasets"):
            return httpx.Response(
                201,
                json={"id": "fresh", "name": SEMANTIC_MODEL_NAME},
            )
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    with PowerBiApi(
        StaticCredential(),
        transport=httpx.MockTransport(handler),
    ) as api:
        dataset = api.reset_test_semantic_model("workspace-id")

    assert dataset["id"] == "fresh"
    assert requests == [
        ("GET", "/v1.0/myorg/groups/workspace-id/datasets"),
        ("DELETE", "/v1.0/myorg/groups/workspace-id/datasets/stale-1"),
        ("DELETE", "/v1.0/myorg/groups/workspace-id/datasets/stale-2"),
        ("POST", "/v1.0/myorg/groups/workspace-id/datasets"),
    ]


def test_arrow_semantic_model_fixture_is_refreshed_and_verified(monkeypatch):
    requests = []
    refresh_reads = 0

    def handler(request):
        nonlocal refresh_reads
        requests.append((request.method, request.url.path))
        if request.method == "GET" and request.url.path.endswith("/datasets"):
            return httpx.Response(
                200,
                json={
                    "value": [
                        {
                            "id": "arrow-dataset-id",
                            "name": ARROW_SEMANTIC_MODEL_NAME,
                        }
                    ]
                },
            )
        if request.method == "POST" and request.url.path.endswith("/refreshes"):
            return httpx.Response(
                202,
                headers={
                    "Location": (
                        "https://api.powerbi.com/v1.0/myorg/groups/"
                        "workspace-id/datasets/arrow-dataset-id/refreshes/"
                        "refresh-id"
                    )
                },
            )
        if request.method == "GET" and request.url.path.endswith(
            "/refreshes/refresh-id"
        ):
            refresh_reads += 1
            if refresh_reads == 1:
                return httpx.Response(
                    202,
                    headers={"Retry-After": "0"},
                    json={
                        "status": "Unknown",
                        "extendedStatus": "InProgress",
                    },
                )
            return httpx.Response(
                200,
                json={
                    "status": "Completed",
                    "extendedStatus": "Completed",
                },
            )
        if request.url.path.endswith("/executeQueries"):
            return httpx.Response(
                200,
                json={
                    "results": [
                        {
                            "tables": [
                                {"rows": [{"[row_count]": 3}]},
                            ]
                        }
                    ]
                },
            )
        raise AssertionError(f"unexpected request: {request.url}")

    credential = StaticCredential()
    original_init = PowerBiApi.__init__

    def fake_init(self, supplied_credential):
        original_init(
            self,
            supplied_credential,
            transport=httpx.MockTransport(handler),
            sleep=lambda _: None,
        )

    monkeypatch.setattr(PowerBiApi, "__init__", fake_init)
    dataset = prepare_arrow_test_semantic_model(
        credential,
        "workspace-id",
    )

    assert dataset["id"] == "arrow-dataset-id"
    assert requests == [
        ("GET", "/v1.0/myorg/groups/workspace-id/datasets"),
        (
            "POST",
            "/v1.0/myorg/groups/workspace-id/datasets/"
            "arrow-dataset-id/refreshes",
        ),
        (
            "GET",
            "/v1.0/myorg/groups/workspace-id/datasets/"
            "arrow-dataset-id/refreshes/refresh-id",
        ),
        (
            "GET",
            "/v1.0/myorg/groups/workspace-id/datasets/"
            "arrow-dataset-id/refreshes/refresh-id",
        ),
        (
            "POST",
            "/v1.0/myorg/groups/workspace-id/datasets/"
            "arrow-dataset-id/executeQueries",
        ),
    ]


def test_arrow_fixture_fails_before_querying_stale_rows(monkeypatch):
    requests = []

    def handler(request):
        requests.append((request.method, request.url.path))
        if request.method == "GET" and request.url.path.endswith("/datasets"):
            return httpx.Response(
                200,
                json={
                    "value": [
                        {
                            "id": "arrow-dataset-id",
                            "name": ARROW_SEMANTIC_MODEL_NAME,
                        }
                    ]
                },
            )
        if request.method == "POST" and request.url.path.endswith("/refreshes"):
            return httpx.Response(
                202,
                headers={
                    "Location": (
                        "https://api.powerbi.com/v1.0/myorg/groups/"
                        "workspace-id/datasets/arrow-dataset-id/refreshes/"
                        "failed-refresh"
                    )
                },
            )
        if request.method == "GET" and request.url.path.endswith(
            "/refreshes/failed-refresh"
        ):
            return httpx.Response(
                200,
                json={
                    "status": "Failed",
                    "extendedStatus": "Failed",
                    "messages": [{"type": "Error", "message": "bad source"}],
                },
            )
        if request.url.path.endswith("/executeQueries"):
            return httpx.Response(
                200,
                json={
                    "results": [
                        {"tables": [{"rows": [{"[row_count]": 3}]}]}
                    ]
                },
            )
        raise AssertionError(f"unexpected request: {request.url}")

    credential = StaticCredential()
    original_init = PowerBiApi.__init__

    def fake_init(self, supplied_credential):
        original_init(
            self,
            supplied_credential,
            transport=httpx.MockTransport(handler),
            sleep=lambda _: None,
        )

    monkeypatch.setattr(PowerBiApi, "__init__", fake_init)
    with pytest.raises(RuntimeError, match="ended in Failed.*bad source"):
        prepare_arrow_test_semantic_model(credential, "workspace-id")

    assert not any(path.endswith("/executeQueries") for _, path in requests)


@pytest.mark.parametrize(
    ("headers", "message"),
    [
        ({}, "Location header"),
        (
            {"Location": "https://attacker.example/refreshes/refresh-id"},
            "configured HTTPS origin",
        ),
        (
            {
                "Location": (
                    "https://api.powerbi.com/v1.0/myorg/groups/other/"
                    "datasets/dataset-id/refreshes/refresh-id"
                )
            },
            "expected dataset refresh route",
        ),
    ],
)
def test_import_refresh_rejects_an_unsafe_status_location(headers, message):
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(202, headers=headers)

    with PowerBiApi(
        StaticCredential(),
        transport=httpx.MockTransport(handler),
    ) as api:
        with pytest.raises((RuntimeError, ValueError), match=message):
            api.refresh_import_model("workspace-id", "dataset-id")

    assert len(requests) == 1


def test_import_refresh_requires_an_accepted_trigger_response():
    location = (
        "https://api.powerbi.com/v1.0/myorg/groups/workspace-id/"
        "datasets/dataset-id/refreshes/refresh-id"
    )

    def handler(_request):
        return httpx.Response(200, headers={"Location": location})

    with PowerBiApi(
        StaticCredential(),
        transport=httpx.MockTransport(handler),
    ) as api:
        with pytest.raises(RuntimeError, match="unexpected HTTP 200"):
            api.refresh_import_model("workspace-id", "dataset-id")
