import json

from azure.core.credentials import AccessToken
import httpx

from fabricqueryr_sandbox.power_bi_api import (
    PowerBiApi,
    SEMANTIC_MODEL_NAME,
    SEMANTIC_MODEL_TABLE,
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
