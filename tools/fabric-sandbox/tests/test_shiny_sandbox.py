import base64
import io
import json
from pathlib import Path
from unittest.mock import MagicMock

import fabricqueryr_sandbox.shiny_sandbox as shiny
import httpx
import pytest
from azure.core.credentials import AccessToken
from fabricqueryr_sandbox.fabric_api import FabricApi
from pyarrow import parquet

ROOT = Path(__file__).parents[3]
OWNER = "11111111-1111-1111-1111-111111111111"
CAPACITY = "22222222-2222-2222-2222-222222222222"
OPERATION = "33333333-3333-3333-3333-333333333333"
REPO = "example/fabricQueryR"


class Credential:
    def get_token(self, *_args, **_kwargs):
        return AccessToken("test-token", 4_102_444_800)


def workspace(*, ready=False, owner=OWNER):
    return {
        "id": "workspace",
        "displayName": shiny.WORKSPACE_NAME,
        "capacityId": CAPACITY,
        "description": shiny.workspace_marker(REPO, owner, ready=ready),
    }


class FakeFabric:
    def __init__(self, existing=None, items=()):
        self.existing = existing
        self.items = list(items)
        self.requests = []

    def list_workspaces(self):
        return [self.existing] if self.existing else []

    def list_items(self, _workspace):
        return self.items

    def request(self, method, path, **kwargs):
        self.requests.append((method, path, kwargs.get("json")))
        if path == "/capacities":
            payload = {"value": [{"id": CAPACITY, "sku": "F2", "state": "Active"}]}
        elif method == "POST" and path == "/workspaces":
            self.existing = {"id": "workspace", **kwargs["json"]}
            payload = self.existing
        elif path == "/workspaces/workspace/roleAssignments":
            payload = {
                "value": [
                    {"id": "owner-role", "principal": {"id": OWNER}, "role": "Admin"}
                ]
            }
        elif path == "/workspaces/workspace":
            if method == "PATCH":
                self.existing.update(kwargs["json"])
            payload = self.existing
        else:
            raise AssertionError((method, path, kwargs))
        return httpx.Response(200, json=payload)


def test_ready_workspace_is_reused_without_reseeding(monkeypatch):
    items = [
        {"id": name, "displayName": name, "type": kind}
        for name, kind in shiny.REQUIRED_ITEMS.items()
    ]
    api = FakeFabric(workspace(ready=True), items)
    seed = MagicMock()
    monkeypatch.setattr(shiny, "seed_targets", seed)
    assert shiny.prepare(api, Credential(), CAPACITY, REPO, OWNER, ROOT) == "workspace"
    seed.assert_not_called()
    assert all(method == "GET" for method, _, _ in api.requests)


@pytest.mark.parametrize(
    "existing", [None, workspace(ready=False), workspace(ready=True)]
)
def test_missing_fixtures_are_prepared_and_marked_ready_only_after_success(
    monkeypatch, existing
):
    api = FakeFabric(existing)
    monkeypatch.setattr(shiny, "ensure_targets", lambda *args: {"fixture": "targets"})

    def seed(_api, _credential, _workspace, targets, _root):
        assert targets == {"fixture": "targets"}
        assert "ready=false" in api.existing["description"]

    monkeypatch.setattr(shiny, "seed_targets", seed)
    assert shiny.prepare(api, Credential(), CAPACITY, REPO, OWNER, ROOT) == "workspace"
    assert "ready=true" in api.existing["description"]
    assert not any(method == "DELETE" for method, _, _ in api.requests)


def test_failed_seed_remains_incomplete_and_an_unowned_workspace_is_untouched(
    monkeypatch,
):
    api = FakeFabric(workspace(owner="different-owner"))
    with pytest.raises(RuntimeError, match="ownership"):
        shiny.prepare(api, Credential(), CAPACITY, REPO, OWNER, ROOT)
    assert all(method == "GET" for method, _, _ in api.requests)
    api = FakeFabric(workspace(ready=True))
    monkeypatch.setattr(shiny, "ensure_targets", lambda *args: {})

    def fail(*_args):
        raise RuntimeError("SQL unavailable")

    monkeypatch.setattr(shiny, "seed_targets", fail)
    with pytest.raises(RuntimeError, match="SQL unavailable"):
        shiny.prepare(api, Credential(), CAPACITY, REPO, OWNER, ROOT)
    assert "ready=false" in api.existing["description"]


def test_owner_assignment_is_added_when_an_interrupted_start_did_not_add_it():
    api = MagicMock()
    api.request.return_value = httpx.Response(200, json={"value": []})
    shiny.ensure_owner(api, "workspace", OWNER)
    api.request.assert_called_with(
        "POST",
        "/workspaces/workspace/roleAssignments",
        json={"principal": {"id": OWNER, "type": "User"}, "role": "Admin"},
    )


def test_target_creation_uses_schema_lakehouse_parent_eventhouse_and_real_model_definition():
    requests = []
    types = {
        "lakehouses": "Lakehouse",
        "warehouses": "Warehouse",
        "mirroredDatabases": "MirroredDatabase",
        "eventhouses": "Eventhouse",
        "kqlDatabases": "KQLDatabase",
        "graphQLApis": "GraphQLApi",
        "semanticModels": "SemanticModel",
    }

    def handler(request):
        body = json.loads(request.content)
        requests.append((request.url.path, body))
        route = request.url.path.rsplit("/", 1)[1]
        return httpx.Response(
            201,
            json={
                "id": route,
                "displayName": body["displayName"],
                "type": types[route],
            },
        )

    with FabricApi(Credential(), transport=httpx.MockTransport(handler)) as api:
        items = []
        targets = shiny.ensure_targets(api, "workspace", items, ROOT)
        count = len(requests)
        assert shiny.ensure_targets(api, "workspace", items, ROOT) == targets
        assert len(requests) == count
    bodies = {path.rsplit("/", 1)[1]: body for path, body in requests}
    assert bodies["lakehouses"]["creationPayload"] == {"enableSchemas": True}
    # The mirrored-database REST API rejects the Terraform-specific "Default"
    # format value. Its documented request supplies definition parts directly.
    assert "format" not in bodies["mirroredDatabases"]["definition"]
    assert bodies["mirroredDatabases"]["definition"]["parts"][0]["path"] == "mirroring.json"
    assert (
        bodies["kqlDatabases"]["creationPayload"]["parentEventhouseItemId"]
        == "eventhouses"
    )
    model = bodies["semanticModels"]["definition"]
    assert model["format"] == "TMSL"
    assert [part["path"] for part in model["parts"]] == [
        "model.bim",
        "definition.pbism",
    ]
    assert (
        base64.b64decode(model["parts"][0]["payload"])
        == (
            ROOT
            / "infra/fabric/workspace/FabricQueryRArrowIntegrationModel.SemanticModel/model.bim"
        ).read_bytes()
    )


def test_lakehouse_uploads_typed_fixture_and_waits_for_the_real_load_operation(
    monkeypatch,
):
    files = shiny.lakehouse_fixture_files(ROOT)
    table = parquet.read_table(io.BytesIO(files["basic.parquet"]))
    assert table["id"].to_pylist() == [1, 2, 3]
    assert table["amount"].to_pylist() == [10.5, 20, None]
    service = MagicMock()
    filesystem = service.__enter__.return_value.get_file_system_client.return_value
    monkeypatch.setattr(shiny, "DataLakeServiceClient", lambda **kwargs: service)
    sql = MagicMock()
    monkeypatch.setattr(shiny, "wait_for_sql_fixture", sql)
    requests = []

    def handler(request):
        requests.append(request)
        if request.url.path.endswith("/load"):
            assert request.url.params["beta"] == "true"
            assert json.loads(request.content) == {
                "relativePath": "Files/fixtures/basic.parquet",
                "pathType": "File",
                "mode": "Overwrite",
                "formatOptions": {"format": "Parquet"},
            }
            return httpx.Response(202, headers={"x-ms-operation-id": OPERATION})
        if "/operations/" in request.url.path:
            return httpx.Response(200, json={"status": "Succeeded"})
        if request.url.path.endswith("/lakehouses/lakehouse"):
            return httpx.Response(
                200,
                json={
                    "properties": {
                        "sqlEndpointProperties": {
                            "id": "sql-id",
                            "connectionString": "sql.test",
                            "provisioningStatus": "Success",
                        }
                    }
                },
            )
        if request.url.path.endswith("/refreshMetadata"):
            return httpx.Response(200, json={})
        raise AssertionError(str(request.url))

    with FabricApi(Credential(), transport=httpx.MockTransport(handler)) as api:
        shiny.seed_lakehouse(api, Credential(), "workspace", "lakehouse", ROOT)
    assert filesystem.get_file_client.call_count == 2
    sql.assert_called_once_with(
        "sql.test", "TestLakehouse", "test-token", "fabricqueryr_basic"
    )
    assert (
        sum(
            request.method == "POST" and request.url.path.endswith("/load")
            for request in requests
        )
        == 1
    )
