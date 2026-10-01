import json
from dataclasses import replace
from unittest.mock import MagicMock

import httpx
import pytest

from fabricqueryr_sandbox import persistent_ci as ci
from fabricqueryr_sandbox.cleanup import parse_ci_description
from fabricqueryr_sandbox.deployment_revision import record_deployments
from fabricqueryr_sandbox.fabric_api import FabricApi
from fabricqueryr_sandbox.fixture_revision import (
    write_fixture_revision,
    fixture_revision,
)
from test_fixture_revision import FakeService, make_settings
from test_shiny_sandbox import Credential, REPO, CAPACITY


class Fabric:
    routes = {
        "lakehouses": "Lakehouse",
        "warehouses": "Warehouse",
        "mirroredDatabases": "MirroredDatabase",
        "sqlDatabases": "SQLDatabase",
        "eventhouses": "Eventhouse",
        "kqlDatabases": "KQLDatabase",
        "graphQLApis": "GraphQLApi",
        "warehouseSnapshots": "WarehouseSnapshot",
    }

    def __init__(self):
        self.workspace = None
        self.items = []
        self.requests = []
        self.serial = 0

    def handle(self, request):
        path, method = request.url.path.removeprefix("/v1"), request.method
        self.requests.append((method, path))
        if path == "/workspaces":
            if method == "POST":
                self.workspace = {"id": "workspace-id", **json.loads(request.content)}
                return httpx.Response(201, json=self.workspace)
            return httpx.Response(
                200, json={"value": [self.workspace] if self.workspace else []}
            )
        if path == "/workspaces/workspace-id":
            return httpx.Response(200, json=self.workspace)
        if method == "GET" and path == "/workspaces/workspace-id/items":
            return httpx.Response(200, json={"value": self.items})
        if method == "DELETE" and "/items/" in path:
            self.items[:] = [i for i in self.items if i["id"] != path.rsplit("/", 1)[1]]
            return httpx.Response(204)
        route = path.rsplit("/", 1)[1]
        if method == "POST" and route in self.routes:
            self.serial += 1
            item = {
                "id": f"item-{self.serial}",
                "type": self.routes[route],
                **json.loads(request.content),
            }
            self.items.append(item)
            return httpx.Response(201, json=item)
        raise AssertionError((method, path))

    def client(self):
        return FabricApi(Credential(), transport=httpx.MockTransport(self.handle))


@pytest.fixture
def sandbox(tmp_path, monkeypatch):
    settings = replace(make_settings(tmp_path), capacity_id=CAPACITY)
    (settings.workspace_definition_dir / "Model.SemanticModel").rename(
        settings.workspace_definition_dir
        / f"{ci.ARROW_SEMANTIC_MODEL_NAME}.SemanticModel"
    )
    definition = tmp_path / "infra/fabric/terraform/definitions/open-mirroring.json"
    definition.parent.mkdir()
    definition.write_text('{"type":"OpenMirroring"}')
    fabric, service = Fabric(), FakeService()
    service.get_file_system_client("workspace-id").get_directory_client = MagicMock()
    published, seeded = [], []

    def publish(config, *, items):
        published.append(items)
        for name in items:
            display, kind = name.rsplit(".", 1)
            if not any(i["displayName"] == display for i in fabric.items):
                fabric.items.append({"id": name, "displayName": display, "type": kind})
        record_deployments(
            config,
            config.workspace_id,
            config.lakehouse_id,
            items,
            service_client=service,
        )

    def seed(config, *, scope):
        seeded.append(scope)
        runtime = {
            "lane": config.spark_runtime_lane,
            "fabric_runtime": config.spark_runtime_version,
        }
        write_fixture_revision(
            config.workspace_id,
            config.lakehouse_id,
            fixture_revision(config, runtime, scope=scope),
            runtime_contract=runtime,
            service_client=service,
            scope=scope,
        )
        if scope == "all" and not any(
            i["displayName"] == ci.SEMANTIC_MODEL_NAME for i in fabric.items
        ):
            fabric.items.append(
                {
                    "id": "push-model",
                    "displayName": ci.SEMANTIC_MODEL_NAME,
                    "type": "SemanticModel",
                }
            )

    monkeypatch.setattr(ci, "deploy", publish)
    monkeypatch.setattr(ci, "seed", seed)
    monkeypatch.setattr(ci, "discover", MagicMock())
    monkeypatch.setattr(ci, "discover_onelake", MagicMock())
    monkeypatch.setattr(ci, "prepare_arrow_test_semantic_model", MagicMock())
    return settings, fabric, service, published, seeded


@pytest.mark.parametrize(
    "lane,runtime,scope", [("core", "1.3", "all"), ("preview", "2.0", "onelake")]
)
def test_two_consecutive_runs_reuse_workspace_items_data_and_snapshot(
    sandbox, lane, runtime, scope
):
    settings, fabric, service, published, seeded = sandbox
    settings = replace(settings, spark_runtime_lane=lane, spark_runtime_version=runtime)
    with fabric.client() as api:
        first = ci.prepare(api, Credential(), service, settings, REPO)
        mutations = [r for r in fabric.requests if r[0] != "GET"]
        second = ci.prepare(api, Credential(), service, settings, REPO)
    assert first == second
    assert seeded == [scope]
    assert len(published) == 1
    assert [r for r in fabric.requests if r[0] != "GET"] == mutations
    assert parse_ci_description(fabric.workspace["description"]) is None
    assert not fabric.workspace["displayName"].startswith("fabricqueryr-ci-")
    if scope == "onelake":
        assert published == [["SeedFixtures.Notebook"]]


def test_changed_job_definition_publishes_without_reseeding(sandbox):
    settings, fabric, service, published, seeded = sandbox
    with fabric.client() as api:
        ci.prepare(api, Credential(), service, settings, REPO)
        (
            settings.workspace_definition_dir
            / "JobFixtures.Notebook/notebook-content.py"
        ).write_text("updated job")
        ci.prepare(api, Credential(), service, settings, REPO)
    assert published[-1] == ["JobFixtures.Notebook"]
    assert seeded == ["all"]


def test_changed_fixture_reseeds_but_preserves_infrastructure(sandbox):
    settings, fabric, service, _, seeded = sandbox
    with fabric.client() as api:
        first = ci.prepare(api, Credential(), service, settings, REPO)
        (settings.fixture_dir / "basic.csv").write_text("id,name\n1,changed")
        second = ci.prepare(api, Credential(), service, settings, REPO)
    assert first == second
    assert seeded == ["all", "all"]
    deletes = [p for method, p in fabric.requests if method == "DELETE"]
    assert len(deletes) == 1  # Only the stale Warehouse snapshot.


def test_interrupted_seed_is_retried_and_completed_targets_are_kept(
    sandbox, monkeypatch
):
    settings, fabric, service, _, seeded = sandbox
    complete = ci.seed
    monkeypatch.setattr(
        ci, "seed", MagicMock(side_effect=RuntimeError("seed interrupted"))
    )
    with fabric.client() as api:
        with pytest.raises(RuntimeError, match="interrupted"):
            ci.prepare(api, Credential(), service, settings, REPO)
        count = fabric.serial
        monkeypatch.setattr(ci, "seed", complete)
        ci.prepare(api, Credential(), service, settings, REPO)
    assert fabric.serial == count + 1  # The post-seed snapshot was not created yet.
    assert seeded == ["all"]


def test_missing_target_repaired_once_and_incompatible_configuration_rejected(sandbox):
    settings, fabric, service, _, seeded = sandbox
    with fabric.client() as api:
        configured = ci.prepare(api, Credential(), service, settings, REPO)
        fabric.items[:] = [
            i for i in fabric.items if i["displayName"] != "TestWarehouse"
        ]
        ci.prepare(api, Credential(), service, settings, REPO)
        assert seeded == ["all", "all"]
        marker = ci.read_marker(service, configured, "targets")
        marker["configurations"]["TestWarehouse"] = {"wrong": "collation"}
        ci.write_marker(service, configured, "targets", marker)
        with pytest.raises(RuntimeError, match="migrate this item"):
            ci.prepare(api, Credential(), service, settings, REPO)
    assert seeded == ["all", "all"]


@pytest.mark.parametrize(
    "field,value",
    [("description", "belongs to someone else"), ("capacityId", "different-capacity")],
)
def test_refuses_wrong_workspace_owner_or_capacity_before_mutation(
    sandbox, field, value
):
    settings, fabric, service, _, _ = sandbox
    with fabric.client() as api:
        ci.prepare(api, Credential(), service, settings, REPO)
        fabric.workspace[field] = value
        fabric.requests.clear()
        with pytest.raises(RuntimeError):
            ci.prepare(api, Credential(), service, settings, REPO)
    assert all(method == "GET" for method, _ in fabric.requests)


def test_snapshot_failure_does_not_repeat_successful_seed(sandbox, monkeypatch):
    settings, fabric, service, _, seeded = sandbox
    ensure = ci.ensure_item

    def fail_snapshot(*args, **kwargs):
        if "TestWarehouseSnapshot" in args:
            raise RuntimeError("snapshot unavailable")
        return ensure(*args, **kwargs)

    with fabric.client() as api:
        monkeypatch.setattr(ci, "ensure_item", fail_snapshot)
        with pytest.raises(RuntimeError, match="snapshot unavailable"):
            ci.prepare(api, Credential(), service, settings, REPO)
        monkeypatch.setattr(ci, "ensure_item", ensure)
        ci.prepare(api, Credential(), service, settings, REPO)
    assert seeded == ["all"]


def test_missing_deployed_item_is_republished_without_data_reset(sandbox):
    settings, fabric, service, published, seeded = sandbox
    with fabric.client() as api:
        ci.prepare(api, Credential(), service, settings, REPO)
        fabric.items[:] = [
            i for i in fabric.items if i["displayName"] != "TestPipeline"
        ]
        ci.prepare(api, Credential(), service, settings, REPO)
    assert published[-1] == ["TestPipeline.DataPipeline"]
    assert seeded == ["all"]


def test_old_snapshot_advances_in_place_without_reseeding(sandbox, monkeypatch):
    settings, fabric, service, published, seeded = sandbox
    clock = [1000000]
    monkeypatch.setattr(ci.time, "time", lambda: clock[0])
    advance = MagicMock()
    monkeypatch.setattr(ci, "advance_snapshot", advance)
    with fabric.client() as api:
        configured = ci.prepare(api, Credential(), service, settings, REPO)
        before = ci.read_marker(service, configured, "snapshot")
        clock[0] += ci.SNAPSHOT_MAX_AGE
        ci.prepare(api, Credential(), service, settings, REPO)
        after = ci.read_marker(service, configured, "snapshot")
        ci.prepare(api, Credential(), service, settings, REPO)
    assert before["id"] == after["id"]
    assert after["captured_at"] == clock[0]
    advance.assert_called_once()
    assert seeded == ["all"]
    assert len(published) == 1
    assert not any(method == "DELETE" for method, _ in fabric.requests)


def test_advancing_snapshot_uses_parent_connection_and_closes_it(sandbox, monkeypatch):
    settings = sandbox[0]
    api, connection = MagicMock(), MagicMock()
    api.get_warehouse.return_value = {
        "properties": {"connectionString": "test.sql.fabric.microsoft.com"}
    }
    connect = MagicMock(return_value=connection)
    monkeypatch.setattr(ci.pyodbc, "connect", connect)
    ci.advance_snapshot(api, Credential(), settings, "warehouse")
    assert "Database=TestWarehouse;" in connect.call_args.args[0]
    connection.cursor.return_value.execute.assert_called_once_with(
        "ALTER DATABASE [TestWarehouseSnapshot] SET TIMESTAMP = CURRENT_TIMESTAMP"
    )
    connection.close.assert_called_once()
