from contextlib import nullcontext
from unittest.mock import MagicMock

import httpx
import pytest
from fabricqueryr_sandbox import persistent_ci, persistent_workspace, shiny_sandbox
from fabricqueryr_sandbox import persistent_sandbox as interactive
from fabricqueryr_sandbox.fabric_api import FabricApi
from test_persistent_ci import (
    sandbox as sandbox,  # noqa: PLC0414 - register the shared pytest fixture
)
from test_shiny_sandbox import (
    CAPACITY,
    OWNER,
    REPO,
    Credential,
    FakeFabric,
    workspace,
)


@pytest.fixture
def development(sandbox, monkeypatch):
    settings, fabric, service, published, seeded = sandbox
    handle = fabric.handle
    sql_error = [None]

    def interactive_handle(request):
        path = request.url.path
        if path.endswith("/capacities"):
            return httpx.Response(
                200, json={"value": [{"id": CAPACITY, "sku": "F2", "state": "Active"}]}
            )
        if path.endswith("/roleAssignments"):
            return httpx.Response(
                200,
                json={
                    "value": [
                        {"id": "owner", "principal": {"id": OWNER}, "role": "Admin"}
                    ]
                },
            )
        if path.endswith("/sqlDatabases") and request.method == "POST" and sql_error[0]:
            return httpx.Response(400, json={"errorCode": sql_error[0]})
        return handle(request)

    fabric.handle = interactive_handle
    monkeypatch.setattr(
        interactive, "DataLakeServiceClient", lambda **_: nullcontext(service)
    )
    monkeypatch.setattr(interactive, "maintain", MagicMock())
    return settings, fabric, service, published, seeded, sql_error


def test_development_start_retains_full_fixtures_and_reuses_runtime_two_workspace(
    development,
):
    settings, fabric, _, published, seeded, _ = development
    with fabric.client() as api:
        first = interactive.prepare(
            api, Credential(), settings, REPO, OWNER, "development"
        )
        mutations = [r for r in fabric.requests if r[0] != "GET"]
        second = interactive.prepare(
            api, Credential(), settings, REPO, OWNER, "development"
        )
    assert first == second
    assert first["displayName"] == persistent_workspace.WORKSPACES["development"]
    assert [r for r in fabric.requests if r[0] != "GET"] == mutations
    assert seeded == ["all"] and len(published) == 1
    assert [c.kwargs for c in interactive.maintain.call_args_list] == [
        {},
        {"clean": False},
        {},
        {"clean": False},
    ]
    assert {i["type"] for i in fabric.items} >= {
        "SQLDatabase",
        "WarehouseSnapshot",
        "GraphQLApi",
        "KQLDatabase",
        "SemanticModel",
        "Notebook",
    }
    discovered = persistent_ci.discover.call_args.args[0]
    assert (discovered.spark_runtime_lane, discovered.spark_runtime_version) == (
        "preview",
        "2.0",
    )
    persistent_ci.discover_onelake.assert_not_called()


def test_reseed_refreshes_development_data_without_deleting_its_workspace(development):
    settings, fabric, _, _, seeded, _ = development
    with fabric.client() as api:
        first = interactive.prepare(
            api, Credential(), settings, REPO, OWNER, "development"
        )
        second = interactive.prepare(
            api, Credential(), settings, REPO, OWNER, "development", reseed=True
        )
    assert first == second and seeded == ["all", "all"]
    deletions = [p for m, p in fabric.requests if m == "DELETE"]
    assert len(deletions) == 1 and "/items/" in deletions[0]  # stale snapshot only


def test_only_sql_capacity_limit_omits_the_optional_database_and_reuses_other_fixtures(
    development,
):
    settings, fabric, _, published, seeded, errors = development
    errors[0] = "SqlDatabasePerCapacityLimitReached"
    with fabric.client() as api:
        interactive.prepare(api, Credential(), settings, REPO, OWNER, "development")
        interactive.prepare(api, Credential(), settings, REPO, OWNER, "development")
    assert seeded == ["all"] and len(published) == 1
    assert not any(i["type"] == "SQLDatabase" for i in fabric.items)
    assert persistent_ci.discover.call_args.args[0].provision_sql_database is False


def test_other_sql_errors_are_reported_without_reseeding_or_deleting(development):
    settings, fabric, _, _, seeded, errors = development
    errors[0] = "Forbidden"
    with (
        fabric.client() as api,
        pytest.raises(httpx.HTTPStatusError, match="Forbidden"),
    ):
        interactive.prepare(api, Credential(), settings, REPO, OWNER, "development")
    assert seeded == []
    assert all(method != "DELETE" for method, _ in fabric.requests)


@pytest.mark.parametrize("reseed", [False, True])
def test_shiny_uses_its_existing_lightweight_fixtures(sandbox, monkeypatch, reseed):
    settings, *_ = sandbox
    items = [
        {"id": n, "displayName": n, "type": t}
        for n, t in shiny_sandbox.REQUIRED_ITEMS.items()
    ]
    api = FakeFabric(workspace(ready=True), items)
    seed = MagicMock()
    monkeypatch.setattr(shiny_sandbox, "seed_targets", seed)
    monkeypatch.setattr(shiny_sandbox, "ensure_targets", lambda *_: {})
    reconcile = MagicMock(
        side_effect=AssertionError("Shiny must not deploy the full development suite")
    )
    monkeypatch.setattr(persistent_ci, "reconcile", reconcile)
    maintain = MagicMock()
    monkeypatch.setattr(interactive, "maintain", maintain)
    result = interactive.prepare(
        api, Credential(), settings, REPO, OWNER, "shiny", reseed=reseed
    )
    assert result["displayName"] == shiny_sandbox.WORKSPACE_NAME
    assert seed.call_count == int(reseed)
    assert [c.kwargs for c in maintain.call_args_list] == [{}, {"clean": False}]
    assert all(method != "DELETE" for method, *_ in api.requests)


@pytest.mark.parametrize("selected", ["development", "shiny"])
@pytest.mark.parametrize("exists", [False, True])
def test_status_cli_only_reads_metadata_even_without_an_active_capacity(
    monkeypatch, capsys, selected, exists
):
    name = persistent_workspace.WORKSPACES[selected]
    existing = {
        "id": "workspace",
        "displayName": name,
        "description": persistent_workspace.workspace_marker(REPO, OWNER),
        "capacityId": "old-trial",
    }
    requests = []

    def handle(request):
        requests.append(request)
        assert request.method == "GET"
        payload = (
            {"value": [existing] if exists else []}
            if request.url.path == "/v1/workspaces"
            else existing
        )
        return httpx.Response(200, json=payload)

    monkeypatch.setattr(interactive, "get_credential", Credential)
    monkeypatch.setattr(
        interactive,
        "FabricApi",
        lambda _: FabricApi(Credential(), transport=httpx.MockTransport(handle)),
    )
    interactive.main(
        [
            "status",
            "--sandbox",
            selected,
            "--capacity-id",
            CAPACITY,
            "--owner-id",
            OWNER,
            "--repository",
            REPO,
        ]
    )
    message = capsys.readouterr().out
    assert name in message
    assert ("Start will assign" if exists else "has not been created") in message
    assert len(requests) == (2 if exists else 1)
