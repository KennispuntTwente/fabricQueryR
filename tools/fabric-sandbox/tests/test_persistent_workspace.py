import json

import httpx
import pytest
from fabricqueryr_sandbox import persistent_workspace as persistent
from fabricqueryr_sandbox.fabric_api import FabricApi
from test_shiny_sandbox import CAPACITY, OWNER, REPO, Credential


@pytest.mark.parametrize("sandbox", ["development", "shiny"])
def test_start_creates_once_and_reuses_the_same_owned_workspace(sandbox):
    requests, workspaces = [], []
    name = persistent.WORKSPACES[sandbox]

    def handle(request):
        requests.append(request)
        if request.method == "POST":
            assert request.url.path == "/v1/workspaces"
            workspaces.append({"id": "workspace", **json.loads(request.content)})
            return httpx.Response(201, json=workspaces[0])
        return httpx.Response(
            200,
            json={"value": workspaces}
            if request.url.path == "/v1/workspaces"
            else workspaces[0],
        )

    with FabricApi(Credential(), transport=httpx.MockTransport(handle)) as api:
        first = persistent.ensure_workspace(
            api, name, CAPACITY, REPO, OWNER, assign_capacity=True
        )
        second = persistent.ensure_workspace(
            api, name, CAPACITY, REPO, OWNER, assign_capacity=True
        )
    assert first == second
    assert first["displayName"] == name
    assert sum(r.method == "POST" for r in requests) == 1
    assert all(r.method != "DELETE" for r in requests)


@pytest.mark.parametrize("previous_capacity", [None, "old-trial"])
def test_owned_trial_workspace_moves_to_f2_without_recreation(previous_capacity):
    requests, waits = [], []
    workspace = {
        "id": "workspace",
        "displayName": persistent.WORKSPACES["development"],
        "description": persistent.workspace_marker(REPO, OWNER),
        "capacityId": previous_capacity,
    }

    def handle(request):
        requests.append(request)
        if request.url.path.endswith("/assignToCapacity"):
            assert request.method == "POST"
            assert json.loads(request.content) == {"capacityId": CAPACITY}
            return httpx.Response(202)
        if waits:
            workspace["capacityId"] = CAPACITY
        return httpx.Response(
            200,
            json={"value": [workspace]}
            if request.url.path == "/v1/workspaces"
            else workspace,
        )

    with FabricApi(
        Credential(), transport=httpx.MockTransport(handle), sleep=waits.append
    ) as api:
        result = persistent.ensure_workspace(
            api, workspace["displayName"], CAPACITY, REPO, OWNER, assign_capacity=True
        )
    assert result["id"] == "workspace" and result["capacityId"] == CAPACITY
    assert waits == [5]
    assert [(r.method, r.url.path) for r in requests if r.method != "GET"] == [
        ("POST", "/v1/workspaces/workspace/assignToCapacity")
    ]


@pytest.mark.parametrize("field", ["repo", "owner", "managed-by"])
def test_a_similarly_named_workspace_is_never_adopted_or_moved(field):
    marker = persistent.workspace_marker(REPO, OWNER)
    expected = {"repo": REPO, "owner": OWNER, "managed-by": persistent.MANAGER}
    workspace = {
        "id": "workspace",
        "displayName": persistent.WORKSPACES["development"],
        "description": marker.replace(
            f"{field}={expected[field]}", f"{field}=someone-else"
        ),
        "capacityId": "old-trial",
    }

    def handle(request):
        assert request.method == "GET"
        return httpx.Response(
            200,
            json={"value": [workspace]}
            if request.url.path == "/v1/workspaces"
            else workspace,
        )

    with (
        FabricApi(Credential(), transport=httpx.MockTransport(handle)) as api,
        pytest.raises(RuntimeError, match="ownership"),
    ):
        persistent.ensure_workspace(
            api,
            workspace["displayName"],
            CAPACITY,
            REPO,
            OWNER,
            assign_capacity=True,
        )


def test_assignment_timeout_does_not_retry_or_recreate_the_workspace():
    mutations = []
    workspace = {
        "id": "workspace",
        "displayName": persistent.WORKSPACES["development"],
        "description": persistent.workspace_marker(REPO, OWNER),
        "capacityId": "old-trial",
    }

    def handle(request):
        if request.method != "GET":
            mutations.append(request.url.path)
            return httpx.Response(202)
        return httpx.Response(
            200,
            json={"value": [workspace]}
            if request.url.path == "/v1/workspaces"
            else workspace,
        )

    with (
        FabricApi(
            Credential(), transport=httpx.MockTransport(handle), sleep=lambda _: None
        ) as api,
        pytest.raises(TimeoutError, match="not assigned"),
    ):
        persistent.ensure_workspace(
            api,
            workspace["displayName"],
            CAPACITY,
            REPO,
            OWNER,
            assign_capacity=True,
        )
    assert mutations == ["/v1/workspaces/workspace/assignToCapacity"]
