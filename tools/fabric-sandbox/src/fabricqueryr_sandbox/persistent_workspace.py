"""Ownership and capacity assignment for the two interactive sandboxes."""

from __future__ import annotations

import os
from datetime import datetime, timezone

from .cleanup import parse_persistent_description

MANAGER = ".github/workflows/fabric-sandbox.yaml"
WORKSPACES = {
    "development": "fabricqueryr-dev-dhrkoning",
    "shiny": "fabricqueryr-shiny-dhrkoning",
}


def workspace_marker(repository, owner):
    timestamp = datetime.now(timezone.utc).isoformat()
    run = os.environ.get("GITHUB_RUN_ID", "local")
    return (
        f"fabricqueryr-persistent; repo={repository}; owner={owner}; "
        f"managed-by={MANAGER}; rebuilt={timestamp}; run={run}"
    )


def find_workspace(api, name, repository, owner):
    matches = [w for w in api.list_workspaces() if w.get("displayName") == name]
    if len(matches) > 1:
        raise RuntimeError(f"Ambiguous persistent workspace: {name}")
    if not matches:
        return None
    workspace = api.request("GET", f"/workspaces/{matches[0]['id']}").json()
    marker = parse_persistent_description(workspace.get("description"))
    expected = {"repo": repository, "owner": owner, "managed-by": MANAGER}
    if not marker or any(
        marker[key].casefold() != value.casefold() for key, value in expected.items()
    ):
        raise RuntimeError("Refusing workspace without matching persistent ownership")
    return workspace


def ensure_workspace(
    api, name, capacity_id, repository, owner, *, assign_capacity=False
):
    workspace = find_workspace(api, name, repository, owner)
    if workspace is None:
        return api.request(
            "POST",
            "/workspaces",
            json={
                "displayName": name,
                "capacityId": capacity_id,
                "description": workspace_marker(repository, owner),
            },
        ).json()
    if (workspace.get("capacityId") or "").casefold() == capacity_id.casefold():
        return workspace
    if not assign_capacity:
        raise RuntimeError("Persistent workspace is not assigned to the configured F2")
    # Preserve the owned workspace and its items when moving off the old trial.
    # This endpoint returns 202 without an operation URL; poll the workspace.
    api.request(
        "POST",
        f"/workspaces/{workspace['id']}/assignToCapacity",
        json={"capacityId": capacity_id},
    )
    for _ in range(60):
        workspace = api.request("GET", f"/workspaces/{workspace['id']}").json()
        if (workspace.get("capacityId") or "").casefold() == capacity_id.casefold():
            return workspace
        api.sleep(5)
    raise TimeoutError("Persistent workspace was not assigned to the configured F2")
