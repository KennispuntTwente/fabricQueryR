"""Remove CI workspaces left behind by interrupted integration runs."""

from __future__ import annotations

from typing import Any

from .credentials import get_credential
from .fabric_api import FabricApi


CI_WORKSPACE_PREFIX = "fabricqueryr-ci-"
CI_DESCRIPTION_PREFIX = "fabricqueryr-ci;"


def cleanup_ci_workspaces(*, confirm: bool = False) -> list[dict[str, Any]]:
    """Find, and optionally delete, only recognized CI sandbox workspaces."""
    with FabricApi(get_credential()) as api:
        candidates = [
            workspace
            for workspace in api.list_workspaces(roles="Admin")
            if workspace.get("type") == "Workspace"
            and workspace.get("displayName", "").startswith(CI_WORKSPACE_PREFIX)
            and workspace.get("description", "").startswith(
                CI_DESCRIPTION_PREFIX
            )
        ]
        for workspace in candidates:
            action = "deleting" if confirm else "would delete"
            print(
                f"{action}: {workspace['displayName']} "
                f"({workspace['id']})"
            )
            if confirm:
                api.delete_workspace(workspace["id"])
    return candidates
