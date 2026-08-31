"""Remove CI workspaces left behind by interrupted integration runs."""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from typing import Any

from .credentials import get_credential
from .fabric_api import FabricApi


CI_WORKSPACE_PREFIX = "fabricqueryr-ci-"
CI_DESCRIPTION_PREFIX = "fabricqueryr-ci;"
PERSISTENT_DESCRIPTION_PREFIX = "fabricqueryr-persistent;"
DEFAULT_MINIMUM_AGE = timedelta(hours=6)


def parse_description(
    description: object,
    *,
    prefix: str,
    required_fields: set[str],
) -> dict[str, str] | None:
    """Parse a complete ownership marker from a workspace description."""
    if not isinstance(description, str) or not description.startswith(prefix):
        return None
    fields: dict[str, str] = {}
    for component in description[len(prefix) :].split(";"):
        key, separator, value = component.strip().partition("=")
        if separator and key and value:
            fields[key] = value
    if not required_fields.issubset(fields):
        return None
    return fields


def parse_ci_description(description: object) -> dict[str, str] | None:
    """Parse a complete ephemeral-CI ownership marker."""
    return parse_description(
        description,
        prefix=CI_DESCRIPTION_PREFIX,
        required_fields={"repo", "created", "run"},
    )


def parse_persistent_description(
    description: object,
) -> dict[str, str] | None:
    """Parse a complete persistent-sandbox ownership marker."""
    return parse_description(
        description,
        prefix=PERSISTENT_DESCRIPTION_PREFIX,
        required_fields={"repo", "owner", "managed-by", "rebuilt", "run"},
    )


def parse_created_at(value: str) -> datetime | None:
    """Return an aware UTC timestamp for a CI marker."""
    try:
        created = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if created.tzinfo is None:
        return None
    return created.astimezone(UTC)


def cleanup_ci_workspaces(
    *,
    confirm: bool = False,
    repository: str | None = None,
    now: datetime | None = None,
    minimum_age: timedelta = DEFAULT_MINIMUM_AGE,
) -> list[dict[str, Any]]:
    """Find, and optionally delete, only recognized CI sandbox workspaces."""
    repository = repository or os.environ.get("GITHUB_REPOSITORY")
    if not repository:
        raise RuntimeError(
            "Repository identity is required via GITHUB_REPOSITORY"
        )
    if minimum_age < timedelta(0):
        raise ValueError("minimum_age must not be negative")
    current_time = (now or datetime.now(UTC)).astimezone(UTC)

    def is_owned_stale_workspace(workspace: object) -> bool:
        if not isinstance(workspace, dict):
            return False
        workspace_id = workspace.get("id")
        display_name = workspace.get("displayName")
        if (
            not isinstance(workspace_id, str)
            or not workspace_id.strip()
            or not isinstance(display_name, str)
        ):
            return False
        marker = parse_ci_description(workspace.get("description"))
        if marker is None or marker["repo"].casefold() != repository.casefold():
            return False
        created = parse_created_at(marker["created"])
        return (
            workspace.get("type") == "Workspace"
            and display_name.startswith(CI_WORKSPACE_PREFIX)
            and created is not None
            and current_time - created >= minimum_age
        )

    with FabricApi(get_credential()) as api:
        candidates = [
            workspace
            for workspace in api.list_workspaces(roles="Admin")
            if is_owned_stale_workspace(workspace)
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


def remove_persistent_workspace(
    *,
    workspace_name: str,
    owner_id: str,
    managed_by: str,
    confirm: bool = False,
    repository: str | None = None,
) -> list[dict[str, Any]]:
    """Find, and optionally delete, one repository's persistent sandbox."""
    repository = repository or os.environ.get("GITHUB_REPOSITORY")
    if not repository:
        raise RuntimeError(
            "Repository identity is required via GITHUB_REPOSITORY"
        )
    if not workspace_name.strip():
        raise ValueError("workspace_name must not be empty")
    if not owner_id.strip():
        raise ValueError("owner_id must not be empty")
    if not managed_by.strip():
        raise ValueError("managed_by must not be empty")

    def is_owned_persistent_workspace(workspace: object) -> bool:
        if not isinstance(workspace, dict):
            return False
        workspace_id = workspace.get("id")
        display_name = workspace.get("displayName")
        if (
            not isinstance(workspace_id, str)
            or not workspace_id.strip()
            or not isinstance(display_name, str)
        ):
            return False
        marker = parse_persistent_description(workspace.get("description"))
        return (
            workspace.get("type") == "Workspace"
            and display_name == workspace_name
            and marker is not None
            and marker["repo"].casefold() == repository.casefold()
            and marker["owner"].casefold() == owner_id.casefold()
            and marker["managed-by"] == managed_by
        )

    with FabricApi(get_credential()) as api:
        candidates = [
            workspace
            for workspace in api.list_workspaces(roles="Admin")
            if is_owned_persistent_workspace(workspace)
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
