"""Deploy source-controlled Fabric item definitions."""

from collections.abc import Sequence
from os import environ

from fabric_cicd import FabricWorkspace, append_feature_flag, publish_all_items

from .credentials import get_credential
from .fabric_api import FabricApi
from .settings import SandboxSettings


def _selected_items(
    settings: SandboxSettings,
    items: Sequence[str] | None,
) -> list[str] | None:
    if items is None:
        return None
    selected = list(dict.fromkeys(items))
    available = {
        path.name
        for path in settings.workspace_definition_dir.iterdir()
        if path.is_dir() and "." in path.name
    }
    missing = sorted(set(selected) - available)
    if missing:
        raise ValueError(
            "Fabric item definitions were not found in the repository: "
            + ", ".join(missing)
        )
    return selected


def _workspace_id(settings: SandboxSettings, api: FabricApi) -> str:
    if settings.workspace_id:
        return settings.workspace_id
    matches = [
        workspace
        for workspace in api.list_workspaces(roles="Admin")
        if workspace.get("displayName") == settings.workspace_name
    ]
    if len(matches) != 1:
        raise RuntimeError(
            f"expected one Admin workspace named {settings.workspace_name!r}, "
            f"found {len(matches)}"
        )
    workspace_id = matches[0].get("id")
    if not isinstance(workspace_id, str) or not workspace_id:
        raise RuntimeError("the matching Fabric workspace has no valid ID")
    return workspace_id


def _item_id(
    deployed_items: Sequence[dict[str, object]],
    display_name: str,
    item_type: str,
) -> str:
    matches = [
        item
        for item in deployed_items
        if item.get("displayName") == display_name
        and item.get("type") == item_type
    ]
    if len(matches) != 1:
        raise RuntimeError(
            f"expected one {item_type} named {display_name!r}, "
            f"found {len(matches)}"
        )
    item_id = matches[0].get("id")
    if not isinstance(item_id, str) or not item_id:
        raise RuntimeError(f"the {item_type} {display_name!r} has no valid ID")
    return item_id


def deploy(
    settings: SandboxSettings,
    *,
    items: Sequence[str] | None = None,
) -> None:
    selected = _selected_items(settings, items)
    credential = get_credential()
    with FabricApi(credential) as api:
        workspace_id = _workspace_id(settings, api)
        deployed_items = (
            api.list_items(workspace_id)
            if not settings.lakehouse_id
            or not settings.non_schema_lakehouse_id
            else []
        )
    lakehouse_id = settings.lakehouse_id or _item_id(
        deployed_items,
        "TestLakehouse",
        "Lakehouse",
    )
    non_schema_lakehouse_id = settings.non_schema_lakehouse_id or _item_id(
        deployed_items,
        "TestLakehouseNoSchemas",
        "Lakehouse",
    )

    append_feature_flag("enable_environment_variable_replacement")
    if selected is not None:
        append_feature_flag("enable_experimental_features")
        append_feature_flag("enable_items_to_include")
    environ["$ENV:FABRIC_TEST_LAKEHOUSE_ID"] = lakehouse_id
    environ["$ENV:FABRIC_NON_SCHEMA_LAKEHOUSE_ID"] = non_schema_lakehouse_id
    environ["$ENV:FABRIC_SPARK_RUNTIME_LANE"] = settings.spark_runtime_lane
    environ["$ENV:FABRIC_SPARK_RUNTIME_VERSION"] = (
        settings.spark_runtime_version
    )
    item_types = settings.item_types
    if selected:
        selected_types = [item.rsplit(".", 1)[1] for item in selected]
        item_types = list(dict.fromkeys([*item_types, *selected_types]))
    workspace = FabricWorkspace(
        workspace_id=workspace_id,
        repository_directory=str(settings.workspace_definition_dir),
        environment=settings.environment,
        item_type_in_scope=item_types,
        token_credential=credential,
    )
    publish_all_items(workspace, items_to_include=selected)
