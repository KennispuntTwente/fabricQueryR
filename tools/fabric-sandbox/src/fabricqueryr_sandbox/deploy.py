"""Deploy source-controlled Fabric item definitions."""

from collections.abc import Sequence
from os import environ
import re
import sys
import time

from fabric_cicd import FabricWorkspace, append_feature_flag, publish_all_items
from fabric_cicd._common._exceptions import InvokeError, PublishError

from .credentials import get_credential
from .deployment_revision import record_deployments
from .fabric_api import FabricApi
from .settings import SandboxSettings


PUBLISH_ATTEMPTS = 3
TRANSIENT_PUBLISH_STATUSES = {408, 429, 500, 502, 503, 504}


def _publish_failure_status(error: Exception) -> int | None:
    # fabric-cicd 1.2 wraps the response in InvokeError's diagnostic text;
    # the public message omits its status. Never log the request/body text.
    if not isinstance(error, InvokeError):
        return None
    match = re.search(
        r"^Response Status: (\d{3})$", error.additional_info or "", re.MULTILINE,
    )
    return int(match[1]) if match else None


def _publish_with_retry(workspace_arguments: dict, selected: list[str] | None) -> None:
    for attempt in range(1, PUBLISH_ATTEMPTS + 1):
        try:
            # Re-discover on every attempt: a failed create may have succeeded
            # remotely. Publishing reconciles by name and updates that item.
            workspace = FabricWorkspace(**workspace_arguments)
            publish_all_items(workspace, items_to_include=selected)
            return
        except (InvokeError, PublishError) as error:
            failures = (
                error.errors if isinstance(error, PublishError)
                else [("workspace", error)]
            )
            statuses = [_publish_failure_status(failure) for _, failure in failures]
            print(
                f"Fabric publish failed; HTTP statuses: {statuses}",
                file=sys.stderr, flush=True,
            )
            if (
                attempt == PUBLISH_ATTEMPTS
                or not statuses
                or any(status not in TRANSIENT_PUBLISH_STATUSES for status in statuses)
            ):
                raise
            delay = 5 * attempt
            print(
                f"Retrying Fabric publication after {delay} seconds "
                f"(attempt {attempt + 1}/{PUBLISH_ATTEMPTS})",
                file=sys.stderr, flush=True,
            )
            time.sleep(delay)


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
    workspace_arguments = dict(
        workspace_id=workspace_id,
        repository_directory=str(settings.workspace_definition_dir),
        environment=settings.environment,
        item_type_in_scope=item_types,
        token_credential=credential,
    )
    _publish_with_retry(workspace_arguments, selected)
    record_deployments(
        settings, workspace_id, lakehouse_id, selected, credential=credential,
    )
