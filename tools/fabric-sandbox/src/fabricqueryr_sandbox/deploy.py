"""Deploy source-controlled Fabric item definitions."""

from os import environ

from fabric_cicd import FabricWorkspace, append_feature_flag, publish_all_items

from .credentials import get_credential
from .settings import SandboxSettings


def deploy(settings: SandboxSettings) -> None:
    append_feature_flag("enable_environment_variable_replacement")
    environ["$ENV:FABRIC_TEST_LAKEHOUSE_ID"] = settings.require_lakehouse()
    workspace = FabricWorkspace(
        workspace_id=settings.require_workspace(),
        repository_directory=str(settings.workspace_definition_dir),
        environment=settings.environment,
        item_type_in_scope=settings.item_types,
        token_credential=get_credential(),
    )
    publish_all_items(workspace)