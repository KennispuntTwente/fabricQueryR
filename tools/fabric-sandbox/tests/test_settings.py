from pathlib import Path

import pytest

from fabricqueryr_sandbox import SandboxSettings


def test_require_workspace_explains_terraform_contract(tmp_path):
    settings = SandboxSettings(
        workspace_id=None,
        lakehouse_id=None,
        workspace_name="test",
        capacity_id=None,
        principal_id=None,
        environment="TEST",
        repository_root=tmp_path,
        manifest_path=tmp_path / "manifest.json",
    )

    with pytest.raises(ValueError, match="Terraform workspace_id output"):
        settings.require_workspace()

    with pytest.raises(ValueError, match="Terraform lakehouse_id output"):
        settings.require_lakehouse()


def test_paths_are_derived_from_repository_root(tmp_path):
    settings = SandboxSettings(
        workspace_id="workspace-id",
        lakehouse_id="lakehouse-id",
        workspace_name="test",
        capacity_id=None,
        principal_id=None,
        environment="TEST",
        repository_root=tmp_path,
        manifest_path=Path("manifest.json"),
    )

    assert settings.workspace_definition_dir == tmp_path / "infra/fabric/workspace"
    assert settings.fixture_dir == tmp_path / "infra/fabric/fixtures"
    assert settings.item_types == ["Notebook"]