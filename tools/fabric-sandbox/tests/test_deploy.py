from os import environ
from pathlib import Path
import re

from fabricqueryr_sandbox.deploy import deploy
from fabricqueryr_sandbox.settings import SandboxSettings


def test_seed_notebook_ids_are_parameterized():
    repository_root = Path(__file__).parents[3]
    notebook = (
        repository_root
        / "infra/fabric/workspace/SeedFixtures.Notebook/notebook-content.py"
    ).read_text()
    parameters = (
        repository_root / "infra/fabric/workspace/parameter.yml"
    ).read_text()

    for name in ("lakehouse_id", "workspace_id"):
        pattern = rf'{name}\s*=\s*"([0-9a-fA-F-]{{36}})"'
        assert pattern in parameters
        assert len(re.findall(pattern, notebook)) == 1
    assert "abfss://" in notebook
    assert '.option("replaceWhere", "category = \'B\'")' in notebook
    assert '"beta-updated"' in notebook


def test_deploy_binds_terraform_lakehouse_id(monkeypatch, tmp_path):
    settings = SandboxSettings(
        workspace_id="workspace-id",
        lakehouse_id="lakehouse-id",
        workspace_name="test",
        capacity_id=None,
        principal_id=None,
        environment="TEST",
        repository_root=tmp_path,
        manifest_path=tmp_path / "manifest.json",
    )
    (settings.workspace_definition_dir / "SeedFixtures.Notebook").mkdir(
        parents=True
    )
    flags = []
    workspaces = []
    published = []

    monkeypatch.setattr(
        "fabricqueryr_sandbox.deploy.append_feature_flag", flags.append
    )
    monkeypatch.setattr(
        "fabricqueryr_sandbox.deploy.FabricWorkspace",
        lambda **kwargs: workspaces.append(kwargs) or kwargs,
    )
    monkeypatch.setattr(
        "fabricqueryr_sandbox.deploy.publish_all_items", published.append
    )
    monkeypatch.setattr(
        "fabricqueryr_sandbox.deploy.get_credential", lambda: "credential"
    )

    deploy(settings)

    assert flags == ["enable_environment_variable_replacement"]
    assert environ["$ENV:FABRIC_TEST_LAKEHOUSE_ID"] == "lakehouse-id"
    assert workspaces[0]["item_type_in_scope"] == ["Notebook"]
    assert workspaces[0]["workspace_id"] == "workspace-id"
    assert published == [workspaces[0]]
