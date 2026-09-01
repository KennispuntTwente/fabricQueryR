import json
from dataclasses import replace
from pathlib import Path

import pytest

from fabricqueryr_sandbox.fixture_revision import (
    FIXTURE_REVISION_PATH,
    JOBS_FIXTURE_REVISION_PATH,
    ONELAKE_FIXTURE_REVISION_PATH,
    fixture_revision,
    read_fixture_contract,
    read_fixture_revision,
    verify_fixture_revision,
    write_fixture_revision,
)
from fabricqueryr_sandbox.settings import SandboxSettings


RUNTIME_CONTRACT = {
    "lane": "core",
    "fabric_runtime": "1.3",
    "spark_version": "3.5.5.5",
    "delta_version": "3.2.1",
}


class FakeDownload:
    def __init__(self, payload):
        self.payload = payload

    def readall(self):
        return self.payload


class FakeFile:
    def __init__(self):
        self.payload = b""

    def upload_data(self, payload, *, overwrite):
        assert overwrite is True
        self.payload = payload

    def download_file(self):
        return FakeDownload(self.payload)


class FakeFilesystem:
    def __init__(self):
        self.files = {}

    def get_file_client(self, path):
        return self.files.setdefault(path, FakeFile())


class FakeService:
    def __init__(self):
        self.filesystems = {}

    def get_file_system_client(self, workspace_id):
        return self.filesystems.setdefault(workspace_id, FakeFilesystem())


def make_settings(root: Path) -> SandboxSettings:
    workspace = root / "infra" / "fabric" / "workspace"
    seed_notebook = workspace / "SeedFixtures.Notebook" / "notebook-content.py"
    seed_notebook.parent.mkdir(parents=True)
    seed_notebook.write_text("seed-v1\n", encoding="utf-8")
    (workspace / "parameter.yml").write_text(
        "parameters-v1\n", encoding="utf-8"
    )
    deployed_files = {
        "JobFixtures.Notebook/notebook-content.py": "job-notebook-v1\n",
        "TestPipeline.DataPipeline/pipeline-content.json": "{}\n",
        "TestSparkJob.SparkJobDefinition/SparkJobDefinitionV1.json": "{}\n",
        "TestSparkJob.SparkJobDefinition/Main/main.py": "print('v1')\n",
        "TestEnvironment.Environment/Setting/Sparkcompute.yml": (
            "runtimeVersion: '1.3'\n"
        ),
        "Model.SemanticModel/definition.pbism": "{}\n",
        "Model.SemanticModel/model.bim": "{}\n",
    }
    for relative, content in deployed_files.items():
        path = workspace / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
    terraform = root / "infra" / "fabric" / "terraform" / "main.tf"
    terraform.parent.mkdir(parents=True)
    terraform.write_text("terraform-v1\n", encoding="utf-8")
    fixture = root / "infra" / "fabric" / "fixtures" / "basic.csv"
    fixture.parent.mkdir(parents=True)
    fixture.write_text("id,name\n1,alpha\n", encoding="utf-8")
    package = (
        root
        / "tools"
        / "fabric-sandbox"
        / "src"
        / "fabricqueryr_sandbox"
    )
    package.mkdir(parents=True)
    for name in (
        "deploy.py",
        "discover.py",
        "fixture_revision.py",
        "graphql_api.py",
        "kusto_api.py",
        "open_mirroring.py",
        "power_bi_api.py",
        "seed.py",
        "sql_api.py",
    ):
        (package / name).write_text(f"{name}-v1\n", encoding="utf-8")
    return SandboxSettings(
        workspace_id="workspace-id",
        lakehouse_id="lakehouse-id",
        workspace_name="fabricqueryr-test",
        capacity_id=None,
        principal_id=None,
        environment="TEST",
        repository_root=root,
        manifest_path=root / "manifest.json",
    )


def test_fixture_revision_changes_with_seed_inputs(tmp_path):
    settings = make_settings(tmp_path)
    first = fixture_revision(settings)

    fixture = settings.fixture_dir / "basic.csv"
    fixture.write_text("id,name\n1,beta\n", encoding="utf-8")
    second = fixture_revision(settings)

    assert len(first) == 64
    assert first != second


def test_fixture_revision_covers_runtime_and_deployment_contract(tmp_path):
    settings = make_settings(tmp_path)
    first = fixture_revision(settings, RUNTIME_CONTRACT)

    different_runtime = fixture_revision(
        replace(
            settings,
            spark_runtime_lane="preview",
            spark_runtime_version="2.0",
        ),
        {
            **RUNTIME_CONTRACT,
            "lane": "preview",
            "fabric_runtime": "2.0",
            "spark_version": "4.1.0.0",
            "delta_version": "4.2.0",
        },
    )
    assert different_runtime != first

    different_build = fixture_revision(
        settings,
        {**RUNTIME_CONTRACT, "spark_version": "3.5.6.0"},
    )
    assert different_build != first

    parameter_file = settings.workspace_definition_dir / "parameter.yml"
    parameter_file.write_text("parameters-v2\n", encoding="utf-8")
    different_parameters = fixture_revision(settings, RUNTIME_CONTRACT)
    assert different_parameters != first

    terraform_file = (
        settings.repository_root / "infra" / "fabric" / "terraform" / "main.tf"
    )
    terraform_file.write_text("terraform-v2\n", encoding="utf-8")
    different_terraform = fixture_revision(settings, RUNTIME_CONTRACT)
    assert different_terraform != different_parameters

    job_notebook = (
        settings.workspace_definition_dir
        / "JobFixtures.Notebook"
        / "notebook-content.py"
    )
    job_notebook.write_text("job-notebook-v2\n", encoding="utf-8")
    different_job_notebook = fixture_revision(settings, RUNTIME_CONTRACT)
    assert different_job_notebook != different_terraform

    graphql_api = (
        settings.repository_root
        / "tools/fabric-sandbox/src/fabricqueryr_sandbox/graphql_api.py"
    )
    graphql_api.write_text("graphql_api.py-v2\n", encoding="utf-8")
    assert fixture_revision(settings, RUNTIME_CONTRACT) != different_job_notebook


def test_jobs_fixture_revision_excludes_unrelated_services(tmp_path):
    settings = make_settings(tmp_path)
    first = fixture_revision(settings, RUNTIME_CONTRACT, scope="jobs")

    graphql_api = (
        settings.repository_root
        / "tools/fabric-sandbox/src/fabricqueryr_sandbox/graphql_api.py"
    )
    graphql_api.write_text("graphql-api-v2\n", encoding="utf-8")
    terraform = (
        settings.repository_root / "infra/fabric/terraform/main.tf"
    )
    terraform.write_text("terraform-v2\n", encoding="utf-8")
    assert fixture_revision(settings, RUNTIME_CONTRACT, scope="jobs") == first

    job_notebook = (
        settings.workspace_definition_dir
        / "JobFixtures.Notebook/notebook-content.py"
    )
    job_notebook.write_text("job-notebook-v2\n", encoding="utf-8")
    assert fixture_revision(settings, RUNTIME_CONTRACT, scope="jobs") != first


def test_onelake_fixture_revision_excludes_unrelated_services(tmp_path):
    settings = make_settings(tmp_path)
    first = fixture_revision(settings, RUNTIME_CONTRACT, scope="onelake")

    graphql_api = (
        settings.repository_root
        / "tools/fabric-sandbox/src/fabricqueryr_sandbox/graphql_api.py"
    )
    graphql_api.write_text("graphql-api-v2\n", encoding="utf-8")
    job_notebook = (
        settings.workspace_definition_dir
        / "JobFixtures.Notebook/notebook-content.py"
    )
    job_notebook.write_text("job-notebook-v2\n", encoding="utf-8")
    assert (
        fixture_revision(settings, RUNTIME_CONTRACT, scope="onelake")
        == first
    )

    seed_notebook = (
        settings.workspace_definition_dir
        / "SeedFixtures.Notebook/notebook-content.py"
    )
    seed_notebook.write_text("seed-v2\n", encoding="utf-8")
    assert (
        fixture_revision(settings, RUNTIME_CONTRACT, scope="onelake")
        != first
    )


def test_fixture_revision_round_trip_and_verification(tmp_path):
    settings = make_settings(tmp_path)
    service = FakeService()
    expected = fixture_revision(settings, RUNTIME_CONTRACT)

    write_fixture_revision(
        "workspace-id",
        "lakehouse-id",
        expected,
        runtime_contract=RUNTIME_CONTRACT,
        service_client=service,
    )

    assert (
        read_fixture_revision(
            "workspace-id",
            "lakehouse-id",
            service_client=service,
        )
        == expected
    )
    assert (
        read_fixture_contract(
            "workspace-id",
            "lakehouse-id",
            service_client=service,
        )
        == {"revision": expected, "runtime": RUNTIME_CONTRACT}
    )
    assert (
        verify_fixture_revision(
            settings,
            "workspace-id",
            "lakehouse-id",
            service_client=service,
        )
        == expected
    )
    marker = (
        service.filesystems["workspace-id"]
        .files[f"lakehouse-id/{FIXTURE_REVISION_PATH}"]
        .payload
    )
    assert json.loads(marker) == {
        "revision": expected,
        "runtime": RUNTIME_CONTRACT,
    }

    write_fixture_revision(
        "workspace-id",
        "lakehouse-id",
        "stale",
        runtime_contract=RUNTIME_CONTRACT,
        service_client=service,
    )
    with pytest.raises(RuntimeError, match="Rebuild or reseed"):
        verify_fixture_revision(
            settings,
            "workspace-id",
            "lakehouse-id",
            service_client=service,
        )


def test_jobs_fixture_revision_uses_an_independent_marker(tmp_path):
    settings = make_settings(tmp_path)
    service = FakeService()
    expected = fixture_revision(settings, RUNTIME_CONTRACT, scope="jobs")

    write_fixture_revision(
        "workspace-id",
        "lakehouse-id",
        expected,
        runtime_contract=RUNTIME_CONTRACT,
        service_client=service,
        scope="jobs",
    )

    assert read_fixture_revision(
        "workspace-id",
        "lakehouse-id",
        service_client=service,
        scope="jobs",
    ) == expected
    assert verify_fixture_revision(
        settings,
        "workspace-id",
        "lakehouse-id",
        service_client=service,
        scope="jobs",
    ) == expected
    files = service.filesystems["workspace-id"].files
    assert f"lakehouse-id/{JOBS_FIXTURE_REVISION_PATH}" in files
    assert f"lakehouse-id/{FIXTURE_REVISION_PATH}" not in files


def test_onelake_fixture_revision_uses_an_independent_marker(tmp_path):
    settings = make_settings(tmp_path)
    service = FakeService()
    expected = fixture_revision(settings, RUNTIME_CONTRACT, scope="onelake")

    write_fixture_revision(
        "workspace-id",
        "lakehouse-id",
        expected,
        runtime_contract=RUNTIME_CONTRACT,
        service_client=service,
        scope="onelake",
    )

    assert read_fixture_revision(
        "workspace-id",
        "lakehouse-id",
        service_client=service,
        scope="onelake",
    ) == expected
    assert verify_fixture_revision(
        settings,
        "workspace-id",
        "lakehouse-id",
        service_client=service,
        scope="onelake",
    ) == expected
    files = service.filesystems["workspace-id"].files
    assert f"lakehouse-id/{ONELAKE_FIXTURE_REVISION_PATH}" in files
    assert f"lakehouse-id/{FIXTURE_REVISION_PATH}" not in files
