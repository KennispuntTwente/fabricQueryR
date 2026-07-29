from os import environ
from pathlib import Path
import json
import re

from azure.core.credentials import AccessToken
from fabric_cicd import FabricWorkspace

from fabricqueryr_sandbox.deploy import deploy
from fabricqueryr_sandbox.settings import SandboxSettings


class StaticCredential:
    def get_token(self, *_scopes, **_kwargs):
        return AccessToken("test-token", 4_102_444_800)


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
    assert '.saveAsTable("dbo.fabricqueryr_empty")' in notebook
    assert '.saveAsTable("dbo.fabricqueryr_typed_partitions")' in notebook
    partition_start = notebook.index(
        ".partitionBy(",
        notebook.index('stage = "write typed and null partition Delta table"'),
    )
    partition_end = notebook.index(".option(", partition_start)
    partition_block = notebook[partition_start:partition_end]
    for column in (
        "event_date",
        "active",
        "integer_part",
        "decimal_part",
        "timestamp_part",
        "timestamp_ntz_part",
        "binary_part",
    ):
        assert f'"{column}"' in partition_block


def test_seed_notebook_uses_valid_nested_delta_column_rename_syntax():
    repository_root = Path(__file__).parents[3]
    notebook = (
        repository_root
        / "infra/fabric/workspace/SeedFixtures.Notebook/notebook-content.py"
    ).read_text()

    valid_rename = "RENAME COLUMN profile.label TO display_label"
    assert notebook.count(valid_rename) == 2
    assert "RENAME COLUMN profile.label TO profile.display_label" not in notebook


def test_job_notebook_exposes_deterministic_job_modes():
    repository_root = Path(__file__).parents[3]
    notebook = (
        repository_root
        / "infra/fabric/workspace/JobFixtures.Notebook/notebook-content.py"
    ).read_text()

    assert notebook.count("# PARAMETERS CELL ********************") == 1
    assert notebook.index("# PARAMETERS CELL ********************") < notebook.index(
        'mode = "success"'
    )
    assert 'mode = "success"' in notebook
    assert 'if mode == "failure":' in notebook
    assert 'if mode == "slow":' in notebook
    assert "FABRICQUERYR_INTENTIONAL_JOB_FAILURE" in notebook
    assert "fabricqueryr-job-success:" in notebook


def test_livy_batch_fixture_persists_each_executed_mode():
    repository_root = Path(__file__).parents[3]
    fixture = (
        repository_root / "infra/fabric/fixtures/livy_batch.py"
    ).read_text()

    assert '.saveAsTable("dbo.fabricqueryr_livy_batch_result")' in fixture
    assert "write_marker(mode, row_count)" in fixture
    assert fixture.count("write_marker(mode, -1)") == 2
    assert fixture.index("write_marker(mode, -1)") < fixture.index(
        'raise RuntimeError("FABRICQUERYR_INTENTIONAL_BATCH_FAILURE")'
    )
    assert fixture.rindex("write_marker(mode, -1)") < fixture.index(
        'print("FABRICQUERYR_BATCH_READY_FOR_CANCELLATION"'
    )


def test_pipeline_and_spark_job_fixtures_are_deployable():
    repository_root = Path(__file__).parents[3]
    workspace = repository_root / "infra/fabric/workspace"
    pipeline = (
        workspace / "TestPipeline.DataPipeline/pipeline-content.json"
    ).read_text()
    spark_definition_text = (
        workspace
        / "TestSparkJob.SparkJobDefinition/SparkJobDefinitionV1.json"
    ).read_text()
    spark_definition = json.loads(spark_definition_text)
    spark_main = (
        workspace / "TestSparkJob.SparkJobDefinition/Main/main.py"
    ).read_text()
    parameters = (workspace / "parameter.yml").read_text()

    assert '"type": "Wait"' in pipeline
    assert '"waitTimeInSeconds": 1' in pipeline
    assert spark_definition["executableFile"] == "main.py"
    assert spark_definition["defaultLakehouseArtifactId"]
    assert spark_definition["retryPolicy"] == {
        "policyType": "SimpleRetry",
        "policyProperties": {
            "retryCount": 1,
            "intervalBetweenRetriesInSeconds": 30,
        },
    }
    assert 'saveAsTable("dbo.fabricqueryr_spark_job_result")' in spark_main
    assert "SELECT COUNT(*) FROM dbo.fabricqueryr_basic" in spark_main
    assert "fabricqueryr-spark-job-success:" in spark_main
    assert 'item_type: "SparkJobDefinition"' in parameters
    assert '"defaultLakehouseArtifactId"' in parameters


def test_workspace_repository_is_discoverable_by_fabric_cicd(monkeypatch):
    repository_root = Path(__file__).parents[3]
    workspace_directory = repository_root / "infra/fabric/workspace"
    monkeypatch.setenv(
        "$ENV:FABRIC_TEST_LAKEHOUSE_ID",
        "00000000-0000-0000-0000-000000000001",
    )

    workspace = FabricWorkspace(
        workspace_id="00000000-0000-0000-0000-000000000002",
        repository_directory=str(workspace_directory),
        environment="TEST",
        item_type_in_scope=[
            "Notebook",
            "DataPipeline",
            "SparkJobDefinition",
        ],
        token_credential=StaticCredential(),
    )
    workspace._refresh_repository_items()

    assert set(workspace.repository_items["Notebook"]) == {
        "JobFixtures",
        "SeedFixtures",
    }
    assert set(workspace.repository_items["DataPipeline"]) == {
        "TestPipeline",
    }
    assert set(workspace.repository_items["SparkJobDefinition"]) == {
        "TestSparkJob",
    }


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
    assert workspaces[0]["item_type_in_scope"] == [
        "Notebook",
        "DataPipeline",
        "SparkJobDefinition",
        "SemanticModel",
    ]
    assert workspaces[0]["workspace_id"] == "workspace-id"
    assert published == [workspaces[0]]
