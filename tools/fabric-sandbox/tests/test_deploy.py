from os import environ
from pathlib import Path
import json
import logging
import re
from types import SimpleNamespace

from azure.core.credentials import AccessToken
import fabric_cicd.constants as fabric_cicd_constants
from fabric_cicd import FabricWorkspace, append_feature_flag
from fabric_cicd._items._environment import _process_environment_file
from fabric_cicd._common._exceptions import InvokeError, PublishError
from fabric_cicd._common._fabric_endpoint import _format_invoke_log
import pytest
import requests

from fabricqueryr_sandbox.deploy import _publish_with_retry, deploy
from fabricqueryr_sandbox.settings import SandboxSettings


class StaticCredential:
    def get_token(self, *_scopes, **_kwargs):
        return AccessToken("test-token", 4_102_444_800)


def publish_error(status):
    response = requests.Response()
    response.status_code = status
    response._content = b'{"message": "An error occurred while processing the operation"}'
    response.headers["Content-Type"] = "application/json"
    diagnostic = _format_invoke_log(
        response, "POST", "https://api.powerbi.com/v1/workspaces/test/items",
        {"private": "do-not-log-request-content"},
    )
    logger = logging.getLogger(__name__)
    return PublishError(
        [("TestPipeline", InvokeError("Publish failed", logger, diagnostic))],
        logger,
    )


@pytest.mark.parametrize("status", [408, 429, 500, 502, 503, 504])
def test_publish_reconciles_with_fresh_workspace_after_transient_failure(
    monkeypatch, capsys, status,
):
    workspaces = []
    publications = []
    delays = []
    def workspace(**kwargs):
        value = SimpleNamespace(**kwargs)
        workspaces.append(value)
        return value
    def publish(value, **kwargs):
        publications.append((value, kwargs))
        if len(publications) == 1:
            raise publish_error(status)
    monkeypatch.setattr("fabricqueryr_sandbox.deploy.FabricWorkspace", workspace)
    monkeypatch.setattr("fabricqueryr_sandbox.deploy.publish_all_items", publish)
    monkeypatch.setattr("fabricqueryr_sandbox.deploy.time.sleep", delays.append)

    _publish_with_retry({"workspace_id": "test"}, ["TestPipeline.DataPipeline"])

    assert len(workspaces) == 2
    assert workspaces[0] is not workspaces[1]
    assert [call[1] for call in publications] == [
        {"items_to_include": ["TestPipeline.DataPipeline"]},
    ] * 2
    assert delays == [5]
    output = capsys.readouterr().err
    assert str(status) in output
    assert "do-not-log-request-content" not in output


@pytest.mark.parametrize("status,attempts", [(400, 1), (401, 1), (403, 1), (404, 1), (503, 3)])
def test_publish_preserves_permanent_or_exhausted_failure(monkeypatch, status, attempts):
    error = publish_error(status)
    publications = []
    delays = []
    def publish(*args, **kwargs):
        publications.append(kwargs)
        raise error
    monkeypatch.setattr("fabricqueryr_sandbox.deploy.FabricWorkspace", lambda **kwargs: kwargs)
    monkeypatch.setattr("fabricqueryr_sandbox.deploy.publish_all_items", publish)
    monkeypatch.setattr("fabricqueryr_sandbox.deploy.time.sleep", delays.append)

    with pytest.raises(PublishError) as caught:
        _publish_with_retry({}, None)

    assert caught.value is error
    assert publications == [{"items_to_include": None}] * attempts
    assert delays == [5, 10][:attempts - 1]


def test_publish_does_not_retry_unknown_or_mixed_failures(monkeypatch):
    logger = logging.getLogger(__name__)
    errors = [
        PublishError([("TestPipeline", InvokeError("Unknown", logger))], logger),
        PublishError(publish_error(503).errors + publish_error(400).errors, logger),
        PublishError([], logger),
    ]
    publications = []
    monkeypatch.setattr("fabricqueryr_sandbox.deploy.FabricWorkspace", lambda **kwargs: kwargs)
    def publish(*args, **kwargs):
        publications.append(kwargs)
        raise error
    monkeypatch.setattr("fabricqueryr_sandbox.deploy.publish_all_items", publish)
    for error in errors:
        with pytest.raises(PublishError) as caught:
            _publish_with_retry({}, None)
        assert caught.value is error
    assert len(publications) == len(errors)


@pytest.fixture(autouse=True)
def recorded_deployments(monkeypatch):
    records = []
    monkeypatch.setattr(
        "fabricqueryr_sandbox.deploy.record_deployments",
        lambda *args, **kwargs: records.append((args, kwargs)),
    )
    return records


def test_seed_notebook_ids_are_parameterized():
    repository_root = Path(__file__).parents[3]
    notebook = (
        repository_root
        / "infra/fabric/workspace/SeedFixtures.Notebook/notebook-content.py"
    ).read_text()
    parameters = (
        repository_root / "infra/fabric/workspace/parameter.yml"
    ).read_text()

    for name in ("lakehouse_id", "non_schema_lakehouse_id", "workspace_id"):
        pattern = rf'(?m)^{name}\s*=\s*"([0-9a-fA-F-]{{36}})"'
        assert pattern in parameters
        assert len(re.findall(pattern, notebook)) == 1
    assert "abfss://" in notebook
    assert '"1.3": ("3.5.", "3.2.")' in notebook
    assert '"2.0": ("4.1.", "4.2.")' in notebook
    assert 'if runtime_lane == "core":' in notebook
    assert "mssparkutils.notebook.exit(success_value)" in notebook
    core_exit = notebook.index('if runtime_lane == "core":')
    assert notebook.rfind("except Exception:", 0, core_exit) > 0
    assert notebook.index("try:", core_exit) > core_exit
    assert 'runtime_lane\\s*=\\s*"(core|runtime2|preview)"' in parameters
    assert 'expected_runtime_version\\s*=\\s*"(1\\.3|2\\.0)"' in parameters
    assert '.saveAsTable("dbo.fabricqueryr_runtime")' in notebook
    assert '.option("replaceWhere", "category = \'B\'")' in notebook
    assert '"beta-updated"' in notebook
    assert '.saveAsTable("dbo.fabricqueryr_empty")' in notebook
    assert '.saveAsTable("dbo.fabricqueryr_typed_partitions")' in notebook
    assert (
        'stage = "write protocol-valid binary partition edge-case Delta table"'
        in notebook
    )
    assert '.saveAsTable("dbo.fabricqueryr_binary_partitions")' not in notebook
    assert '"partitionValues": {"binary_part": binary_value}' in notebook
    assert '(1, "\\u0000")' in notebook
    assert '(2, "\\u0080")' in notebook
    assert '(3, "\\u00ff")' in notebook
    assert "AddFile.partitionValues is authoritative" in notebook
    assert "00000000000000000000.json" in notebook
    assert "ADD COLUMNS (profile.metadata_only STRING)" in notebook
    assert "dbo.fabricqueryr_struct_validity" in notebook
    assert "dbo.fabricqueryr_file_row_number_collision" in notebook
    assert "'delta.enableRowTracking' = 'true'" in notebook
    assert '"_metadata.row_id"' in notebook
    assert '"_metadata.row_commit_version"' in notebook
    assert '"canonicalization": "spark-logical-v1"' in notebook
    assert '"format_version": 2' in notebook
    assert "def canonical_spark_value(column, data_type):" in notebook
    assert '"rows": rows' in notebook
    assert '"schema": without_delta_metadata(' in notebook
    assert '"key_values"' not in notebook
    assert (
        '.saveAsTable("dbo.fabricqueryr_oracle_typed_partitions")'
        in notebook
    )
    assert "oracle_typed_partitions = typed_partitions.withColumn(" in notebook
    assert 'F.lit("0.50").cast("decimal(8,2)")' in notebook
    oracle_partition_start = notebook.index(
        "oracle_typed_partitions.write.format",
    )
    oracle_partition_end = notebook.index(
        '.saveAsTable("dbo.fabricqueryr_oracle_typed_partitions")',
        oracle_partition_start,
    )
    assert (
        "\n        typed_partitions.write.format"
        not in notebook[oracle_partition_start:oracle_partition_end]
    )
    assert (
        "TBLPROPERTIES ('delta.enableDeletionVectors' = 'false')"
        in notebook
    )
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
    assert notebook.count(valid_rename) == 3
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
    assert 'run_id = sys.argv[2] if len(sys.argv) > 2 else ""' in fixture
    assert '"mode string, row_count long, run_id string"' in fixture
    assert '.option("overwriteSchema", "true")' in fixture
    assert "write_marker(mode, row_count)" in fixture
    assert fixture.count("write_marker(mode, -1)") == 2
    assert fixture.index("write_marker(mode, -1)") < fixture.index(
        'raise RuntimeError("FABRICQUERYR_INTENTIONAL_BATCH_FAILURE")'
    )
    assert fixture.rindex("write_marker(mode, -1)") < fixture.index(
        'print("FABRICQUERYR_BATCH_READY_FOR_CANCELLATION"'
    )


def test_job_notebook_reports_observed_configuration(monkeypatch):
    notebook = (
        Path(__file__).parents[3]
        / "infra/fabric/workspace/JobFixtures.Notebook/notebook-content.py"
    ).read_text()
    cell = notebook.split("# CELL ********************", 1)[1]
    class NotebookExit(Exception):
        pass
    def exit_notebook(value):
        raise NotebookExit(value)
    monkeypatch.setitem(__import__("sys").modules, "notebookutils", SimpleNamespace(
        notebook=SimpleNamespace(exit=exit_notebook),
        runtime=SimpleNamespace(context={"defaultLakehouseId": "actual-lakehouse"}),
    ))
    conf = {"spark.sql.shuffle.partitions": "2", "spark.sql.broadcastTimeout": "301"}
    tables = []
    def table(name):
        tables.append(name)
        return SimpleNamespace(count=lambda: 3)
    with pytest.raises(NotebookExit) as result:
        exec(compile(cell, "JobFixtures.Notebook", "exec"), {
            "mode": "configuration", "marker": "probe",
            "spark": SimpleNamespace(conf=conf, table=table),
        })
    assert json.loads(str(result.value)) == {
        "marker": "probe", "shuffle_partitions": "2",
        "environment_broadcast_timeout": "301", "lakehouse_id": "actual-lakehouse",
        "row_count": 3,
    }
    assert tables == ["dbo.fabricqueryr_basic"]


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


def test_environment_fixture_tracks_the_selected_runtime():
    repository_root = Path(__file__).parents[3]
    workspace = repository_root / "infra/fabric/workspace"
    spark_compute = (
        workspace / "TestEnvironment.Environment/Setting/Sparkcompute.yml"
    ).read_text()
    parameters = (workspace / "parameter.yml").read_text()

    assert "instance_pool_id: null" in spark_compute
    assert "runtime_version: 1.3" in spark_compute
    assert "enable_native_execution_engine: false" in spark_compute
    assert 'item_type: "Environment"' in parameters
    assert 'item_name: "TestEnvironment"' in parameters
    assert '"$ENV:FABRIC_SPARK_RUNTIME_VERSION"' in parameters


def test_workspace_repository_is_discoverable_by_fabric_cicd(monkeypatch):
    repository_root = Path(__file__).parents[3]
    workspace_directory = repository_root / "infra/fabric/workspace"
    monkeypatch.setenv(
        "$ENV:FABRIC_TEST_LAKEHOUSE_ID",
        "00000000-0000-0000-0000-000000000001",
    )
    monkeypatch.setenv(
        "$ENV:FABRIC_NON_SCHEMA_LAKEHOUSE_ID",
        "00000000-0000-0000-0000-000000000003",
    )
    monkeypatch.setenv("$ENV:FABRIC_SPARK_RUNTIME_VERSION", "2.0")
    monkeypatch.setattr(fabric_cicd_constants, "FEATURE_FLAG", set())
    append_feature_flag("enable_environment_variable_replacement")

    workspace = FabricWorkspace(
        workspace_id="00000000-0000-0000-0000-000000000002",
        repository_directory=str(workspace_directory),
        environment="TEST",
        item_type_in_scope=[
            "Notebook",
            "DataPipeline",
            "SparkJobDefinition",
            "Environment",
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
    assert set(workspace.repository_items["Environment"]) == {
        "TestEnvironment",
    }
    environment = workspace.repository_items["Environment"]["TestEnvironment"]
    spark_compute = next(
        file
        for file in environment.item_files
        if file.relative_path == "Setting/Sparkcompute.yml"
    )
    original_contents = spark_compute.contents
    try:
        spark_compute.contents = _process_environment_file(
            workspace,
            environment,
            spark_compute,
        )
        rendered = workspace._replace_parameters(spark_compute, environment)
    finally:
        spark_compute.contents = original_contents
    assert "runtime_version: 2.0" in rendered
    assert "runtime_version: 1.3" not in rendered


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
        non_schema_lakehouse_id="non-schema-lakehouse-id",
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
        "fabricqueryr_sandbox.deploy.publish_all_items",
        lambda workspace, **kwargs: published.append((workspace, kwargs)),
    )
    monkeypatch.setattr(
        "fabricqueryr_sandbox.deploy.get_credential", lambda: "credential"
    )

    deploy(settings)

    assert flags == ["enable_environment_variable_replacement"]
    assert environ["$ENV:FABRIC_TEST_LAKEHOUSE_ID"] == "lakehouse-id"
    assert (
        environ["$ENV:FABRIC_NON_SCHEMA_LAKEHOUSE_ID"]
        == "non-schema-lakehouse-id"
    )
    assert workspaces[0]["item_type_in_scope"] == [
        "Notebook",
        "DataPipeline",
        "SparkJobDefinition",
        "SemanticModel",
        "Environment",
    ]
    assert workspaces[0]["workspace_id"] == "workspace-id"
    assert published == [(workspaces[0], {"items_to_include": None})]


def test_deploy_selects_exact_repository_items(monkeypatch, tmp_path, recorded_deployments):
    settings = SandboxSettings(
        workspace_id="workspace-id",
        lakehouse_id="lakehouse-id",
        workspace_name="test",
        capacity_id=None,
        principal_id=None,
        environment="TEST",
        repository_root=tmp_path,
        manifest_path=tmp_path / "manifest.json",
        non_schema_lakehouse_id="non-schema-lakehouse-id",
    )
    (settings.workspace_definition_dir / "JobFixtures.Notebook").mkdir(
        parents=True
    )
    (settings.workspace_definition_dir / "TestPipeline.DataPipeline").mkdir()
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
        "fabricqueryr_sandbox.deploy.publish_all_items",
        lambda workspace, **kwargs: published.append((workspace, kwargs)),
    )
    monkeypatch.setattr(
        "fabricqueryr_sandbox.deploy.get_credential", lambda: "credential"
    )

    deploy(
        settings,
        items=["JobFixtures.Notebook", "TestPipeline.DataPipeline"],
    )

    assert flags == [
        "enable_environment_variable_replacement",
        "enable_experimental_features",
        "enable_items_to_include",
    ]
    assert workspaces[0]["item_type_in_scope"] == [
        "Notebook",
        "DataPipeline",
        "SparkJobDefinition",
        "SemanticModel",
        "Environment",
    ]
    assert published == [
        (
            workspaces[0],
            {
                "items_to_include": [
                    "JobFixtures.Notebook",
                    "TestPipeline.DataPipeline",
                ]
            },
        )
    ]
    assert recorded_deployments[0][0] == (
        settings, "workspace-id", "lakehouse-id",
        ["JobFixtures.Notebook", "TestPipeline.DataPipeline"],
    )

    recorded_deployments.clear()
    def fail_publish(*args, **kwargs):
        raise RuntimeError("publication failed")
    monkeypatch.setattr("fabricqueryr_sandbox.deploy.publish_all_items", fail_publish)
    with pytest.raises(RuntimeError, match="publication failed"):
        deploy(settings, items=["TestPipeline.DataPipeline"])
    assert recorded_deployments == []


def test_deploy_resolves_persistent_targets_by_name(monkeypatch, tmp_path):
    settings = SandboxSettings(
        workspace_id=None,
        lakehouse_id=None,
        workspace_name="fabricqueryr-dev-owner",
        capacity_id=None,
        principal_id=None,
        environment="TEST",
        repository_root=tmp_path,
        manifest_path=tmp_path / "manifest.json",
        non_schema_lakehouse_id=None,
    )
    (settings.workspace_definition_dir / "TestPipeline.DataPipeline").mkdir(
        parents=True
    )

    class FakeFabricApi:
        def __init__(self, credential):
            assert credential == "credential"

        def __enter__(self):
            return self

        def __exit__(self, *_):
            return None

        def list_workspaces(self, *, roles):
            assert roles == "Admin"
            return [
                {
                    "id": "workspace-id",
                    "displayName": "fabricqueryr-dev-owner",
                }
            ]

        def list_items(self, workspace_id):
            assert workspace_id == "workspace-id"
            return [
                {
                    "id": "lakehouse-id",
                    "displayName": "TestLakehouse",
                    "type": "Lakehouse",
                },
                {
                    "id": "non-schema-lakehouse-id",
                    "displayName": "TestLakehouseNoSchemas",
                    "type": "Lakehouse",
                },
            ]

    workspaces = []
    monkeypatch.setattr("fabricqueryr_sandbox.deploy.FabricApi", FakeFabricApi)
    monkeypatch.setattr(
        "fabricqueryr_sandbox.deploy.get_credential", lambda: "credential"
    )
    monkeypatch.setattr(
        "fabricqueryr_sandbox.deploy.append_feature_flag", lambda _: None
    )
    monkeypatch.setattr(
        "fabricqueryr_sandbox.deploy.FabricWorkspace",
        lambda **kwargs: workspaces.append(kwargs) or kwargs,
    )
    monkeypatch.setattr(
        "fabricqueryr_sandbox.deploy.publish_all_items", lambda *_, **__: None
    )

    deploy(settings, items=["TestPipeline.DataPipeline"])

    assert workspaces[0]["workspace_id"] == "workspace-id"
    assert environ["$ENV:FABRIC_TEST_LAKEHOUSE_ID"] == "lakehouse-id"
    assert (
        environ["$ENV:FABRIC_NON_SCHEMA_LAKEHOUSE_ID"]
        == "non-schema-lakehouse-id"
    )


def test_deploy_rejects_unknown_repository_items(monkeypatch, tmp_path):
    settings = SandboxSettings(
        workspace_id="workspace-id",
        lakehouse_id="lakehouse-id",
        workspace_name="test",
        capacity_id=None,
        principal_id=None,
        environment="TEST",
        repository_root=tmp_path,
        manifest_path=tmp_path / "manifest.json",
        non_schema_lakehouse_id="non-schema-lakehouse-id",
    )
    settings.workspace_definition_dir.mkdir(parents=True)
    credentials_requested = []
    monkeypatch.setattr(
        "fabricqueryr_sandbox.deploy.get_credential",
        lambda: credentials_requested.append(True),
    )

    with pytest.raises(
        ValueError,
        match="Missing.Notebook",
    ):
        deploy(settings, items=["Missing.Notebook"])

    assert credentials_requested == []
