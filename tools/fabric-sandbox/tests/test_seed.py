from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from azure.core.credentials import AccessToken

from fabricqueryr_sandbox.seed import (
    _logical_delta_table_row_count,
    _runtime_contract,
    seed,
    upload_fixtures,
    wait_for_delta_log_publication,
)
from fabricqueryr_sandbox.settings import SandboxSettings


RUNTIME_CONTRACT = {
    "lane": "core",
    "fabric_runtime": "1.3",
    "spark_version": "3.5.5.5",
    "delta_version": "3.2.1",
}


def test_runtime_contract_accepts_the_selected_observed_build(tmp_path):
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
    exit_value = (
        "fabricqueryr-seed-success:"
        '{"delta_version":"3.2.1","fabric_runtime":"1.3",'
        '"lane":"core","spark_version":"3.5.5.5"}'
    )

    assert _runtime_contract(exit_value, settings) == RUNTIME_CONTRACT


def test_runtime_contract_rejects_a_mismatched_lane(tmp_path):
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
    exit_value = (
        "fabricqueryr-seed-success:"
        '{"delta_version":"4.2.0","fabric_runtime":"2.0",'
        '"lane":"preview","spark_version":"4.1.0.0"}'
    )

    with pytest.raises(RuntimeError, match="does not match the lane"):
        _runtime_contract(exit_value, settings)


class FakeFileClient:
    def __init__(self, path, uploaded):
        self.path = path
        self.uploaded = uploaded

    def upload_data(self, source, *, overwrite):
        self.uploaded[self.path] = (source.read(), overwrite)


class FakeFileSystem:
    def __init__(self, uploaded):
        self.uploaded = uploaded
        self.directories = []

    def get_file_client(self, path):
        return FakeFileClient(path, self.uploaded)

    def get_directory_client(self, path):
        filesystem = self

        class FakeDirectoryClient:
            def create_directory(self):
                filesystem.directories.append(path)

        return FakeDirectoryClient()


class FakeDataLakeServiceClient:
    uploaded = {}
    account_url = None
    credential = None
    filesystem = None
    file_system_client = None

    def __init__(self, *, account_url, credential):
        type(self).account_url = account_url
        type(self).credential = credential

    def get_file_system_client(self, filesystem):
        type(self).filesystem = filesystem
        type(self).file_system_client = FakeFileSystem(type(self).uploaded)
        return type(self).file_system_client


def test_logical_delta_table_row_count_excludes_deletion_vectors():
    class Column:
        def to_pylist(self):
            return [[True, False, True], [False, True]]

    class DeletionVectors:
        def column(self, name):
            assert name == "selection_vector"
            return Column()

    class Reader:
        def read_all(self):
            return DeletionVectors()

    class DeltaTable:
        def count(self):
            return 5

        def deletion_vectors(self):
            return Reader()

    assert _logical_delta_table_row_count(DeltaTable()) == 3


def test_upload_fixtures_preserves_nested_and_unicode_paths(monkeypatch, tmp_path):
    fixture_dir = tmp_path / "infra/fabric/fixtures"
    fixture_dir.mkdir(parents=True)
    (fixture_dir / "basic.csv").write_text("id,name\n1,alpha\n")
    (fixture_dir / "livy_batch.py").write_text("print('batch')\n")
    nested = fixture_dir / "nested/a"
    nested.mkdir(parents=True)
    (nested / "café-数据.txt").write_text("unicode\n", encoding="utf-8")
    settings = SandboxSettings(
        workspace_id="workspace-id",
        lakehouse_id="lakehouse-id",
        workspace_name="fabricqueryr-test",
        capacity_id=None,
        principal_id=None,
        environment="TEST",
        repository_root=tmp_path,
        manifest_path=tmp_path / "manifest.json",
    )
    FakeDataLakeServiceClient.uploaded = {}
    monkeypatch.setattr(
        "fabricqueryr_sandbox.seed.DataLakeServiceClient",
        FakeDataLakeServiceClient,
    )
    monkeypatch.setattr(
        "fabricqueryr_sandbox.seed.get_credential",
        lambda: "credential",
    )

    upload_fixtures(settings, "workspace-id", "lakehouse-id")

    assert FakeDataLakeServiceClient.account_url == (
        "https://onelake.dfs.fabric.microsoft.com"
    )
    assert FakeDataLakeServiceClient.filesystem == "workspace-id"
    assert FakeDataLakeServiceClient.file_system_client.directories == [
        "lakehouse-id/Files/fixtures",
        "lakehouse-id/Files/fixtures/nested",
        "lakehouse-id/Files/fixtures/nested/a",
    ]
    assert set(FakeDataLakeServiceClient.uploaded) == {
        "lakehouse-id/Files/fixtures/basic.csv",
        "lakehouse-id/Files/fixtures/livy_batch.py",
        "lakehouse-id/Files/fixtures/nested/a/café-数据.txt",
    }
    batch = FakeDataLakeServiceClient.uploaded[
        "lakehouse-id/Files/fixtures/livy_batch.py"
    ]
    assert batch[0].replace(b"\r\n", b"\n") == b"print('batch')\n"
    assert batch[1] is True
    unicode_file = FakeDataLakeServiceClient.uploaded[
        "lakehouse-id/Files/fixtures/nested/a/café-数据.txt"
    ]
    assert unicode_file[0].replace(b"\r\n", b"\n") == b"unicode\n"
    assert unicode_file[1] is True


def test_wait_for_delta_log_publication_requires_a_new_log():
    threshold = datetime(2026, 7, 30, tzinfo=timezone.utc)
    listings = iter(
        [
            [
                SimpleNamespace(
                    name=(
                        "warehouse/Tables/dbo/table/_delta_log/"
                        "00000000000000000000.json"
                    ),
                    is_directory=False,
                    last_modified=threshold - timedelta(seconds=1),
                )
            ],
            [
                SimpleNamespace(
                    name=(
                        "warehouse/Tables/dbo/table/_delta_log/"
                        "00000000000000000001.json"
                    ),
                    is_directory=False,
                    last_modified=threshold,
                )
            ],
        ]
    )
    calls = []

    class FileSystem:
        def get_paths(self, *, path, recursive):
            calls.append((path, recursive))
            return next(listings)

    class Service:
        def get_file_system_client(self, workspace_id):
            assert workspace_id == "workspace"
            return FileSystem()

    sleeps = []
    published = wait_for_delta_log_publication(
        "workspace",
        "warehouse",
        "table",
        not_before=threshold,
        attempts=2,
        retry_delay=3,
        service_client=Service(),
        sleep=sleeps.append,
    )

    assert published.endswith("00000000000000000001.json")
    assert calls == [
        ("warehouse/Tables/dbo/table/_delta_log", False),
        ("warehouse/Tables/dbo/table/_delta_log", False),
    ]
    assert sleeps == [3]


def test_wait_for_delta_log_publication_requires_readable_expected_rows():
    threshold = datetime(2026, 7, 30, tzinfo=timezone.utc)
    published_path = SimpleNamespace(
        name=(
            "warehouse/Tables/dbo/table/_delta_log/"
            "00000000000000000001.json"
        ),
        is_directory=False,
        last_modified=threshold,
    )

    class FileSystem:
        def get_paths(self, *, path, recursive):
            return [published_path]

    class Service:
        def get_file_system_client(self, workspace_id):
            return FileSystem()

    counts = iter([2, 3])
    uris = []

    def table_count(uri):
        uris.append(uri)
        return next(counts)

    sleeps = []
    published = wait_for_delta_log_publication(
        "workspace",
        "warehouse",
        "table",
        not_before=threshold,
        attempts=2,
        expected_rows=3,
        service_client=Service(),
        table_count=table_count,
        sleep=sleeps.append,
    )

    assert published.endswith("00000000000000000001.json")
    assert uris == [
        (
            "abfss://workspace@onelake.dfs.fabric.microsoft.com/"
            "warehouse/Tables/dbo/table"
        ),
        (
            "abfss://workspace@onelake.dfs.fabric.microsoft.com/"
            "warehouse/Tables/dbo/table"
        ),
    ]
    assert sleeps == [10]


def test_seed_requires_every_live_fixture_to_be_ready(monkeypatch, tmp_path):
    settings = SandboxSettings(
        workspace_id="workspace-id",
        lakehouse_id="lakehouse-id",
        workspace_name="fabricqueryr-test",
        capacity_id=None,
        principal_id=None,
        environment="TEST",
        repository_root=tmp_path,
        manifest_path=tmp_path / "manifest.json",
    )
    calls = []

    class FakeFabricApi:
        def __init__(self, credential):
            calls.append(("fabric_client", credential))

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def configure_workspace_spark_runtime(
            self,
            workspace_id,
            runtime_version,
        ):
            calls.append(("spark_runtime", workspace_id, runtime_version))
            return {
                "environment": {
                    "name": "",
                    "runtimeVersion": runtime_version,
                }
            }

        def find_item(self, workspace_id, display_name, item_type):
            calls.append(("find_item", workspace_id, display_name, item_type))
            return {
                "id": f"{display_name}-id",
                "displayName": display_name,
                "type": item_type,
            }

        def run_notebook(
            self,
            workspace_id,
            notebook_id,
            *,
            lakehouse_id,
        ):
            calls.append(
                (
                    "run_notebook",
                    workspace_id,
                    notebook_id,
                    lakehouse_id,
                )
            )
            return {
                "id": "seed-job",
                "exitValue": (
                    "fabricqueryr-seed-success:"
                    '{"delta_version":"3.2.1",'
                    '"fabric_runtime":"1.3","lane":"core",'
                    '"spark_version":"3.5.5.5"}'
                ),
            }

        def get_warehouse(self, workspace_id, warehouse_id):
            return {
                "id": warehouse_id,
                "workspaceId": workspace_id,
                "properties": {
                    "connectionString": "warehouse.sql.test",
                },
            }

        def get_sql_database(self, workspace_id, database_id):
            return {
                "id": database_id,
                "workspaceId": workspace_id,
                "properties": {
                    "connectionString": (
                        "Server=database.sql.test;"
                        "Initial Catalog=TestSQLDatabase-internal"
                    ),
                    "serverFqdn": "database.sql.test,1433",
                    "databaseName": "TestSQLDatabase-internal",
                },
            }

        def get_mirrored_database(self, workspace_id, database_id):
            return {
                "id": database_id,
                "workspaceId": workspace_id,
                "properties": {
                    "defaultSchema": "dbo",
                    "oneLakeTablesPath": "https://onelake/Tables",
                    "sqlEndpointProperties": {
                        "id": "mirrored-endpoint-id",
                        "connectionString": "mirrored.sql.test",
                        "provisioningStatus": "Success",
                    },
                },
            }

        def wait_for_mirroring_running(self, workspace_id, database_id):
            calls.append(("wait_for_mirroring", workspace_id, database_id))
            return {"status": "Running"}

        def wait_for_sql_endpoint_table(
            self,
            workspace_id,
            sql_endpoint_id,
            table_name,
        ):
            calls.append(
                (
                    "wait_for_sql_table",
                    workspace_id,
                    sql_endpoint_id,
                    table_name,
                )
            )
            return {"tableName": table_name, "status": "Success"}

        def update_graphql_definition(
            self,
            workspace_id,
            graphql_api_id,
            definition,
        ):
            calls.append(
                (
                    "update_graphql",
                    workspace_id,
                    graphql_api_id,
                    definition,
                )
            )
            return {"status": "Succeeded"}

        def wait_for_graphql_root_field(
            self,
            workspace_id,
            graphql_api_id,
            root_field,
        ):
            calls.append(
                (
                    "wait_for_graphql",
                    workspace_id,
                    graphql_api_id,
                    root_field,
                )
            )
            return {"name": root_field}

    class FakeKustoApi:
        def __init__(self, credential):
            calls.append(("kusto_client", credential))

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def seed_fixture(self, query_service_uri, database):
            calls.append(("seed_kusto", query_service_uri, database))
            return {"Tables": []}

    monkeypatch.setattr(
        "fabricqueryr_sandbox.seed.FabricApi",
        FakeFabricApi,
    )
    monkeypatch.setattr(
        "fabricqueryr_sandbox.seed.KustoApi",
        FakeKustoApi,
    )
    monkeypatch.setattr(
        "fabricqueryr_sandbox.seed.upload_fixtures",
        lambda settings, workspace_id, lakehouse_id, *, credential: calls.append(
            ("upload", settings, workspace_id, lakehouse_id, credential)
        ),
    )
    monkeypatch.setattr(
        "fabricqueryr_sandbox.seed.upload_open_mirroring_fixture",
        lambda workspace_id, database_id, *, credential: calls.append(
            ("upload_mirror", workspace_id, database_id, credential)
        )
        or "mirrored-id/Files/LandingZone/dbo.schema/table/fixture.parquet",
    )
    monkeypatch.setattr(
        "fabricqueryr_sandbox.seed.fixture_revision",
        lambda settings, runtime_contract: (
            calls.append(("fixture_hash_runtime", runtime_contract))
            or "fixture-revision"
        ),
    )
    monkeypatch.setattr(
        "fabricqueryr_sandbox.seed.write_fixture_revision",
        lambda workspace_id,
        lakehouse_id,
        revision,
        *,
        runtime_contract=None,
        credential: calls.append(
            (
                "fixture_revision",
                workspace_id,
                lakehouse_id,
                revision,
                runtime_contract,
                credential,
            )
        ),
    )
    monkeypatch.setattr(
        "fabricqueryr_sandbox.seed._wait_for_kql_properties",
        lambda api, workspace_id, item_id, *, item_type: {
            "properties": {"queryServiceUri": "https://kusto.test"}
        },
    )
    monkeypatch.setattr(
        "fabricqueryr_sandbox.seed.seed_sql_fixture",
        lambda connection_string, database, token, *, mutate=False: calls.append(
            ("seed_sql", connection_string, database, token, mutate)
        ),
    )
    monkeypatch.setattr(
        "fabricqueryr_sandbox.seed.wait_for_sql_fixture",
        lambda connection_string, database, token, table: calls.append(
            ("wait_for_sql_fixture", connection_string, database, token, table)
        ),
    )
    monkeypatch.setattr(
        "fabricqueryr_sandbox.seed.wait_for_delta_log_publication",
        lambda workspace_id, item_id, table, *, not_before, expected_rows, credential: calls.append(
            (
                "wait_for_delta_log",
                workspace_id,
                item_id,
                table,
                not_before,
                expected_rows,
                credential,
            )
        )
        or "00000000000000000000.json",
    )
    class Credential:
        def get_token(self, audience):
            calls.append(("base_get_token", audience))
            return AccessToken(
                f"token-for-{audience}",
                4102444800,
            )

    credential = Credential()
    monkeypatch.setattr(
        "fabricqueryr_sandbox.seed.get_credential",
        lambda: credential,
    )
    monkeypatch.setattr(
        "fabricqueryr_sandbox.seed.seed_test_semantic_model",
        lambda credential, workspace_id: calls.append(
            ("seed_power_bi", credential, workspace_id)
        )
        or {"id": "semantic-model-id", "name": "TestModel"},
    )
    monkeypatch.setattr(
        "fabricqueryr_sandbox.seed.prepare_arrow_test_semantic_model",
        lambda credential, workspace_id: calls.append(
            ("prepare_arrow_power_bi", credential, workspace_id)
        )
        or {"id": "arrow-model-id", "name": "ArrowTestModel"},
    )

    seed(settings)

    assert ("spark_runtime", "workspace-id", "1.3") in calls
    assert ("fixture_hash_runtime", RUNTIME_CONTRACT) in calls
    cached_credential = next(
        call[1] for call in calls if call[0] == "fabric_client"
    )
    assert cached_credential.credential is credential
    token_calls = [call for call in calls if call[0] == "base_get_token"]
    assert [call[1] for call in token_calls] == [
        "https://api.fabric.microsoft.com/.default",
        "https://storage.azure.com/.default",
        "https://database.windows.net/.default",
        "https://api.kusto.windows.net/.default",
        "https://analysis.windows.net/powerbi/api/.default",
    ]
    assert calls.index(token_calls[-1]) < next(
        index for index, call in enumerate(calls) if call[0] == "fabric_client"
    )
    assert (
        "upload",
        settings,
        "workspace-id",
        "TestLakehouse-id",
        cached_credential,
    ) in calls
    assert (
        "wait_for_mirroring",
        "workspace-id",
        "TestMirroredDatabase-id",
    ) in calls
    assert (
        "upload_mirror",
        "workspace-id",
        "TestMirroredDatabase-id",
        cached_credential,
    ) in calls
    revision_calls = [
        call for call in calls if call[0] == "fixture_revision"
    ]
    assert revision_calls == [
        (
            "fixture_revision",
            "workspace-id",
            "TestLakehouse-id",
            "incomplete",
            None,
            cached_credential,
        ),
        (
            "fixture_revision",
            "workspace-id",
            "TestLakehouse-id",
            "fixture-revision",
            RUNTIME_CONTRACT,
            cached_credential,
        ),
    ]
    assert (
        "run_notebook",
        "workspace-id",
        "SeedFixtures-id",
        "TestLakehouse-id",
    ) in calls
    graphql_call = next(call for call in calls if call[0] == "update_graphql")
    assert graphql_call[1:3] == ("workspace-id", "TestGraphQL-id")
    datasource = graphql_call[3]["datasources"][0]
    assert datasource["sourceWorkspaceId"] == "workspace-id"
    assert datasource["sourceItemId"] == "TestWarehouse-id"
    assert datasource["sourceType"] == "Warehouse"
    assert datasource["objects"][0]["actions"]["Create"] == "Enabled"
    assert (
        "wait_for_graphql",
        "workspace-id",
        "TestGraphQL-id",
        "fabricqueryr_basics",
    ) in calls
    assert (
        "seed_kusto",
        "https://kusto.test",
        "TestKQLDatabase",
    ) in calls
    assert (
        "seed_sql",
        "warehouse.sql.test",
        "TestWarehouse",
        "token-for-https://database.windows.net/.default",
        True,
    ) in calls
    delta_log_calls = [
        call for call in calls if call[0] == "wait_for_delta_log"
    ]
    assert [call[3] for call in delta_log_calls] == [
        "fabricqueryr_sql_types",
        "fabricqueryr_sql_mutations",
        "fabricqueryr_mirror_types",
    ]
    assert all(
        call[1:3] == ("workspace-id", "TestWarehouse-id")
        for call in delta_log_calls[:2]
    )
    assert delta_log_calls[2][1:3] == (
        "workspace-id",
        "TestMirroredDatabase-id",
    )
    assert all(call[4].tzinfo is not None for call in delta_log_calls)
    assert all(call[5] == 3 for call in delta_log_calls)
    assert all(call[6] is cached_credential for call in delta_log_calls)
    assert calls.index(delta_log_calls[0]) > calls.index(
        (
            "seed_sql",
            "warehouse.sql.test",
            "TestWarehouse",
            "token-for-https://database.windows.net/.default",
            True,
        )
    )
    assert (
        "wait_for_sql_fixture",
        "mirrored.sql.test",
        "TestMirroredDatabase",
        "token-for-https://database.windows.net/.default",
        "fabricqueryr_mirror_types",
    ) in calls
    assert calls.index(graphql_call) > max(
        index for index, call in enumerate(calls) if call[0] == "seed_sql"
    )
    assert (
        "seed_sql",
        (
            "Server=database.sql.test;"
            "Initial Catalog=TestSQLDatabase-internal"
        ),
        "TestSQLDatabase-internal",
        "token-for-https://database.windows.net/.default",
        False,
    ) in calls
    assert ("seed_power_bi", cached_credential, "workspace-id") in calls
    assert (
        "prepare_arrow_power_bi",
        cached_credential,
        "workspace-id",
    ) in calls
