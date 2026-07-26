from fabricqueryr_sandbox.seed import seed, upload_fixtures
from fabricqueryr_sandbox.settings import SandboxSettings


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
            return {"id": "seed-job", "exitValue": "fabricqueryr-seed-success"}

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
        lambda settings, workspace_id, lakehouse_id: calls.append(
            ("upload", settings, workspace_id, lakehouse_id)
        ),
    )
    monkeypatch.setattr(
        "fabricqueryr_sandbox.seed._wait_for_lakehouse_sql_endpoint",
        lambda api, workspace_id, lakehouse_id: {
            "properties": {"sqlEndpointProperties": {"id": "sql-endpoint-id"}}
        },
    )
    monkeypatch.setattr(
        "fabricqueryr_sandbox.seed._wait_for_kql_properties",
        lambda api, workspace_id, item_id, *, item_type: {
            "properties": {"queryServiceUri": "https://kusto.test"}
        },
    )
    monkeypatch.setattr(
        "fabricqueryr_sandbox.seed.seed_sql_fixture",
        lambda connection_string, database, token: calls.append(
            ("seed_sql", connection_string, database, token)
        ),
    )
    credential = type(
        "Credential",
        (),
        {
            "get_token": lambda self, audience: type(
                "AccessToken",
                (),
                {"token": f"token-for-{audience}"},
            )()
        },
    )()
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

    seed(settings)

    assert ("upload", settings, "workspace-id", "TestLakehouse-id") in calls
    assert (
        "run_notebook",
        "workspace-id",
        "SeedFixtures-id",
        "TestLakehouse-id",
    ) in calls
    assert (
        "wait_for_sql_table",
        "workspace-id",
        "sql-endpoint-id",
        "dbo.fabricqueryr_basic",
    ) in calls
    graphql_call = next(call for call in calls if call[0] == "update_graphql")
    assert graphql_call[1:3] == ("workspace-id", "TestGraphQL-id")
    datasource = graphql_call[3]["datasources"][0]
    assert datasource["sourceWorkspaceId"] == "workspace-id"
    assert datasource["sourceItemId"] == "sql-endpoint-id"
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
    ) in calls
    assert (
        "seed_sql",
        (
            "Server=database.sql.test;"
            "Initial Catalog=TestSQLDatabase-internal"
        ),
        "TestSQLDatabase-internal",
        "token-for-https://database.windows.net/.default",
    ) in calls
    assert ("seed_power_bi", credential, "workspace-id") in calls
