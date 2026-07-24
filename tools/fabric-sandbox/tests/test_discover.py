from fabricqueryr_sandbox.discover import (
    _wait_for_kql_properties,
    _wait_for_sql_properties,
    discover,
)
from fabricqueryr_sandbox.settings import SandboxSettings


class FakeFabricApi:
    def __init__(self):
        self.sleeps = []
        self.refreshed = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def find_item(self, _workspace_id, display_name, item_type):
        return {
            "id": f"{display_name}-id",
            "displayName": display_name,
            "type": item_type,
        }

    def get_lakehouse(self, workspace_id, lakehouse_id):
        return {
            "id": lakehouse_id,
            "workspaceId": workspace_id,
            "properties": {
                "oneLakeFilesPath": "https://onelake/Files",
                "oneLakeTablesPath": "https://onelake/Tables",
                "sqlEndpointProperties": {
                    "id": "endpoint-id",
                    "connectionString": "lakehouse.sql.test",
                    "provisioningStatus": "Success",
                },
            },
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

    def get_eventhouse(self, workspace_id, eventhouse_id):
        return {
            "id": eventhouse_id,
            "workspaceId": workspace_id,
            "properties": {
                "queryServiceUri": "https://eventhouse.kusto.test",
                "ingestionServiceUri": "https://ingest-eventhouse.kusto.test",
            },
        }

    def get_kql_database(self, workspace_id, database_id):
        return {
            "id": database_id,
            "workspaceId": workspace_id,
            "properties": {
                "parentEventhouseItemId": "TestEventhouse-id",
                "queryServiceUri": "https://eventhouse.kusto.test",
                "ingestionServiceUri": "https://ingest-eventhouse.kusto.test",
                "databaseType": "ReadWrite",
            },
        }

    def refresh_sql_endpoint_metadata(self, workspace_id, endpoint_id):
        self.refreshed.append((workspace_id, endpoint_id))
        return {"status": "Succeeded"}

    def sleep(self, seconds):
        self.sleeps.append(seconds)


class FakePowerBiApi:
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def find_dataset(self, workspace_id, name):
        return {"id": "semantic-model-id", "name": name, "workspaceId": workspace_id}


def test_discover_requires_and_serializes_all_targets(monkeypatch, tmp_path):
    settings = SandboxSettings(
        workspace_id="workspace-id",
        lakehouse_id="TestLakehouse-id",
        workspace_name="fabricqueryr-test",
        capacity_id=None,
        principal_id=None,
        environment="TEST",
        repository_root=tmp_path,
        manifest_path=tmp_path / "manifest.json",
    )
    fabric_api = FakeFabricApi()
    monkeypatch.setattr(
        "fabricqueryr_sandbox.discover.FabricApi",
        lambda _credential: fabric_api,
    )
    monkeypatch.setattr(
        "fabricqueryr_sandbox.discover.PowerBiApi",
        lambda _credential: FakePowerBiApi(),
    )
    monkeypatch.setattr(
        "fabricqueryr_sandbox.discover.get_credential",
        lambda: "credential",
    )

    manifest = discover(settings)

    assert set(manifest.items) == {
        "TestLakehouse",
        "SeedFixtures",
        "TestWarehouse",
        "TestSQLDatabase",
        "TestEventhouse",
        "TestKQLDatabase",
        "TestSemanticModel",
    }
    assert manifest.items["TestWarehouse"] == {
        "id": "TestWarehouse-id",
        "type": "Warehouse",
        "display_name": "TestWarehouse",
        "connection_string": "warehouse.sql.test",
        "database_name": "TestWarehouse",
    }
    assert manifest.items["TestSQLDatabase"] == {
        "id": "TestSQLDatabase-id",
        "type": "SQLDatabase",
        "display_name": "TestSQLDatabase",
        "connection_string": (
            "Server=database.sql.test;"
            "Initial Catalog=TestSQLDatabase-internal"
        ),
        "server_fqdn": "database.sql.test,1433",
        "database_name": "TestSQLDatabase-internal",
    }
    assert fabric_api.refreshed == [("workspace-id", "endpoint-id")]
    assert manifest.items["TestEventhouse"] == {
        "id": "TestEventhouse-id",
        "type": "Eventhouse",
        "display_name": "TestEventhouse",
        "query_service_uri": "https://eventhouse.kusto.test",
        "ingestion_service_uri": "https://ingest-eventhouse.kusto.test",
    }
    assert manifest.items["TestKQLDatabase"] == {
        "id": "TestKQLDatabase-id",
        "type": "KQLDatabase",
        "display_name": "TestKQLDatabase",
        "database_name": "TestKQLDatabase",
        "parent_eventhouse_id": "TestEventhouse-id",
        "query_service_uri": "https://eventhouse.kusto.test",
        "ingestion_service_uri": "https://ingest-eventhouse.kusto.test",
        "tables": {"events": "fabricqueryr_events"},
    }
    assert settings.manifest_path.is_file()


def test_sql_property_readiness_retries_until_complete():
    api = FakeFabricApi()
    calls = 0

    def get_warehouse(workspace_id, warehouse_id):
        nonlocal calls
        calls += 1
        if calls == 1:
            return {"properties": {}}
        return {
            "id": warehouse_id,
            "workspaceId": workspace_id,
            "properties": {"connectionString": "warehouse.sql.test"},
        }

    api.get_warehouse = get_warehouse

    result = _wait_for_sql_properties(
        api,
        "workspace-id",
        "warehouse-id",
        item_type="Warehouse",
    )

    assert result["properties"]["connectionString"] == "warehouse.sql.test"
    assert calls == 2
    assert api.sleeps == [10]


def test_kql_property_readiness_retries_until_query_uri_exists():
    api = FakeFabricApi()
    calls = 0

    def get_kql_database(workspace_id, database_id):
        nonlocal calls
        calls += 1
        if calls == 1:
            return {"properties": {}}
        return {
            "id": database_id,
            "workspaceId": workspace_id,
            "properties": {
                "queryServiceUri": "https://eventhouse.kusto.test",
            },
        }

    api.get_kql_database = get_kql_database

    result = _wait_for_kql_properties(
        api,
        "workspace-id",
        "database-id",
        item_type="KQLDatabase",
    )

    assert result["properties"]["queryServiceUri"] == (
        "https://eventhouse.kusto.test"
    )
    assert calls == 2
    assert api.sleeps == [10]
