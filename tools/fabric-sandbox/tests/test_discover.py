from fabricqueryr_sandbox.discover import (
    NON_SCHEMA_LAKEHOUSE_TABLES,
    ONELAKE_LAKEHOUSE_TABLES,
    _wait_for_kql_properties,
    _wait_for_lakehouse_sql_endpoint,
    _wait_for_sql_properties,
    discover,
    discover_onelake,
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


def test_onelake_discovery_avoids_unrelated_service_dependencies(
    monkeypatch, tmp_path
):
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
        lambda _credential: (_ for _ in ()).throw(
            AssertionError("Power BI must not be used for OneLake discovery")
        ),
    )
    monkeypatch.setattr(
        "fabricqueryr_sandbox.discover.get_credential",
        lambda: "credential",
    )
    monkeypatch.setattr(
        "fabricqueryr_sandbox.discover.verify_fixture_revision",
        lambda settings, workspace_id, lakehouse_id: "fixture-revision",
    )

    manifest = discover_onelake(settings)

    assert set(manifest.items) == {
        "TestLakehouse",
        "TestLakehouseNoSchemas",
        "TestWarehouse",
    }
    assert manifest.fixture_revision == "fixture-revision"
    assert manifest.items["TestLakehouse"]["tables"] == ONELAKE_LAKEHOUSE_TABLES
    assert manifest.items["TestLakehouse"]["tables"]["basic"] == (
        "fabricqueryr_basic"
    )
    assert manifest.items["TestWarehouse"]["tables"] == {
        "types": "fabricqueryr_sql_types",
        "mutations": "fabricqueryr_sql_mutations",
    }
    assert "schema" not in manifest.items["TestLakehouseNoSchemas"]
    assert (
        manifest.items["TestLakehouseNoSchemas"]["tables"]
        == NON_SCHEMA_LAKEHOUSE_TABLES
    )
    assert fabric_api.refreshed == []


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
    monkeypatch.setattr(
        "fabricqueryr_sandbox.discover.verify_fixture_revision",
        lambda settings, workspace_id, lakehouse_id: "fixture-revision",
    )

    manifest = discover(settings)

    assert manifest.fixture_revision == "fixture-revision"
    assert set(manifest.items) == {
        "TestLakehouse",
        "TestLakehouseNoSchemas",
        "SeedFixtures",
        "JobFixtures",
        "TestPipeline",
        "TestSparkJob",
        "TestWarehouse",
        "TestSQLDatabase",
        "TestEventhouse",
        "TestKQLDatabase",
        "TestSemanticModel",
        "TestArrowSemanticModel",
        "TestGraphQL",
    }
    assert manifest.items["JobFixtures"] == {
        "id": "JobFixtures-id",
        "type": "Notebook",
        "display_name": "JobFixtures",
    }
    assert manifest.items["SeedFixtures"] == {
        "id": "SeedFixtures-id",
        "type": "Notebook",
        "display_name": "SeedFixtures",
    }
    assert manifest.items["TestPipeline"] == {
        "id": "TestPipeline-id",
        "type": "DataPipeline",
        "display_name": "TestPipeline",
    }
    assert manifest.items["TestSparkJob"] == {
        "id": "TestSparkJob-id",
        "type": "SparkJobDefinition",
        "display_name": "TestSparkJob",
    }
    assert manifest.items["TestArrowSemanticModel"] == {
        "id": "semantic-model-id",
        "type": "SemanticModel",
        "display_name": "FabricQueryRArrowIntegrationModel",
        "connection_string": (
            "Data Source=powerbi://api.powerbi.com/v1.0/myorg/"
            "fabricqueryr-test;"
            "Initial Catalog=FabricQueryRArrowIntegrationModel;"
        ),
    }
    assert manifest.items["TestWarehouse"] == {
        "id": "TestWarehouse-id",
        "type": "Warehouse",
        "display_name": "TestWarehouse",
        "connection_string": "warehouse.sql.test",
        "database_name": "TestWarehouse",
        "tables": {
            "types": "fabricqueryr_sql_types",
            "mutations": "fabricqueryr_sql_mutations",
        },
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
        "tables": {"types": "fabricqueryr_sql_types"},
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
    assert manifest.items["TestGraphQL"] == {
        "id": "TestGraphQL-id",
        "type": "GraphQLApi",
        "display_name": "TestGraphQL",
        "endpoint": (
            "https://api.fabric.microsoft.com/v1/workspaces/workspace-id/"
            "graphqlapis/TestGraphQL-id/graphql"
        ),
        "root_field": "fabricqueryr_basics",
        "create_field": "createfabricqueryr_basic",
    }
    lakehouse = manifest.items["TestLakehouse"]
    assert lakehouse["tables"] == {
        "runtime": "fabricqueryr_runtime",
        "basic": "fabricqueryr_basic",
        "empty": "fabricqueryr_empty",
        "void": "fabricqueryr_void",
        "partitioned": "fabricqueryr_partitioned",
        "typed_partitions": "fabricqueryr_typed_partitions",
        "binary_partitions": "fabricqueryr_binary_partitions",
        "schema_evolved": "fabricqueryr_schema_evolved",
        "column_mapped": "fabricqueryr_column_mapped",
        "column_mapped_id": "fabricqueryr_column_mapped_id",
        "column_mapped_id_partitioned_dv": (
            "fabricqueryr_column_mapped_id_partitioned_dv"
        ),
        "struct_validity": "fabricqueryr_struct_validity",
        "deletion_vectors": "fabricqueryr_deletion_vectors",
        "file_row_number_collision": (
            "fabricqueryr_file_row_number_collision"
        ),
        "deletion_vectors_stress": "fabricqueryr_deletion_vectors_stress",
        "deletion_vectors_checkpoint": (
            "fabricqueryr_deletion_vectors_checkpoint"
        ),
        "deletion_vectors_dense": "fabricqueryr_deletion_vectors_dense",
        "exact_types": "fabricqueryr_exact_types",
        "complex_types": "fabricqueryr_complex_types",
        "oracle_basic": "fabricqueryr_oracle_basic",
        "oracle_empty": "fabricqueryr_oracle_empty",
        "oracle_typed_partitions": "fabricqueryr_oracle_typed_partitions",
        "oracle_partitioned": "fabricqueryr_oracle_partitioned",
        "oracle_schema_evolved": "fabricqueryr_oracle_schema_evolved",
        "oracle_exact_types": "fabricqueryr_oracle_exact_types",
        "oracle_complex_types": "fabricqueryr_oracle_complex_types",
        "spark_oracle_column_mapped": (
            "fabricqueryr_spark_oracle_column_mapped"
        ),
        "spark_oracle_column_mapped_id": (
            "fabricqueryr_spark_oracle_column_mapped_id"
        ),
        "spark_oracle_column_mapped_id_partitioned_dv": (
            "fabricqueryr_spark_oracle_column_mapped_id_partitioned_dv"
        ),
        "spark_oracle_struct_validity": (
            "fabricqueryr_spark_oracle_struct_validity"
        ),
        "spark_oracle_deletion_vectors": (
            "fabricqueryr_spark_oracle_deletion_vectors"
        ),
        "spark_oracle_file_row_number_collision": (
            "fabricqueryr_spark_oracle_file_row_number_collision"
        ),
        "spark_oracle_deletion_vectors_stress": (
            "fabricqueryr_spark_oracle_deletion_vectors_stress"
        ),
        "spark_oracle_deletion_vectors_checkpoint": (
            "fabricqueryr_spark_oracle_deletion_vectors_checkpoint"
        ),
        "spark_oracle_deletion_vectors_dense": (
            "fabricqueryr_spark_oracle_deletion_vectors_dense"
        ),
        "spark_oracle_type_widened": (
            "fabricqueryr_spark_oracle_type_widened"
        ),
        "spark_oracle_type_widened_exact": (
            "fabricqueryr_spark_oracle_type_widened_exact"
        ),
        "spark_oracle_type_widened_pending": (
            "fabricqueryr_spark_oracle_type_widened_pending"
        ),
        "spark_oracle_type_widened_nested": (
            "fabricqueryr_spark_oracle_type_widened_nested"
        ),
        "spark_oracle_type_widened_map_key": (
            "fabricqueryr_spark_oracle_type_widened_map_key"
        ),
        "spark_oracle_v2_checkpoint": (
            "fabricqueryr_spark_oracle_v2_checkpoint"
        ),
        "spark_oracle_shallow_clone": (
            "fabricqueryr_spark_oracle_shallow_clone"
        ),
        "spark_oracle_variant": "fabricqueryr_spark_oracle_variant",
        "spark_oracle_variant_id_dv": (
            "fabricqueryr_spark_oracle_variant_id_dv"
        ),
        "shallow_clone": "fabricqueryr_shallow_clone",
        "type_widened": "fabricqueryr_type_widened",
        "type_widened_exact": "fabricqueryr_type_widened_exact",
        "type_widened_pending": "fabricqueryr_type_widened_pending",
        "type_widened_nested": "fabricqueryr_type_widened_nested",
        "type_widened_map_key": "fabricqueryr_type_widened_map_key",
        "v2_checkpoint": "fabricqueryr_v2_checkpoint",
        "variant": "fabricqueryr_variant",
        "variant_id_dv": "fabricqueryr_variant_id_dv",
        "livy_batch_result": "fabricqueryr_livy_batch_result",
        "spark_job_result": "fabricqueryr_spark_job_result",
    }
    assert lakehouse["livy_batch_file"] == (
        "abfss://workspace-id@onelake.dfs.fabric.microsoft.com/"
        "TestLakehouse-id/Files/fixtures/livy_batch.py"
    )
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


def test_lakehouse_sql_endpoint_readiness_retries_until_success():
    api = FakeFabricApi()
    calls = 0

    def get_lakehouse(workspace_id, lakehouse_id):
        nonlocal calls
        calls += 1
        status = "Provisioning" if calls == 1 else "Success"
        return {
            "id": lakehouse_id,
            "workspaceId": workspace_id,
            "properties": {
                "sqlEndpointProperties": {
                    "id": "endpoint-id",
                    "provisioningStatus": status,
                },
            },
        }

    api.get_lakehouse = get_lakehouse

    result = _wait_for_lakehouse_sql_endpoint(
        api,
        "workspace-id",
        "lakehouse-id",
    )

    endpoint = result["properties"]["sqlEndpointProperties"]
    assert endpoint["provisioningStatus"] == "Success"
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
