from pathlib import Path


def test_terraform_owns_all_integration_targets():
    repository_root = Path(__file__).parents[3]
    main = (repository_root / "infra/fabric/terraform/main.tf").read_text()
    outputs = (repository_root / "infra/fabric/terraform/outputs.tf").read_text()
    variables = (repository_root / "infra/fabric/terraform/variables.tf").read_text()
    migrations = (repository_root / "infra/fabric/terraform/migrations.tf").read_text()
    versions = (repository_root / "infra/fabric/terraform/versions.tf").read_text()
    mirror_definition = (
        repository_root
        / "infra/fabric/terraform/definitions/open-mirroring.json"
    ).read_text()

    assert 'resource "fabric_lakehouse" "test"' in main
    assert 'resource "fabric_warehouse" "test"' in main
    assert 'resource "fabric_warehouse_snapshot" "test"' in main
    assert 'resource "fabric_sql_database" "test"' in main
    assert 'resource "fabric_mirrored_database" "test"' in main
    assert 'resource "fabric_eventhouse" "test"' in main
    assert 'resource "fabric_kql_database" "test"' in main
    assert 'resource "fabric_graphql_api" "test"' in main
    assert 'display_name = "TestWarehouse"' in main
    assert 'display_name = "TestWarehouseSnapshot"' in main
    assert 'display_name = "TestSQLDatabase"' in main
    assert 'display_name = "TestMirroredDatabase"' in main
    assert 'display_name = "TestEventhouse"' in main
    assert 'display_name = "TestKQLDatabase"' in main
    assert 'display_name = "TestGraphQL"' in main
    assert "eventhouse_id = fabric_eventhouse.test[0].id" in main
    assert main.count("count = local.provision_full_fixture ? 1 : 0") == 4
    assert 'variable "fixture_scope"' in variables
    assert 'variable "provision_sql_database"' in variables
    assert 'default     = "all"' in variables
    assert '["all", "onelake"]' in variables
    assert migrations.count("moved {") == 5
    assert "to   = fabric_sql_database.test[0]" in migrations
    assert "count = local.provision_sql_database ? 1 : 0" in main
    assert '"type": "GenericMirror"' in mirror_definition
    assert '"defaultSchema": "dbo"' in mirror_definition
    assert 'output "warehouse_id"' in outputs
    assert 'output "warehouse_snapshot_id"' in outputs
    assert 'output "sql_database_id"' in outputs
    assert 'output "mirrored_database_id"' in outputs
    assert 'output "eventhouse_id"' in outputs
    assert 'output "kql_database_id"' in outputs
    assert 'output "graphql_api_id"' in outputs
    assert outputs.count("one(fabric_") == 5
    assert "preview = true" in versions
