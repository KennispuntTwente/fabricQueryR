from pathlib import Path


def test_terraform_owns_all_sql_integration_targets():
    repository_root = Path(__file__).parents[3]
    main = (repository_root / "infra/fabric/terraform/main.tf").read_text()
    outputs = (repository_root / "infra/fabric/terraform/outputs.tf").read_text()

    assert 'resource "fabric_lakehouse" "test"' in main
    assert 'resource "fabric_warehouse" "test"' in main
    assert 'resource "fabric_sql_database" "test"' in main
    assert 'display_name = "TestWarehouse"' in main
    assert 'display_name = "TestSQLDatabase"' in main
    assert 'output "warehouse_id"' in outputs
    assert 'output "sql_database_id"' in outputs
