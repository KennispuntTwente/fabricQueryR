from pathlib import Path


def test_live_sql_matrix_installs_both_client_drivers():
    repository_root = Path(__file__).parents[3]
    workflow = (
        repository_root / ".github/workflows/integration-fabric.yaml"
    ).read_text()

    assert "msodbcsql18" in workflow
    assert 'uvx dbc==0.3.0 install "mssql>=1.5,<2"' in workflow
    assert 'filter = "integration-fabric"' in workflow
    assert workflow.index("Install Microsoft ODBC driver") < workflow.index(
        "Install locked sandbox environment"
    )
    assert workflow.index(
        "Install Microsoft SQL Server ADBC driver"
    ) < workflow.index("Run Fabric integration tests")


def test_persistent_sandbox_workflow_is_idempotent_and_manually_removed():
    repository_root = Path(__file__).parents[3]
    workflow = (
        repository_root / ".github/workflows/fabric-sandbox.yaml"
    ).read_text()

    assert "workflow_dispatch:" in workflow
    assert "- rebuild" in workflow
    assert "- teardown" in workflow
    assert "fabricqueryr-dev-dhrkoning" in workflow
    assert "fabricqueryr-persistent;" in workflow
    assert "remove-persistent" in workflow
    assert workflow.index(
        "Remove existing persistent sandbox"
    ) < workflow.index("Create workspace and test targets")
    assert "TF_VAR_test_principal_type: User" in workflow
    assert "TF_VAR_test_principal_role: Admin" in workflow
    assert "9b7dcb13-8485-4429-8b4f-7f1f6ce6ebf5" in workflow
    assert "terraform -chdir=\"$TF_DIR\" destroy" not in workflow
