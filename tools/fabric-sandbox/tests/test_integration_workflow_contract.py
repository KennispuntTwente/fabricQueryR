from pathlib import Path


def test_live_sql_matrix_installs_both_client_drivers():
    repository_root = Path(__file__).parents[3]
    workflow = (
        repository_root / ".github/workflows/integration-fabric.yaml"
    ).read_text()

    assert "msodbcsql18" in workflow
    assert 'uvx dbc==0.3.0 install "mssql>=1.5,<2"' in workflow
    assert 'filter = "integration-fabric"' in workflow
