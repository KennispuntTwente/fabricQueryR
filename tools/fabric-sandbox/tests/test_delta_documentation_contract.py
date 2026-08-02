from pathlib import Path


def test_delta_documentation_matches_the_runtime_contract():
    repository_root = Path(__file__).parents[3]
    roadmap = (repository_root / "roadmap.md").read_text(encoding="utf-8")
    readme = (repository_root / "README.md").read_text(encoding="utf-8")
    source = (
        repository_root / "R/fabric_onelake_read_delta_table.R"
    ).read_text(encoding="utf-8")

    assert "reader stages the transaction log" not in roadmap
    assert "supports V1 and V2 checkpoints" not in roadmap
    assert "streams transaction-log and Parquet data" in roadmap
    assert "V2 checkpoints" in roadmap
    assert "ReadAll" in readme
    assert "storage.azure.com/.default" in readme
    assert "Large files without deletion vectors are not rejected" in source
    assert "materializes deletion-vector masks" in source
