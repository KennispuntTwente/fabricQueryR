from pathlib import Path


def test_delta_documentation_matches_the_runtime_contract():
    repository_root = Path(__file__).parents[3]
    roadmap = (repository_root / "roadmap.md").read_text(encoding="utf-8")
    readme = (repository_root / "README.md").read_text(encoding="utf-8")
    seed = (
        repository_root
        / "infra/fabric/workspace/SeedFixtures.Notebook/notebook-content.py"
    ).read_text(encoding="utf-8")
    normalized_readme = " ".join(readme.split())

    assert "reader stages the transaction log" not in roadmap
    assert "supports V1 and V2 checkpoints" not in roadmap
    assert "streams transaction-log and Parquet data" in roadmap
    assert "V2 checkpoints" in roadmap
    assert "ReadAll" in readme
    assert (
        "Users can access data stored in OneLake with apps external to Fabric"
        in normalized_readme
    )
    assert "service-admin-portal-onelake" in readme
    assert "row- or column-level security" in normalized_readme
    assert "security/permission-model" in readme
    assert "query-delta-lake-logs" in readme
    assert "Type Widening, V2 Checkpoints, or Fabric Variant" in readme
    assert "delta-reader-spark-oracle.json" in seed
    assert "mssparkutils.fs.put" in seed
