from pathlib import Path


INTEGRATION_GROUPS = {
    "auth-discovery",
    "jobs",
    "kql-graphql",
    "livy",
    "onelake",
    "power-bi",
    "sql",
}


def test_live_suite_is_split_into_feature_files():
    repository_root = Path(__file__).parents[3]
    test_directory = repository_root / "tests/testthat"
    files = sorted(test_directory.glob("test-integration-fabric-*.R"))
    groups = {
        path.stem.removeprefix("test-integration-fabric-") for path in files
    }

    assert groups == INTEGRATION_GROUPS
    assert not (test_directory / "test-integration-fabric.R").exists()
    assert all("test_that(" in path.read_text() for path in files)
    assert all(
        path.read_text().startswith("# Fabric integration coverage:")
        for path in files
    )


def test_live_workflow_provisions_once_and_runs_feature_matrix():
    repository_root = Path(__file__).parents[3]
    workflow = (
        repository_root / ".github/workflows/integration-fabric.yaml"
    ).read_text()

    for group in INTEGRATION_GROUPS:
        assert f"filter: integration-fabric-{group}" in workflow
    assert "fail-fast: false" in workflow
    assert "name: fabric-test-manifest" in workflow
    assert "name: fabric-terraform-state" in workflow
    assert "actions/upload-artifact@v4" in workflow
    assert "actions/download-artifact@v4" in workflow
    assert workflow.count("Create workspace and test targets") == 1
    assert workflow.index("Share Fabric test manifest") < workflow.index(
        "Download Fabric test manifest"
    )
    assert workflow.index("Restore Terraform state") < workflow.rindex(
        "Destroy Fabric sandbox"
    )


def test_live_workflow_gates_package_changes_at_the_test_revision():
    repository_root = Path(__file__).parents[3]
    workflow = (
        repository_root / ".github/workflows/integration-fabric.yaml"
    ).read_text()

    assert "push:" in workflow
    assert "pull_request:" in workflow
    assert workflow.count("- R/**") == 2
    assert "tests/testthat/helper-delta-rs-oracle.R" in workflow
    assert "tests/testthat/test-delta-rs-oracle.R" in workflow
    assert workflow.count("tests/testthat/test-integration-fabric-*.R") == 2
    assert "infra/fabric/**" in workflow
    assert "github.event.pull_request.head.repo.full_name" in workflow
    assert 'test "$(git rev-parse HEAD)" = "$GITHUB_SHA"' in workflow


def test_provisioning_uses_refreshable_login_and_tests_get_fresh_tokens():
    repository_root = Path(__file__).parents[3]
    integration = (
        repository_root / ".github/workflows/integration-fabric.yaml"
    ).read_text()
    persistent = (
        repository_root / ".github/workflows/fabric-sandbox.yaml"
    ).read_text()
    resources = [
        "https://storage.azure.com/",
        "https://database.windows.net/",
        "https://api.fabric.microsoft.com/",
        "https://analysis.windows.net/powerbi/api",
        "https://api.kusto.windows.net",
    ]

    provision = integration.split("\n  integration:", maxsplit=1)[0]
    assert provision.index("Sign in to Azure with OIDC") < provision.index(
        "Seed test data"
    )
    assert "Acquire sandbox access tokens" not in provision
    assert "FABRIC_SANDBOX_USE_ENV_TOKENS" not in provision

    assert persistent.index("Sign in to Azure with OIDC") < persistent.index(
        "Seed test data"
    )
    assert "Acquire sandbox access tokens" not in persistent
    assert "FABRIC_SANDBOX_USE_ENV_TOKENS" not in persistent

    acquire = integration.index("Acquire short-lived test tokens")
    assert integration.index("setup-r-dependencies@v2") < acquire
    assert acquire < integration.index("Run Fabric integration tests")
    assert all(resource in integration for resource in resources)


def test_onelake_matrix_installs_the_locked_delta_rs_oracle():
    repository_root = Path(__file__).parents[3]
    workflow = (
        repository_root / ".github/workflows/integration-fabric.yaml"
    ).read_text()

    assert (
        "if: matrix.adbc || "
        "matrix.filter == 'integration-fabric-onelake'"
    ) in workflow
    assert "Install locked delta-rs runtime environment" in workflow
    assert "Select the locked delta-rs Python environment" in workflow
    assert "uv --directory tools/fabric-sandbox sync --locked" in workflow


def test_locked_delta_bridge_runs_on_every_release_platform():
    repository_root = Path(__file__).parents[3]
    workflow = (
        repository_root / ".github/workflows/R-CMD-check.yaml"
    ).read_text()

    assert "matrix.config.os == 'ubuntu-latest'" not in workflow
    assert workflow.count("if: matrix.config.r == 'release'") >= 3
    assert "matrix.config.os != 'windows-latest'" in workflow
    assert ".venv/bin/python" in workflow
    assert "matrix.config.os == 'windows-latest'" in workflow
    assert ".venv/Scripts/python.exe" in workflow
    assert "FABRIC_DELTA_RS_ORACLE_TESTS" in workflow


def test_live_sql_matrix_installs_required_client_drivers():
    repository_root = Path(__file__).parents[3]
    workflow = (
        repository_root / ".github/workflows/integration-fabric.yaml"
    ).read_text()

    assert "msodbcsql18" in workflow
    assert 'uvx dbc==0.3.0 install "mssql>=1.5,<2"' in workflow
    assert "if: matrix.odbc" in workflow
    assert "if: matrix.adbc" in workflow
    assert workflow.index(
        "Install Microsoft SQL Server ADBC driver"
    ) < workflow.index("Run Fabric integration tests")


def test_parallel_sql_reads_ignore_graphql_mutation_sentinel():
    repository_root = Path(__file__).parents[3]
    sql_tests = (
        repository_root
        / "tests/testthat/test-integration-fabric-sql.R"
    ).read_text()

    assert sql_tests.count('"WHERE id > 0"') == 2


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
