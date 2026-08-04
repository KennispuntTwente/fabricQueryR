from pathlib import Path


def test_local_runner_targets_the_marked_persistent_workspace():
    repository_root = Path(__file__).parents[3]
    runner = (
        repository_root / "tools/fabric-sandbox/local-integration.R"
    ).read_text()

    assert 'workspace_name = "fabricqueryr-dev-dhrkoning"' in runner
    assert "fabricqueryr-persistent;" in runner
    assert "9b7dcb13-8485-4429-8b4f-7f1f6ce6ebf5" in runner
    assert "fabric_local_jwt_claims" in runner
    assert "fabric_local_exchange_token" in runner
    assert 'grant_type = "refresh_token"' in runner
    assert 'Sys.getenv("FABRICQUERYR_CLIENT_SECRET")' in runner
    assert "fabric_local_validate_secret_identity" in runner
    assert "AzureAuth identities are never combined with environment secrets" in runner
    assert 'auth_args$auth_type <- "client_credentials"' in runner
    assert "fabric_local_uses_client_credentials" in runner
    assert "fabric_local_validate_identity" in runner
    assert "claims$appid" in runner
    assert "claims$azp" in runner
    assert "FABRIC_SANDBOX_USE_ENV_TOKENS" in runner
    assert "fabric_local_test_audiences" in runner
    assert 'return(all[c("Fabric", "OneLake")])' in runner
    assert 'c("--scope", "onelake")' in runner
    assert 'require_sql = !grepl("onelake", filter' in runner
    assert "if (!isTRUE(require_sql))" in runner
    assert 'filter = "integration-fabric"' in runner
    assert "filter = filter" in runner
    assert "stop_on_failure = TRUE" in runner
