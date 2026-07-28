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
    assert "FABRIC_SANDBOX_USE_ENV_TOKENS" in runner
    assert '"integration-fabric"' in runner
    assert "stop_on_failure = TRUE" in runner
