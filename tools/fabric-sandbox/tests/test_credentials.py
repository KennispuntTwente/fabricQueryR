import pytest

from fabricqueryr_sandbox.credentials import (
    EnvironmentTokenCredential,
    get_credential,
)


def test_environment_credential_selects_tokens_by_scope(monkeypatch):
    monkeypatch.setenv("FABRIC_TEST_API_TOKEN", "fabric-token")
    credential = EnvironmentTokenCredential()

    token = credential.get_token(
        "https://api.fabric.microsoft.com/.default"
    )

    assert token.token == "fabric-token"


def test_environment_credential_requires_the_requested_token(monkeypatch):
    monkeypatch.delenv("FABRIC_TEST_PBI_TOKEN", raising=False)
    credential = EnvironmentTokenCredential()

    with pytest.raises(RuntimeError, match="FABRIC_TEST_PBI_TOKEN"):
        credential.get_token(
            "https://analysis.windows.net/powerbi/api/.default"
        )


def test_environment_credential_rejects_unknown_scope():
    credential = EnvironmentTokenCredential()

    with pytest.raises(ValueError, match="no environment token"):
        credential.get_token("https://example.test/.default")


def test_get_credential_only_uses_environment_tokens_when_enabled(monkeypatch):
    monkeypatch.setenv("FABRIC_SANDBOX_USE_ENV_TOKENS", "true")

    assert isinstance(get_credential(), EnvironmentTokenCredential)
