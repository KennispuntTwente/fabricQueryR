import pytest
from azure.identity import AzureCliCredential, ClientSecretCredential

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


def test_get_credential_uses_fabricqueryr_client_secret(monkeypatch):
    monkeypatch.delenv("FABRIC_SANDBOX_USE_ENV_TOKENS", raising=False)
    monkeypatch.delenv("AZURE_FEDERATED_TOKEN_FILE", raising=False)
    monkeypatch.setenv("FABRICQUERYR_TENANT_ID", "tenant-id")
    monkeypatch.setenv("FABRICQUERYR_CLIENT_ID", "client-id")
    monkeypatch.setenv("FABRICQUERYR_CLIENT_SECRET", "client-secret")

    assert isinstance(get_credential(), ClientSecretCredential)


def test_get_credential_rejects_partial_fabricqueryr_credentials(monkeypatch):
    monkeypatch.delenv("FABRIC_SANDBOX_USE_ENV_TOKENS", raising=False)
    monkeypatch.delenv("AZURE_FEDERATED_TOKEN_FILE", raising=False)
    monkeypatch.setenv("FABRICQUERYR_TENANT_ID", "tenant-id")
    monkeypatch.delenv("FABRICQUERYR_CLIENT_ID", raising=False)
    monkeypatch.setenv("FABRICQUERYR_CLIENT_SECRET", "client-secret")

    with pytest.raises(RuntimeError, match="FABRICQUERYR_CLIENT_ID"):
        get_credential()


def test_get_credential_allows_non_secret_r_configuration(monkeypatch):
    monkeypatch.delenv("FABRIC_SANDBOX_USE_ENV_TOKENS", raising=False)
    monkeypatch.delenv("AZURE_FEDERATED_TOKEN_FILE", raising=False)
    monkeypatch.setenv("FABRICQUERYR_TENANT_ID", "tenant-id")
    monkeypatch.setenv("FABRICQUERYR_CLIENT_ID", "client-id")
    monkeypatch.delenv("FABRICQUERYR_CLIENT_SECRET", raising=False)

    assert isinstance(get_credential(), AzureCliCredential)


def test_get_credential_falls_back_to_azure_cli(monkeypatch):
    monkeypatch.delenv("FABRIC_SANDBOX_USE_ENV_TOKENS", raising=False)
    monkeypatch.delenv("AZURE_FEDERATED_TOKEN_FILE", raising=False)
    monkeypatch.delenv("FABRICQUERYR_TENANT_ID", raising=False)
    monkeypatch.delenv("FABRICQUERYR_CLIENT_ID", raising=False)
    monkeypatch.delenv("FABRICQUERYR_CLIENT_SECRET", raising=False)

    assert isinstance(get_credential(), AzureCliCredential)
