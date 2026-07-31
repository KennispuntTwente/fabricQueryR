"""Explicit Azure credentials for local and CI execution."""

from __future__ import annotations

import time
from os import environ

from azure.core.credentials import AccessToken, TokenCredential
from azure.identity import (
    AzureCliCredential,
    ClientSecretCredential,
    WorkloadIdentityCredential,
)


ENVIRONMENT_TOKEN_VARIABLES = {
    "https://api.fabric.microsoft.com/.default": "FABRIC_TEST_API_TOKEN",
    "https://analysis.windows.net/powerbi/api/.default": (
        "FABRIC_TEST_PBI_TOKEN"
    ),
    "https://database.windows.net/.default": "FABRIC_TEST_SQL_TOKEN",
    "https://storage.azure.com/.default": "FABRIC_TEST_STORAGE_TOKEN",
    "https://api.kusto.windows.net/.default": "FABRIC_TEST_KUSTO_TOKEN",
}


class EnvironmentTokenCredential:
    """Use explicitly enabled, short-lived integration-test tokens."""

    def get_token(self, *scopes: str, **_kwargs: object) -> AccessToken:
        if len(scopes) != 1:
            raise ValueError("exactly one token scope is required")
        scope = scopes[0]
        variable = ENVIRONMENT_TOKEN_VARIABLES.get(scope)
        if variable is None:
            raise ValueError(f"no environment token is configured for {scope}")
        token = environ.get(variable)
        if not token:
            raise RuntimeError(f"{variable} is required for local discovery")
        return AccessToken(token, int(time.time()) + 3600)


def get_credential() -> TokenCredential:
    if environ.get("FABRIC_SANDBOX_USE_ENV_TOKENS", "").lower() in {
        "1",
        "true",
        "yes",
    }:
        return EnvironmentTokenCredential()
    if environ.get("AZURE_FEDERATED_TOKEN_FILE"):
        return WorkloadIdentityCredential()
    client_values = {
        "tenant_id": environ.get("FABRICQUERYR_TENANT_ID", ""),
        "client_id": environ.get("FABRICQUERYR_CLIENT_ID", ""),
        "client_secret": environ.get("FABRICQUERYR_CLIENT_SECRET", ""),
    }
    if client_values["client_secret"]:
        missing = [name for name, value in client_values.items() if not value]
        if missing:
            variables = ", ".join(
                f"FABRICQUERYR_{name.upper()}" for name in missing
            )
            raise RuntimeError(
                "Incomplete local Fabric client credentials; missing "
                f"{variables}"
            )
        return ClientSecretCredential(**client_values)
    return AzureCliCredential()
