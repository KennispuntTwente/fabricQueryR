"""Explicit Azure credentials for local and CI execution."""

from __future__ import annotations

import base64
import binascii
import json
import time
from os import environ

from azure.core.credentials import AccessToken, TokenCredential
from azure.identity import (
    AzureCliCredential,
    ClientSecretCredential,
    WorkloadIdentityCredential,
)

STORAGE_SCOPE = "https://storage.azure.com/.default"
ENVIRONMENT_TOKEN_VARIABLES = {
    "https://api.fabric.microsoft.com/.default": "FABRIC_TEST_API_TOKEN",
    "https://analysis.windows.net/powerbi/api/.default": (
        "FABRIC_TEST_PBI_TOKEN"
    ),
    "https://database.windows.net/.default": "FABRIC_TEST_SQL_TOKEN",
    STORAGE_SCOPE: "FABRIC_TEST_STORAGE_TOKEN",
    "https://api.kusto.windows.net/.default": "FABRIC_TEST_KUSTO_TOKEN",
}


class CachedTokenCredential:
    """Cache access tokens so long operations do not reacquire them late."""

    def __init__(self, credential: TokenCredential) -> None:
        self.credential = credential
        self.tokens: dict[tuple[str, ...], AccessToken] = {}

    def get_token(self, *scopes: str, **kwargs: object) -> AccessToken:
        cached = self.tokens.get(scopes)
        if cached is not None and cached.expires_on > time.time() + 300:
            return cached
        token = self.credential.get_token(*scopes, **kwargs)
        self.tokens[scopes] = token
        return token


class EnvironmentTokenCredential:
    """Use explicitly enabled, short-lived integration-test tokens."""

    @staticmethod
    def _expires_on(token: str, variable: str) -> int:
        try:
            payload = token.split(".")[1]
            payload += "=" * (-len(payload) % 4)
            claims = json.loads(base64.urlsafe_b64decode(payload))
            expires_on = int(claims["exp"])
        except (
            binascii.Error,
            IndexError,
            KeyError,
            TypeError,
            UnicodeDecodeError,
            ValueError,
            json.JSONDecodeError,
        ):
            raise RuntimeError(
                f"{variable} must be a JWT access token with an exp claim"
            ) from None
        return expires_on

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
        return AccessToken(token, self._expires_on(token, variable))


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
