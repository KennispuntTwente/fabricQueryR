"""Explicit Azure credentials for local and CI execution."""

from __future__ import annotations

from os import environ

from azure.core.credentials import TokenCredential
from azure.identity import AzureCliCredential, WorkloadIdentityCredential


def get_credential() -> TokenCredential:
    if environ.get("AZURE_FEDERATED_TOKEN_FILE"):
        return WorkloadIdentityCredential()
    return AzureCliCredential()