"""Entra ID credential shared by all scripts.

Azure CLI first: works locally after `az login` and in GitHub Actions after
azure/login (OIDC). DefaultAzureCredential is the fallback; it's not first
because on Azure Arc-enabled machines its managed identity probe fails hard
before reaching the CLI.
"""

from __future__ import annotations


def get_credential():
    from azure.identity import AzureCliCredential, ChainedTokenCredential, DefaultAzureCredential

    return ChainedTokenCredential(
        AzureCliCredential(process_timeout=30),
        DefaultAzureCredential(exclude_managed_identity_credential=True),
    )
