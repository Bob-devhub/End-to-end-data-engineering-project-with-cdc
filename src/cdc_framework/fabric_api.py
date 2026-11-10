"""Minimal Fabric REST API client (stdlib only)."""

from __future__ import annotations

import json
import urllib.request

API = "https://api.fabric.microsoft.com/v1"


def list_items(workspace_id: str, item_type: str) -> dict[str, str]:
    """{displayName: itemId} for every item of item_type in the workspace."""
    from cdc_framework.auth import get_credential

    token = get_credential().get_token("https://api.fabric.microsoft.com/.default").token
    url = f"{API}/workspaces/{workspace_id}/items?type={item_type}"
    items: dict[str, str] = {}
    while url:
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
        with urllib.request.urlopen(req, timeout=60) as resp:
            body = json.load(resp)
        items.update({i["displayName"]: i["id"] for i in body.get("value", [])})
        url = body.get("continuationUri")
    return items
