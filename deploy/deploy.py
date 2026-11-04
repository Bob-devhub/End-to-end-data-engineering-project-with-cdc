"""Deploy Git-synced Fabric items in fabric/ to a target workspace with fabric-cicd.

    python deploy/deploy.py --environment TEST

Environment-specific IDs (lakehouses, connections, KQL DB) are swapped using
deploy/parameter.yml. Auth: DefaultAzureCredential (OIDC service principal in CI).
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

ITEM_TYPES = [
    "Environment",
    "Lakehouse",
    "SQLDatabase",
    "Eventhouse",
    "KQLDatabase",
    "Eventstream",
    "Notebook",
    "CopyJob",
    "DataPipeline",
    "SemanticModel",
    "Report",
    "KQLDashboard",
    "Reflex",
]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--environment", required=True, choices=["DEV", "TEST", "PROD"])
    parser.add_argument("--workspace-id", default=None)
    parser.add_argument("--unpublish-orphans", action="store_true")
    args = parser.parse_args()

    workspace_id = args.workspace_id or os.getenv(f"FABRIC_WORKSPACE_ID_{args.environment}")
    if not workspace_id:
        parser.error(f"--workspace-id or FABRIC_WORKSPACE_ID_{args.environment} is required")

    from azure.identity import DefaultAzureCredential
    from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items

    # Check each type against the fabric-cicd supported item list for your installed version.
    workspace = FabricWorkspace(
        workspace_id=workspace_id,
        environment=args.environment,
        repository_directory=str(ROOT / "fabric"),
        item_type_in_scope=ITEM_TYPES,
        token_credential=DefaultAzureCredential(),
    )
    publish_all_items(workspace)
    if args.unpublish_orphans:
        unpublish_all_orphan_items(workspace)
    return 0


if __name__ == "__main__":
    sys.exit(main())
