"""Generate (and optionally apply) Eventhouse objects from stream metadata.

    python scripts/deploy_kql.py                      # writes kql/generated/*.kql
    python scripts/deploy_kql.py --apply              # also runs commands against the KQL DB

Apply uses KUSTO_CLUSTER_URI / KUSTO_DATABASE and your az login (see cdc_framework.auth).
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from cdc_framework.config import load_metadata
from cdc_framework.kql import alert_query, all_commands, render_script


def write_files(streams, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "deploy.kql").write_text(render_script(streams), encoding="utf-8")
    print(f"wrote {out_dir / 'deploy.kql'}")
    alerts_dir = out_dir / "alerts"
    alerts_dir.mkdir(exist_ok=True)
    for s in streams:
        for a in s.alerts:
            path = alerts_dir / f"{s.stream_id}__{a.name}.kql"
            header = f"// {a.description}\n// severity={a.severity} action={a.action}\n"
            path.write_text(header + alert_query(s, a) + "\n", encoding="utf-8")
            print(f"wrote {path}")


def apply(streams, cluster: str, database: str) -> None:
    from azure.kusto.data import KustoClient, KustoConnectionStringBuilder

    from cdc_framework.auth import get_credential

    kcsb = KustoConnectionStringBuilder.with_azure_token_credential(cluster, get_credential())
    client = KustoClient(kcsb)
    for cmd in all_commands(streams):
        first_line = cmd.splitlines()[0]
        print(f"> {first_line[:100]}")
        client.execute_mgmt(database, cmd)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--config-dir", default="metadata/config")
    parser.add_argument("--out-dir", default="kql/generated")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--cluster", default=os.getenv("KUSTO_CLUSTER_URI"))
    parser.add_argument("--database", default=os.getenv("KUSTO_DATABASE"))
    args = parser.parse_args()

    streams = load_metadata(args.config_dir).streams
    write_files(streams, Path(args.out_dir))

    if args.apply:
        if not args.cluster or not args.database:
            parser.error("--cluster/--database (or KUSTO_CLUSTER_URI/KUSTO_DATABASE) are required")
        apply(streams, args.cluster, args.database)
    return 0


if __name__ == "__main__":
    sys.exit(main())
