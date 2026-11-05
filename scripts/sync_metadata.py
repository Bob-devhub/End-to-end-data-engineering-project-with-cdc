"""Push validated metadata from Git into the control database.

    python scripts/sync_metadata.py --dry-run
    python scripts/sync_metadata.py --server <host> --database <db>

Auth: az login locally, OIDC (azure/login) in GitHub Actions - see cdc_framework.auth.
"""

from __future__ import annotations

import argparse
import os
import sys

from cdc_framework.config import load_metadata
from cdc_framework.control_db import build_rows, connect, sync


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--config-dir", default="metadata/config")
    parser.add_argument("--server", default=os.getenv("CONTROL_DB_SERVER"))
    parser.add_argument("--database", default=os.getenv("CONTROL_DB_NAME"))
    parser.add_argument("--dry-run", action="store_true", help="print row counts without connecting")
    args = parser.parse_args()

    md = load_metadata(args.config_dir)

    if args.dry_run:
        for table, rows in build_rows(md).items():
            print(f"{table:<22} {len(rows):>4} rows")
        return 0

    if not args.server or not args.database:
        parser.error("--server/--database (or CONTROL_DB_SERVER/CONTROL_DB_NAME) are required")

    with connect(args.server, args.database) as conn:
        counts = sync(conn, md)
    for table, n in counts.items():
        print(f"synced {table:<22} {n:>4} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
