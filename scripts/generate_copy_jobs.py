"""Render Copy job definitions from metadata into fabric/<cj_name>.CopyJob/.

    python scripts/generate_copy_jobs.py           # write/update definitions
    python scripts/generate_copy_jobs.py --check   # CI: fail if fabric/ drifted from metadata

Groups whose read/write combination has no captured Fabric example yet are
skipped (see docs/copy-job-template.md). After writing, commit and let Fabric
Git integration (DEV) or deploy/deploy.py (TEST/PROD) pick them up.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from cdc_framework.config import load_metadata
from cdc_framework.copy_jobs import write_copy_jobs


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--config-dir", default="metadata/config")
    parser.add_argument("--schemas-dir", default="metadata/source_schemas")
    parser.add_argument("--fabric-dir", default="fabric")
    parser.add_argument("--check", action="store_true", help="compare only; exit 1 on drift or errors")
    args = parser.parse_args()

    results = write_copy_jobs(
        load_metadata(args.config_dir), Path(args.fabric_dir), Path(args.schemas_dir), check=args.check
    )
    for r in results:
        print(f"{r.status:<10} {r.name}" + (f"  - {r.detail}" if r.detail else ""))
    failed = [r for r in results if r.status == "error" or (args.check and r.status == "drift")]
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
