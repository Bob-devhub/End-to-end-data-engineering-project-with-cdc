"""Generate one Copy job spec per copy_job_group into build/copy_jobs/.

    python scripts/generate_copy_jobs.py

Each spec lists the source connection, read method (cdc/watermark), write
method (Merge / SCD Type 2) and tables. Next step (see docs/copy-job-template.md):
render these specs into the Copy job item definition captured from Git, so
fabric-cicd can deploy them like any other item.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from cdc_framework.config import load_metadata
from cdc_framework.copy_jobs import build_specs


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--config-dir", default="metadata/config")
    parser.add_argument("--out-dir", default="build/copy_jobs")
    args = parser.parse_args()

    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    for spec in build_specs(load_metadata(args.config_dir)):
        path = out / f"{spec['name']}.json"
        path.write_text(json.dumps(spec, indent=2) + "\n", encoding="utf-8")
        print(f"wrote {path} ({len(spec['tables'])} tables, {spec['read_method']}/{spec['write_method']})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
