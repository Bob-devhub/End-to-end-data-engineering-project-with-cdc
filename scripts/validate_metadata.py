"""Validate every metadata YAML file. Exit code 1 on any error (used by CI)."""

from __future__ import annotations

import argparse
import sys

from pydantic import ValidationError

from cdc_framework.config import load_metadata


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config-dir", default="metadata/config")
    args = parser.parse_args()

    try:
        md = load_metadata(args.config_dir)
    except (ValidationError, ValueError) as exc:
        print(f"Metadata validation FAILED:\n{exc}", file=sys.stderr)
        return 1

    print(f"OK - {len(md.source_systems)} source systems, {len(md.entities)} entities, "
          f"{len(md.streams)} streams")
    for group, members in sorted(md.copy_job_groups().items()):
        first = members[0].load
        print(f"  copy job cj_{group}: {first.read_method}/{first.write_method} "
              f"-> {[m.entity_id for m in members]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
