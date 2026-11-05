"""Snapshot source column metadata for Copy job entities into metadata/source_schemas/.

    python scripts/snapshot_source_schema.py            # all azure_sql systems with copy_job entities

The snapshot lets generate_copy_jobs.py build explicit column mappings (needed
whenever exclude_columns is set) without database access, e.g. in CI.
Re-run it after a source schema change and commit the result.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from cdc_framework.config import load_metadata
from cdc_framework.control_db import connect

QUERY = """
SELECT c.COLUMN_NAME, c.DATA_TYPE, c.CHARACTER_MAXIMUM_LENGTH, c.NUMERIC_PRECISION, c.NUMERIC_SCALE,
       c.DATETIME_PRECISION, c.IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS c
WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ?
ORDER BY c.ORDINAL_POSITION
"""


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--config-dir", default="metadata/config")
    parser.add_argument("--out-dir", default="metadata/source_schemas")
    args = parser.parse_args()

    md = load_metadata(args.config_dir)
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)

    for system_id, members in sorted(_entities_by_system(md).items()):
        system = md.system(system_id)
        if not system.server:
            print(f"skip {system_id}: no 'server' in source_systems.yml", file=sys.stderr)
            continue
        snapshot: dict[str, list[dict]] = {}
        with connect(system.server, system.database) as conn:
            cur = conn.cursor()
            for e in members:
                rows = cur.execute(QUERY, e.source.schema_name, e.source.table).fetchall()
                if not rows:
                    raise SystemExit(f"{e.entity_id}: {e.source.schema_name}.{e.source.table} not found")
                snapshot[f"{e.source.schema_name}.{e.source.table}"] = [
                    {
                        "name": r.COLUMN_NAME,
                        "data_type": r.DATA_TYPE,
                        "max_length": r.CHARACTER_MAXIMUM_LENGTH,
                        "precision": r.NUMERIC_PRECISION,
                        "scale": r.NUMERIC_SCALE,
                        "datetime_precision": r.DATETIME_PRECISION,
                        "nullable": r.IS_NULLABLE == "YES",
                    }
                    for r in rows
                ]
        path = out / f"{system_id}.json"
        path.write_text(json.dumps(snapshot, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(f"wrote {path} ({len(snapshot)} tables)")
    return 0


def _entities_by_system(md):
    groups: dict[str, list] = {}
    for e in md.copy_job_entities():
        if md.system(e.system_id).type == "azure_sql":
            groups.setdefault(e.system_id, []).append(e)
    return groups


if __name__ == "__main__":
    sys.exit(main())
