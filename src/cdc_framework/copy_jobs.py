"""Build Copy job specs from metadata - one Copy job per copy_job_group.

The spec is a tool-neutral description of what each Copy job must contain.
Turning it into a real Fabric item definition is done by
scripts/generate_copy_jobs.py against a template captured from a Copy job you
configured once in the UI and synced to Git (see docs/copy-job-template.md).
"""

from __future__ import annotations

from cdc_framework.config import Entity, Metadata

# Copy job write method names, as shown in the Fabric UI.
_WRITE_METHOD = {
    "scd1_merge": "Merge",
    "scd2": "SCD Type 2",
    "append": "Append",
    "overwrite": "Overwrite",
}


def copy_job_name(group: str) -> str:
    return f"cj_{group}"


def _table_spec(e: Entity) -> dict:
    spec = {
        "entity_id": e.entity_id,
        "source": {"schema": e.source.schema_name, "table": e.source.table},
        "destination": {"lakehouse": e.target.lakehouse, "table": e.target.table},
        "keys": e.load.primary_keys,
    }
    if e.load.watermark_column:
        spec["watermark_column"] = e.load.watermark_column
    # Bronze keeps source column names; renaming/typing happens in Silver. SCD2 audit
    # columns (Valid_From/Valid_To/Is_Current) are added by Copy job, not read from source.
    scd2_cols = {"Valid_From", "Valid_To", "Is_Current"}
    source_cols = [c.source for c in e.columns if c.source not in scd2_cols]
    if source_cols:
        spec["columns"] = source_cols
    return spec


def build_specs(metadata: Metadata) -> list[dict]:
    specs = []
    for group, members in sorted(metadata.copy_job_groups().items()):
        first = members[0]
        system = metadata.system(first.system_id)
        specs.append(
            {
                "name": copy_job_name(group),
                "copy_job_group": group,
                "source": {
                    "system_id": system.system_id,
                    "type": system.type,
                    "connection_name": system.connection_name,
                    "database": system.database,
                },
                "read_method": first.load.read_method,
                "write_method": _WRITE_METHOD[first.load.write_method],
                "tables": [_table_spec(e) for e in members],
            }
        )
    return specs
