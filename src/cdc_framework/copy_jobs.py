"""Render Fabric Copy job item definitions from metadata - one Copy job per copy_job_group.

Output is fabric/<cj_name>.CopyJob/{.platform,copyjob-content.json}, the same
format Fabric Git integration writes, so fabric-cicd deploys them like any item.

The definition shape was captured from a Copy job built in the UI and synced to
Git (cj_wwi_cdc_scd2, cj_wwi_cdc_merge, cj_wwi_watermark). Only (read_method, write_method) combinations with a
captured example are rendered; others raise UnsupportedCopyJob until one is
captured (see docs/copy-job-template.md).
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from pathlib import Path

from cdc_framework.config import Entity, Metadata

# Fabric replaces this with the target workspace at deploy time ("same workspace").
SAME_WORKSPACE = "00000000-0000-0000-0000-000000000000"
_ID_NAMESPACE = uuid.UUID("2f3e7a0c-5d1b-4c7e-9a64-0b6f1f6f2a11")

# (read_method, write_method) -> job/activity settings, as captured from Fabric
# (cj_wwi_cdc_scd2, cj_wwi_cdc_merge, cj_wwi_watermark). The UI's "Merge" is
# stored as "Upsert". Watermark jobs use the same jobMode/readMethod as CDC and
# differ only by the incremental column in changeDataSettings.
_JOB_SETTINGS = {
    ("cdc", "scd2"): {"jobMode": "CDC", "readMethod": "SnapshotPlusIncremental", "writeBehavior": "SCD2"},
    ("cdc", "scd1_merge"): {"jobMode": "CDC", "readMethod": "SnapshotPlusIncremental", "writeBehavior": "Upsert"},
    ("watermark", "scd1_merge"): {
        "jobMode": "CDC",
        "readMethod": "SnapshotPlusIncremental",
        "writeBehavior": "Upsert",
        "watermark": True,
    },
}

# SQL Server type -> (Copy interim type, Delta physicalType).
# Confirmed against a captured definition: int, nvarchar, decimal, date, bit, datetime2.
# The rest follow the same conventions but haven't been verified yet.
_SQL_TYPES: dict[str, tuple[str, str]] = {
    "int": ("Int32", "integer"),
    "bigint": ("Int64", "long"),
    "smallint": ("Int16", "short"),
    "tinyint": ("Byte", "short"),
    "bit": ("Boolean", "boolean"),
    "decimal": ("Decimal", "decimal"),
    "numeric": ("Decimal", "decimal"),
    "money": ("Decimal", "decimal"),
    "smallmoney": ("Decimal", "decimal"),
    "float": ("Double", "double"),
    "real": ("Single", "float"),
    "date": ("DateTime", "date"),
    "datetime": ("DateTime", "timestamp"),
    "datetime2": ("DateTime", "timestamp"),
    "smalldatetime": ("DateTime", "timestamp"),
    "nvarchar": ("String", "string"),
    "varchar": ("String", "string"),
    "nchar": ("String", "string"),
    "char": ("String", "string"),
    "uniqueidentifier": ("Guid", "string"),
    "varbinary": ("Byte[]", "binary"),
    "binary": ("Byte[]", "binary"),
}
UNSUPPORTED_SQL_TYPES = {"geography", "geometry", "hierarchyid", "sql_variant", "xml", "image", "text", "ntext"}
_CHAR_TYPES = {"nvarchar", "varchar", "nchar", "char"}
_DECIMAL_TYPES = {"decimal", "numeric", "money", "smallmoney"}
_PRECISION_TYPES = {"datetime2", "datetimeoffset", "time"}


class UnsupportedCopyJob(Exception):
    """No captured Fabric example for this read/write combination yet."""


class CopyJobRenderError(Exception):
    """Metadata can't be rendered (missing ids, unsupported column types, ...)."""


def copy_job_name(group: str) -> str:
    return f"cj_{group}"


def load_source_schemas(schemas_dir: Path) -> dict[str, dict[str, list[dict]]]:
    """{system_id: {"Schema.Table": [column, ...]}} from metadata/source_schemas/*.json."""
    if not schemas_dir.exists():
        return {}
    return {p.stem: json.loads(p.read_text(encoding="utf-8")) for p in sorted(schemas_dir.glob("*.json"))}


def lakehouse_logical_id(fabric_dir: Path, lakehouse: str) -> str:
    platform = fabric_dir / f"{lakehouse}.Lakehouse" / ".platform"
    if not platform.exists():
        raise CopyJobRenderError(f"{platform} not found - create {lakehouse} in Fabric and commit it first")
    return json.loads(platform.read_text(encoding="utf-8"))["config"]["logicalId"]


def _column_mapping(col: dict) -> dict:
    sql_type = col["data_type"].lower()
    interim, delta = _SQL_TYPES[sql_type]
    source = {"name": col["name"], "type": interim, "physicalType": sql_type}
    destination = {"name": col["name"], "physicalType": delta}
    if sql_type in _CHAR_TYPES:
        source["length"] = str(col["max_length"])
    if sql_type in _DECIMAL_TYPES:
        for side in (source, destination):
            side["scale"] = col["scale"]
            side["precision"] = col["precision"]
    if sql_type in _PRECISION_TYPES and col.get("datetime_precision") is not None:
        source["precision"] = col["datetime_precision"]
    return {"source": source, "destination": destination}


def _translator(e: Entity, columns: list[dict] | None) -> dict:
    """Auto-mapping unless columns are excluded; unsupported types must be excluded."""
    excluded = {c.lower() for c in e.source.exclude_columns}
    if columns is None:
        if excluded:
            raise CopyJobRenderError(
                f"{e.entity_id}: exclude_columns needs a source schema snapshot - "
                "run scripts/snapshot_source_schema.py"
            )
        return {"type": "TabularTranslator"}

    names = {c["name"].lower() for c in columns}
    unknown = sorted(excluded - names)
    if unknown:
        raise CopyJobRenderError(f"{e.entity_id}: exclude_columns not in source table: {unknown}")

    kept = [c for c in columns if c["name"].lower() not in excluded]
    bad = [f"{c['name']} ({c['data_type']})" for c in kept if c["data_type"].lower() not in _SQL_TYPES]
    if bad:
        raise CopyJobRenderError(f"{e.entity_id}: unsupported column types {bad} - add them to exclude_columns")

    if not excluded:
        return {"type": "TabularTranslator"}
    return {"type": "TabularTranslator", "mappings": [_column_mapping(c) for c in kept]}


def _change_data_settings(e: Entity, settings: dict, columns: list[dict] | None) -> dict:
    cds: dict = {"readMethod": settings["readMethod"]}
    if not settings.get("watermark"):
        return cds
    col = next((c for c in columns or [] if c["name"].lower() == (e.load.watermark_column or "").lower()), None)
    if col is None:
        raise CopyJobRenderError(
            f"{e.entity_id}: watermark column '{e.load.watermark_column}' not in source schema snapshot - "
            "run scripts/snapshot_source_schema.py"
        )
    sql_type = col["data_type"].lower()
    if sql_type not in _SQL_TYPES:
        raise CopyJobRenderError(f"{e.entity_id}: unsupported watermark column type {sql_type}")
    cds["columns"] = [{"name": col["name"], "type": _SQL_TYPES[sql_type][0], "physicalType": sql_type}]
    cds["nullWatermarkBehavior"] = "Skip"
    return cds


def _activity(e: Entity, settings: dict, columns: list[dict] | None, activity_id: str) -> dict:
    return {
        "id": activity_id,
        "properties": {
            "source": {
                "datasetSettings": {"schema": e.source.schema_name, "table": e.source.table},
                "changeDataSettings": _change_data_settings(e, settings, columns),
                "partitionSettings": {"partitionOption": "None"},
            },
            "destination": {
                "upsertSettings": {"keys": list(e.load.primary_keys)},
                "writeBehavior": settings["writeBehavior"],
                "datasetSettings": {"schema": e.target.schema_name, "table": e.target.table},
                "applyVOrder": False,
                "enableTimestampNtz": False,
            },
            "enableStaging": False,
            "translator": _translator(e, columns),
            "typeConversionSettings": {
                "typeConversion": {"allowDataTruncation": True, "treatBooleanAsNumber": False}
            },
        },
    }


def _existing_activity_ids(content: dict | None) -> dict[str, str]:
    """Keep activity ids stable across renders: {"Schema.Table": id}."""
    ids = {}
    for a in (content or {}).get("activities", []):
        ds = a.get("properties", {}).get("source", {}).get("datasetSettings", {})
        ids[f"{ds.get('schema')}.{ds.get('table')}"] = a["id"]
    return ids


def render_copy_job(
    md: Metadata,
    group: str,
    *,
    fabric_dir: Path,
    source_schemas: dict[str, dict[str, list[dict]]],
    existing: dict | None = None,
) -> dict:
    members = md.copy_job_groups()[group]
    first = members[0]
    key = (first.load.read_method, first.load.write_method)
    settings = _JOB_SETTINGS.get(key)
    if settings is None:
        raise UnsupportedCopyJob(f"{copy_job_name(group)}: no captured example for read/write {key}")

    system = md.system(first.system_id)
    if not system.fabric_connection_id:
        raise CopyJobRenderError(f"{system.system_id}: fabric_connection_id missing in source_systems.yml")
    lakehouses = {m.target.lakehouse for m in members}
    if len(lakehouses) > 1:
        raise CopyJobRenderError(f"{copy_job_name(group)}: one Copy job writes to one lakehouse, got {lakehouses}")
    lakehouse = lakehouses.pop()
    lh_connection = md.fabric.lakehouse_connections.get(lakehouse)
    if not lh_connection:
        raise CopyJobRenderError(f"{lakehouse}: add its connection id to metadata/config/fabric.yml")

    schema = source_schemas.get(system.system_id, {})
    old_ids = _existing_activity_ids(existing)
    activities = []
    for e in members:
        table_key = f"{e.source.schema_name}.{e.source.table}"
        activity_id = old_ids.get(table_key) or str(uuid.uuid5(_ID_NAMESPACE, f"{group}:{e.entity_id}"))
        activities.append(_activity(e, settings, schema.get(table_key), activity_id))

    return {
        "properties": {
            "jobMode": settings["jobMode"],
            "fullLoadBehavior": "Truncate",
            "source": {
                "type": "AzureSqlTable",
                "connectionSettings": {
                    "type": "AzureSqlDatabase",
                    "externalReferences": {"connection": system.fabric_connection_id},
                },
            },
            "destination": {
                "type": "LakehouseTable",
                "connectionSettings": {
                    "type": "Lakehouse",
                    "typeProperties": {
                        "workspaceId": SAME_WORKSPACE,
                        "artifactId": lakehouse_logical_id(fabric_dir, lakehouse),
                        "rootFolder": "Tables",
                    },
                    "externalReferences": {"connection": lh_connection},
                },
            },
            "policy": {"timeout": "0.12:00:00", "retry": 0},
        },
        "activities": activities,
    }


def _platform(name: str, logical_id: str) -> dict:
    return {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
        "metadata": {"type": "CopyJob", "displayName": name},
        "config": {"version": "2.0", "logicalId": logical_id},
    }


@dataclass
class RenderResult:
    name: str
    status: str  # written | unchanged | drift | skipped | error
    detail: str = ""


def write_copy_jobs(md: Metadata, fabric_dir: Path, schemas_dir: Path, *, check: bool = False) -> list[RenderResult]:
    """Render every copy_job_group into fabric/. With check=True, only compare."""
    schemas = load_source_schemas(schemas_dir)
    results = []
    for group in sorted(md.copy_job_groups()):
        name = copy_job_name(group)
        item_dir = fabric_dir / f"{name}.CopyJob"
        content_path = item_dir / "copyjob-content.json"
        existing = json.loads(content_path.read_text(encoding="utf-8")) if content_path.exists() else None
        try:
            content = render_copy_job(md, group, fabric_dir=fabric_dir, source_schemas=schemas, existing=existing)
        except UnsupportedCopyJob as exc:
            results.append(RenderResult(name, "skipped", str(exc)))
            continue
        except CopyJobRenderError as exc:
            results.append(RenderResult(name, "error", str(exc)))
            continue

        if existing == content:
            results.append(RenderResult(name, "unchanged"))
            continue
        if check:
            results.append(RenderResult(name, "drift", "Fabric definition differs from metadata"))
            continue

        item_dir.mkdir(parents=True, exist_ok=True)
        platform_path = item_dir / ".platform"
        if not platform_path.exists():
            logical_id = str(uuid.uuid5(_ID_NAMESPACE, f"copyjob:{name}"))
            platform_path.write_text(json.dumps(_platform(name, logical_id), indent=2), encoding="utf-8")
        content_path.write_text(json.dumps(content, indent=2), encoding="utf-8")
        results.append(RenderResult(name, "written"))
    return results
