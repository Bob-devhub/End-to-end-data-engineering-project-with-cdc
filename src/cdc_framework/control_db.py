"""Sync validated metadata into the control database (SQL database in Fabric).

Config tables are fully reloaded from Git in one transaction (Git is the source
of truth). Operational tables (watermark, run_log, dq_result) are never touched.
"""

from __future__ import annotations

import json
import struct
from collections.abc import Iterable
from typing import Any

from cdc_framework.config import Metadata

SQL_COPT_SS_ACCESS_TOKEN = 1256  # msodbcsql pre-connect attribute for AAD tokens

# Child tables first so deletes respect foreign keys.
CONFIG_TABLES = [
    "ctl.alert_rule",
    "ctl.stream_entity",
    "ctl.dq_rule",
    "ctl.column_mapping",
    "ctl.source_entity",
    "ctl.source_system",
]


def connect(server: str, database: str):
    """Connect with Entra ID (az login locally, OIDC service principal in GitHub Actions)."""
    import pyodbc

    from cdc_framework.auth import get_credential

    token = get_credential().get_token("https://database.windows.net/.default").token
    raw = token.encode("utf-16-le")
    token_struct = struct.pack(f"<I{len(raw)}s", len(raw), raw)
    conn_str = (
        "Driver={ODBC Driver 18 for SQL Server};"
        f"Server=tcp:{server},1433;Database={database};Encrypt=yes;TrustServerCertificate=no;"
        "Connection Timeout=120;"  # serverless / Fabric SQL databases can take a while to resume
    )
    return pyodbc.connect(conn_str, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct})


def build_rows(md: Metadata) -> dict[str, list[tuple[Any, ...]]]:
    """Flatten metadata into rows per control table (pure function, testable)."""
    rows: dict[str, list[tuple[Any, ...]]] = {t: [] for t in CONFIG_TABLES}
    for s in md.source_systems:
        rows["ctl.source_system"].append(
            (s.system_id, s.type, s.description, s.connection_name, s.database, s.container, s.base_path)
        )
    for e in md.entities:
        rows["ctl.source_entity"].append(
            (
                e.entity_id, e.system_id, e.description, e.is_active, e.priority,
                e.load.ingestion_engine, e.load.read_method, e.load.write_method, e.load.copy_job_group,
                e.source.schema_name, e.source.table, e.source.file_pattern, e.source.format,
                e.target.lakehouse, e.target.bronze_name, e.target.silver_name,
                ",".join(e.load.primary_keys) or None, e.load.watermark_column,
                e.model_dump_json(),
            )
        )
        for ordinal, c in enumerate(e.columns, start=1):
            rows["ctl.column_mapping"].append(
                (e.entity_id, ordinal, c.source, c.target_name, c.type, c.nullable)
            )
        for r in e.dq_rules:
            params = r.model_dump(include={"min", "max", "pattern", "values"}, exclude_none=True)
            rows["ctl.dq_rule"].append(
                (e.entity_id, r.rule_type, r.column, r.action, r.threshold, json.dumps(params))
            )
    for st in md.streams:
        rows["ctl.stream_entity"].append(
            (
                st.stream_id, st.event_type, st.description, st.is_active, st.table, st.time_column,
                st.retention_days, st.onelake_availability, st.model_dump_json(),
            )
        )
        for a in st.alerts:
            rows["ctl.alert_rule"].append(
                (st.stream_id, a.name, a.description, a.severity, a.action, a.lookback, a.condition)
            )
    return rows


_INSERTS = {
    "ctl.source_system": (
        "INSERT INTO ctl.source_system (system_id, source_type, description, connection_name, "
        "database_name, container, base_path) VALUES (?,?,?,?,?,?,?)"
    ),
    "ctl.source_entity": (
        "INSERT INTO ctl.source_entity (entity_id, system_id, description, is_active, priority, "
        "ingestion_engine, read_method, write_method, copy_job_group, source_schema, source_table, "
        "file_pattern, file_format, target_lakehouse, bronze_table, silver_table, primary_keys, "
        "watermark_column, config_json) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
    ),
    "ctl.column_mapping": (
        "INSERT INTO ctl.column_mapping (entity_id, ordinal, source_column, target_column, data_type, "
        "is_nullable) VALUES (?,?,?,?,?,?)"
    ),
    "ctl.dq_rule": (
        "INSERT INTO ctl.dq_rule (entity_id, rule_type, column_name, action, threshold, params_json) "
        "VALUES (?,?,?,?,?,?)"
    ),
    "ctl.stream_entity": (
        "INSERT INTO ctl.stream_entity (stream_id, event_type, description, is_active, kql_table, "
        "time_column, retention_days, onelake_availability, config_json) VALUES (?,?,?,?,?,?,?,?,?)"
    ),
    "ctl.alert_rule": (
        "INSERT INTO ctl.alert_rule (stream_id, alert_name, description, severity, action, lookback, "
        "condition_expr) VALUES (?,?,?,?,?,?,?)"
    ),
}


def sync(conn, md: Metadata) -> dict[str, int]:
    rows = build_rows(md)
    cur = conn.cursor()
    cur.fast_executemany = True
    try:
        for table in CONFIG_TABLES:
            cur.execute(f"DELETE FROM {table}")
        for table in reversed(CONFIG_TABLES):  # parents first
            _executemany(cur, _INSERTS[table], rows[table])
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return {t: len(r) for t, r in rows.items()}


def _executemany(cur, sql: str, rows: Iterable[tuple]) -> None:
    rows = list(rows)
    if rows:
        cur.executemany(sql, rows)
