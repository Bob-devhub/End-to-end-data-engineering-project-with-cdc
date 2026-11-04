"""Generate Eventhouse (KQL) objects from stream metadata.

Design: Eventstream lands every event, untouched, into one raw table
(events_raw: event:dynamic). Each stream gets a parse function and an update
policy that routes rows by $.event_type into a typed table, plus materialized
views for windowed aggregates and a query per alert rule (for Activator /
Real-Time Dashboard tiles).
"""

from __future__ import annotations

import json

from cdc_framework.config import AlertRule, Stream, StreamAggregate

RAW_TABLE = "events_raw"
RAW_MAPPING = "events_raw_mapping"
RAW_RETENTION_DAYS = 7

_CONVERTERS = {
    "string": "tostring",
    "int": "toint",
    "long": "tolong",
    "real": "toreal",
    "decimal": "todecimal",
    "datetime": "todatetime",
    "bool": "tobool",
    "dynamic": "todynamic",
    "guid": "toguid",
}


def _kql_string(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def json_path_to_accessor(path: str, root: str = "event") -> str:
    """$.a.b -> event['a']['b']"""
    if not path.startswith("$"):
        raise ValueError(f"json path must start with '$': {path}")
    parts = [p for p in path.lstrip("$").split(".") if p]
    return root + "".join(f"[{_kql_string(p)}]" for p in parts)


def raw_table_commands() -> list[str]:
    mapping = json.dumps([{"column": "event", "Properties": {"Path": "$"}}])
    return [
        f".create-merge table {RAW_TABLE} (event: dynamic)",
        f".create-or-alter table {RAW_TABLE} ingestion json mapping '{RAW_MAPPING}' '{mapping}'",
        f".alter-merge table {RAW_TABLE} policy retention softdelete = {RAW_RETENTION_DAYS}d",
    ]


def parse_function_name(stream: Stream) -> str:
    return f"fn_parse_{stream.stream_id}"


def stream_commands(stream: Stream) -> list[str]:
    cols = ", ".join(f"{f.name}: {f.type}" for f in stream.fields)
    projections = ",\n        ".join(
        f"{f.name} = {_CONVERTERS[f.type]}({json_path_to_accessor(f.path)})" for f in stream.fields
    )
    fn = parse_function_name(stream)
    update_policy = json.dumps(
        [{"IsEnabled": True, "Source": RAW_TABLE, "Query": f"{fn}()", "IsTransactional": True}]
    )
    cmds = [
        f".create-merge table {stream.table} ({cols})",
        (
            f".create-or-alter function with (folder='parse', docstring={_kql_string(stream.description)})"
            f" {fn}()\n{{\n    {RAW_TABLE}\n"
            f"    | where tostring(event['event_type']) == {_kql_string(stream.event_type)}\n"
            f"    | project\n        {projections}\n}}"
        ),
        f".alter table {stream.table} policy update @'{update_policy}'",
        f".alter-merge table {stream.table} policy retention softdelete = {stream.retention_days}d",
    ]
    if stream.onelake_availability:
        # OneLake availability: table is exposed as Delta in OneLake for Lakehouse/Spark/Direct Lake.
        cmds.append(
            f".alter-merge table {stream.table} policy mirroring dataformat=parquet with (IsEnabled=true)"
        )
    cmds.extend(materialized_view_command(stream, agg) for agg in stream.aggregates)
    return cmds


def materialized_view_command(stream: Stream, agg: StreamAggregate) -> str:
    measures = ", ".join(f"{name} = {expr}" for name, expr in agg.measures.items())
    by = ", ".join([*agg.group_by, f"window_start = bin({stream.time_column}, {agg.window})"])
    return (
        f".create-or-alter materialized-view {agg.name} on table {stream.table}\n{{\n"
        f"    {stream.table}\n    | summarize {measures} by {by}\n}}"
    )


def alert_query(stream: Stream, alert: AlertRule) -> str:
    lines = [stream.table, f"| where {stream.time_column} > ago({alert.lookback})"]
    if alert.filter:
        lines.append(f"| where {alert.filter}")
    by = f" by {', '.join(alert.group_by)}" if alert.group_by else ""
    lines.append(f"| summarize {alert.summarize}{by}")
    lines.append(f"| where {alert.condition}")
    return "\n".join(lines)


def render_script(streams: list[Stream]) -> str:
    """Full idempotent deployment script for the KQL database."""
    active = [s for s in streams if s.is_active]
    blocks = ["// AUTO-GENERATED from metadata/config/streams - do not edit by hand.", ""]
    blocks.extend(raw_table_commands())
    for s in active:
        blocks.append(f"\n// ---- stream: {s.stream_id} ({s.event_type})")
        blocks.extend(stream_commands(s))
    return "\n\n".join(blocks) + "\n"


def all_commands(streams: list[Stream]) -> list[str]:
    cmds = raw_table_commands()
    for s in streams:
        if s.is_active:
            cmds.extend(stream_commands(s))
    return cmds
