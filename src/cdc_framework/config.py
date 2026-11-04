"""Typed metadata model + loader.

The YAML files under metadata/config are the source of truth. They are validated
here (locally and in CI), then synced into the control database and used to
generate Copy job specs and KQL objects.
"""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

SourceType = Literal["azure_sql", "adls"]
IngestionEngine = Literal["copy_job", "pipeline_copy", "notebook"]
ReadMethod = Literal["cdc", "watermark", "full", "new_files"]
WriteMethod = Literal["append", "overwrite", "scd1_merge", "scd2"]
DqAction = Literal["warn", "quarantine", "fail"]
KqlType = Literal["string", "int", "long", "real", "decimal", "datetime", "bool", "dynamic", "guid"]


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid")


# --------------------------------------------------------------------------- batch / CDC


class SourceSystem(_Model):
    system_id: str
    type: SourceType
    description: str = ""
    connection_name: str
    database: str | None = None
    container: str | None = None
    base_path: str | None = None

    @model_validator(mode="after")
    def _check_type_fields(self) -> SourceSystem:
        if self.type == "azure_sql" and not self.database:
            raise ValueError(f"{self.system_id}: azure_sql source requires 'database'")
        if self.type == "adls" and not self.container:
            raise ValueError(f"{self.system_id}: adls source requires 'container'")
        return self


class ColumnMapping(_Model):
    source: str
    target: str | None = None
    type: str | None = None
    nullable: bool = True

    @property
    def target_name(self) -> str:
        return self.target or self.source


class DqRule(_Model):
    rule_type: Literal["not_null", "unique", "range", "regex", "accepted_values"]
    column: str
    action: DqAction = "warn"
    min: float | None = None
    max: float | None = None
    pattern: str | None = None
    values: list[str | int | float] | None = None
    threshold: float = Field(0.0, ge=0.0, le=1.0, description="Allowed failing fraction before action")

    @model_validator(mode="after")
    def _check_params(self) -> DqRule:
        if self.rule_type == "range" and self.min is None and self.max is None:
            raise ValueError(f"range rule on {self.column} needs min and/or max")
        if self.rule_type == "regex" and not self.pattern:
            raise ValueError(f"regex rule on {self.column} needs pattern")
        if self.rule_type == "accepted_values" and not self.values:
            raise ValueError(f"accepted_values rule on {self.column} needs values")
        return self


class EntitySource(_Model):
    schema_name: str | None = None
    table: str | None = None
    file_pattern: str | None = None
    format: Literal["csv", "json", "parquet"] | None = None
    options: dict[str, str] = Field(default_factory=dict)


class EntityTarget(_Model):
    lakehouse: str = "lh_bronze"
    table: str
    silver_table: str


class LoadConfig(_Model):
    ingestion_engine: IngestionEngine
    read_method: ReadMethod
    write_method: WriteMethod
    copy_job_group: str | None = None
    primary_keys: list[str] = Field(default_factory=list)
    watermark_column: str | None = None

    @model_validator(mode="after")
    def _check_combinations(self) -> LoadConfig:
        if self.ingestion_engine == "copy_job" and not self.copy_job_group:
            raise ValueError("ingestion_engine=copy_job requires copy_job_group")
        if self.ingestion_engine != "copy_job" and self.copy_job_group:
            raise ValueError("copy_job_group is only valid with ingestion_engine=copy_job")
        if self.read_method == "watermark" and not self.watermark_column:
            raise ValueError("read_method=watermark requires watermark_column")
        if self.write_method in ("scd1_merge", "scd2") and not self.primary_keys:
            raise ValueError(f"write_method={self.write_method} requires primary_keys")
        if self.read_method == "cdc" and not self.primary_keys:
            raise ValueError("read_method=cdc requires primary_keys")
        if self.read_method == "new_files" and self.ingestion_engine != "notebook":
            raise ValueError("read_method=new_files is only supported by the notebook engine")
        return self


class Entity(_Model):
    entity_id: str
    system_id: str
    description: str = ""
    is_active: bool = True
    priority: int = 100
    source: EntitySource
    target: EntityTarget
    load: LoadConfig
    columns: list[ColumnMapping] = Field(default_factory=list)
    dq_rules: list[DqRule] = Field(default_factory=list)

    @model_validator(mode="after")
    def _check_dq_columns(self) -> Entity:
        if self.columns:
            targets = {c.target_name for c in self.columns}
            missing = [r.column for r in self.dq_rules if r.column not in targets]
            if missing:
                raise ValueError(f"{self.entity_id}: dq_rules reference unmapped columns {missing}")
        return self


# --------------------------------------------------------------------------- real-time


class StreamField(_Model):
    name: str
    path: str
    type: KqlType


class StreamAggregate(_Model):
    name: str
    window: str
    group_by: list[str] = Field(default_factory=list)
    measures: dict[str, str]


class AlertRule(_Model):
    name: str
    description: str = ""
    lookback: str
    filter: str | None = None
    summarize: str
    group_by: list[str] = Field(default_factory=list)
    condition: str
    severity: Literal["low", "medium", "high"] = "medium"
    action: Literal["email", "teams", "pipeline"] = "teams"


class Stream(_Model):
    stream_id: str
    event_type: str
    description: str = ""
    is_active: bool = True
    table: str
    time_column: str = "event_time"
    retention_days: int = 365
    onelake_availability: bool = False
    fields: list[StreamField]
    aggregates: list[StreamAggregate] = Field(default_factory=list)
    alerts: list[AlertRule] = Field(default_factory=list)

    @model_validator(mode="after")
    def _check_time_column(self) -> Stream:
        names = {f.name for f in self.fields}
        if self.time_column not in names:
            raise ValueError(f"{self.stream_id}: time_column '{self.time_column}' not in fields")
        for agg in self.aggregates:
            bad = [g for g in agg.group_by if g not in names]
            if bad:
                raise ValueError(f"{self.stream_id}.{agg.name}: unknown group_by columns {bad}")
        return self


# --------------------------------------------------------------------------- whole project


class Metadata(_Model):
    source_systems: list[SourceSystem]
    entities: list[Entity] = Field(default_factory=list)
    streams: list[Stream] = Field(default_factory=list)

    @model_validator(mode="after")
    def _check_cross_references(self) -> Metadata:
        _require_unique("system_id", [s.system_id for s in self.source_systems])
        _require_unique("entity_id", [e.entity_id for e in self.entities])
        _require_unique("stream_id", [s.stream_id for s in self.streams])
        _require_unique("stream table", [s.table for s in self.streams])

        systems = {s.system_id: s for s in self.source_systems}
        for e in self.entities:
            system = systems.get(e.system_id)
            if system is None:
                raise ValueError(f"{e.entity_id}: unknown system_id '{e.system_id}'")
            if system.type == "azure_sql" and not (e.source.schema_name and e.source.table):
                raise ValueError(f"{e.entity_id}: azure_sql entity requires source.schema_name and table")
            if system.type == "adls" and not (e.source.file_pattern and e.source.format):
                raise ValueError(f"{e.entity_id}: adls entity requires source.file_pattern and format")
            if e.load.read_method == "cdc" and system.type != "azure_sql":
                raise ValueError(f"{e.entity_id}: read_method=cdc requires an azure_sql source")

        # Copy job applies read + write method to the whole job:
        #  - mixing CDC and non-CDC tables makes every table fall back to watermark
        #  - SCD2 history/soft-delete is applied consistently to all selected tables
        for name, members in self.copy_job_groups().items():
            checks = {
                "system_id": {m.system_id for m in members},
                "read_method": {m.load.read_method for m in members},
                "write_method": {m.load.write_method for m in members},
            }
            for attr, values in checks.items():
                if len(values) > 1:
                    raise ValueError(f"copy_job_group '{name}' mixes {attr}: {sorted(values)}")
        return self

    def active_entities(self) -> list[Entity]:
        return sorted((e for e in self.entities if e.is_active), key=lambda e: (e.priority, e.entity_id))

    def copy_job_entities(self) -> list[Entity]:
        return [e for e in self.active_entities() if e.load.ingestion_engine == "copy_job"]

    def copy_job_groups(self) -> dict[str, list[Entity]]:
        groups: dict[str, list[Entity]] = defaultdict(list)
        for e in self.copy_job_entities():
            groups[e.load.copy_job_group].append(e)  # type: ignore[index]
        return dict(groups)

    def system(self, system_id: str) -> SourceSystem:
        return next(s for s in self.source_systems if s.system_id == system_id)


def _require_unique(label: str, values: list[str]) -> None:
    seen: set[str] = set()
    dupes = {v for v in values if v in seen or seen.add(v)}  # type: ignore[func-returns-value]
    if dupes:
        raise ValueError(f"duplicate {label}: {sorted(dupes)}")


def _read_yaml(path: Path) -> dict:
    with path.open(encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    if not isinstance(data, dict):
        raise ValueError(f"{path}: expected a mapping at top level")
    return data


def load_metadata(config_dir: str | Path = "metadata/config") -> Metadata:
    """Load and validate every YAML file under config_dir."""
    root = Path(config_dir)
    systems = _read_yaml(root / "source_systems.yml").get("source_systems", [])
    entities = [_read_yaml(p) for p in sorted((root / "entities").rglob("*.yml"))]
    streams = [_read_yaml(p) for p in sorted((root / "streams").rglob("*.yml"))]
    return Metadata(source_systems=systems, entities=entities, streams=streams)
