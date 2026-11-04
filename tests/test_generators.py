from pathlib import Path

from cdc_framework.config import load_metadata
from cdc_framework.control_db import CONFIG_TABLES, build_rows
from cdc_framework.copy_jobs import build_specs
from cdc_framework.kql import alert_query, json_path_to_accessor, render_script

CONFIG_DIR = Path(__file__).resolve().parents[1] / "metadata" / "config"
MD = load_metadata(CONFIG_DIR)


def test_json_path_accessor():
    assert json_path_to_accessor("$.store_id") == "event['store_id']"
    assert json_path_to_accessor("$.a.b") == "event['a']['b']"


def test_kql_script_has_objects_for_every_stream():
    script = render_script(MD.streams)
    assert ".create-merge table events_raw (event: dynamic)" in script
    for s in MD.streams:
        assert f".create-merge table {s.table} (" in script
        assert f"fn_parse_{s.stream_id}()" in script
        assert f"== '{s.event_type}'" in script
        for agg in s.aggregates:
            assert f"materialized-view {agg.name} on table {s.table}" in script
    assert "policy mirroring" in script  # OneLake availability for at least one stream


def test_alert_query():
    iot = next(s for s in MD.streams if s.stream_id == "iot_readings")
    q = alert_query(iot, iot.alerts[0])
    assert q.splitlines() == [
        "iot_readings",
        "| where event_time > ago(5m)",
        "| where sensor_type == 'freezer'",
        "| summarize avg_temp_c = avg(value) by store_id, device_id",
        "| where avg_temp_c > -15",
    ]


def test_copy_job_specs():
    specs = {s["name"]: s for s in build_specs(MD)}
    assert set(specs) == {"cj_wwi_cdc_scd2", "cj_wwi_cdc_merge", "cj_wwi_watermark"}
    assert specs["cj_wwi_cdc_scd2"]["write_method"] == "SCD Type 2"
    assert specs["cj_wwi_watermark"]["tables"][0]["watermark_column"] == "LastEditedWhen"
    for spec in specs.values():
        for t in spec["tables"]:
            assert "Valid_From" not in t.get("columns", [])


def test_control_db_rows():
    rows = build_rows(MD)
    assert set(rows) == set(CONFIG_TABLES)
    assert len(rows["ctl.source_entity"]) == len(MD.entities)
    assert len(rows["ctl.alert_rule"]) == sum(len(s.alerts) for s in MD.streams)
