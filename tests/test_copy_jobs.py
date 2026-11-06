import copy
import json
from pathlib import Path

import pytest

from cdc_framework.config import Metadata, load_metadata
from cdc_framework.copy_jobs import (
    CopyJobRenderError,
    UnsupportedCopyJob,
    load_source_schemas,
    render_copy_job,
    write_copy_jobs,
)

ROOT = Path(__file__).resolve().parents[1]
FABRIC = ROOT / "fabric"
MD = load_metadata(ROOT / "metadata" / "config")
SCHEMAS = load_source_schemas(ROOT / "metadata" / "source_schemas")


def _captured(group: str) -> dict:
    return json.loads((FABRIC / f"cj_{group}.CopyJob" / "copyjob-content.json").read_text(encoding="utf-8"))


@pytest.mark.parametrize("group", ["wwi_cdc_scd2", "wwi_cdc_merge", "wwi_watermark"])
def test_render_matches_definition_exported_by_fabric(group):
    """Rendering from metadata reproduces the Copy job built in the Fabric UI (compared as JSON)."""
    captured = _captured(group)
    rendered = render_copy_job(MD, group, fabric_dir=FABRIC, source_schemas=SCHEMAS, existing=captured)
    assert rendered == captured


def test_merge_is_stored_as_upsert():
    rendered = render_copy_job(MD, "wwi_cdc_merge", fabric_dir=FABRIC, source_schemas=SCHEMAS)
    assert {a["properties"]["destination"]["writeBehavior"] for a in rendered["activities"]} == {"Upsert"}


def test_new_activity_ids_are_deterministic():
    a = render_copy_job(MD, "wwi_cdc_scd2", fabric_dir=FABRIC, source_schemas=SCHEMAS)
    b = render_copy_job(MD, "wwi_cdc_scd2", fabric_dir=FABRIC, source_schemas=SCHEMAS)
    assert [x["id"] for x in a["activities"]] == [x["id"] for x in b["activities"]]


def test_excluded_columns_get_explicit_mapping_others_auto():
    rendered = render_copy_job(MD, "wwi_cdc_scd2", fabric_dir=FABRIC, source_schemas=SCHEMAS)
    by_table = {a["properties"]["source"]["datasetSettings"]["table"]: a for a in rendered["activities"]}
    customers = by_table["Customers"]["properties"]["translator"]
    names = [m["source"]["name"] for m in customers["mappings"]]
    assert "DeliveryLocation" not in names and "CustomerID" in names
    assert by_table["StockItems"]["properties"]["translator"] == {"type": "TabularTranslator"}


def test_unsupported_type_must_be_excluded():
    raw = copy.deepcopy(MD.model_dump())
    cust = next(e for e in raw["entities"] if e["entity_id"] == "wwi_sales_customers")
    cust["source"]["exclude_columns"] = []
    md = Metadata.model_validate(raw)
    with pytest.raises(CopyJobRenderError, match="DeliveryLocation"):
        render_copy_job(md, "wwi_cdc_scd2", fabric_dir=FABRIC, source_schemas=SCHEMAS)


def test_watermark_column_comes_from_snapshot():
    rendered = render_copy_job(MD, "wwi_watermark", fabric_dir=FABRIC, source_schemas=SCHEMAS)
    cds = rendered["activities"][0]["properties"]["source"]["changeDataSettings"]
    assert cds["columns"] == [{"name": "LastEditedWhen", "type": "DateTime", "physicalType": "datetime2"}]
    assert cds["nullWatermarkBehavior"] == "Skip"


def test_combinations_without_captured_example_are_unsupported():
    raw = copy.deepcopy(MD.model_dump())
    wm = next(e for e in raw["entities"] if e["load"]["read_method"] == "watermark")
    wm["load"]["write_method"] = "scd2"
    wm["columns"] += [{"source": c} for c in ("copyjob_valid_from", "copyjob_valid_to", "copyjob_is_current")]
    md = Metadata.model_validate(raw)
    with pytest.raises(UnsupportedCopyJob):
        render_copy_job(md, "wwi_watermark", fabric_dir=FABRIC, source_schemas=SCHEMAS)


def test_write_creates_item_folder(tmp_path):
    fabric = tmp_path / "fabric"
    lh = fabric / "lh_bronze.Lakehouse"
    lh.mkdir(parents=True)
    (lh / ".platform").write_bytes((FABRIC / "lh_bronze.Lakehouse" / ".platform").read_bytes())

    results = {r.name: r.status for r in write_copy_jobs(MD, fabric, ROOT / "metadata" / "source_schemas")}
    assert results == {"cj_wwi_cdc_merge": "written", "cj_wwi_cdc_scd2": "written", "cj_wwi_watermark": "written"}
    platform = json.loads((fabric / "cj_wwi_cdc_scd2.CopyJob" / ".platform").read_text(encoding="utf-8"))
    assert platform["metadata"] == {"type": "CopyJob", "displayName": "cj_wwi_cdc_scd2"}

    again = {r.name: r.status for r in write_copy_jobs(MD, fabric, ROOT / "metadata" / "source_schemas", check=True)}
    assert again["cj_wwi_cdc_scd2"] == "unchanged"
