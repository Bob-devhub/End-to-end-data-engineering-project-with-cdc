import copy
from pathlib import Path

import pytest
from pydantic import ValidationError

from cdc_framework.config import Metadata, load_metadata

CONFIG_DIR = Path(__file__).resolve().parents[1] / "metadata" / "config"


@pytest.fixture(scope="module")
def md() -> Metadata:
    return load_metadata(CONFIG_DIR)


def test_repo_metadata_is_valid(md):
    assert {s.system_id for s in md.source_systems} == {"wwi_oltp", "partner_files"}
    assert len(md.entities) >= 5
    assert {s.stream_id for s in md.streams} == {"pos_transactions", "iot_readings", "clickstream"}


def test_copy_job_groups_are_homogeneous(md):
    for members in md.copy_job_groups().values():
        assert len({(m.load.read_method, m.load.write_method) for m in members}) == 1


def _raw(md: Metadata) -> dict:
    return copy.deepcopy(md.model_dump())


def test_mixing_cdc_and_watermark_in_one_copy_job_is_rejected(md):
    raw = _raw(md)
    wm = next(e for e in raw["entities"] if e["load"]["read_method"] == "watermark")
    cdc = next(e for e in raw["entities"] if e["load"]["read_method"] == "cdc")
    wm["load"]["copy_job_group"] = cdc["load"]["copy_job_group"]
    wm["load"]["write_method"] = cdc["load"]["write_method"]
    with pytest.raises(ValidationError, match="mixes read_method"):
        Metadata.model_validate(raw)


def test_mixing_scd2_and_merge_in_one_copy_job_is_rejected(md):
    raw = _raw(md)
    scd2 = next(e for e in raw["entities"] if e["load"]["write_method"] == "scd2")
    merge = next(e for e in raw["entities"] if e["load"]["write_method"] == "scd1_merge"
                 and e["load"]["read_method"] == "cdc")
    merge["load"]["copy_job_group"] = scd2["load"]["copy_job_group"]
    with pytest.raises(ValidationError, match="mixes write_method"):
        Metadata.model_validate(raw)


def test_watermark_requires_column(md):
    raw = _raw(md)
    wm = next(e for e in raw["entities"] if e["load"]["read_method"] == "watermark")
    wm["load"]["watermark_column"] = None
    with pytest.raises(ValidationError, match="watermark_column"):
        Metadata.model_validate(raw)


def test_cdc_on_adls_source_is_rejected(md):
    raw = _raw(md)
    f = next(e for e in raw["entities"] if e["system_id"] == "partner_files")
    f["load"].update(ingestion_engine="copy_job", read_method="cdc", write_method="scd1_merge",
                     copy_job_group="bad", primary_keys=["promo_id"])
    with pytest.raises(ValidationError, match="cdc requires an azure_sql"):
        Metadata.model_validate(raw)


def test_unknown_system_is_rejected(md):
    raw = _raw(md)
    raw["entities"][0]["system_id"] = "nope"
    with pytest.raises(ValidationError, match="unknown system_id"):
        Metadata.model_validate(raw)


def test_dq_rule_on_unmapped_column_is_rejected(md):
    raw = _raw(md)
    raw["entities"][0]["dq_rules"].append({"rule_type": "not_null", "column": "does_not_exist"})
    with pytest.raises(ValidationError, match="unmapped columns"):
        Metadata.model_validate(raw)
