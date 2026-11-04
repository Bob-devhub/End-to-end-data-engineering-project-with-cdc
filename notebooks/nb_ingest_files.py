# Fabric notebook source: nb_ingest_files
# Generic ADLS Gen2 file ingestion -> Bronze, driven by ctl.source_entity.config_json.
#
# Default lakehouse: lh_bronze. ADLS landing container is attached as a OneLake
# shortcut at Files/landing (-> abfss://landing@<account>.dfs.core.windows.net).
# Environment: env_cdc_framework (cdc_framework wheel installed).

# %% [parameters]
entity_config_json = "{}"   # from pipeline: ctl.vw_active_entities.config_json
trigger_file = ""           # optional: single file path from storage event trigger
pipeline_run_id = "manual"

# %%
import json

from pyspark.sql import functions as F

from cdc_framework.config import Entity

entity = Entity.model_validate_json(entity_config_json)
assert entity.load.ingestion_engine == "notebook", f"{entity.entity_id} is not a notebook entity"

SHORTCUT_ROOT = "Files/landing/partners"
source_path = trigger_file or f"{SHORTCUT_ROOT}/{entity.source.file_pattern}"
target_table = entity.target.table
print(f"[{entity.entity_id}] reading {source_path} -> {target_table}")

# %%
reader = spark.read.format(entity.source.format)
for k, v in entity.source.options.items():
    reader = reader.option(k, v)

df = (
    reader.load(source_path)
    .withColumn("_source_file", F.input_file_name())
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_pipeline_run_id", F.lit(pipeline_run_id))
)

# new_files: skip files already present in bronze
if entity.load.read_method == "new_files" and spark.catalog.tableExists(target_table):
    seen = spark.table(target_table).select("_source_file").distinct()
    df = df.join(seen, on="_source_file", how="left_anti")

rows_read = df.count()

# %%
if rows_read:
    mode = "overwrite" if entity.load.write_method == "overwrite" else "append"
    (df.write.format("delta").mode(mode).option("mergeSchema", "true").saveAsTable(target_table))

result = {"entity_id": entity.entity_id, "rows_read": rows_read, "rows_written": rows_read}
print(result)
notebookutils.notebook.exit(json.dumps(result))  # noqa: F821 - provided by Fabric runtime
