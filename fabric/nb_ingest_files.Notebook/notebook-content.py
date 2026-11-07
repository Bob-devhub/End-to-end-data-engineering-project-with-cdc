# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d0fc3f77-0e1a-4ec3-a713-c67ab82758e9",
# META       "default_lakehouse_name": "lh_bronze",
# META       "default_lakehouse_workspace_id": "9d04974e-4eed-49f7-8f94-d136f1e83a45",
# META       "known_lakehouses": [
# META         {
# META           "id": "d0fc3f77-0e1a-4ec3-a713-c67ab82758e9"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "1f312b0b-0741-afc0-43c6-d007484d8eff",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

# Fabric notebook source: nb_ingest_files
# Generic ADLS Gen2 file ingestion -> Bronze, driven by ctl.source_entity.config_json.
#
# Default lakehouse: lh_bronze. ADLS landing container is attached as a OneLake
# shortcut at Files/landing (-> abfss://landing@<account>.dfs.core.windows.net).
# Environment: env_cdc_framework (Runtime 2.0 / Spark 4, cdc_framework wheel installed).

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# %% [parameters]
entity_config_json = "{}"   # from pipeline: ctl.vw_active_entities.config_json
trigger_file = ""           # optional: single file path from storage event trigger
pipeline_run_id = "manual"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# %%
import json

from pyspark.sql import functions as F

from cdc_framework.config import Entity

entity = Entity.model_validate_json(entity_config_json)
assert entity.load.ingestion_engine == "notebook", f"{entity.entity_id} is not a notebook entity"

SHORTCUT_ROOT = "Files/landing/partners"
source_path = trigger_file or f"{SHORTCUT_ROOT}/{entity.source.file_pattern}"
target_table = entity.target.bronze_name  # schema-enabled lakehouse: schema.table
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {entity.target.schema_name}")
print(f"[{entity.entity_id}] reading {source_path} -> {target_table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# %%
reader = spark.read.format(entity.source.format)
for k, v in entity.source.options.items():
    reader = reader.option(k, v)

df = (
    reader.load(source_path)
    .withColumn("_source_file", F.col("_metadata.file_path"))  # input_file_name() is deprecated in Spark 4
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_pipeline_run_id", F.lit(pipeline_run_id))
)

# new_files: skip files already present in bronze
if entity.load.read_method == "new_files" and spark.catalog.tableExists(target_table):
    seen = spark.table(target_table).select("_source_file").distinct()
    df = df.join(seen, on="_source_file", how="left_anti")

rows_read = df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# %%
if rows_read:
    mode = "overwrite" if entity.load.write_method == "overwrite" else "append"
    (df.write.format("delta").mode(mode).option("mergeSchema", "true").saveAsTable(target_table))

result = {"entity_id": entity.entity_id, "rows_read": rows_read, "rows_written": rows_read}
print(result)
notebookutils.notebook.exit(json.dumps(result))  # noqa: F821 - provided by Fabric runtime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
