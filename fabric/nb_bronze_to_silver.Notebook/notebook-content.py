# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c27cfc12-93fe-496e-bb9e-1dac7562add1",
# META       "default_lakehouse_name": "lh_silver",
# META       "default_lakehouse_workspace_id": "9d04974e-4eed-49f7-8f94-d136f1e83a45",
# META       "known_lakehouses": [
# META         {
# META           "id": "c27cfc12-93fe-496e-bb9e-1dac7562add1"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "1f312b0b-0741-afc0-43c6-d007484d8eff",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     },
# META     "warehouse": {
# META       "default_warehouse": "c6f0a24c-8418-4d84-a461-cc770f84ec72",
# META       "known_warehouses": [
# META         {
# META           "id": "c6f0a24c-8418-4d84-a461-cc770f84ec72",
# META           "type": "Lakewarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

# Fabric notebook source: nb_bronze_to_silver
# Generic Bronze -> Silver: rename + cast from column_mapping, metadata-driven DQ,
# quarantine failing rows, write Silver. Works for Copy job tables (CDC/SCD2
# already applied in Bronze) and file-ingested tables alike.
#
# Default lakehouse: lh_silver. lh_bronze is attached as a second lakehouse.
# Environment: env_cdc_framework (Runtime 2.0 / Spark 4, cdc_framework wheel installed).

# %% [parameters]
entity_config_json = "{}"
pipeline_run_id = "manual"
bronze_lakehouse = "lh_bronze"

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
from cdc_framework.dq import apply_dq

entity = Entity.model_validate_json(entity_config_json)
bronze = spark.table(f"{bronze_lakehouse}.{entity.target.bronze_name}")  # lakehouse.schema.table
rows_read = bronze.count()
print(f"[{entity.entity_id}] {rows_read} bronze rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# %% rename + cast
has_ingested_at = "_ingested_at" in bronze.columns
if entity.columns:
    # try_cast: Spark 4 (Runtime 2.0) runs in ANSI mode, where a bad value makes cast() fail the
    # whole job. try_cast yields NULL instead, so the DQ rules below can quarantine the row.
    projection = [
        (F.expr(f"try_cast(`{c.source}` AS {c.type})") if c.type else F.col(f"`{c.source}`")).alias(c.target_name)
        for c in entity.columns
    ]
    if has_ingested_at:
        projection.append(F.col("_ingested_at"))
    silver = bronze.select(*projection)
else:
    silver = bronze

# File-ingested entities: keep the latest version per key if keys are defined
if entity.load.ingestion_engine == "notebook" and entity.load.primary_keys and has_ingested_at:
    from pyspark.sql import Window

    rename = {c.source: c.target_name for c in entity.columns}
    keys = [rename.get(k, k) for k in entity.load.primary_keys]
    w = Window.partitionBy(*keys).orderBy(F.col("_ingested_at").desc())
    # Rows with a NULL key are kept as-is (not collapsed into one) so DQ can quarantine each of them
    null_key = " OR ".join(f"`{k}` IS NULL" for k in keys)
    silver = silver.withColumn("_rn", F.row_number().over(w)).where(f"_rn = 1 OR {null_key}").drop("_rn")

if has_ingested_at:
    silver = silver.drop("_ingested_at")
silver = silver.withColumn("_silver_loaded_at", F.current_timestamp())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# %% data quality
valid, quarantine, dq_results = apply_dq(silver, entity.dq_rules)
rows_quarantined = quarantine.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# %% write
# Full rebuild from Bronze keeps Silver simple and idempotent.
# Next step: switch to incremental using Delta change data feed on Bronze.
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {entity.target.silver_schema}")
valid.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    entity.target.silver_name
)
if rows_quarantined:
    (
        quarantine.withColumn("_pipeline_run_id", F.lit(pipeline_run_id))
        .write.format("delta").mode("append").option("mergeSchema", "true")
        .saveAsTable(f"{entity.target.silver_schema}.quarantine_{entity.target.silver_table}")
    )

result = {
    "entity_id": entity.entity_id,
    "rows_read": rows_read,
    "rows_written": valid.count(),
    "rows_quarantined": rows_quarantined,
    "dq_results": [r.to_dict() for r in dq_results],
}
print(json.dumps(result, indent=2))
notebookutils.notebook.exit(json.dumps(result))  # noqa: F821 - provided by Fabric runtime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
