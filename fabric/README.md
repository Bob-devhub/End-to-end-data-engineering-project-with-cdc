# fabric/ — Git-synced Fabric items

This folder is the **Git folder** for the DEV workspace. Connect it in
Fabric: *Workspace settings → Git integration → GitHub → this repo, branch `main`,
folder `/fabric`*. Fabric writes each item here as `<name>.<Type>/` with a
`.platform` file — don't hand-create those; create items in the UI, then commit
from the Source control pane.

`deploy/deploy.py` (fabric-cicd) publishes the contents of this folder to TEST/PROD.

## Items to create in the DEV workspace

| Item | Type | Purpose |
|---|---|---|
| `sqldb_control` | SQL database | Control plane. Bootstrapped with `metadata/ddl/001_control_tables.sql`; its synced database project here is now the schema source of truth |
| `lh_bronze` | Lakehouse (schemas enabled) | Copy job targets as `<SourceSchema>.<Table>`, file ingests in `partner.*`. Shortcut `Files/landing` → ADLS `landing` container |
| `lh_silver` | Lakehouse | Cleansed, typed, DQ-checked tables |
| `lh_gold` | Lakehouse | Star schema. Shortcut `Tables/rt_pos_transactions` → Eventhouse table |
| `env_cdc_framework` | Environment | Custom library: the `cdc_framework` wheel from CI |
| `cj_wwi_cdc_scd2` | Copy job | Customers, StockItems: CDC read, SCD Type 2 write. **Rendered from metadata** by `scripts/generate_copy_jobs.py` |
| `cj_wwi_cdc_merge` | Copy job | Orders, OrderLines: CDC read, Merge write. **Rendered from metadata** |
| `cj_wwi_watermark` | Copy job | StockItemHoldings: watermark on `LastEditedWhen`, Merge write. Build by hand once to capture the template |
| `nb_ingest_files` | Notebook | Paste from `notebooks/nb_ingest_files.py` |
| `nb_bronze_to_silver` | Notebook | Paste from `notebooks/nb_bronze_to_silver.py` |
| `nb_build_gold` | Notebook | Paste from `notebooks/nb_build_gold.py` |
| `pl_master` | Data pipeline | Orchestration (below) |
| `pl_file_arrival` | Data pipeline | Storage event trigger on ADLS `landing/partners/**` |
| `eh_retail_rt` / `kql_retail_rt` | Eventhouse / KQL DB | Hot path. Objects from `scripts/deploy_kql.py --apply` |
| `es_retail_events` | Eventstream | Custom endpoint → `events_raw` |
| `rtd_store_ops` | Real-Time Dashboard | Live sales, freezer temps, funnel |
| `act_store_alerts` | Activator | Alerts from `kql/generated/alerts/*.kql` |
| `sm_retail` / `rpt_retail` | Semantic model / Report | Direct Lake on `lh_gold` |

## pl_master design

```
Lookup  "SELECT * FROM ctl.vw_active_copy_job_groups ORDER BY priority"
  └─ ForEach (sequential=false, batch 3)
       ├─ Stored proc  ctl.usp_log_run_start (stage='copy_job', entity=@item().copy_job_group)
       ├─ Copy job activity  -> @item().copy_job_name
       └─ Stored proc  ctl.usp_log_run_end (succeeded | failed)

Lookup  "SELECT * FROM ctl.vw_active_entities WHERE ingestion_engine='notebook' ORDER BY priority"
  └─ ForEach -> Notebook nb_ingest_files (entity_config_json=@item().config_json)

Lookup  "SELECT * FROM ctl.vw_active_entities ORDER BY priority"
  └─ ForEach
       ├─ Notebook nb_bronze_to_silver (entity_config_json=@item().config_json)
       ├─ Stored proc ctl.usp_log_run_end  (rows from exitValue)
       └─ Stored proc ctl.usp_log_dq_results (@json(activity().output.result.exitValue).dq_results)

Notebook nb_build_gold
```

A Copy job's run is driven by the pipeline, so leave the Copy jobs themselves
unscheduled (or schedule them independently for lower latency on the warm path).
