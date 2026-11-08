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
| `env_cdc_framework` | Environment | Runtime 2.0 (Spark 4). Custom library: the `cdc_framework` wheel from CI (`pydantic` and `pyyaml` are built into the runtime) |
| `cj_wwi_cdc_scd2` | Copy job | Customers, StockItems: CDC read, SCD Type 2 write. **Rendered from metadata** by `scripts/generate_copy_jobs.py` |
| `cj_wwi_cdc_merge` | Copy job | Orders, OrderLines: CDC read, Merge write. **Rendered from metadata** |
| `cj_wwi_watermark` | Copy job | StockItemHoldings: watermark on `LastEditedWhen`, Merge write. **Rendered from metadata** |
| `nb_ingest_files` | Notebook | Created; source of truth is `fabric/nb_ingest_files.Notebook/`. Reads `Files/landing/partners/` via the `landing` shortcut (→ `dataengcdc/landing`) |
| `nb_bronze_to_silver` | Notebook | Created; source of truth is `fabric/nb_bronze_to_silver.Notebook/` (edit in Fabric, commit) |
| `nb_build_gold` | Notebook | Paste from `notebooks/nb_build_gold.py`, then delete that file once synced here |
| `pl_master` | Data pipeline | Orchestration (below) |
| `pl_file_arrival` | Data pipeline | One file → ingest + Silver for the matching entity. Folder/file from `@pipeline()?.TriggerEvent` (parameters are the manual-run fallback) |
| `es_file_arrival` / `act_file_arrival` | Eventstream / Activator | Created by *Add trigger* on `pl_file_arrival`: BlobCreated events from `dataengcdc` → run the pipeline |
| `eh_retail_rt` / `kql_retail_rt` | Eventhouse / KQL DB | Hot path. Objects from `scripts/deploy_kql.py --apply` |
| `es_retail_events` | Eventstream | Custom endpoint → `events_raw` |
| `rtd_store_ops` | Real-Time Dashboard | Live sales, freezer temps, funnel |
| `act_store_alerts` | Activator | Alerts from `kql/generated/alerts/*.kql` |
| `sm_retail` / `rpt_retail` | Semantic model / Report | Direct Lake on `lh_gold` |

## pl_master design

```
[built] lkp_copy_jobs   "SELECT copy_job_group, copy_job_name, copy_job_id FROM ctl.vw_active_copy_job_groups ORDER BY priority"
  └─ fe_copy_jobs (parallel, batch 3)
       ├─ lkp_log_start     EXEC ctl.usp_log_run_start (stage='copy_job')  -> run_log_id
       ├─ run_copy_job      Copy job activity, copyJobId=@item().copy_job_id
       ├─ ✓ sp_log_ok       ctl.usp_log_run_end (succeeded)
       └─ ✗ sp_log_fail     ctl.usp_log_run_end (failed, @activity('run_copy_job').error.message)

[todo]  Lookup "... vw_active_entities WHERE ingestion_engine='notebook'"
  └─ ForEach -> Notebook nb_ingest_files (entity_config_json=@item().config_json)

[built] lkp_entities    "SELECT entity_id, config_json FROM ctl.vw_active_entities WHERE ingestion_engine='copy_job' ORDER BY priority"
  └─ fe_silver (parallel, batch 2)
       ├─ lkp_log_start_silver  EXEC ctl.usp_log_run_start (stage='silver')
       ├─ nb_silver             nb_bronze_to_silver (entity_config_json, pipeline_run_id)
       ├─ ✓ sp_log_ok_silver    ctl.usp_log_run_end (rows from @json(exitValue))
       ├─ ✓ sp_log_dq           ctl.usp_log_dq_results (@string(json(exitValue).dq_results))
       └─ ✗ sp_log_fail_silver  ctl.usp_log_run_end (failed, @activity('nb_silver').error.message)

[todo]  Notebook nb_build_gold
```

Pipeline gotchas we hit:
- Stored procedure Int64 params can't be blank: give a value or tick **Treat as null**.
- Type expressions through **Add dynamic content**; a value saved without the leading `@` is passed as literal text.
- The Notebook activity's connection uses a service principal, which needs Contributor on the workspace.

A Copy job's run is driven by the pipeline, so leave the Copy jobs themselves
unscheduled (or schedule them independently for lower latency on the warm path).
