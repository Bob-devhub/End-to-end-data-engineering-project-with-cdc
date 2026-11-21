# Contoso Retail — Metadata-Driven Data Platform on Microsoft Fabric

An end-to-end data engineering project with three ingestion speeds. All of
them are driven by metadata in Git and deployed with GitHub Actions.

| Path | Mechanism | Latency | Lands in |
|---|---|---|---|
| **Hot** | Event simulator → Eventstream → Eventhouse (update policies, materialized views) → Real-Time Dashboard + Activator | seconds | `kql_retail_rt` |
| **Warm** | Azure SQL DB (CDC) → **Copy job** (CDC read, SCD1 Merge / SCD2 write) | minutes | `lh_bronze` |
| **Cold** | ADLS Gen2 partner files → storage event → generic notebook | on arrival | `lh_bronze` |

Bronze → Silver (rename, cast, DQ, quarantine) and Silver → Gold (star schema)
are shared by all batch paths. Eventhouse tables join Gold through **OneLake
availability**, and a Direct Lake semantic model serves Power BI.

```
 Azure SQL DB (WWI, CDC) ──Copy job──┐
                                     ├─► lh_bronze ─► nb_bronze_to_silver ─► lh_silver ─► nb_build_gold ─► lh_gold ─► Direct Lake ─► Power BI
 ADLS Gen2 landing ──nb_ingest_files─┘                    ▲ DQ rules                                  ▲
                                                          │                                           │ OneLake availability
 sqldb_control (ctl.*) ◄── scripts/sync_metadata.py ◄── metadata/config/*.yml (Git = source of truth)   │
                                                          │                                           │
 simulator ─► Eventstream ─► events_raw ─► update policies ─► typed tables ─► materialized views ─────┘
                                                                     └─► Real-Time Dashboard / Activator alerts
```

**The demo:** merge a PR that adds one YAML file. The new table or stream flows
end to end with no pipeline or notebook changes.

## Repository layout

```
metadata/
  config/                   # SOURCE OF TRUTH - validated by CI
    source_systems.yml
    entities/<system>/*.yml # batch + CDC entities (load methods, column mapping, DQ rules)
    streams/*.yml           # real-time streams (fields, aggregates, alerts)
    fabric.yml              # DEV Fabric connection ids used when rendering items
  source_schemas/           # source column snapshot (scripts/snapshot_source_schema.py)
  ddl/001_control_tables.sql# control DB bootstrap (then sqldb_control's synced project owns the schema)
source/azure_sql/           # enable CDC + change simulator for the OLTP source
src/cdc_framework/          # Python package (also built as a wheel for the Fabric Environment)
  config.py                 # pydantic metadata model + cross-entity validation
  dq.py                     # rules -> Spark SQL predicates, apply_dq()
  kql.py                    # stream metadata -> KQL tables, functions, update policies, MVs, alerts
  copy_jobs.py              # entities -> fabric/cj_*.CopyJob definitions (one per copy_job_group)
  control_db.py             # sync metadata -> control DB
  auth.py                   # Entra credential (az login / GitHub OIDC)
scripts/                    # validate_metadata, sync_metadata, snapshot_source_schema, generate_copy_jobs, deploy_kql
simulator/                  # POS / IoT / clickstream event producer
fabric/                     # Fabric Git integration folder (items synced from DEV)
kql/generated/              # generated KQL (git-ignored, CI artifact)
.github/workflows/          # ci.yml (lint, validate, test, build wheel), sync.yml (metadata → a stage's control DB)
docs/                       # onboarding guide, Copy job template guide
```

## Quick start (local)

```bash
python -m venv .venv
```
```bash
.venv/Scripts/pip install -e ".[dev]"      # macOS/Linux: .venv/bin/pip
```
```bash
python scripts/validate_metadata.py
```
```bash
pytest -q
```
```bash
python scripts/deploy_kql.py                # -> kql/generated/deploy.kql + alerts/
```
```bash
python scripts/generate_copy_jobs.py --check   # fabric/cj_*.CopyJob match metadata?
```
```bash
python simulator/producer.py --dry-run --count 5
```

## Build phases

- [x] **1. Foundations.** Create Azure SQL DB (serverless) and restore WideWorldImporters, then run
      `source/azure_sql/001_enable_cdc.sql`. Create the Fabric workspace, `sqldb_control` (run the DDL),
      and `lh_bronze`/`lh_silver`/`lh_gold`. Connect Git to `/fabric`.
- [ ] **2. Warm path.** ~~Build `cj_wwi_cdc_scd2` by hand~~ (done, now rendered from metadata). Run
      `002_simulate_changes.sql` and check the SCD2 rows in Bronze. All three Copy jobs are
      now rendered from metadata ([docs/copy-job-template.md](docs/copy-job-template.md)).
- [x] **3. Metadata sync.** `scripts/sync_metadata.py`, then `pl_master` driven by `ctl.vw_*` views and logging procs.
      Stage 1 (Copy jobs) and stage 2 (Silver + DQ logging) verified; file ingest and Gold stages come with phases 5 and 7.
- [x] **4. Silver and DQ.** `env_cdc_framework` (Runtime 2.0 + wheel) and `nb_bronze_to_silver`; verified on Customers (SCD2).
- [x] **5. Cold path.** ADLS (`dataengcdc/landing`) shortcut, `nb_ingest_files`, ingest stage in `pl_master`, and
      `pl_file_arrival` run by a storage event trigger (`es_file_arrival` → `act_file_arrival`). Sample files:
      `scripts/generate_partner_files.py`.
- [x] **6. Hot path.** `eh_retail_rt`/`kql_retail_rt` (`deploy_kql.py --apply`), `es_retail_events` custom endpoint →
      `events_raw` (Direct ingestion), simulator, `rtd_store_ops`, freezer and sales-spike alerts
      (`--freezer-fault-store 7`), and `fact_pos_daily` in Gold via the `rt_pos_transactions` shortcut.
- [x] **7. Gold and BI.** `nb_build_gold` (gold stage in `pl_master`), Direct Lake `sm_retail` (13 measures, ops tables
      shortcut from `sqldb_control`), `rpt_retail` (Sales, Stores and Stock, Operations). 
- [x] **8. CI/CD.** Fabric deployment pipeline DEV → TEST → PROD, `vl_retail`, `sm_retail` parameter rules,
      `sync.yml` (OIDC, `sp-fabric-retail`). See *Promotion* below.

## Promotion (DEV → TEST → PROD)

| Stage | Workspace | Changed by | File ingest |
|---|---|---|---|
| DEV | `retail-dev` | Git integration (`bob/dev`) | event trigger (`pl_file_arrival`) + `pl_master` |
| TEST | `Retail-test` | deployment pipeline | scheduled `pl_master` |
| PROD | `Retail` | deployment pipeline (approval) | scheduled `pl_master` |

All stages read the same **sources**: the WWI database and the `dataengcdc/landing` container (partner drop
zone). Each stage keeps its own Copy job checkpoints, `new_files` history, control DB, lakehouses and model.
Don't drop test files into `landing`: every stage ingests what is there.

Per-stage values and how they're handled:

| What | How |
|---|---|
| Item references (lakehouses, notebooks, Copy jobs, KQL DB, `sqldb_control`, report → model) | remapped by the deployment pipeline |
| Copy job workspace in `pl_master` | `@pipeline().DataFactory` |
| `lh_bronze` shortcut `landing` subpath | `vl_retail.landing_container` (resolved at deploy time: redeploy `lh_bronze` after changing a stage's active value set) |
| `sm_retail` Direct Lake source | model parameters `WorkspaceId`, `LakehouseId` + a **parameter rule** per stage |
| Connections (FabricSql, CopyJob, Notebook, Lakehouse, `conn-azsql-wwi`, `conn-adls-landing`) | shared by all stages |
| Control DB metadata and Copy job ids | `sync.yml` (or `scripts/sync_metadata.py`) per stage after a deployment |

After a deployment that changes metadata or Copy jobs, run **Sync metadata** (`sync.yml`) for that stage.
The service principal `msgraph` (Notebook connection) and `sp-fabric-retail` need Contributor on each stage.
Activator file-trigger rules are not remapped by deployments, so they live only in DEV.

## Key design decisions

- **Azure SQL DB as the OLTP source, SQL database in Fabric as the control DB.** Copy job CDC
  doesn't support SQL database in Fabric as a source.
- **Schema-enabled lakehouses.** Bronze mirrors the source (`lh_bronze.Sales.Customers`); Silver/Gold use `dbo`.
- **Copy jobs are rendered, not hand-edited.** CI fails if `fabric/cj_*.CopyJob` drifts from metadata.
- **One Copy job per `(read_method, write_method)` group.** Copy job applies both job-wide,
  and the validator enforces this.
- **Single raw table in Eventhouse.** Update policies route by `event_type`, so adding a stream
  doesn't touch the Eventstream.
- **Git is the source of truth for metadata.** The control DB config tables are fully reloaded on
  every sync, and operational tables (run log, watermarks, DQ results) are never touched.
