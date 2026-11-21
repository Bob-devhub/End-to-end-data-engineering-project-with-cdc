# Lessons learned: metadata-driven CDC platform on Microsoft Fabric

Retrospective for the build (Nov 2026): eight phases from an empty repo to DEV → TEST → PROD, with
CDC, file and streaming ingestion, Silver/DQ, a Gold star schema, Direct Lake BI and Fabric
deployment pipelines.

## What we ended up with

| Area | Result |
|---|---|
| Warm path | Azure SQL (WWI, CDC) → 3 Copy jobs rendered from metadata (SCD2, merge, watermark) → `lh_bronze` |
| Cold path | `dataengcdc/landing` (shared partner drop zone) → `nb_ingest_files` (`new_files`), event trigger in DEV, scheduled in TEST/PROD |
| Silver | One generic notebook: rename, `try_cast`, metadata DQ rules, quarantine, per-run logging in `sqldb_control` |
| Gold | Star schema with SCD2 point-in-time keys, `-1` unknown members, row-count and unmatched-key checks |
| BI | Direct Lake `sm_retail` (16 measures), `rpt_retail` (Sales, Stores and Stock, Operations) |
| Hot path | Simulator → Eventstream → Eventhouse (update policies, materialized views) → dashboard, alerts, `fact_pos_daily` |
| Promotion | Git on DEV only; deployment pipeline to TEST/PROD; `vl_retail`; parameter rules; `sync.yml` over OIDC |

Verified in each stage with `ctl.run_log`, `ctl.dq_result` and DAX queries against each stage's model.

## What went well

- **Metadata as the single source of truth.** Adding a table means adding YAML: CI validates it, Copy jobs are
  rendered from it, and the control DB is reloaded from it. `generate_copy_jobs.py --check` caught drift every time.
- **Build one by hand, then generate.** The first Copy job, built in the UI, gave the exact definition shape
  (`copyjob_*` SCD2 columns, `Upsert` for Merge, watermark settings), so the renderer matched Fabric on the first try.
- **Logging from day one.** `usp_log_run_start/end` and `usp_log_dq_results` turned every check into a SQL query.
  Most problems were found by reading `run_log`, not the Fabric UI.
- **Checking results against expected numbers.** Writing the expected row counts down before each run caught real bugs:
  NULL-key rows collapsed by the dedupe, a store dropped from `dim_store` by quarantine, the Test model reading DEV.
- **Using the Fabric REST API for fiddly edits** (semantic model TMDL, pipeline expressions, the variable library).
  It was faster and more exact than clicking through the UI, and reading the definition back proved each change.
- **Decision: shared sources, isolated stages.** Every stage reads the same WWI DB and landing zone and keeps its own
  checkpoints, lakehouses, control DB and model. Promotion moves code, not data.

## What we'd do differently

- **Plan for promotion from the start.** We parameterized after building, so we had to retrofit: the hard-coded workspace
  in the Copy job activity, the shortcut subpath, the Direct Lake URL in the model, and an Activator that isn't stage-aware.
  Next time, design each item for multiple stages when it's created.
- **Use a dedicated service identity from the start.** All connections use the developer's own sign-in (OAuth), and the Notebook
  connection borrowed an unrelated SP (`msgraph`). See *Open risks* below.
- **Don't fight a wizard's output.** The Eventstream → Eventhouse wizard kept inferring columns and mappings. Making the parse
  functions accept either layout (`pack_all()`) took ten minutes; fighting the wizard took two hours.
- **Keep secrets out of chat and screenshots.** An Event Hub connection string with its key was pasted into the
  conversation. The key was regenerated and the Eventstream later recreated, but the habit matters.
- **Assign the stage workspace before the first deployment.** Deploying to an unassigned stage made Fabric create
  `retail-dev [Test]`, and we had to rename it.

## Fabric gotchas (the expensive ones)

| # | Symptom | Cause | Fix |
|---|---|---|---|
| 1 | Copy job CDC missing from read methods | SQL database in Fabric isn't a CDC source; S0 Azure SQL doesn't support CDC | Azure SQL DB serverless as the OLTP source; Fabric SQL DB only for control |
| 2 | Copy job fails on `geography` | Delta has no geography type | `source.exclude_columns` (explicit column mapping) |
| 3 | SCD2 columns not where expected | Copy job adds string columns `copyjob_valid_from/_to/_is_current` (load time) | Map them in metadata; use the source temporal `ValidFrom` for business time |
| 4 | Notebook ignores pipeline parameters | The parameter cell wasn't marked (`# PARAMETERS CELL`) | Toggle the parameter cell; check after every commit |
| 5 | `cast()` fails the whole job | Runtime 2.0 / Spark 4 runs in ANSI mode | `try_cast` + DQ rules; `_metadata.file_path`, not `input_file_name()` |
| 6 | Stored procedure: "is not a valid value for Int64" | Blank Int64 parameters | Give a value or tick **Treat as null** |
| 7 | Stored procedure receives the expression text | Value saved without the leading `@` | Always use **Add dynamic content** |
| 8 | Notebook activity: "workspace … Forbidden" | The Notebook connection's SP had no workspace role | Contributor for that SP on every stage |
| 9 | Eventhouse rows ×2 | A deleted Eventstream destination left a hidden ingestion connection | Recreate the Eventstream; never delete and re-add a Direct ingestion destination |
| 10 | `.delete` refused on a table | OneLake availability (mirroring) blocks deletes | Disable mirroring, delete, re-enable with backfill |
| 11 | Activator filter can't be changed | "Updating event subscription filters is not supported" | Create a new source/trigger |
| 12 | Activator file trigger points at DEV in TEST | Activator pipeline actions and Eventstream sources aren't remapped by deployment | File trigger only in DEV; TEST/PROD use scheduled `pl_master` |
| 13 | TEST shortcut still points at DEV's path | Shortcut variables resolve at create/deploy time, and an unchanged item isn't redeployed | Re-select the variable (Manage shortcut) or redeploy after switching the value set |
| 14 | TEST model shows DEV numbers | The Direct Lake on OneLake URL holds literal workspace/lakehouse IDs | Model parameters `WorkspaceId`/`LakehouseId` + a deployment parameter rule |
| 15 | Direct Lake refresh: "table does not exist" | The refresh ran right after a new (empty) table was created | Retry / refresh that table; always create every table the model expects |
| 16 | Generic pipeline connections look stage-specific | FabricSql, CopyJob, Notebook and Lakehouse connections hold only credentials | Share them across stages; item references are remapped |
| 17 | Overlapping `pl_master` runs, a Delta read error | A double click plus the hourly schedule's first slot | Pipeline concurrency 1; don't start manually near a scheduled slot |
| 18 | GitHub OIDC: AADSTS700213 | GitHub now sends `repo:owner@id/name@id:environment:X` | Federated credential subject in the ID format |
| 19 | Fabric API 401 for the SP | Tenant setting "Service principals can call Fabric public APIs" limited to a group | Add the SP to that group (it can't be a Fabric admin) |

## Open risks and things easy to overlook

Ordered by impact. Facts were checked on 2026-11-21.

1. **Personal OAuth connections run Prod.** `conn-azsql-wwi`, `FabricSql bob`, `CopyJob bob`, the Lakehouse
   connection and `conn-adls-landing` all use one person's sign-in. They fail after 90 days without use (it already
   happened once) or when that account changes or leaves. **Move PROD (and TEST) connections to a service principal
   or workspace identity.** `sp-fabric-retail` already exists.
2. **The schedules may already have stopped.** TEST and PROD `pl_master` are hourly, but start and end are both saved as
   `2026-11-21 10:00`. Set a proper end date and check the next run happens. DEV has **no** schedule.
3. **CDC retention vs. no schedule.** Azure SQL CDC keeps changes for **3 days by default**. If a stage's Copy jobs don't run
   for longer (DEV has no schedule; TEST/PROD if the schedules stopped), changes are lost and that stage needs a full
   reload. Schedule DEV at least daily, or raise retention (`sys.sp_cdc_change_job @retention`).
4. **Capacity cost of hourly runs.** Each `pl_master` run is about 12 minutes of Spark plus Copy jobs. Hourly in TEST and PROD
   is about 2 × 12 minutes every hour. TEST rarely needs more than daily.
5. **Prod contains sample data.** The shared `landing` holds generated files with deliberately bad rows, and every stage
   ingested them. Before real partner files arrive, clear `partner.*` in each stage's Bronze/Silver (or move the samples
   out and rebuild), and keep test files out of `landing`. A `landing/_samples/` folder outside `partners/` works.
6. **No failure alerts.** Failures are logged in `run_log`, but nobody is told. Add an Activator or Outlook action on
   `pl_master` failure, or an alert on `run_log.status = 'failed'`.
7. **Stuck `running` rows need manual cleanup.** Cancelled runs leave `running` rows (we fixed several by hand).
   Add a step to `pl_master` or a small procedure that marks rows older than a few hours as `abandoned`.
8. **Azure SQL firewall.** `client-bob-dev` (a home IP) and `AllowAllWindowsAzureIps` are open. Remove the client rule when
   you no longer query from this PC; consider private networking for real data.
9. **Git PAT for Fabric Git integration.** The GitHub token used by the DEV workspace has an expiry date. Note it and
   renew it before then, or switch to a GitHub App connection.
10. **Growth without retention.** Silver rebuilds fully every run (fine at this size). `quarantine_*` tables and
    `run_log`/`dq_result` only append. Add retention, and consider incremental Silver (Delta change data feed) as volumes grow.
11. **The hot path exists only in DEV.** TEST/PROD Eventstreams have no destination and PROD has no store alerts, so
    `fact_pos_daily` is empty there. Fine for now; decide whether PROD needs live POS before go-live.
12. **Notebooks aren't covered by CI.** Their code lives in `fabric/*.Notebook` and is excluded from ruff, and there are no
    tests that run them. A smoke-test pipeline in TEST after each deployment (run `pl_master`, assert on `run_log`) would close the gap.
13. **Branch hygiene.** `bob/dev` merges into `main` through PRs, but `main`'s merge commits never come back to `bob/dev`.
    Occasionally merge `main` into `bob/dev` to avoid confusing diffs later.
14. **Leftovers.** `vl_retail.landing_connection_id` is unused. `act_file_arrival` holds both the DEV file trigger and the
    store alerts (rename it, or split it, for clarity). The local `.venv` was repointed to a moved Python 3.14 (recreate it
    if Python moves again).

## Runbook pointers

- Onboard a table/feed/stream: [onboarding-a-table.md](onboarding-a-table.md)
- Copy job rendering: [copy-job-template.md](copy-job-template.md)
- Promotion and per-stage values: README → *Promotion (DEV → TEST → PROD)*
- Health check: `SELECT stage, status, COUNT(*) FROM ctl.run_log WHERE started_at > DATEADD(day,-1,SYSUTCDATETIME()) GROUP BY stage, status`
